//! Read a contiguous range of raw Delta commits.
//!
//! A [`CommitRange`] holds an inclusive `[start_version, end_version]` range and the list of
//! commit files; reads are lazy (no JSON I/O until [`CommitRange::commits`] is called).
//!
//! Two construction paths, differing only in how `_delta_log/` is listed at build time:
//! - [`CommitRange::builder_for`]: lists `_delta_log/` for the requested range.
//! - [`CommitRange::builder_from`]: reuses an existing snapshot's `LogSegment`, avoiding the
//!   listing.
//!
//! [`CommitRange::commits`] takes a list of [`DeltaAction`]
//! and an optional `start_snapshot`. When the snapshot is supplied, the iterator seeds its
//! `latest_protocol` / `latest_metadata` from it and the snapshot's version must match the
//! range's `start_version` (in ascending order) or `end_version` (in descending order). When
//! the snapshot is `None`, validation is purely commit-driven. See [`CommitRange::commits`]
//! for protocol-validation details.
//!
//! # Example
//! ```no_run
//! use std::sync::Arc;
//! use delta_kernel::commit_range::{CommitRange, DeltaAction};
//! use delta_kernel::{Engine, Error, Snapshot};
//! use delta_kernel::object_store::local::LocalFileSystem;
//! use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
//!
//! let engine: Arc<dyn Engine> =
//!     Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
//! let start_snapshot = Snapshot::builder_for("file:///data/T").at_version(0).build(engine.as_ref())?;
//! let range = CommitRange::builder_for("file:///data/T", 0)
//!     .with_end_version(4)
//!     .build(engine.as_ref())?;
//!
//! for commit in range.commits(
//!     engine.clone(),
//!     Some(start_snapshot),
//!     &[DeltaAction::Add, DeltaAction::Remove],
//! )? {
//!     let commit = commit?;
//!     println!("v={} ts={}", commit.version(), commit.timestamp());
//!     for batch in commit.get_actions(engine.as_ref())? {
//!         let _batch = batch?;
//!     }
//! }
//! # Ok::<(), Error>(())
//! ```

mod actions;
mod builder;

use std::sync::Arc;

pub use actions::{CommitAction, DeltaAction};
pub use builder::{CommitOrdering, CommitRangeBuilder};
use url::Url;

use crate::actions::{
    Metadata, Protocol, ADD_FIELD, CDC_FIELD, CHECKPOINT_METADATA_FIELD, COMMIT_INFO_FIELD,
    DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD, REMOVE_FIELD, SET_TRANSACTION_FIELD,
    SIDECAR_FIELD,
};
use crate::path::ParsedLogPath;
use crate::schema::{SchemaRef, StructField, StructType};
use crate::snapshot::SnapshotRef;
use crate::table_features::Operation;
use crate::{DeltaResult, Engine, Error, Version};

/// A contiguous range of Delta commits, holding resolved `[start_version, end_version]` bounds
/// plus the materialized commit-file pointers in `commit_files`.
///
/// The pointer order matches the [`CommitOrdering`] requested at build time (ascending by
/// default; reversed if [`CommitOrdering::DescendingOrder`]). Reading the underlying actions is
/// lazy via [`CommitRange::commits`].
#[derive(Debug)]
pub struct CommitRange {
    table_root: Url,
    commit_files: Vec<ParsedLogPath>,
    start_version: Version,
    end_version: Version,
    commit_ordering: CommitOrdering,
}

impl CommitRange {
    /// Begin building a [`CommitRange`] rooted at `table_root`, starting at `start_version`.
    pub fn builder_for(table_root: impl AsRef<str>, start_version: Version) -> CommitRangeBuilder {
        CommitRangeBuilder::new_for(table_root, start_version)
    }

    /// Begin building a [`CommitRange`] derived from an existing snapshot, starting at
    /// `start_version`.
    ///
    /// The snapshot's table root anchors the range, and the snapshot's `LogSegment` is
    /// reused to enumerate commits, avoiding an extra delta-log listing.
    pub fn builder_from(snapshot: SnapshotRef, start_version: Version) -> CommitRangeBuilder {
        CommitRangeBuilder::new_from(snapshot, start_version)
    }

    /// First version (inclusive) in the range.
    pub fn start_version(&self) -> Version {
        self.start_version
    }

    /// Last version (inclusive) in the range.
    pub fn end_version(&self) -> Version {
        self.end_version
    }

    /// The table root URL this range was built from.
    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// Iterator over the commits in the range, yielding one [`CommitAction`] per commit.
    ///
    /// - `engine`: performs the per-commit JSON reads.
    /// - `start_snapshot`: optional snapshot whose version anchors the range and seeds
    ///   protocol/metadata validation; `None` validates from the commits alone.
    /// - `actions`: the action kinds to project into each commit's read schema.
    ///
    /// Actions are returned raw, exactly as recorded in the commit JSON; no column-mapping
    /// translation is applied.
    ///
    /// This is operation-agnostic: requesting [`DeltaAction::Cdc`] returns the raw `cdc` action
    /// records and does NOT impose change-data-feed support (`Operation::Cdf`). It does not
    /// materialize a change data feed.
    ///
    /// Returns `Err` if `actions` is empty or contains duplicate kinds, or if `start_snapshot`
    /// belongs to a different table, its version does not match the range anchor, or its table
    /// does not support scanning.
    pub fn commits(
        &self,
        engine: Arc<dyn Engine>,
        start_snapshot: Option<SnapshotRef>,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<CommitAction>> + Send> {
        if actions.is_empty() {
            return Err(Error::generic("at least one DeltaAction must be requested"));
        }

        let (latest_protocol, latest_metadata) = match &start_snapshot {
            Some(snapshot) => {
                if snapshot.table_root() != &self.table_root {
                    return Err(Error::generic(format!(
                        "snapshot table root ({}) does not match commit range table root ({})",
                        snapshot.table_root(),
                        self.table_root,
                    )));
                }
                let (anchor_version, anchor_name) = match self.commit_ordering {
                    CommitOrdering::AscendingOrder => (self.start_version, "start_version"),
                    CommitOrdering::DescendingOrder => (self.end_version, "end_version"),
                };
                if snapshot.version() != anchor_version {
                    return Err(Error::generic(format!(
                        "snapshot version {} does not match {anchor_name} ({anchor_version})",
                        snapshot.version(),
                    )));
                }
                let table_config = snapshot.table_configuration();
                table_config.ensure_operation_supported(Operation::Scan)?;
                (
                    Some(table_config.protocol().clone()),
                    Some(table_config.metadata().clone()),
                )
            }
            None => (None, None),
        };

        let read_schema = Arc::new(StructType::try_new(actions.iter().map(action_to_field))?);

        Ok(CommitActionsIterator {
            engine,
            table_root: self.table_root.clone(),
            log_path_iter: self.commit_files.clone().into_iter(),
            commit_ordering: self.commit_ordering,
            read_schema,
            latest_protocol,
            latest_metadata,
        })
    }
}

/// Iterator yielded by [`CommitRange::commits`]. Holds the iterator's accumulated
/// `latest_protocol` / `latest_metadata` and
/// constructs a fresh [`CommitAction`] for each commit, running per-commit protocol
/// validation before yielding.
pub(crate) struct CommitActionsIterator {
    engine: Arc<dyn Engine>,
    table_root: Url,
    log_path_iter: std::vec::IntoIter<ParsedLogPath>,
    commit_ordering: CommitOrdering,
    read_schema: SchemaRef,
    latest_protocol: Option<Protocol>,
    latest_metadata: Option<Metadata>,
}

impl CommitActionsIterator {
    /// Build and validate a `CommitAction` for `log_path` (seeded with the iterator's accumulated
    /// state), then advance that state for the next commit.
    ///
    /// Accumulated `(Protocol, Metadata)` only flows forward in ascending order. Walking backward
    /// can't reconstruct an earlier version's effective state, so descending resets it to `None`.
    /// Commits below the anchor are validated/timestamped best-effort against only their own
    /// actions. Another solution is to walk the commit in ascending then reversing in the
    /// [`CommitOrdering::DescendingOrder`] scenario.
    fn try_advance(&mut self, log_path: ParsedLogPath) -> DeltaResult<CommitAction> {
        let version = log_path.version;
        let commit_action = CommitAction::try_new(
            self.engine.as_ref(),
            self.table_root.clone(),
            log_path,
            self.read_schema.clone(),
            self.latest_protocol.clone(),
            self.latest_metadata.clone(),
        )
        .map_err(|e| with_version_context(version, e))?;

        match self.commit_ordering {
            CommitOrdering::AscendingOrder => {
                self.latest_protocol = commit_action.protocol().cloned();
                self.latest_metadata = commit_action.metadata().cloned();
            }
            CommitOrdering::DescendingOrder => {
                self.latest_protocol = None;
                self.latest_metadata = None;
            }
        }
        Ok(commit_action)
    }
}

/// Prepend `commit v={version}` context to `err`, preserving the original variant for the two
/// kernel error kinds that protocol validation surfaces ([`Error::Unsupported`] and
/// [`Error::InvalidProtocol`]). Other variants fall back to [`Error::generic`].
fn with_version_context(version: Version, err: Error) -> Error {
    match err {
        Error::Unsupported(msg) => Error::Unsupported(format!("commit v={version}: {msg}")),
        Error::InvalidProtocol(msg) => Error::InvalidProtocol(format!("commit v={version}: {msg}")),
        other => Error::generic(format!("commit v={version}: {other}")),
    }
}

impl Iterator for CommitActionsIterator {
    type Item = DeltaResult<CommitAction>;

    fn next(&mut self) -> Option<Self::Item> {
        let log_path = self.log_path_iter.next()?;
        Some(self.try_advance(log_path))
    }
}

/// Build the nullable [`StructField`] that represents this action kind in the read schema.
fn action_to_field(action: &DeltaAction) -> StructField {
    match action {
        DeltaAction::Add => ADD_FIELD.clone(),
        DeltaAction::Remove => REMOVE_FIELD.clone(),
        DeltaAction::Metadata => METADATA_FIELD.clone(),
        DeltaAction::Protocol => PROTOCOL_FIELD.clone(),
        DeltaAction::CommitInfo => COMMIT_INFO_FIELD.clone(),
        DeltaAction::Cdc => CDC_FIELD.clone(),
        DeltaAction::DomainMetadata => DOMAIN_METADATA_FIELD.clone(),
        DeltaAction::SetTxn => SET_TRANSACTION_FIELD.clone(),
        DeltaAction::CheckpointMetadata => CHECKPOINT_METADATA_FIELD.clone(),
        DeltaAction::Sidecar => SIDECAR_FIELD.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use test_utils::add_commit;

    use super::*;
    use crate::actions::visitors::{AddVisitor, RemoveVisitor};
    use crate::actions::{Add, Remove};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::RowVisitor;
    use crate::object_store::memory::InMemory;
    use crate::Snapshot;

    /// Open a `CommitRange` over `table-with-dv-small` with a matching anchor snapshot.
    /// `start_version` drives both the range's start version and the snapshot's version.
    fn open_test_range_at(start_version: Version) -> (CommitRange, Arc<dyn Engine>, SnapshotRef) {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let range = CommitRange::builder_for(table_root.as_str(), start_version)
            .with_end_version(1)
            .build(engine.as_ref())
            .unwrap();
        let anchor_snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(start_version)
            .build(engine.as_ref())
            .unwrap();
        (range, engine, anchor_snapshot)
    }

    #[test]
    fn test_commits_yields_one_per_commit_in_range() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let files = &range.commit_files;
        assert_eq!(
            files.len(),
            2,
            "table-with-dv-small has 2 commits (v=0..=1)"
        );

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let collected = range
            .commits(engine, Some(anchor_snapshot), &actions)
            .unwrap()
            .collect::<DeltaResult<Vec<_>>>()
            .unwrap();

        assert_eq!(collected.len(), 2, "yield one CommitAction per commit");
        for (i, ca) in collected.iter().enumerate() {
            assert_eq!(ca.version(), i as u64, "commit {i} version");
            assert_eq!(
                ca.timestamp(),
                files[i].location.last_modified,
                "commit {i} timestamp must match file last_modified",
            );
        }
    }

    #[test]
    fn test_commits_actions_project_to_requested_schema() {
        // v=1 of table-with-dv-small contains commitInfo + remove + add (DV rewrite).
        // The remove and add reference the same physical file path.
        let (range, engine, anchor_snapshot) = open_test_range_at(1);

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine.clone(), Some(anchor_snapshot), &actions)
            .unwrap();
        let commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(commit.version(), 1);
        assert!(iter.next().is_none(), "single-commit range yields only v=1");

        let mut add_visitor = AddVisitor::default();
        let mut remove_visitor = RemoveVisitor::default();
        for batch_res in commit.get_actions(engine.as_ref()).unwrap() {
            let batch = batch_res.unwrap();
            add_visitor.visit_rows_of(batch.as_ref()).unwrap();
            remove_visitor.visit_rows_of(batch.as_ref()).unwrap();
        }
        assert_eq!(add_visitor.adds.len(), 1, "v=1 has exactly one add");
        assert_eq!(
            remove_visitor.removes.len(),
            1,
            "v=1 has exactly one remove"
        );
    }

    /// Build an in-memory engine pre-loaded with `commits` (each `(version, body)` pair becomes
    /// a commit JSON file) and return `(engine, table_root)`. Tests that need a forged commit
    /// log use this instead of touching the filesystem.
    async fn engine_with_commits(commits: &[(u64, &str)]) -> (Arc<dyn Engine>, &'static str) {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
        for (version, body) in commits {
            add_commit(table_root, store.as_ref(), *version, body.to_string())
                .await
                .unwrap();
        }
        (engine, table_root)
    }

    /// Drain every batch of every commit yielded by `range.commits(...)`, returning the
    /// first error encountered. Used by the protocol-validation tests below.
    fn drain_commits(
        range: &CommitRange,
        engine: Arc<dyn Engine>,
        start_snapshot: Option<SnapshotRef>,
        actions: &[DeltaAction],
    ) -> DeltaResult<()> {
        for commit_res in range.commits(engine.clone(), start_snapshot, actions)? {
            let commit = commit_res?;
            for batch_res in commit.get_actions(engine.as_ref())? {
                batch_res?;
            }
        }
        Ok(())
    }

    const VALID_PROTOCOL_LINE: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#;
    const UNSUPPORTED_PROTOCOL_LINE: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#;
    const VALID_METADATA_LINE: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1000}}"#;
    /// Like [`VALID_METADATA_LINE`] but with a non-empty `configuration` (`foo=bar`), used to
    /// forge a metadata-only change in a later commit.
    const METADATA_CONFIG_CHANGE_LINE: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"foo":"bar"},"createdTime":2000}}"#;

    #[rstest::rstest]
    #[case::too_high_reader_version(
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#,
        |err: &Error| matches!(err, Error::Unsupported(_)),
    )]
    #[case::too_low_reader_version(
        r#"{"protocol":{"minReaderVersion":0,"minWriterVersion":1}}"#,
        |err: &Error| matches!(err, Error::InvalidProtocol(_)),
    )]
    #[tokio::test]
    async fn test_commits_errors_on_unsupported_reader_version(
        #[case] v1: &str,
        #[case] is_expected_err: fn(&Error) -> bool,
    ) {
        let v0 = format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE);
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .build(engine.as_ref())
            .expect("build should succeed; validation runs during iteration");

        let anchor_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let err = drain_commits(&range, engine, Some(anchor_snapshot), &actions)
            .expect_err("v=1 reader version must be rejected");
        assert!(is_expected_err(&err), "unexpected error variant: {err:?}");
    }

    #[rstest::rstest]
    #[case::ascending_rejects_unsupported_feature(
        CommitOrdering::AscendingOrder,
        format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE),
        UNSUPPORTED_PROTOCOL_LINE.to_string(),
        0,
        Some("futureFeature"),
    )]
    #[case::descending_rejects_unsupported_feature_in_earlier_commit(
        CommitOrdering::DescendingOrder,
        format!(
            "{}\n{}",
            UNSUPPORTED_PROTOCOL_LINE,
            VALID_METADATA_LINE,
        ),
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        1,
        Some("futureFeature"),
    )]
    #[case::ascending_metadata_change(
        CommitOrdering::AscendingOrder,
        format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE),
        METADATA_CONFIG_CHANGE_LINE.to_string(),
        0,
        None,
    )]
    #[tokio::test]
    async fn test_commits_validation_governed_by_ordering(
        #[case] ordering: CommitOrdering,
        #[case] v0: String,
        #[case] v1: String,
        #[case] snapshot_version: Version,
        #[case] expected_err_substring: Option<&str>,
    ) {
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, &v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .with_ordering(ordering)
            .build(engine.as_ref())
            .expect("build should succeed");

        let snapshot = Snapshot::builder_for(table_root)
            .at_version(snapshot_version)
            .build(engine.as_ref())
            .expect("snapshot at the ordering's anchor must build");

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let result = drain_commits(&range, engine, Some(snapshot), &actions);
        match expected_err_substring {
            Some(needle) => {
                let err = result.expect_err("expected per-commit validation to reject");
                let msg = format!("{err}");
                assert!(msg.contains(needle), "expected {needle:?} in error: {msg}");
            }
            None => {
                result.expect("per-commit validation must drain cleanly");
            }
        }
    }

    #[rstest::rstest]
    #[case::ascending_with_snapshot_at_end(CommitOrdering::AscendingOrder, 1, "start_version")]
    #[case::descending_with_snapshot_at_start(CommitOrdering::DescendingOrder, 0, "end_version")]
    fn test_commits_errors_on_anchor_snapshot_mismatched(
        #[case] ordering: CommitOrdering,
        #[case] snapshot_version: Version,
        #[case] expected_anchor_name: &str,
    ) {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .with_ordering(ordering)
            .build(engine.as_ref())
            .unwrap();
        let bad_snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(snapshot_version)
            .build(engine.as_ref())
            .unwrap();

        let err = range
            .commits(engine, Some(bad_snapshot), &[DeltaAction::Add])
            .err()
            .unwrap();
        let msg = format!("{err}");
        assert!(msg.contains(expected_anchor_name));
    }

    #[test]
    fn test_commits_errors_on_empty_actions() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let err = range
            .commits(engine, Some(anchor_snapshot), &[])
            .err()
            .unwrap();
        let msg = format!("{err}");
        assert!(msg.contains("DeltaAction"), "got: {msg}");
    }

    #[rstest::rstest]
    #[case::caller_requested_add_remove(
        &[DeltaAction::Add, DeltaAction::Remove],
        ["add", "remove"].into_iter().map(String::from).collect::<Vec<_>>(),
    )]
    #[case::caller_requested_with_protocol(
        &[DeltaAction::Add, DeltaAction::Remove, DeltaAction::Protocol],
        ["add", "remove", "protocol"].into_iter().map(String::from).collect::<Vec<_>>(),
    )]
    fn test_commits_emitted_schema_matches_caller_actions(
        #[case] actions: &[DeltaAction],
        #[case] expected_columns: Vec<String>,
    ) {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let mut saw_batch = false;
        for commit in range
            .commits(engine.clone(), Some(anchor_snapshot), actions)
            .unwrap()
        {
            let commit = commit.unwrap();
            for batch in commit.get_actions(engine.as_ref()).unwrap() {
                let batch = batch.unwrap();
                let arrow = batch
                    .any_ref()
                    .downcast_ref::<ArrowEngineData>()
                    .expect("default engine returns ArrowEngineData");
                let names = arrow
                    .record_batch()
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<_>>();
                assert_eq!(
                    names, expected_columns,
                    "emitted schema must match caller actions"
                );
                saw_batch = true;
            }
        }
        assert!(saw_batch, "expected at least one emitted batch");
    }

    // Collect the `Add` and `Remove` actions from `commit.get_actions()`.
    fn collect_adds_and_removes(
        commit: &CommitAction,
        engine: &dyn Engine,
    ) -> DeltaResult<(Vec<Add>, Vec<Remove>)> {
        let mut add_visitor = AddVisitor::default();
        let mut remove_visitor = RemoveVisitor::default();
        for batch_res in commit.get_actions(engine)? {
            let batch = batch_res?;
            add_visitor.visit_rows_of(batch.as_ref())?;
            remove_visitor.visit_rows_of(batch.as_ref())?;
        }
        Ok((add_visitor.adds, remove_visitor.removes))
    }

    #[test]
    fn test_get_actions_returns_same_content_each_call() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine.clone(), Some(anchor_snapshot), &actions)
            .unwrap();
        let commit = iter.next().expect("v=0 commit").unwrap();

        let first = collect_adds_and_removes(&commit, engine.as_ref()).unwrap();
        let second = collect_adds_and_removes(&commit, engine.as_ref()).unwrap();
        assert!(!first.0.is_empty(), "v=0 should yield at least one add");
        assert_eq!(
            first, second,
            "get_actions must yield identical content across calls",
        );
    }

    #[tokio::test]
    async fn test_error_on_start_snapshot_unsupported_for_scan() {
        let v0 = format!("{}\n{}", UNSUPPORTED_PROTOCOL_LINE, VALID_METADATA_LINE,);
        let (engine, table_root) = engine_with_commits(&[(0, &v0)]).await;

        let snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .expect("snapshot with unknown feature should still build");

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add];
        let err = range
            .commits(engine, Some(snapshot), &actions)
            .err()
            .expect(
                "commits must reject snapshot with unsupported feature before iteration begins",
            );
        match err {
            Error::Unsupported(msg) => assert!(msg.contains("futureFeature"), "got: {msg}"),
            other => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    #[rstest::rstest]
    #[case::validates_first_commit_pm(
        vec![
            (0u64, format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE)),
            (1, METADATA_CONFIG_CHANGE_LINE.to_string()),
        ],
        &[DeltaAction::Add, DeltaAction::Remove],
        false,
    )]
    #[case::rejects_unsupported_protocol(
        vec![(0u64, r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#.to_string())],
        &[DeltaAction::Add],
        true,
    )]
    #[case::skip_when_neither_pm(
        vec![(0u64, r#"{"commitInfo":{"timestamp":1000,"operation":"WRITE"}}"#.to_string())],
        &[DeltaAction::CommitInfo],
        false,
    )]
    #[tokio::test]
    async fn test_protocol_validation_is_commit_driven(
        #[case] commits: Vec<(u64, String)>,
        #[case] actions: &[DeltaAction],
        #[case] expects_unsupported: bool,
    ) {
        let commit_refs: Vec<(u64, &str)> = commits
            .iter()
            .map(|(v, body)| (*v, body.as_str()))
            .collect();
        let end_version = commit_refs.last().expect("at least one commit").0;
        let (engine, table_root) = engine_with_commits(&commit_refs).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(end_version)
            .build(engine.as_ref())
            .unwrap();

        let result = drain_commits(&range, engine, None, actions);
        if expects_unsupported {
            let err = result.expect_err("commit-driven validation must reject");
            assert!(
                matches!(err, Error::Unsupported(_)),
                "expected Error::Unsupported, got: {err:?}",
            );
        } else {
            result.expect("snapshot-less range must drain cleanly");
        }
    }

    #[tokio::test]
    async fn test_commits_descending_validates_each_commit_independently() {
        let v0 = format!("{}\n{}", UNSUPPORTED_PROTOCOL_LINE, VALID_METADATA_LINE,);
        let v1 = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .with_ordering(CommitOrdering::DescendingOrder)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range.commits(engine, None, &actions).unwrap();

        let v1_commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(v1_commit.version(), 1);

        let v0_result = iter.next().expect("v=0 commit yield slot");
        match v0_result {
            Ok(_) => panic!("v=0 must reject during iter.next()"),
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("futureFeature"), "got: {msg}")
            }
            Err(other) => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    const ICT_PROTOCOL_LINE: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["inCommitTimestamp"]}}"#;
    const ICT_METADATA_ENABLED: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true"},"createdTime":1000}}"#;
    const COMMIT_INFO_NO_ICT: &str = r#"{"commitInfo":{"timestamp":0,"operation":"WRITE"}}"#;

    fn ict_metadata_enabled_at(enablement_version: Version, enablement_ts: i64) -> String {
        format!(
            r#"{{"metaData":{{"id":"00000000-0000-0000-0000-000000000000","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[]}}","partitionColumns":[],"configuration":{{"delta.enableInCommitTimestamps":"true","delta.inCommitTimestampEnablementVersion":"{enablement_version}","delta.inCommitTimestampEnablementTimestamp":"{enablement_ts}"}},"createdTime":2000}}}}"#
        )
    }

    fn commit_info_with_ict(ts: i64) -> String {
        format!(
            r#"{{"commitInfo":{{"inCommitTimestamp":{ts},"timestamp":0,"operation":"WRITE"}}}}"#
        )
    }

    #[derive(Clone, Copy)]
    enum ExpectedTs {
        /// Expect exactly this in-commit timestamp.
        Ict(i64),
        /// Expect the commit file's `last_modified`.
        FileModified,
    }

    #[rstest::rstest]
    #[case::ict_never_enabled(
        vec![
            (0u64, format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE)),
            (1, COMMIT_INFO_NO_ICT.to_string()),
        ],
        Some(0),
        CommitOrdering::AscendingOrder,
        vec![(0, ExpectedTs::FileModified), (1, ExpectedTs::FileModified)],
    )]
    #[case::ict_enabled_from_creation(
        vec![(0u64, format!("{}\n{}\n{}", commit_info_with_ict(555_000), ICT_PROTOCOL_LINE, ICT_METADATA_ENABLED))],
        Some(0),
        CommitOrdering::AscendingOrder,
        vec![(0, ExpectedTs::Ict(555_000))],
    )]
    #[case::ict_enabled_mid_range(
        vec![
            (0u64, format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE)),
            (1, format!("{}\n{}\n{}", commit_info_with_ict(700_000), ICT_PROTOCOL_LINE, ict_metadata_enabled_at(1, 700_000))),
            (2, commit_info_with_ict(800_000)),
        ],
        Some(0),
        CommitOrdering::AscendingOrder,
        vec![(0, ExpectedTs::FileModified), (1, ExpectedTs::Ict(700_000)), (2, ExpectedTs::Ict(800_000))],
    )]
    #[case::ict_enablement_check(
        vec![
            (0u64, format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE)),
            (1, COMMIT_INFO_NO_ICT.to_string()),
            (2, format!("{}\n{}\n{}", commit_info_with_ict(800_000), ICT_PROTOCOL_LINE, ict_metadata_enabled_at(2, 800_000))),
        ],
        Some(2),
        CommitOrdering::DescendingOrder,
        vec![(0, ExpectedTs::FileModified), (1, ExpectedTs::FileModified), (2, ExpectedTs::Ict(800_000))],
    )]
    #[case::best_effort_present_ict(
        vec![
            (0u64, commit_info_with_ict(111_000)),
            (1, commit_info_with_ict(222_000)),
        ],
        None,
        CommitOrdering::AscendingOrder,
        vec![(0, ExpectedTs::Ict(111_000)), (1, ExpectedTs::Ict(222_000))],
    )]
    #[case::best_effort_absent_ict(
        vec![(0u64, COMMIT_INFO_NO_ICT.to_string())],
        None,
        CommitOrdering::AscendingOrder,
        vec![(0, ExpectedTs::FileModified)],
    )]
    #[tokio::test]
    async fn test_timestamp_resolution(
        #[case] commits: Vec<(u64, String)>,
        #[case] snapshot_version: Option<Version>,
        #[case] ordering: CommitOrdering,
        #[case] expected: Vec<(Version, ExpectedTs)>,
    ) {
        let commit_refs: Vec<(u64, &str)> = commits
            .iter()
            .map(|(v, body)| (*v, body.as_str()))
            .collect();
        let end_version = commit_refs.iter().map(|(v, _)| *v).max().unwrap();
        let (engine, table_root) = engine_with_commits(&commit_refs).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(end_version)
            .with_ordering(ordering)
            .build(engine.as_ref())
            .unwrap();

        let mtimes: HashMap<Version, i64> = range
            .commit_files
            .iter()
            .map(|f| (f.version, f.location.last_modified))
            .collect();

        let snapshot = snapshot_version.map(|v| {
            Snapshot::builder_for(table_root)
                .at_version(v)
                .build(engine.as_ref())
                .unwrap()
        });

        let expected: HashMap<Version, ExpectedTs> = expected.into_iter().collect();
        let actions = [DeltaAction::Add, DeltaAction::Remove];
        for commit in range.commits(engine, snapshot, &actions).unwrap() {
            let commit = commit.unwrap();
            let version = commit.version();
            let want = match expected[&version] {
                ExpectedTs::Ict(ts) => ts,
                ExpectedTs::FileModified => mtimes[&version],
            };
            assert_eq!(commit.timestamp(), want, "timestamp for v={version}");
        }
    }

    #[tokio::test]
    async fn test_timestamp_errors_when_enabled_but_ict_missing() {
        // ICT enabled from creation, but the commit omits the mandatory inCommitTimestamp.
        let v0 = format!("{}\n{}", ICT_PROTOCOL_LINE, ICT_METADATA_ENABLED);
        let (engine, table_root) = engine_with_commits(&[(0, &v0)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(0)
            .build(engine.as_ref())
            .unwrap();
        let snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let err = drain_commits(&range, engine, Some(snapshot), &actions)
            .expect_err("missing in-commit timestamp must error");
        let msg = format!("{err}");
        assert!(msg.contains("in-commit timestamp"), "got: {msg}");
    }
}
