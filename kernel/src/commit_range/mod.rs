//! Read a contiguous range of raw Delta commits.
//!
//! A [`CommitRange`] holds an inclusive `[start_version, end_version]` range and the list of
//! commit files; reads are lazy (no JSON I/O until [`CommitRange::commits`] or
//! [`CommitRange::actions`] is called).
//!
//! Two construction paths, differing only in how `_delta_log/` is listed at build time:
//! - [`CommitRange::builder_for`]: lists `_delta_log/` for the requested range.
//! - [`CommitRange::builder_from`]: reuses an existing snapshot's `LogSegment`, avoiding the
//!   listing.
//!
//! Both [`CommitRange::commits`] and [`CommitRange::actions`] take a list of [`DeltaAction`]
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
//! use delta_kernel::engine::default::DefaultEngineBuilder;
//! use delta_kernel::object_store::local::LocalFileSystem;
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
//!     for batch in commit.get_actions()? {
//!         let _batch = batch?;
//!     }
//! }
//! # Ok::<(), Error>(())
//! ```

mod actions;
mod builder;

use std::sync::{Arc, LazyLock};

pub use actions::{CommitAction, DeltaAction};
pub use builder::{CommitOrdering, CommitRangeBuilder};
use url::Url;

use crate::actions::{
    Add, Cdc, CommitInfo, Metadata, Protocol, Remove, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::expressions::Expression;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::SnapshotRef;
use crate::table_features::Operation;
use crate::{DeltaResult, Engine, EngineData, Error, Version};

/// Output column name for the per-row commit version emitted by [`CommitRange::actions`].
const COMMIT_VERSION_COL: &str = "version";

/// Output column name for the per-row commit timestamp emitted by [`CommitRange::actions`].
const COMMIT_TIMESTAMP_COL: &str = "timestamp";

/// Per-row metadata columns prepended by [`CommitRange::actions`].
static METADATA_FIELDS: LazyLock<[StructField; 2]> = LazyLock::new(|| {
    [
        StructField::not_null(COMMIT_VERSION_COL, DataType::LONG),
        StructField::not_null(COMMIT_TIMESTAMP_COL, DataType::LONG),
    ]
});

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

    /// Iterator over commits in the range. Each yielded [`CommitAction`] exposes the
    /// commit's version, timestamp (currently the file's `last_modified`), its effective
    /// `Protocol` / `Metadata`, and a re-buildable iterator over the commit's action
    /// batches projected to `actions`.
    ///
    /// `actions` drives the user-facing read schema literally: the engine projects each
    /// commit JSON to a struct with one nullable field per requested [`DeltaAction`].
    /// Callers should pass distinct kinds; duplicates would produce duplicate read-schema
    /// fields. The iterator separately reads `[protocol, metadata]` once per commit for
    /// validation; that extra read is not visible to the caller.
    ///
    /// I/O cost: one `[protocol, metadata]` read per commit yielded by the iterator,
    /// plus one additional read per `CommitAction::get_actions()` call by the caller.
    ///
    /// `start_snapshot` is optional:
    /// - When `Some`, its table configuration seeds the iterator's accumulated `latest_protocol` /
    ///   `latest_metadata`. Its version must equal `self.start_version()` for
    ///   [`CommitOrdering::AscendingOrder`] or `self.end_version()` for
    ///   [`CommitOrdering::DescendingOrder`].
    /// - When `None`, the iterator starts with no seeded state; validation is purely commit-driven
    ///   and is `Ok` until the iterator encounters a commit whose `Protocol` (with optional
    ///   `Metadata`) is unsupported.
    ///
    /// In [`CommitOrdering::AscendingOrder`], the iterator accumulates `latest_*` across
    /// commits so each commit is validated against the cumulative state. In
    /// [`CommitOrdering::DescendingOrder`], `latest_*` are not carried across commits
    /// (the cumulative state cannot evolve backwards); each commit is validated against
    /// only the `Protocol` / `Metadata` it carries.
    ///
    /// Validation errors surface from `iter.next()`; the iterator stops on the first
    /// unsupported commit.
    ///
    /// Returns `Err` eagerly if:
    /// - `actions` is empty,
    /// - `start_snapshot` is supplied but its version does not match the expected anchor, or
    /// - `start_snapshot` is supplied but its table configuration does not support scanning.
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

        let read_schema = build_read_schema(actions.iter().copied());

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

    /// Yield action batches across all commits as [`crate::EngineData`], with
    /// `version` and `timestamp` columns prepended to each batch's struct schema.
    ///
    /// Built on top of [`Self::commits`]: every batch is transformed via the engine's
    /// `EvaluationHandler` to inject the commit's version and timestamp as literal
    /// columns. The original action columns (`add`, `remove`, ...) are passed through
    /// unchanged in the order driven by `actions`.
    ///
    /// Returns `Err` eagerly under the same conditions as [`Self::commits`]. Per-commit
    /// validation errors and per-batch errors surface during iteration.
    pub fn actions(
        &self,
        engine: Arc<dyn Engine>,
        start_snapshot: Option<SnapshotRef>,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
        let action_kinds = actions.to_vec();
        let input_schema = build_read_schema(action_kinds.iter().copied());
        let output_schema = actions_output_schema(&action_kinds);
        let evaluation_handler = engine.evaluation_handler();
        let commits_iter = self.commits(engine, start_snapshot, actions)?;

        Ok(commits_iter.flat_map(
            move |commit_res| -> Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
                let commit = match commit_res {
                    Ok(c) => c,
                    Err(e) => return Box::new(std::iter::once(Err(e))),
                };
                let version_i64 = match i64::try_from(commit.version()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Box::new(std::iter::once(Err(Error::generic(format!(
                            "commit version {} overflows i64",
                            commit.version()
                        )))));
                    }
                };
                let expr =
                    actions_transform_expression(&action_kinds, version_i64, commit.timestamp());
                let evaluator = match evaluation_handler.new_expression_evaluator(
                    input_schema.clone(),
                    Arc::new(expr),
                    output_schema.clone().into(),
                ) {
                    Ok(e) => e,
                    Err(e) => return Box::new(std::iter::once(Err(e))),
                };
                let batches = match commit.get_actions() {
                    Ok(it) => it,
                    Err(e) => return Box::new(std::iter::once(Err(e))),
                };
                Box::new(batches.map(move |batch_res| {
                    batch_res.and_then(|batch| evaluator.evaluate(batch.as_ref()))
                }))
            },
        ))
    }
}

/// Iterator yielded by [`CommitRange::commits`]. Holds the iterator's accumulated
/// `latest_protocol` / `latest_metadata` (only updated under ascending ordering) and
/// constructs a fresh [`CommitAction`] for each commit, running per-commit protocol
/// validation before yielding.
pub(crate) struct CommitActionsIterator {
    engine: Arc<dyn Engine>,
    table_root: Url,
    log_path_iter: std::vec::IntoIter<ParsedLogPath>, // impl IntoIter<ParsedLogPath>
    commit_ordering: CommitOrdering,
    read_schema: SchemaRef,
    latest_protocol: Option<Protocol>,
    latest_metadata: Option<Metadata>,
}

impl CommitActionsIterator {
    /// Build a `CommitAction` for `log_path`, extract and validate its `(Protocol, Metadata)`,
    /// and (under ascending ordering) advance the iterator's accumulated state.
    fn try_advance(&mut self, log_path: ParsedLogPath) -> DeltaResult<CommitAction> {
        let version = log_path.version;
        let mut commit_action = CommitAction::new(
            self.engine.clone(),
            self.table_root.clone(),
            log_path,
            self.read_schema.clone(),
            self.latest_protocol.clone(),
            self.latest_metadata.clone(),
        );
        let (extracted_p, extracted_m) = commit_action
            .get_protocol_and_metadata()
            .map_err(|e| with_version_context(version, e))?;
        commit_action
            .protocol_validation()
            .map_err(|e| with_version_context(version, e))?;

        if self.commit_ordering == CommitOrdering::AscendingOrder {
            if let Some(p) = extracted_p {
                self.latest_protocol = Some(p);
            }
            if let Some(m) = extracted_m {
                self.latest_metadata = Some(m);
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

/// Map a [`DeltaAction`] to the JSON column name used in the Delta log.
fn action_column_name(action: DeltaAction) -> &'static str {
    match action {
        DeltaAction::Add => ADD_NAME,
        DeltaAction::Remove => REMOVE_NAME,
        DeltaAction::Metadata => METADATA_NAME,
        DeltaAction::Protocol => PROTOCOL_NAME,
        DeltaAction::CommitInfo => COMMIT_INFO_NAME,
        DeltaAction::Cdc => CDC_NAME,
    }
}

/// Build the nullable [`StructField`] that represents this action kind in the read schema.
fn action_to_field(action: DeltaAction) -> StructField {
    match action {
        DeltaAction::Add => StructField::nullable(ADD_NAME, Add::to_schema()),
        DeltaAction::Remove => StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        DeltaAction::Metadata => StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        DeltaAction::Protocol => StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        DeltaAction::CommitInfo => StructField::nullable(COMMIT_INFO_NAME, CommitInfo::to_schema()),
        DeltaAction::Cdc => StructField::nullable(CDC_NAME, Cdc::to_schema()),
    }
}

/// Build the read schema for a commit JSON: a top-level struct with one nullable field per
/// requested action kind. Each row of the resulting batch corresponds to one JSON line in
/// the commit file; at most one column is non-null per row.
fn build_read_schema(actions: impl IntoIterator<Item = DeltaAction>) -> SchemaRef {
    Arc::new(StructType::new_unchecked(
        actions.into_iter().map(action_to_field),
    ))
}

/// Output schema produced by [`CommitRange::actions`]:
/// `struct<version: long, timestamp: long, ...input_fields>`.
fn actions_output_schema(actions: &[DeltaAction]) -> SchemaRef {
    let action_fields = actions.iter().copied().map(action_to_field);
    Arc::new(StructType::new_unchecked(
        METADATA_FIELDS.iter().cloned().chain(action_fields),
    ))
}

/// Per-commit transformation expression: emits a struct of
/// `[literal(version), literal(timestamp), col(action_1), col(action_2), ...]`.
fn actions_transform_expression(
    actions: &[DeltaAction],
    commit_version: i64,
    commit_timestamp: i64,
) -> Expression {
    let metadata = [
        Expression::literal(commit_version),
        Expression::literal(commit_timestamp),
    ];
    let action_columns = actions
        .iter()
        .copied()
        .map(|a| Expression::column([action_column_name(a)]));
    Expression::struct_from(metadata.into_iter().chain(action_columns))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, LazyLock};

    use test_utils::add_commit;

    use super::*;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::DefaultEngineBuilder;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
    use crate::object_store::memory::InMemory;
    use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes};
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
    fn commits_yields_one_per_commit_in_range() {
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

    /// Visitor capturing `(add.path, remove.path)` per row.
    #[derive(Default)]
    struct AddRemovePathVisitor {
        rows: Vec<(Option<String>, Option<String>)>,
    }

    impl RowVisitor for AddRemovePathVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static COLS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                (
                    vec![column_name!("add.path"), column_name!("remove.path")],
                    vec![DataType::STRING, DataType::STRING],
                )
                    .into()
            });
            COLS.as_ref()
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                let add_path: Option<String> = getters[0].get_opt(i, "add.path")?;
                let remove_path: Option<String> = getters[1].get_opt(i, "remove.path")?;
                self.rows.push((add_path, remove_path));
            }
            Ok(())
        }
    }

    #[test]
    fn commits_actions_project_to_requested_schema() {
        // v=1 of table-with-dv-small contains commitInfo + remove + add (DV rewrite).
        // The remove and add reference the same physical file path.
        let (range, engine, anchor_snapshot) = open_test_range_at(1);

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine, Some(anchor_snapshot), &actions)
            .unwrap();
        let commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(commit.version(), 1);
        assert!(iter.next().is_none(), "single-commit range yields only v=1");

        let mut visitor = AddRemovePathVisitor::default();
        for batch_res in commit.get_actions().unwrap() {
            visitor.visit_rows_of(batch_res.unwrap().as_ref()).unwrap();
        }

        let adds = visitor
            .rows
            .iter()
            .filter_map(|(a, _)| a.as_deref())
            .collect::<Vec<_>>();
        let removes = visitor
            .rows
            .iter()
            .filter_map(|(_, r)| r.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(adds.len(), 1, "v=1 has exactly one add");
        assert_eq!(removes.len(), 1, "v=1 has exactly one remove");
        // DV rewrite: the same physical file path appears on both sides.
        assert_eq!(adds[0], removes[0], "DV rewrite shares the file path");
    }

    /// Visitor capturing `(version, timestamp, add.path, remove.path)` per row.
    #[derive(Default)]
    struct ActionsTaggedVisitor {
        rows: Vec<(i64, i64, Option<String>, Option<String>)>,
    }

    impl RowVisitor for ActionsTaggedVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static COLS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                (
                    vec![
                        column_name!("version"),
                        column_name!("timestamp"),
                        column_name!("add.path"),
                        column_name!("remove.path"),
                    ],
                    vec![
                        DataType::LONG,
                        DataType::LONG,
                        DataType::STRING,
                        DataType::STRING,
                    ],
                )
                    .into()
            });
            COLS.as_ref()
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                let version: i64 = getters[0].get(i, "version")?;
                let timestamp: i64 = getters[1].get(i, "timestamp")?;
                let add_path: Option<String> = getters[2].get_opt(i, "add.path")?;
                let remove_path: Option<String> = getters[3].get_opt(i, "remove.path")?;
                self.rows.push((version, timestamp, add_path, remove_path));
            }
            Ok(())
        }
    }

    /// Build an in-memory engine pre-loaded with `commits` (each `(version, body)` pair becomes
    /// a commit JSON file) and return `(engine, table_root)`. Tests that need a forged commit
    /// log use this instead of touching the filesystem.
    async fn engine_with_commits(commits: &[(u64, &str)]) -> (Arc<dyn Engine>, &'static str) {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine: Arc<dyn Engine> = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
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
        for commit_res in range.commits(engine, start_snapshot, actions)? {
            let commit = commit_res?;
            for batch_res in commit.get_actions()? {
                batch_res?;
            }
        }
        Ok(())
    }

    /// Assert that draining the range surfaces an `Error::Unsupported` whose message
    /// contains `expected_substring`.
    fn assert_unsupported_with_substring(
        range: &CommitRange,
        engine: Arc<dyn Engine>,
        start_snapshot: Option<SnapshotRef>,
        actions: &[DeltaAction],
        expected_substring: &str,
    ) {
        let err = drain_commits(range, engine, start_snapshot, actions)
            .expect_err("validation must reject");
        match &err {
            Error::Unsupported(msg) => assert!(
                msg.contains(expected_substring),
                "expected message to contain {expected_substring:?}, got: {msg}",
            ),
            other => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    const VALID_PROTOCOL_LINE: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#;
    const VALID_METADATA_LINE: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1000}}"#;

    #[tokio::test]
    async fn commits_errors_on_too_high_reader_version() {
        let v0 = format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE);
        let v1 = r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#;
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
        assert_unsupported_with_substring(&range, engine, Some(anchor_snapshot), &actions, "99");
    }

    #[tokio::test]
    async fn commits_errors_on_too_low_reader_version() {
        let v0 = format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE);
        let v1 = r#"{"protocol":{"minReaderVersion":0,"minWriterVersion":1}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .build(engine.as_ref())
            .expect("build should succeed");

        let anchor_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let err = drain_commits(&range, engine, Some(anchor_snapshot), &actions)
            .expect_err("v=1 reader version must be rejected");
        match &err {
            Error::InvalidProtocol(msg) => assert!(msg.contains('0'), "got: {msg}"),
            other => panic!("expected Error::InvalidProtocol, got: {other:?}"),
        }
    }

    /// Per-commit validation runs in both orderings:
    /// - ascending: `latest_*` accumulate; each commit is validated against the cumulative
    ///   `(Protocol, Metadata)` state.
    /// - descending: `latest_*` are not carried across commits; each commit is validated against
    ///   the seed (from `start_snapshot`) overlaid with its own `Protocol` / `Metadata`.
    ///
    /// Each case forges a 2-commit log and asserts whether the iterator surfaces an error
    /// (with `Some(needle)`) or drains cleanly (with `None`).
    #[rstest::rstest]
    #[case::ascending_rejects_unsupported_feature(
        CommitOrdering::AscendingOrder,
        format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE),
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#.to_string(),
        0,
        Some("futureFeature"),
    )]
    #[case::descending_rejects_unsupported_feature_in_earlier_commit(
        CommitOrdering::DescendingOrder,
        format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
            VALID_METADATA_LINE,
        ),
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        1,
        Some("futureFeature"),
    )]
    #[case::ascending_metadata_change(
        CommitOrdering::AscendingOrder,
        format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE),
        r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"foo":"bar"},"createdTime":2000}}"#.to_string(),
        0,
        None,
    )]
    #[tokio::test]
    async fn commits_validation_governed_by_ordering(
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
    fn commits_errors_on_anchor_snapshot_mismatched(
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
    fn commits_errors_on_empty_actions() {
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
        &["add", "remove"],
    )]
    #[case::caller_requested_with_protocol(
        &[DeltaAction::Add, DeltaAction::Remove, DeltaAction::Protocol],
        &["add", "remove", "protocol"],
    )]
    fn commits_emitted_schema_matches_caller_actions(
        #[case] actions: &[DeltaAction],
        #[case] expected_columns: &[&str],
    ) {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let mut saw_batch = false;
        for commit in range
            .commits(engine, Some(anchor_snapshot), actions)
            .unwrap()
        {
            let commit = commit.unwrap();
            for batch in commit.get_actions().unwrap() {
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
                let expected = expected_columns
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect::<Vec<_>>();
                assert_eq!(names, expected, "emitted schema must match caller actions");
                saw_batch = true;
            }
        }
        assert!(saw_batch, "expected at least one emitted batch");
    }

    #[tokio::test]
    async fn commits_iter_yields_ok_then_err_on_downstream_bad_protocol() {
        let v0 = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}"#,
            VALID_METADATA_LINE,
        );
        let v1 = r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .build(engine.as_ref())
            .expect("build should succeed");

        let anchor_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine, Some(anchor_snapshot), &actions)
            .unwrap();

        let v0_commit = iter.next().expect("v=0 commit").unwrap();
        let v0_batches: DeltaResult<Vec<_>> = v0_commit.get_actions().unwrap().collect();
        v0_batches.expect("v=0 must drain cleanly");

        let v1_result = iter.next().expect("v=1 commit yield slot");
        match v1_result {
            Ok(_) => panic!("v=1 must reject during iter.next()"),
            Err(Error::Unsupported(msg)) => assert!(msg.contains("99"), "got: {msg}"),
            Err(other) => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    #[test]
    fn actions_yields_metadata_columns_prepended() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let files = range.commit_files.clone();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut visitor = ActionsTaggedVisitor::default();
        for batch_res in range
            .actions(engine, Some(anchor_snapshot), &actions)
            .unwrap()
        {
            visitor.visit_rows_of(batch_res.unwrap().as_ref()).unwrap();
        }

        let mut add_versions = visitor
            .rows
            .iter()
            .filter_map(|r| r.2.as_ref().map(|_| r.0))
            .collect::<Vec<_>>();
        add_versions.sort();
        let remove_versions = visitor
            .rows
            .iter()
            .filter_map(|r| r.3.as_ref().map(|_| r.0))
            .collect::<Vec<_>>();
        assert_eq!(add_versions, vec![0, 1], "adds appear in v=0 and v=1");
        assert_eq!(remove_versions, vec![1], "remove appears only in v=1");

        // timestamp matches each commit file's last_modified.
        for (version, timestamp, _, _) in &visitor.rows {
            let v_ix = *version as usize;
            assert_eq!(
                *timestamp, files[v_ix].location.last_modified,
                "timestamp for commit {version} must match file last_modified",
            );
        }
    }

    /// Count the rows in a single batch iterator returned by `get_actions`.
    fn count_rows(it: crate::FileDataReadResultIterator) -> DeltaResult<usize> {
        let mut total = 0;
        for batch_res in it {
            total += batch_res?.len();
        }
        Ok(total)
    }

    #[test]
    fn get_actions_callable_multiple_times() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine, Some(anchor_snapshot), &actions)
            .unwrap();
        let commit = iter.next().expect("v=0 commit").unwrap();

        let first = count_rows(commit.get_actions().unwrap()).unwrap();
        let second = count_rows(commit.get_actions().unwrap()).unwrap();
        assert!(first > 0, "v=0 should have at least one row");
        assert_eq!(first, second, "get_actions must be idempotent");
    }

    /// Collect every (add.path, remove.path) row from `commit.get_actions()`.
    fn collect_add_remove_rows(
        commit: &CommitAction,
    ) -> DeltaResult<Vec<(Option<String>, Option<String>)>> {
        let mut visitor = AddRemovePathVisitor::default();
        for batch_res in commit.get_actions()? {
            visitor.visit_rows_of(batch_res?.as_ref())?;
        }
        Ok(visitor.rows)
    }

    #[test]
    fn get_actions_returns_same_content_each_call() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range
            .commits(engine, Some(anchor_snapshot), &actions)
            .unwrap();
        let commit = iter.next().expect("v=0 commit").unwrap();

        let first = collect_add_remove_rows(&commit).unwrap();
        let second = collect_add_remove_rows(&commit).unwrap();
        assert!(!first.is_empty(), "v=0 should yield at least one row");
        assert_eq!(
            first, second,
            "get_actions must yield identical content across calls",
        );
    }

    #[tokio::test]
    async fn actions_without_snapshot_yields_metadata_columns() {
        // v=0 carries valid P+M plus a single Add. With no snapshot, `actions()` must
        // still prepend `version` and `timestamp` columns to the emitted batches.
        let add_line = r#"{"add":{"path":"foo/bar.parquet","partitionValues":{},"size":100,"modificationTime":1000,"dataChange":true}}"#;
        let v0 = format!(
            "{}\n{}\n{}",
            VALID_PROTOCOL_LINE, VALID_METADATA_LINE, add_line,
        );
        let (engine, table_root) = engine_with_commits(&[(0, &v0)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add];
        let mut saw_metadata_columns = false;
        for batch_res in range.actions(engine, None, &actions).unwrap() {
            let batch = batch_res.unwrap();
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
                names,
                vec![
                    COMMIT_VERSION_COL.to_string(),
                    COMMIT_TIMESTAMP_COL.to_string(),
                    ADD_NAME.to_string(),
                ],
                "actions() must prepend version+timestamp to the requested actions",
            );
            saw_metadata_columns = true;
        }
        assert!(saw_metadata_columns, "expected at least one emitted batch");
    }

    #[tokio::test]
    async fn commits_rejects_snapshot_unsupported_for_scan() {
        // Forge v=0 with an Unknown reader feature ("futureFeature"). Building a Snapshot
        // succeeds because TableConfiguration::try_new does not enforce operation support;
        // `commits()` then eagerly fails because the snapshot's table_configuration does
        // not support Operation::Scan.
        let v0 = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
            VALID_METADATA_LINE,
        );
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
            .expect("commits must reject before iteration begins");
        match err {
            Error::Unsupported(msg) => assert!(msg.contains("futureFeature"), "got: {msg}"),
            other => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn commits_without_snapshot_validates_first_commit_pm() {
        // v=0 carries P+M; v=1 carries only a metadata config change. With no snapshot,
        // the iterator's latest_* start as None and become populated from v=0.
        let v0 = format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE);
        let v1 = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"foo":"bar"},"createdTime":2000}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        drain_commits(&range, engine, None, &actions).expect("snapshot-less range must drain");
    }

    #[tokio::test]
    async fn commits_without_snapshot_rejects_unsupported_protocol() {
        // v=0 carries only an unsupported Protocol (no metadata). With no snapshot,
        // protocol_validation falls through to the stateless `validate_protocol`
        // which rejects min_reader_version > MAX_VALID_READER_VERSION.
        let v0 = r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, v0)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add];
        assert_unsupported_with_substring(&range, engine, None, &actions, "99");
    }

    #[tokio::test]
    async fn commits_without_snapshot_skip_when_neither_pm() {
        // v=0 carries only a CommitInfo (no Protocol, no Metadata). With no snapshot,
        // the effective state stays None and validation is skipped.
        let v0 = r#"{"commitInfo":{"timestamp":1000,"operation":"WRITE"}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, v0)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(0)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::CommitInfo];
        drain_commits(&range, engine, None, &actions).expect("validation must be skipped");
    }

    #[tokio::test]
    async fn commits_descending_validates_each_commit_independently() {
        // Newest commit (v=1) has plain protocol; older commit (v=0) carries an
        // unsupported feature. Descending order visits v=1 first (yields Ok), then
        // v=0 (yields Err). Without a snapshot seed, each commit is validated only
        // against its own carried P/M.
        let v0 = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
            VALID_METADATA_LINE,
        );
        let v1 = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, 0)
            .with_end_version(1)
            .with_ordering(CommitOrdering::DescendingOrder)
            .build(engine.as_ref())
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range.commits(engine, None, &actions).unwrap();

        // v=1 yields first under descending ordering.
        let v1_commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(v1_commit.version(), 1);

        // v=0 yields next and errors on the unsupported feature.
        let v0_result = iter.next().expect("v=0 commit yield slot");
        match v0_result {
            Ok(_) => panic!("v=0 must reject during iter.next()"),
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("futureFeature"), "got: {msg}")
            }
            Err(other) => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }
}
