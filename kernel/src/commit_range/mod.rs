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
//! Both [`CommitRange::commits`] and [`CommitRange::actions`] take a list of [`DeltaAction`] that
//! will be filtered and a snapshot as a parameter whose version must match the range's
//! `start_version` (in ascending listing case) or `end_version` (in descending listing case).
//! See [`CommitRange::commits`] for protocol-validation details.
//!
//! # Example
//! ```no_run
//! use std::sync::Arc;
//! use delta_kernel::commit_range::{CommitRange, DeltaAction};
//! use delta_kernel::{Error, Snapshot};
//! use delta_kernel::engine::default::DefaultEngineBuilder;
//! use delta_kernel::object_store::local::LocalFileSystem;
//!
//! let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build();
//! let anchor_snapshot = Snapshot::builder_for("file:///data/T").at_version(0).build(&engine)?;
//! let range = CommitRange::builder_for("file:///data/T", 0)
//!     .with_end_version(4)
//!     .build(&engine)?;
//!
//! for commit in range.commits(&engine, anchor_snapshot, &[DeltaAction::Add, DeltaAction::Remove])? {
//!     let commit = commit?;
//!     println!("v={} ts={}", commit.version(), commit.timestamp());
//!     for batch in commit.into_actions() {
//!         let _batch = batch?;
//!     }
//! }
//! # Ok::<(), Error>(())
//! ```

mod actions;
mod builder;

use std::slice;
use std::sync::{Arc, LazyLock, Mutex};

pub use actions::{CommitActions, DeltaAction};
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
use crate::table_configuration::TableConfiguration;
use crate::table_features::Operation;
use crate::{DeltaResult, Engine, EngineData, Error, ExpressionEvaluator, JsonHandler, Version};

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

    /// Iterator over commits in the range. Each item is a [`CommitActions`] that exposes the
    /// commit's version, timestamp (currently the file's `last_modified`), and a lazy iterator
    /// over the commit's action batches projected to `actions`.
    ///
    /// `anchor_snapshot` must belong to the same table as this range and must be at:
    /// - `self.start_version()` for [`CommitOrdering::AscendingOrder`], or
    /// - `self.end_version()` for [`CommitOrdering::DescendingOrder`].
    ///
    /// `actions` drives the read schema literally: the engine projects each commit JSON to a
    /// struct with one nullable field per requested [`DeltaAction`]. Callers should pass
    /// distinct kinds; duplicates would produce duplicate read-schema fields.
    ///
    /// In ascending order, [`DeltaAction::Protocol`] and [`DeltaAction::Metadata`] are
    /// auto-injected into the read schema (when not already requested) and per-batch validation
    /// runs. The iterator yields `Err` and stops at the first unsupported protocol.
    /// Auto-injected columns are stripped before emission so the caller sees only the actions
    /// they asked for.
    ///
    /// In descending order, per-batch validation is skipped. The protocol validation is inherited
    /// from the `anchor_snapshot` 's protocol validation.
    ///
    /// Returns `Err` eagerly if:
    /// - `actions` is empty,
    /// - `anchor_snapshot`'s version does not match the expected version,
    /// - `anchor_snapshot`'s [`TableConfiguration`] does not support [`Operation::Scan`], or
    /// - the projection evaluator cannot be constructed.
    pub fn commits(
        &self,
        engine: &dyn Engine,
        anchor_snapshot: SnapshotRef,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<CommitActions>> + Send> {
        if actions.is_empty() {
            return Err(Error::generic("at least one DeltaAction must be requested"));
        }

        let (anchor_version, anchor_name) = match self.commit_ordering {
            CommitOrdering::AscendingOrder => (self.start_version, "start_version"),
            CommitOrdering::DescendingOrder => (self.end_version, "end_version"),
        };
        if anchor_snapshot.version() != anchor_version {
            return Err(Error::generic(format!(
                "snapshot version {} does not match {anchor_name} ({anchor_version})",
                anchor_snapshot.version(),
            )));
        }

        anchor_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Scan)?;

        let action_kinds = actions.to_vec();
        let needs_protocol_validation = self.commit_ordering == CommitOrdering::AscendingOrder;
        let inject_protocol =
            needs_protocol_validation && !action_kinds.contains(&DeltaAction::Protocol);
        let inject_metadata =
            needs_protocol_validation && !action_kinds.contains(&DeltaAction::Metadata);
        let read_schema = build_read_schema(
            action_kinds
                .iter()
                .copied()
                .chain(inject_protocol.then_some(DeltaAction::Protocol))
                .chain(inject_metadata.then_some(DeltaAction::Metadata)),
        );
        let json_handler = engine.json_handler();
        let commit_files = self.commit_files.clone();

        let drop_column_evaluator = if inject_protocol || inject_metadata {
            Some(build_drop_protocol_evaluator(
                engine,
                &action_kinds,
                read_schema.clone(),
            )?)
        } else {
            None
        };

        let protocol_validator = needs_protocol_validation.then(|| {
            Arc::new(ProtocolValidator::from(
                anchor_snapshot,
                drop_column_evaluator,
            ))
        });
        Ok(commit_files.into_iter().map(move |file| {
            open_commit_actions(
                json_handler.as_ref(),
                &file,
                read_schema.clone(),
                protocol_validator.clone(),
            )
        }))
    }

    /// Yield action batches across all commits as [`crate::EngineData`], with
    /// `version` and `timestamp` columns prepended to each batch's
    /// struct schema.
    ///
    /// Built on top of [`Self::commits`]: every batch is transformed via the engine's
    /// `EvaluationHandler` to inject the commit's version and timestamp as literal
    /// columns. The original action columns (`add`, `remove`, ...) are passed through
    /// unchanged in the order driven by `actions`.
    ///
    /// Returns `Err` eagerly under the same conditions as [`Self::commits`] (empty `actions`,
    /// snapshot/anchor version mismatch, unsupported `Operation::Scan`, or evaluator build
    /// failure); per-batch errors surface during iteration.
    pub fn actions(
        &self,
        engine: &dyn Engine,
        anchor_snapshot: SnapshotRef,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
        let action_kinds = actions.to_vec();
        let input_schema = build_read_schema(action_kinds.iter().copied());
        let output_schema = actions_output_schema(&action_kinds);
        let evaluation_handler = engine.evaluation_handler();
        let commits_iter = self.commits(engine, anchor_snapshot, actions)?;

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
                Box::new(commit.into_actions().map(move |batch_res| {
                    batch_res.and_then(|batch| evaluator.evaluate(batch.as_ref()))
                }))
            },
        ))
    }
}

struct ProtocolValidator {
    table_configuration: Arc<Mutex<TableConfiguration>>,
    drop_evaluator: Option<Arc<dyn ExpressionEvaluator>>,
}

impl ProtocolValidator {
    fn from(snapshot: SnapshotRef, drop_evaluator: Option<Arc<dyn ExpressionEvaluator>>) -> Self {
        Self {
            table_configuration: Arc::new(Mutex::new(snapshot.table_configuration().clone())),
            drop_evaluator,
        }
    }

    fn validate_protocol_and_drop_column(
        &self,
        batch: Box<dyn EngineData>,
        version: Version,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let new_protocol = Protocol::try_new_from_data(batch.as_ref())?;
        let new_metadata = Metadata::try_new_from_data(batch.as_ref())?;

        if new_protocol.is_some() || new_metadata.is_some() {
            let mut guard = self
                .table_configuration
                .lock()
                .map_err(|_| Error::generic("table configuration mutex poisoned"))?;
            *guard = TableConfiguration::try_new_from(&guard, new_metadata, new_protocol, version)?;
            guard.ensure_operation_supported(Operation::Scan)?;
        }

        match &self.drop_evaluator {
            Some(eval) => eval.evaluate(batch.as_ref()),
            None => Ok(batch),
        }
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

/// Build an evaluator that projects a batch (with auto-injected `protocol`) onto the
/// user's requested action columns only. Built once per `commits()` call and reused
/// across every batch in the range.
fn build_drop_protocol_evaluator(
    engine: &dyn Engine,
    user_actions: &[DeltaAction],
    input_schema: SchemaRef,
) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
    let output_schema = build_read_schema(user_actions.iter().copied());
    let projection = Expression::struct_from(
        user_actions
            .iter()
            .copied()
            .map(|a| Expression::column([action_column_name(a)])),
    );
    engine.evaluation_handler().new_expression_evaluator(
        input_schema,
        Arc::new(projection),
        output_schema.into(),
    )
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

/// Open a single commit JSON file and package its batch iterator into a [`CommitActions`].
/// When `validator` is `Some`, batches are routed through it for protocol validation and
/// auto-injected-column stripping; when `None`, batches pass through unchanged.
fn open_commit_actions(
    json_handler: &dyn JsonHandler,
    file: &ParsedLogPath,
    read_schema: SchemaRef,
    validator: Option<Arc<ProtocolValidator>>,
) -> DeltaResult<CommitActions> {
    let raw_iter =
        json_handler.read_json_files(slice::from_ref(&file.location), read_schema, None)?;

    let version = file.version;
    let actions = Box::new(raw_iter.map(move |batch_res| {
        let batch = batch_res?;
        match &validator {
            Some(v) => v.validate_protocol_and_drop_column(batch, version),
            None => Ok(batch),
        }
    }));

    Ok(CommitActions {
        version: file.version,
        // Currently the commit file's last_modified time; ICT-enabled tables are not yet
        // supported. TODO: extract `commitInfo.inCommitTimestamp` from the first batch
        // (peek+rewind) when ICT is enabled on the table.
        timestamp: file.location.last_modified,
        actions,
    })
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
    fn open_test_range_at(start_version: Version) -> (CommitRange, SyncEngine, SnapshotRef) {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), start_version)
            .with_end_version(1)
            .build(&engine)
            .unwrap();
        let anchor_snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(start_version)
            .build(&engine)
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
            .commits(&engine, anchor_snapshot, &actions)
            .unwrap()
            .collect::<DeltaResult<Vec<_>>>()
            .unwrap();

        assert_eq!(collected.len(), 2, "yield one CommitActions per commit");
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
        let mut iter = range.commits(&engine, anchor_snapshot, &actions).unwrap();
        let commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(commit.version(), 1);
        assert!(iter.next().is_none(), "single-commit range yields only v=1");

        let mut visitor = AddRemovePathVisitor::default();
        for batch_res in commit.into_actions() {
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
        engine: &dyn Engine,
        anchor_snapshot: SnapshotRef,
        actions: &[DeltaAction],
    ) -> DeltaResult<()> {
        for commit_res in range.commits(engine, anchor_snapshot, actions)? {
            let commit = commit_res?;
            for batch_res in commit.into_actions() {
                batch_res?;
            }
        }
        Ok(())
    }

    /// Assert that draining the range surfaces an `Error::Unsupported` whose message
    /// contains `expected_substring`.
    fn assert_unsupported_with_substring(
        range: &CommitRange,
        engine: &dyn Engine,
        anchor_snapshot: SnapshotRef,
        actions: &[DeltaAction],
        expected_substring: &str,
    ) {
        let err = drain_commits(range, engine, anchor_snapshot, actions)
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
        assert_unsupported_with_substring(&range, engine.as_ref(), anchor_snapshot, &actions, "99");
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
        let err = drain_commits(&range, engine.as_ref(), anchor_snapshot, &actions)
            .expect_err("v=1 reader version must be rejected");
        match &err {
            Error::InvalidProtocol(msg) => assert!(msg.contains('0'), "got: {msg}"),
            other => panic!("expected Error::InvalidProtocol, got: {other:?}"),
        }
    }

    /// Per-batch validation is governed by [`CommitOrdering`]:
    /// - ascending: the validator parses every batch's `Protocol` row and rejects unsupported
    ///   features.
    /// - descending: the validator is `None`; the snapshot at `end_version` already validates the
    ///   protocol, and feature monotonicity covers earlier commits.
    ///
    /// Each case forges a 2-commit log with `futureFeature` in one of the commits, and a
    /// snapshot at whichever end is "clean". `Some(needle)` asserts the iteration error
    /// surfaces the offending feature; `None` asserts the iteration drains cleanly.
    #[rstest::rstest]
    #[case::ascending_rejects_unsupported_feature(
        CommitOrdering::AscendingOrder,
        format!("{}\n{}", VALID_PROTOCOL_LINE, VALID_METADATA_LINE),
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#.to_string(),
        0,
        Some("futureFeature"),
    )]
    #[case::descending_skips_unsupported_feature_in_earlier_commit(
        CommitOrdering::DescendingOrder,
        format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
            VALID_METADATA_LINE,
        ),
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        1,
        None,
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
        let result = drain_commits(&range, engine.as_ref(), snapshot, &actions);
        match expected_err_substring {
            Some(needle) => {
                let err = result.expect_err("expected per-batch validation to reject");
                let msg = format!("{err}");
                assert!(msg.contains(needle), "expected {needle:?} in error: {msg}");
            }
            None => {
                result.expect("descending must skip per-batch validation");
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
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .with_ordering(ordering)
            .build(&engine)
            .unwrap();
        let bad_snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(snapshot_version)
            .build(&engine)
            .unwrap();

        let err = range
            .commits(&engine, bad_snapshot, &[DeltaAction::Add])
            .err()
            .unwrap();
        let msg = format!("{err}");
        assert!(msg.contains(expected_anchor_name));
    }

    #[test]
    fn commits_errors_on_empty_actions() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let err = range.commits(&engine, anchor_snapshot, &[]).err().unwrap();
        let msg = format!("{err}");
        assert!(msg.contains("DeltaAction"), "got: {msg}");
    }

    #[rstest::rstest]
    #[case::strips_injected_protocol(
        &[DeltaAction::Add, DeltaAction::Remove],
        &["add", "remove"],
    )]
    #[case::preserves_caller_requested_protocol(
        &[DeltaAction::Add, DeltaAction::Remove, DeltaAction::Protocol],
        &["add", "remove", "protocol"],
    )]
    fn commits_emitted_schema_matches_caller_actions(
        #[case] actions: &[DeltaAction],
        #[case] expected_columns: &[&str],
    ) {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let mut saw_batch = false;
        for commit in range.commits(&engine, anchor_snapshot, actions).unwrap() {
            let commit = commit.unwrap();
            for batch in commit.into_actions() {
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
            .commits(engine.as_ref(), anchor_snapshot, &actions)
            .unwrap();

        let v0_commit = iter.next().expect("v=0 commit").unwrap();
        let v0_batches: DeltaResult<Vec<_>> = v0_commit.into_actions().collect();
        v0_batches.expect("v=0 must drain cleanly");

        let v1_commit = iter.next().expect("v=1 commit").unwrap();
        let v1_err = v1_commit
            .into_actions()
            .find_map(Result::err)
            .expect("v=1 must reject on validation");
        match v1_err {
            Error::Unsupported(msg) => assert!(msg.contains("99"), "got: {msg}"),
            other => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    #[test]
    fn actions_yields_metadata_columns_prepended() {
        let (range, engine, anchor_snapshot) = open_test_range_at(0);
        let files = range.commit_files.clone();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut visitor = ActionsTaggedVisitor::default();
        for batch_res in range.actions(&engine, anchor_snapshot, &actions).unwrap() {
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
}
