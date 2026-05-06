//! Read a contiguous range of Delta commits without requiring a snapshot at the start version.
//!
//! A [`CommitRange`] is a *plan* over an inclusive `[start_version, end_version]` range of
//! commits. Construction performs a single delta-log listing (plus an optional merge with
//! catalog-supplied commits via `CommitRangeBuilder::with_log_tail`); no JSON is read until
//! the caller iterates [`CommitRange::commits`].
//!
//! Unlike [`crate::table_changes::TableChanges`], `CommitRange` does not load a snapshot at
//! `start_version`. This is the property streaming sources need: log cleanup may have removed
//! the start version's snapshot prerequisites. Path-based ranges (no snapshot supplied) run
//! per-batch protocol validation in lieu of snapshot-level checks; snapshot-based ranges
//! inherit the snapshot's validation.
//!
//! # Example
//! ```ignore
//! use delta_kernel::commit_range::{CommitBoundary, CommitRange, DeltaAction};
//!
//! let range = CommitRange::builder_for("file:///data/T", CommitBoundary::Version(0))
//!     .with_end_boundary(CommitBoundary::Version(4))
//!     .build(engine)?;
//!
//! for commit in range.commits(engine, &[DeltaAction::Add, DeltaAction::Remove])? {
//!     let commit = commit?;
//!     println!("v={} ts={}", commit.version(), commit.timestamp());
//!     for batch in commit.into_actions() {
//!         let _batch = batch?;
//!     }
//! }
//! ```

mod actions;
mod builder;

use std::slice;
use std::sync::{Arc, LazyLock};

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
use crate::table_features::{KernelSupport, MAX_VALID_READER_VERSION};
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

/// A boundary specification for a [`CommitRange`].
///
/// Use [`CommitBoundary::Version`] for a known commit version (no resolution required).
/// A planned `CommitBoundary::Timestamp` variant (TODO) will support resolving boundaries
/// against the table's history.
///
/// Resolving a timestamp boundary will require a snapshot; the caller must enter the
/// builder via [`CommitRange::builder_from`] (which captures a snapshot) when using
/// timestamp boundaries. [`CommitRange::builder_for`] (path-only) supports only
/// [`CommitBoundary::Version`].
#[derive(Debug, Clone, Copy)]
pub enum CommitBoundary {
    /// A specific commit version.
    Version(Version),
    // TODO: support the timestamp CommitBoundary
}

/// A plan over a contiguous range of Delta commits.
///
/// `CommitRange` holds resolved `[start_version, end_version]` bounds plus the materialized
/// commit-file pointers in `commit_files`. The pointer order matches the
/// [`CommitOrdering`] requested at build time (ascending by default; reversed if
/// [`CommitOrdering::DescendingOrder`]). Reading the underlying actions is
/// lazy via [`CommitRange::commits`].
#[derive(Debug)]
pub struct CommitRange {
    table_root: Url,
    commit_files: Vec<ParsedLogPath>,
    start_version: Version,
    end_version: Version,
    start_boundary: CommitBoundary,
    end_boundary: Option<CommitBoundary>,
    /// True for path-based ranges (no snapshot supplied at build time): enables per-batch
    /// protocol validation in [`Self::commits`]. False for snapshot-based ranges, which
    /// inherit the snapshot's validation.
    validate_protocol: bool,
}

impl CommitRange {
    /// Begin building a [`CommitRange`] rooted at `table_root`.
    ///
    /// Path-based entry: only [`CommitBoundary::Version`] boundaries are supported.
    /// Future timestamp boundaries (TODO) will require entering via [`Self::builder_from`].
    pub fn builder_for(
        table_root: impl AsRef<str>,
        start_boundary: CommitBoundary,
    ) -> CommitRangeBuilder {
        CommitRangeBuilder::new_for(table_root, start_boundary)
    }

    /// Begin building a [`CommitRange`] derived from an existing snapshot.
    ///
    /// The snapshot's table root anchors the range, and the snapshot's `LogSegment` is
    /// reused to enumerate commits, avoiding an extra delta-log listing. Per-batch
    /// protocol validation is skipped on the assumption the snapshot already validated
    /// its protocol.
    pub fn builder_from(
        snapshot: SnapshotRef,
        start_boundary: CommitBoundary,
    ) -> CommitRangeBuilder {
        CommitRangeBuilder::new_from(snapshot, start_boundary)
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

    /// The start boundary as originally supplied by the caller.
    pub fn start_boundary(&self) -> &CommitBoundary {
        &self.start_boundary
    }

    /// The end boundary as originally supplied by the caller, if any.
    pub fn end_boundary(&self) -> Option<&CommitBoundary> {
        self.end_boundary.as_ref()
    }

    /// Iterator over commits in the range. Each item is a [`CommitActions`] that exposes
    /// the commit's version, timestamp (currently the file's `last_modified`), and a lazy
    /// iterator over the commit's action batches projected to `actions`.
    ///
    /// `actions` drives the read schema literally: the engine projects each commit JSON
    /// to a struct with one nullable field per requested [`DeltaAction`]. Callers should
    /// pass distinct kinds; duplicates would produce duplicate read-schema fields.
    ///
    /// For path-based ranges (no snapshot supplied at build time), `Protocol` is
    /// auto-injected into the read schema and per-batch protocol validation runs: the
    /// iterator yields `Err` and stops on the first unsupported protocol. The
    /// auto-injected `protocol` column is stripped from emitted batches if the caller
    /// didn't explicitly include [`DeltaAction::Protocol`]. Snapshot-based ranges skip
    /// this validation and inherit the snapshot's protocol checks.
    ///
    /// Returns `Err` eagerly if the projection evaluator cannot be constructed; per-batch
    /// errors surface during iteration.
    pub fn commits(
        &self,
        engine: &dyn Engine,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<CommitActions>> + Send> {
        let action_kinds = actions.to_vec();
        let inject_protocol =
            self.validate_protocol && !action_kinds.contains(&DeltaAction::Protocol);
        let read_schema = build_read_schema(
            action_kinds
                .iter()
                .copied()
                .chain(inject_protocol.then_some(DeltaAction::Protocol)),
        );
        let json_handler = engine.json_handler();
        let validate = self.validate_protocol;
        let commit_files = self.commit_files.clone();

        let drop_column_evaluator = if inject_protocol {
            Some(build_drop_protocol_evaluator(
                engine,
                &action_kinds,
                read_schema.clone(),
            )?)
        } else {
            None
        };

        Ok(commit_files.into_iter().map(move |file| {
            open_commit_actions(
                json_handler.as_ref(),
                &file,
                read_schema.clone(),
                validate,
                drop_column_evaluator.clone(),
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
    /// Returns `Err` eagerly if the underlying [`Self::commits`] call fails to construct
    /// (e.g. evaluator build failure); per-batch errors surface during iteration.
    pub fn actions(
        &self,
        engine: &dyn Engine,
        actions: &[DeltaAction],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
        let action_kinds = actions.to_vec();
        let input_schema = build_read_schema(action_kinds.iter().copied());
        let output_schema = actions_output_schema(&action_kinds);
        let evaluation_handler = engine.evaluation_handler();
        let commits_iter = self.commits(engine, actions)?;

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

/// Validate that Kernel can read a table governed by `protocol`.
///
/// Lower-bound (`min_reader_version >= 1`) is enforced by [`Protocol::try_new`] during
/// deserialization, so this helper only checks the upper bound and the `reader_features`
/// list.
fn validate_protocol_for_read(protocol: &Protocol) -> DeltaResult<()> {
    let version = protocol.min_reader_version();
    if version > MAX_VALID_READER_VERSION {
        return Err(Error::unsupported(format!(
            "Unsupported minimum reader version {version}; \
             kernel supports up to {MAX_VALID_READER_VERSION}",
        )));
    }
    if let Some(features) = protocol.reader_features() {
        for feature in features {
            if matches!(feature.info().kernel_support, KernelSupport::NotSupported) {
                return Err(Error::unsupported(format!(
                    "Reader feature '{feature}' is not supported by kernel",
                )));
            }
        }
    }
    Ok(())
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

/// Open a single commit JSON file via the engine and package its batch iterator into a
/// [`CommitActions`].
///
/// The version is taken from the [`ParsedLogPath`]. The timestamp is currently the file's
/// `last_modified`; ICT extraction is intentionally deferred (TODO) since it requires
/// either a snapshot to check ICT enablement or a single-read peek+rewind.
///
/// When `validate_protocol` is `true`, batches are scanned for a non-null `protocol`
/// row and the row (if found) is validated via [`validate_protocol_for_read`] before
/// emission; the scan short-circuits after the first hit since spec guarantees at most
/// one Protocol action per commit. When `drop_evaluator` is `Some`, each batch is
/// projected through it to strip the auto-injected `protocol` column so the emitted
/// schema matches the caller's original action set.
fn open_commit_actions(
    json_handler: &dyn JsonHandler,
    file: &ParsedLogPath,
    read_schema: SchemaRef,
    validate_protocol: bool,
    drop_evaluator: Option<Arc<dyn ExpressionEvaluator>>,
) -> DeltaResult<CommitActions> {
    let raw_iter =
        json_handler.read_json_files(slice::from_ref(&file.location), read_schema, None)?;

    let mut seen_protocol = false;
    let actions = Box::new(raw_iter.map(move |batch_res| {
        let batch = batch_res?;
        if validate_protocol && !seen_protocol {
            if let Some(protocol) = Protocol::try_new_from_data(batch.as_ref())? {
                validate_protocol_for_read(&protocol)?;
                seen_protocol = true;
            }
        }
        match &drop_evaluator {
            Some(eval) => eval.evaluate(batch.as_ref()),
            None => Ok(batch),
        }
    }));

    Ok(CommitActions {
        version: file.version,
        // TODO: extract `commitInfo.inCommitTimestamp` from the first batch (peek+rewind)
        // when ICT is enabled on the table; fall back to `last_modified` otherwise.
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

    /// Open a `CommitRange` over the `table-with-dv-small` test table.
    ///
    /// The table has two commits with mixed action kinds:
    /// - v=0: protocol + metaData + add (initial file)
    /// - v=1: commitInfo + remove (drops v=0 file) + add (rewrite with deletion vector)
    fn open_test_range() -> (CommitRange, SyncEngine) {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(1))
            .build(&engine)
            .unwrap();
        (range, engine)
    }

    #[test]
    fn commits_yields_one_per_commit_in_range() {
        let (range, engine) = open_test_range();
        let files = &range.commit_files;
        assert_eq!(
            files.len(),
            2,
            "table-with-dv-small has 2 commits (v=0..=1)"
        );

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let collected = range
            .commits(&engine, &actions)
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
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), CommitBoundary::Version(1))
            .with_end_boundary(CommitBoundary::Version(1))
            .build(&engine)
            .unwrap();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range.commits(&engine, &actions).unwrap();
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
        actions: &[DeltaAction],
    ) -> DeltaResult<()> {
        for commit_res in range.commits(engine, actions)? {
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
        actions: &[DeltaAction],
        expected_substring: &str,
    ) {
        let err = drain_commits(range, engine, actions).expect_err("validation must reject");
        match &err {
            Error::Unsupported(msg) => assert!(
                msg.contains(expected_substring),
                "expected message to contain {expected_substring:?}, got: {msg}",
            ),
            other => panic!("expected Error::Unsupported, got: {other:?}"),
        }
    }

    const VALID_METADATA_LINE: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1000}}"#;

    #[tokio::test]
    async fn commits_errors_on_too_high_reader_version() {
        let body = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#, VALID_METADATA_LINE,
        );
        let (engine, table_root) = engine_with_commits(&[(0, &body)]).await;

        let range = CommitRange::builder_for(table_root, CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(0))
            .build(engine.as_ref())
            .expect("build should succeed; validation runs during iteration");

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        assert_unsupported_with_substring(&range, engine.as_ref(), &actions, "99");
    }

    #[tokio::test]
    async fn commits_errors_on_too_low_reader_version() {
        // minReaderVersion=0 is rejected by `Protocol::try_new` before validate_protocol_for_read
        // ever runs; the per-batch path surfaces the InvalidProtocol error. This pins the
        // layered defense: malformed protocols are caught by deserialization, and well-formed
        // but unsupported protocols (e.g. version=99) are caught by validate_protocol_for_read.
        let body = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":0,"minWriterVersion":1}}"#, VALID_METADATA_LINE,
        );
        let (engine, table_root) = engine_with_commits(&[(0, &body)]).await;

        let range = CommitRange::builder_for(table_root, CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(0))
            .build(engine.as_ref())
            .expect("build should succeed");

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let err = drain_commits(&range, engine.as_ref(), &actions)
            .expect_err("v=0 reader version must be rejected");
        match &err {
            Error::InvalidProtocol(msg) => assert!(msg.contains('0'), "got: {msg}"),
            other => panic!("expected Error::InvalidProtocol, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn commits_errors_on_unsupported_reader_feature() {
        let body = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
            VALID_METADATA_LINE,
        );
        let (engine, table_root) = engine_with_commits(&[(0, &body)]).await;

        let range = CommitRange::builder_for(table_root, CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(0))
            .build(engine.as_ref())
            .expect("build should succeed");

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        assert_unsupported_with_substring(&range, engine.as_ref(), &actions, "futureFeature");
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
        let (range, engine) = open_test_range();
        let mut saw_batch = false;
        for commit in range.commits(&engine, actions).unwrap() {
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
        // v=0: valid (3,7) protocol + metadata. v=1: protocol upgrade to an unsupported
        // version. The iterator must yield v=0 cleanly and surface the error on v=1.
        let v0 = format!(
            "{}\n{}",
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}"#,
            VALID_METADATA_LINE,
        );
        let v1 = r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"#;
        let (engine, table_root) = engine_with_commits(&[(0, &v0), (1, v1)]).await;

        let range = CommitRange::builder_for(table_root, CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(1))
            .build(engine.as_ref())
            .expect("build should succeed");

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut iter = range.commits(engine.as_ref(), &actions).unwrap();

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
        let (range, engine) = open_test_range();
        let files = range.commit_files.clone();

        let actions = [DeltaAction::Add, DeltaAction::Remove];
        let mut visitor = ActionsTaggedVisitor::default();
        for batch_res in range.actions(&engine, &actions).unwrap() {
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
