//! Read a contiguous range of Delta commits without requiring a snapshot at the start version.
//!
//! A [`CommitRange`] is a *plan* over an inclusive `[start_version, end_version]` range of
//! commits. Construction performs a single delta-log listing (plus an optional merge with
//! catalog-supplied commits via [`CommitRangeBuilder::with_log_tail`]); no JSON is read until
//! the caller iterates [`CommitRange::commits`].
//!
//! Unlike [`crate::table_changes::TableChanges`], `CommitRange` does not load a snapshot at
//! `start_version`. This is the property streaming sources need: log cleanup may have removed
//! the start version's snapshot prerequisites, and per-batch protocol validation already
//! covers the safety concerns that snapshot-level validation would.
//!
//! # Example
//! ```ignore
//! use delta_kernel::commit_range::{CommitBoundary, CommitRange, DeltaAction};
//!
//! let range = CommitRange::builder_for("file:///data/T", CommitBoundary::Version(0))
//!     .with_end_boundary(CommitBoundary::Version(4))
//!     .build(engine)?;
//!
//! for commit in range.commits(engine, &[DeltaAction::Add, DeltaAction::Remove]) {
//!     let commit = commit?;
//!     println!("v={} ts={}", commit.version(), commit.timestamp());
//!     for batch in commit.into_actions() {
//!         let _batch = batch?;
//!     }
//! }
//! ```

mod actions;
mod builder;

pub use actions::{CommitActions, DeltaAction};
pub use builder::CommitRangeBuilder;

use std::collections::HashSet;
use std::slice;
use std::sync::Arc;

use url::Url;

use crate::actions::{
    Add, Cdc, CommitInfo, Metadata, Protocol, Remove, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::expressions::Expression;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, EngineData, Error, JsonHandler, Version};

/// Output column name for the per-row commit version emitted by [`CommitRange::actions`].
const COMMIT_VERSION_COL: &str = "_commit_version";

/// Output column name for the per-row commit timestamp emitted by [`CommitRange::actions`].
const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

/// A boundary specification for a [`CommitRange`].
///
/// Use [`CommitBoundary::Version`] for a known commit version (no resolution required).
/// Use [`CommitBoundary::Timestamp`] when the boundary must be resolved against the
/// table's history.
///
/// Resolving a [`CommitBoundary::Timestamp`] requires a snapshot; the caller must enter
/// the builder via [`CommitRange::builder_from`] (which captures a snapshot) when using
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
/// `CommitRange` holds resolved `[start_version, end_version]` bounds plus the listed
/// commit-file metadata in a [`LogSegment`]. It does not own a snapshot. Reading the
/// underlying actions is lazy via [`CommitRange::commits`].
#[derive(Debug)]
pub struct CommitRange {
    pub(crate) table_root: Url,
    pub(crate) log_segment: LogSegment,
    pub(crate) start_version: Version,
    pub(crate) end_version: Version,
    pub(crate) start_boundary: CommitBoundary,
    pub(crate) end_boundary: Option<CommitBoundary>,
}

impl CommitRange {
    /// Begin building a [`CommitRange`] rooted at `table_root`.
    ///
    /// Path-based entry: only [`CommitBoundary::Version`] boundaries are supported.
    /// To use [`CommitBoundary::Timestamp`] boundaries, use [`Self::builder_from`] instead.
    pub fn builder_for(
        table_root: impl AsRef<str>,
        start_boundary: CommitBoundary,
    ) -> CommitRangeBuilder {
        CommitRangeBuilder::new_for(table_root, start_boundary)
    }

    /// Begin building a [`CommitRange`] derived from an existing snapshot.
    ///
    /// The snapshot's table root is used as the range's upper bound, and the snapshot's
    /// LogSegment is retained to get the list of commit
    pub fn builder_from(
        snapshot: SnapshotRef,
        start_boundary: CommitBoundary,
    ) -> CommitRangeBuilder {
        let _ = (snapshot, start_boundary);
        todo!("CommitRange::builder_from")
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
    /// `actions` drives the read schema literally: the engine projects each commit JSON to
    /// a struct with one nullable field per requested [`DeltaAction`]. Action kinds not in
    /// the set are dropped during projection by the engine. No auto-injection of
    /// `protocol` or `commitInfo` happens here — protocol validation and ICT resolution
    /// are the caller's responsibility.
    ///
    /// The returned iterator owns its dependencies (an `Arc<dyn JsonHandler>` cloned from
    /// `engine`, an owned `Vec<ParsedLogPath>` cloned from the log segment), so it does
    /// not borrow from `&self` or `&engine` after this call returns.
    pub fn commits(
        &self,
        engine: &dyn Engine,
        actions: &HashSet<DeltaAction>,
    ) -> impl Iterator<Item = DeltaResult<CommitActions>> + Send {
        let action_kinds: Vec<DeltaAction> = actions.iter().copied().collect();
        let read_schema = build_read_schema(&action_kinds);
        let json_handler = engine.json_handler();
        let commit_files = self.log_segment.listed.ascending_commit_files().clone();

        commit_files
            .into_iter()
            .map(move |file| open_commit_actions(json_handler.as_ref(), &file, read_schema.clone()))
    }

    /// Yield action batches across all commits as [`crate::EngineData`], with
    /// `_commit_version` and `_commit_timestamp` columns prepended to each batch's
    /// struct schema.
    ///
    /// Built on top of [`Self::commits`]: every batch is transformed via the engine's
    /// `EvaluationHandler` to inject the commit's version and timestamp as literal
    /// columns. The original action columns (`add`, `remove`, ...) are passed through
    /// unchanged in the order driven by `actions`.
    pub fn actions(
        &self,
        engine: &dyn Engine,
        actions: &HashSet<DeltaAction>,
    ) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send {
        let action_kinds: Vec<DeltaAction> = actions.iter().copied().collect();
        let input_schema = build_read_schema(&action_kinds);
        let output_schema = actions_output_schema(&action_kinds);
        let evaluation_handler = engine.evaluation_handler();
        let commits_iter = self.commits(engine, actions);

        commits_iter.flat_map(
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
        )
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
fn build_read_schema(actions: &[DeltaAction]) -> SchemaRef {
    let fields = actions.iter().copied().map(action_to_field);
    Arc::new(StructType::new_unchecked(fields))
}

/// Output schema produced by [`CommitRange::actions`]:
/// `struct<_commit_version: long, _commit_timestamp: long, ...input_fields>`.
fn actions_output_schema(actions: &[DeltaAction]) -> SchemaRef {
    let metadata_fields = [
        StructField::not_null(COMMIT_VERSION_COL, DataType::LONG),
        StructField::not_null(COMMIT_TIMESTAMP_COL, DataType::LONG),
    ];
    let action_fields = actions.iter().copied().map(action_to_field);
    Arc::new(StructType::new_unchecked(
        metadata_fields.into_iter().chain(action_fields),
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
/// The version is taken from the parsed filename. The timestamp is currently the file's
/// `last_modified`; ICT extraction is intentionally deferred (TODO) since it requires
/// either a snapshot to check ICT enablement or a single-read peek+rewind.
fn open_commit_actions(
    json_handler: &dyn JsonHandler,
    file: &ParsedLogPath,
    read_schema: SchemaRef,
) -> DeltaResult<CommitActions> {
    let actions =
        json_handler.read_json_files(slice::from_ref(&file.location), read_schema, None)?;
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
    use std::sync::LazyLock;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
    use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes};

    /// Open a `CommitRange` over the well-known `table-with-dv-small` test table.
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
        let files = range.log_segment.listed.ascending_commit_files();
        assert_eq!(
            files.len(),
            2,
            "table-with-dv-small has 2 commits (v=0..=1)"
        );

        let actions = HashSet::from([DeltaAction::Add, DeltaAction::Remove]);
        let collected: Vec<CommitActions> = range
            .commits(&engine, &actions)
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

        let actions = HashSet::from([DeltaAction::Add, DeltaAction::Remove]);
        let mut iter = range.commits(&engine, &actions);
        let commit = iter.next().expect("v=1 commit").unwrap();
        assert_eq!(commit.version(), 1);
        assert!(iter.next().is_none(), "single-commit range yields only v=1");

        let mut visitor = AddRemovePathVisitor::default();
        for batch_res in commit.into_actions() {
            visitor.visit_rows_of(batch_res.unwrap().as_ref()).unwrap();
        }

        let adds: Vec<&str> = visitor
            .rows
            .iter()
            .filter_map(|(a, _)| a.as_deref())
            .collect();
        let removes: Vec<&str> = visitor
            .rows
            .iter()
            .filter_map(|(_, r)| r.as_deref())
            .collect();
        assert_eq!(adds.len(), 1, "v=1 has exactly one add");
        assert_eq!(removes.len(), 1, "v=1 has exactly one remove");
        // DV rewrite: the same physical file path appears on both sides.
        assert_eq!(adds[0], removes[0], "DV rewrite shares the file path");
    }

    /// Visitor capturing `(_commit_version, _commit_timestamp, add.path, remove.path)` per row.
    #[derive(Default)]
    struct ActionsTaggedVisitor {
        rows: Vec<(i64, i64, Option<String>, Option<String>)>,
    }

    impl RowVisitor for ActionsTaggedVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static COLS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                (
                    vec![
                        column_name!("_commit_version"),
                        column_name!("_commit_timestamp"),
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
                let version: i64 = getters[0].get(i, "_commit_version")?;
                let timestamp: i64 = getters[1].get(i, "_commit_timestamp")?;
                let add_path: Option<String> = getters[2].get_opt(i, "add.path")?;
                let remove_path: Option<String> = getters[3].get_opt(i, "remove.path")?;
                self.rows.push((version, timestamp, add_path, remove_path));
            }
            Ok(())
        }
    }

    #[test]
    fn actions_yields_metadata_columns_prepended() {
        let (range, engine) = open_test_range();
        let files = range.log_segment.listed.ascending_commit_files().clone();

        let actions = HashSet::from([DeltaAction::Add, DeltaAction::Remove]);
        let mut visitor = ActionsTaggedVisitor::default();
        for batch_res in range.actions(&engine, &actions) {
            visitor.visit_rows_of(batch_res.unwrap().as_ref()).unwrap();
        }

        // v=0 contributes one add (initial file).
        // v=1 contributes one remove + one add (DV rewrite).
        let mut add_versions: Vec<i64> = visitor
            .rows
            .iter()
            .filter_map(|r| r.2.as_ref().map(|_| r.0))
            .collect();
        add_versions.sort();
        let remove_versions: Vec<i64> = visitor
            .rows
            .iter()
            .filter_map(|r| r.3.as_ref().map(|_| r.0))
            .collect();
        assert_eq!(add_versions, vec![0, 1], "adds appear in v=0 and v=1");
        assert_eq!(remove_versions, vec![1], "remove appears only in v=1");

        // _commit_timestamp matches each commit file's last_modified.
        for (version, timestamp, _, _) in &visitor.rows {
            let v_ix = *version as usize;
            assert_eq!(
                *timestamp, files[v_ix].location.last_modified,
                "timestamp for commit {version} must match file last_modified",
            );
        }
    }
}
