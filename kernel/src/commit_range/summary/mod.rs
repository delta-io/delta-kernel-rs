//! [`CommitSummaryPlan`] and [`CommitSummary`]: what each commit of a [`CommitRange`] did, read
//! through a declarative plan. The `Commit summaries` section of the [`commit_range`](super)
//! module docs describes the flow and which checks are Kernel's.
//!
//! The rows of the plan belong to Kernel. Their schema is an implementation detail: pass them to
//! [`CommitSummaryPlan::validate`] unchanged instead of reading them.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use itertools::Itertools as _;
use url::Url;

use super::actions::{ensure_commit_readable, resolve_commit_timestamp};
use super::schema_changes::{SchemaChange, diff_schemas};
use super::{CommitOrdering, CommitRange, add_version_context, with_version_context};
use crate::actions::visitors::{
    METADATA_LEAVES, PROTOCOL_LEAVES, visit_metadata_at, visit_protocol_at,
};
use crate::actions::{
    ADD_NAME, COMMIT_INFO_NAME, METADATA_FIELD, METADATA_NAME, Metadata, PROTOCOL_FIELD,
    PROTOCOL_NAME, Protocol, REMOVE_NAME,
};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{
    ColumnName, Expression, Predicate, Scalar, col, column_name, lit, null_lit,
};
use crate::plans::ir::nodes::{Agg, ScanFile};
use crate::plans::{Plan, PlanBuilder};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructField, lazy_schema_ref, schema,
};
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ColumnMappingMode, TableFeature};
use crate::utils::require;
use crate::{EngineData, KernelError, KernelResult, Result, Version};

#[cfg(test)]
mod tests;

impl CommitRange {
    /// Returns the plan that summarizes the commits of this range, together with the state that
    /// validates the plan's result. See [`CommitSummaryPlan`] for how to use it.
    ///
    /// Kernel reads no commit to build the plan. With a `start_snapshot`, every summary reports the
    /// table's schema and partition columns, and every commit is validated against
    /// the table's full protocol and metadata. Without one, a summary reports them only once a
    /// commit of the range has set them, and validation uses the protocol and metadata actions of
    /// the range alone.
    ///
    /// `start_snapshot` must be the snapshot of this range's table at its start version, whatever
    /// the range's [`CommitOrdering`]. This differs from [`CommitRange::commits`], which anchors a
    /// descending range at its end version.
    ///
    /// # Errors
    ///
    /// Returns [`KernelError::CommitRange`] if the range was built with commit range validation
    /// disabled, or if `start_snapshot` belongs to a different table or version.
    pub fn get_commit_summary_plan(
        &self,
        start_snapshot: Option<SnapshotRef>,
    ) -> Result<CommitSummaryPlan> {
        self.require_commit_actions_supported()?;
        let start_configuration = start_snapshot
            .map(|snapshot| {
                self.snapshot_configuration(&snapshot, self.start_version, "start_version")
                    .cloned()
            })
            .transpose()?;

        let (files, commits): (Vec<_>, Vec<_>) = self
            .commit_files
            .iter()
            .sorted_by_key(|commit| commit.version)
            .map(|commit| {
                let file = ScanFile {
                    meta: commit.location.clone(),
                    file_constants: vec![Scalar::Long(commit.version_as_i64()?)],
                };
                Ok::<_, KernelError>((file, (commit.version, commit.location.last_modified)))
            })
            .process_results(|pairs| pairs.unzip())?;
        Ok(CommitSummaryPlan {
            plan: summary_plan(files)?,
            table_root: self.table_root.clone(),
            commits,
            commit_ordering: self.commit_ordering,
            start_configuration,
        })
    }
}

/// A plan that summarizes the commits of a [`CommitRange`], and the state Kernel needs to turn
/// the plan's result into [`CommitSummary`] values.
///
/// Execute [`plan`](Self::plan) with a query engine, then pass every batch it produces to
/// [`validate`](Self::validate):
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel::commit_range::CommitRange;
/// # use delta_kernel::{Engine, Operation, Snapshot};
/// # fn summarize(engine: Arc<dyn Engine>) -> delta_kernel::Result<()> {
/// let start_snapshot = Snapshot::builder_for("file:///data/T")
///     .at_version(3)
///     .build(engine.as_ref())?;
/// let range = CommitRange::builder_for("file:///data/T", 3)
///     .with_end_version(7)
///     .build(engine.as_ref())?;
///
/// let summary_plan = range.get_commit_summary_plan(Some(start_snapshot))?;
/// // Any query engine can run the plan. Here the engine's own plan executor does.
/// let rows = engine
///     .require_plan_executor()?
///     .execute_op(Operation::QueryPlan(summary_plan.plan().clone()))?
///     .into_data()?;
/// for commit in summary_plan.validate(rows)? {
///     println!("v={} changed data: {:?}", commit.version(), commit.has_data_change());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CommitSummaryPlan {
    plan: Plan,
    table_root: Url,
    /// The version and file modification time of every commit, in ascending version order.
    commits: Vec<(Version, i64)>,
    commit_ordering: CommitOrdering,
    /// The table configuration at the first commit, when the caller supplied its snapshot.
    start_configuration: Option<TableConfiguration>,
}

impl CommitSummaryPlan {
    /// The plan to execute. It produces one row per commit, in no particular order.
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Validates the rows that [`plan`](Self::plan) produced and returns one summary per commit of
    /// the range, in the range's [`CommitOrdering`].
    ///
    /// `plan_result` must hold every row of the plan, in any order and batching.
    ///
    /// Commits are validated in ascending version order whatever the range's ordering, each
    /// against the protocol and metadata in effect at that commit. Validation and timestamps
    /// follow [`CommitRange::commits`] over an ascending range, with these differences:
    ///
    /// - A commit with more than one `protocol`, `metaData`, or `commitInfo` action is rejected,
    ///   because the plan cannot tell which one comes first.
    /// - An in-commit timestamp is used wherever the commit records it, without checking that
    ///   `commitInfo` is the commit's first action.
    /// - The actions of a commit are not checked for the fields that the Delta protocol requires of
    ///   them. An `add` or `remove` action without a `path` is not counted as a file, and one
    ///   without `dataChange` counts as not changing data.
    ///
    /// # Errors
    ///
    /// Returns an error if Kernel cannot read a commit's protocol or metadata, if a commit lacks
    /// an in-commit timestamp that its table requires, if a commit holds more than one protocol,
    /// metadata, or commit info action, or if `plan_result` does not hold exactly one row for
    /// every commit of the range.
    pub fn validate(
        &self,
        plan_result: impl IntoIterator<Item = KernelResult<Box<dyn EngineData>>>,
    ) -> Result<Vec<CommitSummary>> {
        let rows = self.read_rows(plan_result)?;
        let mut state = TableState::new(self.start_configuration.as_ref())?;
        let mut summaries = Vec::with_capacity(self.commits.len());
        for (&(version, last_modified), row) in self.commits.iter().zip(rows) {
            let summary = state
                .advance(&self.table_root, version, last_modified, row)
                .map_err(|error| add_version_context(version, error))?;
            summaries.push(summary);
        }
        if self.commit_ordering == CommitOrdering::DescendingOrder {
            summaries.reverse();
        }
        Ok(summaries)
    }

    /// Reads the plan's rows and returns the row of every commit of the range, in ascending
    /// version order.
    fn read_rows(
        &self,
        plan_result: impl IntoIterator<Item = KernelResult<Box<dyn EngineData>>>,
    ) -> KernelResult<Vec<SummaryRow>> {
        let mut visitor = SummaryRowVisitor::default();
        for batch in plan_result {
            visitor.visit_rows_of(batch?.as_ref())?;
        }
        // The commits are in ascending version order, so sorted rows can be matched to them in
        // one pass.
        let mut rows = visitor.rows;
        rows.sort_unstable_by_key(|&(version, _)| version);
        let mut rows = rows.into_iter().peekable();
        let not_a_commit = |version| {
            KernelError::CommitRange(format!(
                "Commit summary result has a row for version {version}, which is not a commit \
                 of the range"
            ))
        };
        let mut commit_rows = Vec::with_capacity(self.commits.len());
        for &(version, _) in &self.commits {
            match rows.next_if(|&(row_version, _)| row_version <= version) {
                Some((row_version, row)) if row_version == version => commit_rows.push(row),
                Some((row_version, _)) => return Err(not_a_commit(row_version)),
                // Every action of a commit contributes to its row, so only a commit file without
                // any action has none. Such a file is not a valid commit.
                None => {
                    return Err(KernelError::CommitRange(format!(
                        "Commit summary result has no row for version {version}"
                    )));
                }
            }
            require!(
                rows.peek()
                    .is_none_or(|&(row_version, _)| row_version != version),
                KernelError::CommitRange(format!(
                    "Commit summary result has more than one row for version {version}"
                ))
            );
        }
        if let Some((version, _)) = rows.next() {
            return Err(not_a_commit(version));
        }
        Ok(commit_rows)
    }
}

/// What one commit of a [`CommitRange`] did to its table, independent of how the table's log
/// records it.
///
/// A value that the commit does not record, or that Kernel cannot derive, is `None`. `None` never
/// means zero or false: a connector that needs a value it did not get must assume the least
/// convenient one.
#[derive(Debug, Clone)]
pub struct CommitSummary {
    version: Version,
    timestamp: i64,
    table: Option<TableShape>,
    schema_changes: Option<Vec<SchemaChange>>,
    has_data_change: Option<bool>,
    num_added_files: Option<i64>,
    num_removed_files: Option<i64>,
    operation: Option<String>,
    operation_metrics: Option<HashMap<String, String>>,
}

impl CommitSummary {
    /// The version of the commit.
    pub fn version(&self) -> Version {
        self.version
    }

    /// The timestamp of the commit in milliseconds since the Unix epoch, as resolved by
    /// [`CommitAction::timestamp`](super::CommitAction::timestamp).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The logical schema of the table as of this commit, or `None` if it is unknown: the summary
    /// plan was built without a start snapshot and no commit of the range up to this one set the
    /// table's metadata.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.table.as_ref().map(|table| &table.schema)
    }

    /// The logical names of the table's partition columns as of this commit, or `None` under the
    /// same condition as [`schema`](Self::schema).
    pub fn partition_columns(&self) -> Option<&[String]> {
        self.table
            .as_ref()
            .map(|table| table.partition_columns.as_ref())
    }

    /// The changes this commit made to the table's schema. It is empty if the commit left the
    /// schema unchanged, and `None` if the changes are unknown. A commit that sets neither the
    /// table's protocol nor its metadata changes nothing. The changes of a commit that sets one
    /// of them are unknown if Kernel does not know the table's schema or its column mapping mode
    /// before the commit: the commit is the first of the range, or the range was summarized
    /// without a start snapshot and no earlier commit of it has set the metadata, or the
    /// protocol. To learn what the first commit of a range changed, compare its
    /// [`schema`](Self::schema) with the snapshot before the range.
    ///
    /// On a table with column mapping, fields are matched by their column mapping ID. A column
    /// that was dropped and added back under the same name is therefore reported as a
    /// [`SchemaChange::DropColumn`] and a [`SchemaChange::AddColumn`], even though
    /// [`schema`](Self::schema) names the same columns before and after. Without column mapping,
    /// fields are matched by their exact name: a field whose name changes, even if only in case,
    /// is reported as a [`SchemaChange::DropColumn`] and a [`SchemaChange::AddColumn`]. The order
    /// of the fields of a struct is not compared.
    ///
    /// A commit that changes the column mapping mode reports a
    /// [`SchemaChange::ChangeColumnMappingMode`], and its fields are matched by name unless
    /// column mapping is in effect both before and after it.
    pub fn schema_changes(&self) -> Option<&[SchemaChange]> {
        self.schema_changes.as_deref()
    }

    /// Whether the commit changed the table's data: it added or removed a file that does.
    /// Commits that only rearrange existing data, such as compaction, do not.
    ///
    /// On a table with adaptive metadata, the commit itself records whether it changes data,
    /// and that takes precedence over its files. This is `None` if such a commit does not
    /// record it, and if a commit records neither that nor its file changes in the commit file.
    pub fn has_data_change(&self) -> Option<bool> {
        self.has_data_change
    }

    /// The number of files the commit added, whether or not they change the table's data. This
    /// is `None` if the commit records its file changes only in the table's adaptive metadata
    /// tree, which the summary plan does not read.
    pub fn num_added_files(&self) -> Option<i64> {
        self.num_added_files
    }

    /// The number of files the commit removed, whether or not they change the table's data, or
    /// `None` under the same condition as [`num_added_files`](Self::num_added_files).
    pub fn num_removed_files(&self) -> Option<i64> {
        self.num_removed_files
    }

    /// The name of the operation that wrote the commit, exactly as its writer recorded it, or
    /// `None` if the writer recorded none. The Delta protocol does not define operation names: a
    /// connector that interprets them must know the conventions of the table's writers.
    pub fn operation(&self) -> Option<&str> {
        self.operation.as_deref()
    }

    /// The metrics that the writer of the commit recorded for its operation, by name and
    /// unparsed, or `None` if the writer recorded none. A metric recorded as null is omitted. Like
    /// [`operation`](Self::operation), the names and meanings are conventions of the writer.
    ///
    /// The plan reads every metric as a string. A metric recorded as a JSON number or boolean
    /// is reported as its JSON text. One recorded as a JSON array or object is not a string:
    /// what the plan's scan makes of it is up to the engine that executes the plan, and Kernel's
    /// own JSON readers fail.
    pub fn operation_metrics(&self) -> Option<&HashMap<String, String>> {
        self.operation_metrics.as_ref()
    }
}

/// The schema and partition columns of a table at one commit.
#[derive(Debug, Clone)]
struct TableShape {
    schema: SchemaRef,
    partition_columns: Arc<[String]>,
    /// The column mapping mode in effect, or `None` while the table's protocol is unknown: column
    /// mapping needs protocol support.
    column_mapping_mode: Option<ColumnMappingMode>,
}

impl TableShape {
    fn try_new(metadata: &Metadata, table_config: Option<&TableConfiguration>) -> Result<Self> {
        let schema = match table_config {
            Some(table_config) => table_config.logical_schema(),
            None => Arc::new(metadata.parse_schema()?),
        };
        Ok(Self {
            schema,
            partition_columns: metadata.partition_columns().into(),
            column_mapping_mode: table_config.map(TableConfiguration::column_mapping_mode),
        })
    }

    /// Whether column mapping is in effect, so that the schema's fields carry column mapping IDs.
    fn has_column_mapping(&self) -> bool {
        self.column_mapping_mode
            .is_some_and(|mode| mode != ColumnMappingMode::None)
    }

    /// The changes of the commit that turned `previous` into this shape, or `None` if they are
    /// unknown: without the column mapping mode before the commit, Kernel knows neither whether
    /// the commit changed the mode nor how to match the fields.
    ///
    /// `sets_metadata` tells whether the commit set the table's metadata. A commit that only
    /// sets the protocol leaves the schema as it is, but it can put the column mapping mode that
    /// the metadata asks for into effect. Fields are matched by column mapping ID only if both
    /// shapes have column mapping.
    fn changes_since(
        &self,
        previous: &Self,
        sets_metadata: bool,
    ) -> KernelResult<Option<Vec<SchemaChange>>> {
        let mode_change = match (previous.column_mapping_mode, self.column_mapping_mode) {
            (None, _) => return Ok(None),
            (Some(old_mode), Some(new_mode)) if old_mode != new_mode => {
                Some(SchemaChange::ChangeColumnMappingMode { old_mode, new_mode })
            }
            _ => None,
        };
        let mut changes = Vec::from_iter(mode_change);
        if sets_metadata {
            changes.extend(diff_schemas(
                &previous.schema,
                &self.schema,
                previous.has_column_mapping() && self.has_column_mapping(),
            )?);
        }
        Ok(Some(changes))
    }
}

/// The protocol, metadata, and derived table state in effect while walking the commits in
/// ascending version order.
struct TableState {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    /// Present iff both the protocol and the metadata are known.
    table_config: Option<TableConfiguration>,
    /// Present iff the metadata is known.
    schema: Option<TableShape>,
    /// Whether a commit has been applied. Until then, the state is the one after the first
    /// commit, if it is known at all: a start snapshot already includes that commit.
    has_advanced: bool,
}

impl TableState {
    fn new(start_configuration: Option<&TableConfiguration>) -> Result<Self> {
        let schema = start_configuration
            .map(|config| TableShape::try_new(config.metadata(), Some(config)))
            .transpose()?;
        Ok(Self {
            protocol: start_configuration.map(|config| config.protocol().clone()),
            metadata: start_configuration.map(|config| config.metadata().clone()),
            table_config: start_configuration.cloned(),
            schema,
            has_advanced: false,
        })
    }

    /// Applies the commit described by `row` and returns its summary.
    fn advance(
        &mut self,
        table_root: &Url,
        version: Version,
        last_modified: i64,
        row: SummaryRow,
    ) -> Result<CommitSummary> {
        for (action, count) in [
            (PROTOCOL_NAME, row.num_protocol_actions),
            (METADATA_NAME, row.num_metadata_actions),
            // Without a single commitInfo, the commit's timestamp and operation are ambiguous.
            (COMMIT_INFO_NAME, row.num_commit_info_actions),
        ] {
            require!(
                count <= 1,
                KernelError::InvalidProtocol(format!(
                    "found {count} {action} actions, but a commit may hold at most one"
                ))
                .into()
            );
        }

        let sets_protocol = row.protocol.is_some();
        let sets_metadata = row.metadata.is_some();
        // The table shape before the commit, if the commit can change it and it is known.
        let mut previous = None;
        if sets_protocol || sets_metadata {
            self.protocol = row.protocol.or(self.protocol.take());
            self.metadata = row.metadata.or(self.metadata.take());
            self.table_config = match (&self.protocol, &self.metadata) {
                (Some(protocol), Some(metadata)) => Some(TableConfiguration::try_new(
                    metadata.clone(),
                    protocol.clone(),
                    table_root.clone(),
                    version,
                )?),
                _ => None,
            };
            ensure_commit_readable(self.table_config.as_ref(), self.protocol.as_ref())?;
            let current = self
                .metadata
                .as_ref()
                .map(|metadata| TableShape::try_new(metadata, self.table_config.as_ref()))
                .transpose()?;
            // The table shape before the first commit is unknown.
            previous = std::mem::replace(&mut self.schema, current).filter(|_| self.has_advanced);
        }
        self.has_advanced = true;

        let timestamp = resolve_commit_timestamp(
            version,
            last_modified,
            self.table_config.as_ref(),
            row.in_commit_timestamp,
        )?;
        let schema_changes = match (&previous, &self.schema) {
            _ if !sets_protocol && !sets_metadata => Some(Vec::new()),
            (Some(previous), Some(current)) => current.changes_since(previous, sets_metadata)?,
            // The table's schema before or after the commit is unknown.
            _ => None,
        };
        // A manifest commit whose content root is at its own version records its file changes
        // only in the adaptive metadata tree. TODO(#2866): count them from the tree.
        let files_only_in_tree = row
            .content_root_version
            .is_some_and(|root| Version::try_from(root) == Ok(version));
        let files_in_commit = !files_only_in_tree;
        // On a table with adaptive metadata, the commit records whether it changes data, and
        // that takes precedence over its files. The protocol alone tells whether the table has
        // adaptive metadata. While the protocol is unknown, a commit that records whether it
        // changes data is taken at its word.
        let adaptive_metadata = self
            .protocol
            .as_ref()
            .map(|protocol| protocol.has_table_feature(&TableFeature::AdaptiveMetadataPreview));
        let files_change_data = files_in_commit.then_some(row.num_data_change_files > 0);
        let has_data_change = match adaptive_metadata {
            Some(true) => row.data_change,
            Some(false) => files_change_data,
            None => row.data_change.or(files_change_data),
        };
        Ok(CommitSummary {
            version,
            timestamp,
            table: self.schema.clone(),
            schema_changes,
            has_data_change,
            num_added_files: files_in_commit.then_some(row.num_added_files),
            num_removed_files: files_in_commit.then_some(row.num_removed_files),
            operation: row.operation,
            operation_metrics: row.operation_metrics,
        })
    }
}

/// One row of the summary plan: what the commit file of one version contains.
#[derive(Debug, Default)]
struct SummaryRow {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    in_commit_timestamp: Option<i64>,
    operation: Option<String>,
    operation_metrics: Option<HashMap<String, String>>,
    /// Whether the commit changes data according to its `commitInfo`.
    data_change: Option<bool>,
    /// The content root version of the manifest commit that the `commitInfo` points at.
    content_root_version: Option<i64>,
    num_protocol_actions: i64,
    num_metadata_actions: i64,
    num_commit_info_actions: i64,
    num_added_files: i64,
    num_removed_files: i64,
    num_data_change_files: i64,
}

/// Reads the rows of the summary plan along with their commit versions.
#[derive(Default)]
struct SummaryRowVisitor {
    rows: Vec<(Version, SummaryRow)>,
}

impl RowVisitor for SummaryRowVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static LEAVES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| SUMMARY_SCHEMA.leaves(None));
        LEAVES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        // The getters follow the leaves of `SUMMARY_SCHEMA`: the version, the leaves of the
        // protocol and of the metadata, then the leaves that the last pattern below names.
        let wrong_getters = || {
            KernelError::internal_error(format!(
                "Wrong number of commit summary getters: {}",
                getters.len()
            ))
        };
        let [version, rest @ ..] = getters else {
            return Err(wrong_getters());
        };
        let (protocol, rest) = rest
            .split_at_checked(PROTOCOL_LEAVES.as_ref().0.len())
            .ok_or_else(wrong_getters)?;
        let (metadata, rest) = rest
            .split_at_checked(METADATA_LEAVES.as_ref().0.len())
            .ok_or_else(wrong_getters)?;
        let [
            in_commit_timestamp,
            operation,
            operation_metrics,
            data_change,
            content_root_version,
            num_protocol_actions,
            num_metadata_actions,
            num_commit_info_actions,
            num_added_files,
            num_removed_files,
            num_data_change_files,
        ] = rest
        else {
            return Err(wrong_getters());
        };

        for row in 0..row_count {
            let version: i64 = version.get(row, VERSION)?;
            let version = Version::try_from(version).map_err(|_| {
                KernelError::CommitRange(format!(
                    "Commit summary result has a row for the invalid version {version}"
                ))
            })?;
            let in_commit = |error| with_version_context(version, error);
            let summary_row = SummaryRow {
                protocol: visit_protocol_at(row, protocol).map_err(in_commit)?,
                metadata: visit_metadata_at(row, metadata).map_err(in_commit)?,
                in_commit_timestamp: in_commit_timestamp.get_opt(row, IN_COMMIT_TIMESTAMP)?,
                operation: operation.get_opt(row, OPERATION)?,
                operation_metrics: operation_metrics.get_opt(row, OPERATION_METRICS)?,
                data_change: data_change.get_opt(row, DATA_CHANGE)?,
                content_root_version: content_root_version.get_opt(row, CONTENT_ROOT_VERSION)?,
                num_protocol_actions: num_protocol_actions.get(row, NUM_PROTOCOL_ACTIONS)?,
                num_metadata_actions: num_metadata_actions.get(row, NUM_METADATA_ACTIONS)?,
                num_commit_info_actions: num_commit_info_actions
                    .get(row, NUM_COMMIT_INFO_ACTIONS)?,
                num_added_files: num_added_files.get(row, NUM_ADDED_FILES)?,
                num_removed_files: num_removed_files.get(row, NUM_REMOVED_FILES)?,
                num_data_change_files: num_data_change_files.get(row, NUM_DATA_CHANGE_FILES)?,
            };
            self.rows.push((version, summary_row));
        }
        Ok(())
    }
}

const VERSION: &str = "version";
const IN_COMMIT_TIMESTAMP: &str = "inCommitTimestamp";
const OPERATION: &str = "operation";
const OPERATION_METRICS: &str = "operationMetrics";
const LAST_MANIFEST_COMMIT: &str = "lastManifestCommit";
const CONTENT_ROOT_VERSION: &str = "contentRootVersion";
const NUM_PROTOCOL_ACTIONS: &str = "numProtocolActions";
const NUM_METADATA_ACTIONS: &str = "numMetadataActions";
const NUM_COMMIT_INFO_ACTIONS: &str = "numCommitInfoActions";
const NUM_ADDED_FILES: &str = "numAddedFiles";
const NUM_REMOVED_FILES: &str = "numRemovedFiles";
const NUM_DATA_CHANGE_FILES: &str = "numDataChangeFiles";
const ADD_PATH: &str = "addPath";
const REMOVE_PATH: &str = "removePath";
const DATA_CHANGE: &str = "dataChange";

/// The actions and fields the summary plan reads from each commit file, tagged with the file's
/// version.
static SCAN_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (commit_info_field()),
    nullable ADD_NAME: {
        nullable "path": STRING,
        nullable "dataChange": BOOLEAN,
    },
    nullable REMOVE_NAME: {
        nullable "path": STRING,
        nullable "dataChange": BOOLEAN,
    },
    not_null VERSION: LONG,
};

/// The scanned actions with the file-level facts in columns of their own, which the aggregate
/// counts.
static ACTION_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null VERSION: LONG,
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (commit_info_field()),
    nullable ADD_PATH: STRING,
    nullable REMOVE_PATH: STRING,
    nullable DATA_CHANGE: BOOLEAN,
};

/// The output of the summary plan: one row per commit.
static SUMMARY_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null VERSION: LONG,
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (commit_info_field()),
    not_null NUM_PROTOCOL_ACTIONS: LONG,
    not_null NUM_METADATA_ACTIONS: LONG,
    not_null NUM_COMMIT_INFO_ACTIONS: LONG,
    not_null NUM_ADDED_FILES: LONG,
    not_null NUM_REMOVED_FILES: LONG,
    not_null NUM_DATA_CHANGE_FILES: LONG,
};

/// The `commitInfo` fields a summary needs.
fn commit_info_field() -> StructField {
    StructField::nullable(
        COMMIT_INFO_NAME,
        schema! {
            nullable IN_COMMIT_TIMESTAMP: LONG,
            nullable OPERATION: STRING,
            nullable OPERATION_METRICS: { STRING => nullable STRING },
            nullable DATA_CHANGE: BOOLEAN,
            nullable LAST_MANIFEST_COMMIT: {
                nullable CONTENT_ROOT_VERSION: LONG,
            },
        },
    )
}

/// Builds the plan that reads `files`, each tagged with its commit version, and aggregates their
/// actions to one row per version with schema [`SUMMARY_SCHEMA`].
fn summary_plan(files: Vec<ScanFile>) -> KernelResult<Plan> {
    // An action row holds either an add or a remove, so the present one decides.
    let changes_data = Predicate::from_expr(Expression::coalesce([
        col!("add.dataChange"),
        col!("remove.dataChange"),
    ]));
    let actions = Expression::struct_from([
        col!(VERSION),
        col!(PROTOCOL_NAME),
        col!(METADATA_NAME),
        col!(COMMIT_INFO_NAME),
        col!("add.path"),
        col!("remove.path"),
        Expression::case_when([(changes_data, lit(true))], null_lit(DataType::BOOLEAN)),
    ]);
    PlanBuilder::scan_json(files, &[VERSION], SCAN_SCHEMA.clone())?
        .project(actions, ACTION_SCHEMA.clone())?
        .aggregate_by([column_name!(VERSION)], |aggregate| {
            aggregate
                .first_non_null(column_name!(PROTOCOL_NAME))
                .first_non_null(column_name!(METADATA_NAME))
                .first_non_null(column_name!(COMMIT_INFO_NAME))
                .aggregate_as(
                    Agg::count(column_name!(PROTOCOL_NAME)),
                    NUM_PROTOCOL_ACTIONS,
                )
                .aggregate_as(
                    Agg::count(column_name!(METADATA_NAME)),
                    NUM_METADATA_ACTIONS,
                )
                .aggregate_as(
                    Agg::count(column_name!(COMMIT_INFO_NAME)),
                    NUM_COMMIT_INFO_ACTIONS,
                )
                .aggregate_as(Agg::count(column_name!(ADD_PATH)), NUM_ADDED_FILES)
                .aggregate_as(Agg::count(column_name!(REMOVE_PATH)), NUM_REMOVED_FILES)
                .aggregate_as(Agg::count(column_name!(DATA_CHANGE)), NUM_DATA_CHANGE_FILES)
        })?
        .build()
}
