use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use delta_kernel_derive::internal_api;

#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, LastManifestCommit};
use crate::actions::{
    CommitInfo, DomainMetadata, MAX_VALUES, MIN_VALUES, Metadata, NULL_COUNT, NUM_RECORDS,
    Protocol, SetTransaction, TIGHT_BOUNDS,
};
use crate::committer::{
    CommitMetadata, CommitProtocolMetadata, CommitType, Committer, WinningCommits,
};
use crate::crc::{CrcDelta, FileStatsDelta};
use crate::error::KernelError;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::path::{LogRoot, ParsedLogPath};
use crate::scan::log_replay::STATS_NAME;
pub use crate::scan::log_replay::{CLUSTERING_PROVIDER_NAME, TAGS_NAME};
use crate::schema::void_utils::validate_schema_for_write;
use crate::schema::{
    ColumnDefault, GeneratedColumn, MapType, SchemaRef, SchemaStructPatchBuilder, StructField,
    lazy_schema_ref,
};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
#[cfg(feature = "check-constraints-in-dev")]
use crate::table_properties::CheckConstraint;
use crate::utils::{PhantomType, require};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::version_as_i64;
use crate::{DataType, Engine, EngineData, FileMeta, KernelResult, KernelResultIterator, Version};

#[cfg(feature = "internal-api")]
pub mod builder;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod builder;

#[cfg(feature = "internal-api")]
pub mod create_table;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod create_table;

#[cfg(feature = "internal-api")]
pub mod data_layout;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod data_layout;

use builder::TransactionBuilderState;
pub use builder::update_table::UpdateTableTransactionBuilder;
pub use builder::{TransactionBuilder, TransactionType};
use commit_info::collect_operation_metadata;
mod bound_write_context;
mod commit_info;
#[cfg(feature = "declarative-plans")]
mod declarative;
#[cfg(feature = "declarative-plans")]
mod prepared;
#[cfg(feature = "declarative-plans")]
pub use prepared::{CommitAttempt, PreparedTransaction, PreparedTransactionCommitResult};
mod domain_metadata;
mod exec_mode;
mod imperative;
mod operation;
pub(crate) use operation::CommitOperation;
pub use operation::UpdateTableOperation;
#[cfg(feature = "adaptive-metadata-in-dev")]
mod leaf_writer;
#[cfg(feature = "adaptive-metadata-in-dev")]
mod manifest_commit_state;
#[cfg(feature = "adaptive-metadata-in-dev")]
mod root_manifest_file;
pub(crate) mod schema_evolution;
#[internal_api]
#[cfg_attr(not(feature = "internal-api"), allow(unused_imports))]
pub(crate) use schema_evolution::SchemaOperation;
mod state;
#[cfg(feature = "internal-api")]
pub mod stats_verifier;
#[cfg(not(feature = "internal-api"))]
mod stats_verifier;
mod update_table;
mod write_state;

pub use bound_write_context::BoundWriteContext;
#[cfg(feature = "declarative-plans")]
pub use exec_mode::Declarative;
pub use exec_mode::{ExecutionMode, Imperative};
#[cfg(feature = "adaptive-metadata-in-dev")]
#[cfg_attr(not(feature = "internal-api"), allow(unused_imports))]
#[internal_api]
pub(crate) use leaf_writer::{LeafNodeWriter, LeafNodeWriterResult};
#[cfg(feature = "adaptive-metadata-in-dev")]
#[cfg_attr(not(feature = "internal-api"), allow(unused_imports))]
#[internal_api]
pub(crate) use manifest_commit_state::ManifestCommitState;
#[cfg(feature = "adaptive-metadata-in-dev")]
use root_manifest_file::RootManifestFile;
pub use write_state::{BoundWriteContextBuilder, RowTrackingMetadataColumns, WriteState};

/// Type alias for an iterator of [`EngineData`] results.
pub(crate) type EngineDataResultIterator<'a> =
    Box<dyn KernelResultIterator<Box<dyn EngineData>> + 'a>;

/// The static instance referenced by [`add_files_schema`] that doesn't contain the dataChange
/// column.
pub(crate) static MANDATORY_ADD_FILE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null "path": STRING,
    not_null "partitionValues": { STRING => nullable STRING },
    not_null "size": LONG,
    not_null "modificationTime": LONG,
};

/// Returns a reference to the mandatory fields in an add action.
///
/// Note this does not include "dataChange" which is a required field but
/// should be set on the transaction level. Getting the full schema
/// can be done with [`Transaction::add_files_schema`].
pub(crate) fn mandatory_add_file_schema() -> &'static SchemaRef {
    &MANDATORY_ADD_FILE_SCHEMA
}

/// The base schema for add file metadata, referenced by [`Transaction::add_files_schema`].
///
/// The `stats` field represents the minimum structure. The actual stats written by
/// `DefaultEngine::write_parquet` include additional fields computed from the data:
/// - `nullCount`: nested struct mirroring the data schema (all fields LONG)
/// - `minValues`: nested struct with min/max eligible column types
/// - `maxValues`: nested struct with min/max eligible column types
///
/// The nested structures within nullCount/minValues/maxValues depend on the table's data schema
/// and which columns have statistics enabled. Use [`Transaction::stats_schema`] to get the
/// expected stats schema for a specific table.
pub(crate) static BASE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(mandatory_add_file_schema().fields().cloned()),
    nullable STATS_NAME: {
        nullable NUM_RECORDS: LONG,
        // nullCount, minValues, maxValues are dynamic based on data schema. Empty struct
        // placeholders indicate these fields exist but their inner structure depends on the
        // table schema and stats column configuration.
        nullable NULL_COUNT: {},
        nullable MIN_VALUES: {},
        nullable MAX_VALUES: {},
        nullable TIGHT_BOUNDS: BOOLEAN,
    },
};

// The optional per-file add-files columns. Each has the type of the Add action field of the same
// name and is copied into each Add action unchanged.
static ADD_FILE_TAGS_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::nullable(
        TAGS_NAME,
        MapType::new(DataType::STRING, DataType::STRING, true),
    )
});
static ADD_FILE_CLUSTERING_PROVIDER_FIELD: LazyLock<StructField> =
    LazyLock::new(|| StructField::nullable(CLUSTERING_PROVIDER_NAME, DataType::STRING));

static TAGS_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(BASE_ADD_FILES_SCHEMA.fields().cloned()),
    (&ADD_FILE_TAGS_FIELD),
};
static CLUSTERING_PROVIDER_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(BASE_ADD_FILES_SCHEMA.fields().cloned()),
    (&ADD_FILE_CLUSTERING_PROVIDER_FIELD),
};
static TAGS_AND_CLUSTERING_PROVIDER_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(BASE_ADD_FILES_SCHEMA.fields().cloned()),
    (&ADD_FILE_TAGS_FIELD),
    (&ADD_FILE_CLUSTERING_PROVIDER_FIELD),
};

/// The add-files schema with the optional columns present in a batch.
fn add_files_schema_with(tags: bool, clustering_provider: bool) -> &'static SchemaRef {
    match (tags, clustering_provider) {
        (false, false) => &BASE_ADD_FILES_SCHEMA,
        (true, false) => &TAGS_ADD_FILES_SCHEMA,
        (false, true) => &CLUSTERING_PROVIDER_ADD_FILES_SCHEMA,
        (true, true) => &TAGS_AND_CLUSTERING_PROVIDER_ADD_FILES_SCHEMA,
    }
}

/// Extend a schema with row tracking columns and return a new SchemaRef.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_row_tracking_cols(schema: &SchemaRef) -> KernelResult<SchemaRef> {
    let patch = SchemaStructPatchBuilder::new()
        .append(StructField::nullable("baseRowId", DataType::LONG))
        .append(StructField::nullable(
            "defaultRowCommitVersion",
            DataType::LONG,
        ));
    Ok(Arc::new(patch.build(schema)?))
}

/// The row-tracking-augmented add-file write-metadata schema: [`BASE_ADD_FILES_SCHEMA`] extended
/// with the row-tracking columns.
#[cfg(feature = "adaptive-metadata-in-dev")]
pub(crate) fn augmented_write_metadata_schema() -> KernelResult<SchemaRef> {
    with_row_tracking_cols(&BASE_ADD_FILES_SCHEMA)
}

/// Marker type for transactions that update existing tables.
///
/// This is the default state for [`Transaction`] and supports schema changes as well as file
/// additions, removals, deletion-vector updates, and blind append semantics.
#[derive(Debug)]
pub struct UpdateTable;

/// Marker type for create-table transactions.
///
/// Transactions in this state support file additions but not file removals, deletion-vector
/// updates, or domain metadata removals.
#[derive(Debug)]
pub struct CreateTable;

/// A transaction represents an in-progress write to a table. After creating a transaction, changes
/// to the table may be staged via the transaction methods before calling `commit` to commit the
/// changes to the table.
///
/// The type parameter `STATE` controls which table operations are available:
/// - [`UpdateTable`] (default): Full API for modifying existing tables.
/// - [`CreateTable`]: Restricted API for table creation (see
///   [`CreateTableTransaction`](create_table::CreateTableTransaction)).
///
/// The independent type parameter `MODE` selects the [`ExecutionMode`], which provides an
/// abstraction over 1) how staged changes are represented and 2) how to inspect the changes. The
/// default, [`Imperative`], stages [`EngineData`] and provides [`commit`](Self::commit).
/// With the `declarative-plans` feature, `Declarative` stages relation handles instead.
///
/// Both states support file additions. Schema-changing transactions use [`UpdateTable`] rather
/// than a separate schema-only state, allowing schema and file changes in the same transaction.
/// The builder validates schema intent, and commit validates the staged actions. `ALTER TABLE`
/// commits reject staged data-file batches and manifest writes.
///
/// Configure schema and data-change intent on the builder. Built transactions do not expose
/// setters for either, including when the `internal-api` feature is enabled.
///
/// # Examples
///
/// ```rust,ignore
/// let txn = snapshot
///     .update_table(UpdateTableOperation::Write, "my-app/1.0", /* data_change */ true)
///     .build(&engine, Box::new(FileSystemCommitter::new()))?;
/// txn.commit(&engine)?;
/// ```
///
/// Schema changes cannot be added to a built transaction:
///
/// ```compile_fail
/// # use delta_kernel::transaction::Transaction;
/// fn change_schema(txn: Transaction) {
///     let _ = txn.with_schema_changes(Vec::new());
/// }
/// ```
///
/// The data-change flag cannot be overridden on a built transaction:
///
/// ```compile_fail,E0599
/// # use delta_kernel::transaction::Transaction;
/// fn change_data_change<STATE>(txn: Transaction<STATE>) {
///     let _ = txn.with_data_change(false);
/// }
/// ```
///
/// Blind-append intent cannot be added to a built transaction:
///
/// ```compile_fail
/// # use delta_kernel::transaction::Transaction;
/// fn change_blind_append(txn: Transaction) {
///     let _ = txn.with_blind_append();
/// }
/// ```
///
/// The operation cannot be changed on a built transaction:
///
/// ```compile_fail
/// # use delta_kernel::transaction::{Transaction, UpdateTableOperation};
/// fn change_operation(txn: Transaction) {
///     let _ = txn.with_operation(UpdateTableOperation::Write);
/// }
/// ```
pub struct Transaction<STATE = UpdateTable, MODE: ExecutionMode = Imperative> {
    state: TransactionState,
    staged_data_changes: MODE,
    _state: PhantomType<STATE>,
}

/// Owned context moved from input collection into a prepared commit.
/// Staged inputs and prepared action relations live on their respective lifecycle objects.
struct TransactionState {
    span: tracing::Span,
    // Correlates all metric events emitted by this transaction.
    operation_id: MetricId,
    // Opaque, caller-supplied id recorded on this transaction's commit metric events alongside
    // `operation_id`. Set via `with_correlation_id`; not interpreted by kernel.
    correlation_id: Option<Arc<str>>,
    // The snapshot this transaction is based on. None for CREATE TABLE (no pre-existing table).
    // Use `read_snapshot()` to access; it returns an error if None.
    read_snapshot_opt: Option<SnapshotRef>,
    // The table configuration that this commit will produce. For writes that don't change the
    // config, this is cloned from the read snapshot; when the config changes (e.g. schema
    // evolution), it is constructed separately with the new schema/protocol.
    effective_table_config: TableConfiguration,
    // Whether to emit a Protocol action. True for CREATE TABLE, false otherwise.
    should_emit_protocol: bool,
    // Whether to emit a Metadata action. True for CREATE TABLE and transactions with staged
    // schema changes, false otherwise.
    should_emit_metadata: bool,
    committer: Box<dyn Committer>,
    operation: CommitOperation,
    operation_parameters: Option<HashMap<String, Option<String>>>,
    operation_metrics: Option<HashMap<String, Option<String>>>,
    engine_info: String,
    engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    skip_dedup_validation: bool,
    // NB: hashmap would require either duplicating the appid or splitting SetTransaction
    // key/payload. HashSet requires Borrow<&str> with matching Eq, Ord, and Hash. Plus,
    // HashSet::insert drops the to-be-inserted value without returning the existing one, which
    // would make error messaging unnecessarily difficult. Thus, we keep Vec here and deduplicate
    // in the commit method.
    set_transactions: Vec<SetTransaction>,
    // commit-wide timestamp (in milliseconds since epoch) - used in ICT, `txn` action, etc. to
    // keep all timestamps within the same commit consistent.
    commit_timestamp: i64,
    // User-provided domain metadata additions (via with_domain_metadata API).
    user_domain_metadata_additions: Vec<DomainMetadata>,
    // System-generated domain metadata produced by create-table transforms, e.g. clustering.
    system_domain_metadata_additions: Vec<DomainMetadata>,
    // Row-tracking high-water mark explicitly provided by the caller.
    provided_row_tracking_high_water_mark: Option<i64>,
    // Domain names to remove in this transaction. The configuration values are fetched during
    // commit from the log to preserve the pre-image in tombstones.
    user_domain_removals: Vec<String>,
    // Data-change value used by commit validation, file actions, and transaction metrics.
    data_change: bool,
    // TODO(#2499): Replace this state when Conntector responsibilities encode column-default
    // handling. Whether the connector acknowledged responsibility for applying column
    // defaults.
    column_defaults_acknowledged: bool,
    // Whether the connector acknowledged responsibility for enforcing this table's CHECK
    // constraints.
    #[cfg(feature = "check-constraints-in-dev")]
    check_constraints_acknowledged: bool,
    // Whether the connector acknowledged responsibility for enforcing this table's generated
    // columns.
    generated_columns_acknowledged: bool,
    // Whether the connector acknowledged responsibility for preserving Row IDs and Row Commit
    // Versions.
    row_tracking_preservation_acknowledged: bool,
    // Whether this transaction should be marked as a blind append.
    is_blind_append: bool,
    // The manifest this transaction will write, if any. The two ways of writing it are mutually
    // exclusive, so a single field makes staging both unrepresentable.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    manifest_write: Option<ManifestWrite>,
    // Clustering columns from domain metadata. Only populated if the ClusteredTable feature is
    // enabled. Used for determining which columns require statistics collection. Expected to be
    // physical column names.
    physical_clustering_columns: Option<Vec<ColumnName>>,
}

/// The manifest a transaction will write. Root-file and content-tree commits are mutually
/// exclusive ways of writing the manifest, so representing them as one enum makes staging both
/// unrepresentable.
#[cfg(feature = "adaptive-metadata-in-dev")]
enum ManifestWrite {
    /// Caller-supplied root manifest file, staged via
    /// [`with_root_manifest_file`](Transaction::with_root_manifest_file).
    RootFile(RootManifestFile),
    /// In-progress manifest (content-tree) commit, staged via
    /// [`with_manifest_commit`](Transaction::with_manifest_commit).
    Commit(ManifestCommitState),
}

impl<STATE, MODE: ExecutionMode> std::fmt::Debug for Transaction<STATE, MODE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version_info = match &self.state.read_snapshot_opt {
            Some(snap) => format!("{}", snap.version()),
            None => "create_table".to_string(),
        };
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, engine_info: {:?} }}",
            version_info, self.state.engine_info
        ))
    }
}

// =============================================================================
// Shared methods available on ALL transaction types
// =============================================================================
impl<STATE, MODE: ExecutionMode> Transaction<STATE, MODE> {
    /// The effective CHECK constraints of this transaction, sorted by name and then SQL. Empty when
    /// the table declares none.
    ///
    /// Each constraint is a Boolean SQL expression, stored under `delta.constraints.<name>`, that
    /// every row in the table must satisfy. See [`Self::ack_check_constraints`] for what a
    /// connector must do before writing to a table with constraints.
    #[cfg(feature = "check-constraints-in-dev")]
    pub fn check_constraints(&self) -> &[CheckConstraint] {
        &self
            .state
            .effective_table_config
            .table_properties()
            .check_constraints
    }

    /// Acknowledges that the connector enforces this table's CHECK constraints.
    ///
    /// Kernel does not see the rows a connector writes, so it does not evaluate constraints. The
    /// connector must:
    /// - Discover the constraints of this transaction via [`Self::check_constraints`] and parse
    ///   each one's [`raw_sql`](crate::table_properties::CheckConstraint::raw_sql) itself. The SQL
    ///   refers to logical column names.
    /// - Reject every written row, including its partition values, for which a constraint evaluates
    ///   to `false` or `NULL`. This includes files written for an earlier commit attempt, such as
    ///   one that conflicted, and validated against an older table version.
    ///
    /// The acknowledgement covers whatever this transaction commits. Without it,
    /// [`write_state`](Transaction::write_state) and [`commit`](Self::commit) fail whenever
    /// [`Self::check_constraints`] is not empty.
    #[cfg(feature = "check-constraints-in-dev")]
    pub fn ack_check_constraints(&mut self) {
        self.state.check_constraints_acknowledged = true;
    }

    /// The effective [`GeneratedColumn`]s of this transaction, in schema order. Empty when the
    /// table has none, or when its protocol doesn't support the `generatedColumns` writer feature.
    ///
    /// Each generated column's value is defined by a SQL expression over the row's other columns.
    /// See [`Self::ack_generated_columns`] for what a connector must do before writing to a table
    /// with generated columns.
    ///
    /// # Errors
    ///
    /// Returns a schema error if a generated column's `delta.generationExpression` metadata is not
    /// a string.
    pub fn generated_columns(&self) -> KernelResult<Vec<GeneratedColumn>> {
        self.state.effective_table_config.generated_columns()
    }

    /// Acknowledges that the connector enforces this table's generated columns.
    ///
    /// Only the connector sees the rows it writes, so the connector evaluates generation
    /// expressions. It must:
    /// - Discover the generated columns of this transaction via [`Self::generated_columns`] and
    ///   parse each one's [`raw_sql`](GeneratedColumn::raw_sql) itself. The SQL refers to logical
    ///   column names.
    /// - Compute each generated column from its expression when the write omits the column.
    /// - Make every written row, including its partition values, satisfy `(<value> <=> <generation
    ///   expression>) IS TRUE` for each generated column. `<=>` is null-safe equality, so a `NULL`
    ///   value matches a `NULL` expression result. This includes files written for an earlier
    ///   commit attempt, such as one that conflicted, and validated against an older table version.
    ///
    /// The acknowledgement covers whatever this transaction commits. Without it,
    /// [`write_state`](Transaction::write_state) and [`commit`](Self::commit) fail whenever this
    /// transaction has effective generated columns.
    pub fn ack_generated_columns(&mut self) {
        self.state.generated_columns_acknowledged = true;
    }

    /// Replaces operation metrics recorded in `commitInfo`.
    ///
    /// Present values must already be stringified as expected in table history. `None` writes a
    /// null map value. When unset, `operationMetrics` is omitted. An explicitly empty map is
    /// written as `{}`. A later call replaces the complete map, and the last value wins when a key
    /// occurs more than once.
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state.operation_metrics = Some(collect_operation_metadata(metrics));
        self
    }

    /// Adds a [`SetTransaction`] action for `app_id` and `version`. Returns the updated
    /// transaction.
    ///
    /// The action's `lastUpdated` uses this transaction's commit timestamp. Each application id may
    /// occur only once per transaction, including ids added through the builder. Duplicates are not
    /// checked here; [`Self::commit`] rejects them.
    pub fn with_transaction_id(mut self, app_id: String, version: i64) -> Self {
        let set_transaction =
            SetTransaction::new(app_id, version, Some(self.state.commit_timestamp));
        self.state.set_transactions.push(set_transaction);
        self
    }

    /// Set domain metadata to be written to the Delta log.
    /// Note that each domain can only appear once per transaction. That is, multiple configurations
    /// of the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the commit will
    /// fail (that is, we don't eagerly check domain validity here).
    /// Setting metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata(mut self, domain: String, configuration: String) -> Self {
        self.state
            .user_domain_metadata_additions
            .push(DomainMetadata::new(domain, configuration));
        self
    }

    /// The base schema that the [`Engine`]'s [`ParquetHandler`] is expected to use when reporting
    /// information about a Parquet write operation back to Kernel.
    ///
    /// Concretely, it is the expected schema for [`EngineData`] passed to [`add_files`], as it is
    /// the base for constructing an add_file. Each row represents metadata about a
    /// file to be added to the table. Kernel takes this information and extends it to the full
    /// add_file action schema, adding internal fields (e.g., baseRowID) as necessary.
    ///
    /// The `stats` field contains file-level statistics. The schema returned here shows the base
    /// structure; the actual stats written by `DefaultEngine::write_parquet` include dynamically
    /// computed fields (numRecords, nullCount, minValues, maxValues, tightBounds) based on the
    /// data schema and table configuration. See [`stats_schema`] for the table-specific expected
    /// stats schema.
    ///
    /// The returned schema contains the required metadata columns. Batches passed to [`add_files`]
    /// may append nullable [`tags`](TAGS_NAME) (`map<string, string>`, with nullable values) and/or
    /// [`clusteringProvider`](CLUSTERING_PROVIDER_NAME) (`string`) columns, in that order.
    /// Each value is copied into the file's Add action. An absent column or null value leaves
    /// the field unset; batches may differ in which optional columns they include.
    /// A non-null `clusteringProvider` requires the `clustering` writer feature and must name the
    /// implementation that clustered the file.
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    /// [`ParquetHandler`]: crate::ParquetHandler
    /// [`stats_schema`]: Transaction::stats_schema
    pub fn add_files_schema(&self) -> &'static SchemaRef {
        &BASE_ADD_FILES_SCHEMA
    }
}

// =============================================================================
// Shared data file methods
// =============================================================================
impl<STATE, MODE: ExecutionMode> Transaction<STATE, MODE> {
    // TODO(#2499): Remove this API when Engine responsibilities encode column-default handling.
    /// Acknowledges that the connector applies column defaults before writing data files.
    ///
    /// Call this before requesting write state for a table that enables the
    /// `allowColumnDefaults` feature and declares at least one column default. The connector must
    /// materialize every omitted column's default itself; this method records that responsibility
    /// but does not apply any defaults. Without this acknowledgement, write-state creation fails
    /// with an error.
    pub fn ack_column_defaults(&mut self) {
        self.state.column_defaults_acknowledged = true;
    }

    /// Returns the expected schema for file statistics.
    ///
    /// The schema structure is derived from table configuration:
    /// - `delta.dataSkippingStatsColumns`: Explicit column list (if set)
    /// - `delta.dataSkippingNumIndexedCols`: Column count limit (default 32)
    /// - Partition columns: Always excluded
    ///
    /// The returned schema has the following structure:
    /// ```ignore
    /// {
    ///   numRecords: long,
    ///   nullCount: { ... },   // Nested struct mirroring data schema, all fields LONG
    ///   minValues: { ... },   // Nested struct, only min/max eligible types
    ///   maxValues: { ... },   // Nested struct, only min/max eligible types
    ///   tightBounds: boolean,
    /// }
    /// ```
    ///
    /// Engines should collect statistics matching this schema structure when writing files.
    ///
    /// Per the Delta protocol, required columns (e.g. clustering columns) are always included
    /// in statistics, regardless of `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols`
    /// settings.
    #[allow(unused)]
    pub fn stats_schema(&self) -> KernelResult<SchemaRef> {
        self.state
            .effective_table_config
            .stats_schema_builder()
            .with_required_physical_columns(self.state.physical_clustering_columns.as_deref())
            .build()
    }

    /// Returns the list of column names that should have statistics collected.
    ///
    /// This returns leaf column paths as [`ColumnName`] objects. Each `ColumnName`
    /// stores path components separately (e.g., `column_name!("nested.field")`).
    /// See [`ColumnName`'s `Display` implementation][ColumnName#impl-Display-for-ColumnName]
    /// for details on string formatting and escaping.
    ///
    /// Engines can use this to determine which columns need stats during writes.
    ///
    /// Per the Delta protocol, clustering columns are always included in statistics,
    /// regardless of `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols` settings.
    #[allow(unused)]
    pub fn stats_columns(&self) -> Vec<ColumnName> {
        self.state
            .effective_table_config
            .physical_stats_column_names(self.state.physical_clustering_columns.as_deref())
    }

    /// Returns the logical partition column names for this table.
    pub fn logical_partition_columns(&self) -> &[String] {
        self.state
            .effective_table_config
            .logical_partition_columns()
    }

    // TODO(#2630): Expose nested column defaults through the transaction API.
    /// Returns the column default for every top-level column in this table's logical schema that
    /// declares one, keyed by logical column name.
    ///
    /// Connectors use this to discover which columns have defaults, then call
    /// [`ColumnDefault::to_scalar`] on each (or fall back to [`ColumnDefault::raw_sql`] when the
    /// kernel cannot parse the default) to materialize the column before writing. After handling
    /// every omitted column, call [`ack_column_defaults`](Self::ack_column_defaults) before
    /// requesting write state.
    ///
    /// Keys are `String` rather than [`ColumnName`] because the kernel currently surfaces defaults
    /// only for top-level columns, consistent with partition columns. This is a kernel limitation,
    /// not a protocol one.
    ///
    /// Malformed defaults (a non-string `CURRENT_DEFAULT`, or a non-`NULL` default on a Variant
    /// column) are rejected eagerly at snapshot load (when constructing the
    /// [`TableConfiguration`]), so by the time this runs the metadata is already validated.
    /// Orphaned metadata (a `CURRENT_DEFAULT` without the `allowColumnDefaults` writer feature)
    /// is tolerated at table load but is not surfaced through this method.
    ///
    /// # Errors
    ///
    /// Propagates any error from [`StructField::column_default`].
    pub fn top_level_column_defaults(&self) -> KernelResult<HashMap<String, ColumnDefault<'_>>> {
        if !self
            .state
            .effective_table_config
            .is_feature_enabled(&TableFeature::AllowColumnDefaults)
        {
            tracing::info!(
                "allowColumnDefaults is not enabled; the schema may contain orphaned column-default metadata"
            );
            return Ok(HashMap::new());
        }
        let mut defaults = HashMap::new();
        for field in self
            .state
            .effective_table_config
            .logical_schema_ref()
            .fields()
        {
            if let Some(column_default) = field.column_default()? {
                defaults.insert(field.name().clone(), column_default);
            }
        }
        Ok(defaults)
    }

    /// Validates that the table's logical schema supports data writes.
    ///
    /// Called by [`write_state`](Self::write_state), before any Parquet is written, so connectors
    /// fail fast when the schema contains unsupported data types or void placements that cannot
    /// produce valid files.
    ///
    /// We perform this same check during [`commit`](Transaction::commit) as defense-in-depth, for
    /// callers that directly use [`add_files`](Transaction::add_files) without going through
    /// [`write_state`](Self::write_state)
    fn validate_for_data_write(&self) -> KernelResult<()> {
        validate_schema_for_write(&self.state.effective_table_config.logical_schema())
    }

    /// Creates the table-wide state needed to write data files.
    ///
    /// The returned shared state can create [`BoundWriteContext`] instances in this process,
    /// or it can be encoded and transported to distributed writers. Each context retains a
    /// reference to the same immutable state. All table-wide write validation runs before the
    /// state is returned so a writer does not begin producing files for a table that kernel cannot
    /// write to safely. Use [`WriteState::write_context_builder`] to bind values for each partition
    /// being written.
    ///
    /// The state captures the transaction configuration when this method is called. If the
    /// transaction is subsequently modified, call this method again to capture the updated
    /// configuration.
    ///
    /// Returns an error if the table has an empty or unsupported schema, or if the table declares
    /// column defaults, CHECK constraints, or generated columns that the connector has not
    /// acknowledged.
    pub fn write_state(&self) -> KernelResult<Arc<WriteState>> {
        self.state.ensure_schema_non_empty_for_write_state()?;
        self.state.ensure_column_defaults_acknowledged()?;
        #[cfg(feature = "check-constraints-in-dev")]
        self.state.ensure_check_constraints_acknowledged()?;
        self.state.ensure_generated_columns_acknowledged()?;
        self.validate_for_data_write()?;
        // The effective table configuration can change while building a transaction, so this
        // state must be derived on demand rather than cached on the transaction. TODO(#3149):
        // revisit caching if transaction construction becomes immutable.
        Ok(Arc::new(WriteState::new(
            &self.state.effective_table_config,
            self.stats_columns(),
        )))
    }
}

// =============================================================================
// Internal methods available on ALL transaction types (used by commit path)
// =============================================================================
/// In-memory representation of non-file commit actions, built from the transaction.
struct NonfileCommitActions {
    commit_version: Version,
    kernel_commit_info: CommitInfo,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    set_transactions: Vec<SetTransaction>,
}

/// Commit state used to build the post-commit CRC and snapshot.
struct PostCommitInputs {
    commit_info: CommitInfo,
    dm_changes: Vec<DomainMetadata>,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint_action: Option<CheckpointAction>,
}

impl<STATE, MODE: ExecutionMode> Transaction<STATE, MODE> {
    /// Initializes shared transaction state from builder intent.
    fn new(
        common_state: TransactionBuilderState,
        effective_table_config: TableConfiguration,
        committer: Box<dyn Committer>,
        operation: CommitOperation,
        span: tracing::Span,
        commit_timestamp: i64,
    ) -> Self {
        let set_transactions = common_state
            .transaction_ids
            .into_iter()
            .map(|(app_id, version)| SetTransaction::new(app_id, version, Some(commit_timestamp)))
            .collect();

        Self {
            state: TransactionState {
                span,
                operation_id: MetricId::new(),
                correlation_id: common_state.correlation_id,
                read_snapshot_opt: None,
                effective_table_config,
                should_emit_protocol: false,
                should_emit_metadata: false,
                committer,
                operation,
                operation_parameters: common_state.operation_parameters,
                operation_metrics: common_state.operation_metrics,
                engine_info: common_state.engine_info,
                engine_commit_info: common_state.engine_commit_info,
                skip_dedup_validation: common_state.skip_dedup_validation,
                set_transactions,
                commit_timestamp,
                user_domain_metadata_additions: common_state.domain_metadata_additions,
                system_domain_metadata_additions: vec![],
                provided_row_tracking_high_water_mark: None,
                user_domain_removals: vec![],
                data_change: common_state.data_change,
                column_defaults_acknowledged: false,
                #[cfg(feature = "check-constraints-in-dev")]
                check_constraints_acknowledged: false,
                generated_columns_acknowledged: false,
                row_tracking_preservation_acknowledged: false,
                is_blind_append: false,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                manifest_write: None,
                physical_clustering_columns: None,
            },
            staged_data_changes: MODE::default(),
            _state: PhantomType::default(),
        }
    }

    #[cfg(test)]
    fn replace_effective_table_config(&mut self, table_config: TableConfiguration) {
        self.state.effective_table_config = table_config;
    }

    fn into_conflicted(self, conflict_version: Version) -> ConflictedTransaction<STATE, MODE> {
        ConflictedTransaction {
            transaction: self,
            conflict_version,
        }
    }

    fn into_retryable(self, error: KernelError) -> RetryableTransaction<STATE, MODE> {
        RetryableTransaction {
            transaction: self,
            error,
        }
    }
}

/// Kernel exposes information about the state of the table that engines might want to use to
/// trigger actions like checkpointing or log compaction. This struct holds that information.
#[derive(Debug)]
pub struct PostCommitStats {
    /// The number of commits since this table has been checkpointed. Note that commit 0 is
    /// considered a checkpoint for the purposes of this computation.
    pub commits_since_checkpoint: u64,
    /// The number of commits since the log has been compacted on this table. Note that a
    /// checkpoint is considered a compaction for the purposes of this computation. Thus this
    /// is really the number of commits since a compaction OR a checkpoint.
    pub commits_since_log_compaction: u64,
}

/// The result of attempting to commit this transaction. If the commit was
/// successful/conflicted/retryable, the result is Ok(CommitResult), otherwise, if a nonrecoverable
/// error occurred, the result is Err(KernelError).
///
/// The commit result can be one of the following:
/// - [`CommitResult::Committed`]: the transaction was successfully committed. [PostCommitStats] and
///   in the future a post-commit snapshot can be obtained from the committed transaction.
/// - [`CommitResult::Conflicted`]: the transaction conflicted with an existing version. This
///   transcation must be rebased before retrying. (currently no rebase APIs exist, caller must
///   create new txn)
/// - [`CommitResult::Retryable`]: a temporary rejection or IO error occurred during the commit.
///   This transaction can be retried without rebasing.
#[derive(Debug)]
#[must_use]
pub enum CommitResult<STATE = UpdateTable, MODE: ExecutionMode = Imperative> {
    /// The transaction was successfully committed.
    Committed(CommittedTransaction),
    /// This transaction conflicted with an existing version (see
    /// [ConflictedTransaction::conflict_version]). The transaction
    /// is returned so the caller can resolve the conflict (along with the version which
    /// conflicted).
    // TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
    // update the transaction to a new version etc.
    Conflicted(ConflictedTransaction<STATE, MODE>),
    /// A temporary rejection or IO error occurred during the commit.
    Retryable(RetryableTransaction<STATE, MODE>),
}

impl<STATE, MODE: ExecutionMode> CommitResult<STATE, MODE> {
    /// Returns true if the commit was successful.
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitResult::Committed(_))
    }
}

impl<STATE: std::fmt::Debug, MODE: ExecutionMode> CommitResult<STATE, MODE> {
    /// Unwraps the [`CommittedTransaction`], panicking if the commit was not successful.
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::panic)]
    pub fn unwrap_committed(self) -> CommittedTransaction {
        match self {
            CommitResult::Committed(c) => c,
            other => panic!("Expected Committed, got: {other:?}"),
        }
    }

    /// Unwraps the post-commit snapshot of the [`CommittedTransaction`], panicking if the
    /// commit was not successful or the post-commit snapshot is missing.
    /// TODO(#2494): Refactor existing tests to use this.
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::panic, clippy::expect_used)]
    pub fn unwrap_post_commit_snapshot(self) -> SnapshotRef {
        self.unwrap_committed()
            .post_commit_snapshot()
            .expect("expected post-commit snapshot")
            .clone()
    }
}

/// This is the result of a successfully committed [Transaction]. One can retrieve the
/// [post_commit_stats], [commit version], and optionally the [post-commit snapshot] from this
/// struct.
///
/// [post_commit_stats]: Self::post_commit_stats
/// [commit version]: Self::commit_version
/// [post-commit snapshot]: Self::post_commit_snapshot
#[derive(Debug)]
pub struct CommittedTransaction {
    /// The version of the table that was just committed.
    commit_version: Version,
    /// The [`PostCommitStats`] for this transaction.
    post_commit_stats: PostCommitStats,
    /// The [`SnapshotRef`] of the table after this transaction was committed.
    ///
    /// This is optional to allow incremental development of new features (e.g., table creation,
    /// transaction retries) without blocking on implementing post-commit snapshot support.
    post_commit_snapshot: Option<SnapshotRef>,
}

impl CommittedTransaction {
    /// The version of the table that was just sucessfully committed
    pub fn commit_version(&self) -> Version {
        self.commit_version
    }

    /// The [`PostCommitStats`] for this transaction
    pub fn post_commit_stats(&self) -> &PostCommitStats {
        &self.post_commit_stats
    }

    /// The [`SnapshotRef`] of the table after this transaction was committed.
    pub fn post_commit_snapshot(&self) -> Option<&SnapshotRef> {
        self.post_commit_snapshot.as_ref()
    }
}

/// This is the result of a conflicted [Transaction]. One can retrieve the [conflict version] from
/// this struct. In the future a rebase API will be provided (issue #1389).
///
/// [conflict version]: Self::conflict_version
#[derive(Debug)]
pub struct ConflictedTransaction<STATE = UpdateTable, MODE: ExecutionMode = Imperative> {
    transaction: Transaction<STATE, MODE>,
    conflict_version: Version,
}

impl<STATE, MODE: ExecutionMode> ConflictedTransaction<STATE, MODE> {
    /// The version attempted commit that yielded a conflict
    pub fn conflict_version(&self) -> Version {
        self.conflict_version
    }

    /// Fetches winning commits starting at the rejected version in one discovery attempt.
    ///
    /// Each call returns owned validated history or an error without saving either on this result.
    /// A failed read preserves the transaction; another explicit call retries discovery without
    /// resubmitting the commit.
    pub fn get_winning_commits(&self, engine: &dyn Engine) -> KernelResult<WinningCommits> {
        self.transaction
            .state
            .get_winning_commits(engine, self.conflict_version)
    }
}

/// A transaction that failed to commit due to a retryable error (e.g. IO error). The transaction
/// can be recovered with `RetryableTransaction::transaction` and retried without rebasing. The
/// associated error can be inspected via `RetryableTransaction::error`.
#[derive(Debug)]
pub struct RetryableTransaction<STATE = UpdateTable, MODE: ExecutionMode = Imperative> {
    /// The transaction that failed to commit due to a retryable error.
    pub transaction: Transaction<STATE, MODE>,
    /// Transient error that caused the commit to fail.
    pub error: KernelError,
}

#[cfg(test)]
mod tests {
    // Manifest-commit and root-manifest tests live in their own file
    // (tests/manifest_commit_tests.rs) but as a submodule of `tests`, so they reuse this
    // module's private helpers (e.g. `create_existing_table_txn`) without widening their
    // visibility.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    mod manifest_commit_tests;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use ::test_utils::get_column;
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::CommitInfo;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{
        ArrayRef, Float64Array, Int32Array, Int64Array, NullArray, StringArray,
    };
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::{CommitResponse, FileSystemCommitter, PublishMetadata};
    use crate::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::expressions::{
        ArrayData, ExpressionStructPatchBuilder, MapData, Scalar, StructData,
    };
    use crate::metrics::{CommitFailureReason, MetricEvent, TableType, TransactionCommitFailure};
    use crate::object_store::ObjectStoreExt as _;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::scan::state_info::tests::RowTrackingState;
    use crate::schema::{ArrayType, MapType, StructType, column_name, schema, schema_ref};
    use crate::table_features::{ColumnMappingMode, TableFeature};
    use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_ROW_TRACKING};
    use crate::transaction::create_table::create_table;
    use crate::transaction::data_layout::DataLayout;
    use crate::transaction::exec_mode::StagedDataChanges;
    use crate::unit_test_utils::{
        CapturingReporter, MockProtocolBuilder, MockTableConfigurationBuilder,
        assert_result_error_with_message, copy_test_table, create_valid_add_file_batch,
        install_thread_local_metrics_reporter, load_test_table, string_array_to_engine_data,
        test_schema_flat, test_schema_nested, test_schema_with_array, test_schema_with_map,
    };
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::unit_test_utils::{
        adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
    };
    use crate::{EvaluationHandler, Expression, KernelResultIterator, Result, Snapshot};

    #[rstest]
    fn transaction_constructor_initializes_all_defaults(
        #[values(false, true)] expected_data_change: bool,
        #[values("", "test-engine")] expected_engine_info: &str,
    ) -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry());
        let expected_span = tracing::info_span!("transaction_constructor_defaults");
        assert!(expected_span.id().is_some());
        let expected_commit_timestamp = 1234;

        let transaction = Transaction::<UpdateTable>::new(
            TransactionBuilderState {
                engine_info: expected_engine_info.into(),
                data_change: expected_data_change,
                ..Default::default()
            },
            snapshot.table_configuration().clone(),
            Box::new(FileSystemCommitter::new()),
            UpdateTableOperation::Write.into(),
            expected_span.clone(),
            expected_commit_timestamp,
        );
        let Transaction {
            state:
                TransactionState {
                    span,
                    operation_id,
                    correlation_id,
                    read_snapshot_opt,
                    effective_table_config,
                    should_emit_protocol,
                    should_emit_metadata,
                    committer,
                    operation,
                    operation_parameters,
                    operation_metrics,
                    engine_info,
                    engine_commit_info,
                    skip_dedup_validation,
                    set_transactions,
                    commit_timestamp,
                    user_domain_metadata_additions,
                    system_domain_metadata_additions,
                    provided_row_tracking_high_water_mark,
                    user_domain_removals,
                    data_change,
                    column_defaults_acknowledged,
                    #[cfg(feature = "check-constraints-in-dev")]
                    check_constraints_acknowledged,
                    generated_columns_acknowledged,
                    row_tracking_preservation_acknowledged,
                    is_blind_append,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    manifest_write,
                    physical_clustering_columns,
                },
            staged_data_changes:
                Imperative {
                    add_files_metadata,
                    remove_files_metadata,
                    dv_matched_files,
                    num_dv_updates,
                },
            _state: _,
        } = transaction;

        assert_eq!(span.id(), expected_span.id());
        assert_ne!(operation_id, MetricId::nil());
        assert!(correlation_id.is_none());
        assert!(read_snapshot_opt.is_none());
        assert_eq!(&effective_table_config, snapshot.table_configuration());
        assert!(!should_emit_protocol);
        assert!(!should_emit_metadata);
        assert!(!committer.is_catalog_committer());
        assert_eq!(operation, UpdateTableOperation::Write.into());
        assert!(operation_parameters.is_none());
        assert!(operation_metrics.is_none());
        assert_eq!(engine_info, expected_engine_info);
        assert!(engine_commit_info.is_none());
        assert!(add_files_metadata.is_empty());
        assert!(remove_files_metadata.is_empty());
        assert!(!skip_dedup_validation);
        assert!(set_transactions.is_empty());
        assert_eq!(commit_timestamp, expected_commit_timestamp);
        assert!(user_domain_metadata_additions.is_empty());
        assert!(system_domain_metadata_additions.is_empty());
        assert!(provided_row_tracking_high_water_mark.is_none());
        assert!(user_domain_removals.is_empty());
        assert_eq!(data_change, expected_data_change);
        assert!(!column_defaults_acknowledged);
        #[cfg(feature = "check-constraints-in-dev")]
        assert!(!check_constraints_acknowledged);
        assert!(!generated_columns_acknowledged);
        assert!(!row_tracking_preservation_acknowledged);
        assert!(!is_blind_append);
        assert!(dv_matched_files.is_empty());
        assert_eq!(num_dv_updates, 0);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        assert!(manifest_write.is_none());
        assert!(physical_clustering_columns.is_none());
        Ok(())
    }

    #[test]
    fn transaction_constructor_generates_unique_operation_ids() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let first = Transaction::<UpdateTable>::new(
            TransactionBuilderState::default(),
            snapshot.table_configuration().clone(),
            Box::new(FileSystemCommitter::new()),
            UpdateTableOperation::Write.into(),
            tracing::Span::none(),
            1234,
        );
        let second = Transaction::<UpdateTable>::new(
            TransactionBuilderState::default(),
            snapshot.table_configuration().clone(),
            Box::new(FileSystemCommitter::new()),
            UpdateTableOperation::Write.into(),
            tracing::Span::none(),
            1234,
        );

        assert_ne!(first.state.operation_id, second.state.operation_id);
        Ok(())
    }

    #[test]
    fn transaction_constructor_preserves_shared_intent() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let expected_parameters = HashMap::from([("mode".into(), Some("Create".into()))]);
        let expected_metrics = HashMap::from([("numFiles".into(), Some("3".into()))]);
        let expected_domains = vec![DomainMetadata::new("app.domain".into(), "{}".into())];
        let expected_commit_info_schema = schema_ref! { nullable "tag": STRING };
        let commit_info = string_array_to_engine_data(StringArray::from(vec!["connector-tag"]));
        let expected_commit_info = std::ptr::from_ref(commit_info.as_ref());
        let expected_commit_timestamp = 5678;

        let transaction = Transaction::<CreateTable>::new(
            TransactionBuilderState {
                correlation_id: Some("correlation-id".into()),
                operation_parameters: Some(expected_parameters.clone()),
                operation_metrics: Some(expected_metrics.clone()),
                engine_info: "connector/1.0".into(),
                engine_commit_info: Some((commit_info, expected_commit_info_schema.clone())),
                transaction_ids: vec![("app-one".into(), 7), ("app-two".into(), 8)],
                domain_metadata_additions: expected_domains.clone(),
                data_change: false,
                skip_dedup_validation: true,
            },
            snapshot.table_configuration().clone(),
            Box::new(MockCatalogCommitter),
            CommitOperation::CreateTable,
            tracing::Span::none(),
            expected_commit_timestamp,
        );

        assert_eq!(transaction.state.operation, CommitOperation::CreateTable);
        assert_eq!(transaction.state.engine_info, "connector/1.0");
        assert_eq!(
            transaction.state.correlation_id.as_deref(),
            Some("correlation-id")
        );
        assert_eq!(
            transaction.state.operation_parameters,
            Some(expected_parameters)
        );
        assert_eq!(transaction.state.operation_metrics, Some(expected_metrics));
        assert_eq!(
            transaction.state.user_domain_metadata_additions,
            expected_domains
        );
        assert!(transaction.state.committer.is_catalog_committer());
        assert_eq!(
            transaction.state.commit_timestamp,
            expected_commit_timestamp
        );
        assert_eq!(
            transaction.state.set_transactions,
            vec![
                SetTransaction::new("app-one".into(), 7, Some(expected_commit_timestamp)),
                SetTransaction::new("app-two".into(), 8, Some(expected_commit_timestamp)),
            ]
        );
        let (commit_info, commit_info_schema) = transaction.state.engine_commit_info.unwrap();
        assert!(std::ptr::eq(expected_commit_info, commit_info.as_ref()));
        assert_eq!(commit_info_schema, expected_commit_info_schema);
        assert!(!transaction.state.data_change);
        assert!(transaction.state.skip_dedup_validation);
        Ok(())
    }

    struct IoErrorCommitter {
        confirmed_rejection: bool,
    }

    impl Committer for IoErrorCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            _commit_metadata: CommitMetadata,
        ) -> KernelResult<CommitResponse> {
            let error = KernelError::IOError(std::io::Error::other("simulated IO error"));
            if self.confirmed_rejection {
                Ok(CommitResponse::Retryable { error })
            } else {
                Err(error)
            }
        }
        fn is_catalog_committer(&self) -> bool {
            false
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> KernelResult<()> {
            Ok(())
        }
    }

    /// A mock committer that always returns a non-retryable (non-IO) error, used to test the
    /// terminal error path.
    struct GenericErrorCommitter;

    impl Committer for GenericErrorCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            _commit_metadata: CommitMetadata,
        ) -> KernelResult<CommitResponse> {
            Err(KernelError::generic("simulated commit error"))
        }
        fn is_catalog_committer(&self) -> bool {
            false
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> KernelResult<()> {
            Ok(())
        }
    }

    /// A mock catalog committer, used to test catalog committer validation.
    struct MockCatalogCommitter;

    impl Committer for MockCatalogCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            _commit_metadata: CommitMetadata,
        ) -> KernelResult<CommitResponse> {
            // This won't be reached in tests — the validation error fires before commit.
            Ok(CommitResponse::Conflict { version: 0 })
        }
        fn is_catalog_committer(&self) -> bool {
            true
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> KernelResult<()> {
            Ok(())
        }
    }

    /// Sets up a snapshot for a table with deletion vector support at version 1
    fn setup_dv_enabled_table() -> (SyncEngine, Arc<Snapshot>) {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();
        (engine, snapshot)
    }

    fn setup_non_dv_table() -> (SyncEngine, Arc<Snapshot>) {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        (engine, snapshot)
    }

    fn setup_dv_supported_but_disabled_table() -> Result<(Arc<dyn Engine>, Arc<Snapshot>)> {
        let storage = Arc::new(InMemory::new());
        let table_root = url::Url::parse("memory:///").unwrap();
        let engine = Arc::new(SyncEngine::new_with_store(storage.clone()));
        let schema_json = serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "id",
                "type": "integer",
                "nullable": true,
                "metadata": {}
            }]
        });
        let actions = [
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#.to_string(),
            serde_json::json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_json.to_string(),
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1234567890
                }
            })
            .to_string(),
        ]
        .join("\n");

        let commit_path = Path::from("_delta_log/00000000000000000000.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(storage.put(&commit_path, actions.into()))
            .map_err(KernelError::from)?;
        let engine: Arc<dyn Engine> = engine;
        let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
        Ok((engine, snapshot))
    }

    /// Creates a test deletion vector descriptor with default values (the DV might not exist on
    /// disk)
    fn create_test_dv_descriptor(path_suffix: &str) -> DeletionVectorDescriptor {
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: format!("dv_{path_suffix}"),
            offset: Some(0),
            size_in_bytes: 100,
            cardinality: 1,
        }
    }

    fn create_dv_transaction(
        snapshot: Arc<Snapshot>,
        engine: &dyn Engine,
    ) -> KernelResult<Transaction> {
        snapshot
            .update_table(UpdateTableOperation::Delete, "test_engine", true)
            .build(engine, Box::new(FileSystemCommitter::new()))
    }

    // TODO: create a finer-grained unit tests for transactions (issue#1091)
    #[rstest]
    fn test_add_files_schema(
        #[values(DataLayout::None, DataLayout::clustered(["id"]))] layout: DataLayout,
    ) -> KernelResult<()> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let txn = create_table(
            "memory:///t",
            schema_ref! { nullable "id": INTEGER },
            "test",
            true,
        )
        .with_data_layout(layout)
        .build(&engine, Box::new(FileSystemCommitter::new()))?;

        let expected = schema! {
            not_null "path": STRING,
            not_null "partitionValues": { STRING => nullable STRING },
            not_null "size": LONG,
            not_null "modificationTime": LONG,
            nullable "stats": {
                nullable NUM_RECORDS: LONG,
                nullable NULL_COUNT: {},
                nullable MIN_VALUES: {},
                nullable MAX_VALUES: {},
                nullable TIGHT_BOUNDS: BOOLEAN,
            },
        };
        assert_eq!(txn.add_files_schema().as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_new_deletion_vector_path() -> KernelResult<()> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "default engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        let write_state = txn.write_state().unwrap();
        let write_context = write_state.write_context_builder().build().unwrap();

        // Test with empty prefix
        let dv_path1 = write_context.new_deletion_vector_path(String::from(""));
        let abs_path1 = dv_path1.absolute_path()?;
        assert!(abs_path1.as_str().contains(url.as_str()));

        // Test with non-empty prefix
        let prefix = String::from("dv_test");
        let dv_path2 = write_context.new_deletion_vector_path(prefix.clone());
        let abs_path2 = dv_path2.absolute_path()?;
        assert!(abs_path2.as_str().contains(url.as_str()));
        assert!(abs_path2.as_str().contains(&prefix));

        // Test that two paths with same prefix are different (unique UUIDs)
        let dv_path3 = write_context.new_deletion_vector_path(prefix.clone());
        let abs_path3 = dv_path3.absolute_path()?;
        assert_ne!(abs_path2, abs_path3);

        Ok(())
    }

    #[test]
    fn write_state_reflects_schema_configured_on_builder() -> KernelResult<()> {
        let (engine, snapshot) = setup_non_dv_table();
        let txn = snapshot
            .clone()
            .update_table(UpdateTableOperation::Write, "default engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;

        // Independently built transactions must retain their own write schemas.
        let initial_write_state = txn.write_state()?;
        let initial_write_context = initial_write_state.write_context_builder().build()?;
        assert!(
            !initial_write_context
                .logical_data_schema()
                .contains("fresh_column")
        );

        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .add_column(StructField::nullable("fresh_column", DataType::INTEGER))
            .build(&engine, Box::new(FileSystemCommitter::new()))?;

        let updated_write_state = txn.write_state()?;
        let updated_write_context = updated_write_state.write_context_builder().build()?;
        assert!(
            updated_write_context
                .logical_data_schema()
                .contains("fresh_column")
        );
        assert!(
            updated_write_context
                .physical_data_schema()
                .contains("fresh_column")
        );
        assert!(
            !initial_write_context
                .logical_data_schema()
                .contains("fresh_column")
        );

        Ok(())
    }

    #[test]
    fn schema_changes_are_applied_once_and_persisted_on_commit() -> Result<()> {
        let (engine, txn, _tempdir) = create_existing_table_txn_with(true, |builder| {
            builder.add_column(StructField::nullable("first_column", DataType::INTEGER))
        })?;

        let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
        assert!(snapshot.schema().contains("first_column"));

        let snapshot = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .add_column(StructField::nullable("second_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();

        let schema = snapshot.schema();
        let field_names: Vec<&str> = schema.fields().map(|field| field.name().as_str()).collect();
        let first_index = field_names
            .iter()
            .position(|name| *name == "first_column")
            .expect("first schema change must be present");
        let second_index = field_names
            .iter()
            .position(|name| *name == "second_column")
            .expect("second schema change must be present");
        assert!(first_index < second_index);
        Ok(())
    }

    #[test]
    fn schema_set_nullable_is_persisted_on_commit() -> KernelResult<()> {
        let engine: Arc<dyn Engine> =
            Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
        let schema = Arc::new(StructType::try_new([
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])?);
        let snapshot = create_table("memory:///set_nullable", schema, "test", true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();

        let snapshot = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .set_nullable(column_name!("id"))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();

        assert!(snapshot.schema().field("id").unwrap().is_nullable());
        assert!(snapshot.schema().field("name").unwrap().is_nullable());
        Ok(())
    }

    #[test]
    fn schema_add_struct_then_nested_field_is_persisted_on_commit() -> Result<()> {
        let (engine, txn, _tempdir) = create_existing_table_txn_with(true, |builder| {
            builder
                .add_column(StructField::nullable(
                    "address",
                    StructType::try_new([]).unwrap(),
                ))
                .add_column_at(
                    column_name!("address"),
                    StructField::nullable("city", DataType::STRING),
                )
        })?;
        let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

        let schema = snapshot.schema();
        let address = schema.field("address").expect("address must exist");
        let DataType::Struct(address) = address.data_type() else {
            panic!("address must be a struct");
        };
        let city = address
            .field("city")
            .expect("city must be nested in address");
        assert_eq!(city.data_type(), &DataType::STRING);
        assert!(city.is_nullable());
        Ok(())
    }

    #[test]
    fn schema_changes_can_precede_staged_data_in_the_same_commit() -> Result<()> {
        let (engine, mut txn, _tempdir) = create_existing_table_txn_with(true, |builder| {
            builder.add_column(StructField::nullable("fresh_column", DataType::INTEGER))
        })?;
        add_dummy_file(&mut txn);

        let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

        assert!(snapshot.schema().contains("fresh_column"));
        Ok(())
    }

    #[test]
    fn empty_builder_schema_changes_are_rejected() {
        let (_engine, snapshot) = setup_non_dv_table();
        let result = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_schema_changes([]);
        assert_result_error_with_message(
            result,
            "with_schema_changes requires at least one schema operation",
        );
    }

    mod column_defaults {
        use super::*;
        use crate::schema::column_default::{field_with_default, field_with_invalid_default};

        // NB: `test_utils::schema_with_column_defaults` cannot be used here. In `--lib` unit tests
        // the crate under test and the `delta_kernel` that `test_utils` links are two distinct
        // crate instances, so kernel schema types don't unify across the `test_utils` boundary.
        // The `field_with_*` helpers live in-crate (`schema::column_default`) for the same reason.

        /// Builds a transaction whose effective logical schema is `schema`, with the
        /// `allowColumnDefaults` writer feature enabled so any declared defaults are honored.
        fn txn_with_schema(schema: StructType) -> Transaction {
            txn_with_schema_and_writer_features(schema, [TableFeature::AllowColumnDefaults])
        }

        /// Like [`txn_with_schema`] but with an explicit writer-feature list, so a test can
        /// exercise a table that does *not* enable `allowColumnDefaults`. Panics if the table
        /// configuration fails to construct; use [`try_table_config`] to assert that error.
        fn txn_with_schema_and_writer_features(
            schema: StructType,
            writer_features: impl IntoIterator<Item = TableFeature>,
        ) -> Transaction {
            let (engine, snapshot) = setup_non_dv_table();
            let mut txn = snapshot
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .unwrap();
            let table_config = try_table_config(&txn, schema, writer_features).unwrap();
            txn.replace_effective_table_config(table_config);
            txn
        }

        /// Builds the [`TableConfiguration`] a transaction would carry for `schema` and
        /// `writer_features`, swapping a synthetic schema/protocol onto a real snapshot's config so
        /// the validation `try_new` runs at construction can be exercised without `create_table`.
        /// Returns the construction result so a test can assert eager-validation errors.
        fn try_table_config(
            base: &Transaction,
            schema: StructType,
            writer_features: impl IntoIterator<Item = TableFeature>,
        ) -> KernelResult<TableConfiguration> {
            let metadata = base
                .state
                .effective_table_config
                .metadata()
                .clone()
                .with_schema(Arc::new(schema))
                .unwrap();
            let protocol =
                Protocol::try_new_modern(TableFeature::EMPTY_LIST, writer_features).unwrap();
            let version = base.state.effective_table_config.version();
            TableConfiguration::try_new_from(
                &base.state.effective_table_config,
                Some(metadata),
                Some(protocol),
                version,
            )
        }

        /// A transaction over a real (non-DV) table, to use as the base config in
        /// [`try_table_config`].
        fn base_txn() -> Transaction {
            let (engine, snapshot) = setup_non_dv_table();
            snapshot
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .unwrap()
        }

        #[test]
        fn collects_present_defaults_and_skips_columns_without_one() {
            let schema = schema! {
                (field_with_default("parsable", DataType::INTEGER, "42")),
                (field_with_default(
                    "unparsable",
                    DataType::TIMESTAMP,
                    "current_timestamp()",
                )),
                nullable "no_default": STRING,
            };
            let txn = txn_with_schema(schema);

            let defaults = txn.top_level_column_defaults().unwrap();
            assert_eq!(
                defaults.len(),
                2,
                "only columns with a default are returned"
            );
            assert!(!defaults.contains_key("no_default"));

            let parsable = &defaults["parsable"];
            assert_eq!(parsable.raw_sql(), "42");
            assert!(parsable.to_scalar().unwrap().is_some());

            let unparsable = &defaults["unparsable"];
            assert_eq!(unparsable.raw_sql(), "current_timestamp()");
            assert!(unparsable.to_scalar().unwrap().is_none());
        }

        #[test]
        fn returns_empty_map_when_no_column_has_a_default() {
            let schema = schema! {
                nullable "a": INTEGER,
                nullable "b": STRING,
            };
            let txn = txn_with_schema(schema);
            assert!(txn.top_level_column_defaults().unwrap().is_empty());
        }

        #[test]
        fn load_rejects_malformed_default() {
            let schema = schema! {
                (field_with_invalid_default("c")),
            };

            let err = try_table_config(&base_txn(), schema, [TableFeature::AllowColumnDefaults])
                .expect_err("non-string CURRENT_DEFAULT must error at load")
                .to_string();
            assert!(err.contains("non-string"), "got: {err}");
        }

        #[test]
        fn load_tolerates_default_present_but_feature_not_enabled() {
            let schema = schema! {
                (field_with_default("c", DataType::INTEGER, "42")),
            };

            // Orphaned column-default metadata (no `allowColumnDefaults` feature) is tolerated.
            let txn = txn_with_schema_and_writer_features(schema, []);
            assert!(txn.top_level_column_defaults().unwrap().is_empty());
        }
    }

    #[test]
    fn test_write_context_schemas_exclude_partition_columns() -> KernelResult<()> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "default engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;

        let write_state = txn.write_state()?;
        let write_context = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([(
                "letter".to_string(),
                Scalar::String("a".into()),
            )]))
            .build()?;
        let logical_schema = write_context.logical_data_schema();
        let physical_schema = write_context.physical_data_schema();

        // Both schemas exclude partition columns.
        assert!(
            !logical_schema.contains("letter"),
            "Logical schema should not contain partition column 'letter'"
        );
        assert!(
            !physical_schema.contains("letter"),
            "Physical schema should not contain partition column 'letter' (stored in path)"
        );

        // Both should contain the non-partition columns
        assert!(
            logical_schema.contains("number"),
            "Logical schema should contain data column 'number'"
        );

        assert!(
            physical_schema.contains("number"),
            "Physical schema should contain data column 'number'"
        );

        Ok(())
    }

    /// Loads a snapshot from `table_path` and builds a partitioned write context for the given
    /// partition values. The table must be partitioned.
    fn snapshot_and_partitioned_write_context(
        table_path: &str,
        partition_values: HashMap<String, Scalar>,
    ) -> Result<(Arc<Snapshot>, BoundWriteContext)> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot
            .clone()
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        let write_state = txn.write_state()?;
        let wc = write_state
            .write_context_builder()
            .with_partition_values(partition_values)
            .build()?;
        Ok((snapshot, wc))
    }

    /// Helper: evaluates the logical-to-physical transform on the given batch and returns the
    /// output RecordBatch.
    fn eval_logical_to_physical(
        wc: &BoundWriteContext,
        batch: RecordBatch,
    ) -> KernelResult<RecordBatch> {
        let input_schema = StructType::try_from_arrow(batch.schema())?;
        let physical_schema = wc.physical_data_schema();
        let l2p = wc.logical_to_physical();

        let handler = ArrowEvaluationHandler;
        let evaluator = handler.new_expression_evaluator(
            input_schema.into(),
            l2p,
            physical_schema.clone().into(),
        )?;
        let result = ArrowEngineData::try_from_engine_data(
            evaluator.evaluate(&ArrowEngineData::new(batch))?,
        )?;
        Ok(result.record_batch().clone())
    }

    #[rstest]
    #[case::not_materialized("./tests/data/basic_partitioned/", false)]
    #[case::materialized("./tests/data/partitioned_with_materialize_feature/", true)]
    fn test_partition_columns_materialized_in_logical_to_physical(
        #[case] table_path: &str,
        #[case] materialized: bool,
    ) -> Result<()> {
        let (snapshot, wc) = snapshot_and_partitioned_write_context(
            table_path,
            HashMap::from([("letter".to_string(), Scalar::String("a".into()))]),
        )?;
        assert_eq!(
            snapshot
                .table_configuration()
                .protocol()
                .has_table_feature(&TableFeature::MaterializePartitionColumns),
            materialized
        );

        // The input data must exclude the partition column "letter".
        let input_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("number", ArrowDataType::Int64, true),
            ArrowField::new("a_float", ArrowDataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![42])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.5])),
            ],
        )
        .map_err(KernelError::from)?;
        let rb = eval_logical_to_physical(&wc, batch)?;

        let rb_schema = rb.schema();
        let names: Vec<&str> = rb_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        if materialized {
            assert_eq!(names, vec!["letter", "number", "a_float"]);
            assert_eq!(get_column!(rb, "letter", StringArray).value(0), "a");
        } else {
            assert_eq!(names, vec!["number", "a_float"]);
        }
        Ok(())
    }

    #[rstest]
    #[case::cm_none(ColumnMappingMode::None)]
    #[case::cm_name(ColumnMappingMode::Name)]
    #[case::cm_id(ColumnMappingMode::Id)]
    fn test_materialized_partition_column_insert(
        #[case] cm_mode: ColumnMappingMode,
    ) -> KernelResult<()> {
        let cm = match cm_mode {
            ColumnMappingMode::None => "none",
            ColumnMappingMode::Name => "name",
            ColumnMappingMode::Id => "id",
        };
        let engine: Arc<dyn Engine> =
            Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
        // Logical order: [p1, p2, d1, v(void), p3, p4, d2]; partition cols = p1, p2, p3, p4.
        let schema = schema_ref! {
            nullable "p1": STRING,
            nullable "p2": INTEGER,
            nullable "d1": INTEGER,
            nullable "v": VOID,
            nullable "p3": STRING,
            nullable "p4": INTEGER,
            nullable "d2": INTEGER,
        };
        let txn = create_table("memory:///t", schema, "DefaultEngine", true)
            .with_data_layout(DataLayout::partitioned(["p1", "p2", "p3", "p4"]))
            .with_table_properties([
                ("delta.feature.materializePartitionColumns", "supported"),
                ("delta.columnMapping.mode", cm),
            ])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        let write_state = txn.write_state()?;
        let wc = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([
                ("p1".to_string(), Scalar::String("aa".into())),
                ("p2".to_string(), Scalar::Integer(7)),
                ("p3".to_string(), Scalar::String("cc".into())),
                ("p4".to_string(), Scalar::Integer(9)),
            ]))
            .build()?;

        // Input excludes partition columns but keeps the void column, in logical schema
        // order: [d1, v, d2].
        let input_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("d1", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::Null, true),
            ArrowField::new("d2", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
                Arc::new(NullArray::new(1)),
                Arc::new(Int32Array::from(vec![20])),
            ],
        )?;
        let rb = eval_logical_to_physical(&wc, batch)?;

        // With void stripped and partition literals inserted, the output names/order must match
        // the physical schema exactly.
        let rb_schema = rb.schema();
        let names: Vec<&str> = rb_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let physical_schema = wc.physical_data_schema();
        let expected_names: Vec<&str> = physical_schema
            .fields()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, expected_names);

        // Verify the transformed data.
        assert_eq!(get_column!(rb, names[0], StringArray).value(0), "aa"); // p1 (prepended)
        assert_eq!(get_column!(rb, names[1], Int32Array).value(0), 7); // p2 (prepended)
        assert_eq!(get_column!(rb, names[2], Int32Array).value(0), 10); // d1
        assert_eq!(get_column!(rb, names[3], StringArray).value(0), "cc"); // p3 (after d1, void
        // skipped)
        assert_eq!(get_column!(rb, names[4], Int32Array).value(0), 9); // p4 (after d1)
        assert_eq!(get_column!(rb, names[5], Int32Array).value(0), 20); // d2
        Ok(())
    }

    /// Physical schema should include partition columns when materializePartitionColumns is on.
    #[test]
    fn test_physical_schema_includes_partition_columns_when_materialized() -> Result<()> {
        let (_snapshot, write_context) = snapshot_and_partitioned_write_context(
            "./tests/data/partitioned_with_materialize_feature/",
            HashMap::from([("letter".to_string(), Scalar::String("a".into()))]),
        )?;
        let physical_schema = write_context.physical_data_schema();

        assert!(
            physical_schema.contains("letter"),
            "Partition column 'letter' should be in physical schema when materialized"
        );
        assert!(
            physical_schema.contains("number"),
            "Non-partition column 'number' should be in physical schema"
        );
        Ok(())
    }

    // Building the wrong write context for the table's partitioning returns an error.
    #[rstest]
    #[case::partition_value_on_unpartitioned(
        "./tests/data/table-without-dv-small/",
        Some(HashMap::from([("x".to_string(), Scalar::Integer(1))])),
        "not partitioned",
        None
    )]
    #[case::missing_values_on_partitioned(
        "./tests/data/basic_partitioned/",
        None,
        "table is partitioned",
        None
    )]
    #[case::empty_values_on_partitioned(
        "./tests/data/basic_partitioned/",
        Some(HashMap::new()),
        "missing partition column",
        Some("required")
    )]
    fn test_write_context_builder_requires_matching_partition_values(
        #[case] table_path: &str,
        #[case] partition_values: Option<HashMap<String, Scalar>>,
        #[case] expected_msg: &str,
        #[case] unexpected_msg: Option<&str>,
    ) -> Result<()> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        let write_state = txn.write_state()?;
        let mut builder = write_state.write_context_builder();
        if let Some(partition_values) = partition_values {
            builder = builder.with_partition_values(partition_values);
        }
        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, KernelError::InvalidPartitionValues(_)),
            "unexpected error: {err}"
        );
        let err = err.to_string();
        assert!(
            err.contains(expected_msg),
            "expected '{expected_msg}' in error, got: {err}"
        );
        if let Some(unexpected_msg) = unexpected_msg {
            assert!(
                !err.contains(unexpected_msg),
                "did not expect '{unexpected_msg}' in error, got: {err}"
            );
        }
        Ok(())
    }

    /// Tests that update_deletion_vectors validates table protocol requirements.
    /// Validates that attempting DV updates on unsupported tables returns protocol error.
    #[test]
    fn test_update_deletion_vectors_unsupported_table() -> KernelResult<()> {
        let (engine, snapshot) = setup_non_dv_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        let err = result.expect_err("Should fail on table without DV support");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Deletion vector")
                && (err_msg.contains("require") || err_msg.contains("version")),
            "Expected protocol error about DV requirements, got: {err_msg}"
        );
        Ok(())
    }

    #[test]
    fn test_update_deletion_vectors_requires_enablement_property() -> Result<()> {
        let (engine, snapshot) = setup_dv_supported_but_disabled_table()?;
        let mut txn = create_dv_transaction(snapshot, engine.as_ref())?;

        let err = txn
            .update_deletion_vectors(HashMap::new(), std::iter::empty())
            .expect_err("DV updates should require delta.enableDeletionVectors=true");

        assert!(
            matches!(err, KernelError::Unsupported(_)),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("delta.enableDeletionVectors"),
            "error should mention the enablement property, got: {err}"
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors validates DV descriptors match scan files.
    /// Validates detection of mismatch between provided DV descriptors and actual files.
    #[test]
    fn test_update_deletion_vectors_mismatch_count() -> KernelResult<()> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let mut dv_map = HashMap::new();
        let descriptor = create_test_dv_descriptor("non_existent");
        dv_map.insert("non_existent_file.parquet".to_string(), descriptor);

        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        assert!(
            result.is_err(),
            "Should fail when DV descriptors don't match scan files"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("matched") && err_msg.contains("does not match"),
            "Expected error about mismatched count (expected 1 descriptor, 0 matched files), got: {err_msg}"
        );
        Ok(())
    }

    /// Tests that a mismatch after scanning some files does not leave staged DV updates behind.
    #[test]
    fn test_update_deletion_vectors_mismatch_does_not_mutate_transaction() -> KernelResult<()> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot.clone(), &engine)?;
        let scan = snapshot.scan_builder().build()?;
        let scan_metadata = scan
            .scan_metadata(&engine)?
            .collect::<KernelResult<Vec<_>>>()?;

        let mut paths = Vec::new();
        for metadata in &scan_metadata {
            paths =
                metadata.visit_scan_files(paths, |paths, scan_file| paths.push(scan_file.path))?;
        }
        let existing_path = paths
            .into_iter()
            .next()
            .ok_or_else(|| KernelError::generic("expected at least one scan file"))?;

        let mut dv_map = HashMap::new();
        dv_map.insert(existing_path, create_test_dv_descriptor("matched"));
        dv_map.insert(
            "non_existent_file.parquet".to_string(),
            create_test_dv_descriptor("missing"),
        );

        let result = txn.update_deletion_vectors(
            dv_map,
            scan_metadata
                .into_iter()
                .map(|metadata| Ok(metadata.scan_files)),
        );

        assert!(
            result.is_err(),
            "Should fail when only some DV descriptors match scan files"
        );
        assert!(
            txn.staged_data_changes.dv_matched_files.is_empty(),
            "Failed DV update should not leave staged file updates"
        );
        Ok(())
    }

    #[test]
    fn test_update_deletion_vectors_iter_error_does_not_mutate_transaction() -> KernelResult<()> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot.clone(), &engine)?;
        txn.staged_data_changes
            .dv_matched_files
            .push(FilteredEngineData::with_all_rows_selected(
                string_array_to_engine_data(StringArray::from(vec!["sentinel"])),
            ));
        let staged_len_before = txn.staged_data_changes.dv_matched_files.len();
        let scan = snapshot.scan_builder().build()?;
        let scan_metadata = scan
            .scan_metadata(&engine)?
            .collect::<KernelResult<Vec<_>>>()?;

        let mut paths = Vec::new();
        for metadata in &scan_metadata {
            paths =
                metadata.visit_scan_files(paths, |paths, scan_file| paths.push(scan_file.path))?;
        }
        let existing_path = paths
            .into_iter()
            .next()
            .ok_or_else(|| KernelError::generic("expected at least one scan file"))?;

        let mut dv_map = HashMap::new();
        dv_map.insert(existing_path, create_test_dv_descriptor("matched"));

        let result = txn.update_deletion_vectors(
            dv_map,
            scan_metadata
                .into_iter()
                .map(|metadata| Ok(metadata.scan_files))
                .chain(std::iter::once(Err(KernelError::generic(
                    "simulated scan metadata failure",
                )))),
        );

        assert!(result.is_err(), "iterator error should propagate");
        assert_eq!(
            txn.staged_data_changes.dv_matched_files.len(),
            staged_len_before,
            "Failed DV update should not stage additional file updates"
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors handles empty DV updates correctly as a no-op.
    /// This edge case occurs when a DELETE operation matches no rows.
    #[test]
    fn test_update_deletion_vectors_empty_inputs() -> KernelResult<()> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot, &engine)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        assert!(
            result.is_ok(),
            "Empty DV updates should succeed as no-op, got error: {result:?}"
        );

        Ok(())
    }

    #[test]
    fn unmatched_dv_inputs_do_not_require_row_tracking_acknowledgment() -> KernelResult<()> {
        let (engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot.clone(), &engine)?;
        let scan = snapshot.scan_builder().build()?;
        txn.update_deletion_vectors(
            HashMap::new(),
            Transaction::scan_metadata_to_engine_data(scan.scan_metadata(&engine)?),
        )?;

        // Since we staged a DV update (with no files matched), the transaction bypasses
        // row-tracking + CDF checks and does not require explicit row tracking acknowledgement.
        assert!(txn.staged_data_changes.has_data_file_actions());
        assert!(!txn.staged_data_changes.has_dv_updates());
        txn.state.effective_table_config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking, TableFeature::ChangeDataFeed])
                    .build(),
            )
            .with_properties([(ENABLE_ROW_TRACKING, true), (ENABLE_CHANGE_DATA_FEED, true)])
            .build();
        add_dummy_file(&mut txn);
        txn.state.validate_commit(&txn.staged_data_changes)?;

        // Blind append validations only consider staged DV updates (not whether any selection
        // vector actually matched), so a blind append is still disallowed.
        txn.state.is_blind_append = true;
        assert_result_error_with_message(
            txn.state.validate_commit(&txn.staged_data_changes),
            "Blind append cannot update deletion vectors",
        );
        Ok(())
    }

    // ============================================================================
    // validate_blind_append tests
    // ============================================================================
    fn add_dummy_file<STATE>(txn: &mut Transaction<STATE>) {
        let batch = create_valid_add_file_batch(false /* all_nullable */);
        txn.add_files(Box::new(ArrowEngineData::new(batch)));
    }

    /// Build a transaction on a writable copy of the `table-without-dv-small` fixture.
    fn create_existing_table_txn() -> Result<(Arc<dyn Engine>, Transaction, tempfile::TempDir)> {
        create_existing_table_txn_with(true, |builder| builder)
    }

    fn create_existing_table_txn_with(
        data_change: bool,
        configure: impl FnOnce(UpdateTableTransactionBuilder) -> UpdateTableTransactionBuilder,
    ) -> Result<(Arc<dyn Engine>, Transaction, tempfile::TempDir)> {
        let (url, tempdir) = copy_test_table("table-without-dv-small")?;
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
        let txn = configure(snapshot.update_table(
            UpdateTableOperation::Write,
            "test-engine",
            data_change,
        ))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        Ok((engine, txn, tempdir))
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn test_commit_rejects_file_actions_on_iceberg_native_v4_table() -> Result<()> {
        let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn.replace_effective_table_config(adaptive_metadata_table_configuration(
            test_schema_flat_with_column_mapping(),
            &[TableFeature::IcebergNativeV4Preview],
        ));
        add_dummy_file(&mut txn);
        let err = txn.commit(engine.as_ref()).unwrap_err();
        assert!(
            err.to_string().contains("icebergNativeV4"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn test_ctas_rejected_on_iceberg_native_v4_table() -> KernelResult<()> {
        let tempdir = tempfile::tempdir()?;
        let table_path = tempdir.path().join("table");
        std::fs::create_dir(&table_path)?;
        let engine = SyncEngine::new();
        let mut txn = create_table(
            table_path.to_string_lossy(),
            schema_ref! { nullable "id": INTEGER },
            "test-engine",
            true,
        )
        .with_table_properties([("delta.enableIcebergNativeV4", "true")])
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
        add_dummy_file(&mut txn);

        assert_result_error_with_message(
            txn.commit(&engine),
            "commits to icebergNativeV4-preview tables cannot include file actions",
        );
        assert!(!table_path.join("_delta_log").exists());
        Ok(())
    }

    #[rstest]
    #[case::create_with_update_operation(
        true,
        CommitOperation::UpdateTable(UpdateTableOperation::Write),
        "create-table transactions must use the CREATE TABLE operation"
    )]
    #[case::update_with_create_operation(
        false,
        CommitOperation::CreateTable,
        "CREATE TABLE cannot use an update-table transaction"
    )]
    #[case::alter_without_schema_changes(
        false,
        CommitOperation::UpdateTable(UpdateTableOperation::AlterTable),
        "ALTER TABLE requires at least one schema change"
    )]
    #[case::empty_custom_operation(
        false,
        CommitOperation::UpdateTable(UpdateTableOperation::Custom(String::new())),
        "custom operation name cannot be empty"
    )]
    #[case::reserved_custom_operation(
        false,
        CommitOperation::UpdateTable(UpdateTableOperation::Custom("WRITE".to_string())),
        "custom operation name 'WRITE' is reserved"
    )]
    #[case::reserved_reorg_operation(
        false,
        CommitOperation::UpdateTable(UpdateTableOperation::Custom("REORG".to_string())),
        "custom operation name 'REORG' is reserved"
    )]
    fn operation_compatibility_rejects_invalid_internal_state(
        #[case] is_create_table: bool,
        #[case] operation: CommitOperation,
        #[case] expected: &str,
    ) -> Result<()> {
        // Public builders reject these states. Mutate private intent to test the runtime guard.
        let result = if is_create_table {
            let tempdir = tempfile::tempdir().map_err(KernelError::from)?;
            let engine = SyncEngine::new();
            let mut txn = create_table(
                tempdir.path().to_string_lossy(),
                test_schema_flat(),
                "test-engine",
                true,
            )
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
            txn.state.operation = operation;
            txn.state
                .validate_operation_compatibility(&txn.staged_data_changes)
        } else {
            let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
            txn.state.operation = operation;
            txn.state
                .validate_operation_compatibility(&txn.staged_data_changes)
        };

        let error = result.expect_err("invalid internal operation must be rejected");
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_success() -> Result<()> {
        let (_engine, mut txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        add_dummy_file(&mut txn);
        txn.state
            .validate_blind_append_semantics(&txn.staged_data_changes)?;
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_adds() -> Result<()> {
        let (_engine, txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        let result = txn
            .state
            .validate_blind_append_semantics(&txn.staged_data_changes);
        assert!(matches!(
            result,
            Err(KernelError::InvalidTransactionState(_))
        ));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_data_change() -> Result<()> {
        let (_engine, mut txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        // Exercise defensive commit validation of an invalid internal state.
        txn.state.data_change = false;
        add_dummy_file(&mut txn);
        let result = txn
            .state
            .validate_blind_append_semantics(&txn.staged_data_changes);
        assert!(matches!(
            result,
            Err(KernelError::InvalidTransactionState(_))
        ));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_removes() -> Result<()> {
        let (_engine, mut txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        add_dummy_file(&mut txn);
        let remove_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["remove"]),
        ));
        txn.remove_files(remove_data);
        let result = txn
            .state
            .validate_blind_append_semantics(&txn.staged_data_changes);
        assert!(matches!(
            result,
            Err(KernelError::InvalidTransactionState(_))
        ));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_dv_updates() -> Result<()> {
        let (_engine, mut txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        add_dummy_file(&mut txn);
        let dv_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["dv"]),
        ));
        txn.staged_data_changes.dv_matched_files.push(dv_data);
        let result = txn
            .state
            .validate_blind_append_semantics(&txn.staged_data_changes);
        assert!(matches!(
            result,
            Err(KernelError::InvalidTransactionState(_))
        ));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_create_table() -> KernelResult<()> {
        let tempdir = tempfile::tempdir()?;
        let schema = schema_ref! { nullable "id": INTEGER };
        let engine = Arc::new(crate::engine::sync::SyncEngine::new());
        let mut txn = create_table(
            tempdir.path().to_str().expect("valid temp path"),
            schema,
            "test_engine",
            true,
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        // Create-table builders do not expose with_blind_append(). Set the field directly to
        // exercise the defensive runtime check.
        txn.state.is_blind_append = true;
        add_dummy_file(&mut txn);
        let result = txn
            .state
            .validate_blind_append_semantics(&txn.staged_data_changes);
        assert!(matches!(
            result,
            Err(KernelError::InvalidTransactionState(_))
        ));
        Ok(())
    }

    #[test]
    fn test_blind_append_sets_commit_info_flag() -> KernelResult<()> {
        let commit_info = CommitInfo::new(1, None, None, None, true);
        assert_eq!(commit_info.is_blind_append, Some(true));

        let commit_info_false = CommitInfo::new(1, None, None, None, false);
        assert_eq!(commit_info_false.is_blind_append, None);
        Ok(())
    }

    #[test]
    fn test_blind_append_commit_rejects_no_adds() -> Result<()> {
        let (_engine, txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        // No files added — commit should fail with blind append validation
        let err = txn
            .commit(_engine.as_ref())
            .expect_err("Blind append with no adds should fail");
        assert!(
            err.to_string()
                .contains("Blind append requires at least one added data file"),
            "Unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_blind_append_commit_success() -> Result<()> {
        let (engine, mut txn, _tempdir) =
            create_existing_table_txn_with(true, UpdateTableTransactionBuilder::with_blind_append)?;
        add_dummy_file(&mut txn);
        // Blind append with add files should pass validation and proceed to commit.
        // The commit itself may fail due to schema mismatch with the dummy data,
        // but we verify validation (line 415) passes on the Ok path.
        let result = txn.commit(engine.as_ref());
        // If it fails, it should NOT be an InvalidTransactionState error
        if let Err(e) = result {
            assert!(
                !matches!(e, KernelError::InvalidTransactionState(_)),
                "Blind append validation should have passed, got: {e}"
            );
        }
        Ok(())
    }

    // Note: Additional test coverage for partial file matching (where some files in a scan
    // have DV updates but others don't) is provided by the end-to-end integration test
    // kernel/tests/features/dv.rs and kernel/tests/write/remove_dv.rs, which exercise
    // the full deletion vector write workflow including the DvMatchVisitor logic.

    #[rstest]
    #[case::io_error(false)]
    #[case::confirmed_rejection(true)]
    fn test_commit_error_returns_retryable_transaction(
        #[case] confirmed_rejection: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(
                engine.as_ref(),
                Box::new(IoErrorCommitter {
                    confirmed_rejection,
                }),
            )?;
        add_dummy_file(&mut txn);
        let result = txn.commit(engine.as_ref())?;
        assert!(
            matches!(result, CommitResult::Retryable(_)),
            "Expected Retryable, got: {result:?}"
        );
        if let CommitResult::Retryable(retryable) = result {
            assert!(
                retryable.error.to_string().contains("simulated IO error"),
                "Unexpected error: {}",
                retryable.error
            );
        }
        Ok(())
    }

    #[test]
    fn test_existing_table_txn_debug() -> Result<()> {
        let (_engine, txn, _tempdir) = create_existing_table_txn()?;
        let debug_str = format!("{txn:?}");
        // Existing-table transactions should include the snapshot version number
        assert!(
            debug_str.contains("Transaction") && debug_str.contains("read_snapshot version"),
            "Debug output should contain Transaction info: {debug_str}"
        );
        // Should NOT contain "create_table"
        assert!(
            !debug_str.contains("create_table"),
            "Existing table debug should not contain create_table: {debug_str}"
        );
        Ok(())
    }

    // Input schemas have no CM metadata; create_table automatically assigns IDs and
    // physical names when mode is Name or Id.
    #[rstest]
    #[case::flat_none(test_schema_flat(), ColumnMappingMode::None)]
    #[case::flat_name(test_schema_flat(), ColumnMappingMode::Name)]
    #[case::flat_id(test_schema_flat(), ColumnMappingMode::Id)]
    #[case::nested_none(test_schema_nested(), ColumnMappingMode::None)]
    #[case::nested_name(test_schema_nested(), ColumnMappingMode::Name)]
    #[case::nested_id(test_schema_nested(), ColumnMappingMode::Id)]
    #[case::map_none(test_schema_with_map(), ColumnMappingMode::None)]
    #[case::map_name(test_schema_with_map(), ColumnMappingMode::Name)]
    #[case::map_id(test_schema_with_map(), ColumnMappingMode::Id)]
    #[case::array_none(test_schema_with_array(), ColumnMappingMode::None)]
    #[case::array_name(test_schema_with_array(), ColumnMappingMode::Name)]
    #[case::array_id(test_schema_with_array(), ColumnMappingMode::Id)]
    fn test_physical_schema_column_mapping(
        #[case] schema: SchemaRef,
        #[case] mode: ColumnMappingMode,
    ) -> KernelResult<()> {
        let (_engine, txn) = crate::unit_test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_state = txn.write_state().unwrap();
        let write_context = write_state.write_context_builder().build().unwrap();
        crate::unit_test_utils::validate_physical_schema_column_mapping(
            write_context.logical_data_schema(),
            write_context.physical_data_schema(),
            mode,
        );
        Ok(())
    }

    /// Builds two-row [`EngineData`] with logical field names matching [`test_schema_nested`].
    fn build_test_record_batch() -> KernelResult<Box<dyn EngineData>> {
        let schema = test_schema_nested();
        let tag_type = MapType::new(DataType::STRING, DataType::STRING, true);
        let score_type = ArrayType::new(DataType::INTEGER, true);
        let info_fields = vec![
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
            StructField::nullable("tags", tag_type.clone()),
            StructField::nullable("scores", score_type.clone()),
        ];
        let info1 = Scalar::Struct(StructData::try_new(
            info_fields.clone(),
            vec![
                "alice".into(),
                30i32.into(),
                Scalar::Map(MapData::try_new(tag_type.clone(), [("k1", "v1")])?),
                Scalar::Array(ArrayData::try_new(score_type.clone(), [10i32, 20i32])?),
            ],
        )?);
        let info2 = Scalar::Struct(StructData::try_new(
            info_fields,
            vec![
                "bob".into(),
                25i32.into(),
                Scalar::Map(MapData::try_new(tag_type, [("k2", "v2")])?),
                Scalar::Array(ArrayData::try_new(score_type, [30i32])?),
            ],
        )?);
        let rows = vec![vec![1i64.into(), info1], vec![2i64.into(), info2]];
        ArrowEvaluationHandler.create_many(schema, rows)
    }

    /// Validates that [`BoundWriteContext::logical_to_physical`] correctly renames fields at all
    /// nesting
    /// levels. Builds a RecordBatch with logical names, evaluates the transform, and checks
    /// that the output uses physical names from the physical schema — including nested struct
    /// children.
    fn validate_logical_to_physical_transform(mode: ColumnMappingMode) -> KernelResult<()> {
        let schema = test_schema_nested();
        let (_engine, txn) = crate::unit_test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_state = txn.write_state().unwrap();
        let write_context = write_state.write_context_builder().build().unwrap();
        let logical_schema = write_context.logical_data_schema();
        let physical_schema = write_context.physical_data_schema();
        let logical_to_physical_expression = write_context.logical_to_physical();

        if mode != ColumnMappingMode::None {
            assert_ne!(
                logical_schema, physical_schema,
                "Physical schema should differ from logical schema when column mapping is enabled"
            );
        }

        let data = build_test_record_batch()?;

        // Evaluate the logical_to_physical expression
        let input_schema: SchemaRef = logical_schema.clone();
        let handler = ArrowEvaluationHandler;
        let evaluator = handler.new_expression_evaluator(
            input_schema,
            logical_to_physical_expression.clone(),
            physical_schema.clone().into(),
        )?;
        let result = evaluator.evaluate(data.as_ref())?;
        let result = ArrowEngineData::try_from_engine_data(result)?;
        let result_batch = result.record_batch();

        // Verify: all field names, types, and metadata match the physical schema
        let expected_arrow_schema: ArrowSchema = physical_schema.as_ref().try_into_arrow()?;
        assert_eq!(result_batch.schema().as_ref(), &expected_arrow_schema);

        // Verify: data is preserved (id values)
        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        assert_eq!(id_col.values(), &[1i64, 2]);

        Ok(())
    }

    #[rstest]
    #[case::name_mode(ColumnMappingMode::Name)]
    #[case::id_mode(ColumnMappingMode::Id)]
    #[case::none_mode(ColumnMappingMode::None)]
    fn test_logical_to_physical_transform(#[case] mode: ColumnMappingMode) -> KernelResult<()> {
        validate_logical_to_physical_transform(mode)
    }

    #[rstest]
    fn test_logical_to_physical_transform_and_row_tracking_state(
        #[values(
            ColumnMappingMode::None,
            ColumnMappingMode::Name,
            ColumnMappingMode::Id
        )]
        mode: ColumnMappingMode,
        #[values(
            RowTrackingState::Enabled,
            RowTrackingState::SupportedNotEnabled,
            RowTrackingState::Suspended
        )]
        row_tracking_state: RowTrackingState,
    ) -> KernelResult<()> {
        // === Given every column-mapping mode and Row Tracking state ===
        let column_mapping_mode = match mode {
            ColumnMappingMode::None => "none",
            ColumnMappingMode::Name => "name",
            ColumnMappingMode::Id => "id",
        };
        let engine: Arc<dyn Engine> =
            Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
        let mut txn = create_table(
            "memory:///row_tracking_logical_to_physical",
            schema_ref! { nullable "value": INTEGER },
            "test",
            true,
        )
        .with_table_properties([
            ("delta.columnMapping.mode", column_mapping_mode),
            ("delta.feature.rowTracking", "supported"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        let mut metadata = txn.state.effective_table_config.metadata().clone();
        for (key, value) in row_tracking_state.properties() {
            metadata = metadata.with_configuration_entry(key, value);
        }
        let logical_schema = txn.state.effective_table_config.logical_schema();
        let table_config = TableConfiguration::try_new_with_schema(
            &txn.state.effective_table_config,
            metadata,
            logical_schema,
        )?;
        txn.replace_effective_table_config(table_config);
        let write_state = txn.write_state()?;
        let base_logical_field = write_state
            .base_logical_data_schema
            .fields()
            .next()
            .ok_or_else(|| KernelError::internal_error("base logical data field is missing"))?
            .clone();
        let base_physical_field = write_state
            .base_physical_data_schema
            .fields()
            .next()
            .ok_or_else(|| KernelError::internal_error("base physical data field is missing"))?
            .clone();
        let physical_row_id_name = write_state
            .materialized_row_id_column_name
            .clone()
            .ok_or_else(|| {
                KernelError::internal_error("materialized Row ID column name is missing")
            })?;
        let physical_row_commit_version_name = write_state
            .materialized_row_commit_version_column_name
            .clone()
            .ok_or_else(|| {
                KernelError::internal_error(
                    "materialized Row Commit Version column name is missing",
                )
            })?;

        // === When the connector specifies its row-tracking input columns ===
        let write_context = write_state
            .write_context_builder()
            .with_row_tracking_columns(RowTrackingMetadataColumns {
                row_id_col_name: Some("connector_row_id"),
                row_commit_version_col_name: Some("connector_row_commit_version"),
            })
            .build();

        // === Then ===
        // Supplying materialized row-tracking columns requires Row Tracking to be enabled.
        // Logical data schema, physical data schema, and logical_to_physical expression must be
        // generated correctly.
        if row_tracking_state != RowTrackingState::Enabled {
            assert_result_error_with_message(
                write_context,
                "Kernel does not allow writing materialized Row IDs or Row Commit Versions when \
                 Row Tracking is not enabled",
            );
            return Ok(());
        }
        let write_context = write_context?;
        assert_eq!(
            write_context.logical_data_schema(),
            &schema_ref! {
                (base_logical_field),
                nullable "connector_row_id": LONG,
                nullable "connector_row_commit_version": LONG,
            }
        );
        assert_eq!(
            write_context.physical_data_schema(),
            &schema_ref! {
                (base_physical_field),
                nullable (physical_row_id_name): LONG,
                nullable (physical_row_commit_version_name): LONG,
            }
        );
        assert_eq!(
            write_context.logical_to_physical(),
            Arc::new(Expression::struct_patch(
                ExpressionStructPatchBuilder::new()
            )?)
        );
        Ok(())
    }

    #[test]
    fn disallow_catalog_committer_for_non_catalog_managed_table() {
        let storage = Arc::new(InMemory::new());
        let table_root = url::Url::parse("memory:///").unwrap();
        let engine = crate::engine::sync::SyncEngine::new_with_store(storage.clone());

        // Create a non-catalog-managed table (no catalogManaged feature)
        let actions = [
            r#"{"commitInfo":{"timestamp":12345678900,"inCommitTimestamp":12345678900}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["inCommitTimestamp"]}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true"},"createdTime":1234567890}}"#,
        ].join("\n");

        let commit_path = Path::from("_delta_log/00000000000000000000.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(storage.put(&commit_path, actions.into()))
            .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

        // Try to commit with a catalog committer to a non-catalog-managed table
        let committer = Box::new(MockCatalogCommitter);
        let err = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, committer)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::KernelError::Generic(e) if e.contains("This table is path-based and cannot be committed to with a catalog committer")
        ));
    }

    #[test]
    fn disallow_catalog_committer_for_non_catalog_managed_create_table() {
        let storage = Arc::new(InMemory::new());
        let engine = crate::engine::sync::SyncEngine::new_with_store(storage);

        // Create a non-catalog-managed table using a catalog committer
        let schema = schema_ref! { nullable "id": INTEGER };
        let committer = Box::new(MockCatalogCommitter);
        let err = create_table("memory:///", schema, "test-engine", true)
            .build(&engine, committer)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::KernelError::Generic(e) if e.contains("This table is path-based and cannot be committed to with a catalog committer")
        ));
    }

    #[rstest]
    fn conflicted_transaction_fetches_history_on_each_call(
        #[values(false, true)] fail_first_read: bool,
    ) -> KernelResult<()> {
        let storage = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(storage);
        let table_root = "memory:///lazy-conflict/";
        let snapshot = create_table(
            table_root,
            schema_ref! { nullable "id": INTEGER },
            "test-engine",
            true,
        )
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
        let transaction = snapshot
            .clone()
            .update_table(UpdateTableOperation::Write, "pending", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        let winner = snapshot
            .update_table(UpdateTableOperation::Write, "winner", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_post_commit_snapshot();
        let CommitResult::Conflicted(conflicted) = transaction.commit(&engine)? else {
            panic!("Expected a confirmed version conflict");
        };
        assert_eq!(conflicted.conflict_version(), 1);
        if fail_first_read {
            let unavailable = SyncEngine::new_with_store(Arc::new(InMemory::new()));
            assert!(conflicted.get_winning_commits(&unavailable).is_err());
            assert_eq!(conflicted.transaction.state.read_snapshot()?.version(), 0);
        }
        let commits = conflicted.get_winning_commits(&engine)?;
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].version, 1);
        winner
            .update_table(UpdateTableOperation::Write, "later-winner", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_committed();
        let refreshed = conflicted.get_winning_commits(&engine)?;
        assert_eq!(refreshed.len(), 2);
        assert_eq!(commits.len(), 1);
        assert_eq!(conflicted.conflict_version(), 1);
        assert_eq!(conflicted.transaction.state.read_snapshot()?.version(), 0);
        Ok(())
    }

    struct CapturingCommitter {
        captured: Arc<Mutex<Option<i64>>>,
    }

    impl CapturingCommitter {
        fn new() -> (Self, Arc<Mutex<Option<i64>>>) {
            let captured = Arc::new(Mutex::new(None));
            (
                Self {
                    captured: captured.clone(),
                },
                captured,
            )
        }
    }

    impl Committer for CapturingCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            commit_metadata: CommitMetadata,
        ) -> KernelResult<CommitResponse> {
            *self.captured.lock().unwrap() = Some(commit_metadata.in_commit_timestamp());
            Ok(CommitResponse::Conflict {
                version: commit_metadata.version(),
            })
        }
        fn is_catalog_committer(&self) -> bool {
            false
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> KernelResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_commit_metadata_receives_ict_not_wall_time() -> Result<()> {
        // Set up a table with ICT enabled and a very high previous ICT so that the
        // monotonicity rule (max(wall_time, prev_ict + 1)) produces a value strictly
        // greater than the current wall time. This lets us verify the computed ICT is
        // passed to CommitMetadata (not the wall-clock timestamp).
        let tempdir = tempfile::tempdir().unwrap();
        let log_dir = tempdir.path().join("_delta_log");
        std::fs::create_dir_all(&log_dir).unwrap();

        let future_ict: i64 = 9_999_999_999_999; // far-future timestamp in ms
        let commit_info = serde_json::json!({
            "commitInfo": {
                "timestamp": 1000,
                "operation": "WRITE",
                "inCommitTimestamp": future_ict
            }
        });
        let protocol = serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": ["inCommitTimestamp"]
            }
        });
        let schema_json = serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "id",
                "type": "integer",
                "nullable": true,
                "metadata": {}
            }]
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "test-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_json.to_string(),
                "partitionColumns": [],
                "configuration": {
                    "delta.enableInCommitTimestamps": "true"
                }
            }
        });
        let commit0 = format!("{commit_info}\n{protocol}\n{metadata}\n");
        std::fs::write(log_dir.join("00000000000000000000.json"), commit0).unwrap();

        let table_url = Url::from_directory_path(tempdir.path()).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

        let prev_ict = snapshot.reader().get_in_commit_timestamp(&engine)?;
        assert_eq!(prev_ict, Some(future_ict));

        let (committer, captured_ts) = CapturingCommitter::new();
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(committer))?;
        add_dummy_file(&mut txn);

        let result = txn.commit(&engine)?;
        assert!(
            matches!(result, CommitResult::Conflicted(_)),
            "Expected Conflicted from capturing committer"
        );

        // The ICT in CommitMetadata must be prev_ict + 1 (monotonicity), NOT the wall time.
        let captured = captured_ts
            .lock()
            .unwrap()
            .expect("should have captured a timestamp");
        assert_eq!(
            captured,
            future_ict + 1,
            "CommitMetadata.in_commit_timestamp should be the computed ICT (prev_ict + 1), \
             not the wall-clock time"
        );
        Ok(())
    }

    // ===== Commit failure-metric tests =====

    fn commit_failure_event(reporter: &CapturingReporter) -> Option<TransactionCommitFailure> {
        reporter.events().into_iter().find_map(|event| match event {
            MetricEvent::TransactionCommitFailure(f) => Some(f),
            _ => None,
        })
    }

    #[test]
    fn test_commit_io_error_emits_retryable_io_failure_metric() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(
                engine.as_ref(),
                Box::new(IoErrorCommitter {
                    confirmed_rejection: false,
                }),
            )?;
        add_dummy_file(&mut txn);
        let result = txn.commit(engine.as_ref())?;
        assert!(matches!(result, CommitResult::Retryable(_)));
        let failure = commit_failure_event(&reporter).expect("commit failure event");
        assert_eq!(failure.reason, CommitFailureReason::RetryableIo);
        assert_eq!(failure.table_type, TableType::PathBased);
        Ok(())
    }

    #[test]
    fn test_commit_terminal_error_emits_error_failure_metric() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(engine.as_ref(), Box::new(GenericErrorCommitter))?;
        add_dummy_file(&mut txn);
        assert!(txn.commit(engine.as_ref()).is_err());
        let failure = commit_failure_event(&reporter).expect("commit failure event");
        assert_eq!(failure.reason, CommitFailureReason::Error);
        assert_eq!(failure.table_type, TableType::PathBased);
        Ok(())
    }
}
