use std::collections::{HashMap, HashSet};
use std::iter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, LazyLock, OnceLock};

use delta_kernel_derive::internal_api;
use tracing::{info, instrument};

use crate::actions::{
    as_log_add_schema, get_commit_schema, get_log_remove_schema, get_log_txn_schema, CommitInfo,
    DomainMetadata, Metadata, Protocol, SetTransaction, METADATA_NAME, PROTOCOL_NAME,
};
use crate::committer::{
    CommitMetadata, CommitProtocolMetadata, CommitResponse, CommitType, Committer,
};
use crate::crc::{CrcDelta, FileStatsDelta, LazyCrc};
use crate::engine_data::FilteredEngineData;
use crate::error::Error;
use crate::expressions::UnaryExpressionOp::ToJson;
use crate::expressions::{ArrayData, ColumnName, Scalar, Transform};
use crate::log_segment::LogSegment;
use crate::partition::serialization::serialize_partition_value;
use crate::partition::validation::validate_partition_values;
use crate::path::{LogRoot, ParsedLogPath};
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, FILE_CONSTANT_VALUES_NAME,
    PARTITION_VALUES_PARSED_NAME, STATS_PARSED_NAME, TAGS_NAME,
};
use crate::scan::scan_row_schema;
use crate::schema::{ArrayType, MapType, SchemaRef, StructField, StructType, StructTypeBuilder};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, FileMeta, IntoEngineData, RowVisitor,
    Version,
};

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

mod commit_info;
mod domain_metadata;
mod stats_verifier;
mod update;
mod write_context;

use stats_verifier::StatsVerifier;
use write_context::SharedWriteState;
pub use write_context::WriteContext;

/// Type alias for an iterator of [`EngineData`] results.
pub(crate) type EngineDataResultIterator<'a> =
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a>;

/// The static instance referenced by [`add_files_schema`] that doesn't contain the dataChange
/// column.
pub(crate) static MANDATORY_ADD_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
    ]))
});

/// Returns a reference to the mandatory fields in an add action.
///
/// Note this does not include "dataChange" which is a required field but
/// but should be set on the transactoin level. Getting the full schema
/// can be done with [`Transaction::add_files_schema`].
pub(crate) fn mandatory_add_file_schema() -> &'static SchemaRef {
    &MANDATORY_ADD_FILE_SCHEMA
}

/// The base schema for add file metadata, referenced by [`Transaction::add_files_schema`].
///
/// The `stats` field represents the minimum structure. The actual stats written by
/// [`DefaultEngine::write_parquet`] include additional fields computed from the data:
/// - `nullCount`: nested struct mirroring the data schema (all fields LONG)
/// - `minValues`: nested struct with min/max eligible column types
/// - `maxValues`: nested struct with min/max eligible column types
///
/// The nested structures within nullCount/minValues/maxValues depend on the table's data schema
/// and which columns have statistics enabled. Use [`Transaction::stats_schema`] to get the
/// expected stats schema for a specific table.
///
/// [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
pub(crate) static BASE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let stats = StructField::nullable(
        "stats",
        DataType::struct_type_unchecked(vec![
            StructField::nullable("numRecords", DataType::LONG),
            // nullCount, minValues, maxValues are dynamic based on data schema.
            // Empty struct placeholders indicate these fields exist but their inner
            // structure depends on the table schema and stats column configuration.
            StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]),
    );

    StructTypeBuilder::from_schema(mandatory_add_file_schema())
        .add_field(stats)
        .build_arc_unchecked()
});

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

/// The static instance referenced by [`add_files_schema`] that contains the dataChange column.
static ADD_FILES_SCHEMA_WITH_DATA_CHANGE: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = BASE_ADD_FILES_SCHEMA.fields().collect::<Vec<_>>();
    let len = fields.len();
    let insert_position = fields
        .iter()
        .position(|f| f.name() == "modificationTime")
        .unwrap_or(len);
    fields.insert(insert_position + 1, &DATA_CHANGE_COLUMN);
    Arc::new(StructType::new_unchecked(fields.into_iter().cloned()))
});

/// Extend a schema with a statistics column and return a new SchemaRef.
///
/// The stats column is of type string as required by the spec.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_stats_col(schema: &SchemaRef) -> SchemaRef {
    StructTypeBuilder::from_schema(schema)
        .add_field(StructField::nullable("stats", DataType::STRING))
        .build_arc_unchecked()
}

/// Extend a schema with row tracking columns and return a new SchemaRef.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_row_tracking_cols(schema: &SchemaRef) -> SchemaRef {
    StructTypeBuilder::from_schema(schema)
        .add_field(StructField::nullable("baseRowId", DataType::LONG))
        .add_field(StructField::nullable(
            "defaultRowCommitVersion",
            DataType::LONG,
        ))
        .build_arc_unchecked()
}

/// Marker type for transactions on existing tables.
///
/// This is the default state for [`Transaction`] and provides the full set of operations
/// including file removal, deletion vector updates, and blind append semantics.
#[derive(Debug)]
pub struct ExistingTable;

/// Marker type for create-table transactions.
///
/// Transactions in this state have a restricted API surface — operations that are semantically
/// invalid for table creation (e.g. file removal, domain metadata removal) are not available.
#[derive(Debug)]
pub struct CreateTable;

/// A transaction represents an in-progress write to a table. After creating a transaction, changes
/// to the table may be staged via the transaction methods before calling `commit` to commit the
/// changes to the table.
///
/// The type parameter `S` controls which operations are available:
/// - [`ExistingTable`] (default): Full API for modifying existing tables.
/// - [`CreateTable`]: Restricted API for table creation (see
///   [`CreateTableTransaction`](create_table::CreateTableTransaction)).
///
/// # Examples
///
/// ```rust,ignore
/// // create a transaction
/// let mut txn = table.new_transaction(&engine)?;
/// // stage table changes (right now only commit info)
/// txn.commit_info(Box::new(ArrowEngineData::new(engine_commit_info)));
/// // commit! (consume the transaction)
/// txn.commit(&engine)?;
/// ```
pub struct Transaction<S = ExistingTable> {
    span: tracing::Span,
    // The snapshot this transaction is based on. None for CREATE TABLE (no pre-existing table).
    // Use `read_snapshot()` to access; it returns an error if None.
    read_snapshot_opt: Option<SnapshotRef>,
    // The table configuration that this commit will produce. For writes that don't change the
    // config, this is cloned from the read snapshot; when the config changes (e.g. schema
    // evolution), it is constructed separately with the new schema/protocol.
    effective_table_config: TableConfiguration,
    // Whether to emit a Protocol action. True for CREATE TABLE and ALTER TABLE, false otherwise.
    should_emit_protocol: bool,
    // Whether to emit a Metadata action. True for CREATE TABLE and ALTER TABLE, false otherwise.
    should_emit_metadata: bool,
    committer: Box<dyn Committer>,
    operation: Option<String>,
    engine_info: Option<String>,
    engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    add_files_metadata: Vec<Box<dyn EngineData>>,
    remove_files_metadata: Vec<FilteredEngineData>,
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
    // System-generated domain metadata (from transforms, e.g., clustering).
    // TODO(#1779): Currently only populated during CREATE TABLE. For inserts, row tracking
    // domain metadata is handled separately via `row_tracking_high_watermark` parameter in
    // `generate_domain_metadata_actions`. Consider unifying system domain handling.
    system_domain_metadata_additions: Vec<DomainMetadata>,
    // Domain names to remove in this transaction. The configuration values are fetched during
    // commit from the log to preserve the pre-image in tombstones.
    user_domain_removals: Vec<String>,
    // Whether this transaction contains any logical data changes.
    data_change: bool,
    // Whether this transaction should be marked as a blind append.
    is_blind_append: bool,
    // Files matched by update_deletion_vectors() with new DV descriptors appended. These are used
    // to generate remove/add action pairs during commit, ensuring file statistics are preserved.
    dv_matched_files: Vec<FilteredEngineData>,
    // Clustering columns from domain metadata. Only populated if the ClusteredTable feature is
    // enabled. Used for determining which columns require statistics collection. Expected to be
    // physical column names.
    physical_clustering_columns: Option<Vec<ColumnName>>,
    // See `shared_write_state()` method.
    shared_write_state: OnceLock<Arc<SharedWriteState>>,
    // PhantomData marker for transaction state (ExistingTable or CreateTable).
    // Zero-sized; only affects the type system.
    _state: PhantomData<S>,
}

impl<S> std::fmt::Debug for Transaction<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version_info = match &self.read_snapshot_opt {
            Some(snap) => format!("{}", snap.version()),
            None => "create_table".to_string(),
        };
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, engine_info: {} }}",
            version_info,
            self.engine_info.is_some()
        ))
    }
}

/// Transforms add file metadata into commit-ready add actions by converting stats to JSON
/// and setting the `dataChange` field.
fn build_add_actions<'a, I, T>(
    engine: &dyn Engine,
    add_files_metadata: I,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    data_change: bool,
) -> impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + 'a
where
    I: Iterator<Item = DeltaResult<T>> + Send + 'a,
    T: Deref<Target = dyn EngineData> + Send + 'a,
{
    let evaluation_handler = engine.evaluation_handler();
    add_files_metadata.map(move |add_files_batch| {
        let transform = Expression::transform(
            Transform::new_top_level()
                .with_inserted_field(
                    Some("modificationTime"),
                    Expression::literal(data_change).into(),
                )
                .with_replaced_field(
                    "stats",
                    Expression::unary(ToJson, Expression::column(["stats"])).into(),
                ),
        );
        let adds_expr = Expression::struct_from([transform]);
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            input_schema.clone(),
            Arc::new(adds_expr),
            as_log_add_schema(output_schema.clone()).into(),
        )?;
        adds_evaluator.evaluate(add_files_batch?.deref())
    })
}

// =============================================================================
// Shared methods available on ALL transaction types
// =============================================================================
impl<S> Transaction<S> {
    /// Consume the transaction and commit it to the table. The result is a result of
    /// [CommitResult] with the following semantics:
    /// - Ok(CommitResult) for either success or a recoverable error (includes the failed
    ///   transaction in case of a conflict so the user can retry, etc.)
    /// - Err(Error) indicates a non-retryable error (e.g. logic/validation error).
    #[instrument(
        parent = &self.span,
        name = "txn.commit",
        skip_all,
        fields(
            commit_version = self.get_commit_version(),
        ),
        err
    )]
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult<S>> {
        info!(
            num_add_files = self.add_files_metadata.len(),
            num_remove_files = self.remove_files_metadata.len(),
            num_dv_updates = self.dv_matched_files.len(),
        );
        // Step 1: Check for duplicate app_ids and generate set transactions (`txn`)
        // Note: The commit info must always be the first action in the commit but we generate it in
        // step 2 to fail early on duplicate transaction appIds
        // TODO(zach): we currently do this in two passes - can we do it in one and still keep refs
        // in the HashSet?
        let mut app_ids = HashSet::with_capacity(self.set_transactions.len());
        if let Some(dup) = self
            .set_transactions
            .iter()
            .find(|t| !app_ids.insert(&t.app_id))
        {
            return Err(Error::generic(format!(
                "app_id {} already exists in transaction",
                dup.app_id
            )));
        }

        self.validate_blind_append_semantics()?;

        // CDF check only applies to existing tables (not create table)
        // If there are add and remove files with data change in the same transaction, we block it.
        // This is because kernel does not yet have a way to discern DML operations. For DML
        // operations that perform updates on rows, ChangeDataFeed requires that a `cdc` file be
        // written to the delta log.
        if !self.is_create_table()
            && !self.add_files_metadata.is_empty()
            && !self.remove_files_metadata.is_empty()
            && self.data_change
        {
            let cdf_enabled = self
                .effective_table_config
                .table_properties()
                .enable_change_data_feed
                .unwrap_or(false);
            require!(
                !cdf_enabled,
                Error::generic(
                    "Cannot add and remove data in the same transaction when Change Data Feed is enabled (delta.enableChangeDataFeed = true). \
                     This would require writing CDC files for DML operations, which is not yet supported. \
                     Consider using separate transactions: one to add files, another to remove files."
                )
            );
        }

        // Validate clustering column stats if ClusteredTable feature is enabled
        self.validate_add_files_stats(&self.add_files_metadata)?;

        // Step 1: Generate SetTransaction actions
        let set_transaction_actions = self
            .set_transactions
            .clone()
            .into_iter()
            .map(|txn| txn.into_engine_data(get_log_txn_schema().clone(), engine));

        // Step 2: Construct commit info with ICT if enabled
        let in_commit_timestamp = self.get_in_commit_timestamp(engine)?;
        let kernel_commit_info = CommitInfo::new(
            self.commit_timestamp,
            in_commit_timestamp,
            self.operation.clone(),
            self.engine_info.clone(),
            self.is_blind_append,
        );
        let commit_info_action = self.generate_commit_info(engine, kernel_commit_info);

        // Step 3: Generate Protocol and Metadata actions based on emit flags
        let (protocol_action, protocol) = if self.should_emit_protocol {
            let protocol = self.effective_table_config.protocol().clone();
            let schema = get_commit_schema().project(&[PROTOCOL_NAME])?;
            let action = protocol.clone().into_engine_data(schema, engine)?;
            (Some(action), Some(protocol))
        } else {
            (None, None)
        };
        let (metadata_action, metadata) = if self.should_emit_metadata {
            let metadata = self.effective_table_config.metadata().clone();
            let schema = get_commit_schema().project(&[METADATA_NAME])?;
            let action = metadata.clone().into_engine_data(schema, engine)?;
            (Some(action), Some(metadata))
        } else {
            (None, None)
        };

        // Step 4: Generate add actions and get data for domain metadata actions (e.g. row tracking
        // high watermark)
        let commit_version = self.get_commit_version();
        let (add_actions, row_tracking_domain_metadata) =
            self.generate_adds(engine, commit_version)?;

        // Step 4b: Generate all domain metadata actions (user and system domains)
        let (domain_metadata_actions, dm_changes) =
            self.generate_domain_metadata_actions(engine, row_tracking_domain_metadata)?;

        // Step 5: Generate DV update actions (remove/add pairs) if any DV updates are present
        let dv_update_actions = self.generate_dv_update_actions(engine)?;

        // Step 6: Generate remove actions (collect to avoid borrowing self)
        let remove_actions =
            self.generate_remove_actions(engine, self.remove_files_metadata.iter(), &[])?;

        // Build the action chain
        // For create-table: CommitInfo -> Protocol -> Metadata -> adds -> txns -> domain_metadata
        // -> removes For existing table: CommitInfo -> adds -> txns -> domain_metadata ->
        // removes
        let actions = iter::once(commit_info_action)
            .chain(protocol_action.map(Ok))
            .chain(metadata_action.map(Ok))
            .chain(add_actions)
            .chain(set_transaction_actions)
            .chain(domain_metadata_actions);

        let filtered_actions = actions
            .map(|action_result| action_result.map(FilteredEngineData::with_all_rows_selected))
            .chain(remove_actions)
            .chain(dv_update_actions);

        // Step 7: Commit via the committer
        let commit_metadata = self.create_commit_metadata(
            commit_version,
            in_commit_timestamp,
            protocol,
            metadata,
            dm_changes.clone(),
        )?;
        match self
            .committer
            .commit(engine, Box::new(filtered_actions), commit_metadata)
        {
            Ok(CommitResponse::Committed { file_meta }) => {
                let bin_boundaries = self
                    .read_snapshot_opt
                    .as_ref()
                    .and_then(|snap| snap.get_file_stats_if_loaded())
                    .and_then(|s| s.file_size_histogram)
                    .map(|h| h.sorted_bin_boundaries);
                let crc_delta = self.build_crc_delta(
                    in_commit_timestamp,
                    dm_changes,
                    bin_boundaries.as_deref(),
                )?;
                Ok(CommitResult::CommittedTransaction(
                    self.into_committed(file_meta, crc_delta)?,
                ))
            }
            Ok(CommitResponse::Conflict { version }) => Ok(CommitResult::ConflictedTransaction(
                self.into_conflicted(version),
            )),
            // TODO: we may want to be more or less selective about what is retryable (this is tied
            // to the idea of "what kind of Errors should write_json_file return?")
            Err(e @ Error::IOError(_)) => {
                Ok(CommitResult::RetryableTransaction(self.into_retryable(e)))
            }
            Err(e) => Err(e),
        }
    }

    /// Set the data change flag.
    ///
    /// True indicates this commit is a "data changing" commit. False indicates table data was
    /// reorganized but not materially modified.
    ///
    /// Data change might be set to false in the following scenarios:
    /// 1. Operations that only change metadata (e.g. backfilling statistics)
    /// 2. Operations that make no logical changes to the contents of the table (i.e. rows are only
    ///    moved from old files to new ones.  OPTIMIZE commands is one example of this type of
    ///    optimizaton).
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = data_change;
        self
    }

    /// Same as [`Transaction::with_data_change`] but set the value directly instead of
    /// using a fluent API.
    #[internal_api]
    #[allow(dead_code)] // used in FFI
    pub(crate) fn set_data_change(&mut self, data_change: bool) {
        self.data_change = data_change;
    }

    /// Set the engine info field of this transaction's commit info action. This field is optional.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    /// Set the content of the commitInfo action for this transaction. Note that kernel will
    /// _always_ write a commitInfo, this function simply allows engines to add their own data
    /// into that action if they wish. Note that the following fields in `engine_commit_info`
    /// will be overridden by kernel if they are set (meaning you should not set them):
    /// - timestamp
    /// - inCommitTimestamp
    /// - operation
    /// - operationParameters
    /// - kernelVersion
    /// - isBlindAppend
    /// - engineInfo
    /// - txnId
    pub fn with_commit_info(
        mut self,
        engine_commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.engine_commit_info = Some((engine_commit_info, commit_info_schema));
        self
    }

    /// Include a SetTransaction (app_id and version) action for this transaction (with an optional
    /// `last_updated` timestamp).
    /// Note that each app_id can only appear once per transaction. That is, multiple app_ids with
    /// different versions are disallowed in a single transaction. If a duplicate app_id is
    /// included, the `commit` will fail (that is, we don't eagerly check app_id validity here).
    pub fn with_transaction_id(mut self, app_id: String, version: i64) -> Self {
        let set_transaction = SetTransaction::new(app_id, version, Some(self.commit_timestamp));
        self.set_transactions.push(set_transaction);
        self
    }

    /// Set domain metadata to be written to the Delta log.
    /// Note that each domain can only appear once per transaction. That is, multiple configurations
    /// of the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the commit will
    /// fail (that is, we don't eagerly check domain validity here).
    /// Setting metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata(mut self, domain: String, configuration: String) -> Self {
        self.user_domain_metadata_additions
            .push(DomainMetadata::new(domain, configuration));
        self
    }

    /// Determines the commit type based on whether this is a create-table operation and whether
    /// the table is catalog-managed.
    fn determine_commit_type(
        is_create: bool,
        table_config: &crate::table_configuration::TableConfiguration,
    ) -> CommitType {
        let is_catalog_managed = table_config.is_catalog_managed();

        // TODO: Handle UpgradeToCatalogManaged and DowngradeToPathBased when ALTER TABLE
        // SET TBLPROPERTIES is supported.
        match (is_create, is_catalog_managed) {
            (true, true) => CommitType::CatalogManagedCreate,
            (true, false) => CommitType::PathBasedCreate,
            (false, true) => CommitType::CatalogManagedWrite,
            (false, false) => CommitType::PathBasedWrite,
        }
    }

    /// Validates that the committer type matches the commit type. A catalog committer must be
    /// used for catalog-managed operations, and a non-catalog committer for path-based operations.
    fn validate_commit_type(
        is_catalog_committer: bool,
        commit_type: &CommitType,
    ) -> DeltaResult<()> {
        match (
            is_catalog_committer,
            commit_type.requires_catalog_committer(),
        ) {
            (true, true) | (false, false) => Ok(()),
            (false, true) => Err(Error::generic(
                "This table is catalog-managed and requires a catalog committer. \
                 Please provide a catalog committer via Snapshot::transaction().",
            )),
            (true, false) => Err(Error::generic(
                "This table is path-based and cannot be committed to with a catalog committer.",
            )),
        }
    }

    /// Builds the [`CommitMetadata`] for this transaction. Determines the commit type,
    /// validates the committer, and assembles the protocol/metadata state.
    fn create_commit_metadata(
        &self,
        commit_version: Version,
        in_commit_timestamp: Option<i64>,
        new_protocol: Option<Protocol>,
        new_metadata: Option<Metadata>,
        domain_metadata_changes: Vec<crate::actions::DomainMetadata>,
    ) -> DeltaResult<CommitMetadata> {
        let log_root = LogRoot::new(self.effective_table_config.table_root().clone())?;
        let is_create = self.is_create_table();
        let commit_type = Self::determine_commit_type(is_create, &self.effective_table_config);
        Self::validate_commit_type(self.committer.is_catalog_committer(), &commit_type)?;
        // For create-table: previous P&M is None (no prior table), new P&M is set.
        // For existing table with metadata change: previous P&M is from snapshot, new P&M
        // is from effective config.
        // For existing table without metadata change: previous P&M is from snapshot, new is None.
        let (read_protocol, read_metadata, max_published_version) = if is_create {
            (None, None, None)
        } else {
            let snap = self.read_snapshot()?;
            let read_config = snap.table_configuration();
            (
                Some(read_config.protocol().clone()),
                Some(read_config.metadata().clone()),
                snap.log_segment().listed.max_published_version,
            )
        };
        let protocol_metadata = CommitProtocolMetadata::try_new(
            read_protocol,
            read_metadata,
            new_protocol,
            new_metadata,
        )?;
        Ok(CommitMetadata::new(
            log_root,
            commit_version,
            commit_type,
            in_commit_timestamp.unwrap_or(self.commit_timestamp),
            max_published_version,
            protocol_metadata,
            domain_metadata_changes,
        ))
    }

    /// Validate that the transaction is eligible to be marked as a blind append.
    ///
    /// Note: Domain metadata additions/removals are allowed; blind append only constrains
    /// data-file operations and read predicates. Conflict resolution determines whether
    /// metadata changes are problematic.
    fn validate_blind_append_semantics(&self) -> DeltaResult<()> {
        if !self.is_blind_append {
            return Ok(());
        }
        require!(
            !self.is_create_table(),
            Error::invalid_transaction_state(
                "Blind append is not supported for create-table transactions",
            )
        );
        require!(
            !self.add_files_metadata.is_empty(),
            Error::invalid_transaction_state("Blind append requires at least one added data file")
        );
        require!(
            self.data_change,
            Error::invalid_transaction_state("Blind append requires data_change to be true")
        );
        require!(
            self.remove_files_metadata.is_empty(),
            Error::invalid_transaction_state("Blind append cannot remove files")
        );
        require!(
            self.dv_matched_files.is_empty(),
            Error::invalid_transaction_state("Blind append cannot update deletion vectors")
        );

        Ok(())
    }

    /// Returns true if this is a create-table transaction.
    /// A create-table transaction has no read snapshot (no pre-existing table).
    fn is_create_table(&self) -> bool {
        debug_assert!(
            self.operation.as_deref() != Some("CREATE TABLE") || self.read_snapshot_opt.is_none(),
            "CREATE TABLE operation should not have a read snapshot"
        );
        self.read_snapshot_opt.is_none()
    }

    // Returns the read snapshot. Returns an error if this is a create-table transaction.
    // To get the `Option<SnapshotRef>` directly, use the `read_snapshot_opt` field.
    fn read_snapshot(&self) -> DeltaResult<&Snapshot> {
        self.read_snapshot_opt.as_deref().ok_or_else(|| {
            Error::internal_error("read_snapshot() called on create-table transaction")
        })
    }

    /// Computes the in-commit timestamp for this transaction if ICT is enabled.
    /// Returns `None` if ICT is not enabled on the table. A feature being in the protocol
    /// (`is_feature_supported`) is not sufficient -- the `delta.enableInCommitTimestamps`
    /// property must also be `true` (`is_feature_enabled`).
    fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        let has_ict = self
            .effective_table_config
            .is_feature_enabled(&TableFeature::InCommitTimestamp);

        if !has_ict {
            return Ok(None);
        }

        if self.is_create_table() {
            // For CREATE TABLE there are no prior commits -- use the wall-clock time directly.
            return Ok(Some(self.commit_timestamp));
        }

        // Existing table: enforce monotonicity per the Delta protocol. The timestamp
        // must be the larger of:
        // - The time at which the writer attempted the commit
        // - One millisecond later than the previous commit's inCommitTimestamp
        Ok(self
            .read_snapshot()?
            .get_in_commit_timestamp(engine)?
            .map(|prev_ict| self.commit_timestamp.max(prev_ict + 1)))
    }

    /// Returns the commit version for this transaction.
    /// For existing table transactions, this is snapshot.version() + 1.
    /// For create-table transactions, this is 0.
    fn get_commit_version(&self) -> Version {
        match &self.read_snapshot_opt {
            Some(snap) => snap.version() + 1,
            None => 0,
        }
    }

    /// The schema that the [`Engine`]'s [`ParquetHandler`] is expected to use when reporting
    /// information about a Parquet write operation back to Kernel.
    ///
    /// Concretely, it is the expected schema for [`EngineData`] passed to [`add_files`], as it is
    /// the base for constructing an add_file. Each row represents metadata about a
    /// file to be added to the table. Kernel takes this information and extends it to the full
    /// add_file action schema, adding internal fields (e.g., baseRowID) as necessary.
    ///
    /// The `stats` field contains file-level statistics. The schema returned here shows the base
    /// structure; the actual stats written by [`DefaultEngine::write_parquet`] include dynamically
    /// computed fields (numRecords, nullCount, minValues, maxValues, tightBounds) based on the
    /// data schema and table configuration. See [`stats_schema`] for the table-specific expected
    /// stats schema.
    ///
    /// Note: While currently static, in the future the schema might change depending on
    /// options set on the transaction or features enabled on the table.
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    /// [`ParquetHandler`]: crate::ParquetHandler
    /// [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
    /// [`stats_schema`]: Transaction::stats_schema
    pub fn add_files_schema(&self) -> &'static SchemaRef {
        &BASE_ADD_FILES_SCHEMA
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
    pub fn stats_schema(&self) -> DeltaResult<SchemaRef> {
        let stats_schemas = self
            .effective_table_config
            .build_expected_stats_schemas(self.physical_clustering_columns.as_deref(), None)?;
        Ok(stats_schemas.physical)
    }

    /// Returns the list of column names that should have statistics collected.
    ///
    /// This returns leaf column paths as [`ColumnName`] objects. Each `ColumnName`
    /// stores path components separately (e.g., `ColumnName::new(["nested", "field"])`).
    /// See [`ColumnName`'s `Display` implementation][ColumnName#impl-Display-for-ColumnName]
    /// for details on string formatting and escaping.
    ///
    /// Engines can use this to determine which columns need stats during writes.
    ///
    /// Per the Delta protocol, clustering columns are always included in statistics,
    /// regardless of `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols` settings.
    #[allow(unused)]
    pub fn stats_columns(&self) -> Vec<ColumnName> {
        self.effective_table_config
            .physical_stats_column_names(self.physical_clustering_columns.as_deref())
    }

    // Generate the logical-to-physical transform expression which must be evaluated on every data
    // chunk before writing. At the moment, this is a transaction-wide expression.
    fn generate_logical_to_physical(&self) -> Expression {
        let partition_cols = self.effective_table_config.partition_columns().to_vec();
        // Check if materializePartitionColumns feature is enabled
        let materialize_partition_columns = self
            .effective_table_config
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns);
        // Build a Transform expression that drops partition columns from the input
        // (unless materializePartitionColumns is enabled).
        let mut transform = Transform::new_top_level();
        if !materialize_partition_columns {
            for col in &partition_cols {
                transform = transform.with_dropped_field_if_exists(col);
            }
        }
        Expression::transform(transform)
    }

    /// Returns the logical partition column names for this table.
    pub fn logical_partition_columns(&self) -> &[String] {
        self.effective_table_config.partition_columns()
    }

    /// Lazily builds and caches the [`SharedWriteState`] for this transaction.
    fn shared_write_state(&self) -> &Arc<SharedWriteState> {
        self.shared_write_state.get_or_init(|| {
            let table_config = &self.effective_table_config;
            Arc::new(SharedWriteState {
                table_root: table_config.table_root().clone(),
                logical_schema: table_config.logical_schema(),
                physical_schema: table_config.physical_write_schema(),
                logical_to_physical: Arc::new(self.generate_logical_to_physical()),
                column_mapping_mode: table_config.column_mapping_mode(),
                stats_columns: self.stats_columns(),
                logical_partition_columns: table_config.partition_columns().to_vec(),
            })
        })
    }

    /// Creates a write context for writing data to a specific partition.
    ///
    /// Performs the following validations and transformations:
    ///
    /// - **Key completeness**: ensures all partition columns are present and no extra keys exist.
    ///   For example, if the table has partition columns `["year", "region"]` and you pass
    ///   `{"year": Scalar::Integer(2024)}`, this returns an error for missing "region".
    ///
    /// - **Case normalization**: matches keys case-insensitively against the schema and normalizes
    ///   to schema case. For example, passing `"YEAR"` for a column named `"year"` is accepted and
    ///   normalized.
    ///
    /// - **Type checking**: rejects non-primitive partition column types (struct, array, map) and
    ///   validates that each non-null `Scalar`'s type matches the partition column's schema type.
    ///   For example, passing `Scalar::String("2024")` for an `INTEGER` column returns an error.
    ///   Null scalars skip the value type check (null is valid for any primitive partition column).
    ///
    /// - **Value serialization**: serializes each `Scalar` to a protocol-compliant string per the
    ///   Delta protocol's "Partition Value Serialization" rules. `Scalar::Null(...)` becomes `None`
    ///   in `add.partitionValues` (JSON null). `Scalar::String("")` also becomes `None` (empty
    ///   string equals null for all types). `Scalar::Date(19723)` becomes `Some("2024-01-01")`.
    ///
    /// - **Key translation**: translates logical column names to physical names using the table's
    ///   column mapping mode. For example, under `ColumnMappingMode::Name`, logical `"year"` might
    ///   become physical `"col-abc-123"` in the `partitionValues` map.
    ///
    /// The returned [`WriteContext`] also provides a [`write_dir`] that returns the correct
    /// target directory (Hive-style paths when column mapping is off, random prefix when on).
    ///
    /// Returns an error if the table is not partitioned (use
    /// [`unpartitioned_write_context`](Self::unpartitioned_write_context) instead).
    ///
    /// [`write_dir`]: WriteContext::write_dir
    pub fn partitioned_write_context(
        &self,
        partition_values: HashMap<String, Scalar>,
    ) -> DeltaResult<WriteContext> {
        let shared = self.shared_write_state();
        require!(
            !shared.logical_partition_columns.is_empty(),
            Error::generic("table is not partitioned; use unpartitioned_write_context() instead")
        );

        // Validate keys (completeness, case normalization) and value types, then return
        // the map re-keyed to schema case.
        let normalized = validate_partition_values(
            &shared.logical_partition_columns,
            &shared.logical_schema,
            partition_values,
        )?;

        // Serialize values and translate keys from logical to physical names.
        let mut serialized = HashMap::with_capacity(normalized.len());
        for logical_name in &shared.logical_partition_columns {
            let scalar = normalized.get(logical_name).ok_or_else(|| {
                Error::internal_error(format!(
                    "partition column '{logical_name}' missing after validation"
                ))
            })?;
            let value = serialize_partition_value(scalar)?;
            let physical_name = shared
                .logical_schema
                .field(logical_name)
                .ok_or_else(|| {
                    Error::internal_error(format!(
                        "partition column '{logical_name}' not found in schema after validation"
                    ))
                })?
                .physical_name(shared.column_mapping_mode)
                .to_string();
            serialized.insert(physical_name, value);
        }

        Ok(WriteContext {
            shared: shared.clone(),
            physical_partition_values: serialized,
        })
    }

    /// Creates a write context for writing data to an unpartitioned table.
    ///
    /// Returns an error if the table has partition columns (use
    /// [`partitioned_write_context`](Self::partitioned_write_context) instead).
    pub fn unpartitioned_write_context(&self) -> DeltaResult<WriteContext> {
        let shared = self.shared_write_state();
        require!(
            shared.logical_partition_columns.is_empty(),
            Error::generic("table is partitioned; use partitioned_write_context() instead")
        );
        Ok(WriteContext {
            shared: shared.clone(),
            physical_partition_values: HashMap::new(),
        })
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }

    /// Validate that add files have required statistics for clustering columns.
    ///
    /// Per the Delta protocol, writers MUST collect per-file statistics for clustering columns
    /// when the `ClusteredTable` feature is enabled. Other stat columns (e.g. the conventional
    /// "first 32 columns") are not validated here because they are not protocol-required.
    ///
    /// Only add files are validated — remove files do not carry statistics.
    fn validate_add_files_stats(&self, add_files: &[Box<dyn EngineData>]) -> DeltaResult<()> {
        if add_files.is_empty() {
            return Ok(());
        }
        if let Some(ref clustering_cols) = self.physical_clustering_columns {
            if !clustering_cols.is_empty() {
                let physical_schema = self.effective_table_config.physical_schema();
                let columns_with_types: Vec<(ColumnName, DataType)> = clustering_cols
                    .iter()
                    .map(|col| {
                        let data_type = physical_schema
                            .walk_column_fields(col)?
                            .last()
                            .map(|field| field.data_type().clone())
                            .ok_or_else(|| {
                                Error::internal_error(format!(
                                    "Required column '{col}' not found in table schema"
                                ))
                            })?;
                        Ok((col.clone(), data_type))
                    })
                    .collect::<DeltaResult<_>>()?;
                let verifier = StatsVerifier::new(columns_with_types);
                verifier.verify(add_files)?;
            }
        }
        Ok(())
    }

    /// Generates add actions and row tracking domain metadata for a commit.
    #[instrument(name = "txn.gen_adds", skip_all, err)]
    fn generate_adds<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: u64,
    ) -> DeltaResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
        // Note: this does not require delta.enableRowTracking=true. "supported" is sufficient
        // for writers to assign row IDs.
        let row_tracking_supported = self.effective_table_config.should_write_row_tracking();

        if self.add_files_metadata.is_empty() {
            // No files to add. For an empty CREATE TABLE with row tracking, emit the initial
            // high water mark domain metadata (rowIdHighWaterMark = -1) so subsequent writes
            // have a valid starting point. For all other empty commits (metadata-only, etc.),
            // nothing row-tracking-related needs to be written.
            let row_tracking_dm = (row_tracking_supported && self.is_create_table())
                .then(RowTrackingDomainMetadata::initial);
            return Ok((Box::new(iter::empty()), row_tracking_dm));
        }

        let commit_version = i64::try_from(commit_version)
            .map_err(|_| Error::generic("Commit version too large to fit in i64"))?;

        if row_tracking_supported {
            self.generate_adds_with_row_tracking(engine, commit_version)
        } else {
            let add_actions = build_add_actions(
                engine,
                self.add_files_metadata.iter().map(|a| Ok(a.deref())),
                self.add_files_schema().clone(),
                with_stats_col(&ADD_FILES_SCHEMA_WITH_DATA_CHANGE.clone()),
                self.data_change,
            );
            Ok((Box::new(add_actions), None))
        }
    }

    /// Generates add actions with row tracking columns and the row ID high water mark
    /// domain metadata.
    ///
    /// Visits all add file batches once to read `numRecords` per file, assigning a unique
    /// non-overlapping `baseRowId` range to each file and computing the final high water mark
    /// for the domain metadata action. The initial high water mark is read from the snapshot
    /// for existing tables, or defaults to -1 for create-table (no prior log to read from).
    fn generate_adds_with_row_tracking<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: i64,
    ) -> DeltaResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
        let row_id_high_water_mark = if self.is_create_table() {
            None
        } else {
            RowTrackingDomainMetadata::get_high_water_mark(self.read_snapshot()?, engine)?
        };

        // Create a row tracking visitor and visit all files to collect row tracking information
        let mut row_tracking_visitor =
            RowTrackingVisitor::new(row_id_high_water_mark, Some(self.add_files_metadata.len()));

        // We visit all files with the row visitor before creating the add action iterator because
        // we need to know the final row ID high water mark to create the domain metadata action.
        for add_files_batch in &self.add_files_metadata {
            row_tracking_visitor.visit_rows_of(add_files_batch.deref())?;
        }

        // Destructure the visitor to move base_row_id_batches into the add-files iterator
        // while also extracting the final high water mark for the domain metadata action.
        let RowTrackingVisitor {
            base_row_id_batches,
            row_id_high_water_mark,
        } = row_tracking_visitor;

        // Create extended add files with row tracking columns
        let extended_add_files = self.add_files_metadata.iter().zip(base_row_id_batches).map(
            move |(add_files_batch, base_row_ids)| {
                let commit_versions = vec![commit_version; base_row_ids.len()];
                let base_row_ids_array =
                    ArrayData::try_new(ArrayType::new(DataType::LONG, true), base_row_ids)?;
                let commit_versions_array =
                    ArrayData::try_new(ArrayType::new(DataType::LONG, true), commit_versions)?;

                add_files_batch.append_columns(
                    with_row_tracking_cols(&Arc::new(StructType::new_unchecked(vec![]))),
                    vec![base_row_ids_array, commit_versions_array],
                )
            },
        );

        // Generate add actions including row tracking metadata
        let add_actions = build_add_actions(
            engine,
            extended_add_files,
            with_row_tracking_cols(self.add_files_schema()),
            with_row_tracking_cols(&with_stats_col(&ADD_FILES_SCHEMA_WITH_DATA_CHANGE.clone())),
            self.data_change,
        );

        // Generate a row tracking domain metadata based on the final high water mark
        let row_tracking_domain_metadata: RowTrackingDomainMetadata =
            RowTrackingDomainMetadata::new(row_id_high_water_mark);

        Ok((Box::new(add_actions), Some(row_tracking_domain_metadata)))
    }

    fn into_committed(
        self,
        file_meta: FileMeta,
        crc_delta: CrcDelta,
    ) -> DeltaResult<CommittedTransaction> {
        let parsed_commit = ParsedLogPath::parse_commit(file_meta)?;

        let commit_version = parsed_commit.version;

        let (post_commit_stats, post_commit_snapshot) = match &self.read_snapshot_opt {
            Some(snap) => {
                // Existing table path: use the read snapshot to compute post-commit state.
                let stats = PostCommitStats {
                    commits_since_checkpoint: snap.log_segment().commits_since_checkpoint() + 1,
                    commits_since_log_compaction: snap
                        .log_segment()
                        .commits_since_log_compaction_or_checkpoint()
                        + 1,
                };
                let snapshot = snap.new_post_commit(parsed_commit, crc_delta)?;
                (stats, Arc::new(snapshot))
            }
            None => {
                // CREATE TABLE path: build a fresh Snapshot at version 0.
                let log_root = self
                    .effective_table_config
                    .table_root()
                    .join("_delta_log/")?;
                let log_segment = LogSegment::new_for_version_zero(log_root, parsed_commit)?;
                let crc = crc_delta.into_crc_for_version_zero().ok_or_else(|| {
                    Error::internal_error("CREATE TABLE CRC delta is missing protocol or metadata")
                })?;
                let stats = PostCommitStats {
                    commits_since_checkpoint: 1,
                    commits_since_log_compaction: 1,
                };
                let snapshot = Snapshot::new_with_crc(
                    log_segment,
                    self.effective_table_config,
                    Arc::new(LazyCrc::new_precomputed(crc, 0)),
                );
                (stats, Arc::new(snapshot))
            }
        };

        Ok(CommittedTransaction {
            commit_version,
            post_commit_stats,
            post_commit_snapshot: Some(post_commit_snapshot),
        })
    }

    /// Build a [`CrcDelta`] from the transaction's staged file metadata and commit state.
    fn build_crc_delta(
        &self,
        in_commit_timestamp: Option<i64>,
        dm_changes: Vec<DomainMetadata>,
        bin_boundaries: Option<&[i64]>,
    ) -> DeltaResult<CrcDelta> {
        let file_stats = FileStatsDelta::try_compute_for_txn(
            &self.add_files_metadata,
            &self.remove_files_metadata,
            bin_boundaries,
        )?;
        Ok(CrcDelta {
            file_stats,
            protocol: self
                .should_emit_protocol
                .then(|| self.effective_table_config.protocol().clone()),
            metadata: self
                .should_emit_metadata
                .then(|| self.effective_table_config.metadata().clone()),
            domain_metadata_changes: dm_changes,
            set_transaction_changes: self.set_transactions.clone(),
            in_commit_timestamp,
            operation: self.operation.clone(),
            has_missing_file_size: false, // writes always have sizes
        })
    }

    fn into_conflicted(self, conflict_version: Version) -> ConflictedTransaction<S> {
        ConflictedTransaction {
            transaction: self,
            conflict_version,
        }
    }

    fn into_retryable(self, error: Error) -> RetryableTransaction<S> {
        RetryableTransaction {
            transaction: self,
            error,
        }
    }

    /// Generates Remove actions from scan file metadata.
    ///
    /// This internal method transforms scan row metadata into Remove actions for the Delta log.
    /// It's called during commit to process files staged via [`remove_files`] or files being
    /// updated with new deletion vectors via [`update_deletion_vectors`].
    ///
    /// # Parameters
    ///
    /// - `engine`: The engine used for expression evaluation
    /// - `remove_files_metadata`: Iterator over scan file metadata to transform into Remove actions
    /// - `columns_to_drop`: Column names to drop from the scan metadata before transformation. This
    ///   is used to remove temporary columns like the intermediate deletion vector column added
    ///   during DV updates.
    ///
    /// # Returns
    ///
    /// An iterator of FilteredEngineData containing Remove actions in the log schema format.
    ///
    /// [`remove_files`]: Transaction::remove_files
    /// [`update_deletion_vectors`]: Transaction::update_deletion_vectors
    #[instrument(name = "txn.gen_removes", skip_all, err)]
    fn generate_remove_actions<'a>(
        &'a self,
        engine: &dyn Engine,
        remove_files_metadata: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
        columns_to_drop: &'a [&str],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        // Create-table transactions should not have any remove actions.
        // Only error if there are actually files queued for removal.
        if self.is_create_table() && !self.remove_files_metadata.is_empty() {
            return Err(Error::internal_error(
                "CREATE TABLE transaction cannot have remove actions",
            ));
        }

        let input_schema = scan_row_schema();
        let target_schema = schema_with_all_fields_nullable(get_log_remove_schema())?;
        let evaluation_handler = engine.evaluation_handler();

        let make_eval = |coalesce_stats_with_parsed: bool| -> DeltaResult<_> {
            let transform = build_remove_transform(
                self.commit_timestamp,
                self.data_change,
                columns_to_drop,
                coalesce_stats_with_parsed,
            );
            let expr = Arc::new(Expression::struct_from([Expression::transform(transform)]));
            evaluation_handler.new_expression_evaluator(
                input_schema.clone(),
                expr,
                target_schema.clone().into(),
            )
        };

        // Build two evaluators: one for the common case where scan files do not include a
        // stats_parsed column, and one for predicate-based scans that include stats_parsed.
        // The stats_parsed evaluator coalesces stats with ToJson(stats_parsed) to handle the
        // case where stats is null (e.g., when skip_stats=true was used) and then drops the
        // stats_parsed column.
        let base_eval = Arc::new(make_eval(false)?);
        let stats_parsed_eval = Arc::new(make_eval(true)?);
        let stats_parsed_col = ColumnName::new([STATS_PARSED_NAME]);

        Ok(remove_files_metadata.map(move |file_metadata_batch| {
            let data = file_metadata_batch.data();
            let evaluator = if data.has_field(&stats_parsed_col) {
                &stats_parsed_eval
            } else {
                &base_eval
            };
            let updated_engine_data = evaluator.evaluate(data)?;
            FilteredEngineData::try_new(
                updated_engine_data,
                file_metadata_batch.selection_vector().to_vec(),
            )
        }))
    }
}

/// Builds the transform expression for converting scan row metadata into a Remove action.
///
/// Handles two "parsed" columns that predicate-based scans add to scan metadata:
///
/// - `stats_parsed`: when `coalesce_stats_with_parsed` is true, the `stats` field is replaced with
///   `COALESCE(stats, TO_JSON(stats_parsed))` and `stats_parsed` is dropped. The coalesce handles
///   cases where `stats` is null (e.g., `skip_stats=true` or V2 checkpoints with
///   `writeStatsAsJson=false`) by reconstructing the JSON from the parsed representation.
/// - `partitionValues_parsed`: dropped if present. Unlike stats, no reconstruction is needed: the
///   Remove action's `partitionValues` is sourced from `fileConstantValues.partitionValues`, which
///   scans always populate from `add.partitionValues`.
fn build_remove_transform(
    commit_timestamp: i64,
    data_change: bool,
    columns_to_drop: &[&str],
    coalesce_stats_with_parsed: bool,
) -> Transform {
    let mut transform = Transform::new_top_level()
        // deletionTimestamp
        .with_inserted_field(Some("path"), Expression::literal(commit_timestamp).into())
        // dataChange
        .with_inserted_field(Some("path"), Expression::literal(data_change).into())
        // extended_file_metadata
        .with_inserted_field(Some("path"), Expression::literal(true).into())
        .with_inserted_field(
            Some("path"),
            Expression::column([FILE_CONSTANT_VALUES_NAME, "partitionValues"]).into(),
        );

    if coalesce_stats_with_parsed {
        // Replace stats with COALESCE(stats, TO_JSON(stats_parsed)), then insert tags after.
        // Both expressions are registered on the "stats" field_transform (is_replace=true),
        // so the evaluator emits [coalesced_stats, tags] in place of the original stats field.
        let coalesce_stats = Expression::coalesce([
            Expression::column(["stats"]),
            Expression::unary(ToJson, Expression::column([STATS_PARSED_NAME])),
        ]);
        transform = transform
            .with_replaced_field("stats", coalesce_stats.into())
            .with_inserted_field(
                Some("stats"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, TAGS_NAME]).into(),
            )
            .with_dropped_field_if_exists(STATS_PARSED_NAME);
    } else {
        // tags inserted after stats; stats passes through unchanged
        transform = transform.with_inserted_field(
            Some("stats"),
            Expression::column([FILE_CONSTANT_VALUES_NAME, TAGS_NAME]).into(),
        );
    }

    transform = transform
        .with_inserted_field(
            Some("deletionVector"),
            Expression::column([FILE_CONSTANT_VALUES_NAME, BASE_ROW_ID_NAME]).into(),
        )
        .with_inserted_field(
            Some("deletionVector"),
            Expression::column([FILE_CONSTANT_VALUES_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME]).into(),
        )
        .with_dropped_field(FILE_CONSTANT_VALUES_NAME)
        .with_dropped_field("modificationTime")
        // Added to scan output when the predicate touches a partition column.
        .with_dropped_field_if_exists(PARTITION_VALUES_PARSED_NAME);

    for column_to_drop in columns_to_drop {
        transform = transform.with_dropped_field(*column_to_drop);
    }

    transform
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
/// error occurred, the result is Err(Error).
///
/// The commit result can be one of the following:
/// - [CommittedTransaction]: the transaction was successfully committed. [PostCommitStats] and in
///   the future a post-commit snapshot can be obtained from the committed transaction.
/// - [ConflictedTransaction]: the transaction conflicted with an existing version. This transcation
///   must be rebased before retrying. (currently no rebase APIs exist, caller must create new txn)
/// - [RetryableTransaction]: an IO (retryable) error occurred during the commit. This transaction
///   can be retried without rebasing.
#[derive(Debug)]
#[must_use]
pub enum CommitResult<S = ExistingTable> {
    /// The transaction was successfully committed.
    CommittedTransaction(CommittedTransaction),
    /// This transaction conflicted with an existing version (see
    /// [ConflictedTransaction::conflict_version]). The transaction
    /// is returned so the caller can resolve the conflict (along with the version which
    /// conflicted).
    // TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
    // update the transaction to a new version etc.
    ConflictedTransaction(ConflictedTransaction<S>),
    /// An IO (retryable) error occurred during the commit.
    RetryableTransaction(RetryableTransaction<S>),
}

impl<S> CommitResult<S> {
    /// Returns true if the commit was successful.
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitResult::CommittedTransaction(_))
    }
}

impl<S: std::fmt::Debug> CommitResult<S> {
    /// Unwraps the [`CommittedTransaction`], panicking if the commit was not successful.
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::panic)]
    pub fn unwrap_committed(self) -> CommittedTransaction {
        match self {
            CommitResult::CommittedTransaction(c) => c,
            other => panic!("Expected CommittedTransaction, got: {other:?}"),
        }
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
pub struct ConflictedTransaction<S = ExistingTable> {
    // TODO: remove after rebase APIs
    #[allow(dead_code)]
    transaction: Transaction<S>,
    conflict_version: Version,
}

impl<S> ConflictedTransaction<S> {
    /// The version attempted commit that yielded a conflict
    pub fn conflict_version(&self) -> Version {
        self.conflict_version
    }
}

/// A transaction that failed to commit due to a retryable error (e.g. IO error). The transaction
/// can be recovered with `RetryableTransaction::transaction` and retried without rebasing. The
/// associated error can be inspected via `RetryableTransaction::error`.
#[derive(Debug)]
pub struct RetryableTransaction<S = ExistingTable> {
    /// The transaction that failed to commit due to a retryable error.
    pub transaction: Transaction<S>,
    /// Transient error that caused the commit to fail.
    pub error: Error,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::actions::CommitInfo;
    use crate::arrow::array::{ArrayRef, Int64Array, StringArray};
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::{FileSystemCommitter, PublishMetadata};
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{MapData, Scalar, StructData};
    use crate::object_store::local::LocalFileSystem;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::schema::MapType;
    use crate::table_features::ColumnMappingMode;
    use crate::transaction::create_table::create_table;
    use crate::utils::test_utils::{
        load_test_table, string_array_to_engine_data, test_schema_flat, test_schema_nested,
        test_schema_with_array, test_schema_with_map,
    };
    use crate::{EvaluationHandler, Snapshot};

    impl Transaction {
        /// Set clustering columns for testing purposes without needing a table
        /// with the ClusteredTable feature enabled.
        fn with_clustering_columns_for_test(mut self, columns: Vec<ColumnName>) -> Self {
            self.physical_clustering_columns = Some(columns);
            self
        }
    }

    /// A mock committer that always returns an IOError, used to test the retryable error path.
    struct IoErrorCommitter;

    impl Committer for IoErrorCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
            _commit_metadata: CommitMetadata,
        ) -> DeltaResult<CommitResponse> {
            Err(Error::IOError(std::io::Error::other("simulated IO error")))
        }
        fn is_catalog_committer(&self) -> bool {
            false
        }
        fn publish(
            &self,
            _engine: &dyn Engine,
            _publish_metadata: PublishMetadata,
        ) -> DeltaResult<()> {
            Ok(())
        }
    }

    /// A mock catalog committer, used to test catalog committer validation.
    struct MockCatalogCommitter;

    impl Committer for MockCatalogCommitter {
        fn commit(
            &self,
            _engine: &dyn Engine,
            _actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
            _commit_metadata: CommitMetadata,
        ) -> DeltaResult<CommitResponse> {
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
        ) -> DeltaResult<()> {
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
    ) -> DeltaResult<Transaction> {
        Ok(snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine)?
            .with_operation("DELETE".to_string())
            .with_engine_info("test_engine"))
    }

    // TODO: create a finer-grained unit tests for transactions (issue#1091)
    #[test]
    fn test_add_files_schema() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");

        let schema = txn.add_files_schema();
        let expected = StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable(
                "stats",
                DataType::struct_type_unchecked(vec![
                    StructField::nullable("numRecords", DataType::LONG),
                    StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])),
                    StructField::nullable("tightBounds", DataType::BOOLEAN),
                ]),
            ),
        ]);
        assert_eq!(*schema, expected.into());
        Ok(())
    }

    #[test]
    fn test_new_deletion_vector_path() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");
        let write_context = txn.unpartitioned_write_context().unwrap();

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
    fn test_physical_schema_excludes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");

        let write_context = txn.partitioned_write_context(HashMap::from([(
            "letter".to_string(),
            Scalar::String("a".into()),
        )]))?;
        let logical_schema = write_context.logical_schema();
        let physical_schema = write_context.physical_schema();

        // Logical schema should include the partition column
        assert!(
            logical_schema.contains("letter"),
            "Logical schema should contain partition column 'letter'"
        );

        // Physical schema should exclude the partition column
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

    /// Helper: loads a test table snapshot and returns both the snapshot and its write context.
    /// For partitioned tables, creates a partitioned write context with null values.
    /// Returns a snapshot and a partitioned write context (with null partition values) for the
    /// given test table. The table must be partitioned.
    fn snapshot_and_partitioned_write_context(
        table_path: &str,
    ) -> Result<(Arc<Snapshot>, WriteContext), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        let partition_cols = txn.logical_partition_columns();
        assert!(
            !partition_cols.is_empty(),
            "expected a partitioned table at {table_path}"
        );
        let schema = snapshot.schema();
        let partition_vals: HashMap<String, Scalar> = partition_cols
            .iter()
            .map(|col| {
                let dt = schema.field(col).unwrap().data_type().clone();
                (col.clone(), Scalar::Null(dt))
            })
            .collect();
        let wc = txn.partitioned_write_context(partition_vals)?;
        Ok((snapshot, wc))
    }

    /// Helper: evaluates the logical-to-physical transform on the given batch and returns the
    /// output RecordBatch.
    fn eval_logical_to_physical(
        wc: &WriteContext,
        batch: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let logical_schema = wc.logical_schema();
        let physical_schema = wc.physical_schema();
        let l2p = wc.logical_to_physical();

        let handler = ArrowEvaluationHandler;
        let evaluator = handler.new_expression_evaluator(
            logical_schema.clone(),
            l2p,
            physical_schema.clone().into(),
        )?;
        let result = ArrowEngineData::try_from_engine_data(
            evaluator.evaluate(&ArrowEngineData::new(batch))?,
        )?;
        Ok(result.record_batch().clone())
    }

    #[test]
    fn test_materialize_partition_columns_in_write_context(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Without materializePartitionColumns, partition column should be dropped
        let (snap_without, wc_without) =
            snapshot_and_partitioned_write_context("./tests/data/basic_partitioned/")?;
        let partition_cols = snap_without.table_configuration().partition_columns();
        assert_eq!(partition_cols.len(), 1);
        assert_eq!(partition_cols[0], "letter");
        assert!(
            !snap_without
                .table_configuration()
                .protocol()
                .has_table_feature(&TableFeature::MaterializePartitionColumns),
            "basic_partitioned should not have materializePartitionColumns feature"
        );
        let expr_str = format!("{}", wc_without.logical_to_physical());
        assert!(
            expr_str.contains("drop letter"),
            "Partition column 'letter' should be dropped. Expression: {expr_str}"
        );

        // With materializePartitionColumns, no columns should be dropped (identity transform)
        let (snap_with, wc_with) = snapshot_and_partitioned_write_context(
            "./tests/data/partitioned_with_materialize_feature/",
        )?;
        let partition_cols = snap_with.table_configuration().partition_columns();
        assert_eq!(partition_cols.len(), 1);
        assert_eq!(partition_cols[0], "letter");
        assert!(
            snap_with
                .table_configuration()
                .protocol()
                .has_table_feature(&TableFeature::MaterializePartitionColumns),
            "partitioned_with_materialize_feature should have materializePartitionColumns feature"
        );
        let expr_str = format!("{}", wc_with.logical_to_physical());
        assert!(
            !expr_str.contains("drop"),
            "No columns should be dropped with materializePartitionColumns. Expression: {expr_str}"
        );

        Ok(())
    }

    /// Physical schema should include partition columns when materializePartitionColumns is on.
    #[test]
    fn test_physical_schema_includes_partition_columns_when_materialized(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/partitioned_with_materialize_feature/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).at_version(1).build(&engine)?;

        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        let write_context = txn.partitioned_write_context(HashMap::from([(
            "letter".to_string(),
            Scalar::String("a".into()),
        )]))?;
        let physical_schema = write_context.physical_schema();

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

    /// Using the wrong write context method for the table's partitioning returns an error.
    #[rstest]
    #[case::partitioned_on_unpartitioned(
        "./tests/data/table-without-dv-small/",
        true,
        "not partitioned"
    )]
    #[case::unpartitioned_on_partitioned(
        "./tests/data/basic_partitioned/",
        false,
        "table is partitioned"
    )]
    fn test_wrong_write_context_method_returns_error(
        #[case] table_path: &str,
        #[case] call_partitioned: bool,
        #[case] expected_msg: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        let result = if call_partitioned {
            txn.partitioned_write_context(HashMap::from([("x".to_string(), Scalar::Integer(1))]))
        } else {
            txn.unpartitioned_write_context()
        };
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(expected_msg),
            "expected '{expected_msg}' in error, got: {err}"
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors validates table protocol requirements.
    /// Validates that attempting DV updates on unsupported tables returns protocol error.
    #[test]
    fn test_update_deletion_vectors_unsupported_table() -> Result<(), Box<dyn std::error::Error>> {
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

    /// Tests that update_deletion_vectors validates DV descriptors match scan files.
    /// Validates detection of mismatch between provided DV descriptors and actual files.
    #[test]
    fn test_update_deletion_vectors_mismatch_count() -> Result<(), Box<dyn std::error::Error>> {
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
            "Expected error about mismatched count (expected 1 descriptor, 0 matched files), got: {err_msg}");
        Ok(())
    }

    /// Tests that update_deletion_vectors handles empty DV updates correctly as a no-op.
    /// This edge case occurs when a DELETE operation matches no rows.
    #[test]
    fn test_update_deletion_vectors_empty_inputs() -> Result<(), Box<dyn std::error::Error>> {
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

    // ============================================================================
    // validate_blind_append tests
    // ============================================================================
    fn add_dummy_file<S>(txn: &mut Transaction<S>) {
        let data = string_array_to_engine_data(StringArray::from(vec!["dummy"]));
        txn.add_files(data);
    }

    fn create_existing_table_txn(
    ) -> DeltaResult<(Arc<dyn Engine>, Transaction, Option<tempfile::TempDir>)> {
        let (engine, snapshot, tempdir) = load_test_table("table-without-dv-small")?;
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
        Ok((engine, txn, tempdir))
    }

    #[test]
    fn test_validate_blind_append_success() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        txn.validate_blind_append_semantics()?;
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_adds() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_requires_data_change() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        txn.set_data_change(false);
        add_dummy_file(&mut txn);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_removes() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        let remove_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["remove"]),
        ));
        txn.remove_files(remove_data);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_dv_updates() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        let dv_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
            StringArray::from(vec!["dv"]),
        ));
        txn.dv_matched_files.push(dv_data);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_validate_blind_append_rejects_create_table() -> DeltaResult<()> {
        let tempdir = tempfile::tempdir()?;
        let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
            "id",
            DataType::INTEGER,
        )])?);
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(
            crate::engine::default::DefaultEngineBuilder::new(store, Path::from("")).build(),
        );
        let mut txn = create_table(
            tempdir.path().to_str().expect("valid temp path"),
            schema,
            "test_engine",
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        // CreateTableTransaction does not expose with_blind_append() (compile-time
        // prevention per #1768). Directly set the field to test the runtime check.
        txn.is_blind_append = true;
        add_dummy_file(&mut txn);
        let result = txn.validate_blind_append_semantics();
        assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
        Ok(())
    }

    #[test]
    fn test_blind_append_sets_commit_info_flag() -> Result<(), Box<dyn std::error::Error>> {
        let commit_info = CommitInfo::new(1, None, None, None, true);
        assert_eq!(commit_info.is_blind_append, Some(true));

        let commit_info_false = CommitInfo::new(1, None, None, None, false);
        assert_eq!(commit_info_false.is_blind_append, None);
        Ok(())
    }

    #[test]
    fn test_blind_append_commit_rejects_no_adds() -> DeltaResult<()> {
        let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
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
    fn test_blind_append_commit_success() -> DeltaResult<()> {
        let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
        txn = txn.with_blind_append();
        add_dummy_file(&mut txn);
        // Blind append with add files should pass validation and proceed to commit.
        // The commit itself may fail due to schema mismatch with the dummy data,
        // but we verify validation (line 415) passes on the Ok path.
        let result = txn.commit(engine.as_ref());
        // If it fails, it should NOT be an InvalidTransactionState error
        if let Err(e) = result {
            assert!(
                !matches!(e, Error::InvalidTransactionState(_)),
                "Blind append validation should have passed, got: {e}"
            );
        }
        Ok(())
    }

    // Note: Additional test coverage for partial file matching (where some files in a scan
    // have DV updates but others don't) is provided by the end-to-end integration test
    // kernel/tests/dv.rs and kernel/tests/write.rs, which exercises
    // the full deletion vector write workflow including the DvMatchVisitor logic.

    #[test]
    fn test_commit_io_error_returns_retryable_transaction() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut txn = snapshot.transaction(Box::new(IoErrorCommitter), engine.as_ref())?;
        add_dummy_file(&mut txn);
        let result = txn.commit(engine.as_ref())?;
        assert!(
            matches!(result, CommitResult::RetryableTransaction(_)),
            "Expected RetryableTransaction, got: {result:?}"
        );
        if let CommitResult::RetryableTransaction(retryable) = result {
            assert!(
                retryable.error.to_string().contains("simulated IO error"),
                "Unexpected error: {}",
                retryable.error
            );
        }
        Ok(())
    }

    #[test]
    fn test_existing_table_txn_debug() -> DeltaResult<()> {
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
    ) -> DeltaResult<()> {
        let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_context = txn.unpartitioned_write_context().unwrap();
        crate::utils::test_utils::validate_physical_schema_column_mapping(
            write_context.logical_schema(),
            write_context.physical_schema(),
            mode,
        );
        Ok(())
    }

    /// Builds two-row [`EngineData`] with logical field names matching [`test_schema_nested`].
    fn build_test_record_batch() -> DeltaResult<Box<dyn EngineData>> {
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
        ArrowEvaluationHandler.create_many(schema, &[&[1i64.into(), info1], &[2i64.into(), info2]])
    }

    /// Validates that [`WriteContext::logical_to_physical`] correctly renames fields at all nesting
    /// levels. Builds a RecordBatch with logical names, evaluates the transform, and checks
    /// that the output uses physical names from the physical schema — including nested struct
    /// children.
    fn validate_logical_to_physical_transform(mode: ColumnMappingMode) -> DeltaResult<()> {
        let schema = test_schema_nested();
        let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
        let write_context = txn.unpartitioned_write_context().unwrap();
        let logical_schema = write_context.logical_schema();
        let physical_schema = write_context.physical_schema();
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
    fn test_logical_to_physical_transform(#[case] mode: ColumnMappingMode) -> DeltaResult<()> {
        validate_logical_to_physical_transform(mode)
    }

    #[rstest]
    #[case::dropped("./tests/data/basic_partitioned/", 2, &[])]
    #[case::kept("./tests/data/partitioned_with_materialize_feature/", 3, &["letter"])]
    fn test_partition_column_in_eval_output(
        #[case] table_path: &str,
        #[case] expected_cols: usize,
        #[case] expected_partition_cols: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::Float64Array;
        let (_snap, wc) = snapshot_and_partitioned_write_context(table_path)?;
        let batch = RecordBatch::try_new(
            Arc::new(wc.logical_schema().as_ref().try_into_arrow()?),
            vec![
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![1.5])),
            ],
        )?;
        let rb = eval_logical_to_physical(&wc, batch)?;
        assert_eq!(rb.num_columns(), expected_cols);
        for col in expected_partition_cols {
            assert!(rb.schema().fields().iter().any(|f| f.name() == *col));
        }
        Ok(())
    }

    // =========================================================================
    // Stats validation tests for clustering columns
    // =========================================================================

    /// Per-file stats configuration for test add file helpers.
    enum TestFileStats {
        /// No stats (null stats struct)
        None,
        /// Normal stats with non-null min/max
        Present,
        /// All-null column: nullCount == numRecords, null min/max
        AllNull,
    }

    /// Creates test add file metadata with configurable stats for the "value" column.
    fn create_test_add_files(paths: Vec<&str>, stats: Vec<TestFileStats>) -> Box<dyn EngineData> {
        let value_fields = vec![StructField::nullable("value", DataType::LONG)];
        let value_struct_type = DataType::struct_type_unchecked(value_fields.clone());
        let stats_type = DataType::struct_type_unchecked(vec![
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", value_struct_type.clone()),
            StructField::nullable("minValues", value_struct_type.clone()),
            StructField::nullable("maxValues", value_struct_type.clone()),
        ]);
        let stats_fields = vec![
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", value_struct_type.clone()),
            StructField::nullable("minValues", value_struct_type.clone()),
            StructField::nullable("maxValues", value_struct_type),
        ];
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable("stats", stats_type.clone()),
        ]));

        let empty_map = Scalar::Map(
            MapData::try_new(
                MapType::new(DataType::STRING, DataType::STRING, true),
                Vec::<(&str, &str)>::new(),
            )
            .unwrap(),
        );

        let rows: Vec<Vec<Scalar>> = paths
            .iter()
            .zip(stats.iter())
            .map(|(path, stat)| {
                let stats_scalar = match stat {
                    TestFileStats::None => Scalar::Null(stats_type.clone()),
                    TestFileStats::Present | TestFileStats::AllNull => {
                        let value_struct = |v: Option<i64>| {
                            let scalar = v.map_or(Scalar::Null(DataType::LONG), |n| n.into());
                            Scalar::Struct(
                                StructData::try_new(value_fields.clone(), vec![scalar]).unwrap(),
                            )
                        };
                        let (null_count, min, max) = match stat {
                            TestFileStats::Present => (
                                value_struct(Some(0)),
                                value_struct(Some(1)),
                                value_struct(Some(100)),
                            ),
                            _ => (
                                value_struct(Some(100)),
                                value_struct(None),
                                value_struct(None),
                            ),
                        };
                        Scalar::Struct(
                            StructData::try_new(
                                stats_fields.clone(),
                                vec![100i64.into(), null_count, min, max],
                            )
                            .unwrap(),
                        )
                    }
                };
                vec![
                    (*path).into(),
                    empty_map.clone(),
                    1024i64.into(),
                    1000000i64.into(),
                    stats_scalar,
                ]
            })
            .collect();
        let row_refs: Vec<&[Scalar]> = rows.iter().map(|r| r.as_slice()).collect();
        ArrowEvaluationHandler
            .create_many(schema, &row_refs)
            .unwrap()
    }

    #[test]
    fn test_stats_validation_allows_all_null_clustering_column() {
        let (engine, snapshot) = setup_non_dv_table();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_operation("WRITE".to_string())
            .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::AllNull]);

        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should pass for all-null clustering columns, got: {result:?}",
        );
    }

    #[test]
    fn test_stats_validation_when_clustering_cols_missing_stats() {
        let (engine, snapshot) = setup_non_dv_table();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_operation("WRITE".to_string())
            // Enable clustering columns for this test
            .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

        // Add files WITHOUT stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

        // Directly test the validation method instead of committing
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_err(),
            "Expected validation to fail when stats are missing for clustering columns"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Stats validation error") || err_msg.contains("no stats"),
            "Expected stats validation error, got: {err_msg}"
        );
    }

    #[test]
    fn test_stats_validation_when_clustering_stats_present() {
        let (engine, snapshot) = setup_non_dv_table();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_operation("WRITE".to_string())
            // Enable clustering columns for this test
            .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

        // Add files WITH stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::Present]);

        // Directly test the validation method
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should pass when stats are present, got: {result:?}"
        );
    }

    #[test]
    fn test_stats_validation_skipped_without_clustering() {
        let (engine, snapshot) = setup_non_dv_table();
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_operation("WRITE".to_string());
        // No clustering columns set (default)

        // Add files WITHOUT stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

        // Directly test the validation method - should pass because no clustering
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should be skipped without clustering, got: {result:?}"
        );
    }

    #[test]
    fn disallow_catalog_committer_for_non_catalog_managed_table() {
        let storage = Arc::new(InMemory::new());
        let table_root = url::Url::parse("memory:///").unwrap();
        let engine =
            crate::engine::default::DefaultEngineBuilder::new(storage.clone(), Path::from(""))
                .build();

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
            .transaction(committer, &engine)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Generic(e) if e.contains("This table is path-based and cannot be committed to with a catalog committer")
        ));
    }

    #[test]
    fn disallow_catalog_committer_for_non_catalog_managed_create_table() {
        let storage = Arc::new(InMemory::new());
        let engine =
            crate::engine::default::DefaultEngineBuilder::new(storage, Path::from("")).build();

        // Create a non-catalog-managed table using a catalog committer
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![
            crate::schema::StructField::new("id", crate::schema::DataType::INTEGER, true),
        ]));
        let committer = Box::new(MockCatalogCommitter);
        let err = create_table("memory:///", schema, "test-engine")
            .build(&engine, committer)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Generic(e) if e.contains("This table is path-based and cannot be committed to with a catalog committer")
        ));
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
            _actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
            commit_metadata: CommitMetadata,
        ) -> DeltaResult<CommitResponse> {
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
        ) -> DeltaResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_commit_metadata_receives_ict_not_wall_time() -> DeltaResult<()> {
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

        let prev_ict = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(prev_ict, Some(future_ict));

        let (committer, captured_ts) = CapturingCommitter::new();
        let mut txn = snapshot.transaction(Box::new(committer), &engine)?;
        add_dummy_file(&mut txn);

        let result = txn.commit(&engine)?;
        assert!(
            matches!(result, CommitResult::ConflictedTransaction(_)),
            "Expected ConflictedTransaction from capturing committer"
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
}
