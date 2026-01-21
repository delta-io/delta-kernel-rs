use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::deletion_vector::DeletionVectorPath;
use crate::actions::{
    as_log_add_schema, domain_metadata::scan_domain_metadatas, get_log_add_schema,
    get_log_commit_info_schema, get_log_domain_metadata_schema, get_log_remove_schema,
    get_log_txn_schema, CommitInfo, DomainMetadata, SetTransaction, INTERNAL_DOMAIN_PREFIX,
};
#[cfg(feature = "catalog-managed")]
use crate::committer::FileSystemCommitter;
use crate::committer::{CommitMetadata, CommitResponse, Committer};
use crate::engine_data::FilteredEngineData;
use crate::engine_data::{GetData, TypedGetData};
use crate::error::Error;
use crate::expressions::{column_name, ColumnName};
use crate::expressions::{ArrayData, Scalar, StructData, Transform, UnaryExpressionOp::ToJson};
use crate::path::{LogRoot, ParsedLogPath};
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::scan::log_replay::{
    get_scan_metadata_transform_expr, BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME,
    FILE_CONSTANT_VALUES_NAME, TAGS_NAME,
};
use crate::scan::{restored_add_schema, scan_row_schema};
use crate::schema::{
    ArrayType, MapType, SchemaRef, StructField, StructType, StructTypeBuilder, ToSchema,
};
use crate::snapshot::SnapshotRef;
use crate::table_features::{Operation, TableFeature};
use crate::utils::{current_time_ms, require};
use crate::FileMeta;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, ExpressionRef, IntoEngineData,
    RowVisitor, SchemaTransform, Version,
};
use delta_kernel_derive::internal_api;

// This is a workaround due to the fact that expression evaluation happens
// on the whole EngineData instead of accounting for filtered rows, which can lead to null values in
// required fields.
// TODO: Move this to a common place (dedupe from data_skipping.rs) or remove when evaluations work
// on FilteredEngineData directly.
struct NullableStatsTransform;
impl<'a> SchemaTransform<'a> for NullableStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;
        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: field.metadata.clone(),
            }),
        };
        Some(field)
    }
}

/// Type alias for an iterator of [`EngineData`] results.
pub(crate) type EngineDataResultIterator<'a> =
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a>;

/// The static instance referenced by [`add_files_schema`] that doesn't contain the dataChange column.
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

/// The static instance referenced by [`add_files_schema`].
pub(crate) static BASE_ADD_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let stats = StructField::nullable(
        "stats",
        DataType::struct_type_unchecked(vec![StructField::nullable("numRecords", DataType::LONG)]),
    );

    StructTypeBuilder::from_schema(mandatory_add_file_schema())
        .add_field(stats)
        .build_arc_unchecked()
});

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

/// Column name for temporary column used during deletion vector updates.
/// This column holds new DV descriptors appended to scan file metadata before transforming to final add actions.
static NEW_DELETION_VECTOR_NAME: &str = "newDeletionVector";

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

/// Schema for scan row data with an additional column for new deletion vector descriptors.
/// This is an intermediate schema used during deletion vector updates before transforming to final add actions.
static INTERMEDIATE_DV_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(
        scan_row_schema()
            .fields()
            .cloned()
            .chain([StructField::nullable(
                NEW_DELETION_VECTOR_NAME.to_string(),
                DeletionVectorDescriptor::to_schema(),
            )]),
    ))
});

/// Returns the intermediate schema with deletion vector column appended to scan row schema.
fn intermediate_dv_schema() -> &'static SchemaRef {
    &INTERMEDIATE_DV_SCHEMA
}

/// Schema for scan row data with nullable statistics fields.
/// Used when generating remove actions to ensure statistics can be null if missing.
// Safety: The panic here is acceptable because scan_row_schema() is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_SCAN_ROWS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(scan_row_schema().as_ref())
        .unwrap_or_else(|| panic!("Failed to transform scan_row_schema"))
        .into_owned()
        .into()
});

/// Returns the nullable scan row schema.
fn nullable_scan_rows_schema() -> &'static SchemaRef {
    &NULLABLE_SCAN_ROWS_SCHEMA
}

/// Schema for restored add actions with nullable statistics fields.
/// Used when transforming scan data back to add actions with potentially missing statistics.
// Safety: The panic here is acceptable because restored_add_schema() is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_RESTORED_ADD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(restored_add_schema())
        .unwrap_or_else(|| panic!("Failed to transform restored_add_schema"))
        .into_owned()
        .into()
});

/// Returns the nullable restored add action schema.
fn nullable_restored_add_schema() -> &'static SchemaRef {
    &NULLABLE_RESTORED_ADD_SCHEMA
}

/// Schema for add actions that is nullable for use in transforms as as a workaround to avoid issues with null values in required fields
/// that aren't selected.
// Safety: The panic here is acceptable because add_log_schema is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_ADD_LOG_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(get_log_add_schema())
        .unwrap_or_else(|| panic!("Failed to transform nullable_restored_add_schema"))
        .into_owned()
        .into()
});

/// Returns the schema for nullable restored add actions with dataChange field.
/// This schema extends the nullable restored add schema with a dataChange boolean field
/// that indicates whether the add action represents a logical data change.
fn nullable_add_log_schema() -> &'static SchemaRef {
    &NULLABLE_ADD_LOG_SCHEMA
}

/// Schema for an array of deletion vector descriptors.
/// Used when appending DV columns to scan file data.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
static STRUCT_DELETION_VECTOR_SCHEMA: LazyLock<ArrayType> =
    LazyLock::new(|| ArrayType::new(DeletionVectorDescriptor::to_schema().into(), true));

/// Returns the schema for an array of deletion vector descriptors.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
fn struct_deletion_vector_schema() -> &'static ArrayType {
    &STRUCT_DELETION_VECTOR_SCHEMA
}

/// Schema for the intermediate column holding new DV descriptors.
/// This temporary column is dropped during transformation to final add actions.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
static NEW_DV_COLUMN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![StructField::nullable(
        NEW_DELETION_VECTOR_NAME,
        DeletionVectorDescriptor::to_schema(),
    )]))
});

/// Returns the schema for the intermediate column holding new DV descriptors.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
fn new_dv_column_schema() -> &'static SchemaRef {
    &NEW_DV_COLUMN_SCHEMA
}

/// A transaction represents an in-progress write to a table. After creating a transaction, changes
/// to the table may be staged via the transaction methods before calling `commit` to commit the
/// changes to the table.
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
pub struct Transaction {
    read_snapshot: SnapshotRef,
    committer: Box<dyn Committer>,
    operation: Option<String>,
    engine_info: Option<String>,
    add_files_metadata: Vec<Box<dyn EngineData>>,
    remove_files_metadata: Vec<FilteredEngineData>,
    // NB: hashmap would require either duplicating the appid or splitting SetTransaction
    // key/payload. HashSet requires Borrow<&str> with matching Eq, Ord, and Hash. Plus,
    // HashSet::insert drops the to-be-inserted value without returning the existing one, which
    // would make error messaging unnecessarily difficult. Thus, we keep Vec here and deduplicate in
    // the commit method.
    set_transactions: Vec<SetTransaction>,
    // commit-wide timestamp (in milliseconds since epoch) - used in ICT, `txn` action, etc. to
    // keep all timestamps within the same commit consistent.
    commit_timestamp: i64,
    // Domain metadata additions for this transaction.
    domain_metadata_additions: Vec<DomainMetadata>,
    // Domain names to remove in this transaction. The configuration values are fetched during
    // commit from the log to preserve the pre-image in tombstones.
    domain_removals: Vec<String>,
    // Whether this transaction contains any logical data changes.
    data_change: bool,
    // Files matched by update_deletion_vectors() with new DV descriptors appended. These are used
    // to generate remove/add action pairs during commit, ensuring file statistics are preserved.
    dv_matched_files: Vec<FilteredEngineData>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, engine_info: {} }}",
            self.read_snapshot.version(),
            self.engine_info.is_some()
        ))
    }
}

impl Transaction {
    /// Create a new transaction from a snapshot. The snapshot will be used to read the current
    /// state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Snapshot::transaction](crate::snapshot::Snapshot::transaction) to create a transaction from
    /// a snapshot.
    pub(crate) fn try_new(
        snapshot: impl Into<SnapshotRef>,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Self> {
        let read_snapshot = snapshot.into();

        // important! before writing to the table we must check it is supported
        read_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Write)?;

        let commit_timestamp = current_time_ms()?;

        Ok(Transaction {
            read_snapshot: read_snapshot.clone(),
            committer,
            operation: None,
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp,
            domain_metadata_additions: vec![],
            domain_removals: vec![],
            data_change: true,
            dv_matched_files: vec![],
        })
    }

    /// Set the committer that will be used to commit this transaction. If not set, the default
    /// filesystem-based committer will be used. Note that the default committer is only allowed
    /// for non-catalog-managed tables. That is, you _must_ provide a committer via this API in
    /// order to write to catalog-managed tables.
    ///
    /// See [`committer`] module for more details.
    ///
    /// [`committer`]: crate::committer
    #[cfg(feature = "catalog-managed")]
    pub fn with_committer(mut self, committer: Box<dyn Committer>) -> Self {
        self.committer = committer;
        self
    }

    /// Consume the transaction and commit it to the table. The result is a result of
    /// [CommitResult] with the following semantics:
    /// - Ok(CommitResult) for either success or a recoverable error (includes the failed
    ///   transaction in case of a conflict so the user can retry, etc.)
    /// - Err(Error) indicates a non-retryable error (e.g. logic/validation error).
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        // Step 1: Check for duplicate app_ids and generate set transactions (`txn`)
        // Note: The commit info must always be the first action in the commit but we generate it in
        // step 2 to fail early on duplicate transaction appIds
        // TODO(zach): we currently do this in two passes - can we do it in one and still keep refs
        // in the HashSet?
        let mut app_ids = HashSet::new();
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

        // If there are add and remove files with data change in the same transaction, we block it.
        // This is because kernel does not yet have a way to discern DML operations. For DML
        // operations that perform updates on rows, ChangeDataFeed requires that a `cdc` file be
        // written to the delta log.
        if !self.add_files_metadata.is_empty()
            && !self.remove_files_metadata.is_empty()
            && self.data_change
        {
            let cdf_enabled = self
                .read_snapshot
                .table_configuration()
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

        // Step 1: Generate SetTransaction actions
        let set_transaction_actions = self
            .set_transactions
            .clone()
            .into_iter()
            .map(|txn| txn.into_engine_data(get_log_txn_schema().clone(), engine));

        // Step 2: Construct commit info with ICT if enabled
        let in_commit_timestamp =
            self.read_snapshot
                .get_in_commit_timestamp(engine)?
                .map(|prev_ict| {
                    // The Delta protocol requires the timestamp to be "the larger of two values":
                    // - The time at which the writer attempted the commit (current_time)
                    // - One millisecond later than the previous commit's inCommitTimestamp (last_commit_timestamp + 1)
                    self.commit_timestamp.max(prev_ict + 1)
                });
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            in_commit_timestamp,
            self.operation.clone(),
            self.engine_info.clone(),
        );
        let commit_info_action =
            commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine);

        // Step 3: Generate add actions and get data for domain metadata actions (e.g. row tracking high watermark)
        let commit_version = self.read_snapshot.version() + 1;
        let (add_actions, row_tracking_domain_metadata) =
            self.generate_adds(engine, commit_version)?;

        // Step 3b: Generate DV update actions (remove/add pairs) if any DV updates are present
        let dv_update_actions = self.generate_dv_update_actions(engine)?;

        // Step 4: Generate all domain metadata actions (user and system domains)
        let domain_metadata_actions =
            self.generate_domain_metadata_actions(engine, row_tracking_domain_metadata)?;

        // Step 5: Generate remove actions (collect to avoid borrowing self)
        let remove_actions =
            self.generate_remove_actions(engine, self.remove_files_metadata.iter(), &[])?;

        let actions = iter::once(commit_info_action)
            .chain(add_actions)
            .chain(set_transaction_actions)
            .chain(domain_metadata_actions);

        let filtered_actions = actions
            .map(|action_result| action_result.map(FilteredEngineData::with_all_rows_selected))
            .chain(remove_actions)
            .chain(dv_update_actions);

        // Step 6: Commit via the committer
        #[cfg(feature = "catalog-managed")]
        if self.committer.any_ref().is::<FileSystemCommitter>()
            && self
                .read_snapshot
                .table_configuration()
                .protocol()
                .is_catalog_managed()
        {
            return Err(Error::generic("The FileSystemCommitter cannot be used to commit to catalog-managed tables. Please provide a committer for your catalog via Transaction::with_committer()."));
        }
        let log_root = LogRoot::new(self.read_snapshot.table_root().clone())?;
        let commit_metadata = CommitMetadata::new(
            log_root,
            commit_version,
            self.commit_timestamp,
            self.read_snapshot.log_segment().max_published_version,
        );
        match self
            .committer
            .commit(engine, Box::new(filtered_actions), commit_metadata)
        {
            Ok(CommitResponse::Committed { file_meta }) => Ok(CommitResult::CommittedTransaction(
                self.into_committed(file_meta)?,
            )),
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
    /// 2. Operations that make no logical changes to the contents of the table (i.e. rows are only moved
    ///    from old files to new ones.  OPTIMIZE commands is one example of this type of optimizaton).
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

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commit and visible to anyone who describes the table history.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Set the engine info field of this transaction's commit info action. This field is optional.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
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
        self.domain_metadata_additions
            .push(DomainMetadata::new(domain, configuration));
        self
    }

    /// Remove domain metadata from the Delta log.
    /// If the domain exists in the Delta log, this creates a tombstone to logically delete
    /// the domain. The tombstone preserves the previous configuration value.
    /// If the domain does not exist in the Delta log, this is a no-op.
    /// Note that each domain can only appear once per transaction. That is, multiple operations
    /// on the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the `commit` will
    /// fail (that is, we don't eagerly check domain validity here).
    /// Removing metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.domain_removals.push(domain);
        self
    }

    /// Validate that user domains don't conflict with system domains or each other.
    fn validate_user_domain_operations(&self) -> DeltaResult<()> {
        let mut seen_domains = HashSet::new();

        // Validate domain additions
        for dm in &self.domain_metadata_additions {
            let domain = dm.domain();
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        // Validate domain removals
        for domain in &self.domain_removals {
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            if !seen_domains.insert(domain.as_str()) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        Ok(())
    }

    /// Helper function to convert scan metadata iterator to filtered engine data iterator.
    ///
    /// This adapter extracts the `scan_files` field from each [`crate::scan::ScanMetadata`] item,
    /// making it easy to pass scan results directly to [`Self::update_deletion_vectors`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let scan = snapshot.scan_builder().build()?;
    /// let metadata = scan.scan_metadata(engine)?;
    /// let mut dv_map = HashMap::new();
    /// // ... populate dv_map ...
    /// let files_iter = Transaction::scan_metadata_to_engine_data(metadata);
    /// txn.update_deletion_vectors(dv_map, files_iter)?;
    /// ```
    pub fn scan_metadata_to_engine_data(
        scan_metadata: impl Iterator<Item = DeltaResult<crate::scan::ScanMetadata>>,
    ) -> impl Iterator<Item = DeltaResult<FilteredEngineData>> {
        scan_metadata.map(|result| result.map(|metadata| metadata.scan_files))
    }

    /// Update deletion vectors for files in the table.
    ///
    /// This method can be called multiple times to update deletion vectors for different sets of files.
    ///
    /// This method takes a map of file paths to new deletion vector descriptors and an iterator
    /// of scan file data. It joins the two together internally and will generate appropriate
    /// remove/add actions on commit to update the deletion vectors.
    ///
    /// # Arguments
    ///
    /// * `new_dv_descriptors` - A map from data file path (as provided in scan operations) to
    ///   the new deletion vector descriptor for that file.
    /// * `existing_data_files` - An iterator over FilteredEngineData from scan metadata. The
    ///   selected elements of each FilteredEngineData must be a superset of the paths that key
    ///   `new_dv_descriptors`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A file path in `new_dv_descriptors` is not found in `existing_data_files`
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut txn = snapshot.clone().transaction(Box::new(FileSystemCommitter::new()))?
    ///     .with_operation("UPDATE".to_string());
    ///
    /// let scan = snapshot.scan_builder().build()?;
    /// let files: Vec<FilteredEngineData> = scan.scan_metadata(engine)?
    ///     .collect::<Result<Vec<_>, _>>()?
    ///     .into_iter()
    ///     .map(|sm| sm.scan_files)
    ///     .collect();
    ///
    /// // Create map of file paths to new deletion vector descriptors
    /// let mut dv_map = HashMap::new();
    /// // ... populate dv_map with file paths and their new DV descriptors ...
    ///
    /// txn.update_deletion_vectors(dv_map, files.into_iter())?;
    /// txn.commit(engine)?;
    /// ```
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn update_deletion_vectors(
        &mut self,
        new_dv_descriptors: HashMap<String, DeletionVectorDescriptor>,
        existing_data_files: impl Iterator<Item = DeltaResult<FilteredEngineData>>,
    ) -> DeltaResult<()> {
        if !self
            .read_snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DeletionVectors)
        {
            return Err(Error::unsupported(
                "Deletion vector operations require reader version 3, writer version 7, \
                 and the 'deletionVectors' feature in both reader and writer features",
            ));
        }

        let mut matched_dv_files = 0;
        let mut visitor = DvMatchVisitor::new(&new_dv_descriptors);

        // Process each batch of scan file metadata to prepare for DV updates:
        // 1. Visit rows to match file paths against the DV descriptor map
        // 2. Append new DV descriptors as a temporary column to matched files
        // 3. Update selection vector to only keep files that need DV updates
        // 4. Cache the result in dv_matched_files for generating remove/add actions during commit
        for scan_file_result in existing_data_files {
            let scan_file = scan_file_result?;
            visitor.new_dv_entries.clear();
            visitor.matched_file_indexes.clear();
            let (data, mut selection_vector) = scan_file.into_parts();
            visitor.visit_rows_of(data.as_ref())?;

            // Update selection vector to keep only files that matched DV descriptors.
            // This ensures we only generate remove/add actions for files being updated.
            let mut current_matched_index = 0;
            for (i, selected) in selection_vector.iter_mut().enumerate() {
                if current_matched_index < visitor.matched_file_indexes.len() {
                    if visitor.matched_file_indexes[current_matched_index] != i {
                        *selected = false;
                    } else {
                        current_matched_index += 1;
                        matched_dv_files += if *selected { 1 } else { 0 };
                    }
                } else {
                    // Deselect any files after the last matched file
                    *selected = false;
                }
            }

            let new_columns = vec![ArrayData::try_new(
                struct_deletion_vector_schema().clone(),
                visitor.new_dv_entries.clone(),
            )?];
            self.dv_matched_files.push(FilteredEngineData::try_new(
                data.append_columns(new_dv_column_schema().clone(), new_columns)?,
                selection_vector,
            )?);
        }

        if matched_dv_files != new_dv_descriptors.len() {
            return Err(Error::generic(format!(
                "Number of matched DV files does not match number of new DV descriptors: {} != {}",
                matched_dv_files,
                new_dv_descriptors.len()
            )));
        }

        Ok(())
    }

    /// Generate domain metadata actions with validation. Handle both user and system domains.
    ///
    /// This function may perform an expensive log replay operation if there are any domain removals.
    /// The log replay is required to fetch the previous configuration value for the domain to preserve
    /// in removal tombstones as mandated by the Delta spec.
    fn generate_domain_metadata_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + 'a> {
        // Validate feature support for user domain operations
        if (!self.domain_metadata_additions.is_empty() || !self.domain_removals.is_empty())
            && !self
                .read_snapshot
                .table_configuration()
                .is_feature_supported(&TableFeature::DomainMetadata)
        {
            return Err(Error::unsupported("Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature"));
        }

        // Validate user domain operations
        self.validate_user_domain_operations()?;

        // Generate user domain removals via log replay (expensive if non-empty)
        let removal_actions = if !self.domain_removals.is_empty() {
            // Scan log to fetch existing configurations for tombstones
            let existing_domains =
                scan_domain_metadatas(self.read_snapshot.log_segment(), None, engine)?;

            // Create removal tombstones with pre-image configurations
            let removals: Vec<_> = self
                .domain_removals
                .iter()
                .filter_map(|domain| {
                    // If domain doesn't exist in the log, this is a no-op (filter it out)
                    existing_domains.get(domain).map(|existing| {
                        DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
                    })
                })
                .collect();

            removals
        } else {
            vec![]
        };

        // Generate system domain actions (row tracking)
        let system_domain_actions = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        // Chain all domain actions and convert to EngineData
        Ok(self
            .domain_metadata_additions
            .clone()
            .into_iter()
            .chain(removal_actions)
            .chain(system_domain_actions)
            .map(|dm| dm.into_engine_data(get_log_domain_metadata_schema().clone(), engine)))
    }

    /// The schema that the [`Engine`]'s [`ParquetHandler`] is expected to use when reporting information about
    /// a Parquet write operation back to Kernel.
    ///
    /// Concretely, it is the expected schema for [`EngineData`] passed to [`add_files`], as it is the base
    /// for constructing an add_file. Each row represents metadata about a
    /// file to be added to the table. Kernel takes this information and extends it to the full add_file
    /// action schema, adding internal fields (e.g., baseRowID) as necessary.
    ///
    /// For now, Kernel only supports the number of records as a file statistic.
    /// This will change in a future release.
    ///
    /// Note: While currently static, in the future the schema might change depending on
    /// options set on the transaction or features enabled on the table.
    ///
    /// [`add_files`]: crate::transaction::Transaction::add_files
    /// [`ParquetHandler`]: crate::ParquetHandler
    pub fn add_files_schema(&self) -> &'static SchemaRef {
        &BASE_ADD_FILES_SCHEMA
    }

    // Generate the logical-to-physical transform expression which must be evaluated on every data
    // chunk before writing. At the moment, this is a transaction-wide expression.
    fn generate_logical_to_physical(&self) -> Expression {
        let partition_columns = self
            .read_snapshot
            .table_configuration()
            .metadata()
            .partition_columns();
        let schema = self.read_snapshot.schema();

        // Check if materializePartitionColumns feature is enabled
        let materialize_partition_columns = self
            .read_snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns);

        // If the materialize partition columns feature is enabled, pass through all columns in the
        // schema. Otherwise, exclude partition columns.
        let fields = schema
            .fields()
            .filter(|f| materialize_partition_columns || !partition_columns.contains(f.name()))
            .map(|f| Expression::column([f.name()]));
        Expression::struct_from(fields)
    }

    /// Get the write context for this transaction. At the moment, this is constant for the whole
    /// transaction.
    // Note: after we introduce metadata updates (modify table schema, etc.), we need to make sure
    // that engines cannot call this method after a metadata change, since the write context could
    // have invalid metadata.
    pub fn get_write_context(&self) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        let logical_to_physical = self.generate_logical_to_physical();

        // Compute physical schema: exclude partition columns since they're stored in the path
        let partition_columns = self
            .read_snapshot
            .table_configuration()
            .metadata()
            .partition_columns();
        let physical_fields = snapshot_schema
            .fields()
            .filter(|f| !partition_columns.contains(f.name()))
            .cloned();
        let physical_schema = Arc::new(StructType::new_unchecked(physical_fields));

        WriteContext::new(
            target_dir.clone(),
            snapshot_schema,
            physical_schema,
            Arc::new(logical_to_physical),
        )
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.add_files_metadata.push(add_metadata);
    }

    /// Generate add actions, handling row tracking internally if needed
    fn generate_adds<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: u64,
    ) -> DeltaResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
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
                // Convert stats to a JSON string and nest the add action in a top-level struct
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

        if self.add_files_metadata.is_empty() {
            return Ok((Box::new(iter::empty()), None));
        }

        let commit_version = i64::try_from(commit_version)
            .map_err(|_| Error::generic("Commit version too large to fit in i64"))?;

        let needs_row_tracking = self
            .read_snapshot
            .table_configuration()
            .should_write_row_tracking();

        if needs_row_tracking {
            // Read the current rowIdHighWaterMark from the snapshot's row tracking domain metadata
            let row_id_high_water_mark =
                RowTrackingDomainMetadata::get_high_water_mark(&self.read_snapshot, engine)?;

            // Create a row tracking visitor and visit all files to collect row tracking information
            let mut row_tracking_visitor = RowTrackingVisitor::new(
                row_id_high_water_mark,
                Some(self.add_files_metadata.len()),
            );

            // We visit all files with the row visitor before creating the add action iterator
            // because we need to know the final row ID high water mark to create the domain metadata action
            for add_files_batch in &self.add_files_metadata {
                row_tracking_visitor.visit_rows_of(add_files_batch.deref())?;
            }

            // Deconstruct the row tracking visitor to avoid borrowing issues
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
        } else {
            // Simple case without row tracking
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

    fn into_committed(self, file_meta: FileMeta) -> DeltaResult<CommittedTransaction> {
        let parsed_commit = ParsedLogPath::parse_commit(file_meta)?;
        let stats = PostCommitStats {
            commits_since_checkpoint: self.read_snapshot.log_segment().commits_since_checkpoint()
                + 1,
            commits_since_log_compaction: self
                .read_snapshot
                .log_segment()
                .commits_since_log_compaction_or_checkpoint()
                + 1,
        };

        Ok(CommittedTransaction {
            transaction: self,
            commit_version: parsed_commit.version,
            post_commit_stats: stats,
        })
    }

    fn into_conflicted(self, conflict_version: Version) -> ConflictedTransaction {
        ConflictedTransaction {
            transaction: self,
            conflict_version,
        }
    }

    fn into_retryable(self, error: Error) -> RetryableTransaction {
        RetryableTransaction {
            transaction: self,
            error,
        }
    }

    /// Remove files from the table in this transaction. This API generally enables the engine to
    /// delete data (at file-level granularity) from the table. Note that this API can be called
    /// multiple times to remove multiple batches.
    ///
    /// The expected schema for `remove_metadata` is given by [`scan_row_schema`]. It is expected
    /// this will be the result of passing [`FilteredEngineData`] returned from a scan
    /// with the selection vector modified to select rows for removal (selected rows in the selection vector are the ones to be removed).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # use delta_kernel::snapshot::Snapshot;
    /// # #[cfg(feature = "catalog-managed")]
    /// # use delta_kernel::committer::FileSystemCommitter;
    /// # fn example(engine: Arc<dyn Engine>, table_url: url::Url) -> delta_kernel::DeltaResult<()> {
    /// # #[cfg(feature = "catalog-managed")]
    /// # {
    /// // Create a snapshot and transaction
    /// let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    /// let mut txn = snapshot.clone().transaction(Box::new(FileSystemCommitter::new()))?;
    ///
    /// // Get file metadata from a scan
    /// let scan = snapshot.scan_builder().build()?;
    /// let scan_metadata = scan.scan_metadata(engine.as_ref())?;
    ///
    /// // Remove specific files based on scan metadata
    /// for metadata in scan_metadata {
    ///     let metadata = metadata?;
    ///     // In practice, you would modify the selection vector to choose which files to remove
    ///     let files_to_remove = metadata.scan_files;
    ///     txn.remove_files(files_to_remove);
    /// }
    ///
    /// // Commit the transaction
    /// txn.commit(engine.as_ref())?;
    /// # }
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_files(&mut self, remove_metadata: FilteredEngineData) {
        self.remove_files_metadata.push(remove_metadata);
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
    /// - `columns_to_drop`: Column names to drop from the scan metadata before transformation.
    ///   This is used to remove temporary columns like the intermediate deletion vector column
    ///   added during DV updates.
    ///
    /// # Returns
    ///
    /// An iterator of FilteredEngineData containing Remove actions in the log schema format.
    ///
    /// [`remove_files`]: Transaction::remove_files
    /// [`update_deletion_vectors`]: Transaction::update_deletion_vectors
    fn generate_remove_actions<'a>(
        &'a self,
        engine: &dyn Engine,
        remove_files_metadata: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
        columns_to_drop: &'a [&str],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        let input_schema = scan_row_schema();
        let target_schema = NullableStatsTransform
            .transform_struct(get_log_remove_schema())
            .ok_or_else(|| Error::generic("Failed to transform remove schema"))?
            .into_owned();
        let evaluation_handler = engine.evaluation_handler();

        // Create the transform expression once, since it only contains literals and column references
        let mut transform = Transform::new_top_level()
            // deletionTimestamp
            .with_inserted_field(
                Some("path"),
                Expression::literal(self.commit_timestamp).into(),
            )
            // dataChange
            .with_inserted_field(Some("path"), Expression::literal(self.data_change).into())
            .with_inserted_field(
                // extended_file_metadata
                Some("path"),
                Expression::literal(true).into(),
            )
            .with_inserted_field(
                Some("path"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, "partitionValues"]).into(),
            )
            // tags
            .with_inserted_field(
                Some("stats"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, TAGS_NAME]).into(),
            )
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, BASE_ROW_ID_NAME]).into(),
            )
            .with_inserted_field(
                Some("deletionVector"),
                Expression::column([FILE_CONSTANT_VALUES_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME])
                    .into(),
            )
            .with_dropped_field(FILE_CONSTANT_VALUES_NAME)
            .with_dropped_field("modificationTime");

        // Drop any additional columns specified in columns_to_drop
        for column_to_drop in columns_to_drop {
            transform = transform.with_dropped_field(*column_to_drop);
        }

        let expr = Arc::new(Expression::struct_from([Expression::transform(transform)]));
        let file_action_eval = Arc::new(evaluation_handler.new_expression_evaluator(
            input_schema.clone(),
            expr.clone(),
            target_schema.clone().into(),
        )?);

        Ok(remove_files_metadata.map(move |file_metadata_batch| {
            let updated_engine_data = file_action_eval.evaluate(file_metadata_batch.data())?;
            FilteredEngineData::try_new(
                updated_engine_data,
                file_metadata_batch.selection_vector().to_vec(),
            )
        }))
    }

    /// Generate remove/add action pairs for files with DV updates.
    ///
    /// This method processes the cached matched files, generating the necessary Remove and Add actions.
    /// For each file:
    /// 1. A Remove action is generated for the old file
    /// 2. An Add action is generated with the new DV descriptor
    fn generate_dv_update_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        static COLUMNS_TO_DROP: &[&str] = &[NEW_DELETION_VECTOR_NAME];
        let remove_actions =
            self.generate_remove_actions(engine, self.dv_matched_files.iter(), COLUMNS_TO_DROP)?;
        let add_actions = self.generate_adds_for_dv_update(engine, self.dv_matched_files.iter())?;
        Ok(remove_actions.chain(add_actions))
    }

    /// Generates Add actions for files with updated deletion vectors.
    ///
    /// This transforms scan file metadata with new DV descriptors (appended as a temporary column)
    /// into Add actions for the Delta log.
    fn generate_adds_for_dv_update<'a>(
        &'a self,
        engine: &'a dyn Engine,
        file_metadata_batch: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        let evaluation_handler = engine.evaluation_handler();
        // Transform to replace the deletionVector field with the new DV from NEW_DELETION_VECTOR_NAME,
        // then drop the NEW_DELETION_VECTOR_NAME column. The engine data has this temporary column
        // appended by update_deletion_vectors(), but it is not expected by the transforms used in
        // generate_remove_actions() which expect only the scan row schema fields.
        let with_new_dv_transform = Expression::transform(
            Transform::new_top_level()
                .with_replaced_field(
                    "deletionVector",
                    Expression::column([NEW_DELETION_VECTOR_NAME]).into(),
                )
                .with_dropped_field(NEW_DELETION_VECTOR_NAME),
        );
        let with_new_dv_eval = evaluation_handler.new_expression_evaluator(
            intermediate_dv_schema().clone(),
            Arc::new(with_new_dv_transform),
            nullable_scan_rows_schema().clone().into(),
        )?;
        let restored_add_eval = evaluation_handler.new_expression_evaluator(
            nullable_scan_rows_schema().clone(),
            get_scan_metadata_transform_expr(),
            nullable_restored_add_schema().clone().into(),
        )?;
        let with_data_change_transform =
            Arc::new(Expression::struct_from([Expression::transform(
                Transform::new_nested(["add"]).with_inserted_field(
                    Some("modificationTime"),
                    Expression::literal(self.data_change).into(),
                ),
            )]));
        let with_data_change_eval = evaluation_handler.new_expression_evaluator(
            nullable_restored_add_schema().clone(),
            with_data_change_transform,
            nullable_add_log_schema().clone().into(),
        )?;
        Ok(file_metadata_batch.map(
            move |file_metadata_batch| -> DeltaResult<FilteredEngineData> {
                let with_new_dv_data = with_new_dv_eval.evaluate(file_metadata_batch.data())?;

                let as_partial_add_data = restored_add_eval.evaluate(with_new_dv_data.as_ref())?;

                let with_data_change_data =
                    with_data_change_eval.evaluate(as_partial_add_data.as_ref())?;

                FilteredEngineData::try_new(
                    with_data_change_data,
                    file_metadata_batch.selection_vector().to_vec(),
                )
            },
        ))
    }
}

/// Visitor that matches file paths from scan data against new deletion vector descriptors.
/// Used by update_deletion_vectors() to attach new DV descriptors to scan file metadata.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
struct DvMatchVisitor<'a> {
    /// Map from file path to the new deletion vector descriptor for that file
    dv_updates: &'a HashMap<String, DeletionVectorDescriptor>,
    /// Accumulated DV descriptors (or nulls) for each visited row, in visit order
    new_dv_entries: Vec<Scalar>,
    /// Indexes of rows that matched a file path in dv_update. These must be in
    /// ascending order
    matched_file_indexes: Vec<usize>,
}

impl<'a> DvMatchVisitor<'a> {
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    const PATH_INDEX: usize = 0;

    /// Creates a new DvMatchVisitor that will match file paths against the provided DV updates map.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    fn new(dv_updates: &'a HashMap<String, DeletionVectorDescriptor>) -> Self {
        Self {
            dv_updates,
            new_dv_entries: Vec::new(),
            matched_file_indexes: Vec::new(),
        }
    }
}

/// A `RowVisitor` that matches file paths against the provided DV updates map.
impl RowVisitor for DvMatchVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> = LazyLock::new(|| {
            let names = vec![column_name!("path")];
            let types = vec![DataType::STRING];
            (names, types)
        });
        (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
    }

    /// For each path checks if it is in the hash-map and if it is, extract DV
    /// details that can be appended back to the EngineData.  Also track matched
    /// rows so the selected rows can be updated to only contain matches.
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Use get_opt since path is nullable in the schema
            let path_opt: Option<String> = getters[Self::PATH_INDEX].get_opt(i, "path")?;

            // Skip rows with null paths (these are rows that were deselected by the selection vector,
            // but still appear in the EngineData since visitors operate on the full EngineData)
            let Some(path) = path_opt else {
                self.new_dv_entries.push(Scalar::Null(DataType::from(
                    DeletionVectorDescriptor::to_schema(),
                )));
                continue;
            };

            if let Some(dv_result) = self.dv_updates.get(&path) {
                self.new_dv_entries.push(Scalar::Struct(StructData::try_new(
                    DeletionVectorDescriptor::to_schema()
                        .into_fields()
                        .collect(),
                    vec![
                        Scalar::from(dv_result.storage_type.to_string()),
                        Scalar::from(dv_result.path_or_inline_dv.clone()),
                        Scalar::from(dv_result.offset),
                        Scalar::from(dv_result.size_in_bytes),
                        Scalar::from(dv_result.cardinality),
                    ],
                )?));
                self.matched_file_indexes.push(i);
            } else {
                self.new_dv_entries.push(Scalar::Null(DataType::from(
                    DeletionVectorDescriptor::to_schema(),
                )));
            }
        }
        Ok(())
    }
}

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    target_dir: Url,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    logical_to_physical: ExpressionRef,
}

impl WriteContext {
    fn new(
        target_dir: Url,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        logical_to_physical: ExpressionRef,
    ) -> Self {
        WriteContext {
            target_dir,
            logical_schema,
            physical_schema,
            logical_to_physical,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    pub fn logical_schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    pub fn physical_schema(&self) -> &SchemaRef {
        &self.physical_schema
    }

    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.logical_to_physical.clone()
    }

    /// Generate a new unique absolute URL for a deletion vector file.
    ///
    /// This method generates a unique file name in the table directory.
    /// Each call to this method returns a new unique path.
    ///
    /// # Arguments
    ///
    /// * `random_prefix` - A random prefix to use for the deletion vector file name.
    ///   Making this non-empty can help distributed load on object storage when writing/reading
    ///   to avoid throttling.  Typically a random string fo 2-4 characters is sufficient
    ///   for this purpose.
    ///
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let write_context = transaction.get_write_context();
    /// let dv_path = write_context.new_deletion_vector_path(String::from(rand_string()));
    /// // dv_url might be: s3://bucket/table/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin
    /// ```
    pub fn new_deletion_vector_path(&self, random_prefix: String) -> DeletionVectorPath {
        DeletionVectorPath::new(self.target_dir.clone(), random_prefix)
    }
}

/// Kernel exposes information about the state of the table that engines might want to use to
/// trigger actions like checkpointing or log compaction. This struct holds that information.
#[derive(Debug)]
pub struct PostCommitStats {
    /// The number of commits since this table has been checkpointed. Note that commit 0 is
    /// considered a checkpoint for the purposes of this computation.
    pub commits_since_checkpoint: u64,
    /// The number of commits since the log has been compacted on this table. Note that a checkpoint
    /// is considered a compaction for the purposes of this computation. Thus this is really the
    /// number of commits since a compaction OR a checkpoint.
    pub commits_since_log_compaction: u64,
}

/// The result of attempting to commit this transaction. If the commit was
/// successful/conflicted/retryable, the result is Ok(CommitResult), otherwise, if a nonrecoverable
/// error occurred, the result is Err(Error).
///
/// The commit result can be one of the following:
/// - [CommittedTransaction]: the transaction was successfully committed. [PostCommitStats] and
///   in the future a post-commit snapshot can be obtained from the committed transaction.
/// - [ConflictedTransaction]: the transaction conflicted with an existing version. This transcation
///   must be rebased before retrying. (currently no rebase APIs exist, caller must create new txn)
/// - [RetryableTransaction]: an IO (retryable) error occurred during the commit. This transaction
///   can be retried without rebasing.
#[derive(Debug)]
#[must_use]
pub enum CommitResult {
    /// The transaction was successfully committed.
    CommittedTransaction(CommittedTransaction),
    /// This transaction conflicted with an existing version (see
    /// [ConflictedTransaction::conflict_version]). The transaction
    /// is returned so the caller can resolve the conflict (along with the version which
    /// conflicted).
    // TODO(zach): in order to make the returning of a transaction useful, we need to add APIs to
    // update the transaction to a new version etc.
    ConflictedTransaction(ConflictedTransaction),
    /// An IO (retryable) error occurred during the commit.
    RetryableTransaction(RetryableTransaction),
}

impl CommitResult {
    /// Returns true if the commit was successful.
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitResult::CommittedTransaction(_))
    }
}

/// This is the result of a successfully committed [Transaction]. One can retrieve the
/// [PostCommitStats] and [commit version] from this struct. In the future a post-commit snapshot
/// can be obtained as well.
///
/// [commit version]: Self::commit_version
#[derive(Debug)]
pub struct CommittedTransaction {
    // TODO: remove after post-commit snapshot
    #[allow(dead_code)]
    transaction: Transaction,
    /// the version of the table that was just committed
    commit_version: Version,
    /// The [`PostCommitStats`] for this transaction
    post_commit_stats: PostCommitStats,
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

    // TODO(#916): post-commit snapshot
}

/// This is the result of a conflicted [Transaction]. One can retrieve the [conflict version] from
/// this struct. In the future a rebase API will be provided (issue #1389).
///
/// [conflict version]: Self::conflict_version
#[derive(Debug)]
pub struct ConflictedTransaction {
    // TODO: remove after rebase APIs
    #[allow(dead_code)]
    transaction: Transaction,
    conflict_version: Version,
}

impl ConflictedTransaction {
    /// The version attempted commit that yielded a conflict
    pub fn conflict_version(&self) -> Version {
        self.conflict_version
    }
}

/// A transaction that failed to commit due to a retryable error (e.g. IO error). The transaction
/// can be recovered with `RetryableTransaction::transaction` and retried without rebasing. The
/// associated error can be inspected via `RetryableTransaction::error`.
#[derive(Debug)]
pub struct RetryableTransaction {
    /// The transaction that failed to commit due to a retryable error.
    pub transaction: Transaction,
    /// Transient error that caused the commit to fail.
    pub error: Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::schema::MapType;
    use crate::Snapshot;
    use std::path::PathBuf;

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

    /// Creates a test deletion vector descriptor with default values (the DV might not exist on disk)
    fn create_test_dv_descriptor(path_suffix: &str) -> DeletionVectorDescriptor {
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: format!("dv_{}", path_suffix),
            offset: Some(0),
            size_in_bytes: 100,
            cardinality: 1,
        }
    }

    fn create_dv_transaction(snapshot: Arc<Snapshot>) -> DeltaResult<Transaction> {
        Ok(snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
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
            .transaction(Box::new(FileSystemCommitter::new()))?
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
                DataType::struct_type_unchecked(vec![StructField::nullable(
                    "numRecords",
                    DataType::LONG,
                )]),
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
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_engine_info("default engine");
        let write_context = txn.get_write_context();

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
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_engine_info("default engine");

        let write_context = txn.get_write_context();
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

    #[test]
    fn test_materialize_partition_columns_in_write_context(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let engine = SyncEngine::new();

        // Test 1: Use basic_partitioned table (without materializePartitionColumns feature)
        // Partition columns should be excluded from the logical_to_physical expression
        let path_without =
            std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url_without = url::Url::from_directory_path(path_without).unwrap();
        let snapshot_without = Snapshot::builder_for(url_without)
            .at_version(0)
            .build(&engine)?;

        // Verify the table is partitioned by "letter" column
        let partition_cols_without = snapshot_without
            .table_configuration()
            .metadata()
            .partition_columns();
        assert_eq!(partition_cols_without.len(), 1);
        assert_eq!(partition_cols_without[0], "letter");

        // Verify the table does not have the materializePartitionColumns feature
        let has_feature_without = snapshot_without
            .table_configuration()
            .protocol()
            .has_table_feature(&TableFeature::MaterializePartitionColumns);
        assert!(
            !has_feature_without,
            "basic_partitioned should not have materializePartitionColumns feature"
        );

        // Create a transaction and get write context
        let txn_without = snapshot_without.transaction(Box::new(FileSystemCommitter::new()))?;
        let write_context_without = txn_without.get_write_context();
        let expr_without = write_context_without.logical_to_physical();

        // Without materializePartitionColumns, partition column should be excluded
        let expr_str_without = format!("{:?}", expr_without);
        assert!(!expr_str_without.contains("letter"),
            "Partition column 'letter' should be excluded when materializePartitionColumns is not enabled. Expression: {}",
            expr_str_without);
        assert!(
            expr_str_without.contains("number"),
            "Non-partition column 'number' should be included. Expression: {}",
            expr_str_without
        );
        assert!(
            expr_str_without.contains("a_float"),
            "Non-partition column 'a_float' should be included. Expression: {}",
            expr_str_without
        );

        // Test 2: Use partitioned_with_materialize_feature table (with materializePartitionColumns feature)
        // Partition columns should be included in the logical_to_physical expression
        let path_with = std::fs::canonicalize(PathBuf::from(
            "./tests/data/partitioned_with_materialize_feature/",
        ))
        .unwrap();
        let url_with = url::Url::from_directory_path(path_with).unwrap();
        let snapshot_with = Snapshot::builder_for(url_with)
            .at_version(1)
            .build(&engine)?;

        // Verify the table is partitioned by "letter" column
        let partition_cols_with = snapshot_with
            .table_configuration()
            .metadata()
            .partition_columns();
        assert_eq!(partition_cols_with.len(), 1);
        assert_eq!(partition_cols_with[0], "letter");

        // Verify the table HAS the materializePartitionColumns feature
        let has_feature_with = snapshot_with
            .table_configuration()
            .protocol()
            .has_table_feature(&TableFeature::MaterializePartitionColumns);
        assert!(
            has_feature_with,
            "partitioned_with_materialize_feature should have materializePartitionColumns feature"
        );

        // Create a transaction and get write context
        let txn_with = snapshot_with.transaction(Box::new(FileSystemCommitter::new()))?;
        let write_context_with = txn_with.get_write_context();
        let expr_with = write_context_with.logical_to_physical();

        // With materializePartitionColumns, ALL columns including partition columns should be included
        let expr_str_with = format!("{:?}", expr_with);
        assert!(expr_str_with.contains("letter"),
            "Partition column 'letter' should be included when materializePartitionColumns is enabled. Expression: {}",
            expr_str_with);
        assert!(
            expr_str_with.contains("number"),
            "Non-partition column 'number' should be included. Expression: {}",
            expr_str_with
        );
        assert!(
            expr_str_with.contains("a_float"),
            "Non-partition column 'a_float' should be included. Expression: {}",
            expr_str_with
        );

        Ok(())
    }

    /// Tests that update_deletion_vectors validates table protocol requirements.
    /// Validates that attempting DV updates on unsupported tables returns protocol error.
    #[test]
    fn test_update_deletion_vectors_unsupported_table() -> Result<(), Box<dyn std::error::Error>> {
        let (_engine, snapshot) = setup_non_dv_table();
        let mut txn = create_dv_transaction(snapshot)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        let err = result.expect_err("Should fail on table without DV support");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Deletion vector")
                && (err_msg.contains("require") || err_msg.contains("version")),
            "Expected protocol error about DV requirements, got: {}",
            err_msg
        );
        Ok(())
    }

    /// Tests that update_deletion_vectors validates DV descriptors match scan files.
    /// Validates detection of mismatch between provided DV descriptors and actual files.
    #[test]
    fn test_update_deletion_vectors_mismatch_count() -> Result<(), Box<dyn std::error::Error>> {
        let (_engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot)?;

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
            "Expected error about mismatched count (expected 1 descriptor, 0 matched files), got: {}",
            err_msg);
        Ok(())
    }

    /// Tests that update_deletion_vectors handles empty DV updates correctly as a no-op.
    /// This edge case occurs when a DELETE operation matches no rows.
    #[test]
    fn test_update_deletion_vectors_empty_inputs() -> Result<(), Box<dyn std::error::Error>> {
        let (_engine, snapshot) = setup_dv_enabled_table();
        let mut txn = create_dv_transaction(snapshot)?;

        let dv_map = HashMap::new();
        let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

        assert!(
            result.is_ok(),
            "Empty DV updates should succeed as no-op, got error: {:?}",
            result
        );

        Ok(())
    }

    // Note: Additional test coverage for partial file matching (where some files in a scan
    // have DV updates but others don't) is provided by the end-to-end integration test
    // kernel/tests/dv.rs and kernel/tests/write.rs, which exercises
    // the full deletion vector write workflow including the DvMatchVisitor logic.
}
