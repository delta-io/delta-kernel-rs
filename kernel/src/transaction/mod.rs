use crate::SchemaTransform;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use crate::actions::{
    as_log_add_schema, domain_metadata::scan_domain_metadatas, get_log_commit_info_schema,
    get_log_domain_metadata_schema, get_log_remove_schema, get_log_txn_schema, CommitInfo,
    DomainMetadata, SetTransaction,
};
#[cfg(feature = "catalog-managed")]
use crate::committer::FileSystemCommitter;
use crate::committer::{CommitMetadata, CommitResponse, Committer};
    as_log_add_schema, get_log_commit_info_schema, get_log_domain_metadata_schema,
    get_log_remove_schema, get_log_txn_schema,
    as_log_add_schema, get_log_commit_info_schema, get_log_domain_metadata_schema, ADD_NAME
    get_log_remove_schema, get_log_txn_schema,CommitInfo, DomainMetadata, REMOVE_NAME, SetTransaction

};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::FilteredEngineData;
use crate::error::Error;
use crate::expressions::{ArrayData, Transform, UnaryExpressionOp::ToJson};
use crate::path::LogRoot;
use crate::expressions::{ArrayData, Scalar, StructData, Transform, UnaryExpressionOp::ToJson};
use crate::path::ParsedLogPath;
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::scan::log_replay::{
    get_scan_metadata_transform_expr, BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, 
    FILE_CONSTANT_VALUES_NAME, TAGS_NAME,
};
use crate::scan::{restored_add_schema, scan_row_schema};
use crate::schema::{ArrayType, MapType, SchemaRef, StructField, StructType, ToSchema};
use crate::engine_data::GetData;
use crate::expressions::{column_name, ColumnName};
use crate::snapshot::SnapshotRef;
use crate::utils::current_time_ms;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Expression, ExpressionRef, IntoEngineData,
    RowVisitor, Version,
};
use delta_kernel_derive::internal_api;
use url::Url;

// This is a workaround due to the fact that expression evaluation happens
        // on the whole EngineData instead of accounting for filtered rows, which can lead to null values in
        // required fields.
        // TODO: Move this to a common place (dedupe from data_skipping.rs) or remove when evaluations work
        // on FilteredEngineData directly.
        struct NullableStatsTransform;
        impl<'a> SchemaTransform<'a> for NullableStatsTransform {
            fn transform_struct_field(
                &mut self,
                field: &'a StructField,
            ) -> Option<Cow<'a, StructField>> {
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

    Arc::new(StructType::new_unchecked(
        mandatory_add_file_schema().fields().cloned().chain([stats]),
    ))
});

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

static NEW_DELETION_VECTOR_NAME: LazyLock<String> = LazyLock::new(|| "newDeletionVector".to_string());

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

// NOTE: The following two methods are a workaround for the fact that we do not have a proper SchemaBuilder yet.
// See https://github.com/delta-io/delta-kernel-rs/issues/1284
/// Extend a schema with a statistics column and return a new SchemaRef.
///
/// The stats column is of type string as required by the spec.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_stats_col(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .cloned()
        .chain([StructField::nullable("stats", DataType::STRING)]);
    Arc::new(StructType::new_unchecked(fields))
}

/// Extend a schema with row tracking columns and return a new SchemaRef.
///
/// Note that this method is only useful to extend an Add action schema.
fn with_row_tracking_cols(schema: &SchemaRef) -> SchemaRef {
    let fields = schema.fields().cloned().chain([
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
    ]);
    Arc::new(StructType::new_unchecked(fields))
}

static INTERMEDIATE_DV_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(scan_row_schema().fields().cloned().chain([
        StructField::nullable(NEW_DELETION_VECTOR_NAME.to_string(), DeletionVectorDescriptor::to_schema()),
    ])))
});

fn intermediate_dv_schema() -> SchemaRef {
    &INTERMEDIATE_DV_SCHEMA
}

static NULLABLE_SCAN_ROWS_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(scan_row_schema())
        .expect("Failed to transform scan_row_schema")
        .into_owned()
});

fn nullable_scan_rows_schema() -> &'static DataType {
    &NULLABLE_SCAN_ROWS_SCHEMA
}

static NULLABLE_RESTORED_ADD_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    NullableStatsTransform
        .transform(restored_add_schema())
        .expect("Failed to transform restored_add_schema")
        .into_owned()
});

fn nullable_restored_add_schema() -> &'static DataType {
    &NULLABLE_RESTORED_ADD_SCHEMA
}

static NULLABLE_ADD_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(as_log_add_schema())
        .expect("Failed to transform as_log_add_schema")
        .into_owned()
});

fn nullable_add_schema() -> &'static DataType {
    &NULLABLE_ADD_SCHEMA
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
    domain_metadatas: Vec<DomainMetadata>,
    // Whether this transaction contains any logical data changes.
    data_change: bool,
    // Cached matched file metadata and DV results for generating remove/add actions
    dv_matched_files: Vec<FilteredEngineData>,
    // Flag indicating if this transaction will update deletion vectors
    will_update_dvs: bool,
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

        // important! before a read/write to the table we must check it is supported
        read_snapshot
            .table_configuration()
            .ensure_write_supported()?;

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
            domain_metadatas: vec![],
            data_change: true,
            dv_matched_files: vec![],
            will_update_dvs: false,
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

        // Step 5: Generate remove actions
        let remove_actions = self.generate_remove_actions(engine, self.remove_files_metadata.iter().map(|a| Ok(a.deref())))?;

        let actions = iter::once(commit_info_action)
            .chain(add_actions)
            .chain(dv_update_actions)
            .chain(set_transaction_actions)
            .chain(domain_metadata_actions);

        // Convert EngineData to FilteredEngineData with all rows selected
        let filtered_actions = actions
            .map(|action_result| action_result.map(FilteredEngineData::with_all_rows_selected));

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
        let commit_metadata = CommitMetadata::new(log_root, commit_version);
        match self
            .committer
            .commit(engine, Box::new(filtered_actions), commit_metadata)
        {
            Ok(CommitResponse::Committed { version }) => Ok(CommitResult::CommittedTransaction(
                self.into_committed(version),
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
        self.domain_metadatas
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
        // actual configuration value determined during commit
        self.domain_metadatas
            .push(DomainMetadata::remove(domain, String::new()));
        self
    }

    /// Indicate that this transaction will update deletion vectors.
    ///
    /// This method must be called before retrieving a scan builder if the transaction
    /// will add or update deletion vectors. It ensures that scans retrieve all necessary
    /// statistics to maintain them when generating new add actions.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut txn = snapshot.transaction()?
    ///     .with_dv_update()
    ///     .with_operation("DELETE");
    ///
    /// // Now scan the table to find files to update
    /// let scan = txn.get_write_context().scan_builder().build()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn with_dv_update(mut self) -> Self {
        self.will_update_dvs = true;
        self
    }

    // Helper function to create the ArrayType for deletion vector structs
    fn struct_deletion_vector_schema() -> ArrayType {
        ArrayType::new(DeletionVectorDescriptor::to_schema().into(), true)
    }

    // Helper function to create the schema for the intermediate DV column
    fn intermediate_dv_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        ]))
    }

    /// Update deletion vectors for files in the table.
    ///
    /// This method takes a map of file paths to new deletion vector descriptors and an iterator
    /// of scan file data. It joins the two together internally and will generate appropriate
    /// remove/add actions on commit to update the deletion vectors.
    ///
    /// The transaction must have been created with [`with_dv_update`](Self::with_dv_update) called
    /// before scanning, otherwise statistics may not be properly maintained.
    ///
    /// # Arguments
    ///
    /// * `new_dv_descriptors` - A map from data file path (as provided in scan operations) to
    ///   the new deletion vector write result for that file.
    /// * `existing_data_files` - An iterator over FilteredEngineData from scan metadata. The
    ///   selected elements of each FilteredEngineData must be a superset of the paths that key
    ///   `new_dv_descriptors`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A file path in `new_dv_descriptors` is not found in `existing_data_files`
    /// - [`with_dv_update`](Self::with_dv_update) was not called before scanning
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use std::collections::HashMap;
    /// use delta_kernel::actions::deletion_vector_writer::DeletionVectorWriteResult;
    ///
    /// let mut txn = snapshot.transaction()?
    ///     .with_dv_update()
    ///     .with_operation("DELETE");
    ///
    /// let scan = txn.scan_builder().build()?;
    /// let files: Vec<FilteredEngineData> = scan.scan_metadata().collect()?;
    ///
    /// // Write deletion vectors and collect results
    /// let mut dv_map = HashMap::new();
    /// for file in &files {
    ///     // ... write DVs and get descriptors ...
    ///     let descriptor = DeletionVectorWriteResult { /* ... */ };
    ///     dv_map.insert(file_path.to_string(), descriptor);
    /// }
    ///
    /// txn.update_deletion_vectors(&engine, dv_map, files.into_iter())?;
    /// txn.commit(&engine)?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn update_deletion_vectors(
        &mut self,
        new_dv_descriptors: HashMap<String, DeletionVectorDescriptor>,
        existing_data_files: impl Iterator<Item = FilteredEngineData>,
    ) -> DeltaResult<()> {
        use crate::RowVisitor;

        if !self.will_update_dvs {
            return Err(Error::generic(
                "Must call with_dv_update() before update_deletion_vectors(). \
                 This ensures statistics are properly retrieved during scanning.",
            ));
        }

        let mut matched_dv_files = 0;
        // Visitor to extract file metadata and match with DV updates
        
        let mut visitor = DvMatchVisitor::new(&new_dv_descriptors);

        // Process each scan file to find matches
        for scan_file in existing_data_files {
            visitor.new_dv_entries.clear();
            visitor.matched_file_indexes.clear();
            let (data, mut selection_vector) = scan_file.into_parts();
            visitor.visit_rows_of(data.as_ref())?;
            let mut current_matched_index = 0;
            for i in 0..existing_data_files.len() {
                if current_matched_index < visitor.matched_file_indexes.len() {
                 if visitor.matched_file_indexes[current_matched_index] != i {
                      selection_vector[i] = false;
                  } else {
                      // Advance to the next match when we find the index. 
                      current_matched_index += 1;
                      matched_dv_files += if selection_vector[i] { 1 } else  {0};
                  }
              }
            }
            
            let new_columns = vec![
                ArrayData::try_new(Self::struct_deletion_vector_schema(), visitor.new_dv_entries.clone())?,
            ];
            self.dv_matched_files.push(FilteredEngineData::try_new(data.append_columns(Self::intermediate_dv_schema(), new_columns)?, selection_vector)?);
        }

        if matched_dv_files != new_dv_descriptors.len() {
            return Err(Error::generic(
                format!("Number of matched DV files does not match number of new DV descriptors: {} != {}", matched_dv_files, new_dv_descriptors.len()),
            ));
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
        // if there are domain metadata actions, the table must support it
        if !self.domain_metadatas.is_empty()
            && !self
                .read_snapshot
                .table_configuration()
                .is_domain_metadata_supported()
        {
            return Err(Error::unsupported(
                "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature"
            ));
        }

        // validate user domain metadata and check if we have removals
        let mut seen_domains = HashSet::new();
        let mut has_removals = false;
        for dm in &self.domain_metadatas {
            if dm.is_internal() {
                return Err(Error::Generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled"
                        .to_string(),
                ));
            }

            if !seen_domains.insert(dm.domain()) {
                return Err(Error::Generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    dm.domain()
                )));
            }

            if dm.is_removed() {
                has_removals = true;
            }
        }

        // fetch previous configuration values (requires log replay)
        let existing_domains = if has_removals {
            scan_domain_metadatas(self.read_snapshot.log_segment(), None, engine)?
        } else {
            HashMap::new()
        };

        let user_domains = self
            .domain_metadatas
            .iter()
            .filter_map(move |dm: &DomainMetadata| {
                if dm.is_removed() {
                    existing_domains.get(dm.domain()).map(|existing| {
                        DomainMetadata::remove(
                            dm.domain().to_string(),
                            existing.configuration().to_string(),
                        )
                    })
                } else {
                    Some(dm.clone())
                }
            });

        let system_domains = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        Ok(user_domains
            .chain(system_domains)
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
        // for now, we just pass through all the columns except partition columns.
        // note this is _incorrect_ if table config deems we need partition columns.
        let partition_columns = self
            .read_snapshot
            .table_configuration()
            .metadata()
            .partition_columns();
        let schema = self.read_snapshot.schema();
        let fields = schema
            .fields()
            .filter(|f| !partition_columns.contains(f.name()))
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
        WriteContext::new(
            target_dir.clone(),
            snapshot_schema,
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
                );
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

    fn into_committed(self, version: Version) -> CommittedTransaction {
        let stats = PostCommitStats {
            commits_since_checkpoint: self.read_snapshot.log_segment().commits_since_checkpoint()
                + 1,
            commits_since_log_compaction: self
                .read_snapshot
                .log_segment()
                .commits_since_log_compaction_or_checkpoint()
                + 1,
        };

        CommittedTransaction {
            transaction: self,
            commit_version: version,
            post_commit_stats: stats,
        }
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
    /// the expected schema for `remove_metadata` is given by [`scan_row_schema`] it, is expected
    /// this will be the result of passing [`FilteredEngineData`] returned from a scan
    /// with rows modified for removal (selected rows in FilteredEngineData are the ones to be removed).
    pub fn remove_files(&mut self, remove_metadata: FilteredEngineData) {
        self.remove_files_metadata.push(remove_metadata);
    }

    fn generate_remove_actions<'a>(
        &'a self,
        engine: &dyn Engine,
        remove_files_metadata: impl Iterator<Item = FilteredEngineData> + Send + 'a,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        
        let input_schema = scan_row_schema();
        let target_schema = NullableStatsTransform
            .transform_struct(get_log_remove_schema())
            .ok_or_else(|| Error::generic("Failed to transform remove schema"))?
            .into_owned();
        let evaluation_handler = engine.evaluation_handler();

        Ok(
            remove_files_metadata
            .iter()
            .map(move |file_metadata_batch| {
                let transform = Expression::transform(
                    Transform::new_top_level()
                        .with_inserted_field(
                            Some("path"),
                            Expression::literal(self.commit_timestamp).into(),
                        )
                        .with_inserted_field(
                            Some("path"),
                            Expression::literal(self.data_change).into(),
                        )
                        .with_inserted_field(
                            // extended_file_metadata
                            Some("path"),
                            Expression::literal(true).into(),
                        )
                        .with_inserted_field(
                            Some("path"),
                            Expression::column([FILE_CONSTANT_VALUES_NAME, "partitionValues"])
                                .into(),
                        )
                        // tags
                        .with_inserted_field(
                            Some("stats"),
                            Expression::column([FILE_CONSTANT_VALUES_NAME, TAGS_NAME]).into(),
                        )
                        .with_inserted_field(
                            Some("deletionVector"),
                            Expression::column([FILE_CONSTANT_VALUES_NAME, BASE_ROW_ID_NAME])
                                .into(),
                        )
                        .with_inserted_field(
                            Some("deletionVector"),
                            Expression::column([
                                FILE_CONSTANT_VALUES_NAME,
                                DEFAULT_ROW_COMMIT_VERSION_NAME,
                            ])
                            .into(),
                        )
                        .with_dropped_field(FILE_CONSTANT_VALUES_NAME)
                        .with_dropped_field("modificationTime"),
                );
                let expr = Expression::struct_from([transform]);
                let file_action_eval = evaluation_handler.new_expression_evaluator(
                    input_schema.clone(),
                    Arc::new(expr),
                    target_schema.clone().into(),
                );

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
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + 'a> {
        let remove_actions = 
          self.generate_remove_actions(engine, self.dv_matched_files.iter().map(|a| Ok(a.deref())))?;
          let add_actions = self.generate_adds_for_dv_update(engine, self.dv_matched_files.iter().map(|a| Ok(a.deref())))?;
          Ok(remove_actions.chain(add_actions))
    }

    fn generate_adds_for_dv_update<'a>(
        &'a self,
        engine: &'a dyn Engine,
        file_metadata_batch: impl Iterator<Item = FilteredEngineData> + Send + 'a,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        
        let evaluation_handler = engine.evaluation_handler();

        Ok(
            self.dv_matched_files
            .iter()
            .map(move |file_metadata_batch| {
                let with_new_dv_transform = Expression::transform(
                    Transform::new_top_level()
                        .with_replaced_field(
                            "deletionVector",
                            Expression::column([NEW_DELETION_VECTOR_NAME]).into(),
                        )
                );
                let expr = Expression::struct_from([with_new_dv_transform]);
                let with_new_dv_eval= evaluation_handler.new_expression_evaluator(
                    intermediate_dv_schema(),
                    Arc::new(expr),
                    nullable_scan_rows_schema().into(),
                );

                let (data, selection_vector) = file_metadata_batch.into_parts();
                let with_new_dv_data = with_new_dv_eval.evaluate(data)?;

                let restored_add_eval= evaluation_handler.new_expression_evaluator(
                    intermediate_dv_schema(),
                    get_scan_metadata_transform_expr(),
                    nullable_restored_add_schema().into(),
                );
                let as_partial_add_data = restored_add_eval.evaluate(with_new_dv_data)?;

                let with_data_change_transform = Expression::transform(
                    Transform::new_top_level()
                        .with_inserted_field(
                            Some("modificationTime"),
                            Expression::literal(self.data_change).into(),
                        )
                );
                let expr = Expression::struct_from([with_data_change_transform]);
                let with_data_change_eval= evaluation_handler.new_expression_evaluator(
                    nullable_restored_add_schema().into(),
                    Arc::new(expr),
                    nullable_add_schema().into(),
                );
                let with_data_change_data = with_data_change_eval.evaluate(as_partial_add_data)?;

                FilteredEngineData::try_new(
                    with_data_change_data,
                    selection_vector,
                )
            }))

    } 
}

struct DvMatchVisitor<'a> {
            dv_updates: &'a HashMap<String, DeletionVectorDescriptor>,
            new_dv_entries: Vec<Scalar>,
            matched_file_indexes: Vec<usize>,
        }

        impl<'a> DvMatchVisitor<'a> {
            const PATH_INDEX: usize = 0;

            fn new(dv_updates: &'a HashMap<String, DeletionVectorDescriptor>) -> Self {
                Self {
                    dv_updates,
                    new_dv_entries: Vec::new(),
                    matched_file_indexes: Vec::new(),
                }
            }
        }

        impl RowVisitor for DvMatchVisitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> = LazyLock::new(|| {
                    let names = vec![
                        column_name!("path"),
                    ];
                    let types = vec![
                        DataType::STRING,
                    ];
                    (names, types)
                });
                (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
            }

            fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
                for i in 0..row_count {
                    let path: String = getters[Self::PATH_INDEX].get(i, "path")?;

                    // Check if this file has a DV update
                    if let Some(dv_result) = self.dv_updates.get(&path) {
                     self.new_dv_entries.push(Scalar::Struct(StructData::try_new(
                        DeletionVectorDescriptor::to_schema().into_fields().collect(),
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
                      self.new_dv_entries.push(Scalar::Null(DataType::from(DeletionVectorDescriptor::to_schema())));
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
    schema: SchemaRef,
    logical_to_physical: ExpressionRef,
}

impl WriteContext {
    fn new(target_dir: Url, schema: SchemaRef, logical_to_physical: ExpressionRef) -> Self {
        WriteContext {
            target_dir,
            schema,
            logical_to_physical,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.logical_to_physical.clone()
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
}
