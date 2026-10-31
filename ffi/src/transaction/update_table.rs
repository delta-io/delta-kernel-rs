//! Existing-table transaction FFI lifecycle.

use delta_kernel::engine_data::FilteredEngineData;

use super::committed::commit_result_to_committed_handle;
use super::*;

/// A handle for an update-table transaction (`Transaction<UpdateTable>`).
///
/// Returned by an update-table builder's build function. Supports all transaction
/// operations including existing-table-only operations like blind append and file removal.
#[handle_descriptor(target=Transaction, mutable=true, sized=true)]
pub struct ExclusiveUpdateTableTransaction;

/// A handle for configuring a transaction against an existing table.
///
/// Every `update_table_txn_builder_with_*` function consumes its input handle and returns a
/// replacement on success. On error, the builder is dropped. The caller must build or free the
/// final returned handle.
#[handle_descriptor(target=UpdateTableTransactionBuilder, mutable=true, sized=true)]
pub struct ExclusiveUpdateTableTransactionBuilder;

/// Creates a builder from a snapshot, known `operation`, connector `engine_info`, and
/// `data_change`.
///
/// Returns an owned builder, or an error if `engine_info` is not valid UTF-8. The operation and
/// engine info cannot be changed. `data_change` is also immutable and is preserved for empty
/// commits. Use `true` for inserting or deleting rows or updating their values, even if the row
/// count stays the same. Use `false` for metadata-only changes, such as adding a nullable column
/// with `ALTER TABLE`, or compaction that rewrites files without changing any records. Build
/// validation rejects `ALTER TABLE` with `true` and blind append with `false`.
///
/// # Safety
///
/// `snapshot`, `engine_info`, and `engine` must be valid and are borrowed for this call.
/// `operation` must contain a valid [`KernelUpdateTableOperation`] discriminant. Any
/// other tag is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_update_table_txn_builder(
    snapshot: Handle<SharedSnapshot>,
    operation: KernelUpdateTableOperation,
    engine_info: KernelStringSlice,
    data_change: bool,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let engine_info: KernelResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    let result = engine_info.map(|engine_info| {
        let snapshot = unsafe { snapshot.clone_as_arc() };
        Box::new(snapshot.update_table(operation.into(), engine_info, data_change)).into()
    });
    unsafe { result.into_extern_result(&engine) }
}

/// Creates a builder with a connector-defined `operation` name and connector `engine_info`.
///
/// Returns an owned builder on success, or an error if either string is not valid UTF-8. Name
/// restrictions and intent are validated when the builder is built. See
/// [`UpdateTableOperation::Custom`] for name restrictions and compatibility across Kernel versions.
/// `data_change` has the same contract as in [`new_update_table_txn_builder`].
///
/// # Safety
///
/// `snapshot`, `operation`, `engine_info`, and `engine` must be valid and are borrowed for this
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_update_table_txn_builder_with_custom_operation(
    snapshot: Handle<SharedSnapshot>,
    operation: KernelStringSlice,
    engine_info: KernelStringSlice,
    data_change: bool,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let operation: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&operation) };
    let engine_info: KernelResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    let result = operation.and_then(|operation| {
        let engine_info = engine_info?;
        let snapshot = unsafe { snapshot.clone_as_arc() };
        Ok(Box::new(snapshot.update_table(
            UpdateTableOperation::Custom(operation),
            engine_info,
            data_change,
        ))
        .into())
    });
    unsafe { result.into_extern_result(&engine) }
}

/// Builds an update-table transaction with the default filesystem committer.
///
/// Returns an error if the operation name or configured intent is invalid.
///
/// # Safety
///
/// Both handles must be valid. This call borrows `engine` and unconditionally consumes `builder`,
/// including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_build(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let result = update_table_txn_builder_build_with_committer_impl(
        builder,
        extern_engine,
        Box::new(FileSystemCommitter::new()),
    );
    unsafe { result.into_extern_result(&extern_engine) }
}

/// Builds an update-table transaction with a custom committer.
///
/// Returns an error if the operation name or configured intent is invalid.
///
/// # Safety
///
/// `builder`, `engine`, and `committer` must be valid handles. This call borrows `engine` and
/// unconditionally consumes both `builder` and `committer`, including when building returns an
/// error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_build_with_committer(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
    committer: Handle<MutableCommitter>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let committer = unsafe { committer.into_inner() };
    let result = update_table_txn_builder_build_with_committer_impl(builder, engine, committer);
    unsafe { result.into_extern_result(&engine) }
}

fn update_table_txn_builder_build_with_committer_impl(
    builder: UpdateTableTransactionBuilder,
    extern_engine: &dyn ExternEngine,
    committer: Box<dyn Committer>,
) -> KernelResult<Handle<ExclusiveUpdateTableTransaction>> {
    let engine = extern_engine.engine();
    let transaction = builder.build(engine.as_ref(), committer);
    Ok(Box::new(transaction?).into())
}

/// Frees an update-table builder without building it.
///
/// # Safety
///
/// `builder` must be a valid handle and is consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_update_table_txn_builder(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
) {
    unsafe { builder.drop_handle() }
}

/// Attaches a correlation identifier to update transaction metric events.
///
/// Repeated calls replace the previous value. An empty value clears it.
///
/// # Safety
///
/// All handles and `correlation_id` must be valid. This call borrows `engine` and `correlation_id`
/// and unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_correlation_id(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    correlation_id: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let correlation_id: KernelResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&correlation_id) };
    let result = correlation_id.map(|id| Box::new(builder.with_correlation_id(id)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Replaces the operation parameters recorded in `commitInfo`.
///
/// Duplicate keys are rejected by the FFI map decoder. This map replaces, rather than merges with,
/// the prior map. Dedicated parameters override same-named nested fields in connector commit
/// information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `parameters`
/// and unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_operation_parameters(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    parameters: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_nullable_string_map(
            builder,
            parameters,
            UpdateTableTransactionBuilder::with_operation_parameters,
        )
    }
    .map(|builder| Box::new(builder).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Replaces the operation metrics recorded in `commitInfo` before writes begin.
///
/// Duplicate keys are rejected by the FFI map decoder. Metrics supplied later through
/// [`Transaction::with_operation_metrics`] replace these builder metrics. Dedicated metrics
/// override the nested `operationMetrics` field in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `metrics`
/// and unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_operation_metrics(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    metrics: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_nullable_string_map(
            builder,
            metrics,
            UpdateTableTransactionBuilder::with_operation_metrics,
        )
    }
    .map(|builder| Box::new(builder).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Supplies one connector-defined `commitInfo` row to the update builder.
///
/// Repeated calls replace the previous row. Kernel-owned fields and dedicated operation
/// parameters or metrics take precedence over same-named fields in this row.
///
/// # Safety
///
/// All handles and `schema` must be valid. This call borrows `engine` and `schema` and
/// unconditionally consumes both `builder` and `commit_info`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_commit_info(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_commit_info(
            builder,
            commit_info,
            schema,
            UpdateTableTransactionBuilder::with_commit_info,
        )
    }
    .map(|builder| Box::new(builder).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Adds user-controlled domain metadata to the update builder.
///
/// Duplicate domains and add/remove conflicts are rejected when the builder is built.
///
/// # Safety
///
/// All handles and strings must be valid. This call borrows `engine`, `domain`, and `configuration`
/// and unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_domain_metadata(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_domain_metadata(
            builder,
            domain,
            configuration,
            UpdateTableTransactionBuilder::with_domain_metadata,
        )
    }
    .map(|builder| Box::new(builder).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Adds a nullable top-level field described by a single-field engine schema.
///
/// # Safety
///
/// All handles and `field` must be valid. This call borrows `engine` and `field` and
/// unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_add_column(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    field: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = decode_single_field(field).map(|field| Box::new(builder.add_column(field)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Adds a nullable field beneath a nested parent path.
///
/// # Safety
///
/// All handles and nested pointers must be valid. This call borrows `engine`, `parent`, and `field`
/// and unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_add_column_at(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    parent: &FfiColumnName,
    field: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let parent = unsafe { parent.try_to_column_name() };
    let result = parent
        .and_then(|parent| decode_single_field(field).map(|field| (parent, field)))
        .map(|(parent, field)| Box::new(builder.add_column_at(parent, field)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Changes the field at a possibly nested path from non-nullable to nullable.
///
/// # Safety
///
/// All handles and nested pointers must be valid. This call borrows `engine` and `column` and
/// unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_set_nullable(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    column: &FfiColumnName,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe { column.try_to_column_name() }
        .map(|column| Box::new(builder.set_nullable(column)).into());
    unsafe { result.into_extern_result(&engine) }
}

fn decode_single_field(schema: &EngineSchema) -> KernelResult<delta_kernel::schema::StructField> {
    let mut fields = decode_engine_schema(schema)?.into_fields();
    match (fields.next(), fields.next()) {
        (Some(field), None) => Ok(field),
        _ => Err(delta_kernel::KernelError::invalid_transaction_state(
            "add-column schema must contain exactly one field",
        )),
    }
}

// ============================================================================
// Existing-table transaction FFI functions
// ============================================================================

/// Adds a transaction identifier for application `app_id` at `version` before commit.
///
/// Returns the updated transaction handle, or an error if `app_id` is not valid UTF-8. The
/// action's `lastUpdated` uses the transaction's commit timestamp. Each application id may occur
/// only once, including ids added through the builder; duplicates are rejected at commit.
///
/// # Safety
///
/// `txn` and `engine` must be [valid][Handle#Validity] handles, and `app_id` must be a valid string
/// slice. This call borrows `engine` and `app_id` for the call and unconditionally consumes `txn`,
/// including on error. The caller must commit or free the returned handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_transaction_id(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    app_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let app_id: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&app_id) };
    let result = app_id.map(|app_id| Box::new(txn.with_transaction_id(app_id, version)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Replaces operation metrics after writing and before commit.
///
/// This is the late-binding counterpart to the builder setter. It replaces builder metrics and
/// the nested `operationMetrics` field in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `metrics`
/// and unconditionally consumes `txn`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_operation_metrics(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    metrics: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result =
        unsafe { apply_nullable_string_map(txn, metrics, Transaction::with_operation_metrics) }
            .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Replaces the connector-defined `commitInfo` row after writing and before commit.
///
/// Kernel-owned fields and dedicated parameter or metric maps take precedence over same-named
/// fields in this row.
///
/// # Safety
///
/// All handles and `schema` must be valid. This call borrows `engine` and `schema` and
/// unconditionally consumes both `txn` and `commit_info`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_commit_info(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result =
        unsafe { apply_commit_info(txn, commit_info, schema, Transaction::with_commit_info) }
            .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Free an existing-table transaction handle without committing.
///
/// # Safety
///
/// `txn` must be a valid handle and is consumed. Do not use or free it again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_update_table_txn(txn: Handle<ExclusiveUpdateTableTransaction>) {
    unsafe { txn.drop_handle() }
}

/// Operations with stable Delta history names.
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum KernelUpdateTableOperation {
    Write = 0,
    StreamingUpdate = 1,
    AlterTable = 2,
    Delete = 3,
    Update = 4,
    Merge = 5,
    Optimize = 6,
}

impl From<KernelUpdateTableOperation> for UpdateTableOperation {
    fn from(value: KernelUpdateTableOperation) -> Self {
        match value {
            KernelUpdateTableOperation::Write => Self::Write,
            KernelUpdateTableOperation::StreamingUpdate => Self::StreamingUpdate,
            KernelUpdateTableOperation::AlterTable => Self::AlterTable,
            KernelUpdateTableOperation::Delete => Self::Delete,
            KernelUpdateTableOperation::Update => Self::Update,
            KernelUpdateTableOperation::Merge => Self::Merge,
            KernelUpdateTableOperation::Optimize => Self::Optimize,
        }
    }
}

/// Add domain metadata to the transaction. The domain metadata will be written to the Delta log
/// as a `domainMetadata` action when the transaction is committed.
///
/// `domain` identifies the metadata domain (e.g. `"myApp"`). `configuration` is an arbitrary
/// string value associated with the domain (typically JSON).
///
/// Each domain can only appear once per transaction. Setting metadata for multiple distinct
/// domains is allowed. Duplicate domains or setting and removing the same domain in a single
/// transaction will cause the commit to fail.
///
/// # Safety
///
/// All handles and strings must be valid. This call borrows `engine`, `domain`, and `configuration`
/// and unconditionally consumes `txn`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_domain_metadata(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_domain_metadata(
            txn,
            domain,
            configuration,
            Transaction::with_domain_metadata,
        )
    }
    .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Stages a domain-metadata tombstone that preserves the previous configuration.
///
/// # Safety
///
/// All handles and `domain` must be valid. This call borrows `engine` and `domain` and
/// unconditionally consumes `txn`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_domain_metadata_removed(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    domain: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let domain: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&domain) };
    let result = domain.map(|domain| Box::new(txn.with_domain_metadata_removed(domain)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Adds a domain-metadata removal to the builder.
///
/// # Safety
///
/// All handles and `domain` must be valid. This call borrows `engine` and `domain` and
/// unconditionally consumes `builder`, including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_domain_metadata_removed(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    domain: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let domain: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&domain) };
    let result = domain.map(|domain| Box::new(builder.with_domain_metadata_removed(domain)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Set an explicit row-tracking high-water mark for this transaction.
///
/// Use this when row IDs must also be coordinated with another system. Kernel still assigns
/// row-tracking fields to files passed to [`update_table_txn_add_files`] and rejects
/// `high_water_mark` if it is less than the value calculated from those files. The generic
/// [`update_table_txn_with_domain_metadata`] API cannot modify `delta.rowTracking` or other
/// system-controlled domains.
///
/// Returns the updated transaction handle, or an error if the transaction already has an explicit
/// high-water mark. Table-feature and current-table-state validation occurs during commit.
///
/// # Safety
///
/// Both handles must be valid. This call borrows `engine` and unconditionally consumes `txn`,
/// including on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_row_tracking_high_water_mark(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    high_water_mark: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = with_row_tracking_high_water_mark_impl(*txn, high_water_mark);
    unsafe { result.into_extern_result(&engine) }
}

fn with_row_tracking_high_water_mark_impl(
    txn: Transaction,
    high_water_mark: i64,
) -> KernelResult<Handle<ExclusiveUpdateTableTransaction>> {
    Ok(Box::new(txn.with_row_tracking_high_water_mark(high_water_mark)?).into())
}

/// Stages `file` to be committed as this transaction's root manifest.
///
/// # Safety
///
/// All handles, `file`, and its nested path slice must be valid. This call borrows `engine` and
/// `file` and unconditionally consumes `txn`, including on error.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_with_root_manifest_file(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    file: &FileMeta,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = with_root_manifest_file_impl(*txn, file);
    unsafe { result.into_extern_result(&engine) }
}

#[cfg(feature = "adaptive-metadata-in-dev")]
fn with_root_manifest_file_impl(
    txn: Transaction,
    file: &FileMeta,
) -> KernelResult<Handle<ExclusiveUpdateTableTransaction>> {
    let path: &str = unsafe { TryFromStringSlice::try_from_slice(&file.path) }?;
    let location = Url::parse(path)?;
    let size = file
        .size
        .try_into()
        .map_err(|_| delta_kernel::KernelError::generic("manifest size does not fit a FileSize"))?;
    let delta_file = delta_kernel::FileMeta {
        location,
        last_modified: file.last_modified,
        size,
    };
    Ok(Box::new(txn.with_root_manifest_file(delta_file)?).into())
}

/// Add file metadata to the transaction for files that have been written. The metadata contains
/// information about files written during the transaction that will be added to the Delta log
/// during commit.
///
/// # Safety
///
/// Both handles must be valid. This call mutably borrows `txn` and consumes `write_metadata`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_add_files(
    mut txn: Handle<ExclusiveUpdateTableTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
) {
    let txn = unsafe { txn.as_mut() };
    let write_metadata = unsafe { write_metadata.into_inner() };
    txn.add_files(write_metadata);
}

/// Remove selected files from an existing-table transaction.
///
/// `data` must use the scan-row schema and be derived from scan metadata. The removal selection
/// must select only rows active in the original scan selection vector.
///
/// A null or empty selection vector selects every row. Use it only when every row is active and
/// intended for removal. The engine-data handle is consumed even on error, while the transaction
/// and engine handles remain owned by the caller.
///
/// # Safety
///
/// All handles must be valid. This call mutably borrows `txn`, borrows `engine` and the selection
/// vector, and unconditionally consumes `data`, including on error. When `selection_vector` is
/// non-null and `selection_vector_len` is nonzero, it must address that many readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_remove_files(
    mut txn: Handle<ExclusiveUpdateTableTransaction>,
    data: Handle<ExclusiveEngineData>,
    selection_vector: *const u8,
    selection_vector_len: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let data = unsafe { data.into_inner() };
    let txn = unsafe { txn.as_mut() };
    let selection_vector = if selection_vector.is_null() || selection_vector_len == 0 {
        vec![]
    } else {
        let raw = unsafe { std::slice::from_raw_parts(selection_vector, selection_vector_len) };
        raw.iter().map(|&value| value != 0).collect()
    };
    let result: KernelResult<bool> = (|| {
        let filtered = FilteredEngineData::try_new(data, selection_vector)?;
        txn.remove_files(filtered);
        Ok(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Marks the update as a blind append assertion.
///
/// # Safety
///
/// `builder` must be valid and is consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_blind_append(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
) -> Handle<ExclusiveUpdateTableTransactionBuilder> {
    let builder = unsafe { *builder.into_inner() };
    Box::new(builder.with_blind_append()).into()
}

/// Attempt to commit a transaction to the table. On success, returns a handle to the
/// [`CommittedTransaction`] from which the caller can read the version and the optional
/// post-commit snapshot. The returned handle must be freed with [`free_committed_transaction`].
///
/// Returns an error if the commit fails. The FFI surfaces conflicted and retryable
/// `CommitResult` variants as errors today (see TODO on `commit_result_to_committed_handle`).
///
/// # Safety
///
/// Both handles must be valid. This call borrows `engine` and unconditionally consumes `txn`,
/// including on error. Do not use or free `txn` afterward.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_commit(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let engine = extern_engine.engine();
    let result = commit_result_to_committed_handle(txn.commit(engine.as_ref()));
    unsafe { result.into_extern_result(&extern_engine) }
}
