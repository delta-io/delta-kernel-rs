//! Existing-table transaction FFI lifecycle.

use delta_kernel::engine_data::FilteredEngineData;

use super::*;

/// A handle for an existing-table transaction (`Transaction<ExistingTable>`).
///
/// Returned by an update-table builder's build function. Supports all transaction
/// operations including existing-table-only operations like blind append and file removal.
#[handle_descriptor(target=Transaction, mutable=true, sized=true)]
pub struct ExclusiveUpdateTableTransaction;

/// A handle for configuring a transaction against an existing table.
///
/// Every `update_table_txn_builder_with_*` function consumes its input handle and returns a
/// replacement. The caller must build or free the final handle.
#[handle_descriptor(target=UpdateTableTransactionBuilder, mutable=true, sized=true)]
pub struct ExclusiveUpdateTableTransactionBuilder;

/// Creates an update-table transaction builder from a snapshot.
///
/// # Safety
///
/// `snapshot` must be a valid shared handle. This call borrows it, and the caller retains
/// ownership.
#[no_mangle]
pub unsafe extern "C" fn new_update_table_txn_builder(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<ExclusiveUpdateTableTransactionBuilder> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    Box::new(snapshot.transaction_builder()).into()
}

/// Builds an update-table transaction with the default filesystem committer.
///
/// # Safety
///
/// `builder` and `engine` must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_build(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let transaction = builder.build(
        extern_engine.engine().as_ref(),
        Box::new(FileSystemCommitter::new()),
    );
    transaction
        .map(|txn| Box::new(txn).into())
        .into_extern_result(&extern_engine)
}

/// Starts an update-table transaction with a custom committer.
///
/// # Safety
///
/// `builder`, `engine`, and `committer` must be valid handles. This call borrows `engine` and
/// unconditionally consumes both `builder` and `committer`, including when building returns an
/// error.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_build_with_committer(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
    committer: Handle<MutableCommitter>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let committer = unsafe { committer.into_inner() };
    update_table_txn_builder_build_with_committer_impl(builder, engine, committer)
        .into_extern_result(&engine)
}

fn update_table_txn_builder_build_with_committer_impl(
    builder: UpdateTableTransactionBuilder,
    extern_engine: &dyn ExternEngine,
    committer: Box<dyn Committer>,
) -> DeltaResult<Handle<ExclusiveUpdateTableTransaction>> {
    let engine = extern_engine.engine();
    let transaction = builder.build(engine.as_ref(), committer);
    Ok(Box::new(transaction?).into())
}

/// Frees an update-table builder without building it.
///
/// # Safety
///
/// `builder` must be a valid handle and is consumed.
#[no_mangle]
pub unsafe extern "C" fn free_update_table_txn_builder(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
) {
    builder.drop_handle();
}

/// Attaches a correlation identifier to update transaction metric events.
///
/// Repeated calls replace the previous value. An empty value clears it.
///
/// # Safety
///
/// All handles and `correlation_id` must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_correlation_id(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    correlation_id: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let correlation_id: DeltaResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&correlation_id) };
    correlation_id
        .map(|id| Box::new(builder.with_correlation_id(id)).into())
        .into_extern_result(&engine)
}

/// Replaces the operation parameters recorded in `commitInfo`.
///
/// Empty or duplicate keys are rejected. This map replaces, rather than merges with, the prior
/// map. Dedicated parameters override same-named nested fields in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_operation_parameters(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    parameters: &FfiStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_string_map(
            builder,
            parameters,
            UpdateTableTransactionBuilder::with_operation_parameters,
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Replaces the operation metrics recorded in `commitInfo` before writes begin.
///
/// Empty or duplicate keys are rejected. Metrics supplied later through
/// [`Transaction::with_operation_metrics`] replace these builder metrics. Dedicated metrics
/// override the nested `operationMetrics` field in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_operation_metrics(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    metrics: &FfiStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_string_map(
            builder,
            metrics,
            UpdateTableTransactionBuilder::with_operation_metrics,
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Supplies one connector-defined `commitInfo` row to the update builder.
///
/// Repeated calls replace the previous row. Kernel-owned fields and dedicated operation
/// parameters or metrics take precedence over same-named fields in this row.
///
/// # Safety
///
/// All handles and `schema` must be valid. This unconditionally consumes both `builder` and
/// `commit_info`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_commit_info(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_commit_info(
            builder,
            commit_info,
            schema,
            |builder, commit_info, schema| Ok(builder.with_commit_info(commit_info, schema)),
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Adds user-controlled domain metadata to the update builder.
///
/// Duplicate domains and add/remove conflicts are rejected when the builder is built.
///
/// # Safety
///
/// All handles and strings must be valid. This unconditionally consumes `builder`.
#[no_mangle]
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
            |builder, domain, configuration| Ok(builder.with_domain_metadata(domain, configuration)),
        )
    };
    result
        .map(|builder| Box::new(builder).into())
        .into_extern_result(&engine)
}

/// Adds a nullable top-level field described by a single-field engine schema.
///
/// # Safety
///
/// All handles and `field` must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_add_column(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    field: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    decode_single_field(field)
        .map(|field| Box::new(builder.add_column(field)).into())
        .into_extern_result(&engine)
}

/// Adds a nullable field beneath a nested parent path.
///
/// # Safety
///
/// All handles and nested pointers must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_add_column_at(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    parent: &FfiColumnName,
    field: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let parent = unsafe { decode_column_name(parent) };
    parent
        .and_then(|parent| decode_single_field(field).map(|field| (parent, field)))
        .map(|(parent, field)| Box::new(builder.add_column_at(parent, field)).into())
        .into_extern_result(&engine)
}

/// Changes the field at a possibly nested path from non-nullable to nullable.
///
/// # Safety
///
/// All handles and nested pointers must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_set_nullable(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    column: &FfiColumnName,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe { decode_column_name(column) }
        .map(|column| Box::new(builder.set_nullable(column)).into())
        .into_extern_result(&engine)
}

/// Convert a [`CommitResult`] into a [`CommittedTransaction`] handle, or an error if the commit
/// was not successful.
///
/// The returned handle owns the [`CommittedTransaction`] and must be freed with
/// [`free_committed_transaction`].
///
/// TODO: expose the full `CommitResult` enum through FFI for conflict resolution.
pub(super) fn commit_result_to_committed_handle<S>(
    result: DeltaResult<CommitResult<S>>,
) -> DeltaResult<Handle<ExclusiveCommittedTransaction>> {
    match result? {
        CommitResult::Committed(committed) => Ok(Box::new(committed).into()),
        CommitResult::Retryable(_) => Err(delta_kernel::KernelError::unsupported(
            "commit failed: retryable transaction not supported in FFI (yet)",
        )),
        CommitResult::Conflicted(conflicted) => Err(delta_kernel::KernelError::Generic(format!(
            "commit conflict at version {}",
            conflicted.conflict_version()
        ))),
    }
}

unsafe fn decode_column_name(column: &FfiColumnName) -> DeltaResult<ColumnName> {
    let parts = unsafe { column.path.try_as_slice() }?
        .iter()
        .map(|part| unsafe { part.try_to_string() })
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(ColumnName::new(parts))
}

fn decode_single_field(schema: &EngineSchema) -> DeltaResult<delta_kernel::schema::StructField> {
    let schema = decode_engine_schema(schema)?;
    let mut fields = schema.into_fields();
    let field = fields.next().ok_or_else(|| {
        delta_kernel::KernelError::invalid_transaction_state(
            "add-column schema must contain exactly one field",
        )
    })?;
    if fields.next().is_some() {
        return Err(delta_kernel::KernelError::invalid_transaction_state(
            "add-column schema must contain exactly one field",
        ));
    }
    Ok(field)
}

// ============================================================================
// Existing-table transaction FFI functions
// ============================================================================

/// Replaces operation metrics after writing and before commit.
///
/// This is the late-binding counterpart to the builder setter. It replaces builder metrics and
/// the nested `operationMetrics` field in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This unconditionally consumes `txn`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_with_operation_metrics(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    metrics: &FfiStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe { apply_string_map(txn, metrics, Transaction::with_operation_metrics) }
        .map(|txn| Box::new(txn).into())
        .into_extern_result(&engine)
}

/// Replaces the connector-defined `commitInfo` row after writing and before commit.
///
/// Kernel-owned fields and dedicated parameter or metric maps take precedence over same-named
/// fields in this row.
///
/// # Safety
///
/// All handles and `schema` must be valid. This consumes both `txn` and `commit_info`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_with_commit_info(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_commit_info(txn, commit_info, schema, |txn, commit_info, schema| {
            Ok(txn.with_commit_info(commit_info, schema))
        })
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Free an existing-table transaction handle without committing.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_update_table_txn(txn: Handle<ExclusiveUpdateTableTransaction>) {
    txn.drop_handle();
}

/// Operations with stable Delta history names.
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum KernelUpdateTableOperation {
    Write,
    StreamingUpdate,
    AlterTable,
    Delete,
    Update,
    Merge,
    Optimize,
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

/// Attaches engine info to an update-table builder.
///
/// # Safety
///
/// `builder`, `engine_info`, and `engine` must be valid. This call consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_engine_info(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let info = unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    info.map(|info: &str| Box::new(builder.with_engine_info(info)).into())
        .into_extern_result(&engine)
}

/// Sets a known operation on an update-table builder.
///
/// # Safety
///
/// `builder` must be valid and is consumed by this call. `operation` must contain a valid
/// [`KernelUpdateTableOperation`] discriminant; any other tag is undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_operation(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    operation: KernelUpdateTableOperation,
) -> Handle<ExclusiveUpdateTableTransactionBuilder> {
    let builder = unsafe { *builder.into_inner() };
    Box::new(builder.with_operation(operation.into())).into()
}

/// Sets a connector-defined operation name on an update-table builder.
///
/// # Safety
///
/// `builder`, `operation`, and `engine` must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_custom_operation(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    operation: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let operation: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&operation) };
    operation
        .map(|operation: String| {
            Box::new(builder.with_operation(UpdateTableOperation::Custom(operation))).into()
        })
        .into_extern_result(&engine)
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
/// Caller is responsible for passing valid handles. CONSUMES the transaction handle and returns
/// a new one.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_with_domain_metadata(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_domain_metadata(txn, domain, configuration, |txn, domain, configuration| {
            Ok(txn.with_domain_metadata(domain, configuration))
        })
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Remove domain metadata from the table in this transaction. A tombstone action with
/// `removed: true` will be written to the Delta log when the transaction is committed.
///
/// The caller does not need to provide a configuration value -- the existing value is
/// automatically preserved in the tombstone.
///
/// # Safety
///
/// Caller is responsible for passing valid handles. CONSUMES the builder handle and returns a new
/// one.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_domain_metadata_removed(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    domain: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let domain: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&domain) };
    domain
        .map(|domain| Box::new(builder.with_domain_metadata_removed(domain)).into())
        .into_extern_result(&engine)
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
/// Caller is responsible for passing valid handles. CONSUMES the transaction handle and returns
/// a new one.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_with_row_tracking_high_water_mark(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    high_water_mark: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    with_row_tracking_high_water_mark_impl(*txn, high_water_mark).into_extern_result(&engine)
}

fn with_row_tracking_high_water_mark_impl(
    txn: Transaction,
    high_water_mark: i64,
) -> DeltaResult<Handle<ExclusiveUpdateTableTransaction>> {
    Ok(Box::new(txn.with_row_tracking_high_water_mark(high_water_mark)?).into())
}

/// Stages `file` to be committed as this transaction's root manifest.
///
/// # Safety
///
/// Caller is responsible for passing valid handles. CONSUMES the transaction handle.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_with_root_manifest_file(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    file: &FileMeta,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    with_root_manifest_file_impl(*txn, file).into_extern_result(&engine)
}

#[cfg(feature = "adaptive-metadata-in-dev")]
fn with_root_manifest_file_impl(
    txn: Transaction,
    file: &FileMeta,
) -> DeltaResult<Handle<ExclusiveUpdateTableTransaction>> {
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
/// Caller is responsible for passing a valid handle. Consumes write_metadata.
#[no_mangle]
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
/// A null or empty selection vector selects every row. The engine-data handle is consumed, while
/// the transaction and engine handles remain owned by the caller.
///
/// # Safety
///
/// All handles must be valid. When `selection_vector_len` is nonzero, `selection_vector` must
/// address that many readable bytes.
#[no_mangle]
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
    let result: DeltaResult<bool> = (|| {
        let filtered = FilteredEngineData::try_new(data, selection_vector)?;
        txn.remove_files(filtered);
        Ok(true)
    })();
    result.into_extern_result(&engine)
}

/// Sets whether file actions represent a logical data change.
///
/// # Safety
///
/// `builder` must be valid and is consumed.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_data_change(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    data_change: bool,
) -> Handle<ExclusiveUpdateTableTransactionBuilder> {
    let builder = unsafe { *builder.into_inner() };
    Box::new(builder.with_data_change(data_change)).into()
}

/// Marks the update as a blind append assertion.
///
/// # Safety
///
/// `builder` must be valid and is consumed.
#[no_mangle]
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
/// Caller is responsible for passing a valid handle. And MUST NOT USE transaction after this
/// method is called.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_commit(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let engine = extern_engine.engine();
    commit_result_to_committed_handle(txn.commit(engine.as_ref()))
        .into_extern_result(&extern_engine)
}
