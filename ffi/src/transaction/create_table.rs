//! Create-table transaction FFI lifecycle.

use super::committed::commit_result_to_committed_handle;
use super::*;

/// A handle for a create-table transaction (`Transaction<CreateTable>`).
///
/// Returned by [`create_table_txn_builder_build`]. Only supports operations valid during table
/// creation: adding files, late-bound commit information, domain metadata, and committing.
/// Operations like
/// file removal, blind append, and deletion vector updates are not available.
#[handle_descriptor(target=CreateTableTransaction, mutable=true, sized=true)]
pub struct ExclusiveCreateTableTransaction;

// ============================================================================
// Create-table transaction FFI functions
// ============================================================================

/// Replaces create-table operation metrics after writing and before commit.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `metrics`
/// and unconditionally consumes `txn`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_with_operation_metrics(
    txn: Handle<ExclusiveCreateTableTransaction>,
    metrics: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_nullable_string_map(txn, metrics, CreateTableTransaction::with_operation_metrics)
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Replaces the connector-defined create-table `commitInfo` row before commit.
///
/// # Safety
///
/// All handles and `schema` must be valid. This call borrows `engine` and `schema` and
/// unconditionally consumes both `txn` and `commit_info`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_with_commit_info(
    txn: Handle<ExclusiveCreateTableTransaction>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_commit_info(
            txn,
            commit_info,
            schema,
            CreateTableTransaction::with_commit_info,
        )
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Free a create-table transaction handle without committing.
///
/// # Safety
///
/// `txn` must be a valid handle and is consumed. Do not use or free it again.
#[no_mangle]
pub unsafe extern "C" fn free_create_table_txn(txn: Handle<ExclusiveCreateTableTransaction>) {
    txn.drop_handle();
}

/// Add domain metadata to a create-table transaction.
///
/// `domain` identifies the user-controlled metadata domain, and `configuration` is its arbitrary
/// string value. Returns the updated transaction handle. Invalid strings are returned as errors;
/// domain and table-feature validation occurs when the transaction is committed.
///
/// # Safety
///
/// All handles and strings must be valid. This call borrows `engine`, `domain`, and `configuration`
/// and unconditionally consumes `txn`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_with_domain_metadata(
    txn: Handle<ExclusiveCreateTableTransaction>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_domain_metadata(
            txn,
            domain,
            configuration,
            CreateTableTransaction::with_domain_metadata,
        )
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Add file metadata to a create-table transaction for files that have been written. The metadata
/// contains information about files written during the transaction that will be added to the
/// Delta log during commit.
///
/// # Safety
///
/// Both handles must be valid. This call mutably borrows `txn` and consumes `write_metadata`.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_add_files(
    mut txn: Handle<ExclusiveCreateTableTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
) {
    let txn = unsafe { txn.as_mut() };
    let write_metadata = unsafe { write_metadata.into_inner() };
    txn.add_files(write_metadata);
}

/// Attempt to commit a create-table transaction. On success, returns a handle to the
/// [`CommittedTransaction`] from which the caller can read the version and the optional
/// post-commit snapshot. The returned handle must be freed with [`free_committed_transaction`].
///
/// Returns an error if the commit fails.
///
/// # Safety
///
/// Both handles must be valid. This call borrows `engine` and unconditionally consumes `txn`,
/// including on error. Do not use or free `txn` afterward.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_commit(
    txn: Handle<ExclusiveCreateTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let engine = extern_engine.engine();
    commit_result_to_committed_handle(txn.commit(engine.as_ref()))
        .into_extern_result(&extern_engine)
}

// ============================================================================
// Create Table DDL
// ============================================================================

/// A handle representing an exclusive [`CreateTableTransactionBuilder`].
///
/// The caller must eventually either call [`create_table_txn_builder_build`] (which consumes the
/// handle and returns a transaction) or [`free_create_table_txn_builder`] (which drops it without
/// creating anything).
#[handle_descriptor(target=CreateTableTransactionBuilder, mutable=true, sized=true)]
pub struct ExclusiveCreateTableTransactionBuilder;

/// Sets whether files added during creation represent a logical data change.
///
/// `data_change` defaults to `true`. Returns the updated builder handle. Consecutive calls
/// replace the previous value, which is preserved through commit even when no files are added.
///
/// # Safety
///
/// `builder` must be valid. This unconditionally consumes `builder`.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_data_change(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    data_change: bool,
) -> Handle<ExclusiveCreateTableTransactionBuilder> {
    let builder = unsafe { *builder.into_inner() };
    Box::new(builder.with_data_change(data_change)).into()
}

/// Attaches a correlation identifier to create transaction metric events.
///
/// Repeated calls replace the previous value; an empty value clears it.
///
/// # Safety
///
/// All handles and `correlation_id` must be valid. This call borrows `engine` and `correlation_id`
/// and unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_correlation_id(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    correlation_id: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let correlation_id: Result<String> =
        unsafe { TryFromStringSlice::try_from_slice(&correlation_id) };
    correlation_id
        .map(|id| Box::new(builder.with_correlation_id(id)).into())
        .into_extern_result(&engine)
}

/// Replaces create-table operation parameters recorded in `commitInfo`.
///
/// Duplicate keys are rejected by the FFI map decoder; consecutive calls replace rather than
/// merge.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `parameters`
/// and unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_operation_parameters(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    parameters: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_nullable_string_map(
            builder,
            parameters,
            CreateTableTransactionBuilder::with_operation_parameters,
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Replaces create-table operation metrics recorded in `commitInfo` before writes begin.
///
/// Metrics supplied to the built transaction replace these metrics. Dedicated metrics override
/// the nested `operationMetrics` field in connector commit information.
///
/// # Safety
///
/// All handles and nested map pointers must be valid. This call borrows `engine` and `metrics`
/// and unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_operation_metrics(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    metrics: &FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_nullable_string_map(
            builder,
            metrics,
            CreateTableTransactionBuilder::with_operation_metrics,
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Supplies one connector-defined `commitInfo` row to the create-table builder.
///
/// Repeated calls replace the prior row. Kernel-owned fields and dedicated parameter or metric
/// maps take precedence over same-named nested fields.
///
/// # Safety
///
/// All handles and `schema` must be valid. This call borrows `engine` and `schema` and
/// unconditionally consumes both `builder` and `commit_info`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_commit_info(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: &EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_commit_info(
            builder,
            commit_info,
            schema,
            CreateTableTransactionBuilder::with_commit_info,
        )
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Adds an application transaction identifier to a create-table builder.
///
/// Duplicate application ids are rejected when the builder is built.
///
/// # Safety
///
/// All handles and `app_id` must be valid. This call borrows `engine` and `app_id` and
/// unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_transaction_id(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    app_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let app_id: Result<String> = unsafe { TryFromStringSlice::try_from_slice(&app_id) };
    app_id
        .map(|app_id| Box::new(builder.with_transaction_id(app_id, version)).into())
        .into_extern_result(&engine)
}

/// Adds user-controlled domain metadata to the create-table builder.
///
/// Duplicate domains are rejected when the builder is built.
///
/// # Safety
///
/// All handles and strings must be valid. This call borrows `engine`, `domain`, and `configuration`
/// and unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_domain_metadata(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        apply_domain_metadata(
            builder,
            domain,
            configuration,
            CreateTableTransactionBuilder::with_domain_metadata,
        )
    };
    result
        .map(|builder| Box::new(builder).into())
        .into_extern_result(&engine)
}

/// Collect `num_columns` column-name string slices into owned `String`s.
///
/// Returns an empty `Vec` when `num_columns == 0` without dereferencing `columns`, so a null
/// pointer is sound in the empty case. Returns `Err` if any slice is not valid UTF-8.
///
/// # Safety
///
/// When `num_columns > 0`, `columns` must point to `num_columns` contiguous, valid
/// [`KernelStringSlice`] values whose backing bytes are readable for the duration of the call.
pub(super) unsafe fn collect_create_table_columns(
    columns: *const KernelStringSlice,
    num_columns: usize,
) -> Result<Vec<String>> {
    if num_columns == 0 {
        return Ok(Vec::new());
    }
    let slices = unsafe { std::slice::from_raw_parts(columns, num_columns) };
    slices
        .iter()
        .map(|slice| {
            unsafe { TryFromStringSlice::try_from_slice(slice) }.map(|s: &str| s.to_string())
        })
        .collect()
}

/// Set a clustered data layout on a [`CreateTableTransactionBuilder`] from an array of top-level
/// clustering column names (in order). Clustering and partitioning are mutually exclusive; the
/// last data-layout call wins. Column validation (existence, stats-eligible types, duplicates)
/// happens later at [`create_table_txn_builder_build`].
///
/// Only top-level columns are supported through this entry point (each slice is one column name);
/// nested clustering columns must be set on the Rust builder directly.
///
/// # Safety
///
/// `builder` and `engine` must be valid. This call borrows `engine` and the column array. When
/// `num_columns > 0`, `columns` must point to `num_columns` contiguous, valid `KernelStringSlice`
/// values whose backing bytes are readable for the duration of the call; `columns` may be null
/// when `num_columns == 0`. `builder` is consumed even when this returns an error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_clustering_columns(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    columns: *const KernelStringSlice,
    num_columns: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { *builder.into_inner() };
    let columns = unsafe { collect_create_table_columns(columns, num_columns) };
    create_table_txn_builder_with_data_layout_impl(builder, columns.map(DataLayout::clustered))
        .into_extern_result(&engine)
}

/// Set a partitioned data layout on a [`CreateTableTransactionBuilder`] from an array of top-level
/// partition column names (in order). Clustering and partitioning are mutually exclusive; the last
/// data-layout call wins. Column validation (existence, primitive types, subset of schema) happens
/// later at [`create_table_txn_builder_build`].
///
/// # Safety
///
/// `builder` and `engine` must be valid. This call borrows `engine` and the column array. When
/// `num_columns > 0`, `columns` must point to `num_columns` contiguous, valid `KernelStringSlice`
/// values whose backing bytes are readable for the duration of the call; `columns` may be null
/// when `num_columns == 0`. `builder` is consumed even when this returns an error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_partition_columns(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    columns: *const KernelStringSlice,
    num_columns: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { *builder.into_inner() };
    let columns = unsafe { collect_create_table_columns(columns, num_columns) };
    create_table_txn_builder_with_data_layout_impl(builder, columns.map(DataLayout::partitioned))
        .into_extern_result(&engine)
}

/// Applies a parsed layout while preserving consuming-handle semantics on parse failure.
pub(super) fn create_table_txn_builder_with_data_layout_impl(
    builder: CreateTableTransactionBuilder,
    layout: Result<DataLayout>,
) -> Result<Handle<ExclusiveCreateTableTransactionBuilder>> {
    Ok(Box::new(builder.with_data_layout(layout?)).into())
}

/// Create a new [`CreateTableTransactionBuilder`] for creating a Delta table at the given path.
///
/// `schema` supplies the table schema through the [`EngineSchema`] visitor callback.
///
/// The returned builder can be configured with [`create_table_txn_builder_with_table_property`]
/// before building with [`create_table_txn_builder_build`]. The engine is only used for error
/// reporting at this stage.
///
/// # Safety
///
/// `path`, `schema`, `engine_info`, and `engine` must be valid. All inputs are borrowed for the
/// call; the caller retains ownership.
#[no_mangle]
pub unsafe extern "C" fn new_create_table_txn_builder(
    path: KernelStringSlice,
    schema: &EngineSchema,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let path = unsafe { TryFromStringSlice::try_from_slice(&path) };
    let info = unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    new_create_table_txn_builder_impl(path, schema, info).into_extern_result(&engine)
}

fn new_create_table_txn_builder_impl(
    path: Result<&str>,
    schema: &EngineSchema,
    engine_info: Result<&str>,
) -> Result<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let schema = decode_engine_schema(schema)?;
    let builder = delta_kernel::transaction::create_table::create_table(
        path?,
        Arc::new(schema),
        engine_info?.to_string(),
    );
    Ok(Box::new(builder).into())
}

/// Add a single table property to a [`CreateTableTransactionBuilder`].
///
/// This consumes the builder handle and returns a new one. The caller MUST replace their handle
/// pointer with the returned handle. On error, the old builder handle is consumed and gone --
/// do not free or reuse it. There is no new handle to free either.
///
/// # Safety
///
/// `builder`, `key`, `value`, and `engine` must be valid. This call borrows `engine`, `key`, and
/// `value` and unconditionally consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_with_table_property(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    key: KernelStringSlice,
    value: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { *builder.into_inner() };
    let key = unsafe { TryFromStringSlice::try_from_slice(&key) };
    let value = unsafe { TryFromStringSlice::try_from_slice(&value) };
    create_table_txn_builder_with_table_property_impl(builder, key, value)
        .into_extern_result(&engine)
}

fn create_table_txn_builder_with_table_property_impl(
    builder: CreateTableTransactionBuilder,
    key: Result<String>,
    value: Result<String>,
) -> Result<Handle<ExclusiveCreateTableTransactionBuilder>> {
    let builder = builder.with_table_properties([(key?, value?)]);
    Ok(Box::new(builder).into())
}

/// Build a create-table transaction using the default [`FileSystemCommitter`]. Returns a
/// create-table transaction handle that can be used with [`create_table_txn_add_files`] and
/// [`create_table_txn_commit`] to optionally stage initial data before committing.
///
/// # Safety
///
/// Both handles must be valid. This call borrows `engine` and unconditionally consumes `builder`,
/// including on error. Do not use or free `builder` afterward.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_build(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let committer = Box::new(FileSystemCommitter::new());
    create_table_txn_builder_build_impl(builder, committer, extern_engine)
        .into_extern_result(&extern_engine)
}

/// Build a create-table transaction with a custom committer. Same as
/// [`create_table_txn_builder_build`] but uses the provided committer instead of the default.
///
/// # Safety
///
/// All handles must be valid. This call borrows `engine` and unconditionally consumes both
/// `builder` and `committer`, including on error. Do not use or free either consumed handle
/// afterward.
#[no_mangle]
pub unsafe extern "C" fn create_table_txn_builder_build_with_committer(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
    committer: Handle<MutableCommitter>,
) -> ExternResult<Handle<ExclusiveCreateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let committer = unsafe { committer.into_inner() };
    create_table_txn_builder_build_impl(builder, committer, extern_engine)
        .into_extern_result(&extern_engine)
}

fn create_table_txn_builder_build_impl(
    builder: CreateTableTransactionBuilder,
    committer: Box<dyn Committer>,
    extern_engine: &dyn ExternEngine,
) -> Result<Handle<ExclusiveCreateTableTransaction>> {
    let engine = extern_engine.engine();
    let create_txn = builder.build(engine.as_ref(), committer)?;
    Ok(Box::new(create_txn).into())
}

/// Free a [`CreateTableTransactionBuilder`] without building.
///
/// Use this on failure paths when the builder will not be built.
///
/// # Safety
///
/// `builder` must be a valid handle and is consumed. Do not use or free it again.
#[no_mangle]
pub unsafe extern "C" fn free_create_table_txn_builder(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
) {
    builder.drop_handle();
}
