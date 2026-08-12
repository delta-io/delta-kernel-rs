use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::expressions::Scalar;
use delta_kernel::transaction::WriteContext;
use delta_kernel::{DeltaResult, Error};
use delta_kernel_ffi_macros::handle_descriptor;

use super::partition_value::{ExclusivePartitionValueMap, PartitionValueMap};
use super::{ExclusiveCreateTransaction, ExclusiveTransaction};
use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::SharedExpression;
use crate::handle::Handle;
use crate::{
    kernel_string_slice, AllocateStringFn, KernelStringSlice, NullableCvoid, SharedExternEngine,
    SharedSchema, TryFromStringSlice, Url,
};

/// A [`WriteContext`] that provides schema and path information needed for writing data.
/// This is a shared reference that can be cloned and used across multiple consumers.
///
/// The [`WriteContext`] must be freed using [`free_write_context`] when no longer needed.
#[handle_descriptor(target=WriteContext, mutable=false, sized=true)]
pub struct SharedWriteContext;

/// Gets the write context from a transaction for an unpartitioned table. The write context
/// provides schema and path information needed for writing data.
///
/// For partitioned tables, use [`get_partitioned_write_context`] instead. Returns an error if the
/// table is partitioned.
///
/// # Safety
///
/// Caller is responsible for passing a [valid][Handle#Validity] transaction handle and engine.
#[no_mangle]
pub unsafe extern "C" fn get_unpartitioned_write_context(
    txn: Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedWriteContext>> {
    let txn = unsafe { txn.as_ref() };
    let engine = unsafe { engine.as_ref() };
    txn.unpartitioned_write_context()
        .map(|wc| Arc::new(wc).into())
        .into_extern_result(&engine)
}

/// Gets the write context from a create-table transaction for an unpartitioned table.
///
/// For partitioned tables, use [`create_table_get_partitioned_write_context`] instead. Returns an
/// error if the table is partitioned.
///
/// # Safety
///
/// Caller is responsible for passing a [valid][Handle#Validity] transaction handle and engine.
#[no_mangle]
pub unsafe extern "C" fn create_table_get_unpartitioned_write_context(
    txn: Handle<ExclusiveCreateTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedWriteContext>> {
    let txn = unsafe { txn.as_ref() };
    let engine = unsafe { engine.as_ref() };
    txn.unpartitioned_write_context()
        .map(|wc| Arc::new(wc).into())
        .into_extern_result(&engine)
}

/// Gets the write context from a transaction for a partitioned table, for the partition described
/// by `partition_values`. A separate write context (and write directory) is needed per partition,
/// so call this once per distinct set of partition values.
///
/// `partition_values` maps each partition column's logical name to its value; build it with
/// [`partition_value_map_new`](super::partition_value::partition_value_map_new) and the
/// `partition_value_map_insert_*` functions. The map must contain exactly the table's partition
/// columns (the kernel validates completeness and value types and rejects extras). This function
/// consumes the map handle on both success and error; do not use or free it afterward.
///
/// Returns an error if the table is not partitioned (use [`get_unpartitioned_write_context`]
/// instead) or if the partition values are invalid for the table's partition schema.
///
/// # Safety
///
/// Caller is responsible for passing a [valid][Handle#Validity] transaction handle, partition
/// value map handle, and engine.
#[no_mangle]
pub unsafe extern "C" fn get_partitioned_write_context(
    txn: Handle<ExclusiveTransaction>,
    partition_values: Handle<ExclusivePartitionValueMap>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedWriteContext>> {
    let txn = unsafe { txn.as_ref() };
    let partition_values = unsafe { partition_values.into_inner() };
    let engine = unsafe { engine.as_ref() };
    partitioned_write_context_impl(|pv| txn.partitioned_write_context(pv), *partition_values)
        .into_extern_result(&engine)
}

/// Gets the write context from a create-table transaction for a partitioned table. See
/// [`get_partitioned_write_context`] for the contract; this is the create-table counterpart.
///
/// # Safety
///
/// Caller is responsible for passing a [valid][Handle#Validity] transaction handle, partition
/// value map handle, and engine.
#[no_mangle]
pub unsafe extern "C" fn create_table_get_partitioned_write_context(
    txn: Handle<ExclusiveCreateTransaction>,
    partition_values: Handle<ExclusivePartitionValueMap>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedWriteContext>> {
    let txn = unsafe { txn.as_ref() };
    let partition_values = unsafe { partition_values.into_inner() };
    let engine = unsafe { engine.as_ref() };
    partitioned_write_context_impl(|pv| txn.partitioned_write_context(pv), *partition_values)
        .into_extern_result(&engine)
}

/// Shared body for the partitioned write-context entry points: hand the owned partition values to
/// the transaction's `partitioned_write_context` and wrap the result in a shared handle.
fn partitioned_write_context_impl(
    build: impl FnOnce(HashMap<String, Scalar>) -> DeltaResult<WriteContext>,
    partition_values: PartitionValueMap,
) -> DeltaResult<Handle<SharedWriteContext>> {
    let wc = build(partition_values.inner)?;
    Ok(Arc::new(wc).into())
}

#[no_mangle]
pub unsafe extern "C" fn free_write_context(write_context: Handle<SharedWriteContext>) {
    write_context.drop_handle();
}

/// Returns the logical (user-facing) write schema from a [`WriteContext`] handle. For
/// column-mapping-enabled writes, pair with [`get_physical_write_schema`] and
/// [`get_logical_to_physical`].
///
/// The returned schema must be freed via [`crate::free_schema`].
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_write_schema(
    write_context: Handle<SharedWriteContext>,
) -> Handle<SharedSchema> {
    let write_context = unsafe { write_context.as_ref() };
    write_context.logical_schema().clone().into()
}

/// Returns the physical write schema from a [`WriteContext`] handle: the schema of the data
/// written to parquet files. With column mapping enabled, field names are physical
/// (e.g. `col-<uuid>`) and each field has a `parquet.field.id` metadata entry per the Delta
/// column-mapping spec; otherwise it matches the logical schema. Partition columns are
/// excluded unless the `materializePartitionColumns` writer feature or `IcebergCompatV3` is
/// enabled.
///
/// Use this as the parquet writer schema and as the output schema of the evaluator built
/// from [`get_logical_to_physical`].
///
/// The returned schema must be freed via [`crate::free_schema`].
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_physical_write_schema(
    write_context: Handle<SharedWriteContext>,
) -> Handle<SharedSchema> {
    let write_context = unsafe { write_context.as_ref() };
    write_context.physical_schema().clone().into()
}

/// Returns the logical-to-physical expression from a [`WriteContext`] handle. Engines apply
/// it via an [`ExpressionEvaluator`] to each batch of logical data before writing parquet.
/// The logical data batches must not contain partition columns. The column rename itself is encoded
/// in the physical schema (the evaluator matches input columns to output fields by position), not
/// in this expression.
///
/// To build the evaluator, pass the schema of the partition-free input data as the input, this
/// value as the expression to evaluate, and [`get_physical_write_schema`] as the output. See
/// [`crate::engine_funcs::new_expression_evaluator`].
///
/// The returned expression must be freed via [`crate::expressions::free_kernel_expression`].
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
///
/// [`ExpressionEvaluator`]: delta_kernel::ExpressionEvaluator
#[no_mangle]
pub unsafe extern "C" fn get_logical_to_physical(
    write_context: Handle<SharedWriteContext>,
) -> Handle<SharedExpression> {
    let write_context = unsafe { write_context.as_ref() };
    write_context.logical_to_physical().into()
}

/// Get the table root URL from a WriteContext handle. Returns the table root, not the
/// recommended write directory (which may include Hive-style partition paths or random
/// prefixes); use [`get_write_dir`] for the latter.
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_write_path(
    write_context: Handle<SharedWriteContext>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let write_context = unsafe { write_context.as_ref() };
    let write_path = write_context.table_root_dir().to_string();
    allocate_fn(kernel_string_slice!(write_path))
}

/// Get the recommended directory URL for writing data files from a WriteContext handle.
/// Connectors should write files as `<write_dir>/<uuid>.parquet`. For a partitioned write context
/// this includes the Hive-style partition prefix (e.g. `year=2024/`) when column mapping is off, or
/// a random prefix when column mapping or `delta.randomizeFilePrefixes` is on.
///
/// The returned URL is URI-encoded. Engines that write to a local filesystem must URI-decode it
/// once before using it as a path; the still-encoded URL (plus the file name) is what
/// [`resolve_file_path`] expects to produce the `add.path` recorded in the Delta log.
///
/// A fresh random prefix is generated on each call when column mapping or random prefixes are
/// enabled, so call this once per file batch and reuse the result.
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_write_dir(
    write_context: Handle<SharedWriteContext>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let write_context = unsafe { write_context.as_ref() };
    let write_dir = write_context.write_dir().to_string();
    allocate_fn(kernel_string_slice!(write_dir))
}

/// Visit the serialized partition values of a WriteContext handle by invoking `visitor` once per
/// partition column. Keys are *physical* column names (column-mapping applied) and values are the
/// protocol-serialized strings the engine must record in each Add action's `partitionValues`. When
/// a partition value is null, `is_null` is `true` and `value` is an empty slice. For an
/// unpartitioned write context, `visitor` is never called. Entries are visited in sorted key order
/// so the callback sequence is deterministic across runs.
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer, a valid `engine_context`
/// pointer passed through to each `visitor` invocation, and a valid `visitor` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_partition_values(
    write_context: Handle<SharedWriteContext>,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
        is_null: bool,
    ),
) {
    let write_context = unsafe { write_context.as_ref() };
    let values = write_context.physical_partition_values();
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    for key in keys {
        let value = &values[key];
        let value_str = value.as_deref().unwrap_or("");
        visitor(
            engine_context,
            kernel_string_slice!(key),
            kernel_string_slice!(value_str),
            value.is_none(),
        );
    }
}

/// Compute the relative `add.path` for the Delta log from the absolute URL of a data file the
/// engine has written. `file_url` is the full (URI-encoded) URL of the written file, typically
/// formed by appending the file name to [`get_write_dir`]'s result.
///
/// Returns an error if `file_url` is not a valid URL or does not live under the table root.
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer, a valid `file_url` slice, and
/// a valid engine handle.
#[no_mangle]
pub unsafe extern "C" fn resolve_file_path(
    write_context: Handle<SharedWriteContext>,
    file_url: KernelStringSlice,
    allocate_fn: AllocateStringFn,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<NullableCvoid> {
    let write_context = unsafe { write_context.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let file_url: DeltaResult<&str> = unsafe { TryFromStringSlice::try_from_slice(&file_url) };
    resolve_file_path_impl(write_context, file_url)
        .map(|path| allocate_fn(kernel_string_slice!(path)))
        .into_extern_result(&engine)
}

fn resolve_file_path_impl(
    write_context: &WriteContext,
    file_url: DeltaResult<&str>,
) -> DeltaResult<String> {
    let url = Url::parse(file_url?).map_err(|e| {
        Error::generic(format!("invalid file URL passed to resolve_file_path: {e}"))
    })?;
    write_context.resolve_file_path(&url)
}
