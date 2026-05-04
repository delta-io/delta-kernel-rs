use std::sync::Arc;

use delta_kernel::transaction::WriteContext;
use delta_kernel_ffi_macros::handle_descriptor;

use super::{ExclusiveCreateTransaction, ExclusiveTransaction};
use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::SharedExpression;
use crate::handle::Handle;
use crate::{
    kernel_string_slice, AllocateStringFn, NullableCvoid, SharedExternEngine, SharedSchema,
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
/// For partitioned tables, use a partitioned write context instead.
/// TODO(#2355): expose partitioned_write_context via FFI.
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
/// For partitioned tables, use a partitioned write context instead.
/// TODO(#2355): expose partitioned_write_context via FFI.
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
/// excluded unless the `materializePartitionColumns` writer feature is enabled.
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

/// Returns the logical-to-physical transform from a [`WriteContext`] handle. Engines apply
/// it via an [`ExpressionEvaluator`] to each batch of logical data before writing parquet.
/// It drops partition columns when `materializePartitionColumns` is not enabled. The column
/// rename itself is encoded in the physical schema (the evaluator matches input columns to
/// output fields by position), not in this expression.
///
/// To build the evaluator, pass [`get_write_schema`] as the input, this expression as the
/// transform, and [`get_physical_write_schema`] as the output. See
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
/// prefixes). See TODO(#2355) for full partitioned write support via FFI.
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
