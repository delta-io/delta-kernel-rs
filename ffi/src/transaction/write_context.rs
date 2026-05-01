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

/// Get the **logical** schema from a [`WriteContext`] handle. This is the user-facing schema
/// the engine should produce as the input to its logical-to-physical transform.
///
/// To write a column-mapping-enabled table, an engine MUST also use
/// [`get_physical_write_schema`] (the evaluator's output schema and the parquet writer's
/// schema) and [`get_logical_to_physical`] (the transform expression that drops partition
/// columns when not materialized). For non-column-mapping unpartitioned tables, the logical
/// and physical schemas are identical.
///
/// The returned schema must be freed when no longer needed via [`crate::free_schema`].
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

/// Get the **physical write schema** from a [`WriteContext`] handle. This is the schema of
/// the data that will be written to parquet files:
///
/// - Field names are physical column-mapping names (e.g. `col-<uuid>`) when column mapping is
///   enabled, and identical to the logical names otherwise.
/// - Per-field `parquet.field.id` metadata is stamped under both `Id` and `Name` modes so the
///   parquet footer carries field IDs (required by the Delta protocol).
/// - Partition columns are excluded unless the table has the `materializePartitionColumns` feature
///   enabled.
///
/// Engines that perform their own logical-to-physical transformation must pass this schema
/// as the output schema of the expression evaluator (see
/// [`crate::engine_funcs::new_expression_evaluator`]). Engines that write parquet directly
/// must use this as the writer's schema so that field IDs and physical names land in the file.
///
/// The returned schema must be freed when no longer needed via [`crate::free_schema`].
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

/// Get the **logical-to-physical transform** from a [`WriteContext`] handle. This is the
/// kernel-built [`Expression`] an engine must apply (via an [`ExpressionEvaluator`]) to
/// every batch of logical data before writing it to a parquet file.
///
/// The expression itself only encodes structural changes -- specifically, dropping partition
/// columns when `materializePartitionColumns` is not enabled. The logical-to-physical column
/// rename is carried by the schema returned from [`get_physical_write_schema`], not by this
/// expression. Callers must therefore construct an evaluator with:
///
/// - input schema  = [`get_write_schema`] (logical)
/// - transform     = this expression
/// - output schema = [`get_physical_write_schema`] (physical)
///
/// The returned expression must be freed when no longer needed via
/// [`crate::expressions::free_kernel_expression`].
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
///
/// [`Expression`]: delta_kernel::Expression
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
