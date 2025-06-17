use crate::handle::Handle;
use crate::{kernel_string_slice, AllocateStringFn, NullableCvoid, SharedSchema};
use delta_kernel::transaction::WriteContext;
use delta_kernel_ffi_macros::handle_descriptor;

use std::sync::Arc;

use super::ExclusiveTransaction;

/// A [`WriteContext`] that provides schema and path information needed for writing data.
/// This is a shared reference that can be cloned and used across multiple consumers.
///
/// The [`WriteContext`] must be freed using [`free_write_context`] when no longer needed.
#[handle_descriptor(target=WriteContext, mutable=false, sized=true)]
pub struct SharedWriteContext;

/// Gets the write context from a transaction. The write context provides schema and path information
/// needed for writing data.
///
/// # Safety
///
/// Caller is responsible for passing a [valid][Handle#Validity] transaction handle.
#[no_mangle]
pub unsafe extern "C" fn get_write_context(
    txn: Handle<ExclusiveTransaction>,
) -> Handle<SharedWriteContext> {
    let txn = unsafe { txn.as_ref() };
    Arc::new(txn.get_write_context()).into()
}

#[no_mangle]
pub unsafe extern "C" fn free_write_context(write_context: Handle<SharedWriteContext>) {
    write_context.drop_handle();
}

/// Get schema from WriteContext handle. The schema must be freed when no longer needed via
/// [`free_schema`].
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_write_schema(
    write_context: Handle<SharedWriteContext>,
) -> Handle<SharedSchema> {
    let write_context = unsafe { write_context.as_ref() };
    write_context.schema().clone().into()
}

/// Get write path from WriteContext handle.
///
/// # Safety
/// Engine is responsible for providing a valid WriteContext pointer
#[no_mangle]
pub unsafe extern "C" fn get_write_path(
    write_context: Handle<SharedWriteContext>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let write_context = unsafe { write_context.as_ref() };
    let write_path = write_context.target_dir().to_string();
    allocate_fn(kernel_string_slice!(write_path))
}
