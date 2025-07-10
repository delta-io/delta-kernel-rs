//! Utility functions used for tests in this crate.

use crate::error::{EngineError, ExternResult, KernelError};
use crate::{KernelStringSlice, NullableCvoid, TryFromStringSlice};
use std::os::raw::c_void;
use std::ptr::NonNull; // TODO: move?

#[no_mangle]
pub(crate) extern "C" fn allocate_err(
    etype: KernelError,
    _: KernelStringSlice,
) -> *mut EngineError {
    let boxed = Box::new(EngineError { etype });
    Box::leak(boxed)
}

#[no_mangle]
pub(crate) extern "C" fn allocate_str(kernel_str: KernelStringSlice) -> NullableCvoid {
    let s = unsafe { String::try_from_slice(&kernel_str) };
    let ptr = Box::into_raw(Box::new(s.unwrap())).cast(); // never null
    let ptr = unsafe { NonNull::new_unchecked(ptr) };
    Some(ptr)
}

// helper to recover an error from the above
pub(crate) unsafe fn recover_error(ptr: *mut EngineError) -> EngineError {
    *Box::from_raw(ptr)
}

// helper to recover a string from the above
pub(crate) fn recover_string(ptr: NonNull<c_void>) -> String {
    let ptr = ptr.as_ptr().cast();
    *unsafe { Box::from_raw(ptr) }
}
pub(crate) fn ok_or_panic<T>(result: ExternResult<T>) -> T {
    match result {
        ExternResult::Ok(t) => t,
        ExternResult::Err(e) => unsafe {
            panic!("Got engine error with type {:?}", (*e).etype);
        },
    }
}
