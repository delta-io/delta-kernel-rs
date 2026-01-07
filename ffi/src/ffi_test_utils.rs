//! Utility functions used for tests in this crate.

use crate::error::{EngineError, ExternResult, KernelError};
use crate::{KernelStringSlice, NullableCvoid, TryFromStringSlice};
use std::os::raw::c_void;
use std::ptr::NonNull;

// Used to allocate EngineErrors with test information from Rust tests
#[cfg(test)]
#[repr(C)]
pub(crate) struct EngineErrorWithMessage {
    pub(crate) etype: KernelError,
    pub(crate) message: String,
}

#[no_mangle]
pub(crate) extern "C" fn allocate_err(
    etype: KernelError,
    message: KernelStringSlice,
) -> *mut EngineError {
    let message = unsafe { String::try_from_slice(&message).unwrap() };
    let boxed = Box::new(EngineErrorWithMessage { etype, message });

    Box::into_raw(boxed) as *mut EngineError
}

#[no_mangle]
pub(crate) extern "C" fn allocate_str(kernel_str: KernelStringSlice) -> NullableCvoid {
    let s = unsafe { String::try_from_slice(&kernel_str) };
    let ptr = Box::into_raw(Box::new(s.unwrap())).cast(); // never null
    let ptr = unsafe { NonNull::new_unchecked(ptr) };
    Some(ptr)
}

/// Recover an error from 'allocate_err'
pub(crate) unsafe fn recover_error(ptr: *mut EngineError) -> EngineErrorWithMessage {
    *Box::from_raw(ptr as *mut EngineErrorWithMessage)
}

/// Recover a string from `allocate_str`
pub(crate) fn recover_string(ptr: NonNull<c_void>) -> String {
    let ptr = ptr.as_ptr().cast();
    *unsafe { Box::from_raw(ptr) }
}

pub(crate) fn ok_or_panic<T>(result: ExternResult<T>) -> T {
    match result {
        ExternResult::Ok(t) => t,
        ExternResult::Err(e) => unsafe {
            let error = recover_error(e);
            panic!(
                "Got engine error with type {:?} message: {}",
                error.etype, error.message
            );
        },
    }
}

/// Check error type and message while also recovering the error to prevent leaks
pub(crate) fn assert_extern_result_error_with_message<T>(
    res: ExternResult<T>,
    expected_etype: KernelError,
    expected_message: &str,
) {
    match res {
        ExternResult::Err(e) => {
            let error = unsafe { recover_error(e) };
            assert_eq!(error.etype, expected_etype);
            assert_eq!(error.message, expected_message);
        }
        _ => panic!("Expected error of type '{expected_etype:?}' and message '{expected_message}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    #[test]
    fn test_ok_or_panic_with_error() {
        // Create a test error
        let message = "Test error message";
        let error_ptr = allocate_err(
            KernelError::GenericError,
            KernelStringSlice {
                ptr: message.as_ptr() as *const i8,
                len: message.len(),
            },
        );
        let result = ExternResult::<i32>::Err(error_ptr);

        // Test that ok_or_panic panics with the expected message
        let panic_result = panic::catch_unwind(|| {
            ok_or_panic(result);
        });

        assert!(panic_result.is_err(), "Expected ok_or_panic to panic");

        // Check that the panic message contains the error type and message
        let panic_message = panic_result.unwrap_err();
        let panic_str = if let Some(s) = panic_message.downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic type".to_string()
        };

        assert!(
            panic_str.contains("Got engine error with type"),
            "Panic message should contain 'Got engine error with type', got: {}",
            panic_str
        );
        assert!(
            panic_str.contains("GenericError"),
            "Panic message should contain error type 'GenericError', got: {}",
            panic_str
        );
        assert!(
            panic_str.contains(message),
            "Panic message should contain error message 'Test error message', got: {}",
            panic_str
        );
    }

    #[test]
    fn test_ok_or_panic_with_ok() {
        // Test that ok_or_panic returns the value when the result is Ok
        let result = ExternResult::<i32>::Ok(42);
        let value = ok_or_panic(result);
        assert_eq!(value, 42);
    }
}
