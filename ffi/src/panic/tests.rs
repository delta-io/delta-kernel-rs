use std::panic::panic_any;
use std::sync::Arc;

use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;

use super::catch_panic;
use crate::error::{ExternResult, FFIKernelError, IntoExternResult};
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error, ok_or_panic,
    recover_error,
};

struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic_any(Self);
    }
}

#[rstest]
#[case::literal(|| panic!("literal panic"), "literal panic")]
#[case::owned(|| panic_any("owned panic".to_owned()), "owned panic")]
#[case::opaque(|| panic_any(42_u32), "non-string panic payload")]
#[case::capacity(|| drop(Vec::<u8>::with_capacity(usize::MAX)), "capacity overflow")]
#[case::panicking_drop(|| panic_any(PanicOnDrop), "non-string panic payload")]
fn contains_panics(#[case] panic: fn(), #[case] expected: &str) {
    let allocation = Arc::new(());
    let weak = Arc::downgrade(&allocation);
    let line = line!() + 1;
    let result = catch_panic::<(), KernelError>(|| {
        let _allocation = allocation;
        panic();
        Ok(())
    });
    assert!(weak.upgrade().is_none());
    let result = unsafe { result.into_extern_result(&TEST_ERROR_ALLOCATORS) };
    let ExternResult::Err(error) = result else {
        panic!("expected panic error");
    };
    let error = unsafe { recover_error(error) };
    assert_eq!(error.etype, FFIKernelError::InternalError);
    assert!(error.message.contains(&format!("{}:{line}:", file!())));
    assert!(error.message.contains(expected));
    assert_eq!(catch_panic::<_, KernelError>(|| Ok(42)).unwrap(), 42);
}

#[rstest]
#[case::success(Ok(42))]
#[case::ordinary_error(Err(KernelError::MissingVersion(7)))]
fn preserves_results(#[case] input: KernelResult<usize>) {
    let expected = input.as_ref().copied().map_err(ToString::to_string);
    let result = catch_panic(|| input);
    let result = unsafe { result.into_extern_result(&TEST_ERROR_ALLOCATORS) };
    match expected {
        Ok(value) => assert_eq!(ok_or_panic(result), value),
        Err(message) => assert_error(result, FFIKernelError::MissingVersionError, &message),
    }
}
