use std::panic::panic_any;
use std::sync::Arc;

use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;

use super::{catch, catch_panic_into_result};
use crate::error::FFIKernelError;
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error, ok_or_panic,
};

#[rstest]
#[case::literal(|| panic!("literal panic"), "literal panic")]
#[case::owned(|| panic_any("owned panic".to_owned()), "owned panic")]
#[case::opaque(|| panic_any(42_u32), "non-string panic payload")]
#[case::capacity(|| drop(Vec::<u8>::with_capacity(usize::MAX)), "capacity overflow")]
fn contains_panics(#[case] panic: fn(), #[case] expected: &str) {
    let allocation = Arc::new(());
    let weak = Arc::downgrade(&allocation);
    let error = catch::<(), KernelError>("probe", || {
        let _allocation = allocation;
        panic();
        Ok(())
    })
    .unwrap_err();
    assert!(weak.upgrade().is_none());
    let message = error.to_string();
    assert!(matches!(error, KernelError::InternalError(_)));
    assert!(message.contains("Rust panic in probe: ") && message.contains(expected));
    assert_eq!(catch::<_, KernelError>("next call", || Ok(42)).unwrap(), 42);
}

#[rstest]
#[case::success(Ok(42))]
#[case::ordinary_error(Err(KernelError::MissingVersion(7)))]
fn preserves_results(#[case] input: KernelResult<usize>) {
    let expected = input.as_ref().copied().map_err(ToString::to_string);
    let result = unsafe { catch_panic_into_result(&TEST_ERROR_ALLOCATORS, || input) };
    match expected {
        Ok(value) => assert_eq!(ok_or_panic(result), value),
        Err(message) => assert_error(result, FFIKernelError::MissingVersionError, &message),
    }
}
