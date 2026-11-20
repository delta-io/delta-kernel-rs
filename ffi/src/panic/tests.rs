#[cfg(all(unix, not(miri)))]
use std::os::unix::process::ExitStatusExt;
use std::panic::panic_any;
#[cfg(all(unix, not(miri)))]
use std::process::Command;
use std::sync::{Arc, Barrier};

use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;

use super::catch;
use crate::error::{ExternResult, FFIKernelError, IntoExternResult};
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error,
};

#[inline(never)]
fn panic_leaf(panic: fn()) -> KernelResult<()> {
    panic();
    Ok(())
}

#[rstest]
#[case::literal(|| panic!("literal panic"), "literal panic")]
#[case::owned(|| panic_any("owned panic".to_owned()), "owned panic")]
#[case::opaque(|| panic_any(42_u32), "non-string panic payload")]
#[case::capacity(|| drop(Vec::<u8>::with_capacity(usize::MAX)), "capacity overflow")]
fn contains_panics(#[case] panic: fn(), #[case] expected: &str) {
    let allocation = Arc::new(());
    let weak = Arc::downgrade(&allocation);
    let error = catch("probe", || {
        let _allocation = allocation;
        panic_leaf(panic)
    })
    .unwrap_err();
    assert!(weak.upgrade().is_none());
    let message = error.to_string();
    assert!(matches!(error, KernelError::Backtraced { .. }));
    assert!(message.contains("Rust panic in probe at ") && message.contains(expected));
    assert!(!message.contains("unknown location"));
    #[cfg(not(miri))]
    assert!(message.contains("panic_leaf"), "{message}");
    assert_eq!(catch::<_, KernelError>("next call", || Ok(42)).unwrap(), 42);
}

#[test]
#[cfg(all(unix, not(miri)))]
fn infallible_panics_abort() {
    extern "C" fn entry() {
        super::catch_or_abort("entry", || panic!("fatal probe"));
    }
    if std::env::var_os("KERNEL_FFI_FATAL_PANIC_TEST").is_some() {
        entry();
        unreachable!();
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "panic::tests::infallible_panics_abort",
            "--nocapture",
        ])
        .env("KERNEL_FFI_FATAL_PANIC_TEST", "1")
        .output()
        .unwrap();
    assert_eq!(output.status.signal(), Some(6));
    assert!(String::from_utf8_lossy(&output.stderr).contains("fatal probe"));
}

#[rstest]
#[case::success(Ok(42))]
#[case::ordinary_error(Err(KernelError::MissingVersion(7)))]
fn preserves_results(#[case] input: KernelResult<usize>) {
    let expected = input.as_ref().copied().map_err(ToString::to_string);
    assert_eq!(
        super::catch_or_abort("outer", || catch("probe", || input)).map_err(|e| e.to_string()),
        expected
    );
}

#[test]
fn concurrent_panics_keep_separate_captures() {
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        for message in ["first panic", "second panic"] {
            let barrier = &barrier;
            scope.spawn(move || {
                let error = catch::<(), KernelError>("concurrent", || {
                    barrier.wait();
                    panic_any(message);
                })
                .unwrap_err();
                assert!(matches!(error, KernelError::Backtraced { .. }));
                assert!(error.to_string().contains(message));
            });
        }
    });
}

#[test]
fn nested_c_entries_restore_the_outer_capture() {
    unsafe extern "C" fn nested_entry() -> ExternResult<()> {
        let result = catch::<(), KernelError>("inner", || panic!("inner panic"));
        unsafe { result.into_extern_result(&TEST_ERROR_ALLOCATORS) }
    }
    extern "C" fn upcall() {
        assert_error(
            unsafe { nested_entry() },
            FFIKernelError::InternalError,
            "Rust panic in inner",
        );
    }
    let error = catch::<(), KernelError>("outer", || {
        upcall();
        panic!("outer panic");
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("Rust panic in outer") && !error.contains("inner panic"));
}
