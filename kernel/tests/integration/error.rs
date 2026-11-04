use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::io;
use std::num::{IntErrorKind, ParseIntError};

use delta_kernel::{
    DeltaError, DeltaErrorParameter, Error, KernelError, KernelResult, KernelResultIterator, Result,
};
use rstest::rstest;

#[rstest]
#[case::without_source(KernelError::file_not_found("missing.parquet"))]
#[case::with_source(KernelError::generic_err(io::Error::other("read failed")))]
#[case::with_backtrace(KernelError::Backtraced {
    source: Box::new(KernelError::file_not_found("missing.parquet")),
    backtrace: Box::new(Backtrace::disabled()),
})]
fn test_error_preserves_kernel_display_and_source(#[case] kernel: KernelError) {
    let expected_display = kernel.to_string();
    let error = Error::Kernel(kernel);
    assert_eq!(error.to_string(), expected_display);

    let Error::Kernel(kernel) = &error else {
        panic!("expected a kernel error");
    };
    match (error.source(), kernel.source()) {
        (Some(actual), Some(expected)) => assert!(std::ptr::eq(actual, expected)),
        (None, None) => {}
        _ => panic!("source changed when wrapping the kernel error"),
    }
    if let KernelError::GenericError { .. } = kernel {
        let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(source.to_string(), "read failed");
    }
}

#[test]
fn test_kernel_result_explicitly_maps_into_error() {
    let propagate = |result: KernelResult<()>| -> Result<()> {
        result.map_err(Error::Kernel)?;
        Ok(())
    };

    assert!(propagate(Ok(())).is_ok());
    let error = propagate(Err(KernelError::file_not_found("missing.parquet"))).unwrap_err();
    assert!(
        matches!(error, Error::Kernel(KernelError::FileNotFound(path)) if path == "missing.parquet")
    );
}

#[test]
fn test_kernel_result_preserves_custom_error() {
    let result: KernelResult<u64, ParseIntError> = "invalid".parse();
    assert_eq!(result.unwrap_err().kind(), &IntErrorKind::InvalidDigit);
}

#[test]
fn test_kernel_result_iterator_explicitly_maps_into_error() {
    let kernel: Box<dyn KernelResultIterator<()>> = Box::new(std::iter::once(Err(
        KernelError::file_not_found("missing.parquet"),
    )));
    let mut result = kernel.map(|item| item.map_err(Error::Kernel));
    assert!(matches!(
        result.next().unwrap(),
        Err(Error::Kernel(KernelError::FileNotFound(path))) if path == "missing.parquet"
    ));
    assert!(result.next().is_none());
}

#[rstest]
#[case::unwrapped(0)]
#[case::one_wrapper(1)]
#[case::nested_wrappers(2)]
fn test_kernel_error_without_backtrace(#[case] wrapper_count: usize) {
    let mut error = KernelError::file_not_found("missing.parquet");
    for _ in 0..wrapper_count {
        error = KernelError::Backtraced {
            source: Box::new(error),
            backtrace: Box::new(Backtrace::disabled()),
        };
    }

    assert!(
        matches!(error.without_backtrace(), KernelError::FileNotFound(path) if path == "missing.parquet")
    );
}

#[test]
fn test_error_trait_bounds() {
    fn assert_error<T: StdError + Send + Sync + 'static>() {}
    fn assert_parameter<T: Clone + std::fmt::Debug + Eq + Send + Sync + 'static>() {}
    assert_error::<Error>();
    assert_error::<DeltaError>();
    assert_parameter::<DeltaErrorParameter>();
}
