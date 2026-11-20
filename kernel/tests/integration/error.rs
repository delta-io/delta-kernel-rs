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
#[case::with_context(
    KernelError::file_not_found("missing.parquet").wrap_with_context("Reading checkpoint")
)]
#[case::with_backtrace(KernelError::Backtraced {
    source: Box::new(KernelError::file_not_found("missing.parquet")),
    backtrace: Box::new(Backtrace::disabled()),
})]
fn test_error_preserves_kernel_display_and_source(#[case] kernel: KernelError) {
    let expected_display = kernel.to_string();
    let error = Error::from(kernel);
    assert_eq!(error.to_string(), expected_display);

    let Error::Kernel(kernel) = &error else {
        panic!("expected a kernel error");
    };
    match (error.source(), kernel.source()) {
        (Some(actual), Some(expected)) => assert!(std::ptr::eq(actual, expected)),
        (None, None) => {}
        _ => panic!("source changed when wrapping the kernel error"),
    }
}

#[test]
fn test_question_mark_converts_kernel_result_into_error() {
    let propagate = |result: KernelResult<()>| -> Result<()> {
        result?;
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

#[test]
fn test_wrap_with_context_preserves_nested_diagnostics_and_original_cause() {
    let error = KernelError::file_not_found("part-7.parquet")
        .wrap_with_context("Reading checkpoint manifest")
        .wrap_with_context("Planning table 'orders' at snapshot 42");

    let message = error.to_string();
    assert_eq!(
        message.split("\n\nStack backtrace:").next().unwrap(),
        "Generic error: Planning table 'orders' at snapshot 42\n\n\
         Caused by:\n    0: Reading checkpoint manifest\n    1: File not found: part-7.parquet",
    );
    let cause = std::iter::successors(error.source(), |error| (*error).source())
        .find_map(|error| error.downcast_ref::<KernelError>())
        .unwrap();
    assert!(matches!(cause, KernelError::FileNotFound(path) if path == "part-7.parquet"));
}

#[test]
fn test_generic_err_preserves_default_message() {
    let error = KernelError::generic_err(io::Error::other("read failed"));
    assert_eq!(error.to_string(), "Generic error: read failed");
    assert!(error.source().unwrap().is::<io::Error>());
}

#[rstest]
fn test_wrap_with_context_preserves_cancellation(#[values(false, true)] backtraced: bool) {
    let error = if backtraced {
        KernelError::Backtraced {
            source: Box::new(KernelError::Cancelled),
            backtrace: Box::new(Backtrace::disabled()),
        }
    } else {
        KernelError::Cancelled
    };
    let original_message = error.to_string();
    let source_address = |error: &KernelError| match error {
        KernelError::Backtraced { source, .. } => Some(std::ptr::from_ref(source.as_ref())),
        _ => None,
    };
    let original_source = source_address(&error);

    let error = error.wrap_with_context("Planning scan");

    assert!(matches!(error.without_backtrace(), KernelError::Cancelled));
    assert_eq!(error.to_string(), original_message);
    assert_eq!(source_address(&error), original_source);
}

#[test]
fn test_wrap_with_context_preserves_backtrace_and_typed_io_cause() {
    let backtrace = Box::new(Backtrace::disabled());
    let original_backtrace = std::ptr::from_ref(backtrace.as_ref());
    let error = KernelError::Backtraced {
        source: Box::new(KernelError::IOError(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "access denied",
        ))),
        backtrace,
    }
    .wrap_with_context("Reading checkpoint manifest")
    .wrap_with_context("Planning scan");

    let KernelError::Backtraced { backtrace, .. } = &error else {
        panic!("expected original backtrace");
    };
    assert_eq!(std::ptr::from_ref(backtrace.as_ref()), original_backtrace);
    let cause = std::iter::successors(error.source(), |error| (*error).source())
        .find_map(|error| match error.downcast_ref::<KernelError>() {
            Some(KernelError::IOError(source)) => Some(source),
            _ => None,
        })
        .unwrap();
    assert_eq!(cause.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string().matches("access denied").count(), 1);
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
