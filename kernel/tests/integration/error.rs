use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::io;

use delta_kernel::{Error, KernelError, KernelResult, Result, ResultExt};
use rstest::rstest;

#[rstest]
#[case::without_source(KernelError::file_not_found("missing.parquet"))]
#[case::with_source(KernelError::generic_err(io::Error::other("read failed")))]
#[case::with_backtrace(KernelError::Backtraced {
    source: Box::new(KernelError::file_not_found("missing.parquet")),
    backtrace: Box::new(Backtrace::disabled()),
})]
fn test_error_preserves_kernel_display_and_source(
    #[case] kernel: KernelError,
    #[values(
        Error::Kernel,
        Error::kernel,
        |error| Err::<(), _>(error).into_public_result().unwrap_err()
    )]
    wrap: fn(KernelError) -> Error,
) {
    let expected_display = kernel.to_string();
    let expected_source = kernel.source().map(ToString::to_string);
    let boxed_source_address = |error: &KernelError| match error {
        KernelError::GenericError { source } => {
            Some(std::ptr::from_ref(source.as_ref()).cast::<()>())
        }
        KernelError::Backtraced { source, .. } => {
            Some(std::ptr::from_ref(source.as_ref()).cast::<()>())
        }
        _ => None,
    };
    let expected_boxed_source = boxed_source_address(&kernel);
    let expected_backtrace = match &kernel {
        KernelError::Backtraced { backtrace, .. } => Some(std::ptr::from_ref(backtrace.as_ref())),
        _ => None,
    };
    let error = wrap(kernel);
    assert_eq!(error.to_string(), expected_display);

    let Error::Kernel(kernel) = &error;
    // Trait-object vtables can differ between codegen units; compare the source's data address.
    match (error.source(), kernel.source()) {
        (Some(actual), Some(expected)) => assert!(std::ptr::addr_eq(actual, expected)),
        (None, None) => {}
        _ => panic!("source changed when wrapping the kernel error"),
    }
    if let KernelError::GenericError { .. } = kernel {
        let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(source.to_string(), "read failed");
    }

    let kernel = KernelError::from(error);
    assert_eq!(kernel.to_string(), expected_display);
    assert_eq!(kernel.source().map(ToString::to_string), expected_source);
    assert_eq!(boxed_source_address(&kernel), expected_boxed_source);
    if let Some(expected) = expected_backtrace {
        let KernelError::Backtraced { backtrace, .. } = kernel else {
            panic!("backtrace wrapper was lost");
        };
        assert!(std::ptr::eq(backtrace.as_ref(), expected));
    }
}

#[rstest]
#[case::json_success(serde_json::from_str::<u64>("42"))]
#[case::json_failure(serde_json::from_str::<u64>("invalid"))]
#[case::io_success(Ok::<_, io::Error>(42))]
#[case::io_failure(Err::<u64, _>(io::Error::other("read failed")))]
fn test_error_kernel_maps_foreign_results<E: Into<KernelError> + StdError + 'static>(
    #[case] result: std::result::Result<u64, E>,
) {
    let expected = result.as_ref().copied().map_err(ToString::to_string);
    let mapped: Result<u64> = result.map_err(Error::kernel);
    match (mapped, expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
        (Err(Error::Kernel(error)), Err(message)) => {
            let source: &dyn StdError = match error.without_backtrace() {
                KernelError::MalformedJson(source) => source,
                KernelError::IOError(source) => source,
                other => panic!("unexpected kernel error: {other:?}"),
            };
            let source = source.downcast_ref::<E>().expect("source type changed");
            assert_eq!(source.to_string(), message);
        }
        other => panic!("result changed when mapping the error: {other:?}"),
    }
}

#[test]
fn test_kernel_result_explicitly_maps_into_error() {
    let propagate = |result: KernelResult<u64>| -> Result<u64> {
        let value = result.into_public_result()?;
        Ok(value)
    };

    assert_eq!(propagate(Ok(42)).unwrap(), 42);
    let error = propagate(Err(KernelError::file_not_found("missing.parquet"))).unwrap_err();
    assert!(
        matches!(error, Error::Kernel(KernelError::FileNotFound(path)) if path == "missing.parquet")
    );
}

#[test]
fn test_private_operation_propagates_public_error() {
    fn private_operation(result: Result<u64>) -> KernelResult<u64> {
        Ok(result?)
    }

    assert_eq!(private_operation(Ok(42)).unwrap(), 42);
    let error = Error::Kernel(KernelError::file_not_found("missing.parquet"));
    assert!(matches!(
        private_operation(Err(error)).unwrap_err(),
        KernelError::FileNotFound(path) if path == "missing.parquet"
    ));
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
    assert_error::<Error>();
}
