use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::io;

use delta_kernel::schema::PrimitiveType;
use delta_kernel::{Error, KernelError, KernelResult, Result};
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
fn test_error_trait_bounds() {
    fn assert_error<T: StdError + Send + Sync + 'static>() {}
    assert_error::<Error>();
}

#[test]
fn test_public_scalar_parser_returns_error_envelope() {
    let result: Result<_> = PrimitiveType::Integer.parse_scalar("not an integer");
    assert!(matches!(
        result,
        Err(Error::Kernel(KernelError::ParseError(..)))
    ));
}
