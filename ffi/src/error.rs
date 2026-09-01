use delta_kernel::{DeltaResult, KernelError};
use tracing::warn;

use crate::handle::Handle;
use crate::{kernel_string_slice, ExclusiveRustString, ExternEngine, KernelStringSlice};

// We explicitly assign integer values to the error codes here because C and Rust are inconsistent
// about values for "typedefed" features. Rust reserves the numbers for them regardless, so
// `EngineDataTypeError` will be `3` whether or not `default-engine-base` is on becasue `ArrowError`
// _always_ is `2`. But in the C header we get:

// #if defined(DEFINE_DEFAULT_ENGINE_BASE)
// ArrowError,
// #endif

// and C will _NOT_ count that if `DEFINE_DEFAULT_ENGINE_BASE` isn't defined, so
// `EngineDataTypeError` will end up as `2`, and everything is confused.  By manually specifying the
// values we avoid this issue.

/// Stable FFI error codes for kernel and FFI-layer failures.
///
/// The explicit discriminants are part of the C ABI and must not be renumbered.
#[repr(C)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum FFIKernelError {
    UnknownError = 0, // catch-all for unrecognized KernelError variants
    FFIError = 1,     // errors encountered in the code layer that supports FFI
    #[cfg(feature = "default-engine-base")]
    ArrowError = 2,
    EngineDataTypeError = 3,
    ExtractError = 4,
    GenericError = 5,
    IOErrorError = 6,
    #[cfg(feature = "default-engine-base")]
    ParquetError = 7,
    #[cfg(feature = "default-engine-base")]
    ObjectStoreError = 8,
    #[cfg(feature = "default-engine-base")]
    ObjectStorePathError = 9,
    #[cfg(feature = "default-engine-base")]
    ReqwestError = 10,
    FileNotFoundError = 11,
    MissingColumnError = 12,
    UnexpectedColumnTypeError = 13,
    MissingDataError = 14,
    MissingVersionError = 15,
    DeletionVectorError = 16,
    InvalidUrlError = 17,
    MalformedJsonError = 18,
    MissingMetadataError = 19,
    MissingProtocolError = 20,
    InvalidProtocolError = 21,
    MissingMetadataAndProtocolError = 22,
    ParseError = 23,
    JoinFailureError = 24,
    Utf8Error = 25,
    ParseIntError = 26,
    InvalidColumnMappingModeError = 27,
    InvalidTableLocationError = 28,
    InvalidDecimalError = 29,
    InvalidStructDataError = 30,
    InternalError = 31,
    InvalidExpression = 32,
    InvalidLogPath = 33,
    FileAlreadyExists = 34,
    UnsupportedError = 35,
    ParseIntervalError = 36,
    ChangeDataFeedUnsupported = 37,
    ChangeDataFeedIncompatibleSchema = 38,
    InvalidCheckpoint = 39,
    LiteralExpressionTransformError = 40,
    CheckpointWriteError = 41,
    SchemaError = 42,
    LogHistoryError = 43,
    RowTrackingChangeFeedUnsupported = 44,
    CancelledError = 45,
    InvalidTransactionStateError = 46,
}

impl From<KernelError> for FFIKernelError {
    fn from(e: KernelError) -> Self {
        match e {
            // NOTE: By definition, no KernelError variant maps to FFIError
            #[cfg(feature = "default-engine-base")]
            KernelError::Arrow(_) => FFIKernelError::ArrowError,
            KernelError::CheckpointWrite(_) => FFIKernelError::CheckpointWriteError,
            KernelError::EngineDataType(_) => FFIKernelError::EngineDataTypeError,
            KernelError::Extract(..) => FFIKernelError::ExtractError,
            KernelError::Generic(_) => FFIKernelError::GenericError,
            KernelError::GenericError { .. } => FFIKernelError::GenericError,
            KernelError::MaxCatalogVersion(_) => FFIKernelError::GenericError,
            KernelError::LogTailVersionsNotContiguous { .. } => FFIKernelError::GenericError,
            KernelError::IOError(_) => FFIKernelError::IOErrorError,
            #[cfg(feature = "default-engine-base")]
            KernelError::Parquet(_) => FFIKernelError::ParquetError,
            #[cfg(feature = "default-engine-base")]
            KernelError::ObjectStore(_) => FFIKernelError::ObjectStoreError,
            #[cfg(feature = "default-engine-base")]
            KernelError::ObjectStorePath(_) => FFIKernelError::ObjectStorePathError,
            #[cfg(feature = "default-engine-base")]
            KernelError::Reqwest(_) => FFIKernelError::ReqwestError,
            KernelError::FileNotFound(_) => FFIKernelError::FileNotFoundError,
            KernelError::MissingColumn(_) => FFIKernelError::MissingColumnError,
            KernelError::UnexpectedColumnType(_) => FFIKernelError::UnexpectedColumnTypeError,
            KernelError::MissingData(_) => FFIKernelError::MissingDataError,
            KernelError::MissingVersion => FFIKernelError::MissingVersionError,
            KernelError::DeletionVector(_) => FFIKernelError::DeletionVectorError,
            KernelError::InvalidUrl(_) => FFIKernelError::InvalidUrlError,
            KernelError::MalformedJson(_) => FFIKernelError::MalformedJsonError,
            KernelError::MissingMetadata => FFIKernelError::MissingMetadataError,
            KernelError::MissingProtocol => FFIKernelError::MissingProtocolError,
            KernelError::InvalidProtocol(_) => FFIKernelError::InvalidProtocolError,
            KernelError::MissingMetadataAndProtocol => {
                FFIKernelError::MissingMetadataAndProtocolError
            }
            KernelError::ParseError(..) => FFIKernelError::ParseError,
            KernelError::JoinFailure(_) => FFIKernelError::JoinFailureError,
            KernelError::Utf8Error(_) => FFIKernelError::Utf8Error,
            KernelError::ParseIntError(_) => FFIKernelError::ParseIntError,
            KernelError::InvalidColumnMappingMode(_) => {
                FFIKernelError::InvalidColumnMappingModeError
            }
            KernelError::InvalidTableLocation(_) => FFIKernelError::InvalidTableLocationError,
            KernelError::InvalidDecimal(_) => FFIKernelError::InvalidDecimalError,
            KernelError::InvalidStructData(_) => FFIKernelError::InvalidStructDataError,
            KernelError::InternalError(_) => FFIKernelError::InternalError,
            KernelError::Backtraced {
                source,
                backtrace: _,
            } => Self::from(*source),
            KernelError::InvalidExpressionEvaluation(_) => FFIKernelError::InvalidExpression,
            KernelError::InvalidLogPath(_) => FFIKernelError::InvalidLogPath,
            KernelError::FileAlreadyExists(_) => FFIKernelError::FileAlreadyExists,
            KernelError::Unsupported(_) => FFIKernelError::UnsupportedError,
            KernelError::ParseIntervalError(_) => FFIKernelError::ParseIntervalError,
            KernelError::ChangeDataFeedUnsupported(_) => FFIKernelError::ChangeDataFeedUnsupported,
            KernelError::RowTrackingChangeFeedUnsupported(_) => {
                FFIKernelError::RowTrackingChangeFeedUnsupported
            }
            KernelError::ChangeDataFeedIncompatibleSchema(_, _) => {
                FFIKernelError::ChangeDataFeedIncompatibleSchema
            }
            KernelError::InvalidCheckpoint(_) => FFIKernelError::InvalidCheckpoint,
            KernelError::LiteralExpressionTransformError(_) => {
                FFIKernelError::LiteralExpressionTransformError
            }
            KernelError::Schema(_) => FFIKernelError::SchemaError,
            KernelError::InvalidTransactionState(_) => FFIKernelError::InvalidTransactionStateError,
            KernelError::LogHistory(_) => FFIKernelError::LogHistoryError,
            KernelError::Cancelled => FFIKernelError::CancelledError,
            _ => FFIKernelError::UnknownError,
        }
    }
}

/// An error that can be returned to the engine. Engines that wish to associate additional
/// information can define and use any type that is [pointer
/// interconvertible](https://en.cppreference.com/w/cpp/language/static_cast#pointer-interconvertible)
/// with this one -- e.g. by subclassing this struct or by embedding this struct as the first member
/// of a [standard layout](https://en.cppreference.com/w/cpp/language/data_members#Standard-layout)
/// class.
#[repr(C)]
pub struct EngineError {
    pub(crate) etype: FFIKernelError,
}

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible for freeing it.
#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

pub type AllocateErrorFn =
    extern "C" fn(etype: FFIKernelError, msg: KernelStringSlice) -> *mut EngineError;

impl<T> ExternResult<T> {
    pub fn is_ok(&self) -> bool {
        match self {
            Self::Ok(_) => true,
            Self::Err(_) => false,
        }
    }
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

/// Represents an engine error allocator. Ultimately all implementations will fall back to an
/// [`AllocateErrorFn`] provided by the engine, but the trait allows us to conveniently access the
/// allocator in various types that may wrap it.
pub trait AllocateError {
    /// Allocates a new error in engine memory and returns the resulting pointer. The engine is
    /// expected to copy the passed-in message, which is only guaranteed to remain valid until the
    /// call returns. Kernel will always immediately return the result of this method to the engine.
    ///
    /// # Safety
    ///
    /// The string slice must be valid until the call returns, and the error allocator must also be
    /// valid.
    unsafe fn allocate_error(
        &self,
        etype: FFIKernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError;
}

impl AllocateError for AllocateErrorFn {
    unsafe fn allocate_error(
        &self,
        etype: FFIKernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        self(etype, msg)
    }
}

// We do this instead of `impl AllocateError for &dyn ExternEngine` since we can then directly use
// this trait on type T instead of having to cast it to a trait object first.
impl<T: ExternEngine + ?Sized> AllocateError for &T {
    /// # Safety
    ///
    /// In addition to the usual requirements, the engine handle must be valid.
    unsafe fn allocate_error(
        &self,
        etype: FFIKernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        self.error_allocator().allocate_error(etype, msg)
    }
}

/// Converts a [DeltaResult] into an [ExternResult], using the engine's error allocator.
///
/// # Safety
///
/// The allocator must be valid.
pub(crate) trait IntoExternResult<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T>;
}

// NOTE: We can't "just" impl From<DeltaResult<T>> because we require an error allocator.
impl<T> IntoExternResult<T> for DeltaResult<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T> {
        match self {
            Ok(ok) => ExternResult::Ok(ok),
            Err(err) => {
                let msg = format!("{err}");
                let err = unsafe { alloc.allocate_error(err.into(), kernel_string_slice!(msg)) };
                ExternResult::Err(err)
            }
        }
    }
}

/// An error that can be returned from engine-side execution (e.g during an upcall).
///
/// This is intended to be a kernel-allocated error which Engines can return TO kernel. It is the
/// inverse of [`EngineError`] (which is engine-allocated, and returned FROM kernel).
///
/// The message is an [`ExclusiveRustString`] handle, which means the engine must
/// downcall to [`allocate_kernel_string`](crate::allocate_kernel_string) to construct it. Kernel
/// can then take ownership and free it appropriately after receiving the error.
#[repr(C)]
pub struct EngineExecError {
    // TODO: we re-use FFIKernelError for convenience, but we should ideally split this into a
    // separate enum, containing only error types that make sense for the engine to return.
    pub etype: FFIKernelError,
    pub message: Handle<ExclusiveRustString>,
}

/// Generic wrapper around an EngineExecError, representing the result of an engine upcall.
///
/// Typically, engines will populate an out pointer with this result type. We include an `Uninit`
/// variant to signal that the engine returned without writing to the out pointer. Kernel should
/// always initialize such an out pointer to `Uninit` before handing it to an engine upcall.
///
/// The variants are deliberately named `Success`/`Failure` rather than `Ok`/`Err` to avoid a
/// conflict with [`ExternResult`]. This is due to an issue in cbindgen, where generic types sharing
/// the same variant names causes failures during monomorphization (<https://github.com/mozilla/cbindgen/issues/1166>).
#[repr(C)]
pub enum EngineExecResult<T> {
    Success(T),
    Failure(EngineExecError),
    Uninit,
}

/// Maps the given FFIKernelError code to the given KernelError variant. Logs a warning if the
/// associated error message is non-empty. Useful for mapping kernel errors to variants that don't
/// carry a message, but for some reason the engine still provided one.
fn messageless_error(code: FFIKernelError, message: String, error: KernelError) -> KernelError {
    if !message.is_empty() {
        warn!("Discarding message for engine execution error ({code:?}): {message}");
    }
    error
}

impl From<EngineExecError> for KernelError {
    /// Converts an [`EngineExecError`] into a [`delta_kernel::KernelError`], translating the
    /// [`FFIKernelError`] code back into its matching kernel error variant and consuming (and
    /// thereby freeing) the message handle.
    fn from(err: EngineExecError) -> Self {
        let EngineExecError { etype, message } = err;
        // SAFETY: `message` is an `ExclusiveRustString` handle that kernel owns and has not yet
        // consumed. It is produced by the engine downcalling `allocate_kernel_string` and is
        // consumed exactly once, here.
        let message = *unsafe { message.into_inner() };
        match etype {
            FFIKernelError::CheckpointWriteError => KernelError::CheckpointWrite(message),
            FFIKernelError::EngineDataTypeError => KernelError::EngineDataType(message),
            FFIKernelError::GenericError => KernelError::Generic(message),
            FFIKernelError::InternalError => KernelError::InternalError(message),
            FFIKernelError::FileNotFoundError => KernelError::FileNotFound(message),
            FFIKernelError::MissingColumnError => KernelError::MissingColumn(message),
            FFIKernelError::UnexpectedColumnTypeError => KernelError::UnexpectedColumnType(message),
            FFIKernelError::MissingDataError => KernelError::MissingData(message),
            FFIKernelError::DeletionVectorError => KernelError::DeletionVector(message),
            FFIKernelError::InvalidProtocolError => KernelError::InvalidProtocol(message),
            FFIKernelError::JoinFailureError => KernelError::JoinFailure(message),
            FFIKernelError::InvalidColumnMappingModeError => {
                KernelError::InvalidColumnMappingMode(message)
            }
            FFIKernelError::InvalidTableLocationError => KernelError::InvalidTableLocation(message),
            FFIKernelError::InvalidDecimalError => KernelError::InvalidDecimal(message),
            FFIKernelError::InvalidStructDataError => KernelError::InvalidStructData(message),
            FFIKernelError::InvalidExpression => KernelError::InvalidExpressionEvaluation(message),
            FFIKernelError::InvalidLogPath => KernelError::InvalidLogPath(message),
            FFIKernelError::FileAlreadyExists => KernelError::FileAlreadyExists(message),
            FFIKernelError::UnsupportedError => KernelError::Unsupported(message),
            FFIKernelError::InvalidCheckpoint => KernelError::InvalidCheckpoint(message),
            FFIKernelError::SchemaError => KernelError::Schema(message),
            FFIKernelError::InvalidTransactionStateError => {
                KernelError::InvalidTransactionState(message)
            }
            code @ FFIKernelError::MissingVersionError => {
                messageless_error(code, message, KernelError::MissingVersion)
            }
            code @ FFIKernelError::MissingMetadataError => {
                messageless_error(code, message, KernelError::MissingMetadata)
            }
            code @ FFIKernelError::MissingProtocolError => {
                messageless_error(code, message, KernelError::MissingProtocol)
            }
            code @ FFIKernelError::MissingMetadataAndProtocolError => {
                messageless_error(code, message, KernelError::MissingMetadataAndProtocol)
            }
            code @ FFIKernelError::CancelledError => {
                messageless_error(code, message, KernelError::Cancelled)
            }

            // These codes have no well-defined equivalent (e.g they wrap a foreign error type,
            // carry a non-string payload, etc), so just map them to a generic error and
            // preserve the code + message in the error string.
            code @ (FFIKernelError::UnknownError
            | FFIKernelError::FFIError
            | FFIKernelError::ExtractError
            | FFIKernelError::IOErrorError
            | FFIKernelError::InvalidUrlError
            | FFIKernelError::MalformedJsonError
            | FFIKernelError::ParseError
            | FFIKernelError::Utf8Error
            | FFIKernelError::ParseIntError
            | FFIKernelError::ParseIntervalError
            | FFIKernelError::ChangeDataFeedUnsupported
            | FFIKernelError::ChangeDataFeedIncompatibleSchema
            | FFIKernelError::RowTrackingChangeFeedUnsupported
            | FFIKernelError::LiteralExpressionTransformError
            | FFIKernelError::LogHistoryError) => {
                KernelError::generic(format!("engine execution error ({code:?}): {message}"))
            }
            #[cfg(feature = "default-engine-base")]
            code @ (FFIKernelError::ArrowError
            | FFIKernelError::ParquetError
            | FFIKernelError::ObjectStoreError
            | FFIKernelError::ObjectStorePathError
            | FFIKernelError::ReqwestError) => {
                KernelError::generic(format!("engine execution error ({code:?}): {message}"))
            }
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn row_tracking_change_feed_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::RowTrackingChangeFeedUnsupported(7)),
            FFIKernelError::RowTrackingChangeFeedUnsupported
        );
        assert_eq!(FFIKernelError::RowTrackingChangeFeedUnsupported as i32, 44);
    }
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use rstest::rstest;

    use super::*;

    fn exec_error(etype: FFIKernelError, message: &str) -> EngineExecError {
        let message: Handle<ExclusiveRustString> = Box::new(message.to_string()).into();
        EngineExecError { etype, message }
    }

    /// Each code should translate into its matching kernel error variant (preserving the message),
    /// unit variants drop the message, and unmapped codes fall back to a generic error that retains
    /// both the original code and message.
    #[rstest]
    #[case::file_not_found(FFIKernelError::FileNotFoundError, "File not found: boom")]
    #[case::schema(FFIKernelError::SchemaError, "Schema error: boom")]
    #[case::unsupported(FFIKernelError::UnsupportedError, "Unsupported: boom")]
    #[case::generic(FFIKernelError::GenericError, "Generic delta kernel error: boom")]
    #[case::invalid_expr(
        FFIKernelError::InvalidExpression,
        "Invalid expression evaluation: boom"
    )]
    #[case::unit_missing_version(FFIKernelError::MissingVersionError, "No table version found.")]
    #[case::fallback_io(
        FFIKernelError::IOErrorError,
        "Generic delta kernel error: engine execution error (IOErrorError): boom"
    )]
    #[case::fallback_row_tracking(
        FFIKernelError::RowTrackingChangeFeedUnsupported,
        "Generic delta kernel error: engine execution error (RowTrackingChangeFeedUnsupported): boom"
    )]
    fn engine_exec_error_maps_kernel_error_code(
        #[case] etype: FFIKernelError,
        #[case] expected: &str,
    ) {
        let err: KernelError = exec_error(etype, "boom").into();
        assert_eq!(err.to_string(), expected);
    }
}
