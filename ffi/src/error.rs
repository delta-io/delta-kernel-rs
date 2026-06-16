use delta_kernel::{DeltaResult, Error};
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

#[repr(C)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum KernelError {
    UnknownError = 0, // catch-all for unrecognized kernel Error types
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
}

impl From<Error> for KernelError {
    fn from(e: Error) -> Self {
        match e {
            // NOTE: By definition, no kernel Error maps to FFIError
            #[cfg(feature = "default-engine-base")]
            Error::Arrow(_) => KernelError::ArrowError,
            Error::CheckpointWrite(_) => KernelError::CheckpointWriteError,
            Error::EngineDataType(_) => KernelError::EngineDataTypeError,
            Error::Extract(..) => KernelError::ExtractError,
            Error::Generic(_) => KernelError::GenericError,
            Error::GenericError { .. } => KernelError::GenericError,
            Error::IOError(_) => KernelError::IOErrorError,
            #[cfg(feature = "default-engine-base")]
            Error::Parquet(_) => KernelError::ParquetError,
            #[cfg(feature = "default-engine-base")]
            Error::ObjectStore(_) => KernelError::ObjectStoreError,
            #[cfg(feature = "default-engine-base")]
            Error::ObjectStorePath(_) => KernelError::ObjectStorePathError,
            #[cfg(feature = "default-engine-base")]
            Error::Reqwest(_) => KernelError::ReqwestError,
            Error::FileNotFound(_) => KernelError::FileNotFoundError,
            Error::MissingColumn(_) => KernelError::MissingColumnError,
            Error::UnexpectedColumnType(_) => KernelError::UnexpectedColumnTypeError,
            Error::MissingData(_) => KernelError::MissingDataError,
            Error::MissingVersion => KernelError::MissingVersionError,
            Error::DeletionVector(_) => KernelError::DeletionVectorError,
            Error::InvalidUrl(_) => KernelError::InvalidUrlError,
            Error::MalformedJson(_) => KernelError::MalformedJsonError,
            Error::MissingMetadata => KernelError::MissingMetadataError,
            Error::MissingProtocol => KernelError::MissingProtocolError,
            Error::InvalidProtocol(_) => KernelError::InvalidProtocolError,
            Error::MissingMetadataAndProtocol => KernelError::MissingMetadataAndProtocolError,
            Error::ParseError(..) => KernelError::ParseError,
            Error::JoinFailure(_) => KernelError::JoinFailureError,
            Error::Utf8Error(_) => KernelError::Utf8Error,
            Error::ParseIntError(_) => KernelError::ParseIntError,
            Error::InvalidColumnMappingMode(_) => KernelError::InvalidColumnMappingModeError,
            Error::InvalidTableLocation(_) => KernelError::InvalidTableLocationError,
            Error::InvalidDecimal(_) => KernelError::InvalidDecimalError,
            Error::InvalidStructData(_) => KernelError::InvalidStructDataError,
            Error::InternalError(_) => KernelError::InternalError,
            Error::Backtraced {
                source,
                backtrace: _,
            } => Self::from(*source),
            Error::InvalidExpressionEvaluation(_) => KernelError::InvalidExpression,
            Error::InvalidLogPath(_) => KernelError::InvalidLogPath,
            Error::FileAlreadyExists(_) => KernelError::FileAlreadyExists,
            Error::Unsupported(_) => KernelError::UnsupportedError,
            Error::ParseIntervalError(_) => KernelError::ParseIntervalError,
            Error::ChangeDataFeedUnsupported(_) => KernelError::ChangeDataFeedUnsupported,
            Error::ChangeDataFeedIncompatibleSchema(_, _) => {
                KernelError::ChangeDataFeedIncompatibleSchema
            }
            Error::InvalidCheckpoint(_) => KernelError::InvalidCheckpoint,
            Error::LiteralExpressionTransformError(_) => {
                KernelError::LiteralExpressionTransformError
            }
            Error::Schema(_) => KernelError::SchemaError,
            _ => KernelError::UnknownError,
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
    pub(crate) etype: KernelError,
}

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible for freeing it.
#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

pub type AllocateErrorFn =
    extern "C" fn(etype: KernelError, msg: KernelStringSlice) -> *mut EngineError;

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
    unsafe fn allocate_error(&self, etype: KernelError, msg: KernelStringSlice)
        -> *mut EngineError;
}

impl AllocateError for AllocateErrorFn {
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
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
        etype: KernelError,
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
    // TODO: we re-use KernelError for convenience, but we should ideally split this into a
    // separate enum, containing only error types that make sense for the engine to return.
    pub etype: KernelError,
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

/// Maps the given KernelError code to the given Error variant. Logs a warning if the associated
/// error message is non-empty. Useful for mapping kernel errors to error variants that don't
/// carry a message, but for some reason the engine still provided one.
fn messageless_error(code: KernelError, message: String, error: Error) -> Error {
    if !message.is_empty() {
        warn!("Discarding message for engine execution error ({code:?}): {message}");
    }
    error
}

impl From<EngineExecError> for Error {
    /// Converts an [`EngineExecError`] into a [`delta_kernel::Error`], translating the
    /// [`KernelError`] code back into its matching kernel error variant and consuming (and thereby
    /// freeing) the message handle.
    fn from(err: EngineExecError) -> Self {
        let EngineExecError { etype, message } = err;
        // SAFETY: `message` is an `ExclusiveRustString` handle that kernel owns and has not yet
        // consumed. It is produced by the engine downcalling `allocate_kernel_string` and is
        // consumed exactly once, here.
        let message = *unsafe { message.into_inner() };
        match etype {
            KernelError::CheckpointWriteError => Error::CheckpointWrite(message),
            KernelError::EngineDataTypeError => Error::EngineDataType(message),
            KernelError::GenericError => Error::Generic(message),
            KernelError::InternalError => Error::InternalError(message),
            KernelError::FileNotFoundError => Error::FileNotFound(message),
            KernelError::MissingColumnError => Error::MissingColumn(message),
            KernelError::UnexpectedColumnTypeError => Error::UnexpectedColumnType(message),
            KernelError::MissingDataError => Error::MissingData(message),
            KernelError::DeletionVectorError => Error::DeletionVector(message),
            KernelError::InvalidProtocolError => Error::InvalidProtocol(message),
            KernelError::JoinFailureError => Error::JoinFailure(message),
            KernelError::InvalidColumnMappingModeError => Error::InvalidColumnMappingMode(message),
            KernelError::InvalidTableLocationError => Error::InvalidTableLocation(message),
            KernelError::InvalidDecimalError => Error::InvalidDecimal(message),
            KernelError::InvalidStructDataError => Error::InvalidStructData(message),
            KernelError::InvalidExpression => Error::InvalidExpressionEvaluation(message),
            KernelError::InvalidLogPath => Error::InvalidLogPath(message),
            KernelError::FileAlreadyExists => Error::FileAlreadyExists(message),
            KernelError::UnsupportedError => Error::Unsupported(message),
            KernelError::InvalidCheckpoint => Error::InvalidCheckpoint(message),
            KernelError::SchemaError => Error::Schema(message),
            code @ KernelError::MissingVersionError => {
                messageless_error(code, message, Error::MissingVersion)
            }
            code @ KernelError::MissingMetadataError => {
                messageless_error(code, message, Error::MissingMetadata)
            }
            code @ KernelError::MissingProtocolError => {
                messageless_error(code, message, Error::MissingProtocol)
            }
            code @ KernelError::MissingMetadataAndProtocolError => {
                messageless_error(code, message, Error::MissingMetadataAndProtocol)
            }

            // These codes have no well-defined equivalent (e.g they wrap a foreign error type,
            // carry a non-string payload, etc), so just map them to a generic error and
            // preserve the code + message in the error string.
            code @ (KernelError::UnknownError
            | KernelError::FFIError
            | KernelError::ExtractError
            | KernelError::IOErrorError
            | KernelError::InvalidUrlError
            | KernelError::MalformedJsonError
            | KernelError::ParseError
            | KernelError::Utf8Error
            | KernelError::ParseIntError
            | KernelError::ParseIntervalError
            | KernelError::ChangeDataFeedUnsupported
            | KernelError::ChangeDataFeedIncompatibleSchema
            | KernelError::LiteralExpressionTransformError) => {
                Error::generic(format!("engine execution error ({code:?}): {message}"))
            }
            #[cfg(feature = "default-engine-base")]
            code @ (KernelError::ArrowError
            | KernelError::ParquetError
            | KernelError::ObjectStoreError
            | KernelError::ObjectStorePathError
            | KernelError::ReqwestError) => {
                Error::generic(format!("engine execution error ({code:?}): {message}"))
            }
        }
    }
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use rstest::rstest;

    use super::*;

    fn exec_error(etype: KernelError, message: &str) -> EngineExecError {
        let message: Handle<ExclusiveRustString> = Box::new(message.to_string()).into();
        EngineExecError { etype, message }
    }

    /// Each code should translate into its matching kernel error variant (preserving the message),
    /// unit variants drop the message, and unmapped codes fall back to a generic error that retains
    /// both the original code and message.
    #[rstest]
    #[case::file_not_found(KernelError::FileNotFoundError, "File not found: boom")]
    #[case::schema(KernelError::SchemaError, "Schema error: boom")]
    #[case::unsupported(KernelError::UnsupportedError, "Unsupported: boom")]
    #[case::generic(KernelError::GenericError, "Generic delta kernel error: boom")]
    #[case::invalid_expr(KernelError::InvalidExpression, "Invalid expression evaluation: boom")]
    #[case::unit_missing_version(KernelError::MissingVersionError, "No table version found.")]
    #[case::fallback_io(
        KernelError::IOErrorError,
        "Generic delta kernel error: engine execution error (IOErrorError): boom"
    )]
    fn engine_exec_error_maps_kernel_error_code(
        #[case] etype: KernelError,
        #[case] expected: &str,
    ) {
        let err: Error = exec_error(etype, "boom").into();
        assert_eq!(err.to_string(), expected);
    }
}
