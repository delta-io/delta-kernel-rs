use delta_kernel::{DeltaResult, Error};

use crate::{kernel_string_slice, ExternEngine, KernelStringSlice};

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
