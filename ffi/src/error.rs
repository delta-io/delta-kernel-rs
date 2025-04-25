use delta_kernel::{DeltaResult, Error};

use crate::{kernel_string_slice, ExternEngine, KernelStringSlice};

#[repr(C)]
#[derive(Debug)]
pub enum KernelError {
    UnknownError, // catch-all for unrecognized kernel Error types
    FFIError,     // errors encountered in the code layer that supports FFI
    #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
    ArrowError,
    EngineDataTypeError,
    ExtractError,
    GenericError,
    IOErrorError,
    #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
    ParquetError,
    #[cfg(feature = "default-engine")]
    ObjectStoreError,
    #[cfg(feature = "default-engine")]
    ObjectStorePathError,
    #[cfg(feature = "default-engine")]
    ReqwestError,
    FileNotFoundError,
    MissingColumnError,
    UnexpectedColumnTypeError,
    MissingDataError,
    MissingVersionError,
    DeletionVectorError,
    InvalidUrlError,
    MalformedJsonError,
    MissingMetadataError,
    MissingProtocolError,
    InvalidProtocolError,
    MissingMetadataAndProtocolError,
    ParseError,
    JoinFailureError,
    Utf8Error,
    ParseIntError,
    InvalidColumnMappingModeError,
    InvalidTableLocationError,
    InvalidDecimalError,
    InvalidStructDataError,
    InternalError,
    InvalidExpression,
    InvalidLogPath,
    InvalidCommitInfo,
    FileAlreadyExists,
    MissingCommitInfo,
    UnsupportedError,
    ParseIntervalError,
    ChangeDataFeedUnsupported,
    ChangeDataFeedIncompatibleSchema,
    InvalidCheckpoint,
    LiteralExpressionTransformError,
}

impl From<Error> for KernelError {
    fn from(e: Error) -> Self {
        match e {
            // NOTE: By definition, no kernel Error maps to FFIError
            #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
            Error::Arrow(_) => KernelError::ArrowError,
            Error::EngineDataType(_) => KernelError::EngineDataTypeError,
            Error::Extract(..) => KernelError::ExtractError,
            Error::Generic(_) => KernelError::GenericError,
            Error::GenericError { .. } => KernelError::GenericError,
            Error::IOError(_) => KernelError::IOErrorError,
            #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
            Error::Parquet(_) => KernelError::ParquetError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStore(_) => KernelError::ObjectStoreError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStorePath(_) => KernelError::ObjectStorePathError,
            #[cfg(feature = "default-engine")]
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
            Error::InvalidCommitInfo(_) => KernelError::InvalidCommitInfo,
            Error::FileAlreadyExists(_) => KernelError::FileAlreadyExists,
            Error::MissingCommitInfo => KernelError::MissingCommitInfo,
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
                let msg = format!("{}", err);
                let err = unsafe { alloc.allocate_error(err.into(), kernel_string_slice!(msg)) };
                ExternResult::Err(err)
            }
        }
    }
}
