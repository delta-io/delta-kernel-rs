use delta_kernel::{DeltaResult, EngineError as RustEngineError, Error, KernelError};

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
    /// Legacy inbound callback code. Kernel never emits this code.
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
    DeltaError = 47,
    IntegerConversionError = 48,
    NumericOverflowError = 49,
    JsonSerializationError = 50,
    ExpressionConversionError = 51,
    LockPoisonedError = 52,
    CatalogOperationError = 53,
    ClockBeforeEpochError = 54,
    RuntimeUnavailableError = 55,
    RuntimePanicError = 56,
    WrittenFileSizeMismatchError = 57,
    ForeignCallbackError = 58,
    FfiContractError = 59,
    TracingFailureError = 60,
    TracingSlotMissingError = 61,
    MaxCatalogVersionError = 62,
    LogTailVersionsNotContiguousError = 63,
    ScalarConversionError = 64,
    InvalidPartitionValuesError = 65,
    InvalidSelectionVectorError = 66,
    InvalidGeoParamsError = 67,
    StatsValidationError = 68,
    ChecksumWriteUnsupportedError = 69,
    PlanResultTypeMismatchError = 70,
    ProtobufDecodeError = 71,
    LogSegmentError = 72,
    ScanError = 73,
    CommitRangeError = 74,
    SnapshotError = 75,
    TableChangesError = 76,
    PublishError = 77,
    TablePropertyError = 78,
    CrcError = 79,
    LogCompactionError = 80,
    ArrowEngineError = 81,
    PlanError = 82,
    CorruptFileError = 83,
    ExternalEngineError = 84,
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
            KernelError::Context { source, .. } => Self::from(*source),
            KernelError::IntegerConversion { .. } => Self::IntegerConversionError,
            KernelError::NumericOverflow { .. } => Self::NumericOverflowError,
            KernelError::JsonSerialization { .. } => Self::JsonSerializationError,
            KernelError::ExpressionConversion { .. } => Self::ExpressionConversionError,
            KernelError::LockPoisoned { .. } => Self::LockPoisonedError,
            KernelError::CatalogOperation { .. } => Self::CatalogOperationError,
            KernelError::ClockBeforeEpoch(_) => Self::ClockBeforeEpochError,
            KernelError::RuntimeUnavailable { .. } => Self::RuntimeUnavailableError,
            KernelError::RuntimePanic { .. } => Self::RuntimePanicError,
            KernelError::WrittenFileSizeMismatch { .. } => Self::WrittenFileSizeMismatchError,
            KernelError::ForeignCallback { .. } => Self::ForeignCallbackError,
            KernelError::FfiContract(_) => Self::FfiContractError,
            KernelError::TracingFailure { .. } => Self::TracingFailureError,
            KernelError::TracingSlotMissing { .. } => Self::TracingSlotMissingError,
            KernelError::MaxCatalogVersion(_) => Self::MaxCatalogVersionError,
            KernelError::LogTailVersionsNotContiguous { .. } => {
                Self::LogTailVersionsNotContiguousError
            }
            KernelError::ScalarConversion(_) => Self::ScalarConversionError,
            KernelError::InvalidPartitionValues(_) => Self::InvalidPartitionValuesError,
            KernelError::InvalidSelectionVector(_) => Self::InvalidSelectionVectorError,
            KernelError::InvalidGeoParams(_) => Self::InvalidGeoParamsError,
            KernelError::StatsValidation(_) => Self::StatsValidationError,
            KernelError::ChecksumWriteUnsupported(_) => Self::ChecksumWriteUnsupportedError,
            #[cfg(feature = "declarative-plans")]
            KernelError::PlanResultTypeMismatch { .. } => Self::PlanResultTypeMismatchError,
            #[cfg(feature = "declarative-plans")]
            KernelError::ProtobufDecode { .. } => Self::ProtobufDecodeError,
            KernelError::LogSegment(_) => Self::LogSegmentError,
            KernelError::Scan(_) => Self::ScanError,
            KernelError::CommitRange(_) => Self::CommitRangeError,
            KernelError::Snapshot(_) => Self::SnapshotError,
            KernelError::TableChanges(_) => Self::TableChangesError,
            KernelError::Publish(_) => Self::PublishError,
            KernelError::TableProperty(_) => Self::TablePropertyError,
            KernelError::Crc(_) => Self::CrcError,
            KernelError::LogCompaction(_) => Self::LogCompactionError,
            #[cfg(feature = "default-engine-base")]
            KernelError::ArrowEngine(_) => Self::ArrowEngineError,
            #[cfg(feature = "declarative-plans")]
            KernelError::Plan(_) => Self::PlanError,
            _ => FFIKernelError::UnknownError,
        }
    }
}

impl From<Error> for FFIKernelError {
    fn from(error: Error) -> Self {
        match error {
            Error::Kernel(error) => error.into(),
            Error::Delta(_) => Self::DeltaError,
            Error::Engine(error) => error.into(),
        }
    }
}

impl From<RustEngineError> for FFIKernelError {
    fn from(error: RustEngineError) -> Self {
        match error {
            RustEngineError::FileNotFound { .. } => Self::FileNotFoundError,
            RustEngineError::FileAlreadyExists { .. } => Self::FileAlreadyExists,
            RustEngineError::CorruptFile { .. } => Self::CorruptFileError,
            RustEngineError::Cancelled => Self::CancelledError,
            RustEngineError::ParseError { .. } => Self::ParseError,
            RustEngineError::External { .. } => Self::ExternalEngineError,
            _ => Self::UnknownError,
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

/// Retains a recognized payload-free error and any diagnostic supplied by the callback.
fn messageless_error(code: FFIKernelError, message: String, error: KernelError) -> KernelError {
    if message.is_empty() {
        error
    } else {
        error.with_context(delta_kernel::error::ErrorContext::ForeignCallback {
            code: code as i32,
            message,
        })
    }
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
            FFIKernelError::GenericError => KernelError::ForeignCallback { code: 5, message },
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

            // Typed payloads cannot be reconstructed from the callback wire message.
            code @ (FFIKernelError::LogSegmentError
            | FFIKernelError::ScanError
            | FFIKernelError::CommitRangeError
            | FFIKernelError::SnapshotError
            | FFIKernelError::TableChangesError
            | FFIKernelError::PublishError
            | FFIKernelError::TablePropertyError
            | FFIKernelError::CrcError
            | FFIKernelError::LogCompactionError
            | FFIKernelError::ArrowEngineError
            | FFIKernelError::PlanError
            | FFIKernelError::CorruptFileError
            | FFIKernelError::ExternalEngineError
            | FFIKernelError::IntegerConversionError
            | FFIKernelError::NumericOverflowError
            | FFIKernelError::JsonSerializationError
            | FFIKernelError::ExpressionConversionError
            | FFIKernelError::LockPoisonedError
            | FFIKernelError::CatalogOperationError
            | FFIKernelError::ClockBeforeEpochError
            | FFIKernelError::RuntimeUnavailableError
            | FFIKernelError::RuntimePanicError
            | FFIKernelError::WrittenFileSizeMismatchError
            | FFIKernelError::ForeignCallbackError
            | FFIKernelError::FfiContractError
            | FFIKernelError::TracingFailureError
            | FFIKernelError::TracingSlotMissingError
            | FFIKernelError::MaxCatalogVersionError
            | FFIKernelError::LogTailVersionsNotContiguousError
            | FFIKernelError::ScalarConversionError
            | FFIKernelError::InvalidPartitionValuesError
            | FFIKernelError::InvalidSelectionVectorError
            | FFIKernelError::InvalidGeoParamsError
            | FFIKernelError::StatsValidationError
            | FFIKernelError::ChecksumWriteUnsupportedError
            | FFIKernelError::PlanResultTypeMismatchError
            | FFIKernelError::ProtobufDecodeError
            | FFIKernelError::UnknownError
            | FFIKernelError::FFIError
            | FFIKernelError::DeltaError
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
            | FFIKernelError::LogHistoryError) => KernelError::ForeignCallback {
                code: code as i32,
                message,
            },
            #[cfg(feature = "default-engine-base")]
            code @ (FFIKernelError::ArrowError
            | FFIKernelError::ParquetError
            | FFIKernelError::ObjectStoreError
            | FFIKernelError::ObjectStorePathError
            | FFIKernelError::ReqwestError) => KernelError::ForeignCallback {
                code: code as i32,
                message,
            },
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use std::backtrace::Backtrace;

    use delta_kernel::error::{
        CommitRangeError, CrcError, ErrorContext, FfiContractError, LogCompactionError,
        LogSegmentError, PublishError, ScanError, SnapshotError, TableChangesError,
        TablePropertyError,
    };

    use super::*;

    #[test]
    fn row_tracking_change_feed_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::RowTrackingChangeFeedUnsupported(7)),
            FFIKernelError::RowTrackingChangeFeedUnsupported
        );
        assert_eq!(FFIKernelError::RowTrackingChangeFeedUnsupported as i32, 44);
    }

    #[test]
    fn all_ffi_discriminants_are_stable() {
        assert_eq!(FFIKernelError::UnknownError as i32, 0);
        assert_eq!(FFIKernelError::FFIError as i32, 1);
        #[cfg(feature = "default-engine-base")]
        assert_eq!(FFIKernelError::ArrowError as i32, 2);
        assert_eq!(FFIKernelError::EngineDataTypeError as i32, 3);
        assert_eq!(FFIKernelError::ExtractError as i32, 4);
        assert_eq!(FFIKernelError::GenericError as i32, 5);
        assert_eq!(FFIKernelError::IOErrorError as i32, 6);
        #[cfg(feature = "default-engine-base")]
        assert_eq!(FFIKernelError::ParquetError as i32, 7);
        #[cfg(feature = "default-engine-base")]
        assert_eq!(FFIKernelError::ObjectStoreError as i32, 8);
        #[cfg(feature = "default-engine-base")]
        assert_eq!(FFIKernelError::ObjectStorePathError as i32, 9);
        #[cfg(feature = "default-engine-base")]
        assert_eq!(FFIKernelError::ReqwestError as i32, 10);
        assert_eq!(FFIKernelError::FileNotFoundError as i32, 11);
        assert_eq!(FFIKernelError::MissingColumnError as i32, 12);
        assert_eq!(FFIKernelError::UnexpectedColumnTypeError as i32, 13);
        assert_eq!(FFIKernelError::MissingDataError as i32, 14);
        assert_eq!(FFIKernelError::MissingVersionError as i32, 15);
        assert_eq!(FFIKernelError::DeletionVectorError as i32, 16);
        assert_eq!(FFIKernelError::InvalidUrlError as i32, 17);
        assert_eq!(FFIKernelError::MalformedJsonError as i32, 18);
        assert_eq!(FFIKernelError::MissingMetadataError as i32, 19);
        assert_eq!(FFIKernelError::MissingProtocolError as i32, 20);
        assert_eq!(FFIKernelError::InvalidProtocolError as i32, 21);
        assert_eq!(FFIKernelError::MissingMetadataAndProtocolError as i32, 22);
        assert_eq!(FFIKernelError::ParseError as i32, 23);
        assert_eq!(FFIKernelError::JoinFailureError as i32, 24);
        assert_eq!(FFIKernelError::Utf8Error as i32, 25);
        assert_eq!(FFIKernelError::ParseIntError as i32, 26);
        assert_eq!(FFIKernelError::InvalidColumnMappingModeError as i32, 27);
        assert_eq!(FFIKernelError::InvalidTableLocationError as i32, 28);
        assert_eq!(FFIKernelError::InvalidDecimalError as i32, 29);
        assert_eq!(FFIKernelError::InvalidStructDataError as i32, 30);
        assert_eq!(FFIKernelError::InternalError as i32, 31);
        assert_eq!(FFIKernelError::InvalidExpression as i32, 32);
        assert_eq!(FFIKernelError::InvalidLogPath as i32, 33);
        assert_eq!(FFIKernelError::FileAlreadyExists as i32, 34);
        assert_eq!(FFIKernelError::UnsupportedError as i32, 35);
        assert_eq!(FFIKernelError::ParseIntervalError as i32, 36);
        assert_eq!(FFIKernelError::ChangeDataFeedUnsupported as i32, 37);
        assert_eq!(FFIKernelError::ChangeDataFeedIncompatibleSchema as i32, 38);
        assert_eq!(FFIKernelError::InvalidCheckpoint as i32, 39);
        assert_eq!(FFIKernelError::LiteralExpressionTransformError as i32, 40);
        assert_eq!(FFIKernelError::CheckpointWriteError as i32, 41);
        assert_eq!(FFIKernelError::SchemaError as i32, 42);
        assert_eq!(FFIKernelError::LogHistoryError as i32, 43);
        assert_eq!(FFIKernelError::RowTrackingChangeFeedUnsupported as i32, 44);
        assert_eq!(FFIKernelError::CancelledError as i32, 45);
        assert_eq!(FFIKernelError::InvalidTransactionStateError as i32, 46);
        assert_eq!(FFIKernelError::DeltaError as i32, 47);
        assert_eq!(FFIKernelError::IntegerConversionError as i32, 48);
        assert_eq!(FFIKernelError::NumericOverflowError as i32, 49);
        assert_eq!(FFIKernelError::JsonSerializationError as i32, 50);
        assert_eq!(FFIKernelError::ExpressionConversionError as i32, 51);
        assert_eq!(FFIKernelError::LockPoisonedError as i32, 52);
        assert_eq!(FFIKernelError::CatalogOperationError as i32, 53);
        assert_eq!(FFIKernelError::ClockBeforeEpochError as i32, 54);
        assert_eq!(FFIKernelError::RuntimeUnavailableError as i32, 55);
        assert_eq!(FFIKernelError::RuntimePanicError as i32, 56);
        assert_eq!(FFIKernelError::WrittenFileSizeMismatchError as i32, 57);
        assert_eq!(FFIKernelError::ForeignCallbackError as i32, 58);
        assert_eq!(FFIKernelError::FfiContractError as i32, 59);
        assert_eq!(FFIKernelError::TracingFailureError as i32, 60);
        assert_eq!(FFIKernelError::TracingSlotMissingError as i32, 61);
        assert_eq!(FFIKernelError::MaxCatalogVersionError as i32, 62);
        assert_eq!(FFIKernelError::LogTailVersionsNotContiguousError as i32, 63);
        assert_eq!(FFIKernelError::ScalarConversionError as i32, 64);
        assert_eq!(FFIKernelError::InvalidPartitionValuesError as i32, 65);
        assert_eq!(FFIKernelError::InvalidSelectionVectorError as i32, 66);
        assert_eq!(FFIKernelError::InvalidGeoParamsError as i32, 67);
        assert_eq!(FFIKernelError::StatsValidationError as i32, 68);
        assert_eq!(FFIKernelError::ChecksumWriteUnsupportedError as i32, 69);
        assert_eq!(FFIKernelError::PlanResultTypeMismatchError as i32, 70);
        assert_eq!(FFIKernelError::ProtobufDecodeError as i32, 71);
        assert_eq!(FFIKernelError::LogSegmentError as i32, 72);
        assert_eq!(FFIKernelError::ScanError as i32, 73);
        assert_eq!(FFIKernelError::CommitRangeError as i32, 74);
        assert_eq!(FFIKernelError::SnapshotError as i32, 75);
        assert_eq!(FFIKernelError::TableChangesError as i32, 76);
        assert_eq!(FFIKernelError::PublishError as i32, 77);
        assert_eq!(FFIKernelError::TablePropertyError as i32, 78);
        assert_eq!(FFIKernelError::CrcError as i32, 79);
        assert_eq!(FFIKernelError::LogCompactionError as i32, 80);
        assert_eq!(FFIKernelError::ArrowEngineError as i32, 81);
        assert_eq!(FFIKernelError::PlanError as i32, 82);
        assert_eq!(FFIKernelError::CorruptFileError as i32, 83);
        assert_eq!(FFIKernelError::ExternalEngineError as i32, 84);
    }

    #[test]
    fn appended_error_codes_have_stable_discriminants() {
        assert_eq!(FFIKernelError::InvalidTransactionStateError as i32, 46);
        assert_eq!(FFIKernelError::DeltaError as i32, 47);
    }

    #[rstest::rstest]
    #[case(
        RustEngineError::file_not_found("missing"),
        FFIKernelError::FileNotFoundError
    )]
    #[case(
        RustEngineError::file_already_exists("existing"),
        FFIKernelError::FileAlreadyExists
    )]
    #[case(
        RustEngineError::CorruptFile { path: "corrupt".into(), source: None },
        FFIKernelError::CorruptFileError
    )]
    #[case(RustEngineError::Cancelled, FFIKernelError::CancelledError)]
    #[case(
        RustEngineError::ParseError {
            value: "bad".into(),
            data_type: delta_kernel::schema::DataType::INTEGER,
            source: None,
        },
        FFIKernelError::ParseError
    )]
    #[case(
        RustEngineError::external(std::io::Error::other("native failure")),
        FFIKernelError::ExternalEngineError
    )]
    fn engine_errors_have_explicit_ffi_codes(
        #[case] error: RustEngineError,
        #[case] expected: FFIKernelError,
    ) {
        assert_eq!(FFIKernelError::from(Error::Engine(error)), expected);
    }

    #[test]
    fn public_error_categories_are_exhaustive() {
        let error = Error::from(RustEngineError::Cancelled);
        let category = match error {
            Error::Delta(_) => "delta",
            Error::Engine(_) => "engine",
            Error::Kernel(_) => "kernel",
        };
        assert_eq!(category, "engine");
    }

    #[test]
    fn inbound_delta_error_code_preserves_foreign_callback() {
        let message: Handle<ExclusiveRustString> =
            Box::new("engine delta error".to_string()).into();
        let error: KernelError = EngineExecError {
            etype: FFIKernelError::DeltaError,
            message,
        }
        .into();

        assert_eq!(
            error.to_string(),
            "Foreign callback error (47): engine delta error"
        );
        assert!(matches!(
            error,
            KernelError::ForeignCallback { code: 47, .. }
        ));
    }

    #[rstest::rstest]
    #[case(KernelError::NumericOverflow { operation: "add", value: "u64::MAX + 1".into() }, FFIKernelError::NumericOverflowError)]
    #[case(KernelError::LockPoisoned { resource: "scan" }, FFIKernelError::LockPoisonedError)]
    #[case(KernelError::WrittenFileSizeMismatch { expected: 5, actual: 4 }, FFIKernelError::WrittenFileSizeMismatchError)]
    #[case(KernelError::ForeignCallback { code: 5, message: "failure".into() }, FFIKernelError::ForeignCallbackError)]
    #[case(
        KernelError::FfiContract(FfiContractError::StreamConsumed),
        FFIKernelError::FfiContractError
    )]
    #[case(KernelError::TracingSlotMissing { slot: "logging" }, FFIKernelError::TracingSlotMissingError)]
    #[case(KernelError::MaxCatalogVersion("missing".into()), FFIKernelError::MaxCatalogVersionError)]
    #[case(KernelError::LogTailVersionsNotContiguous { first_version: 1, second_version: 3 }, FFIKernelError::LogTailVersionsNotContiguousError)]
    #[case(KernelError::InvalidPartitionValues("missing column".into()), FFIKernelError::InvalidPartitionValuesError)]
    #[case(KernelError::InvalidSelectionVector("too long".into()), FFIKernelError::InvalidSelectionVectorError)]
    #[case(KernelError::InvalidGeoParams("CRS".into()), FFIKernelError::InvalidGeoParamsError)]
    #[case(KernelError::StatsValidation("missing min".into()), FFIKernelError::StatsValidationError)]
    #[case(KernelError::ChecksumWriteUnsupported("not published".into()), FFIKernelError::ChecksumWriteUnsupportedError)]
    #[case::integer_conversion(
        KernelError::integer_conversion("version", -1, "u64", u64::try_from(-1i64).unwrap_err()),
        FFIKernelError::IntegerConversionError
    )]
    #[case::json_serialization(
        KernelError::JsonSerialization {
            operation: "serialize metadata",
            source: serde_json::to_string(&std::collections::BTreeMap::from([(vec![1], true)])).unwrap_err(),
        },
        FFIKernelError::JsonSerializationError
    )]
    #[case::expression_conversion(
        KernelError::ExpressionConversion {
            operation: "parse integer literal",
            source: Box::new("invalid".parse::<i64>().unwrap_err()),
        },
        FFIKernelError::ExpressionConversionError
    )]
    #[case::catalog_operation(
        KernelError::CatalogOperation {
            operation: "publish",
            source: Box::new(std::io::Error::other("catalog unavailable")),
        },
        FFIKernelError::CatalogOperationError
    )]
    #[case::clock_before_epoch(
        KernelError::ClockBeforeEpoch(std::time::UNIX_EPOCH.duration_since(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1)
        ).unwrap_err()),
        FFIKernelError::ClockBeforeEpochError
    )]
    #[case::runtime_unavailable(
        KernelError::RuntimeUnavailable { source: Box::new(tokio::runtime::Handle::try_current().unwrap_err()) },
        FFIKernelError::RuntimeUnavailableError
    )]
    #[case::runtime_panic(
        KernelError::RuntimePanic { operation: "block_on", message: Some("runtime stopped".into()) },
        FFIKernelError::RuntimePanicError
    )]
    #[case::tracing_failure(
        KernelError::TracingFailure { operation: "install subscriber", source: Box::new(std::io::Error::other("subscriber unavailable")) },
        FFIKernelError::TracingFailureError
    )]
    #[case::log_segment(
        KernelError::LogSegment(LogSegmentError::Empty),
        FFIKernelError::LogSegmentError
    )]
    #[case::scan(
        KernelError::Scan(ScanError::IncompleteReplay),
        FFIKernelError::ScanError
    )]
    #[case::commit_range(KernelError::CommitRange(CommitRangeError::Reversed { start: 2, end: 1 }), FFIKernelError::CommitRangeError)]
    #[case::snapshot(KernelError::Snapshot(SnapshotError::VersionBeforeHint { requested: 1, hint: 2 }), FFIKernelError::SnapshotError)]
    #[case::table_changes(
        KernelError::TableChanges(TableChangesError::RemoveFileWithRemoveVector),
        FFIKernelError::TableChangesError
    )]
    #[case::publish(KernelError::Publish(PublishError::Empty { version: 3 }), FFIKernelError::PublishError)]
    #[case::table_property(
        KernelError::TableProperty(TablePropertyError::InvalidValue {
            property: "delta.checkpointInterval".into(), value: "-1".into(), expected: "positive integer",
        }),
        FFIKernelError::TablePropertyError
    )]
    #[case::crc(KernelError::Crc(CrcError::InvalidActionCount { version: 3, field: "numMetadata", actual: 0 }), FFIKernelError::CrcError)]
    #[case::log_compaction(KernelError::LogCompaction(LogCompactionError::InvalidRange { start: 2, end: 1 }), FFIKernelError::LogCompactionError)]
    fn classified_errors_have_semantic_ffi_codes(
        #[case] error: KernelError,
        #[case] expected: FFIKernelError,
    ) {
        assert_eq!(FFIKernelError::from(error), expected);
    }

    #[cfg(feature = "default-engine-base")]
    #[test]
    fn arrow_engine_error_has_semantic_ffi_code() {
        let error = KernelError::ArrowEngine(
            delta_kernel::error::ArrowEngineError::DuplicateRowGroupOrdinal { ordinal: 1 },
        );
        assert_eq!(
            FFIKernelError::from(error),
            FFIKernelError::ArrowEngineError
        );
    }

    #[cfg(feature = "declarative-plans")]
    #[rstest::rstest]
    #[case(
        KernelError::Plan(delta_kernel::error::PlanError::EmptyInput { operation: "union" }),
        FFIKernelError::PlanError
    )]
    #[case(
        KernelError::ProtobufDecode { message_type: "Plan", source: prost::DecodeError::new("truncated message") },
        FFIKernelError::ProtobufDecodeError
    )]
    #[case(
        KernelError::PlanResultTypeMismatch { expected: "rows", actual: "files" },
        FFIKernelError::PlanResultTypeMismatchError
    )]
    fn plan_errors_have_semantic_ffi_codes(
        #[case] error: KernelError,
        #[case] expected: FFIKernelError,
    ) {
        assert_eq!(FFIKernelError::from(error), expected);
    }

    #[test]
    fn scalar_conversion_error_has_semantic_ffi_code() {
        let error = i64::try_from(delta_kernel::expressions::Scalar::Boolean(true)).unwrap_err();
        assert_eq!(
            FFIKernelError::from(error),
            FFIKernelError::ScalarConversionError
        );
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    fn context_and_backtrace_preserve_leaf_code(#[case] context_outermost: bool) {
        let leaf = KernelError::LockPoisoned { resource: "scan" };
        let context = ErrorContext::Commit { version: 9 };
        let error = if context_outermost {
            KernelError::Backtraced {
                source: Box::new(leaf),
                backtrace: Box::new(Backtrace::disabled()),
            }
            .with_context(context)
        } else {
            KernelError::Backtraced {
                source: Box::new(leaf.with_context(context)),
                backtrace: Box::new(Backtrace::disabled()),
            }
        };
        assert!(error.to_string().contains("commit v=9"));
        assert_eq!(
            FFIKernelError::from(error),
            FFIKernelError::LockPoisonedError
        );
    }

    #[test]
    fn legacy_generic_callback_is_not_emitted_as_generic() {
        let error = KernelError::from(EngineExecError {
            etype: FFIKernelError::GenericError,
            message: Box::new("native failure".to_string()).into(),
        });
        assert!(
            matches!(&error, KernelError::ForeignCallback { code: 5, message } if message == "native failure")
        );
        assert_eq!(
            FFIKernelError::from(error),
            FFIKernelError::ForeignCallbackError
        );
    }

    #[test]
    fn recognized_callback_retains_diagnostic_and_classification() {
        let error = KernelError::from(EngineExecError {
            etype: FFIKernelError::CancelledError,
            message: Box::new("shutdown requested".to_string()).into(),
        });
        assert!(error.to_string().contains("shutdown requested"));
        assert_eq!(FFIKernelError::from(error), FFIKernelError::CancelledError);
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
    #[case::generic(FFIKernelError::GenericError, "Foreign callback error (5): boom")]
    #[case::invalid_expr(
        FFIKernelError::InvalidExpression,
        "Invalid expression evaluation: boom"
    )]
    #[case::unit_missing_version(
        FFIKernelError::MissingVersionError,
        "callback (15): boom: No table version found."
    )]
    #[case::fallback_io(FFIKernelError::IOErrorError, "Foreign callback error (6): boom")]
    #[case::fallback_row_tracking(
        FFIKernelError::RowTrackingChangeFeedUnsupported,
        "Foreign callback error (44): boom"
    )]
    fn engine_exec_error_maps_kernel_error_code(
        #[case] etype: FFIKernelError,
        #[case] expected: &str,
    ) {
        let err: KernelError = exec_error(etype, "boom").into();
        assert_eq!(err.to_string(), expected);
    }
}
