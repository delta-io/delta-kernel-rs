use std::ffi::c_void;

use delta_kernel::snapshot::SnapshotHintError;
use delta_kernel::{Error, KernelError, KernelResult};
use tracing::warn;

use crate::handle::Handle;
use crate::{ExclusiveRustString, ExternEngine};

pub mod allocation;

pub use allocation::ErrorAllocators;

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
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub enum FFIKernelError {
    UnknownError = 0, // catch-all for unrecognized KernelError types
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
    CheckpointWriteError = 41,
    SchemaError = 42,
    LogHistoryError = 43,
    RowTrackingChangeFeedUnsupported = 44,
    CancelledError = 45,
    InvalidTransactionStateError = 46,
    InvalidLogSegment = 47,
    UnpublishedVersionError = 48,
    EmptyLogError = 49,
    InvalidSnapshotHint = 50,
    StartVersionNotFound = 51,
    InvalidGeoParamsError = 52,
    MaxCatalogVersionError = 53,
    TableAlreadyExistsAtPath = 54,
    CommitRangeError = 55,
}

impl From<KernelError> for FFIKernelError {
    fn from(e: KernelError) -> Self {
        Self::from(&e)
    }
}

impl From<&KernelError> for FFIKernelError {
    fn from(e: &KernelError) -> Self {
        match e {
            // NOTE: By definition, no KernelError maps to FFIError
            #[cfg(feature = "default-engine-base")]
            KernelError::Arrow(_) => FFIKernelError::ArrowError,
            KernelError::CheckpointWrite(_) => FFIKernelError::CheckpointWriteError,
            KernelError::EngineDataType(_) => FFIKernelError::EngineDataTypeError,
            KernelError::Extract(..) => FFIKernelError::ExtractError,
            KernelError::Generic(_) => FFIKernelError::GenericError,
            KernelError::GenericError { .. } => FFIKernelError::GenericError,
            KernelError::CommitRange(_) => FFIKernelError::CommitRangeError,
            KernelError::MaxCatalogVersion(_) => FFIKernelError::MaxCatalogVersionError,
            KernelError::LogTailVersionsNotContiguous { .. } => FFIKernelError::InvalidLogSegment,
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
            KernelError::EmptyLog => FFIKernelError::EmptyLogError,
            KernelError::MissingVersion(_) => FFIKernelError::MissingVersionError,
            KernelError::StartVersionNotFound { .. } => FFIKernelError::StartVersionNotFound,
            KernelError::UnpublishedVersion(_) => FFIKernelError::UnpublishedVersionError,
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
            KernelError::InvalidGeoParams(_) => FFIKernelError::InvalidGeoParamsError,
            KernelError::InvalidStructData(_) => FFIKernelError::InvalidStructDataError,
            KernelError::InternalError(_) => FFIKernelError::InternalError,
            KernelError::Backtraced {
                source,
                backtrace: _,
            } => Self::from(source.as_ref()),
            KernelError::InvalidExpressionEvaluation(_) => FFIKernelError::InvalidExpression,
            KernelError::InvalidLogPath(_) => FFIKernelError::InvalidLogPath,
            KernelError::InvalidLogSegment(_) => FFIKernelError::InvalidLogSegment,
            KernelError::SnapshotHint(_) => FFIKernelError::InvalidSnapshotHint,
            KernelError::FileAlreadyExists(_) => FFIKernelError::FileAlreadyExists,
            KernelError::TableAlreadyExistsAtPath(_) => FFIKernelError::TableAlreadyExistsAtPath,
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
            KernelError::Schema(_) => FFIKernelError::SchemaError,
            KernelError::InvalidTransactionState(_) => FFIKernelError::InvalidTransactionStateError,
            KernelError::LogHistory(_) => FFIKernelError::LogHistoryError,
            KernelError::Cancelled => FFIKernelError::CancelledError,
            _ => FFIKernelError::UnknownError,
        }
    }
}

/// An FFI result containing a value or a connector-owned error pointer.
///
/// Kernel returns the pointer produced by the connector's allocation callback unchanged, including
/// NULL when allocation fails. `Err(NULL)` is still a failure. The connector interprets and frees
/// non-null error objects; Kernel does not inspect their contents.
#[repr(C)]
pub enum ExternResult<T> {
    /// The operation succeeded with this value.
    Ok(T),
    /// The operation failed; the pointer comes from the connector's allocation callback.
    Err(*mut c_void),
}

impl<T> ExternResult<T> {
    /// Returns `true` for `Ok` and `false` for `Err`, including `Err(NULL)`.
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

/// Allocates connector-owned error objects using the registered callbacks.
pub trait AllocateError {
    /// Converts `error` into a connector-owned error pointer, or NULL if allocation fails.
    ///
    /// # Safety
    ///
    /// The registered context and callbacks must satisfy [`ErrorAllocators`]'s lifetime,
    /// concurrency, and memory access requirements.
    unsafe fn allocate_error(&self, error: Error) -> *mut c_void;

    /// Converts a Kernel `error` into a connector-owned error pointer, or NULL on allocation
    /// failure.
    ///
    /// # Safety
    ///
    /// The registered context and callbacks must satisfy [`ErrorAllocators`]'s lifetime,
    /// concurrency, and memory access requirements.
    unsafe fn allocate_kernel_error(&self, error: KernelError) -> *mut c_void;
}

impl AllocateError for ErrorAllocators {
    unsafe fn allocate_error(&self, error: Error) -> *mut c_void {
        unsafe { self.allocate_error(error) }
    }

    unsafe fn allocate_kernel_error(&self, error: KernelError) -> *mut c_void {
        unsafe { self.allocate_kernel_error(error) }
    }
}

impl<T: ExternEngine + ?Sized> AllocateError for &T {
    unsafe fn allocate_error(&self, error: Error) -> *mut c_void {
        unsafe { self.error_allocator().allocate_error(error) }
    }

    unsafe fn allocate_kernel_error(&self, error: KernelError) -> *mut c_void {
        unsafe { self.error_allocator().allocate_kernel_error(error) }
    }
}

/// Converts [`KernelResult`] or [`delta_kernel::Result`] to an FFI result.
///
/// Only errors invoke the allocator.
pub(crate) trait IntoExternResult<T> {
    /// Preserves a successful value or calls `alloc` to create the returned error pointer.
    ///
    /// # Safety
    ///
    /// `alloc` must satisfy [`ErrorAllocators`]'s callback and context requirements.
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T>;
}

impl<T> IntoExternResult<T> for KernelResult<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T> {
        match self {
            Ok(value) => ExternResult::Ok(value),
            Err(error) => ExternResult::Err(unsafe { alloc.allocate_kernel_error(error) }),
        }
    }
}

impl<T> IntoExternResult<T> for delta_kernel::Result<T> {
    unsafe fn into_extern_result(self, alloc: &dyn AllocateError) -> ExternResult<T> {
        match self {
            Ok(value) => ExternResult::Ok(value),
            Err(error) => ExternResult::Err(unsafe { alloc.allocate_error(error) }),
        }
    }
}

/// An error the engine returns to Kernel when an execution callback fails.
///
/// The engine creates `message` with [`allocate_kernel_string`](crate::allocate_kernel_string)
/// and transfers ownership of that handle to Kernel with the error. Kernel consumes and frees
/// the message. Errors returned to the connector through [`ExternResult`] are connector-owned.
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
/// associated error message is non-empty. Useful for mapping kernel errors to error variants that
/// don't carry a message, but for some reason the engine still provided one.
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
            FFIKernelError::CommitRangeError => KernelError::CommitRange(message),
            FFIKernelError::MaxCatalogVersionError => KernelError::MaxCatalogVersion(message),
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
            FFIKernelError::InvalidGeoParamsError => KernelError::InvalidGeoParams(message),
            FFIKernelError::InvalidStructDataError => KernelError::InvalidStructData(message),
            FFIKernelError::InvalidExpression => KernelError::InvalidExpressionEvaluation(message),
            FFIKernelError::InvalidLogPath => KernelError::InvalidLogPath(message),
            FFIKernelError::InvalidLogSegment => KernelError::InvalidLogSegment(message),
            FFIKernelError::InvalidSnapshotHint => SnapshotHintError::Connector {
                message,
                source: None,
            }
            .into(),
            FFIKernelError::FileAlreadyExists => KernelError::FileAlreadyExists(message),
            FFIKernelError::TableAlreadyExistsAtPath => {
                KernelError::TableAlreadyExistsAtPath(message)
            }
            FFIKernelError::UnsupportedError => KernelError::Unsupported(message),
            FFIKernelError::InvalidCheckpoint => KernelError::InvalidCheckpoint(message),
            FFIKernelError::SchemaError => KernelError::Schema(message),
            FFIKernelError::InvalidTransactionStateError => {
                KernelError::InvalidTransactionState(message)
            }
            code @ FFIKernelError::EmptyLogError => {
                messageless_error(code, message, KernelError::EmptyLog)
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
            | FFIKernelError::LogHistoryError
            | FFIKernelError::MissingVersionError
            | FFIKernelError::UnpublishedVersionError
            | FFIKernelError::StartVersionNotFound) => {
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

    fn exec_error(etype: FFIKernelError, message: &str) -> EngineExecError {
        let message: Handle<ExclusiveRustString> = Box::new(message.to_string()).into();
        EngineExecError { etype, message }
    }

    #[test]
    fn row_tracking_change_feed_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::RowTrackingChangeFeedUnsupported(7)),
            FFIKernelError::RowTrackingChangeFeedUnsupported
        );
        assert_eq!(FFIKernelError::RowTrackingChangeFeedUnsupported as i32, 44);
    }

    #[test]
    fn commit_range_error_has_stable_ffi_mapping() {
        let error = KernelError::CommitRange("invalid builder mode".to_string());
        assert_eq!(
            error.to_string(),
            "Commit range error: invalid builder mode"
        );
        assert_eq!(
            FFIKernelError::from(error),
            FFIKernelError::CommitRangeError
        );
        assert_eq!(FFIKernelError::CommitRangeError as i32, 55);

        let error: KernelError =
            exec_error(FFIKernelError::CommitRangeError, "invalid builder mode").into();
        assert!(matches!(
            error,
            KernelError::CommitRange(message) if message == "invalid builder mode"
        ));
    }

    #[test]
    fn log_segment_errors_have_stable_ffi_mappings() {
        let missing_version = KernelError::MissingVersion(7);
        assert_eq!(
            missing_version.to_string(),
            "Table version 7 is missing or unavailable for this log operation."
        );
        assert_eq!(
            FFIKernelError::from(missing_version),
            FFIKernelError::MissingVersionError
        );
        assert_eq!(
            FFIKernelError::from(KernelError::EmptyLog),
            FFIKernelError::EmptyLogError
        );
        assert_eq!(
            FFIKernelError::from(KernelError::UnpublishedVersion(7)),
            FFIKernelError::UnpublishedVersionError
        );
        assert_eq!(
            FFIKernelError::from(KernelError::InvalidLogSegment("invalid".to_string())),
            FFIKernelError::InvalidLogSegment
        );
        assert_eq!(
            FFIKernelError::from(KernelError::LogTailVersionsNotContiguous {
                first_version: 1,
                second_version: 3,
            }),
            FFIKernelError::InvalidLogSegment
        );
        assert_eq!(FFIKernelError::InvalidLogSegment as i32, 47);
        assert_eq!(FFIKernelError::UnpublishedVersionError as i32, 48);
        assert_eq!(FFIKernelError::EmptyLogError as i32, 49);

        let start_not_found = KernelError::StartVersionNotFound {
            requested: 5,
            earliest: 12,
        };
        assert_eq!(
            start_not_found.to_string(),
            "Start version 5 is not available; earliest available version is 12."
        );
        assert_eq!(
            FFIKernelError::from(start_not_found),
            FFIKernelError::StartVersionNotFound
        );
        assert_eq!(FFIKernelError::StartVersionNotFound as i32, 51);
    }

    #[test]
    fn engine_log_segment_errors_use_supported_ffi_mappings() {
        let missing_version: KernelError =
            exec_error(FFIKernelError::MissingVersionError, "7").into();
        assert!(matches!(
            missing_version,
            KernelError::Generic(message)
                if message == "engine execution error (MissingVersionError): 7"
        ));

        let empty_log: KernelError = exec_error(FFIKernelError::EmptyLogError, "").into();
        assert_eq!(empty_log.to_string(), "No table version found.");
        assert!(matches!(empty_log, KernelError::EmptyLog));
    }

    #[test]
    fn invalid_snapshot_hint_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::from(SnapshotHintError::Connector {
                message: "invalid".to_string(),
                source: None,
            })),
            FFIKernelError::InvalidSnapshotHint
        );
        assert_eq!(FFIKernelError::InvalidSnapshotHint as i32, 50);
    }

    #[test]
    fn invalid_geo_params_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::InvalidGeoParams("invalid".to_string())),
            FFIKernelError::InvalidGeoParamsError
        );
        assert_eq!(FFIKernelError::InvalidGeoParamsError as i32, 52);

        let err: KernelError = exec_error(FFIKernelError::InvalidGeoParamsError, "invalid").into();
        assert!(matches!(err, KernelError::InvalidGeoParams(message) if message == "invalid"));
    }

    #[test]
    fn max_catalog_version_error_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::MaxCatalogVersion("invalid".to_string())),
            FFIKernelError::MaxCatalogVersionError
        );
        assert_eq!(FFIKernelError::MaxCatalogVersionError as i32, 53);

        let err: KernelError = exec_error(FFIKernelError::MaxCatalogVersionError, "invalid").into();
        assert!(matches!(err, KernelError::MaxCatalogVersion(message) if message == "invalid"));
    }

    #[test]
    fn table_already_exists_at_path_has_stable_ffi_mapping() {
        assert_eq!(
            FFIKernelError::from(KernelError::TableAlreadyExistsAtPath(
                "memory:///table".to_string()
            )),
            FFIKernelError::TableAlreadyExistsAtPath
        );
        assert_eq!(FFIKernelError::TableAlreadyExistsAtPath as i32, 54);

        let err: KernelError =
            exec_error(FFIKernelError::TableAlreadyExistsAtPath, "memory:///table").into();
        assert!(matches!(
            err,
            KernelError::TableAlreadyExistsAtPath(path) if path == "memory:///table"
        ));
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

    /// Variants that cannot preserve their original message across FFI reconstruct from their
    /// default `Display`; unmapped codes retain the original code and message.
    #[rstest]
    #[case::file_not_found(FFIKernelError::FileNotFoundError, "File not found: boom")]
    #[case::schema(FFIKernelError::SchemaError, "Schema error: boom")]
    #[case::unsupported(FFIKernelError::UnsupportedError, "Unsupported: boom")]
    #[case::generic(FFIKernelError::GenericError, "Generic delta kernel error: boom")]
    #[case::invalid_expr(
        FFIKernelError::InvalidExpression,
        "Invalid expression evaluation: boom"
    )]
    #[case::invalid_log_segment(FFIKernelError::InvalidLogSegment, "Invalid log segment: boom")]
    #[case::empty_log(FFIKernelError::EmptyLogError, "No table version found.")]
    #[case::invalid_snapshot_hint(
        FFIKernelError::InvalidSnapshotHint,
        "Invalid snapshot hint: boom"
    )]
    #[case::fallback_io(
        FFIKernelError::IOErrorError,
        "Generic delta kernel error: engine execution error (IOErrorError): boom"
    )]
    #[case::fallback_row_tracking(
        FFIKernelError::RowTrackingChangeFeedUnsupported,
        "Generic delta kernel error: engine execution error (RowTrackingChangeFeedUnsupported): boom"
    )]
    #[case::fallback_missing_version(
        FFIKernelError::MissingVersionError,
        "Generic delta kernel error: engine execution error (MissingVersionError): boom"
    )]
    #[case::fallback_unpublished_version(
        FFIKernelError::UnpublishedVersionError,
        "Generic delta kernel error: engine execution error (UnpublishedVersionError): boom"
    )]
    #[case::fallback_start_version_not_found(
        FFIKernelError::StartVersionNotFound,
        "Generic delta kernel error: engine execution error (StartVersionNotFound): boom"
    )]
    fn engine_exec_error_maps_kernel_error_code(
        #[case] etype: FFIKernelError,
        #[case] expected: &str,
    ) {
        let err: KernelError = exec_error(etype, "boom").into();
        assert_eq!(err.to_string(), expected);
    }
}
