use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::mem::size_of;

use delta_kernel::{DeltaResult, EngineError as KernelEngineError, EngineErrorKind, Error};
use tracing::warn;

use crate::handle::Handle;
use crate::{
    kernel_string_slice, ExclusiveRustString, ExternEngine, KernelStringSlice, NullableCvoid,
    OptionalValue,
};

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    LogHistoryError = 43,
    RowTrackingChangeFeedUnsupported = 44,
    CancelledError = 45,
}

impl From<&Error> for KernelError {
    fn from(error: &Error) -> Self {
        // Typed terminal categories take precedence over source inspection. For example, the
        // object-store error retained by a missing-file error is useful diagnostic context, but
        // the V1 category for that error is `FileNotFoundError`.
        if let Some(kind) = error.as_engine_error().map(|error| error.kind()) {
            match kind {
                EngineErrorKind::FileNotFound => return Self::FileNotFoundError,
                EngineErrorKind::FileAlreadyExists => return Self::FileAlreadyExists,
                EngineErrorKind::Cancelled => return Self::CancelledError,
                _ => {}
            }
        }

        if let Some(legacy_type) = engine_exec_legacy_type(error) {
            return legacy_type;
        }

        // Stable terminal Delta conditions define their V1 compatibility category even when a
        // diagnostic source would independently map to a legacy category. Unclassified Delta
        // errors continue to derive their V1 category from their source chain below.
        if let Some(error) = error.as_delta_error() {
            match error.condition() {
                "DELTA_LOG_ALREADY_EXISTS" => return Self::GenericError,
                "DELTA_VERSION_NOT_FOUND" => return Self::MissingVersionError,
                "DELTA_MISSING_PART_FILES" => return Self::InvalidCheckpoint,
                // V1 distinguishes missing metadata, protocol, or both through this condition's
                // source. Preserve those compatibility categories below.
                "DELTA_STATE_RECOVER_ERROR" => {}
                "DELTA_INVALID_PROTOCOL_VERSION" => return Self::InvalidProtocolError,
                "DELTA_UNSUPPORTED_FEATURES_FOR_READ" | "DELTA_UNSUPPORTED_FEATURES_FOR_WRITE" => {
                    return Self::UnsupportedError
                }
                "DELTA_VERSIONS_NOT_CONTIGUOUS" => return Self::GenericError,
                _ => {}
            }
        }

        if let Some(kind) = error.legacy_error_kind() {
            return match kind {
                "CheckpointWrite" => Self::CheckpointWriteError,
                "EngineDataType" => Self::EngineDataTypeError,
                "Generic" => Self::GenericError,
                "MissingColumn" => Self::MissingColumnError,
                "UnexpectedColumnType" => Self::UnexpectedColumnTypeError,
                "MissingData" => Self::MissingDataError,
                "DeletionVector" => Self::DeletionVectorError,
                "InvalidUrl" => Self::InvalidUrlError,
                "IOError" => Self::IOErrorError,
                "MissingMetadata" => Self::MissingMetadataError,
                "MissingProtocol" => Self::MissingProtocolError,
                "MissingMetadataAndProtocol" => Self::MissingMetadataAndProtocolError,
                "MalformedJson" => Self::MalformedJsonError,
                "Utf8" => Self::Utf8Error,
                #[cfg(feature = "default-engine-base")]
                "Arrow" => Self::ArrowError,
                #[cfg(feature = "default-engine-base")]
                "Parquet" => Self::ParquetError,
                #[cfg(feature = "default-engine-base")]
                "ObjectStore" => Self::ObjectStoreError,
                #[cfg(feature = "default-engine-base")]
                "ObjectStorePath" => Self::ObjectStorePathError,
                #[cfg(feature = "default-engine-base")]
                "Reqwest" => Self::ReqwestError,
                "InvalidProtocol" => Self::InvalidProtocolError,
                "JoinFailure" => Self::JoinFailureError,
                "ParseInt" => Self::ParseIntError,
                "InvalidColumnMappingMode" => Self::InvalidColumnMappingModeError,
                "InvalidTableLocation" => Self::InvalidTableLocationError,
                "InvalidDecimal" => Self::InvalidDecimalError,
                "InvalidStructData" => Self::InvalidStructDataError,
                "Internal" => Self::InternalError,
                "InvalidExpression" => Self::InvalidExpression,
                "InvalidLogPath" => Self::InvalidLogPath,
                "Parse" => Self::ParseError,
                "Unsupported" => Self::UnsupportedError,
                "ParseInterval" => Self::ParseIntervalError,
                "CdfUnsupported" => Self::ChangeDataFeedUnsupported,
                "CdfIncompatibleSchema" => Self::ChangeDataFeedIncompatibleSchema,
                "RowTrackingCdfUnsupported" => Self::RowTrackingChangeFeedUnsupported,
                "InvalidCheckpoint" => Self::InvalidCheckpoint,
                "LiteralExpressionTransform" => Self::LiteralExpressionTransformError,
                "Schema" => Self::SchemaError,
                "LogHistory" => Self::LogHistoryError,
                _ => Self::UnknownError,
            };
        }

        match error.as_engine_error().map(|error| error.kind()) {
            Some(EngineErrorKind::FileNotFound) => Self::FileNotFoundError,
            Some(EngineErrorKind::FileAlreadyExists) => Self::FileAlreadyExists,
            Some(EngineErrorKind::CorruptData) => Self::GenericError,
            Some(EngineErrorKind::Cancelled) => Self::CancelledError,
            _ => Self::UnknownError,
        }
    }
}

impl From<Error> for KernelError {
    fn from(error: Error) -> Self {
        Self::from(&error)
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

/// Indicates that an FFI error descriptor has no known origin.
pub const FFI_ERROR_ORIGIN_UNKNOWN: u32 = 0;
/// Indicates that an FFI error originated in Delta Kernel.
pub const FFI_ERROR_ORIGIN_KERNEL: u32 = 1;
/// Indicates that an FFI error originated in an engine implementation.
pub const FFI_ERROR_ORIGIN_ENGINE: u32 = 2;

/// A borrowed named error parameter passed to a V2 error allocator.
///
/// Both strings are valid only for the duration of the allocator callback. The callback must copy
/// either string if it needs to retain it.
#[repr(C)]
pub struct FfiErrorParameterV1 {
    /// The case-sensitive Spark-compatible parameter name.
    pub name: KernelStringSlice,
    /// The rendered parameter value.
    pub value: KernelStringSlice,
}

/// A borrowed structured error descriptor passed to a V2 error allocator.
///
/// The descriptor, parameter array, and all string slices are valid only for the duration of the
/// allocator callback. The callback must deep-copy any data it retains. `debug` is diagnostic text
/// and can contain sensitive table paths or storage details.
#[repr(C)]
pub struct FfiErrorDescriptorV1 {
    /// Descriptor schema version. This is `1` for this layout.
    pub descriptor_version: u32,
    /// Reserved for alignment and future flags. Callers must ignore it.
    pub reserved: u32,
    /// Size of this descriptor in bytes, allowing compatible extension of the layout.
    pub descriptor_size: usize,
    /// One of the `FFI_ERROR_ORIGIN_*` constants.
    pub origin: u32,
    /// The closest V1 [`KernelError`] category.
    pub legacy_type: KernelError,
    /// Stable Delta condition, or `None` for engine-originated errors.
    pub condition: OptionalValue<KernelStringSlice>,
    /// Spark SQLSTATE, or `None` when the condition has no SQLSTATE.
    pub sql_state: OptionalValue<KernelStringSlice>,
    /// Lexically ordered named parameters. This is null when `parameter_count` is zero; otherwise,
    /// the pointer is valid for `parameter_count` elements.
    pub parameters: *const FfiErrorParameterV1,
    /// Number of entries in `parameters`.
    pub parameter_count: usize,
    /// User-facing rendering of the error.
    pub display: KernelStringSlice,
    /// Diagnostic rendering including the source chain and captured backtrace, when available.
    pub debug: KernelStringSlice,
}

/// Callback used by [`FfiErrorAllocatorV2`] to allocate an engine-owned error.
///
/// The callback must deep-copy any descriptor data it retains, must not unwind across the FFI
/// boundary, and owns responsibility for freeing the returned pointer. Returning null is allowed.
pub type AllocateErrorFnV2 = extern "C" fn(
    context: NullableCvoid,
    descriptor: *const FfiErrorDescriptorV1,
) -> *mut EngineError;

/// A V2 structured error allocator supplied by an engine.
///
/// `context` remains caller-owned and must stay valid for the lifetimes of the builder, the engine,
/// and every derived handle or object that retains that engine. Releasing the caller's engine
/// handle does not end this requirement while a derived object still retains the engine. The
/// callback and context must be safe for concurrent invocation throughout these lifetimes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FfiErrorAllocatorV2 {
    /// Caller-owned context forwarded to `allocate`.
    pub context: NullableCvoid,
    /// Callback that deep-copies the borrowed descriptor and returns an engine-owned error.
    pub allocate: AllocateErrorFnV2,
}

// SAFETY: The FFI contract requires the callback and its caller-owned context to support
// concurrent invocation while a builder, engine, or derived object retaining that engine exists.
unsafe impl Send for FfiErrorAllocatorV2 {}
// SAFETY: See the `Send` implementation.
unsafe impl Sync for FfiErrorAllocatorV2 {}

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible for freeing it.
#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

pub type AllocateErrorFn =
    extern "C" fn(etype: KernelError, msg: KernelStringSlice) -> *mut EngineError;

#[derive(Clone, Copy)]
#[cfg(feature = "default-engine-base")]
pub(crate) enum ErrorAllocator {
    V1(AllocateErrorFn),
    V2(FfiErrorAllocatorV2),
}

#[cfg(feature = "default-engine-base")]
impl ErrorAllocator {
    pub(crate) fn v1(self) -> Option<AllocateErrorFn> {
        match self {
            Self::V1(allocate) => Some(allocate),
            Self::V2(_) => None,
        }
    }
}

#[cfg(feature = "default-engine-base")]
impl From<AllocateErrorFn> for ErrorAllocator {
    fn from(value: AllocateErrorFn) -> Self {
        Self::V1(value)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<FfiErrorAllocatorV2> for ErrorAllocator {
    fn from(value: FfiErrorAllocatorV2) -> Self {
        Self::V2(value)
    }
}

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

/// Represents an engine error allocator.
pub trait AllocateError {
    /// Allocates a new error in engine memory and returns the resulting pointer.
    ///
    /// # Safety
    ///
    /// The error allocator and any caller-owned context it retains must be valid for the lifetime
    /// of every object that can invoke it.
    unsafe fn allocate_error(&self, error: &Error) -> *mut EngineError;
}

impl AllocateError for AllocateErrorFn {
    unsafe fn allocate_error(&self, error: &Error) -> *mut EngineError {
        let msg = error.to_string();
        self(KernelError::from(error), kernel_string_slice!(msg))
    }
}

impl AllocateError for FfiErrorAllocatorV2 {
    unsafe fn allocate_error(&self, error: &Error) -> *mut EngineError {
        let display = error.to_string();
        let debug = debug_error(error);
        let delta_error = error.as_delta_error();
        let origin = if delta_error.is_some() {
            FFI_ERROR_ORIGIN_KERNEL
        } else if error.as_engine_error().is_some() {
            FFI_ERROR_ORIGIN_ENGINE
        } else {
            FFI_ERROR_ORIGIN_UNKNOWN
        };

        let condition = delta_error.map(|error| error.condition());
        let sql_state = delta_error.and_then(|error| error.sql_state());
        let mut named_parameters = delta_error
            .map(|error| {
                error
                    .parameters()
                    .iter()
                    .map(|parameter| (parameter.name(), parameter.value()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        named_parameters.sort_unstable_by(|left, right| left.0.cmp(right.0));
        let parameters = named_parameters
            .iter()
            .map(|(name, value)| FfiErrorParameterV1 {
                // SAFETY: Both source strings outlive the synchronous allocator callback below.
                name: unsafe { KernelStringSlice::new_unsafe(name) },
                // SAFETY: See `name`.
                value: unsafe { KernelStringSlice::new_unsafe(value) },
            })
            .collect::<Vec<_>>();

        let descriptor = FfiErrorDescriptorV1 {
            descriptor_version: 1,
            reserved: 0,
            descriptor_size: size_of::<FfiErrorDescriptorV1>(),
            origin,
            legacy_type: KernelError::from(error),
            condition: optional_string_slice(condition),
            sql_state: optional_string_slice(sql_state),
            parameters: if parameters.is_empty() {
                std::ptr::null()
            } else {
                parameters.as_ptr()
            },
            parameter_count: parameters.len(),
            display: kernel_string_slice!(display),
            debug: kernel_string_slice!(debug),
        };
        (self.allocate)(self.context, &descriptor)
    }
}

#[cfg(feature = "default-engine-base")]
impl AllocateError for ErrorAllocator {
    unsafe fn allocate_error(&self, error: &Error) -> *mut EngineError {
        match self {
            Self::V1(allocate) => unsafe { allocate.allocate_error(error) },
            Self::V2(allocate) => unsafe { allocate.allocate_error(error) },
        }
    }
}

// We do this instead of `impl AllocateError for &dyn ExternEngine` since we can then directly use
// this trait on type T instead of having to cast it to a trait object first.
impl<T: ExternEngine + ?Sized> AllocateError for &T {
    /// # Safety
    ///
    /// In addition to the usual requirements, the engine handle must be valid.
    unsafe fn allocate_error(&self, error: &Error) -> *mut EngineError {
        self.error_allocator().allocate_error(error)
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
                let err = unsafe { alloc.allocate_error(&err) };
                ExternResult::Err(err)
            }
        }
    }
}

fn optional_string_slice(value: Option<&str>) -> OptionalValue<KernelStringSlice> {
    value
        .map(|value| {
            // SAFETY: Callers only use the resulting slice during the synchronous allocator
            // callback, while the borrowed source remains alive.
            unsafe { KernelStringSlice::new_unsafe(value) }
        })
        .into()
}

fn debug_error(error: &Error) -> String {
    let mut debug = format!("{error:?}");
    let mut source = error
        .as_delta_error()
        .and_then(|error| error.source())
        .or_else(|| error.as_engine_error().and_then(|error| error.source()));
    for depth in 0..16 {
        let Some(current) = source else {
            break;
        };
        let _ = write!(debug, "\nsource[{depth}]: {current}");
        source = current.source();
    }

    let backtrace = error
        .as_delta_error()
        .map(|error| error.backtrace())
        .or_else(|| error.as_engine_error().map(|error| error.backtrace()));
    if let Some(backtrace) = backtrace.filter(|trace| trace.status() == BacktraceStatus::Captured) {
        let _ = write!(debug, "\nbacktrace:\n{backtrace}");
    }
    debug
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

impl From<EngineExecError> for KernelEngineError {
    /// Converts an [`EngineExecError`] into a typed engine error and consumes the message handle.
    fn from(err: EngineExecError) -> Self {
        let EngineExecError { etype, message } = err;
        // SAFETY: `message` is an `ExclusiveRustString` handle that kernel owns and has not yet
        // consumed. It is produced by the engine downcalling `allocate_kernel_string` and is
        // consumed exactly once, here.
        let message = *unsafe { message.into_inner() };
        match etype {
            KernelError::FileNotFoundError => Self::file_not_found(message),
            KernelError::FileAlreadyExists => Self::file_already_exists(message),
            KernelError::CancelledError => {
                if !message.is_empty() {
                    warn!("Discarding message for cancelled engine execution error: {message}");
                }
                Self::cancelled()
            }
            KernelError::IOErrorError => {
                let rendered = format!("engine execution error (IOErrorError): {message}");
                Self::other(rendered).with_source(std::io::Error::other(message))
            }
            code => {
                let rendered = format!("engine execution error ({code:?}): {message}");
                Self::other(rendered).with_source(EngineExecErrorSource {
                    legacy_type: code,
                    message,
                })
            }
        }
    }
}

impl From<EngineExecError> for Error {
    fn from(error: EngineExecError) -> Self {
        KernelEngineError::from(error).into()
    }
}

#[derive(Debug)]
struct EngineExecErrorSource {
    legacy_type: KernelError,
    message: String,
}

impl std::fmt::Display for EngineExecErrorSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "engine execution error ({:?}): {}",
            self.legacy_type, self.message
        )
    }
}

impl StdError for EngineExecErrorSource {}

fn engine_exec_legacy_type(error: &Error) -> Option<KernelError> {
    let mut source = error.as_engine_error().and_then(StdError::source);
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<EngineExecErrorSource>() {
            return Some(error.legacy_type);
        }
        source = current.source();
    }
    None
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use rstest::rstest;

    use super::*;

    fn exec_error(etype: KernelError, message: &str) -> EngineExecError {
        let message: Handle<ExclusiveRustString> = Box::new(message.to_string()).into();
        EngineExecError { etype, message }
    }

    #[rstest]
    #[case::file_not_found(KernelError::FileNotFoundError, EngineErrorKind::FileNotFound)]
    #[case::file_already_exists(KernelError::FileAlreadyExists, EngineErrorKind::FileAlreadyExists)]
    #[case::cancelled(KernelError::CancelledError, EngineErrorKind::Cancelled)]
    #[case::fallback(KernelError::IOErrorError, EngineErrorKind::Other)]
    fn engine_exec_error_maps_kernel_error_code(
        #[case] etype: KernelError,
        #[case] expected: EngineErrorKind,
    ) {
        let err: KernelEngineError = exec_error(etype, "boom").into();
        assert_eq!(err.kind(), expected);
    }

    #[test]
    fn engine_exec_io_error_retains_io_source() {
        let error: KernelEngineError =
            exec_error(KernelError::IOErrorError, "storage failed").into();
        assert_eq!(error.kind(), EngineErrorKind::Other);
        assert!(
            std::error::Error::source(&error).is_some_and(|source| source.is::<std::io::Error>())
        );
    }

    #[rstest]
    #[case::generic(KernelError::GenericError)]
    #[case::missing_column(KernelError::MissingColumnError)]
    #[case::invalid_protocol(KernelError::InvalidProtocolError)]
    #[case::unsupported(KernelError::UnsupportedError)]
    #[case::schema(KernelError::SchemaError)]
    #[case::log_history(KernelError::LogHistoryError)]
    fn engine_exec_error_preserves_v1_category(#[case] expected: KernelError) {
        let error: Error = exec_error(expected, "boom").into();

        assert_eq!(KernelError::from(&error), expected);
    }
}

#[cfg(test)]
mod structured_error_tests {
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::TryFromStringSlice;

    #[derive(Debug)]
    struct OwnedDescriptor {
        descriptor_version: u32,
        reserved: u32,
        descriptor_size: usize,
        origin: u32,
        legacy_type: KernelError,
        condition: Option<String>,
        sql_state: Option<String>,
        parameters: Vec<(String, String)>,
        display: String,
        debug: String,
        empty_parameters_pointer_is_null: bool,
    }

    #[repr(C)]
    struct OwnedEngineError {
        base: EngineError,
        descriptor: OwnedDescriptor,
    }

    unsafe fn copy_string(value: &KernelStringSlice) -> String {
        unsafe { String::try_from_slice(value) }.unwrap()
    }

    unsafe fn copy_optional(value: &OptionalValue<KernelStringSlice>) -> Option<String> {
        match value {
            OptionalValue::Some(value) => Some(unsafe { copy_string(value) }),
            OptionalValue::None => None,
        }
    }

    extern "C" fn copy_descriptor(
        _context: NullableCvoid,
        descriptor: *const FfiErrorDescriptorV1,
    ) -> *mut EngineError {
        // SAFETY: The allocator contract guarantees a valid descriptor for this callback.
        let descriptor = unsafe { &*descriptor };
        let parameters = if descriptor.parameter_count == 0 {
            &[]
        } else {
            // SAFETY: The descriptor guarantees this many valid parameter entries.
            unsafe { std::slice::from_raw_parts(descriptor.parameters, descriptor.parameter_count) }
        };
        let parameters = parameters
            .iter()
            .map(|parameter| {
                // SAFETY: All strings remain valid for the duration of this callback.
                unsafe { (copy_string(&parameter.name), copy_string(&parameter.value)) }
            })
            .collect();
        let owned = OwnedEngineError {
            base: EngineError {
                etype: descriptor.legacy_type,
            },
            descriptor: OwnedDescriptor {
                descriptor_version: descriptor.descriptor_version,
                reserved: descriptor.reserved,
                descriptor_size: descriptor.descriptor_size,
                origin: descriptor.origin,
                legacy_type: descriptor.legacy_type,
                // SAFETY: All strings remain valid for the duration of this callback.
                condition: unsafe { copy_optional(&descriptor.condition) },
                // SAFETY: See `condition`.
                sql_state: unsafe { copy_optional(&descriptor.sql_state) },
                parameters,
                // SAFETY: See `condition`.
                display: unsafe { copy_string(&descriptor.display) },
                // SAFETY: See `condition`.
                debug: unsafe { copy_string(&descriptor.debug) },
                empty_parameters_pointer_is_null: descriptor.parameter_count != 0
                    || descriptor.parameters.is_null(),
            },
        };
        Box::into_raw(Box::new(owned)).cast()
    }

    extern "C" fn count_descriptor(
        context: NullableCvoid,
        _descriptor: *const FfiErrorDescriptorV1,
    ) -> *mut EngineError {
        if let Some(context) = context {
            // SAFETY: The test keeps the atomic counter alive until every callback completes.
            let calls = unsafe { &*context.as_ptr().cast::<AtomicUsize>() };
            calls.fetch_add(1, Ordering::Relaxed);
        }
        std::ptr::null_mut()
    }

    unsafe fn recover_owned(error: *mut EngineError) -> OwnedDescriptor {
        unsafe { Box::from_raw(error.cast::<OwnedEngineError>()) }.descriptor
    }

    #[test]
    fn v2_allocator_descriptor_can_be_deep_copied() {
        let allocator = FfiErrorAllocatorV2 {
            context: None,
            allocate: copy_descriptor,
        };
        let error = Error::generic("invalid λ path");

        // SAFETY: The allocator and callback are valid for this call.
        let allocated = unsafe { allocator.allocate_error(&error) };
        drop(error);
        // SAFETY: `copy_descriptor` allocated this pointer with the matching layout.
        let descriptor = unsafe { recover_owned(allocated) };

        assert_eq!(descriptor.descriptor_version, 1);
        assert_eq!(descriptor.reserved, 0);
        assert_eq!(
            descriptor.descriptor_size,
            size_of::<FfiErrorDescriptorV1>()
        );
        assert_eq!(descriptor.origin, FFI_ERROR_ORIGIN_KERNEL);
        assert_eq!(descriptor.legacy_type, KernelError::GenericError);
        assert_eq!(
            descriptor.condition.as_deref(),
            Some("DELTA_KERNEL_UNCLASSIFIED")
        );
        assert_eq!(descriptor.sql_state, None);
        assert!(descriptor.parameters.is_empty());
        assert!(descriptor.empty_parameters_pointer_is_null);
        assert!(descriptor.display.contains("invalid λ path"));
        assert!(descriptor.debug.contains("invalid λ path"));
    }

    #[test]
    fn engine_error_descriptor_has_no_delta_metadata() {
        let allocator = FfiErrorAllocatorV2 {
            context: None,
            allocate: copy_descriptor,
        };
        let error = Error::file_not_found("s3://bucket/missing");

        // SAFETY: The allocator and callback are valid for this call.
        let allocated = unsafe { allocator.allocate_error(&error) };
        // SAFETY: `copy_descriptor` allocated this pointer with the matching layout.
        let descriptor = unsafe { recover_owned(allocated) };

        assert_eq!(descriptor.origin, FFI_ERROR_ORIGIN_ENGINE);
        assert_eq!(descriptor.legacy_type, KernelError::FileNotFoundError);
        assert_eq!(descriptor.condition, None);
        assert_eq!(descriptor.sql_state, None);
        assert!(descriptor.parameters.is_empty());
    }

    #[test]
    fn v1_mapping_preserves_unclassified_legacy_category() {
        let parse_error: Error = "not an integer".parse::<i64>().unwrap_err().into();
        let generic_source = Error::generic_err(std::io::Error::other("source detail"));

        assert_eq!(KernelError::from(&parse_error), KernelError::ParseIntError);
        assert_eq!(
            KernelError::from(&Error::generic("generic failure")),
            KernelError::GenericError
        );
        assert_eq!(
            KernelError::from(&generic_source),
            KernelError::GenericError
        );
    }

    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn v1_structured_condition_precedes_diagnostic_source_category() {
        use std::sync::Arc;

        use delta_kernel::object_store::memory::InMemory;
        use delta_kernel::{EngineErrorKind, Snapshot};
        use delta_kernel_default_engine::DefaultEngineBuilder;
        use test_utils::add_commit;

        let table_root = "memory:///";
        let store = Arc::new(InMemory::new());
        let commit = r#"{"metaData":{"id":"id","format":{"provider":"parquet","options":{}},"partitionColumns":[],"configuration":{}}}
{"protocol":{"minReaderVersion":2147483647,"minWriterVersion":2147483647,"readerFeatures":[],"writerFeatures":[]}}"#;
        add_commit(table_root, store.as_ref(), 0, commit.to_string())
            .await
            .unwrap();
        let engine = DefaultEngineBuilder::new(store).build();

        let error = Snapshot::builder_for(table_root)
            .build(&engine)
            .unwrap_err();
        let source = error
            .as_delta_error()
            .and_then(StdError::source)
            .and_then(|source| source.downcast_ref::<Error>())
            .and_then(Error::as_engine_error)
            .expect("structured condition should retain the corrupt engine error");

        assert_eq!(source.kind(), EngineErrorKind::CorruptData);
        assert_eq!(KernelError::from(&error), KernelError::InvalidProtocolError);
    }

    #[test]
    fn v1_typed_engine_categories_take_precedence_over_diagnostic_sources() {
        let missing: Error = KernelEngineError::file_not_found("missing")
            .with_source(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "source detail",
            ))
            .into();
        let existing: Error = KernelEngineError::file_already_exists("existing")
            .with_source(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "source detail",
            ))
            .into();
        let cancelled: Error = KernelEngineError::cancelled()
            .with_source(std::io::Error::other("source detail"))
            .into();
        let other: Error = KernelEngineError::other("other")
            .with_source(std::io::Error::other("source detail"))
            .into();

        assert_eq!(KernelError::from(&missing), KernelError::FileNotFoundError);
        assert_eq!(KernelError::from(&existing), KernelError::FileAlreadyExists);
        assert_eq!(KernelError::from(&cancelled), KernelError::CancelledError);
        assert_eq!(KernelError::from(&other), KernelError::IOErrorError);
    }

    #[test]
    fn v1_error_code_discriminants_are_stable() {
        macro_rules! assert_code {
            ($variant:ident, $value:literal) => {
                assert_eq!(KernelError::$variant as i32, $value);
            };
        }

        assert_code!(UnknownError, 0);
        assert_code!(FFIError, 1);
        #[cfg(feature = "default-engine-base")]
        assert_code!(ArrowError, 2);
        assert_code!(EngineDataTypeError, 3);
        assert_code!(ExtractError, 4);
        assert_code!(GenericError, 5);
        assert_code!(IOErrorError, 6);
        #[cfg(feature = "default-engine-base")]
        assert_code!(ParquetError, 7);
        #[cfg(feature = "default-engine-base")]
        assert_code!(ObjectStoreError, 8);
        #[cfg(feature = "default-engine-base")]
        assert_code!(ObjectStorePathError, 9);
        #[cfg(feature = "default-engine-base")]
        assert_code!(ReqwestError, 10);
        assert_code!(FileNotFoundError, 11);
        assert_code!(MissingColumnError, 12);
        assert_code!(UnexpectedColumnTypeError, 13);
        assert_code!(MissingDataError, 14);
        assert_code!(MissingVersionError, 15);
        assert_code!(DeletionVectorError, 16);
        assert_code!(InvalidUrlError, 17);
        assert_code!(MalformedJsonError, 18);
        assert_code!(MissingMetadataError, 19);
        assert_code!(MissingProtocolError, 20);
        assert_code!(InvalidProtocolError, 21);
        assert_code!(MissingMetadataAndProtocolError, 22);
        assert_code!(ParseError, 23);
        assert_code!(JoinFailureError, 24);
        assert_code!(Utf8Error, 25);
        assert_code!(ParseIntError, 26);
        assert_code!(InvalidColumnMappingModeError, 27);
        assert_code!(InvalidTableLocationError, 28);
        assert_code!(InvalidDecimalError, 29);
        assert_code!(InvalidStructDataError, 30);
        assert_code!(InternalError, 31);
        assert_code!(InvalidExpression, 32);
        assert_code!(InvalidLogPath, 33);
        assert_code!(FileAlreadyExists, 34);
        assert_code!(UnsupportedError, 35);
        assert_code!(ParseIntervalError, 36);
        assert_code!(ChangeDataFeedUnsupported, 37);
        assert_code!(ChangeDataFeedIncompatibleSchema, 38);
        assert_code!(InvalidCheckpoint, 39);
        assert_code!(LiteralExpressionTransformError, 40);
        assert_code!(CheckpointWriteError, 41);
        assert_code!(SchemaError, 42);
        assert_code!(LogHistoryError, 43);
        assert_code!(RowTrackingChangeFeedUnsupported, 44);
        assert_code!(CancelledError, 45);
    }

    #[test]
    fn v2_descriptor_abi_layout_is_stable() {
        use std::mem::{align_of, offset_of};

        assert_eq!(size_of::<KernelError>(), size_of::<std::os::raw::c_int>());
        assert_eq!(size_of::<FfiErrorParameterV1>(), 4 * size_of::<usize>());
        assert_eq!(offset_of!(FfiErrorParameterV1, name), 0);
        assert_eq!(
            offset_of!(FfiErrorParameterV1, value),
            2 * size_of::<usize>()
        );
        assert_eq!(size_of::<FfiErrorAllocatorV2>(), 2 * size_of::<usize>());
        assert_eq!(align_of::<FfiErrorAllocatorV2>(), align_of::<usize>());
        assert_eq!(offset_of!(FfiErrorAllocatorV2, context), 0);
        assert_eq!(
            offset_of!(FfiErrorAllocatorV2, allocate),
            size_of::<usize>()
        );

        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(size_of::<FfiErrorDescriptorV1>(), 120);
            assert_eq!(align_of::<FfiErrorDescriptorV1>(), 8);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, descriptor_version), 0);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, reserved), 4);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, descriptor_size), 8);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, origin), 16);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, legacy_type), 20);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, condition), 24);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, sql_state), 48);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, parameters), 72);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, parameter_count), 80);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, display), 88);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, debug), 104);
        }

        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(size_of::<FfiErrorDescriptorV1>(), 68);
            assert_eq!(align_of::<FfiErrorDescriptorV1>(), 4);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, descriptor_version), 0);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, reserved), 4);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, descriptor_size), 8);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, origin), 12);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, legacy_type), 16);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, condition), 20);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, sql_state), 32);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, parameters), 44);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, parameter_count), 48);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, display), 52);
            assert_eq!(offset_of!(FfiErrorDescriptorV1, debug), 60);
        }
    }

    #[test]
    fn v2_debug_includes_typed_source_chain() {
        let allocator = FfiErrorAllocatorV2 {
            context: None,
            allocate: copy_descriptor,
        };
        let error: Error = delta_kernel::EngineError::other("outer engine failure")
            .with_source(std::io::Error::other("inner storage failure"))
            .into();

        // SAFETY: The allocator and callback are valid for this call.
        let allocated = unsafe { allocator.allocate_error(&error) };
        // SAFETY: `copy_descriptor` allocated this pointer with the matching layout.
        let descriptor = unsafe { recover_owned(allocated) };

        assert!(descriptor.debug.contains("outer engine failure"));
        assert!(descriptor
            .debug
            .contains("source[0]: inner storage failure"));
    }

    #[test]
    fn v2_allocator_supports_concurrent_callbacks() {
        let calls = AtomicUsize::new(0);
        let allocator = FfiErrorAllocatorV2 {
            context: Some(NonNull::from(&calls).cast()),
            allocate: count_descriptor,
        };

        std::thread::scope(|scope| {
            for index in 0..8 {
                scope.spawn(move || {
                    let error = Error::engine(format!("concurrent failure {index}"));
                    // SAFETY: The callback context remains valid for this scoped thread.
                    let allocated = unsafe { allocator.allocate_error(&error) };
                    assert!(allocated.is_null());
                });
            }
        });

        assert_eq!(calls.load(Ordering::Relaxed), 8);
    }
}
