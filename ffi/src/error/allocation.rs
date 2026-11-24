//! Callbacks for converting Kernel and Delta errors into connector-owned error objects.
//!
//! The connector supplies an allocation callback for each error kind and a context pointer.
//! Builders and engines retain a copy of the callback table; standalone calls use it only during
//! the call. Copying the table copies its pointers, not the connector state they reference. The
//! connector keeps that state and the callback code alive for every use.

use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::ffi::c_void;

use delta_kernel::{Error, KernelError};

use super::FFIKernelError;
use crate::delta_types::{FfiStringMap, FfiStringMapEntry};
use crate::{FfiSlice, KernelStringSlice, OptionalValue, kernel_string_slice};

/// Kernel error code and diagnostic details.
///
/// Use the code to classify the error. Diagnostic text is unredacted and may contain user data.
/// All strings and arrays are borrowed for the allocation callback.
#[repr(C)]
pub struct KernelErrorInfo {
    /// Numeric code identifying the kind of Kernel error.
    pub code: FFIKernelError,
    /// Rendered diagnostic message, without a trailing NUL.
    ///
    /// For [`KernelError::Backtraced`], this includes the wrapper's backtrace.
    pub message: KernelStringSlice,
    /// Rendered source messages, ordered from the immediate cause to the root cause.
    ///
    /// Each entry uses the source's `Display` output. A [`KernelError::Backtraced`] source
    /// includes its own backtrace and is followed by its inner error, so messages can repeat.
    pub sources: FfiSlice<KernelStringSlice>,
}

/// Delta catalog condition and diagnostic details.
///
/// Use the condition or SQLSTATE to classify the error. Diagnostic text is unredacted and may
/// contain user data. All strings and arrays are borrowed for the allocation callback.
#[repr(C)]
pub struct DeltaErrorInfo {
    /// Stable catalog condition.
    pub condition: KernelStringSlice,
    /// Catalog SQLSTATE, absent when undefined.
    pub sql_state: OptionalValue<KernelStringSlice>,
    /// Named string parameters in catalog template order.
    pub parameters: FfiStringMap,
    /// Rendered catalog message.
    pub message: KernelStringSlice,
    /// Construction-site Rust backtrace, absent when capture is disabled or unsupported.
    ///
    /// Capture follows Rust's environment settings; see [`delta_kernel::DeltaError::backtrace`].
    /// The trace belongs to the original error; converting it for FFI does not capture another
    /// stack.
    pub backtrace: OptionalValue<KernelStringSlice>,
    /// Rendered source messages, with the same ordering and rendering as
    /// [`KernelErrorInfo::sources`].
    pub sources: FfiSlice<KernelStringSlice>,
}

/// Connector-supplied callbacks that turn Kernel and Delta errors into connector-owned objects.
///
/// Kernel never reads or frees the error objects returned by these callbacks. The connector is
/// responsible for freeing them. Callbacks must not unwind across the FFI boundary. Their code
/// and context must remain valid whenever they are called. If shared across threads, callbacks
/// must support concurrent calls and synchronize access to their context as needed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ErrorAllocators {
    /// Connector state passed unchanged to both callbacks; NULL is allowed if unused.
    pub context: *mut c_void,
    /// Creates a connector error object from the Kernel error information in the second argument.
    ///
    /// The information pointer is non-null. It, the source array, and all referenced strings are
    /// valid only until the callback returns. If the connector needs the information afterward,
    /// it must copy it into memory it owns. The callback must not modify or free the information.
    ///
    /// Return a pointer to the connector's error object, or NULL if allocation fails. Kernel
    /// returns this pointer unchanged to the caller.
    pub allocate_kernel_error:
        unsafe extern "C" fn(*mut c_void, *const KernelErrorInfo) -> *mut c_void,
    /// Creates a connector error object from the Delta error information in the second argument.
    ///
    /// The information pointer is non-null. It, the arrays, and all referenced strings are valid
    /// only until the callback returns. If the connector needs any of this information afterward,
    /// it must copy it into memory it owns. The callback must not modify or free the information.
    ///
    /// Return a pointer to the connector's error object, or NULL if allocation fails. Kernel
    /// returns this pointer unchanged to the caller.
    pub allocate_delta_error:
        unsafe extern "C" fn(*mut c_void, *const DeltaErrorInfo) -> *mut c_void,
}

// SAFETY: Moving or sharing the table does not access its context. Calling either callback is
// unsafe and requires the caller to uphold the context's lifetime and concurrency requirements.
unsafe impl Send for ErrorAllocators {}
unsafe impl Sync for ErrorAllocators {}

impl ErrorAllocators {
    /// Calls the Kernel callback for `error` and returns its pointer unchanged, including NULL.
    ///
    /// # Safety
    ///
    /// The context and callback must satisfy [`ErrorAllocators`]'s lifetime and concurrency
    /// requirements. The callback must follow its field's memory access and ownership rules.
    pub unsafe fn allocate_kernel_error(&self, error: KernelError) -> *mut c_void {
        let message = error.to_string();
        let sources = source_messages(&error);
        let source_slices: Vec<_> = sources
            .iter()
            .map(|source| kernel_string_slice!(source))
            .collect();
        let descriptor = KernelErrorInfo {
            code: error.into(),
            message: kernel_string_slice!(message),
            sources: unsafe { FfiSlice::new_unsafe(&source_slices) },
        };
        unsafe { (self.allocate_kernel_error)(self.context, &descriptor) }
    }

    /// Calls the callback for `error` and returns its pointer unchanged, including NULL.
    ///
    /// The outer error variant selects the corresponding registered callback, including when a
    /// Delta error has a Kernel cause. The supplied error information remains valid until the
    /// selected callback returns.
    ///
    /// # Safety
    ///
    /// The context and callbacks must satisfy [`ErrorAllocators`]'s lifetime and concurrency
    /// requirements. Each callback must follow its field's memory access and ownership rules.
    pub unsafe fn allocate_error(&self, error: Error) -> *mut c_void {
        match error {
            Error::Kernel(error) => unsafe { self.allocate_kernel_error(error) },
            Error::Delta(error) => {
                let message = error.message();
                let backtrace = (error.backtrace().status() == BacktraceStatus::Captured)
                    .then(|| error.backtrace().to_string());
                let sources = source_messages(&error);
                let source_slices: Vec<_> = sources
                    .iter()
                    .map(|source| kernel_string_slice!(source))
                    .collect();
                let parameters: Vec<_> = error
                    .parameters()
                    .iter()
                    .map(|parameter| FfiStringMapEntry {
                        key: unsafe { KernelStringSlice::new_unsafe(parameter.name()) },
                        value: unsafe { KernelStringSlice::new_unsafe(parameter.value()) },
                    })
                    .collect();
                let descriptor = DeltaErrorInfo {
                    condition: unsafe { KernelStringSlice::new_unsafe(error.name()) },
                    sql_state: error
                        .sql_state()
                        .map(|state| kernel_string_slice!(state))
                        .into(),
                    parameters: unsafe { FfiSlice::new_unsafe(&parameters) },
                    message: kernel_string_slice!(message),
                    backtrace: backtrace
                        .as_deref()
                        .map(|trace| kernel_string_slice!(trace))
                        .into(),
                    sources: unsafe { FfiSlice::new_unsafe(&source_slices) },
                };
                unsafe { (self.allocate_delta_error)(self.context, &descriptor) }
            }
            error => unsafe { self.allocate_kernel_error(KernelError::generic(error)) },
        }
    }
}

fn source_messages(error: &dyn StdError) -> Vec<String> {
    let mut sources = Vec::new();
    let mut source = error.source();
    while let Some(cause) = source {
        sources.push(cause.to_string());
        source = cause.source();
    }
    sources
}

#[cfg(test)]
mod tests {
    use std::backtrace::{Backtrace, BacktraceStatus};
    use std::ffi::c_void;
    use std::process::Command;
    use std::sync::Arc;

    use delta_kernel::{DeltaError, DeltaErrorCondition, Engine, Error, KernelError};
    use rstest::rstest;

    use super::{DeltaErrorInfo, ErrorAllocators, KernelErrorInfo};
    use crate::error::{AllocateError, ExternResult, IntoExternResult};
    use crate::{
        ExternEngine, KernelStringSlice, OptionalValue, allocate_kernel_string, kernel_string_slice,
    };

    const DIAGNOSTICS_CHECK_COMPLETE: &str = "FFI diagnostics checked";

    #[derive(Default)]
    struct Capture {
        fields: Vec<(String, String)>,
        backtrace: Option<String>,
        sources: Vec<String>,
        calls: usize,
        fail: bool,
    }

    unsafe fn capture(context: *mut c_void, fields: Vec<(String, String)>) -> *mut c_void {
        let output = unsafe { &mut *context.cast::<Capture>() };
        output.fields = fields;
        output.calls += 1;
        if output.fail {
            std::ptr::null_mut()
        } else {
            context
        }
    }

    unsafe extern "C" fn kernel(
        context: *mut c_void,
        error: *const KernelErrorInfo,
    ) -> *mut c_void {
        let error = unsafe { &*error };
        let output = unsafe { &mut *context.cast::<Capture>() };
        output.sources = unsafe { error.sources.try_as_slice().unwrap() }
            .iter()
            .map(|source| unsafe { source.try_to_string().unwrap() })
            .collect();
        unsafe {
            capture(
                context,
                vec![
                    ("code".into(), format!("{:?}", error.code)),
                    ("message".into(), error.message.try_to_string().unwrap()),
                ],
            )
        }
    }

    unsafe extern "C" fn delta(context: *mut c_void, error: *const DeltaErrorInfo) -> *mut c_void {
        let error = unsafe { &*error };
        let mut fields = unsafe {
            vec![
                ("condition".into(), error.condition.try_to_string().unwrap()),
                (
                    "sql_state".into(),
                    match &error.sql_state {
                        OptionalValue::Some(value) => value.try_to_string().unwrap(),
                        OptionalValue::None => String::new(),
                    },
                ),
            ]
        };
        for parameter in unsafe { error.parameters.try_as_slice().unwrap() } {
            fields.push(unsafe {
                (
                    parameter.key.try_to_string().unwrap(),
                    parameter.value.try_to_string().unwrap(),
                )
            });
        }
        fields.push(("message".into(), unsafe {
            error.message.try_to_string().unwrap()
        }));
        let output = unsafe { &mut *context.cast::<Capture>() };
        output.backtrace = match &error.backtrace {
            OptionalValue::Some(trace) => Some(unsafe { trace.try_to_string().unwrap() }),
            OptionalValue::None => None,
        };
        output.sources = unsafe { error.sources.try_as_slice().unwrap() }
            .iter()
            .map(|source| unsafe { source.try_to_string().unwrap() })
            .collect();
        unsafe { capture(context, fields) }
    }

    struct AllocationOnlyEngine {
        allocators: ErrorAllocators,
    }

    impl ExternEngine for AllocationOnlyEngine {
        fn engine(&self) -> Arc<dyn Engine> {
            panic!("error conversion does not access the engine")
        }

        fn error_allocator(&self) -> &dyn AllocateError {
            &self.allocators
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "Miri cannot spawn subprocesses; this test executes no unsafe code"
    )]
    fn delta_backtrace_environment_settings() {
        for (lib_backtrace, backtrace, captured) in [
            (None, None, false),
            (None, Some("1"), true),
            (Some("0"), Some("1"), false),
            (Some("1"), Some("0"), true),
        ] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "error::allocation::tests::allocation_copies_backtrace_and_source_chain::",
                    "--nocapture",
                ])
                .env_remove("RUST_LIB_BACKTRACE")
                .env_remove("RUST_BACKTRACE")
                .env("DELTA_KERNEL_TEST_CAPTURE_BACKTRACE", captured.to_string());
            if let Some(value) = lib_backtrace {
                command.env("RUST_LIB_BACKTRACE", value);
            }
            if let Some(value) = backtrace {
                command.env("RUST_BACKTRACE", value);
            }
            let output = command.output().unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "RUST_LIB_BACKTRACE={lib_backtrace:?}, RUST_BACKTRACE={backtrace:?}:\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr),
            );
            assert!(
                stdout.contains(DIAGNOSTICS_CHECK_COMPLETE),
                "child did not run the test: {stdout}"
            );
        }
    }

    #[derive(Clone, Copy)]
    enum EntryPoint {
        Direct,
        Kernel,
        KernelResult,
        Result,
        EngineResult,
    }

    #[rstest]
    #[case::kernel(None, EntryPoint::Direct)]
    #[case::kernel_helper(None, EntryPoint::Kernel)]
    #[case::kernel_result(None, EntryPoint::KernelResult)]
    #[case::result_kernel(None, EntryPoint::Result)]
    #[case::result_delta(Some(false), EntryPoint::Result)]
    #[case::result_delta_with_kernel_source(Some(true), EntryPoint::Result)]
    #[case::engine_result_kernel(None, EntryPoint::EngineResult)]
    #[case::engine_result_delta(Some(false), EntryPoint::EngineResult)]
    #[case::engine_result_delta_with_kernel_source(Some(true), EntryPoint::EngineResult)]
    #[case::delta(Some(false), EntryPoint::Direct)]
    #[case::delta_with_kernel_source(Some(true), EntryPoint::Direct)]
    fn dispatch_copies_diagnostics_and_preserves_callback_pointer(
        #[case] delta_source: Option<bool>,
        #[values(false, true)] fail: bool,
        #[case] entry_point: EntryPoint,
    ) {
        let mut output = Box::new(Capture {
            fail,
            ..Default::default()
        });
        let context = std::ptr::from_mut(output.as_mut()).cast();
        let allocators = ErrorAllocators {
            context,
            allocate_kernel_error: kernel,
            allocate_delta_error: delta,
        };
        let error = match delta_source {
            None => Error::Kernel(KernelError::MissingVersion(7)),
            Some(with_source) => DeltaError::for_test(
                DeltaErrorCondition::DeltaVersionsNotContiguous,
                &["10,12\0東京", "10", "12", "12"],
                with_source.then(|| Box::new(KernelError::MissingVersion(11)) as _),
            )
            .unwrap()
            .into(),
        };
        let expected: Vec<(String, String)> = match &error {
            Error::Kernel(error) => vec![
                ("code".into(), "MissingVersionError".into()),
                ("message".into(), error.to_string()),
            ],
            _ => [
                ("condition", "DELTA_VERSIONS_NOT_CONTIGUOUS"),
                ("sql_state", "KD00C"),
                ("versionList", "10,12\0東京"),
                ("startVersion", "10"),
                ("endVersion", "12"),
                ("versionToLoad", "12"),
                (
                    "message",
                    concat!(
                    "Versions (10,12\0東京) are not contiguous. \n",
                    "A gap in the delta log between versions 10 and 12 was detected while trying ",
                    "to load version 12."
                ),
                ),
            ]
            .map(|(name, value)| (name.into(), value.into()))
            .into(),
        };
        let expected_backtrace = match &error {
            Error::Delta(error) if error.backtrace().status() == BacktraceStatus::Captured => {
                Some(error.backtrace().to_string())
            }
            _ => None,
        };
        let result: ExternResult<()> = match (entry_point, error) {
            (EntryPoint::Kernel, Error::Kernel(error)) => {
                ExternResult::Err(unsafe { allocators.allocate_kernel_error(error) })
            }
            (EntryPoint::KernelResult, Error::Kernel(error)) => {
                let result: delta_kernel::KernelResult<()> = Err(error);
                unsafe { result.into_extern_result(&allocators) }
            }
            (EntryPoint::Result, error) => {
                let result: delta_kernel::Result<()> = Err(error);
                unsafe { result.into_extern_result(&allocators) }
            }
            (EntryPoint::EngineResult, error) => {
                let engine = AllocationOnlyEngine { allocators };
                let result: delta_kernel::Result<()> = Err(error);
                unsafe { result.into_extern_result(&&engine) }
            }
            (EntryPoint::Direct, error) => {
                ExternResult::Err(unsafe { allocators.allocate_error(error) })
            }
            _ => panic!("Kernel-only entry point requires a Kernel error"),
        };
        let ExternResult::Err(pointer) = result else {
            panic!("expected an error");
        };
        assert_eq!(pointer, if fail { std::ptr::null_mut() } else { context });
        assert_eq!(output.calls, 1);
        assert_eq!(output.fields, expected);
        assert_eq!(output.backtrace, expected_backtrace);
        let expected_sources = if delta_source == Some(true) {
            vec![KernelError::MissingVersion(11).to_string()]
        } else {
            Vec::new()
        };
        assert_eq!(output.sources, expected_sources);
    }

    #[rstest]
    #[case::kernel(false)]
    #[case::delta(true)]
    fn allocation_copies_backtrace_and_source_chain(
        #[case] delta_error: bool,
        #[values(false, true)] backtraced_source: bool,
        #[values(KernelError::MissingVersion(7), KernelError::EmptyLog)] root_cause: KernelError,
    ) {
        let mut output = Capture::default();
        let allocators = ErrorAllocators {
            context: std::ptr::from_mut(&mut output).cast(),
            allocate_kernel_error: kernel,
            allocate_delta_error: delta,
        };
        let root_message = root_cause.to_string();
        let source = KernelError::GenericError {
            source: Box::new(root_cause),
        };
        let mut expected_sources = vec![source.to_string(), root_message];
        let source = if backtraced_source {
            let source = KernelError::Backtraced {
                source: Box::new(source),
                backtrace: Box::new(Backtrace::capture()),
            };
            expected_sources.insert(0, source.to_string());
            source
        } else {
            source
        };
        let expected_message = if delta_error {
            DeltaError::path_does_not_exist("memory:///table/", None).message()
        } else {
            format!("Generic error: {source}")
        };
        let error = if delta_error {
            DeltaError::path_does_not_exist("memory:///table/", Some(Box::new(source))).into()
        } else {
            KernelError::GenericError {
                source: Box::new(source),
            }
            .into()
        };
        let expected_backtrace = if let Error::Delta(error) = &error {
            let captured = error.backtrace().status() == BacktraceStatus::Captured;
            if let Ok(expected) = std::env::var("DELTA_KERNEL_TEST_CAPTURE_BACKTRACE") {
                assert_eq!(captured, expected.parse::<bool>().unwrap());
            }
            captured.then(|| error.backtrace().to_string())
        } else {
            None
        };
        unsafe { allocators.allocate_error(error) };
        assert_eq!(output.backtrace, expected_backtrace);
        assert_eq!(output.sources, expected_sources);
        assert_eq!(output.fields.last().unwrap().1, expected_message);
        println!("{DIAGNOSTICS_CHECK_COMPLETE}");
    }
    #[rstest]
    fn successful_result_does_not_allocate(#[values(false, true)] top_level: bool) {
        let mut output = Capture::default();
        let allocators = ErrorAllocators {
            context: std::ptr::from_mut(&mut output).cast(),
            allocate_kernel_error: kernel,
            allocate_delta_error: delta,
        };
        let result = if top_level {
            let result: delta_kernel::Result<_> = Ok(7);
            unsafe { result.into_extern_result(&allocators) }
        } else {
            let result: delta_kernel::KernelResult<_> = Ok(7);
            unsafe { result.into_extern_result(&allocators) }
        };
        assert!(matches!(result, ExternResult::Ok(7)));
        assert_eq!(output.calls, 0);
    }
    #[test]
    fn standalone_export_forwards_context_and_returns_callback_pointer() {
        let mut output = Capture::default();
        let context = std::ptr::from_mut(&mut output).cast();
        let allocators = ErrorAllocators {
            context,
            allocate_kernel_error: kernel,
            allocate_delta_error: delta,
        };
        let bytes = [0xff];
        let invalid_utf8 = KernelStringSlice {
            ptr: bytes.as_ptr().cast(),
            len: bytes.len(),
        };
        let result = unsafe { allocate_kernel_string(invalid_utf8, &allocators) };
        let ExternResult::Err(pointer) = result else {
            panic!("expected invalid UTF-8 error")
        };
        assert_eq!(pointer, context);
        assert_eq!(output.calls, 1);
        assert_eq!(output.fields[0], ("code".into(), "Utf8Error".into()));
    }

    #[cfg(feature = "default-engine-base")]
    #[test]
    fn builder_retains_callback_table_after_registration_returns() {
        let mut output = Capture::default();
        let context = std::ptr::from_mut(&mut output).cast();
        let path = "memory:///";
        let value = "value";
        let builder = {
            let allocators = ErrorAllocators {
                context,
                allocate_kernel_error: kernel,
                allocate_delta_error: delta,
            };
            let result =
                unsafe { crate::get_engine_builder(kernel_string_slice!(path), &allocators) };
            let ExternResult::Ok(builder) = result else {
                panic!("expected a builder")
            };
            builder
        };
        let bytes = [0xff];
        let invalid_utf8 = KernelStringSlice {
            ptr: bytes.as_ptr().cast(),
            len: bytes.len(),
        };
        let result = unsafe {
            crate::builder_with_option(builder, invalid_utf8, kernel_string_slice!(value))
        };
        let ExternResult::Err(pointer) = result else {
            panic!("expected invalid UTF-8 error")
        };
        assert_eq!(pointer, context);
        assert_eq!(output.calls, 1);
        assert_eq!(output.fields[0], ("code".into(), "Utf8Error".into()));
    }
}
