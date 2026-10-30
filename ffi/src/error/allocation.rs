//! Outgoing Kernel/Delta allocation for registered engines.
//!
//! Engine errors and native causes require a separate ABI change.

use std::ffi::c_void;

use delta_kernel::Error;

use super::FFIKernelError;
use crate::delta_types::{FfiStringMap, FfiStringMapEntry};
use crate::{kernel_string_slice, FfiSlice, KernelStringSlice, OptionalValue};

/// An opaque connector-owned allocation. Kernel neither reads nor frees it.
pub struct ConnectorError {
    _private: (),
}

/// Borrowed Kernel classification and diagnostics, valid only during the allocation callback.
#[repr(C)]
pub struct KernelErrorAllocation {
    /// Existing numeric Kernel classification.
    pub code: FFIKernelError,
    /// Rendered diagnostic message, without a trailing NUL.
    pub message: KernelStringSlice,
}

/// Borrowed catalog diagnostics, valid only during the allocation callback.
///
/// Classify by condition/SQLSTATE. Values and message are unredacted diagnostics; Rust sources and
/// backtraces are excluded. The parameter array and every referenced UTF-8 string are borrowed.
#[repr(C)]
pub struct DeltaErrorAllocation {
    /// Stable catalog condition.
    pub condition: KernelStringSlice,
    /// Catalog SQLSTATE, absent when undefined.
    pub sql_state: OptionalValue<KernelStringSlice>,
    /// Named string parameters in catalog template order.
    pub parameters: FfiStringMap,
    /// Rendered catalog message.
    pub message: KernelStringSlice,
}

/// Connector context and required callbacks for structured Kernel/Delta allocation.
///
/// Callbacks copy retained descriptor fields and return allocations the connector frees, or NULL
/// on allocation failure. They must not unwind. Context and callback code must remain valid for
/// every use and support concurrent calls if shared across threads. Null context is permitted.
/// Engine-error allocation and native causes require a separate ABI change.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ErrorAllocators {
    /// Connector state forwarded unchanged to the selected callback.
    pub context: *mut c_void,
    /// Allocates a Kernel error from a borrowed descriptor.
    pub allocate_kernel_error:
        unsafe extern "C" fn(*mut c_void, *const KernelErrorAllocation) -> *mut ConnectorError,
    /// Allocates a Delta error from a borrowed descriptor.
    pub allocate_delta_error:
        unsafe extern "C" fn(*mut c_void, *const DeltaErrorAllocation) -> *mut ConnectorError,
}

// Safety: every invocation is unsafe and requires the context and callbacks to support concurrent
// use. Copying the table neither accesses nor transfers ownership of the context.
unsafe impl Send for ErrorAllocators {}
unsafe impl Sync for ErrorAllocators {}

impl super::AllocateError for ErrorAllocators {
    unsafe fn allocate_error(
        &self,
        code: FFIKernelError,
        message: KernelStringSlice,
    ) -> *mut super::EngineError {
        let descriptor = KernelErrorAllocation { code, message };
        unsafe { (self.allocate_kernel_error)(self.context, &descriptor).cast() }
    }
}

impl super::AllocateResultError for ErrorAllocators {
    unsafe fn allocate_result_error(&self, error: Error) -> *mut super::EngineError {
        unsafe { self.allocate_error(error).cast() }
    }
}

impl ErrorAllocators {
    /// Allocates `error` by its outer variant, returning the connector pointer unchanged, even
    /// NULL.
    ///
    /// A Delta error retaining a Kernel source still invokes only the Delta callback. Descriptors
    /// and their backing storage stay valid until that callback returns. Call only for failures;
    /// successful operations must bypass allocation.
    ///
    /// # Safety
    ///
    /// Context and callbacks must satisfy [`ErrorAllocators`]'s lifetime and concurrency contract.
    pub unsafe fn allocate_error(&self, error: Error) -> *mut ConnectorError {
        error.dispatch(
            |error| {
                let message = error.to_string();
                let descriptor = KernelErrorAllocation {
                    code: error.into(),
                    message: kernel_string_slice!(message),
                };
                unsafe { (self.allocate_kernel_error)(self.context, &descriptor) }
            },
            |error| {
                let message = error.message();
                let parameters: Vec<_> = error
                    .parameters()
                    .iter()
                    .map(|parameter| FfiStringMapEntry {
                        key: unsafe { KernelStringSlice::new_unsafe(parameter.name()) },
                        value: unsafe { KernelStringSlice::new_unsafe(parameter.value()) },
                    })
                    .collect();
                let descriptor = DeltaErrorAllocation {
                    condition: unsafe { KernelStringSlice::new_unsafe(error.name()) },
                    sql_state: error
                        .sql_state()
                        .map(|state| kernel_string_slice!(state))
                        .into(),
                    parameters: unsafe { FfiSlice::new_unsafe(&parameters) },
                    message: kernel_string_slice!(message),
                };
                unsafe { (self.allocate_delta_error)(self.context, &descriptor) }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use delta_kernel::{DeltaError, DeltaErrorCondition, Error, KernelError};
    use rstest::rstest;

    use super::{ConnectorError, DeltaErrorAllocation, ErrorAllocators, KernelErrorAllocation};
    use crate::OptionalValue;

    #[derive(Default)]
    struct Capture {
        fields: Vec<(String, String)>,
        calls: usize,
        fail: bool,
    }

    unsafe fn capture(context: *mut c_void, fields: Vec<(String, String)>) -> *mut ConnectorError {
        let output = unsafe { &mut *context.cast::<Capture>() };
        output.fields = fields;
        output.calls += 1;
        if output.fail {
            std::ptr::null_mut()
        } else {
            context.cast()
        }
    }

    unsafe extern "C" fn kernel(
        context: *mut c_void,
        error: *const KernelErrorAllocation,
    ) -> *mut ConnectorError {
        let error = unsafe { &*error };
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

    unsafe extern "C" fn delta(
        context: *mut c_void,
        error: *const DeltaErrorAllocation,
    ) -> *mut ConnectorError {
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
        unsafe { capture(context, fields) }
    }

    #[rstest]
    #[case::kernel(None)]
    #[case::delta(Some(false))]
    #[case::delta_with_kernel_source(Some(true))]
    fn dispatch_copies_diagnostics_and_preserves_callback_pointer(
        #[case] delta_source: Option<bool>,
        #[values(false, true)] fail: bool,
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
        let pointer = unsafe { allocators.allocate_error(error) };
        assert_eq!(
            pointer,
            if fail {
                std::ptr::null_mut()
            } else {
                context.cast()
            }
        );
        assert_eq!(output.calls, 1);
        assert_eq!(output.fields, expected);
    }
    #[cfg(feature = "default-engine-base")]
    #[test]
    fn absent_snapshot_dispatches_condition_and_parameters_through_registered_engine() {
        let mut output = Box::<Capture>::default();
        let context = std::ptr::from_mut(output.as_mut()).cast();
        let engine = delta_kernel_default_engine::DefaultEngineBuilder::new(std::sync::Arc::new(
            delta_kernel::object_store::memory::InMemory::new(),
        ))
        .build();
        let handle = crate::engine_to_handle(
            std::sync::Arc::new(engine),
            ErrorAllocators {
                context,
                allocate_kernel_error: kernel,
                allocate_delta_error: delta,
            },
        );
        let table_path = "memory:///";
        let builder = unsafe {
            crate::get_snapshot_builder(
                crate::kernel_string_slice!(table_path),
                handle.shallow_copy(),
            )
        };
        let crate::ExternResult::Ok(builder) = builder else {
            panic!("valid table URL must produce a builder")
        };
        assert_eq!(output.calls, 0);
        output.fail = true;
        let result = unsafe { crate::snapshot_builder_build(builder) };
        assert!(matches!(result, crate::ExternResult::Err(pointer) if pointer.is_null()));
        assert_eq!(output.calls, 1);
        assert_eq!(
            output.fields[0],
            ("condition".into(), "DELTA_PATH_DOES_NOT_EXIST".into())
        );
        assert_eq!(output.fields[1], ("sql_state".into(), "42K03".into()));
        assert_eq!(output.fields[2], ("path".into(), "memory:///".into()));
        unsafe { crate::free_engine(handle) };
    }
}
