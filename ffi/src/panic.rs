//! Host callbacks for Rust panic diagnostics captured before unwinding.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::ffi::c_void;
use std::panic::PanicHookInfo;

use crate::{KernelStringSlice, OptionalValue, kernel_string_slice};

/// Diagnostics borrowed for one panic callback on the panicking thread.
///
/// The descriptor and all referenced strings expire when the callback returns. Hosts must copy
/// retained values. Diagnostic text is unredacted and may contain user data.
#[repr(C)]
pub struct PanicInfo {
    /// Panic payload text, or `non-string panic payload` for other payload types.
    pub message: KernelStringSlice,
    /// Original Rust backtrace, absent only when the platform does not support capture.
    pub backtrace: OptionalValue<KernelStringSlice>,
    /// Source file containing the panic, absent when no location is available.
    pub file: OptionalValue<KernelStringSlice>,
    /// One-based source line, or zero when no location is available.
    pub line: u32,
    /// One-based source column, or zero when no location is available.
    pub column: u32,
}

/// A host receiver retained until process exit after successful registration.
///
/// Calls run synchronously on panicking threads and may overlap. The callback and context must
/// remain valid until process exit, and the native library must remain loaded. The receiver must
/// not unwind, re-enter Kernel, or wait on locks that the panicking thread may hold. It must finish
/// reporting before it returns if the diagnostic needs to survive a fatal panic.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PanicReporter {
    /// Host state passed unchanged to the callback; NULL is allowed if unused.
    pub context: *mut c_void,
    /// Receives a non-null descriptor borrowed until the callback returns.
    pub report: unsafe extern "C" fn(*mut c_void, *const PanicInfo),
}

// SAFETY: Copying or sharing the table does not access its context. Registration requires the
// host to guarantee callback lifetime and concurrency safety for every subsequent invocation.
unsafe impl Send for PanicReporter {}
unsafe impl Sync for PanicReporter {}

/// Installs a host receiver for Rust panics before unwinding or abort handling.
///
/// Call during application initialization, before starting native work. Kernel copies the table;
/// the table pointer is borrowed only for this call. A successful registration is permanent.
/// The receiver runs before the existing panic hook, regardless of backtrace environment settings.
/// It reports all panics using this Rust runtime, including native worker panics.
///
/// Returns `false` for NULL, a repeated registration, or a caller that is currently unwinding.
/// Reporting does not catch panics or change their termination behavior.
///
/// # Safety
/// A non-null pointer must reference an aligned, initialized [`PanicReporter`] for this call.
/// On success, its callback and context must satisfy [`PanicReporter`]'s lifetime and concurrency
/// requirements. The callback must uphold [`PanicInfo`]'s borrowing contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enable_panic_reporting(reporter: *const PanicReporter) -> bool {
    let Some(reporter) = (unsafe { reporter.as_ref() }) else {
        return false;
    };
    let reporter = *reporter;
    delta_kernel::panic::install_panic_reporter(move |info, backtrace| unsafe {
        reporter.report_panic(info, backtrace);
    })
}

impl PanicReporter {
    unsafe fn report_panic(&self, info: &PanicHookInfo<'_>, backtrace: &Backtrace) {
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        let backtrace =
            (backtrace.status() == BacktraceStatus::Captured).then(|| backtrace.to_string());
        let location = info.location();
        let descriptor = PanicInfo {
            message: kernel_string_slice!(message),
            backtrace: backtrace
                .as_deref()
                .map(|trace| kernel_string_slice!(trace))
                .into(),
            file: location
                .map(|location| {
                    let file = location.file();
                    kernel_string_slice!(file)
                })
                .into(),
            line: location.map_or(0, |location| location.line()),
            column: location.map_or(0, |location| location.column()),
        };
        unsafe { (self.report)(self.context, &descriptor) };
    }
}

#[cfg(test)]
mod tests {
    use std::backtrace::Backtrace;
    use std::ffi::c_void;
    use std::io::{self, Write};
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use std::{panic, thread};

    use rstest::rstest;

    use super::{PanicInfo, PanicReporter, enable_panic_reporting};
    use crate::{KernelStringSlice, OptionalValue};

    const CHILD_MODE: &str = "DELTA_KERNEL_FFI_PANIC_REPORTER_TEST_MODE";

    #[derive(Default)]
    struct Capture {
        message: String,
        backtrace: Option<String>,
        file: Option<String>,
        line: u32,
        column: u32,
    }

    #[rstest]
    #[cfg_attr(miri, ignore = "Miri cannot spawn subprocesses")]
    #[case::caller("caller")]
    #[case::worker("worker")]
    #[case::ffi_abort("ffi_abort")]
    #[case::non_string("non_string")]
    fn host_receives_the_original_panic_on_its_thread(#[case] mode: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "panic::tests::panic_reporter_subprocess",
                "--nocapture",
            ])
            .env(CHILD_MODE, mode)
            .env("RUST_LIB_BACKTRACE", "0")
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.success(),
            mode != "ffi_abort",
            "{stdout}\n{stderr}"
        );
        let message = if mode == "non_string" {
            "non-string panic payload"
        } else {
            "ffi panic diagnostic"
        };
        assert!(stdout.contains(message), "{stdout}");
        assert!(stdout.contains("panic_at_origin"), "{stdout}");
    }

    #[test]
    fn callback_copies_borrowed_payload_and_location_without_a_backtrace() {
        assert!(!unsafe { enable_panic_reporting(std::ptr::null()) });
        let capture = Mutex::new(Capture::default());
        let reporter = PanicReporter {
            context: std::ptr::from_ref(&capture).cast_mut().cast(),
            report: report_panic,
        };
        let origin_thread = thread::current().id();
        let previous = Arc::new(panic::take_hook());
        let chained = previous.clone();
        panic::set_hook(Box::new(move |info| {
            if thread::current().id() == origin_thread {
                unsafe { reporter.report_panic(info, &Backtrace::disabled()) };
            }
            chained(info);
        }));
        let result = panic::catch_unwind(|| panic::panic_any(String::from("borrowed\0東京")));
        panic::set_hook(Box::new(move |info| previous(info)));
        drop(result);
        let capture = capture.lock().unwrap();
        assert_eq!(capture.message, "borrowed\0東京");
        assert!(capture.backtrace.is_none());
        assert!(capture.file.as_ref().unwrap().ends_with("ffi/src/panic.rs"));
        assert!(capture.line > 0);
        assert!(capture.column > 0);
    }

    #[test]
    fn panic_reporter_subprocess() {
        let Ok(mode) = std::env::var(CHILD_MODE) else {
            return;
        };
        panic::set_hook(Box::new(|_| {}));
        assert!(!unsafe { enable_panic_reporting(std::ptr::null()) });
        let capture: &'static Mutex<Capture> = Box::leak(Box::new(Mutex::new(Capture::default())));
        {
            let reporter = PanicReporter {
                context: std::ptr::from_ref(capture).cast_mut().cast(),
                report: report_panic,
            };
            assert!(unsafe { enable_panic_reporting(&reporter) });
            assert!(!unsafe { enable_panic_reporting(&reporter) });
        }
        let non_string = mode == "non_string";
        if mode == "worker" {
            assert!(thread::spawn(|| panic_at_origin(false)).join().is_err());
        } else if mode == "ffi_abort" {
            panic_over_c_boundary();
        } else {
            assert!(panic::catch_unwind(|| panic_at_origin(non_string)).is_err());
        }
        let capture = capture.lock().unwrap();
        assert!(capture.file.as_ref().unwrap().ends_with("ffi/src/panic.rs"));
        assert!(capture.line > 0);
        assert!(capture.column > 0);
        assert!(
            capture
                .backtrace
                .as_ref()
                .unwrap()
                .contains("panic_at_origin")
        );
        assert_eq!(
            capture.message,
            if non_string {
                "non-string panic payload"
            } else {
                "ffi panic diagnostic"
            }
        );
    }

    unsafe extern "C" fn report_panic(context: *mut c_void, info: *const PanicInfo) {
        let info = unsafe { &*info };
        let capture = unsafe { &*context.cast::<Mutex<Capture>>() };
        let mut capture = capture.lock().unwrap();
        capture.message = unsafe { info.message.try_to_string().unwrap() };
        capture.backtrace = unsafe { copy_optional(&info.backtrace) };
        capture.file = unsafe { copy_optional(&info.file) };
        capture.line = info.line;
        capture.column = info.column;
        println!(
            "{}\n{}",
            capture.message,
            capture.backtrace.as_deref().unwrap_or("")
        );
        io::stdout().flush().unwrap();
    }

    unsafe fn copy_optional(value: &OptionalValue<KernelStringSlice>) -> Option<String> {
        match value {
            OptionalValue::Some(value) => Some(unsafe { value.try_to_string().unwrap() }),
            OptionalValue::None => None,
        }
    }

    extern "C" fn panic_over_c_boundary() {
        panic_at_origin(false);
    }

    #[inline(never)]
    fn panic_at_origin(non_string: bool) {
        if non_string {
            panic::panic_any(7_usize);
        }
        panic!("ffi panic diagnostic");
    }
}
