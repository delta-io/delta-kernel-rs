//! Reporting Rust panics before their original stack is unwound.

use std::backtrace::Backtrace;
use std::panic::{self, PanicHookInfo};
use std::sync::atomic::{AtomicBool, Ordering};

/// Installs a process-wide reporter that captures panic diagnostics before unwinding.
///
/// The reporter receives the original panic and a backtrace captured regardless of Rust's
/// backtrace environment variables. It runs synchronously on the panicking thread, including
/// worker threads, before the previously installed hook. Reporting does not catch the panic or
/// change whether it unwinds or aborts.
///
/// Call once during application initialization, before starting concurrent native work. The
/// reporter remains installed for the library's lifetime. Other users of the process-wide panic
/// hook must preserve this hook when replacing it. All panics using the same Rust runtime are
/// reported, including panics outside Kernel.
///
/// Calls to the reporter may overlap. It must not panic, re-enter Kernel, or wait on locks that
/// the panicking thread may hold. Diagnostic values are unredacted.
///
/// Returns `false` if a reporter is already installed or the caller is currently unwinding.
pub fn install_panic_reporter(
    reporter: impl Fn(&PanicHookInfo<'_>, &Backtrace) + Send + Sync + 'static,
) -> bool {
    if std::thread::panicking()
        || INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return false;
    }
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let backtrace = Backtrace::force_capture();
        reporter(info, &backtrace);
        previous(info);
    }));
    true
}

static INSTALLED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
mod tests {
    use std::backtrace::BacktraceStatus;
    use std::io::{self, Write};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{panic, thread};

    use rstest::rstest;

    use super::install_panic_reporter;

    const CHILD_MODE: &str = "DELTA_KERNEL_PANIC_REPORTER_TEST_MODE";

    #[rstest]
    #[cfg_attr(miri, ignore = "Miri cannot spawn subprocesses")]
    #[case::caller("caller")]
    #[case::worker("worker")]
    #[case::fatal_previous_hook("abort")]
    fn reporter_preserves_the_panic_origin_before_chaining(#[case] mode: &str) {
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
            mode != "abort",
            "{stdout}\n{stderr}"
        );
        assert!(stdout.contains("panic reporter original stack"), "{stdout}");
        assert!(stdout.contains("panic_at_origin"), "{stdout}");
        assert!(stdout.contains("kernel/src/panic.rs:"), "{stdout}");
        assert!(stdout.contains("previous hook"), "{stdout}");
    }

    #[test]
    fn panic_reporter_subprocess() {
        let Ok(mode) = std::env::var(CHILD_MODE) else {
            return;
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let previous_calls = calls.clone();
        let abort = mode == "abort";
        panic::set_hook(Box::new(move |_| {
            previous_calls.fetch_add(1, Ordering::Relaxed);
            assert!(!install_panic_reporter(|_, _| {}));
            println!("previous hook");
            io::stdout().flush().unwrap();
            if abort {
                std::process::abort();
            }
        }));
        assert!(install_panic_reporter(|info, backtrace| {
            assert_eq!(backtrace.status(), BacktraceStatus::Captured);
            println!("{info}\n{backtrace}");
            io::stdout().flush().unwrap();
        }));
        assert!(!install_panic_reporter(|_, _| {
            panic!("a second registration must not replace the reporter");
        }));
        if mode == "worker" {
            assert!(thread::spawn(panic_at_origin).join().is_err());
        } else {
            assert!(panic::catch_unwind(panic_at_origin).is_err());
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[inline(never)]
    fn panic_at_origin() {
        panic!("panic reporter original stack");
    }
}
