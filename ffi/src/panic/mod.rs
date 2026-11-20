//! Panic diagnostics and containment for fallible, operation-local FFI work.
//!
//! The hook is installed once for this Rust runtime. It captures the original stack only while
//! a boundary is active on the panicking thread, and preserves the previous hook. Reporting
//! uses the existing tracing subscriber and error allocator after unwinding, without an engine
//! callback from inside the hook.

use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind, take_hook};
use std::sync::Once;

use delta_kernel::KernelError;

/// Logs and aborts when an entry point cannot return a panic through its C signature.
pub(crate) fn catch_or_abort<T>(operation: &'static str, f: impl FnOnce() -> T) -> T {
    match catch::<_, KernelError>(operation, || Ok(f())) {
        Ok(result) => result,
        Err(_) => std::process::abort(),
    }
}

/// Contains an unwinding panic before it reaches the enclosing C export.
///
/// Callers must keep all fallible work inside `f`, including serialization. Consumed handles must
/// become Rust owners before any work that can panic; unwind then drops those owners. Shared
/// inputs must remain valid after unwinding, and partially mutated objects must not be reused.
/// This assertion is specific to each call site, rather than a promise of rollback by this helper.
///
/// Nested C entries need their own boundary. Foreign exceptions, allocator aborts, aborting
/// panics, and panics during unwind are not caught. A failure while reporting a caught panic
/// aborts.
pub(crate) fn catch<T, E>(operation: &'static str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
where
    E: From<KernelError>,
{
    install_hook();
    let scope = CaptureScope::enter();
    // Each call site owns its in-progress work and discards it on failure. Requiring UnwindSafe
    // would exclude engine trait objects without proving the necessary ownership invariants.
    let result = catch_unwind(AssertUnwindSafe(f));
    let frame = FRAME.take();
    drop(scope);
    match result {
        Ok(result) => result,
        Err(payload) => {
            let abort = AbortOnDrop;
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            let location = frame
                .as_ref()
                .and_then(|frame| frame.location.as_deref())
                .unwrap_or("unknown location");
            let source = KernelError::InternalError(format!(
                "Rust panic in {operation} at {location}: {message}"
            ));
            let error = match frame.and_then(|frame| frame.backtrace) {
                Some(backtrace) => KernelError::Backtraced {
                    source: Box::new(source),
                    backtrace,
                },
                None => source,
            };
            drop(payload);
            tracing::error!("{error}");
            let error = error.into();
            std::mem::forget(abort);
            Err(error)
        }
    }
}

#[derive(Default)]
struct Frame {
    location: Option<String>,
    backtrace: Option<Box<Backtrace>>,
}

thread_local! {
    static FRAME: RefCell<Option<Frame>> = const { RefCell::new(None) };
}

static INSTALL_HOOK: Once = Once::new();

fn install_hook() {
    INSTALL_HOOK.call_once(|| {
        let previous = take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = FRAME.try_with(|frame| {
                let Ok(mut frame) = frame.try_borrow_mut() else {
                    return;
                };
                let Some(frame) = frame.as_mut() else {
                    return;
                };
                *frame = Frame {
                    location: Some(
                        info.location()
                            .map(|loc| loc.to_string())
                            .unwrap_or_default(),
                    ),
                    backtrace: Some(Box::new(Backtrace::force_capture())),
                };
            });
            previous(info);
        }));
    });
}

struct CaptureScope(Option<Frame>);

impl CaptureScope {
    fn enter() -> Self {
        Self(FRAME.replace(Some(Frame::default())))
    }
}

impl Drop for CaptureScope {
    fn drop(&mut self) {
        FRAME.set(self.0.take());
    }
}

// A second panic during diagnostic formatting, payload destruction, or tracing cannot be allowed
// to escape the C boundary or recursively invoke the same reporting machinery.
struct AbortOnDrop;

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        std::process::abort();
    }
}

#[cfg(test)]
mod tests;
