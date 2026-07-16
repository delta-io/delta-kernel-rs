//! Cooperative cancellation for long-running Kernel reads.
//!
//! Kernel never does I/O itself and owns no async runtime, so cancellation is *cooperative* and
//! *runtime-agnostic*: a caller supplies a [`CancellationToken`] (via
//! [`ScanBuilder::with_cancellation_token`](crate::scan::ScanBuilder::with_cancellation_token)),
//! Kernel polls it at action-batch boundaries, and cancellation-aware [`Engine`](crate::Engine)
//! reads may race their I/O against it. Cancellation is always surfaced as
//! [`Error::Cancelled`] — never as normal iterator exhaustion — so a partial listing can never be
//! mistaken for a complete one.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::{DeltaResult, Error};

/// A shared, thread-safe cancellation token. Held as an `Arc` because the lazy scan iterator and
/// the engine reads it drives can outlive the builder call and run on other threads.
pub type CancellationTokenRef = Arc<dyn CancellationToken>;

/// A future that resolves when a [`CancellationToken`] is cancelled. Runtime-neutral: it is a
/// plain boxed [`Future`], so an engine can `select!` it against its own async reads without
/// Kernel taking on any async-runtime dependency.
pub type CancelledFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Returns `Err(Error::Cancelled)` if `token` is present and already cancelled, else `Ok(())`.
///
/// Used to fail fast before starting a setup/read operation (e.g. a footer read or a sidecar
/// listing) so cancelled work is not begun.
pub(crate) fn check_cancelled(token: Option<&CancellationTokenRef>) -> DeltaResult<()> {
    match token {
        Some(t) if t.is_cancelled() => Err(Error::Cancelled),
        _ => Ok(()),
    }
}

/// A cooperative cancellation signal supplied by a caller.
///
/// Implementors wrap whatever their runtime provides (e.g. `tokio_util::sync::CancellationToken`).
/// Kernel and cancellation-aware engines only *consume* it: Kernel polls [`is_cancelled`] between
/// action batches, while an async engine may await [`cancelled`] to wake blocked I/O.
///
/// [`is_cancelled`]: CancellationToken::is_cancelled
/// [`cancelled`]: CancellationToken::cancelled
pub trait CancellationToken: Send + Sync {
    /// Returns `true` once cancellation has been requested. Cheap, synchronous, and monotonic:
    /// once it returns `true` it must never return `false` again.
    fn is_cancelled(&self) -> bool;

    /// Returns a future that resolves when the token is cancelled (immediately if it already is).
    ///
    /// There is no default implementation: a correct notification cannot be synthesized from
    /// [`is_cancelled`](Self::is_cancelled) alone without either busy-polling or a runtime, and
    /// Kernel has neither. Implementors back this with their own notification primitive.
    fn cancelled(&self) -> CancelledFuture<'_>;
}

/// Wraps a fallible iterator so that cancellation terminates it with a single
/// [`Error::Cancelled`] rather than silent truncation.
///
/// Before each pull, the token is polled: if cancelled, one `Err(Error::Cancelled)` is yielded
/// and every subsequent call returns `None` (the iterator is fused). With no token, or before
/// cancellation, items pass through unchanged. This is deliberately **not** `take_while`, which
/// would end the iterator with `None` and make a cancelled listing look complete.
pub(crate) struct CancellableIterator<I> {
    inner: I,
    token: Option<CancellationTokenRef>,
    /// Set once cancellation has been observed and the terminal error emitted, fusing the
    /// iterator to `None` thereafter.
    done: bool,
}

impl<I> CancellableIterator<I> {
    pub(crate) fn new(inner: I, token: Option<CancellationTokenRef>) -> Self {
        Self {
            inner,
            token,
            done: false,
        }
    }
}

impl<I, T> Iterator for CancellableIterator<I>
where
    I: Iterator<Item = DeltaResult<T>>,
{
    type Item = DeltaResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.token.as_ref().is_some_and(|t| t.is_cancelled()) {
            self.done = true;
            return Some(Err(Error::Cancelled));
        }
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Minimal [`CancellationToken`] backed by an [`AtomicBool`], for tests.
    #[derive(Default)]
    struct TestToken(AtomicBool);

    impl TestToken {
        fn cancel(&self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl CancellationToken for TestToken {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
        fn cancelled(&self) -> CancelledFuture<'_> {
            // Tests only drive `is_cancelled`; a resolved/pending future is enough here.
            Box::pin(ready(()))
        }
    }

    fn ok_iter(n: usize) -> impl Iterator<Item = DeltaResult<usize>> {
        (0..n).map(Ok)
    }

    #[test]
    fn no_token_passes_through_unchanged() {
        let out: Vec<_> = CancellableIterator::new(ok_iter(3), None)
            .map(Result::unwrap)
            .collect();
        assert_eq!(out, vec![0, 1, 2]);
    }

    #[test]
    fn uncancelled_token_passes_through_unchanged() {
        let token: CancellationTokenRef = Arc::new(TestToken::default());
        let out: Vec<_> = CancellableIterator::new(ok_iter(3), Some(token))
            .map(Result::unwrap)
            .collect();
        assert_eq!(out, vec![0, 1, 2]);
    }

    #[test]
    fn pre_cancelled_yields_one_error_then_ends() {
        let token = Arc::new(TestToken::default());
        token.cancel();
        let mut iter = CancellableIterator::new(ok_iter(3), Some(token as CancellationTokenRef));
        assert!(matches!(iter.next(), Some(Err(Error::Cancelled))));
        // Fused: never a `Some(Ok(..))` after cancellation, and no infinite error stream.
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn mid_stream_cancellation_yields_error_not_silent_truncation() {
        let token = Arc::new(TestToken::default());
        let ct: CancellationTokenRef = token.clone();
        let mut iter = CancellableIterator::new(ok_iter(5), Some(ct));
        assert!(matches!(iter.next(), Some(Ok(0))));
        assert!(matches!(iter.next(), Some(Ok(1))));
        token.cancel();
        // The terminal item is an error, so a cancelled listing can't look complete (which a
        // bare `None` / `take_while` would).
        assert!(matches!(iter.next(), Some(Err(Error::Cancelled))));
        assert!(iter.next().is_none());
    }

    #[test]
    fn check_cancelled_reports_state() {
        let token = Arc::new(TestToken::default());
        let ct: CancellationTokenRef = token.clone();
        assert!(check_cancelled(Some(&ct)).is_ok());
        assert!(check_cancelled(None).is_ok());
        token.cancel();
        assert!(matches!(check_cancelled(Some(&ct)), Err(Error::Cancelled)));
    }
}
