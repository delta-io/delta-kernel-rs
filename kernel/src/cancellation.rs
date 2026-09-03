//! Cooperative cancellation for long-running Kernel reads.
//!
//! Kernel never does I/O itself and owns no async runtime, so cancellation is *cooperative* and
//! *runtime-agnostic*: a caller supplies a [`CancellationToken`] and Kernel passes it to
//! cancellation-aware [`Engine`](crate::Engine) operations. Those operations own cancellation for
//! the I/O and iterators they produce. Kernel polls the token only for work that bypasses an
//! Engine handler, such as cached scan metadata.
//!
//! # Engine operation contract
//!
//! A cancellation-aware Engine operation must check the token before initiating I/O. If that check
//! reports cancellation, it must immediately fail with [`Error::Cancelled`]. Cancellation can race
//! with the check, and an I/O request that had already started may complete normally. This does not
//! permit draining an arbitrary prefetch queue before terminating.
//!
//! Iterator-producing operations should check the token before each pull that could initiate more
//! I/O, and terminate promptly when cancellation is reported. They must not initiate replacement or
//! additional I/O after such a check reports cancellation. They may still return data from I/O
//! that was already in flight, and may interrupt that I/O when the Engine supports it. If an
//! iterator stops early because of cancellation, it must surface [`Error::Cancelled`] rather than
//! normal exhaustion.
//!
//! A custom `*_with_cancellation` implementation replaces the provided implementation and owns
//! this contract itself. The provided iterator implementations check before delegating and before
//! each pull, but cannot interrupt work inside the delegated operation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::{AsAny, DeltaResult, Error};

/// A shared, thread-safe cancellation token. Held as an `Arc` because the lazy scan iterator and
/// the engine reads it drives can outlive the builder call and run on other threads.
pub type CancellationTokenRef = Arc<dyn CancellationToken>;

/// A future that resolves when a [`CancellationToken`] is cancelled. Runtime-neutral: it is a
/// plain boxed [`Future`], so an engine can `select!` it against its own async reads without
/// Kernel taking on any async-runtime dependency.
pub type CancelledFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Returns `Err(Error::Cancelled)` if `token` is present and already cancelled, else `Ok(())`.
///
/// Used as a pre-flight check to avoid starting an already-cancelled operation.
pub(crate) fn check_cancelled(token: Option<&CancellationTokenRef>) -> DeltaResult<()> {
    match token {
        Some(t) if t.is_cancelled() => Err(Error::Cancelled),
        _ => Ok(()),
    }
}

/// A cooperative cancellation signal supplied by a caller.
///
/// Implementors wrap whatever their runtime provides (e.g. `tokio_util::sync::CancellationToken`).
/// Kernel and cancellation-aware engines only *consume* it: [`is_cancelled`] provides a
/// synchronous pre-flight check, while an async engine may `select!` to race in-flight I/O against
/// [`cancelled_future`].
///
/// # Recovering the underlying token
///
/// The `Arc` kernel hands to each `*_with_cancellation` [`Engine`] method is the same one the
/// caller supplied, so an engine that supplied its own implementation can downcast it back to the
/// concrete type through [`AsAny`]. (Kernel may poll a composed or derived token internally; the
/// guarantee is only about what the engine receives.) This lets an engine reach a native
/// cancellation handle it wrapped, for code that cannot accept a Rust trait object.
///
/// Borrow with `as_ref().any_ref()`, or take an owned handle with [`AsAny::as_any`] -- see
/// [`AsAny::any_ref`] for why the borrow must go through `as_ref()` first.
///
/// ```
/// # use delta_kernel::cancellation::{CancellationToken, CancellationTokenRef, CancelledFuture};
/// # use delta_kernel::AsAny;
/// # use std::sync::Arc;
/// # struct MyToken;
/// # impl CancellationToken for MyToken {
/// #     fn is_cancelled(&self) -> bool { false }
/// #     fn cancelled_future(&self) -> CancelledFuture<'_> { Box::pin(std::future::pending()) }
/// # }
/// # let token: CancellationTokenRef = Arc::new(MyToken);
/// // In an engine's `read_*_with_cancellation`, given `token: CancellationTokenRef`:
/// if let Some(mine) = token.as_ref().any_ref().downcast_ref::<MyToken>() {
///     let _ = mine.is_cancelled(); // recovered the caller's concrete token
/// }
/// ```
///
/// [`is_cancelled`]: CancellationToken::is_cancelled
/// [`cancelled_future`]: CancellationToken::cancelled_future
/// [`Engine`]: crate::Engine
/// [`AsAny`]: crate::AsAny
pub trait CancellationToken: AsAny {
    /// Returns `true` once cancellation has been requested. Cheap, synchronous, and monotonic:
    /// once it returns `true` it must never return `false` again.
    fn is_cancelled(&self) -> bool;

    /// Returns a future that resolves when the token is cancelled (immediately if it already is).
    ///
    /// There is no default implementation: a correct notification cannot be synthesized from
    /// [`is_cancelled`](Self::is_cancelled) alone without either busy-polling or a runtime, and
    /// Kernel has neither. Implementors back this with their own notification primitive.
    fn cancelled_future(&self) -> CancelledFuture<'_>;
}

/// Wraps a fallible iterator so that cancellation terminates it with a single
/// [`Error::Cancelled`] rather than silent truncation.
///
/// Before each pull, the token is polled: if cancelled, one `Err(Error::Cancelled)` is yielded
/// and every subsequent call returns `None`. Any error or normal exhaustion also terminates the
/// iterator. With no token, or before cancellation, inner items pass through unchanged.
pub(crate) struct CancellableIterator<I> {
    inner: I,
    token: Option<CancellationTokenRef>,
    /// Set once the inner iterator has returned an error or reached normal exhaustion.
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
        let item = match self.token.as_ref() {
            Some(token) if token.is_cancelled() => Some(Err(Error::Cancelled)),
            _ => self.inner.next(),
        };
        self.done = !matches!(&item, Some(Ok(_)));
        item
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
        fn cancelled_future(&self) -> CancelledFuture<'_> {
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
    fn inner_error_terminates_iteration() {
        let token: CancellationTokenRef = Arc::new(TestToken::default());
        let inner = vec![Ok(0), Err(Error::generic("boom")), Ok(99)].into_iter();
        let mut iter = CancellableIterator::new(inner, Some(token));
        assert!(matches!(iter.next(), Some(Ok(0))));
        assert!(matches!(iter.next(), Some(Err(Error::Generic(_)))));
        // Fused on the inner error: the trailing Ok is never yielded.
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

    /// A second token type, to check that a downcast discriminates rather than always succeeding.
    #[derive(Default)]
    struct OtherToken;

    impl CancellationToken for OtherToken {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn cancelled_future(&self) -> CancelledFuture<'_> {
            Box::pin(ready(()))
        }
    }

    // Downcasting an erased token recovers the original value, not a copy: cancelling through the
    // recovered handle is observable through the erased one.
    #[test]
    fn downcast_recovers_the_same_token() {
        let erased: CancellationTokenRef = Arc::new(TestToken::default());

        let recovered = erased
            .clone()
            .as_any()
            .downcast::<TestToken>()
            .expect("erased token should downcast to its concrete type");
        recovered.cancel();

        assert!(erased.is_cancelled());
    }

    #[test]
    fn downcast_to_the_wrong_type_fails() {
        let erased: CancellationTokenRef = Arc::new(TestToken::default());
        assert!(erased.clone().as_any().downcast::<OtherToken>().is_err());
        assert!(erased
            .as_ref()
            .any_ref()
            .downcast_ref::<OtherToken>()
            .is_none());
    }

    // `Arc<dyn CancellationToken>` satisfies the blanket `AsAny` impl in its own right, so
    // `arc.any_ref()` resolves to the *`Arc`* rather than the token inside it and downcasts to the
    // concrete type fail. Borrowing goes through the trait object: `arc.as_ref().any_ref()`.
    #[test]
    fn any_ref_borrows_the_token_through_the_trait_object() {
        let token = Arc::new(TestToken::default());
        let erased: CancellationTokenRef = token.clone();

        assert!(erased.any_ref().downcast_ref::<TestToken>().is_none());

        let borrowed = erased
            .as_ref()
            .any_ref()
            .downcast_ref::<TestToken>()
            .expect("erased token should downcast to its concrete type");
        assert!(!borrowed.is_cancelled());
        token.cancel();
        assert!(borrowed.is_cancelled());
    }
}
