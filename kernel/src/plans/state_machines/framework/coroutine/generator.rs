//! Minimal hand-rolled stackless coroutine shim.
//!
//! Replaces the `genawaiter` crate with a narrow, no-deps implementation that
//! covers exactly the shape the [`super::driver::CoroutineSM`] driver needs:
//!
//! - [`Co<Y, R>`] -- the coroutine-side handle. An `async fn` given a `Co` calls [`Co::yield_`] to
//!   surrender control with a value of type `Y`, and receives back a value of type `R` when the
//!   driver resumes it.
//! - [`Gen<Y, R, O>`] -- the driver-side generator. Wraps a boxed future produced by the user's
//!   `FnOnce(Co<Y, R>) -> F` closure. [`Gen::start`] performs the first poll (no resume value
//!   needed); subsequent calls to [`Gen::resume_with`] feed each yield's awaited resume value.
//!
//! ## Mechanics
//!
//! A single [`Slot<Y, R>`] shared between `Co` and `Gen` (via `Arc<Mutex<_>>`) carries one
//! payload at a time. The protocol is strictly alternating:
//!
//! 1. The body calls [`Co::yield_(y)`](Co::yield_), which stores `y` as `Slot::Yielded(y)` and
//!    awaits a [`YieldFuture`] that returns `Pending` on its first poll (handing control back to
//!    the driver).
//! 2. The driver retrieves `y` from `Slot::Yielded`, then calls [`Gen::resume_with(r)`] which
//!    stores `r` as `Slot::Resumed(r)` and re-polls the body.
//! 3. The body's [`YieldFuture`], on its second poll, takes `r` out of `Slot::Resumed` and
//!    completes with it.
//!
//! ## Constraints
//!
//! - The producer must not hold the slot lock across an `.await` boundary -- [`Co::yield_`] and
//!   [`YieldFuture::poll`] take the lock only to load/store one slot value, then drop it before
//!   returning. This is enforced by keeping the critical sections trivial.
//! - [`Gen::resume_with`] / [`Gen::start`] are `&mut self`: only one thread advances the coroutine
//!   at a time. The `Send` marker applies to futures that don't hold non-`Send` state across await
//!   points.
//! - The no-op waker never schedules a re-poll; the driver polls explicitly in response to external
//!   events (engine results), never speculatively.
//!
//! ## Errors and panics
//!
//! Per the kernel's no-panic policy this module never panics in production:
//!
//! - **Mutex poisoning** is recovered via
//!   [`PoisonError::into_inner`](std::sync::PoisonError::into_inner). The slot is a small enum and
//!   a poisoned guard always observes a valid variant.
//! - **Driver/body protocol violations** (advance after completion, body awaits something other
//!   than `Co::yield_`) are reported as [`GenError`] from [`Gen::start`] / [`Gen::resume_with`].
//!   The [`super::driver::CoroutineSM`] driver lifts these into
//!   [`DeltaError`](crate::plans::errors::DeltaError).
//! - [`YieldFuture::poll`] cannot return `Result`; if it ever observes an inconsistent slot state
//!   it leaves the slot untouched and yields `Poll::Pending`. The driver's next
//!   [`Gen::resume_with`] call detects the stalled state and surfaces a [`GenError`].

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

/// One-slot rendezvous between the coroutine body and the driver.
///
/// At most one of `Yielded` / `Resumed` is ever present:
/// - The body transitions `Empty` -> `Yielded(y)` in [`Co::yield_`].
/// - The driver transitions `Yielded(y)` -> `Empty` (taking `y`) and then `Empty` -> `Resumed(r)`
///   in [`Gen::resume_with`] (or, for [`Gen::start`], simply observes the first yield without ever
///   putting `Resumed`).
/// - The body transitions `Resumed(r)` -> `Empty` (taking `r`) on the second poll of its
///   [`YieldFuture`].
enum Slot<Y, R> {
    Empty,
    Yielded(Y),
    Resumed(R),
}

/// Recover the inner guard regardless of poisoning. The slot is a small
/// `enum` whose state is fully restored before each lock is dropped, so a
/// poisoned guard still observes a coherent variant.
fn lock_slot<Y, R>(slot: &Mutex<Slot<Y, R>>) -> MutexGuard<'_, Slot<Y, R>> {
    slot.lock().unwrap_or_else(|e| e.into_inner())
}

/// Coroutine-side handle. Passed into the producer closure.
pub struct Co<Y, R> {
    slot: Arc<Mutex<Slot<Y, R>>>,
}

impl<Y, R> Co<Y, R> {
    /// Yield control to the driver with value `y` and await the corresponding
    /// resume value `R`.
    pub async fn yield_(&self, y: Y) -> R {
        {
            let mut s = lock_slot(&self.slot);
            debug_assert!(
                matches!(*s, Slot::Empty),
                "previous yield/resume not consumed"
            );
            *s = Slot::Yielded(y);
        }
        YieldFuture {
            slot: self.slot.clone(),
            armed: true,
        }
        .await
    }
}

/// Future returned by [`Co::yield_`]. First poll hands control back to the
/// driver by returning `Poll::Pending`; second poll (after the driver has
/// stored a resume value) returns `Poll::Ready` with that value.
struct YieldFuture<Y, R> {
    slot: Arc<Mutex<Slot<Y, R>>>,
    armed: bool,
}

impl<Y, R> Future for YieldFuture<Y, R> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        if self.armed {
            // First poll -- the yielded value is already in the slot; surrender
            // control to the driver.
            self.armed = false;
            return Poll::Pending;
        }
        // Second poll -- driver has (normally) stored a resume value; take it.
        // Off-protocol slot states leave the slot untouched and stall this
        // future. The driver's next `poll_once` then sees the slot still
        // empty (no yield was produced) and surfaces the bug as
        // `GenError::BodyAwaitedNonYield`.
        let mut s = lock_slot(&self.slot);
        match std::mem::replace(&mut *s, Slot::Empty) {
            Slot::Resumed(r) => Poll::Ready(r),
            other => {
                *s = other;
                Poll::Pending
            }
        }
    }
}

/// Result of [`Gen::start`] / [`Gen::resume_with`].
pub enum GeneratorState<Y, O> {
    /// The coroutine yielded a value and is waiting for the next resume.
    Yielded(Y),
    /// The coroutine completed with the given output and cannot be resumed.
    Complete(O),
}

/// Driver/body protocol violation reported by [`Gen::start`] /
/// [`Gen::resume_with`]. These are kernel-internal bugs; the
/// [`super::driver::CoroutineSM`] driver lifts them into
/// [`DeltaError`](crate::plans::errors::DeltaError).
#[derive(Debug)]
pub enum GenError {
    /// `Gen::start` or `Gen::resume_with` was called after the body
    /// already returned [`GeneratorState::Complete`].
    AdvanceAfterComplete,
    /// The body returned `Poll::Pending` without producing a yield through
    /// [`Co::yield_`] -- typically because it awaited some other future
    /// (timer, channel, ...) that the no-op waker never wakes.
    BodyAwaitedNonYield,
}

impl std::fmt::Display for GenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdvanceAfterComplete => f.write_str(
                "coroutine advanced after completion -- the driver kept resuming a finished body",
            ),
            Self::BodyAwaitedNonYield => f.write_str(
                "coroutine body returned Pending without yielding via Co::yield_ \
                 -- bodies must only await futures produced by the Phase API",
            ),
        }
    }
}

impl std::error::Error for GenError {}

/// Driver-side generator. Owns the boxed future produced by the user's
/// producer closure.
///
/// Use [`Gen::new`] to create a generator, then [`Gen::start`] for the
/// first poll, then drive with [`Gen::resume_with`] until you see
/// [`GeneratorState::Complete`].
pub struct Gen<Y, R, O> {
    slot: Arc<Mutex<Slot<Y, R>>>,
    future: Option<Pin<Box<dyn Future<Output = O> + Send>>>,
}

impl<Y, R, O> Gen<Y, R, O>
where
    Y: Send + 'static,
    R: Send + 'static,
    O: Send + 'static,
{
    /// Construct a generator from a producer closure. The closure receives a
    /// [`Co<Y, R>`] and returns the body future; the body is not polled until
    /// [`Gen::start`] is called.
    pub fn new<F, Fut>(producer: F) -> Self
    where
        F: FnOnce(Co<Y, R>) -> Fut,
        Fut: Future<Output = O> + Send + 'static,
    {
        let slot = Arc::new(Mutex::new(Slot::<Y, R>::Empty));
        let co = Co { slot: slot.clone() };
        let future: Pin<Box<dyn Future<Output = O> + Send>> = Box::pin(producer(co));
        Self {
            slot,
            future: Some(future),
        }
    }

    /// Perform the first poll of the body. Returns the body's first
    /// [`GeneratorState::Yielded`] value (the protocol's first operation)
    /// or [`GeneratorState::Complete`] if the body completed without ever
    /// yielding.
    ///
    /// Returns [`GenError::AdvanceAfterComplete`] if called on an already-
    /// completed generator, or [`GenError::BodyAwaitedNonYield`] if the
    /// body awaited something other than [`Co::yield_`].
    pub fn start(&mut self) -> Result<GeneratorState<Y, O>, GenError> {
        self.poll_once()
    }

    /// Advance the coroutine: store `r` as the resume value the currently-
    /// pending [`Co::yield_`] will observe, poll the body future once, and
    /// report whether it yielded or completed.
    ///
    /// Returns the same errors as [`Gen::start`].
    pub fn resume_with(&mut self, r: R) -> Result<GeneratorState<Y, O>, GenError> {
        {
            let mut s = lock_slot(&self.slot);
            debug_assert!(
                matches!(*s, Slot::Empty),
                "Gen::resume_with called with slot not empty -- driver/body protocol broken",
            );
            *s = Slot::Resumed(r);
        }
        self.poll_once()
    }

    fn poll_once(&mut self) -> Result<GeneratorState<Y, O>, GenError> {
        let future = self.future.as_mut().ok_or(GenError::AdvanceAfterComplete)?;
        // The body is driven synchronously: every call to `poll_once` is the
        // direct response to either `start` or `resume_with`, never to a
        // wake notification. Bodies are only allowed to await `Co::yield_`,
        // whose `YieldFuture::poll` never registers the waker (it just
        // returns `Pending` once and then `Ready` once the slot is filled).
        // A no-op waker is therefore both sufficient and intentional --
        // wake-driven re-polling is structurally impossible, and using
        // `Waker::noop()` makes that contract obvious and avoids allocating
        // a real waker on every advance.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => {
                let mut s = lock_slot(&self.slot);
                match std::mem::replace(&mut *s, Slot::Empty) {
                    Slot::Yielded(y) => Ok(GeneratorState::Yielded(y)),
                    other => {
                        // Body returned Pending without producing a yield, or
                        // YieldFuture stalled on an inconsistent slot state.
                        // Restore the slot so a subsequent resume can still
                        // observe it during diagnostics, and report the bug.
                        *s = other;
                        Err(GenError::BodyAwaitedNonYield)
                    }
                }
            }
            Poll::Ready(o) => {
                self.future = None;
                Ok(GeneratorState::Complete(o))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yields_then_completes() {
        let mut g = Gen::<u32, &'static str, i64>::new(|co| async move {
            let a = co.yield_(1).await;
            let b = co.yield_(2).await;
            (a.len() + b.len()) as i64
        });

        match g.start().unwrap() {
            GeneratorState::Yielded(y) => assert_eq!(y, 1),
            _ => panic!("expected first yield"),
        }
        match g.resume_with("hello").unwrap() {
            GeneratorState::Yielded(y) => assert_eq!(y, 2),
            _ => panic!("expected second yield"),
        }
        match g.resume_with("world!").unwrap() {
            GeneratorState::Complete(o) => assert_eq!(o, 5 + 6),
            _ => panic!("expected completion"),
        }
    }

    #[test]
    fn empty_body_completes_on_start() {
        let mut g = Gen::<(), (), &'static str>::new(|_co| async move { "done" });
        match g.start().unwrap() {
            GeneratorState::Complete(o) => assert_eq!(o, "done"),
            GeneratorState::Yielded(_) => panic!("unexpected yield"),
        }
    }

    #[test]
    fn resume_after_complete_returns_advance_after_complete() {
        let mut g = Gen::<(), (), ()>::new(|_co| async move {});
        let _ = g.start().unwrap();
        assert!(matches!(
            g.resume_with(()),
            Err(GenError::AdvanceAfterComplete)
        ));
    }

    #[test]
    fn body_awaiting_non_yield_future_returns_body_awaited_non_yield() {
        // The body awaits std::future::pending() instead of co.yield_(),
        // which (under the no-op waker) returns Pending forever without
        // producing a yield value.
        let mut g = Gen::<(), (), ()>::new(|_co| async move {
            std::future::pending::<()>().await;
        });
        assert!(matches!(g.start(), Err(GenError::BodyAwaitedNonYield)));
    }

    #[test]
    fn propagates_resume_values_through_multiple_yields() {
        // Three yields; the body completes with `a + b + c` where each of
        // a/b/c is the resume value fed to the corresponding yield's `.await`.
        let mut g = Gen::<(), i32, i32>::new(|co| async move {
            let a = co.yield_(()).await;
            let b = co.yield_(()).await;
            let c = co.yield_(()).await;
            a + b + c
        });

        assert!(matches!(g.start().unwrap(), GeneratorState::Yielded(())));
        assert!(matches!(
            g.resume_with(10).unwrap(),
            GeneratorState::Yielded(())
        )); // a = 10
        assert!(matches!(
            g.resume_with(20).unwrap(),
            GeneratorState::Yielded(())
        )); // b = 20
        match g.resume_with(30).unwrap() {
            // c = 30 -- body returns 10 + 20 + 30 = 60.
            GeneratorState::Complete(o) => assert_eq!(o, 60),
            _ => panic!("expected complete"),
        }
    }
}
