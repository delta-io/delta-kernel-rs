//! Core infrastructure for kernel coroutines.
//!
//! Coroutines are normal async tasks that call normal async functions. The coroutine's synchronous
//! driver code launches the coroutine by polling the compiler-generated future that represents
//! it. As with all async code, the task runs inside [`Future::poll`] and can invoke other functions
//! as it goes. Async functions return futures that it polls in turn. When the coroutine needs to
//! communicate with the connector, it creates and polls a special `Wait` future whose `poll`
//! method immediately returns [`Poll::Pending`]. That triggers a cascading
//! unwind of all the parent `poll` invocations until control returns to the synchronous coroutine
//! driver. The driver then returns the coroutine's request to the connector, along with a
//! [`Resume`] closure. When the connector invokes the `Resume` with its response, the closure again
//! polls the coroutine's future, which rebuilds the chain of `poll` calls back to
//! `Wait::poll`. This time, that call returns [`Poll::Ready`] with the connector's response, and
//! execution continues until the coroutine either completes or suspends again.
//!
//! Because [`Poll::Pending`] does not carry a payload, the coroutine driver creates an `Outbox`
//! which it shares with the coroutine via a [`Channel`]. Whenever the coroutine needs to suspend,
//! it creates an `Exchange` in `Outbound` state. It stores one
//! reference to the exchange in the outbox so the sync coroutine driver can access the request, and
//! initializes a `Wait` instance with a second reference to the exchange. `Wait::poll` returns
//! [`Poll::Pending`] because the exchange is still `Outbound`, the async
//! poll stack unwinds, and the sync driver extracts the exchange from the outbox, leaving it empty
//! again. When invoked, the [`Resume`] closure stores the connector's response in the exchange as
//! `Inbound`, the async poll stack builds back up, and `Wait::poll`
//! extracts the response from the exchange.
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};

use delta_kernel_derive::internal_api;
use tracing::{error, Instrument as _, Span};

use super::Resume;
use crate::{DeltaResult, Error};

/// A sendable future that resolves to a kernel result.
pub trait DeltaFuture<O>: Future<Output = DeltaResult<O>> + Send {}

impl<O, F: Future<Output = DeltaResult<O>> + Send> DeltaFuture<O> for F {}

/// A sendable, static closure that steps a coroutine to its next state.
pub trait Step<N>: FnOnce() -> DeltaResult<N> + Send + 'static {}

impl<N, F: FnOnce() -> DeltaResult<N> + Send + 'static> Step<N> for F {}

/// Coroutine-side handle for operations represented by pending request type `P`.
///
/// It shares a request outbox with the coroutine driver and admits one live request at a time.
pub struct Channel<P: OutboxEntry>(Arc<Outbox<P>>);

impl<P: OutboxEntry> Channel<P> {
    /// Initiate a request/response exchange with the connector.
    #[internal_api]
    pub(crate) async fn exchange<Out: Send + 'static, In: Send + 'static, T: Into<P>>(
        &self,
        outbound: Out,
        pending: impl FnOnce(Pending<Out, In>) -> T + Send,
    ) -> DeltaResult<In> {
        let exchange = Arc::new(Exchange::new(outbound));
        let pending = pending(Pending(Arc::downgrade(&exchange)));
        self.0.put(pending.into())?;
        Wait(exchange).await
    }
}

/// Internal coroutine state behind a crate-level workflow.
pub struct WorkflowTask<P: OutboxEntry, O: Send + 'static> {
    body: Body<O>,
    requests: Arc<Outbox<P>>,
}

impl<P: OutboxEntry, O: Send + 'static> WorkflowTask<P, O> {
    /// Create a workflow task without stepping it.
    #[internal_api]
    pub(crate) fn new<Fut>(workflow: impl FnOnce(Channel<P>) -> Fut) -> Self
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let requests = Arc::new(Outbox::default());
        let future = workflow(Channel(Arc::clone(&requests)));
        let future = async move {
            let guard = WorkflowCompletionGuard;
            let output = future.await;
            let _ = ManuallyDrop::new(guard);
            output.inspect_err(|err| error!(error = %err, "coroutine workflow failed"))
        };
        let body = Box::pin(future.instrument(Span::current()));
        Self { body, requests }
    }

    /// Run the task until it completes or suspends with a request.
    #[internal_api]
    pub(crate) fn step<W, R: 'static>(mut self) -> DeltaResult<W>
    where
        W: From<O> + From<R> + Send + 'static,
        P: IntoRequest<W, Request = R>,
    {
        let mut context = Context::from_waker(Waker::noop());
        match self.body.as_mut().poll(&mut context) {
            Poll::Ready(output) => output.map(W::from),
            Poll::Pending => {
                let pending = self.requests.take()?.ok_or_else(|| {
                    Error::internal_error(
                        "coroutine returned Pending without a live connector request",
                    )
                })?;
                Ok(W::from(pending.into_request(move || self.step())?))
            }
        }
    }
}

/// Internal coroutine state behind a crate-level generator.
pub struct GeneratorTask<P: OutboxEntry, Y: Send + 'static, O: Send + 'static> {
    body: Body<O>,
    requests: Arc<Outbox<P>>,
    yields: Arc<Outbox<Weak<YieldExchange<Y>>>>,
}

impl<P: OutboxEntry, Y: Send + 'static, O: Send + 'static> GeneratorTask<P, Y, O> {
    /// Create a generator task without stepping it.
    #[internal_api]
    pub(crate) fn new<Fut>(generator: impl FnOnce(Channel<P>, YieldChannel<Y>) -> Fut) -> Self
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let requests = Arc::new(Outbox::default());
        let yields = Arc::new(Outbox::default());
        let channel = Channel(Arc::clone(&requests));
        let yielder = YieldChannel(Arc::clone(&yields));
        let future = generator(channel, yielder);
        let future = async move {
            future
                .await
                .inspect_err(|err| error!(error = %err, "coroutine generator failed"))
        };
        Self {
            body: Box::pin(future.instrument(Span::current())),
            requests,
            yields,
        }
    }

    /// Run the task until it completes, suspends with a request, or yields an item.
    #[internal_api]
    pub(crate) fn step<G, R: 'static>(mut self) -> DeltaResult<G>
    where
        G: From<O> + From<R> + From<(Y, Resume<G, ()>)> + Send + 'static,
        P: IntoRequest<G, Request = R>,
    {
        let mut context = Context::from_waker(Waker::noop());
        if let Poll::Ready(output) = self.body.as_mut().poll(&mut context) {
            return output.map(G::from);
        }

        if let Some(pending) = self.yields.take()?.and_then(|pending| pending.upgrade()) {
            let step = move || self.step();
            let yield_item = |item, resume| G::from((item, resume));
            return Pending(Arc::downgrade(&pending)).into_request(step, yield_item);
        }

        let pending = self.requests.take()?.ok_or_else(|| {
            Error::internal_error("coroutine returned Pending without a live connector request")
        })?;
        Ok(G::from(pending.into_request(move || self.step())?))
    }
}

/// Coroutine-side handle for yielding generator output.
pub struct YieldChannel<Y: Send + 'static>(Arc<Outbox<Weak<YieldExchange<Y>>>>);

impl<Y: Send + 'static> YieldChannel<Y> {
    /// Yield one item and suspend until the consumer resumes the generator.
    ///
    /// An error supplied by the consumer is returned at this await point.
    #[internal_api]
    pub(crate) async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        let emission = Arc::new(Exchange::new(item));
        self.0.put(Arc::downgrade(&emission))?;
        Wait(emission).await
    }
}

/// Converts a pending request into a connector-facing request.
pub trait IntoRequest<N: Send + 'static>: OutboxEntry {
    /// Connector-facing request whose resume handle produces `N`.
    type Request;

    /// Convert this pending request into its connector-facing representation.
    fn into_request(self, step: impl Step<N>) -> DeltaResult<Self::Request>;
}

/// Opaque pending request/response exchange stored by a request vocabulary.
pub struct Pending<Out, In>(Weak<Exchange<Out, In>>);

impl<Out, In> Default for Pending<Out, In> {
    fn default() -> Self {
        Self(Weak::new())
    }
}

impl<Out: Send + 'static, In: Send + 'static> Pending<Out, In> {
    /// Return whether the coroutine still owns this exchange.
    #[internal_api]
    pub(crate) fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }

    /// Convert this exchange into a connector-facing request.
    #[internal_api]
    pub(crate) fn into_request<N: 'static, T>(
        self,
        step: impl Step<N>,
        make_request: impl FnOnce(Out, Resume<N, In>) -> T,
    ) -> DeltaResult<T> {
        let Some(exchange) = self.0.upgrade() else {
            return Err(Error::internal_error(
                "coroutine request expired before it was claimed",
            ));
        };
        let outbound = exchange.claim()?;
        let resume = Box::new(move |response| {
            exchange.respond(response)?;
            step()
        });
        Ok(make_request(outbound, resume))
    }
}

/// Entry stored in a coroutine outbox.
///
/// Entries are always weak references, in case the future that posted an entry gets dropped while
/// the coroutine's poll stack is unwinding (after its wait future returned `Pending` but before
/// control returns to the coroutine driver that would claim the entry). If that ever happened, the
/// outbox (and the coroutine as a whole) would be unable to process any more requests, effectively
/// killing the coroutine. This would require unusual circumstances, such as a kernel generator
/// racing requests against yields and dropping the loser (even tho neither request nor yield is a
/// terminal state for a generator). [`Default`] is a dead weak that doubles as an empty outbox
/// slot, so the mutex always holds a `T`.
pub trait OutboxEntry: Default + Send + 'static {
    /// True if the entry's weak reference is still valid.
    fn is_live(&self) -> bool;
}

/// Single-slot outbox that delivers [`Exchange`] instances to the coroutine's driver when the
/// coroutine suspends. It is always empty, except while the async poll stack unwinds.
///
/// Each outbox has exactly two shared references. The coroutine driver holds one reference, and the
/// coroutine's [`Channel`](crate::coroutine::Channel) holds the other. While the coroutine is
/// executing, both handles are owned by their respective stack frames, ensuring sequential
/// access. The [`Resume`](crate::coroutine::Resume) closure holds both references while the
/// coroutine is suspended, so the references move between threads together or not at all.
// NOTE: Rc+RefCell would be safe, but Arc+Mutex allows the compiler to derive Send
#[derive(Default)]
struct Outbox<T: OutboxEntry>(Mutex<T>);

impl<T: OutboxEntry> Outbox<T> {
    /// Put an entry, returning an error if the outbox is poisoned or already occupied.
    fn put(&self, entry: T) -> DeltaResult<()> {
        // WARNING: Never acquire another lock or invoke external code while holding this guard.
        let mut existing_entry = self.0.lock()?;
        if existing_entry.is_live() {
            return Err(Error::internal_error(
                "coroutine outbox is already occupied",
            ));
        }
        *existing_entry = entry;
        Ok(())
    }

    /// Take the entry, returning `None` if the outbox is empty.
    fn take(&self) -> DeltaResult<Option<T>> {
        // WARNING: Never acquire another lock or invoke external code while holding this guard.
        let mut entry = self.0.lock()?;
        let pending = std::mem::take(&mut *entry);
        Ok(pending.is_live().then_some(pending))
    }
}

/// Single-use operation or yield handoff shared by its waiter and resume handle.
///
/// Each exchange has exactly two shared references. The task holds one, and places the other in an
/// [`Outbox`] before suspending, for use by the synchronous coroutine driver. All accesses are
/// sequential. The [`Resume`](crate::coroutine::Resume) closure holds both references while the
/// coroutine is suspended, so the references move between threads together or not at all.
// NOTE: Rc+RefCell would be safe, but Arc+Mutex allows the compiler to derive Send
struct Exchange<Out, In>(Mutex<ExchangeState<Out, In>>);

/// Lifecycle state of an exchange.
enum ExchangeState<Out, In> {
    /// Kernel offered a request and has suspended the workflow.
    Outbound(Out),
    /// Connector claimed the request but has not responded yet.
    InFlight,
    /// Connector supplied a response but kernel did not claim it yet.
    Inbound(DeltaResult<In>),
    /// Kernel has consumed the response.
    Complete,
}

impl<Out, In> Exchange<Out, In> {
    /// Create an exchange containing the outbound operation.
    fn new(outbound: Out) -> Self {
        Self(Mutex::new(ExchangeState::Outbound(outbound)))
    }

    /// Claim the request kernel offered when suspending a workflow.
    pub(super) fn claim(&self) -> DeltaResult<Out> {
        // WARNING: Never acquire another lock or invoke external code while holding this guard.
        let mut state = self.lock()?;
        match std::mem::replace(&mut *state, ExchangeState::InFlight) {
            ExchangeState::Outbound(outbound) => Ok(outbound),
            previous => {
                *state = previous;
                Err(Error::internal_error(
                    "coroutine suspended without providing any outbound exchange",
                ))
            }
        }
    }

    /// Supply a response before resuming the workflow.
    fn respond(&self, response: DeltaResult<In>) -> DeltaResult<()> {
        // WARNING: Never acquire another lock or invoke external code while holding this guard.
        let mut state = self.lock()?;
        if !matches!(*state, ExchangeState::InFlight) {
            return Err(Error::internal_error(
                "coroutine exchange was not awaiting a response",
            ));
        }
        *state = ExchangeState::Inbound(response);
        Ok(())
    }

    fn lock(&self) -> DeltaResult<MutexGuard<'_, ExchangeState<Out, In>>> {
        Ok(self.0.lock()?)
    }
}

/// The `Future` suspension boundary between kernel coroutines and the connector driving them.
///
/// When the connector starts or resumes a kernel coroutine, its compiler-generated `poll` runs
/// kernel code until it reaches a suspension point that creates and polls this `Wait`. The
/// connector has not yet seen the request, so that first call returns `Pending`, which suspends the
/// entire nested chain of futures and returns control to the connector. The futures remain
/// suspended indefinitely, unless/until the connector invokes its [`crate::coroutine::Resume`] to
/// trigger a second poll. That second `poll` propagates through the suspended chain of futures
/// until it reaches `Wait::poll`, which now returns `Ready` and allows the kernel coroutine to
/// continue executing until it completes or suspends again.
///
/// Generator yields use the same mechanism, with the yield consumer taking the connector's role.
///
/// Polling is strictly sequential in the connector's calling thread, using a no-op waker. The side
/// channels afforded by [`Exchange`] and [`Outbox`] elminate the need for an async runtime.
struct Wait<Out, In>(Arc<Exchange<Out, In>>);

impl<Out: Send, In: Send> Future for Wait<Out, In> {
    type Output = DeltaResult<In>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // WARNING: Never acquire another lock or invoke external code while holding this guard.
        let mut state = match self.0.lock() {
            Err(err) => return Poll::Ready(Err(err)),
            Ok(state) => state,
        };
        match std::mem::replace(&mut *state, ExchangeState::Complete) {
            pending @ (ExchangeState::Outbound(_) | ExchangeState::InFlight) => {
                *state = pending;
                Poll::Pending
            }
            ExchangeState::Inbound(response) => Poll::Ready(response),
            ExchangeState::Complete => Poll::Ready(Err(Error::internal_error(
                "coroutine exchange future was polled after completion",
            ))),
        }
    }
}

/// Marks a workflow's reporting span as failed unless the workflow completes.
struct WorkflowCompletionGuard;

impl Drop for WorkflowCompletionGuard {
    fn drop(&mut self) {
        error!(
            error = "abandoned",
            "coroutine workflow was abandoned while suspended"
        );
    }
}

/// Boxed coroutine body, so tasks are not generic over the concrete future type.
type Body<O> = Pin<Box<dyn DeltaFuture<O> + 'static>>;

/// Exchange used to suspend a generator at a yielded item.
type YieldExchange<Y> = Exchange<Y, ()>;

impl<Y: Send + 'static> OutboxEntry for Weak<YieldExchange<Y>> {
    fn is_live(&self) -> bool {
        self.strong_count() > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_weak_outbox_entries_do_not_block_reuse() {
        let outbox = Outbox::default();
        let abandoned = Arc::new(Exchange::new(()));
        outbox.put(Arc::downgrade(&abandoned)).unwrap();
        drop(abandoned);

        let live = Arc::new(Exchange::new(()));
        outbox.put(Arc::downgrade(&live)).unwrap();
        assert!(outbox.take().unwrap().is_some());
    }
}
