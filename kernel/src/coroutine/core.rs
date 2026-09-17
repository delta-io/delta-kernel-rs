//! Transport primitives for connector-driven coroutines.
//!
//! A [`Channel`] posts a request and suspends its coroutine until the request's [`Reply`] is
//! completed. A [`Task`] owns the corresponding receiver and polls the coroutine until it either
//! completes or posts another request.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use delta_kernel_derive::internal_api;

use crate::{DeltaResult, Error};

/// Sendable future whose output uses Kernel's error type.
pub trait DeltaFuture<T>: Future<Output = DeltaResult<T>> + Send {}

impl<T, F> DeltaFuture<T> for F where F: Future<Output = DeltaResult<T>> + Send {}

/// Coroutine-side capability for posting typed work to a task driver.
///
/// Posting suspends the coroutine; it never invokes the driver.
pub struct Channel<P>(Weak<Outbox<P>>);

impl<P> Channel<P> {
    /// Post one request and suspend until its driver replies.
    #[internal_api]
    pub(crate) async fn request<In>(
        &self,
        make_request: impl FnOnce(Reply<In>) -> P,
    ) -> DeltaResult<In> {
        let response = Arc::new(Response::default());
        let request = make_request(Reply(Arc::downgrade(&response)));
        self.0
            .upgrade()
            .ok_or_else(|| Error::internal_error("coroutine channel outlived its receiver"))?
            .put(request)?;
        Wait {
            response,
            outbox: self.0.clone(),
        }
        .await
    }
}

/// Driver-side owner of one request lane.
///
/// A [`Task`] retains the receiver beside its coroutine future so the lane remains valid while the
/// task is alive.
pub struct Receiver<P>(Arc<Outbox<P>>);

impl<P> Receiver<P> {
    fn new() -> (Self, Channel<P>) {
        let outbox = Arc::new(Outbox::default());
        let channel = Channel(Arc::downgrade(&outbox));
        (Self(outbox), channel)
    }
}

/// Typed, single-use capability to complete one suspended request.
///
/// A reply is tied to its originating await point and cannot resume another request.
pub struct Reply<In>(Weak<Response<In>>);

impl<In> Reply<In> {
    /// Deliver `result` to the suspended request.
    ///
    /// Returns an error if the suspended request no longer exists.
    pub fn send(self, result: DeltaResult<In>) -> DeltaResult<()> {
        let response = self
            .0
            .upgrade()
            .ok_or_else(|| Error::internal_error("coroutine request expired before its reply"))?;
        *response.lock()? = Some(result);
        Ok(())
    }
}

/// Adapts one or more receivers into a task's request stream.
#[internal_api]
pub(crate) trait RequestSource {
    /// Request type produced by this source.
    type Request;

    /// Take the one pending request, if any.
    fn take_request(&self) -> DeltaResult<Option<Self::Request>>;
}

impl<P> RequestSource for Receiver<P> {
    type Request = P;

    fn take_request(&self) -> DeltaResult<Option<Self::Request>> {
        self.0.take()
    }
}

/// Identifies which of two lanes produced a request.
#[internal_api]
pub(crate) enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A: RequestSource, B: RequestSource> RequestSource for (A, B) {
    type Request = Either<A::Request, B::Request>;

    fn take_request(&self) -> DeltaResult<Option<Self::Request>> {
        match self.0.take_request()? {
            Some(request) => Ok(Some(Either::Left(request))),
            None => Ok(self.1.take_request()?.map(Either::Right)),
        }
    }
}

/// Result of advancing a [`Task`].
pub enum Step<R, O> {
    /// The task completed.
    Done(O),
    /// The task posted a request and remains suspended until its reply is completed.
    Request(R),
}

/// Retains one pinned coroutine and the request lanes observed by its driver.
#[internal_api]
pub(crate) struct Task<'a, R: RequestSource, O> {
    body: Pin<Box<dyn Future<Output = DeltaResult<O>> + Send + 'a>>,
    requests: R,
    done: bool,
}

impl<'a, R: RequestSource, O> Task<'a, R, O> {
    /// Poll once and return completion or the request posted by that poll.
    ///
    /// Calling after completion or before the previous request receives a response returns an
    /// error.
    pub fn advance(&mut self) -> DeltaResult<Step<R::Request, O>> {
        match self.poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(step) => step,
            Poll::Pending => Err(Error::internal_error(
                "coroutine returned Pending without a live request",
            )),
        }
    }

    #[internal_api]
    pub(crate) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<DeltaResult<Step<R::Request, O>>> {
        if self.done {
            return Poll::Ready(Err(Error::internal_error(
                "coroutine task was advanced after completion",
            )));
        }
        if let Poll::Ready(output) = self.body.as_mut().poll(context) {
            self.done = true;
            return Poll::Ready(output.map(Step::Done));
        }
        match self.requests.take_request().transpose() {
            None => Poll::Pending,
            Some(request) => Poll::Ready(request.map(Step::Request)),
        }
    }
}

impl<'a, P, O> Task<'a, Receiver<P>, O> {
    /// Create a task with one request lane and pass its channel to `make_body`.
    pub fn single_lane<F, Fut>(make_body: F) -> Self
    where
        F: FnOnce(Channel<P>) -> Fut,
        Fut: Future<Output = DeltaResult<O>> + Send + 'a,
    {
        let (receiver, channel) = Receiver::new();
        Self {
            body: Box::pin(make_body(channel)),
            requests: receiver,
            done: false,
        }
    }
}

impl<'a, A, B, O> Task<'a, (Receiver<A>, Receiver<B>), O> {
    /// Create a task with two request lanes and pass both channels to `make_body`.
    pub fn dual_lane<F, Fut>(make_body: F) -> Self
    where
        F: FnOnce(Channel<A>, Channel<B>) -> Fut,
        Fut: Future<Output = DeltaResult<O>> + Send + 'a,
    {
        let (left, left_channel) = Receiver::new();
        let (right, right_channel) = Receiver::new();
        Self {
            body: Box::pin(make_body(left_channel, right_channel)),
            requests: (left, right),
            done: false,
        }
    }
}

struct Wait<P, In> {
    response: Arc<Response<In>>,
    outbox: Weak<Outbox<P>>,
}

impl<P, In> Future for Wait<P, In> {
    type Output = DeltaResult<In>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.response.lock() {
            Ok(mut response) => response.take().map_or(Poll::Pending, Poll::Ready),
            Err(err) => Poll::Ready(Err(err.into())),
        }
    }
}

impl<P, In> Drop for Wait<P, In> {
    fn drop(&mut self) {
        if let Some(outbox) = self.outbox.upgrade() {
            let _ = outbox.clear();
        }
    }
}

type Response<In> = Mutex<Option<DeltaResult<In>>>;

struct Outbox<P>(Mutex<Slot<P>>);

enum Slot<P> {
    Vacant,
    Posted(P),
    // The driver owns the request, but its waiter still owns the lane.
    Claimed,
}

impl<P> Default for Outbox<P> {
    fn default() -> Self {
        Self(Mutex::new(Slot::Vacant))
    }
}

impl<P> Outbox<P> {
    fn put(&self, request: P) -> DeltaResult<()> {
        let mut slot = self.0.lock()?;
        let Slot::Vacant = &*slot else {
            return Err(Error::internal_error(
                "coroutine outbox is already occupied",
            ));
        };
        *slot = Slot::Posted(request);
        Ok(())
    }

    fn take(&self) -> DeltaResult<Option<P>> {
        let mut slot = self.0.lock()?;
        match std::mem::replace(&mut *slot, Slot::Claimed) {
            Slot::Claimed => Ok(None),
            Slot::Posted(request) => Ok(Some(request)),
            Slot::Vacant => {
                *slot = Slot::Vacant;
                Ok(None)
            }
        }
    }

    fn clear(&self) -> DeltaResult<()> {
        // Posted and Claimed both reject new requests, so this waiter cannot clear a newer one.
        // Release the lock before dropping a request; its destructor may run arbitrary code.
        let _ = {
            let mut slot = self.0.lock()?;
            std::mem::replace(&mut *slot, Slot::Vacant)
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;

    enum Request {
        Ping(Reply<()>),
    }

    #[test]
    fn channel_cannot_outlive_its_receiver() {
        let (receiver, channel) = Receiver::<Request>::new();
        drop(receiver);

        let mut future = pin!(channel.request(Request::Ping));
        let Poll::Ready(Err(err)) = future.as_mut().poll(&mut context()) else {
            panic!("orphaned channel did not fail");
        };
        assert!(err.to_string().contains("outlived its receiver"));
    }

    #[test]
    fn abandoned_requests_do_not_block_the_lane() {
        let (receiver, channel) = Receiver::<Request>::new();
        {
            let mut abandoned = pin!(channel.request(Request::Ping));
            assert!(abandoned.as_mut().poll(&mut context()).is_pending());
        }

        let mut active = pin!(channel.request(Request::Ping));
        assert!(active.as_mut().poll(&mut context()).is_pending());
        let Some(Request::Ping(reply)) = receiver.take_request().unwrap() else {
            panic!("second request did not replace the abandoned request");
        };
        reply.send(Ok(())).unwrap();
        assert!(matches!(
            active.as_mut().poll(&mut context()),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn claimed_requests_keep_the_lane_occupied() {
        let (receiver, channel) = Receiver::<Request>::new();
        let mut first = pin!(channel.request(Request::Ping));
        assert!(first.as_mut().poll(&mut context()).is_pending());
        let Some(Request::Ping(reply)) = receiver.take_request().unwrap() else {
            panic!("first request was not posted");
        };

        let mut overlapping = pin!(channel.request(Request::Ping));
        let Poll::Ready(Err(err)) = overlapping.as_mut().poll(&mut context()) else {
            panic!("occupied lane accepted another request");
        };
        assert!(err.to_string().contains("already occupied"));

        reply.send(Ok(())).unwrap();
        assert!(matches!(
            first.as_mut().poll(&mut context()),
            Poll::Ready(Ok(()))
        ));

        let mut next = pin!(channel.request(Request::Ping));
        assert!(next.as_mut().poll(&mut context()).is_pending());
        assert!(receiver.take_request().unwrap().is_some());
    }

    #[test]
    fn dropping_claimed_request_expires_reply_and_releases_lane() {
        let (receiver, channel) = Receiver::<Request>::new();
        let reply = {
            let mut future = pin!(channel.request(Request::Ping));
            assert!(future.as_mut().poll(&mut context()).is_pending());
            let Some(Request::Ping(reply)) = receiver.take_request().unwrap() else {
                panic!("request was not posted");
            };
            reply
        };

        let err = reply.send(Ok(())).unwrap_err();
        assert!(err.to_string().contains("expired before its reply"));

        let mut next = pin!(channel.request(Request::Ping));
        assert!(next.as_mut().poll(&mut context()).is_pending());
        assert!(receiver.take_request().unwrap().is_some());
    }

    fn context() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }
}
