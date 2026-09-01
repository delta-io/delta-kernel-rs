//! Stackless, connector-driven kernel workflows.
//!
//! By design, kernel maintains a strict separation of concerns, delegating I/O and dataflow
//! operations to the connector. The [`crate::Engine`] traits allow kernel to delegate work, but
//! they produce nested connector -> kernel -> connector call stacks with kernel stuck in the middle
//! between two sides of the connector, which complicates the connector's state passing, scheduling,
//! error handling, etc. It also creates a high risk of control inversion, where kernel code forces
//! design decisions that rightfully belong with the connector, by treating connector as a library
//! to be called upon at will. Stackless coroutines avoid all of that: Whenever kernel needs to
//! communicate back to the connector, it suspends itself and the connector receives the request as
//! a normal function call return. The connector responds to a request by running its own code on
//! behalf of kernel and resuming kernel with the response. Kernel coroutines take two forms: A
//! [`Workflow`] evolves through multiple request-response steps to produce a final answer, while a
//! [`Generator`] can additionally yield items before producing its final answer. Whenever a
//! connector starts or resumes either kind of coroutine, kernel runs on the calling thread until it
//! either completes or suspends with the next request/yield. Because these are stackless
//! coroutines, no kernel functions are in the call stack while kernel is suspended.
//!
//! The exhaustive [`Request`] enum defines the operations kernel may delegate. Each request carries
//! the operation inputs and a [`TypedResume`] handle, which owns the suspended continuation and
//! which declares the expected response type. A connector drives a [`Workflow`] or [`Generator`] by
//! repeatedly matching each request, performing the operation, and invoking [`TypedResume::resume`]
//! with the result:
//!
//! ```no_run
//! use delta_kernel::coroutine::listing::ForwardListing;
//! use delta_kernel::coroutine::{Cursor, Page, PageRequest, Workflow};
//! use delta_kernel::DeltaResult;
//!
//! # /*
//! /// Operations kernel can request.
//! ///
//! /// `N` is the next [`Workflow`] or [`Generator`] state returned by each resume handle.
//! pub enum Request<N: Send + 'static> {
//!     /// List a bounded path range in ascending order.
//!     ListForward(PageRequest<N, ForwardListing>),
//!     /// List page ranges from high to low, with entries ascending within each page.
//!     ListBackward(PageRequest<N, BackwardListing>),
//!     /// Read a whole file when the range is `None`, or exactly the half-open range otherwise.
//!     ReadSmallFile(FileSlice, TypedResume<N, Bytes>),
//!       ...
//! }
//! # */
//! # use delta_kernel::coroutine::Request;
//!
//! // A kernel-defined return type.
//! pub struct Foo;
//!
//! // A public kernel workflow entry point. It does _not_ take an `Engine` instance.
//! pub fn public_kernel_entry_point() -> DeltaResult<Workflow<Foo>> {
//!     // Kernel constructs and starts its coroutine here.
//!     # Err(delta_kernel::Error::generic("example-only entry point"))
//! }
//!
//! async fn prepare_listing(
//!     _request: ForwardListing,
//! ) -> DeltaResult<Cursor<ForwardListing>> {
//!     // Connector creates its cursor state and may begin prefetching here.
//!     # Ok(Cursor::id(0))
//! }
//!
//! async fn advance_listing(
//!     _cursor: Cursor<ForwardListing>,
//! ) -> DeltaResult<Page<ForwardListing>> {
//!     // Connector resolves its cursor state and fetches the next page here.
//!     # Ok(Page {
//!     #     data: vec![],
//!     #     next: None,
//!     # })
//! }
//!
//! // Connector decides how to dispatch kernel requests
//! async fn serve_request<N: Send + 'static>(
//!     request: Request<N>,
//! ) -> DeltaResult<N> {
//!     match request {
//!         Request::ListForward(PageRequest::Prepare(args, resume)) => {
//!             resume.resume(prepare_listing(args).await)
//!         }
//!         Request::ListForward(PageRequest::Start(args, resume)) => {
//!             let cursor = prepare_listing(args).await?;
//!             resume.resume(advance_listing(cursor).await)
//!         }
//!         Request::ListForward(PageRequest::Continue(cursor, resume)) => {
//!             resume.resume(advance_listing(cursor).await)
//!         }
//!         _ => Err(delta_kernel::Error::unsupported(
//!             "this connector does not support the requested operation",
//!         )),
//!     }
//! }
//!
//! // Connector also decides how to drive workflows.
//! async fn connector_workflow_driver<T: Send + 'static>(
//!     mut workflow: DeltaResult<Workflow<T>>,
//! ) -> DeltaResult<T> {
//!     loop {
//!         workflow = match workflow? {
//!             Workflow::Done(output) => return Ok(output),
//!             Workflow::Request(request) => serve_request(request).await,
//!         };
//!     }
//! }
//!
//! async fn connector_entry_point() -> DeltaResult<Foo> {
//!     connector_workflow_driver(public_kernel_entry_point()).await
//! }
//! ```
//!
//! Kernel coroutines are an async-friendly synchronous API. Between suspensions, kernel performs
//! only light, bounded CPU work. It must delegate all blocking, heavy, or unbounded work to the
//! connector as a [`Request`]; failure to do so is a kernel bug. Connectors therefore do not need
//! to treat kernel calls as scheduling points. Resume handles are [`Send`] and may move freely
//! between tasks and threads as the workflow progresses. The connector may execute requests
//! synchronously, asynchronously, on threads, or across distributed workers using the runtimes and
//! libraries of its choosing.
//!
//! # Pagination
//!
//! Operations that may produce large or unbounded results, such as file reads or query output, use
//! a paginated request model. This allows kernel to consume a logical stream of results while still
//! allowing connector to control the amount of in-flight memory, terminate the operation if
//! resource limits are hit, etc.
//!
//! Paged operations have three phases:
//!
//! - [`PageRequest::Start`] initializes the operation and returns its first [`Page`] of output.
//! - [`PageRequest::Continue`] consumes a cursor and returns the next page, along with Some new
//!   cursor if the operation has not yet completed.
//! - [`PageRequest::Prepare`] initializes an operation and returns a [`Cursor`] without producing a
//!   first page. This lets the connector begin work in the background while waiting for the first
//!   [`PageRequest::Continue`] from kernel.
//!
//! Every response to a pagination request carries its data and an optional [`Cursor`] for
//! continuing. `None` ends pagination; an empty page with a live cursor does not. Connectors choose
//! page boundaries and cursor representation; cursor payloads are opaque to kernel, forwarded
//! blindly back to connector with each continuation request (or dropped if kernel abandoned the
//! operation). [`CursorState::Boxed`] owns and drops in-process state while [`CursorState::Id`] is
//! a raw (connector-managed) reference with no drop notification.
//!
//! Kernel coroutines expose only one request at a time, but pagination allows kernel to expose
//! upcoming I/O streams so connectors can begin prefetching.
//!
//! # Yielding
//!
//! A [`Generator`] adds [`Generator::Yield`] to the completion and request states of a
//! [`Workflow`]. Resuming the corresponding [`YieldResume`] continues the generator, and dropping
//! the resume abandons the generator.
//!
//! # Error handling
//!
//! Every transition is fallible:
//!
//! - Starting a coroutine returns `DeltaResult<Workflow<_>>` or `DeltaResult<Generator<_, _>>`. An
//!   error means the workflow failed before producing the next state.
//! - [`TypedResume::resume`] accepts `DeltaResult<R>`. A connector error is delivered to the
//!   suspended kernel await point, where kernel may handle it or propagate it.
//! - `resume` itself returns `DeltaResult`. An error means the workflow failed while kernel was
//!   processing the response or advancing the coroutine.
//!
//! A connector may drop any resume handle at any time. This abandons the suspended coroutine and
//! drops all associated state. For connector-side work failures, resume with `Err` when kernel
//! should observe the failure; otherwise drop the handle and return the error directly.
// === Kernel implementation ===
//
// Kernel workflows are ordinary async Rust functions, but this module allows the connector to drive
// them directly, without runtime scheduling: Kernel futures are polled with a no-op waker and
// advance only on explicit start or resume calls:
//
// 1. The connector calls a kernel entry point that creates a `Workflow`.
//
// 2. The entry point calls `Workflow::start` with a closure that, when invoked synchronously,
//    associates the workflow's async logic with a `Channel` and returns the resulting async future.
//
// 3. `Workflow::start` creates an `Arc<RequestMailbox>`. It keeps one reference for itself, and
//    shares a second reference with kernel by wrapping it in a `Channel`, which it uses to invoke
//    the workflow-creation closure. Because that closure returns a future, invoking it creates but
//    does not start the workflow's (compiler-generated) async state machine.
//
// 4. `Workflow::start` wraps and instruments that future, and box-pins it (type alias: `Task<O>`).
//    This `Task` owns the complete compiler-generated state of the kernel workflow.
//
// 5. `Workflow::start` passes the task and mailbox to `advance_workflow`.
//
// 6. `advance_workflow` creates a `Context` with a no-op waker and explicitly polls the task. No
//    async runtime participates in coroutine scheduling.
//
// 7. Polling the task launches it (standard async Rust), executing the kernel workflow until it
//    calls an async channel method such as `channel.read_small_file(...)`. All channel methods are
//    thin wrappers that call and await the async `Channel::exchange` method.
//
// 8. The kernel’s `.await` polls that channel-method future. Its body calls
//    `Channel::exchange(...)`, creating and immediately awaiting another compiler-generated future.
//
// 9. The first poll of the `exchange` future creates an `Arc<Exchange>` in `Outbound(operation)`
//    state and stores a `Weak<Exchange>` in the channel mailbox. The weak pointer will be dropped
//    automatically if kernel abandons the exchange after publishing it to the connector.
//
// 10. The `exchange` future then constructs `Wait(exchange)` and awaits it. The generated
//     `exchange` future invokes `Wait::poll`.
//
// 11. `Wait::poll` sees `Outbound`, leaves that state intact, and returns `Pending`.
//
// 12. `Pending` propagates through the `exchange` future, channel-method future, and workflow
//     future. Their compiler-generated async state machines retain the nested futures and all
//     suspended locals, including the only strong reference to the `Exchange`. The task’s poll
//     finally returns `Pending` to `advance_workflow`.
//
// 13. After that poll returns, `advance_workflow` removes the pending request from the mailbox and
//     calls `PendingRequest::attach`.
//
// 14. `attach` upgrades the mailbox’s `Weak<Exchange>`, claims the operation, and transitions the
//     exchange from `Outbound` to `InFlight`.
//
// 15. `attach` creates a `TypedResume` whose closure owns the upgraded `Arc<Exchange>`, pinned
//     workflow task (which still owns the original `Arc<Exchange>`), mailbox, and the function
//     needed to advance that task again. It combines the claimed operation with this resume handle
//     to produce a typed `Request`.
//
// 16. `advance_workflow` returns `Workflow::Request(request)` to `Workflow::start`, which returns
//     it to the connector. At this point, the `Mailbox`, `Channel`, and `Exchange` are all
//     reachable only through the `TypedResume`. If the connector drops that `request`, the entire
//     workflow and all associated state are dropped as Arc reference counts go to zero.
//
// 17. The connector matches the request, performs the operation, and calls
//     `TypedResume::resume(response)`. The resume closure stores the response in the exchange,
//     transitioning `InFlight` to `Inbound(response)`, then forwards the `Task` and the `Mailbox`
//     to `advance_workflow`.
//
// 18. `advance_workflow` polls the task again, and the compiler-generated async state machines
//     resume the nested poll chain at the suspended `.await`. The `Wait` instance now sees
//     `Inbound(response)`, transitions the exchange to `Complete`, and returns `Ready(response)`.
//     That result propagates through the `exchange` and channel-method futures, and becomes the
//     return value of kernel’s original `.await`.
//
// 19. The kernel workflow continues until it creates and awaits another channel future, repeating
//     the process, or returns its final `DeltaResult<O>`. `advance_workflow` converts successful
//     completion to `Workflow::Done(output)` and propagates any error directly.
//
// `Yielder` adds `yield_item`, which logically separates the entity that consumes yielded items
// from the entity that processes requests. When kernel drives a child generator, it consumes the
// yielded items directly while forwarding its requests back to the connector through its own
// `Channel`. The connector therefore sees those requests as part of the overall workflow's
// coroutine rather than driving a separate child generator. Kernel uses these internal generators
// to manipulate streaming outputs produced by e.g. log replay.
//
// For connectors that don't want to deal with coroutines at all, kernel continues to provide legacy
// sync/imperative entry points. Each entry point launches the corresponding workflow, then drives
// it to completion using an `EngineConnector` that uses the caller-provided `Engine` instance to
// serve requestse.

use std::any::Any;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use tracing::{error, Instrument as _, Span};
#[cfg(test)]
use url::Url;

pub(crate) use self::core::DeltaFuture;
use self::core::{
    advance_generator, advance_workflow, Exchange, PendingRequest, RequestMailbox, Wait,
    WeakExchange, YieldMailbox,
};
use self::listing::{BackwardListing, BackwardListingResult, ForwardListing, ListingBounds};
#[cfg(feature = "declarative-plans")]
use self::read::ExecutePlan;
use self::read::{ReadJsonFiles, ReadParquetFiles};
use self::write::WriteBytes;
#[cfg(test)]
use crate::Error;
use crate::{DeltaResult, FileMeta, FileSlice, ParquetFooter};

mod core;
pub(crate) mod engine;
pub mod listing;
pub mod read;
pub mod write;

#[cfg(test)]
mod tests;

/// Resume handle for one yielded generator item.
///
/// Pass `Ok(())` to continue or an error for kernel to observe at the suspended yield.
pub type YieldResume<N> = TypedResume<N, ()>;

/// Describes a connector operation that may return multiple pages.
pub trait PagedOperation: Send + Sized + 'static {
    /// Data returned in one page.
    type Page: Send + 'static;
}

/// One page of connector data and the cursor for requesting more.
pub struct Page<Op: PagedOperation> {
    /// Data returned in this page.
    pub data: Op::Page,
    /// Cursor to pass to `Continue`, or `None` when no pages remain.
    pub next: Option<Cursor<Op>>,
}

/// Opaque pagination handle typed to one operation; kernel stores it until `Continue`.
pub struct Cursor<Op: PagedOperation> {
    state: CursorState,
    operation: PhantomData<fn() -> Op>,
}

/// Type-erased connector state carried by a [`Cursor`].
pub enum CursorState {
    /// Scalar handle, commonly for FFI; kernel is not notified when it is dropped.
    Id(i64),
    /// Owned in-process state, dropped with the cursor.
    Boxed(Box<dyn Any + Send>),
}

impl<Op: PagedOperation> Cursor<Op> {
    /// Construct and return a cursor carrying connector-defined scalar `id`.
    pub fn id(id: i64) -> Self {
        Self::from_state(CursorState::Id(id))
    }

    /// Construct and return a cursor owning connector-defined in-process `state`.
    pub fn boxed(state: impl Any + Send) -> Self {
        Self::from_state(CursorState::Boxed(Box::new(state)))
    }

    /// Consume this cursor and return its connector-defined state.
    pub fn into_state(self) -> CursorState {
        self.state
    }

    fn from_state(state: CursorState) -> Self {
        Self {
            state,
            operation: PhantomData,
        }
    }
}

/// A workflow that has either completed or suspended with a request for the connector.
pub enum Workflow<O: Send + 'static> {
    Done(O),
    Request(Request<Self>),
}

impl<O: Send + 'static> Workflow<O> {
    /// Start a workflow and run it until completion or its first connector request.
    pub(crate) fn start<Fut>(workflow: impl FnOnce(Channel) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let mailbox = Arc::new(RequestMailbox::default());
        let future = workflow(Channel(Arc::clone(&mailbox)));
        let future = async move {
            future
                .await
                .inspect_err(|err| error!(error = %err, "kernel workflow failed"))
        };
        let task = Box::pin(future.instrument(Span::current()));
        advance_workflow(task, mailbox)
    }
}

/// A connector-facing generator that can complete, yield an item, or request connector work.
pub enum Generator<O: Send + 'static, Y: Send + 'static> {
    Done(O),
    Yield(Y, YieldResume<Self>),
    Request(Request<Self>),
}

impl<O: Send + 'static, Y: Send + 'static> Generator<O, Y> {
    /// Start a generator and run it until completion, its first yield, or a connector request.
    pub(crate) fn start<Fut>(generator: impl FnOnce(Yielder<Y>) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let mailbox = Arc::new(RequestMailbox::default());
        let yields = Arc::new(YieldMailbox::default());
        let yielder = Yielder {
            channel: Channel(Arc::clone(&mailbox)),
            mailbox: Arc::clone(&yields),
        };
        let future = generator(yielder);
        let future = async move {
            future
                .await
                .inspect_err(|err| error!(error = %err, "kernel generator failed"))
        };
        let task = Box::pin(future.instrument(Span::current()));
        advance_generator(task, mailbox, yields)
    }
}

/// Operations kernel can request.
///
/// `N` is the next [`Workflow`] or [`Generator`] state returned by each resume handle.
pub enum Request<N: Send + 'static> {
    /// List a bounded path range in ascending order.
    ListForward(PageRequest<N, ForwardListing>),
    /// List page ranges from high to low, with entries ascending within each page.
    ListBackward(PageRequest<N, BackwardListing>),
    /// Read a whole file when the range is `None`, or exactly the half-open range otherwise.
    ReadSmallFile(FileSlice, TypedResume<N, Bytes>),
    /// Read one Parquet footer.
    ReadParquetFooter(FileMeta, TypedResume<N, ParquetFooter>),
    /// Read JSON files as ordered [`crate::EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadJson(PageRequest<N, ReadJsonFiles>),
    /// Read Parquet files as ordered [`crate::EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadParquet(PageRequest<N, ReadParquetFiles>),
    #[cfg(feature = "declarative-plans")]
    /// Execute a declarative plan in connector-selected pages.
    ExecutePlan(PageRequest<N, ExecutePlan>),
    /// Write one complete object.
    WriteBytes(WriteBytes, TypedResume<N, ()>),
}

/// One phase of a paginated connector operation.
pub enum PageRequest<N: Send + 'static, Op: PagedOperation> {
    /// Initialize the operation and return its first page.
    Start(Op, TypedResume<N, Page<Op>>),
    /// Initialize the operation and return a cursor without fetching the first page.
    Prepare(Op, TypedResume<N, Cursor<Op>>),
    /// Consume a cursor and return the next page.
    Continue(Cursor<Op>, TypedResume<N, Page<Op>>),
}

impl<N: Send + 'static> Request<N> {
    /// Forward this request to the connector through the given channel.
    async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::ListForward(request) => request.forward_to(parent).await,
            Self::ListBackward(request) => request.forward_to(parent).await,
            Self::ReadSmallFile(file, resume) => {
                resume.resume(parent.read_small_file(file.0, file.1).await)
            }
            Self::ReadParquetFooter(file, resume) => {
                resume.resume(parent.read_parquet_footer(file).await)
            }
            Self::ReadJson(request) => request.forward_to(parent).await,
            Self::ReadParquet(request) => request.forward_to(parent).await,
            #[cfg(feature = "declarative-plans")]
            Self::ExecutePlan(request) => request.forward_to(parent).await,
            Self::WriteBytes(operation, resume) => resume.resume(
                parent
                    .write_bytes(operation.url, operation.data, operation.overwrite)
                    .await,
            ),
        }
    }
}

/// Owns a suspended continuation from an operation's response of type `R` to the next
/// workflow/generator state `N`.
pub struct TypedResume<N, R>(Box<dyn FnOnce(DeltaResult<R>) -> DeltaResult<N> + Send>);

impl<N, R> TypedResume<N, R> {
    /// Resume with connector `response` and run kernel to its next boundary.
    ///
    /// Returns the next workflow or generator state. A connector error is delivered to kernel,
    /// which may handle it or return it; errors while advancing kernel are also returned.
    pub fn resume(self, response: DeltaResult<R>) -> DeltaResult<N> {
        self.0(response)
    }
}

/// Kernel-side handle for typed connector operations.
///
/// It shares a mailbox with the coroutine driver and admits one live request at a time.
pub(crate) struct Channel(Arc<RequestMailbox>);

impl Channel {
    /// Initiate a request/response exchange with the connector
    async fn exchange<Out: Send + 'static, In: Send + 'static>(
        &self,
        outbound: Out,
        pending: impl FnOnce(WeakExchange<Out, In>) -> PendingRequest + Send,
    ) -> DeltaResult<In> {
        let exchange = Arc::new(Exchange::new(outbound));
        self.0.publish(pending(Arc::downgrade(&exchange)))?;
        Wait(exchange).await
    }
}

/// Kernel-side handle passed to a generator body.
///
/// Forward all requests to the underlying [`Channel`] so connector can handle them, while
/// delivering the input of [`Self::yield_item`] calls to the generator's immediate consumer.
pub(crate) struct Yielder<Y> {
    channel: Channel,
    mailbox: Arc<YieldMailbox<Y>>,
}

impl<Y: Send + 'static> Yielder<Y> {
    /// Yield one item and suspend until the consumer resumes the generator.
    ///
    /// An error supplied by the consumer is returned at this await point.
    pub(crate) async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        let emission = Arc::new(Exchange::new(item));
        self.mailbox.publish(Arc::downgrade(&emission))?;
        Wait(emission).await
    }
}

impl<Y> Deref for Yielder<Y> {
    type Target = Channel;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

/// Kernel-side state for consuming a child generator.
pub(crate) enum GeneratorState<W> {
    Start(W),
    Continue(YieldResume<W>),
    Exhausted,
}

impl<Y: Send + 'static> GeneratorState<Generator<(), Y>> {
    /// Return the next yielded item, forwarding connector requests through `parent`.
    ///
    /// Returns `None` after the child generator completes.
    pub(crate) async fn next(&mut self, parent: &Channel) -> DeltaResult<Option<Y>> {
        let state = std::mem::replace(self, Self::Exhausted);
        let mut generator = match state {
            Self::Start(generator) => Ok(generator),
            Self::Continue(resume) => resume.resume(Ok(())),
            Self::Exhausted => return Ok(None),
        };

        loop {
            generator = match generator? {
                Generator::Request(request) => request.forward_to(parent).await,
                Generator::Done(()) => return Ok(None),
                Generator::Yield(item, resume) => {
                    *self = Self::Continue(resume);
                    return Ok(Some(item));
                }
            };
        }
    }
}
