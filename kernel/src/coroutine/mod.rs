//! Stackless, connector-driven kernel coroutines.
//!
//! When a connector invokes a kernel operation such as snapshot creation, kernel delegates all I/O
//! and dataflow operations back to the connector. However, delegation by calling back into
//! connector code would produce nested connector -> kernel -> connector call stacks, with connector
//! blocking on kernel while kernel invokes connector code:
//!
//! ```text
//!   ╔══════════════════════════════ CONNECTOR ══════════════════════════════╗
//!   ║                                                                       ║
//! ----> analyze_query          ┌──── KERNEL ─────┐                          ║
//!   ║     |                    │                 │                          ║
//!   ║     +----------------------> load_snapshot │                          ║
//!   ║                          │     |           │                          ║
//!   ║                          │     +-------------------> list             ║
//!   ║                          │                 │          |               ║
//!   ║                          │                 │          |               ║
//!   ║                          │     + <--------------------+               ║
//!   ║                          │     |           │                          ║
//!   ║                          │     +-------------------> read_json        ║
//!   ║                          │                 │          |               ║
//!   ║                          │                 │          |               ║
//!   ║                          │     + <--------------------+               ║
//!   ║                          │     |           │                          ║
//!   ║                          │     +-------------------> read_parquet     ║
//!   ║                          │                 │          |               ║
//!   ║                          │                 │          |               ║
//!   ║                          │     + <--------------------+               ║
//!   ║                          │     |           │                          ║
//!   ║     + <-----------(snapshot)---+           │                          ║
//!   ║     |                    │                 │                          ║
//! <-------+                    └─────────────────┘                          ║
//!   ║                                                                       ║
//!   ╚═══════════════════════════════════════════════════════════════════════╝
//! ```
//!
//! Because kernel is stuck "inside" the connector, it interferes with connector's scheduling model
//! (sync/async/parallel/distributed), error handling, etc. It also forces connector to implement
//! the complex [`Engine`](crate::Engine) trait hierarchy so kernel can call back out to it.
//!
//! Coroutines flip the control flow around by modeling kernel's work as a resumable continuation
//! function. Connector still invokes a kernel function to start running kernel, but kernel
//! delegates requests back to the connector by suspending itself. When that happens, the
//! connector's function call returns with a (request, resume) pair as a return value, and connector
//! invokes the resume function to pass a response to kernel and allow it to continue. The process
//! repeats until the coroutine finishes and returns the requested value:
//!
//! ```text
//!   ╔══════ CONNECTOR ════════╗          ┌──────── KERNEL ─────────┐
//!   ║                         ║          │                         │
//! ----> analyze_query         ║          │                         │
//!   ║     |                   ║          │                         │
//!   ║     +-----------------------------------> load_snapshot      │
//!   ║                         ║          │        |                │
//!   ║     + <------------(list request, resume)---*                │
//!   ║     |                   ║          │        .                │
//!   ║     +---> list          ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║     + <----+            ║          │        .                │
//!   ║     |                   ║          │        .                │
//!   ║     +---(list response)-----------------> (resume)           │
//!   ║                         ║          │        |                │
//!   ║     + <------------(JSON request, resume)---*                │
//!   ║     |                   ║          │        .                │
//!   ║     +---> read_json     ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║     + <----+            ║          │        .                │
//!   ║     |                   ║          │        .                │
//!   ║     +---(JSON data)---------------------> (resume)           │
//!   ║                         ║          │        |                │
//!   ║     + <---------(parquet request, resume)---*                │
//!   ║     |                   ║          │        .                │
//!   ║     +---> read_parqueet ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║            |            ║          │        .                │
//!   ║     + <----+            ║          │        .                │
//!   ║     |                   ║          │        .                │
//!   ║     +-----(parquet data)----------------> (resume)           │
//!   ║                         ║          │        |                │
//!   ║     + <------------------------(snapshot)---+                │
//!   ║     |                   ║          │                         │
//! <-------+                   ║          │                         │
//!   ║                         ║          │                         │
//!   ╚═════════════════════════╝          └─────────────────────────┘
//! ```
//!
//! This way, the connector always invokes its own code, acting on behalf of kernel as needed. The
//! coroutines are "stackless" because there is no kernel code in the call stack while the connector
//! processes a kernel request. When the connector starts or resumes a kernel coroutine, kernel runs
//! synchronously in the calling thread until it either completes or suspends again (which happens
//! quickly because kernel delegates all slow or heavy work to the connector). At each step, the
//! connector decides how to serve the request and whether to resume kernel.
//!
//! Kernel coroutines come in two flavors:
//! * A [`Workflow`] suspends zero or more times before producing a final output
//! * A [`Generator`] is like a workflow, but can additionally yield zero or more items back to the
//!   connector before producing its final output.
//!
//! Kernel coroutines are an async-friendly synchronous API: Between suspensions, kernel performs
//! only light, bounded CPU work. It delegates all blocking, heavy, or unbounded work to the
//! connector as one or more [`Request`] instances. Connectors therefore do not need to treat kernel
//! calls as scheduling points, and they can schedule requests like any other work. Resume handles
//! are [`Send`] and may move freely between tasks and threads as the workflow progresses. The
//! connector may execute requests synchronously, asynchronously, in parallel with threads, or
//! across distributed workers; and it can use the runtimes and libraries of its choosing to do so.
//!
//! The [`Request`] enum defines the operations kernel may delegate. Each request carries the
//! operation inputs and a [`Resume`] function, which owns the suspended continuation and which
//! declares the expected response type. A connector drives a [`Workflow`] or [`Generator`] by
//! repeatedly matching each request, performing the operation, and resuming kernel with the result.
//!
//! For example, if kernel defines this:
//!
//! ```no_run
//! use delta_kernel::coroutine::listing::ForwardListing;
//! use delta_kernel::coroutine::{PageRequest, Workflow};
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
//!     ReadSmallFile(FileSlice, Resume<N, Bytes>),
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
//!     // Kernel constructs and starts its coroutine here, returning to the caller
//!     // when it needs to make a Request or has a final result ready.
//!     todo!()
//! }
//! ```
//!
//! Then a connector would drive the `public_kernel_entry_point` workflow as follows:
//!
//! ```no_run
//! use delta_kernel::coroutine::listing::ForwardListing;
//! use delta_kernel::coroutine::{Cursor, Page, PageRequest, Request, Workflow};
//! use delta_kernel::DeltaResult;
//! # pub struct Foo;
//! #
//! # pub fn public_kernel_entry_point() -> DeltaResult<Workflow<Foo>> {
//! #    todo!()
//! # }
//!
//! async fn prepare_listing(
//!     request: ForwardListing,
//! ) -> DeltaResult<Cursor<ForwardListing>> {
//!     // Connector creates its cursor state and may begin prefetching here.
//!     todo!()
//! }
//!
//! async fn advance_listing(
//!     cursor: Cursor<ForwardListing>,
//! ) -> DeltaResult<Page<ForwardListing>> {
//!     // Connector resolves its cursor state and fetches the next page here.
//!     todo!()
//! }
//!
//! // Connector decides how to dispatch kernel requests. Here it uses async scheduling.
//! async fn serve_request<N: Send + 'static>(
//!     request: Request<N>,
//! ) -> DeltaResult<N> {
//!     match request {
//!         Request::ListForward(PageRequest::Prepare(args, resume)) => {
//!             resume(prepare_listing(args).await)
//!         }
//!         Request::ListForward(PageRequest::Start(args, resume)) => {
//!             let cursor = prepare_listing(args).await?;
//!             resume(advance_listing(cursor).await)
//!         }
//!         Request::ListForward(PageRequest::Continue(cursor, resume)) => {
//!             resume(advance_listing(cursor).await)
//!         }
//!         _ => Err(delta_kernel::Error::unsupported(
//!             "this connector does not support the requested operation",
//!         )),
//!     }
//! }
//!
//! // Connector also decides how to drive workflows. Here, a generic driver loop. It could also
//! // inject cancellation checks, logging, etc. if desired.
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
//! // A convenient connector-side entry point for the workflow, that hides all of the above.
//! async fn connector_entry_point() -> DeltaResult<Foo> {
//!     connector_workflow_driver(public_kernel_entry_point()).await
//! }
//! ```
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
//! operation).
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
//! - Starting a coroutine returns `DeltaResult<Workflow<_>>` or `DeltaResult<Generator<_>>`. An
//!   error means the workflow failed before producing the next state.
//! - The [`Resume`] function accepts `DeltaResult<R>`. A connector error is delivered to the
//!   suspended kernel await point, where kernel may handle it or propagate it.
//! - [`Resume`] itself returns `DeltaResult`. An error means the workflow failed while kernel was
//!   processing the response or advancing the coroutine.
//!
//! A connector may drop any [`Resume`] handle at any time. This abandons the suspended coroutine
//! and drops all associated state. For connector-side work failures, resume with `Err` when kernel
//! should observe the failure; otherwise drop the handle and return the error directly.
use std::any::{type_name, Any};
use std::marker::PhantomData;

use delta_kernel_derive::internal_api;
use derive_more::Constructor;

#[internal_api]
pub(crate) use self::kernel::{Channel, GeneratorState, Yielder};
pub use self::kernel::{Generator, Request, Workflow};
use self::write::SinkOperation;
#[cfg(feature = "declarative-plans")]
pub use crate::plans::Operation as PlanOperation;
use crate::{DeltaResult, Error};

/// Uninhabited plan operation used when declarative plans are disabled.
///
/// The placeholder keeps [`Request`]'s variant set independent of Cargo feature unification. A
/// dependency can enable `delta_kernel/declarative-plans` without enabling a connector crate's
/// corresponding feature, so cfg-gating `Request::ExecutePlan` would make exhaustive connector
/// matches depend on features selected elsewhere in the dependency graph.
#[cfg(not(feature = "declarative-plans"))]
pub enum PlanOperation {}

#[cfg(feature = "internal-api")]
pub mod core;
#[cfg(not(feature = "internal-api"))]
#[allow(unreachable_pub)]
pub(crate) mod core;
pub(crate) mod engine;
pub mod evaluation;
#[cfg(feature = "internal-api")]
pub mod kernel;
#[cfg(not(feature = "internal-api"))]
#[allow(unreachable_pub)]
pub(crate) mod kernel;
pub mod listing;
pub mod read;
pub mod write;

/// Drive `workflow` synchronously through `engine`.
///
/// Returns the workflow output after serving every connector request.
///
/// # Errors
///
/// Returns an error if the workflow or any connector operation fails.
#[internal_api]
pub(crate) fn drive_workflow<O: Send + 'static>(
    engine: &dyn crate::Engine,
    workflow: DeltaResult<Workflow<O>>,
) -> DeltaResult<O> {
    engine::EngineConnector::drive(engine, workflow)
}

#[cfg(test)]
mod tests;

/// Resume handle for one yielded generator item.
///
/// Pass `Ok(())` to continue or an error for kernel to observe at the suspended yield.
pub type YieldResume<N> = Resume<N, ()>;

/// Describes a connector operation that may return multiple pages.
pub trait PagedOperation: Send + Sized + 'static {
    /// Data returned in one page.
    type Page: Send + 'static;
}

/// One page of connector data and the cursor for requesting more.
#[derive(Constructor)]
pub struct Page<Op: PagedOperation> {
    /// Data returned in this page.
    pub data: Op::Page,
    /// Cursor to pass to `Continue`, or `None` when no pages remain.
    pub next: Option<Cursor<Op>>,
}

/// Opaque connector state typed to one operation.
pub struct Cursor<Op> {
    state: Box<dyn Any + Send>,
    operation: PhantomData<fn() -> Op>,
}

impl<Op> Cursor<Op> {
    /// Construct operation state owned by this cursor.
    pub fn new(state: impl Any + Send + 'static) -> Self {
        Self {
            state: Box::new(state),
            operation: PhantomData,
        }
    }

    /// Attempt to consume this cursor by casting and returning its connector-defined state.
    pub fn into_inner<T: Any>(self) -> DeltaResult<T> {
        self.state.downcast().map(|inner| *inner).map_err(|_| {
            Error::internal_error(format!(
                "invalid cursor: unable to cast as {}",
                type_name::<T>()
            ))
        })
    }
}

/// One phase of a paginated connector operation.
pub enum PageRequest<N: Send + 'static, Op: PagedOperation> {
    /// Initialize the operation and return its first page.
    Start(Op, Resume<N, Page<Op>>),
    /// Initialize the operation and return a cursor without fetching the first page.
    Prepare(Op, Resume<N, Cursor<Op>>),
    /// Consume a cursor and return the next page.
    Continue(Cursor<Op>, Resume<N, Page<Op>>),
}

/// One phase of a connector-managed sink.
pub enum SinkRequest<N: Send + 'static, Op: SinkOperation> {
    /// Initialize the sink.
    Start(Op, Resume<N, Cursor<Op>>),
    /// Write one input item and return the sink for the next phase.
    Write(Cursor<Op>, Op::Input, Resume<N, Cursor<Op>>),
    /// Finish the sink and return its output.
    Finish(Cursor<Op>, Resume<N, Op::Output>),
}

/// Resumes a suspended [`Workflow`] or [`Generator`] with connector's `response`, advancing it to
/// the next communication point (suspend, yield, or completion).
pub type Resume<N, R> = Box<dyn FnOnce(DeltaResult<R>) -> DeltaResult<N> + Send>;
