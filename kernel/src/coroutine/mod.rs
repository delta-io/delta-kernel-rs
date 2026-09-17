//! Stackless coroutines that keep connectors in control of execution.
//!
//! Kernel never invokes connector code through this API. An operation returns a lazy [`Workflow`]
//! or [`Generator`], which does no work until it is composed or started. A connector starts a root
//! operation and retains its task. Each call to `advance` runs kernel until the task completes,
//! yields, or posts one [`Request`]. The connector performs that request, completes its [`Reply`],
//! and advances the same task again.
//!
//! ```text
//! connector                 kernel task
//!     |                         |
//!     |------ advance --------->|
//!     |<----- Request ----------|
//!     |                          suspended
//!     | perform requested work  |
//!     |------ Reply::send ------|
//!     |------ advance --------->|
//!     |<----- output -----------|
//! ```
//!
//! The connector owns scheduling throughout this exchange. Requested work may block, await, run in
//! parallel, or cross a process boundary; advancing kernel is always synchronous. A [`Reply`] holds
//! no kernel continuation, and dropping the task abandons the operation.
//!
//! # Choosing the ownership boundary
//!
//! Use [`Workflow::start`] or [`Generator::start`] when no parent kernel task exists. Starting
//! creates an independently driven task and its request channel, so the caller must retain and
//! advance that task.
//!
//! Kernel code that already has a [`Channel`] composes a child operation through
//! [`Workflow::run_with`] or [`Generator::bind`]. The child borrows the parent's channel, and its
//! requests surface from the parent task. Starting a child despite having a parent channel creates
//! an unnecessary second task that needs its own driver.
//!
//! # Driving tasks
//!
//! A workflow driver alternates between [`WorkflowStep::Request`] and
//! [`WorkflowStep::Done`]:
//!
//! ```ignore
//! let mut task = workflow.start();
//! loop {
//!     match task.advance()? {
//!         WorkflowStep::Done(output) => break output,
//!         WorkflowStep::Request(request) => connector.reply(request).await?,
//!     }
//! }
//! ```
//!
//! After receiving a request, complete its reply before advancing the task. The `resume` methods on
//! [`WorkflowTask`] and [`GeneratorTask`] combine those operations. A reply fails if its suspended
//! request was abandoned, which prevents a response from being routed to another request.
//!
//! A started generator additionally returns [`GeneratorStep::Yield`]. The consumer owns the
//! yielded item and must acknowledge its `Reply<()>` before requesting another step. A generator
//! composed with [`Generator::bind`] exposes the same exchange as
//! [`BoundGenerator::next`], which acknowledges each item automatically.
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
//! - [`PageRequest::Continue`] consumes a cursor and returns the next page, with `Some(cursor)`
//!   when more pages remain.
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
//! Kernel exposes at most one connector request per task at a time. Pagination still lets a
//! connector retain state and prefetch work between page requests.
//!
//! # Error handling
//!
//! Sending `Err` delivers a connector failure to the suspended kernel await point. A task cannot be
//! advanced after completion or while its previous request still lacks a response. Dropping the
//! task abandons all associated state.
use std::any::{type_name, Any};
use std::marker::PhantomData;

use delta_kernel_derive::internal_api;
use derive_more::Constructor;

pub use self::core::Reply;
pub use self::kernel::generator::{BoundGenerator, Generator, GeneratorStep, GeneratorTask};
pub(crate) use self::kernel::workflow::WorkflowImpl;
pub use self::kernel::workflow::{Workflow, WorkflowStep, WorkflowTask};
pub use self::kernel::{generator, Channel, Request};
#[cfg(feature = "internal-api")]
use self::write::WriteJsonFile;
#[cfg(feature = "declarative-plans")]
pub use crate::plans::Operation as PlanOperation;
use crate::{DeltaResult, Error};
#[cfg(feature = "internal-api")]
use crate::{Engine, FileMeta};

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

/// Write generated JSON rows through the handlers exposed by `engine`.
#[cfg(feature = "internal-api")]
#[internal_api]
pub(crate) fn write_json_file_with_engine(
    engine: &dyn Engine,
    operation: WriteJsonFile,
) -> DeltaResult<FileMeta> {
    engine::write_json_file_with_engine(engine, operation)
}

/// Drive `workflow` to completion by serving its requests through `engine`.
#[internal_api]
pub(crate) fn drive_workflow<W: Workflow>(
    engine: &dyn crate::Engine,
    workflow: W,
) -> DeltaResult<W::Output> {
    engine::EngineConnector::drive(engine, workflow.start())
}

#[cfg(test)]
mod tests;

/// Describes a connector operation that may return multiple pages.
pub trait PagedOperation: Send + Sized + 'static {
    /// Page payload produced by the connector.
    type Page: Send + 'static;
}

/// One page of connector data and the cursor for requesting more.
#[derive(Constructor)]
pub struct Page<Op: PagedOperation> {
    /// Data returned in this page.
    pub data: Op::Page,
    /// Cursor to pass to [`PageRequest::Continue`], or `None` when the operation is exhausted.
    ///
    /// An empty payload with `Some(cursor)` is not exhaustion.
    pub next: Option<Cursor<Op>>,
}

/// Opaque connector-owned continuation state typed to one paged operation.
///
/// Kernel stores and returns cursors without inspecting their contents.
pub struct Cursor<Op> {
    state: Box<dyn Any + Send>,
    operation: PhantomData<fn() -> Op>,
}

impl<Op> Cursor<Op> {
    /// Wrap connector-owned `state` for a later continuation request.
    pub fn new(state: impl Any + Send + 'static) -> Self {
        Self {
            state: Box::new(state),
            operation: PhantomData,
        }
    }

    /// Recover the connector-owned state.
    ///
    /// Returns an error if `T` is not the type originally passed to [`Self::new`].
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
pub enum PageRequest<Op: PagedOperation> {
    /// Initialize the operation and return its first page.
    Start(Op, Reply<Page<Op>>),
    /// Initialize the operation and return a cursor without fetching the first page.
    Prepare(Op, Reply<Cursor<Op>>),
    /// Consume a cursor and return the next page.
    Continue(Cursor<Op>, Reply<Page<Op>>),
}
