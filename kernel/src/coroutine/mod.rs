//! Stackless coroutines that keep connectors in control of execution.
//!
//! Kernel never invokes connector code through this API. Instead, it suspends itself whenever it
//! needs the connector to perform work on its behalf. The connector receives each [`Request`] as an
//! ordinary return value from the coroutine, performs the work, and resumes the suspended kernel
//! operation with the result.
//!
//! This module provides two coroutine forms. Both may issue zero or more requests. A [`Workflow`]
//! completes with one final output while a [`Generator`] interleaves requests with a stream of
//! yielded items before completing.
//!
//! ```text
//! connector              kernel operation
//!     |                         |
//!     |------- advance -------->|
//!     |<------- Request ---------|
//!     |                          suspended
//!     | perform requested work  |
//!     |--------- reply --------->|
//!     |------- advance -------->|
//!     |<----- output -----------|
//! ```
//!
//! The connector owns scheduling throughout this exchange. Requested work may block, await, run in
//! parallel, or cross a process boundary; advancing kernel is always synchronous. A [`Reply`] holds
//! no kernel continuation, and dropping the workflow or generator abandons the operation.
//!
//! # Choosing the ownership boundary
//!
//! A [`Workflow`] already owns its request channel. For a streaming operation without a parent,
//! pass owned inputs through [`StaticGenerator::new`]. Its callback must return
//! `Generator<'static, _>`, statically preventing borrowed state from escaping.
//!
//! Kernel code that already has a [`kernel::Channel`] calls async workflow implementations
//! directly. Passing that channel to a streaming operation returns a [`Generator`] whose lifetime
//! also covers any other borrowed inputs. Requests then surface from the parent coroutine.
//!
//! # Driving operations
//!
//! A workflow driver advances through [`WorkflowStep::Request`] and [`WorkflowStep::Done`]:
//!
//! ```ignore
//! let mut workflow = workflow;
//! loop {
//!     match workflow.advance()? {
//!         WorkflowStep::Done(output) => break output,
//!         WorkflowStep::Request(request) => {
//!             connector.reply(request).await?;
//!         }
//!     }
//! }
//! ```
//!
//! Each workflow request carries a single-use [`Reply`] tied to its originating await point.
//!
//! Process static generator requests and complete their replies. Calling `advance` after
//! [`GeneratorStep::Done`] returns an error. Static generators and [`Generator::next`] acknowledge
//! yielded items automatically.
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
//! Kernel exposes at most one connector request per operation at a time. Pagination still lets a
//! connector retain state and prefetch work between page requests.
//!
//! # Error handling
//!
//! Replying with `Err` delivers a connector failure to the suspended Kernel await point. Calling
//! `advance` after completion or without a live request returns an error.
use std::any::{type_name, Any};

use delta_kernel_derive::internal_api;
use derive_more::Constructor;

pub use self::core::Reply;
#[doc(inline)]
#[internal_api]
pub(crate) use self::core::{Channel, ChannelExchange, DeltaFuture};
#[doc(inline)]
#[internal_api]
pub(crate) use self::kernel::ChannelExt;
pub use self::kernel::{
    Generator, GeneratorStep, Request, StaticGenerator, StaticWorkflow, UnboundGenerator, Workflow,
    WorkflowStep,
};
#[cfg(feature = "declarative-plans")]
pub use crate::plans::Operation as PlanOperation;
use crate::utils::PhantomType;
use crate::{DeltaResult, Error};

/// Uninhabited plan operation used when declarative plans are disabled.
///
/// The placeholder keeps [`Request`]'s variant set independent of Cargo feature unification. A
/// dependency can enable `delta_kernel/declarative-plans` without enabling a connector crate's
/// corresponding feature, so cfg-gating `Request::ExecutePlan` would make exhaustive connector
/// matches depend on features selected elsewhere in the dependency graph.
#[cfg(not(feature = "declarative-plans"))]
pub enum PlanOperation {}

mod core;
pub(crate) mod engine;
pub mod evaluation;
// cbindgen parses cfg-exclusive module declarations together. Ignore each private declaration so
// generic types from its public counterpart are not registered twice.
#[cfg(feature = "internal-api")]
pub mod generator;
#[cfg(not(feature = "internal-api"))]
/// cbindgen:ignore
#[allow(unreachable_pub)]
pub(crate) mod generator;
#[cfg(feature = "internal-api")]
pub mod kernel;
#[cfg(not(feature = "internal-api"))]
/// cbindgen:ignore
#[allow(unreachable_pub)]
pub(crate) mod kernel;
pub mod listing;
pub mod read;
#[cfg(feature = "internal-api")]
pub mod workflow;
#[cfg(not(feature = "internal-api"))]
/// cbindgen:ignore
#[allow(unreachable_pub)]
pub(crate) mod workflow;
pub mod write;

#[doc(inline)]
#[internal_api]
pub(crate) use self::engine::drive_storage;
#[doc(inline)]
#[internal_api]
pub(crate) use self::engine::drive_workflow;
#[cfg(feature = "internal-api")]
#[doc(inline)]
pub use self::engine::{
    run_workflow_with_connector, run_workflow_with_engine, run_workflow_with_storage,
};

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
    operation: PhantomType<Op>,
}

impl<Op> Cursor<Op> {
    /// Wrap connector-owned `state` for a later continuation request.
    pub fn new(state: impl Any + Send + 'static) -> Self {
        Self {
            state: Box::new(state),
            operation: PhantomType::default(),
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
