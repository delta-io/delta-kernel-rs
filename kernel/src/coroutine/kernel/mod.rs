//! Kernel workflows and generators post [`Request`]s through a [`Channel`] and await connector
//! replies.
//!
//! Each operation owns the receiving end of its request channel. Generator bodies receive the
//! same request channel plus a separate lane for yielded items.
use bytes::Bytes;
use delta_kernel_derive::internal_api;
use derive_more::From;

use super::evaluation::{
    CreateEngineData, CreateExpressionEvaluator, EvaluateExpression, EvaluateFilteredExpression,
    EvaluatorHandle,
};
use super::listing::{BackwardListing, ForwardListing};
use super::read::{ReadJsonFiles, ReadParquetFiles};
use super::write::{CopyAtomic, WriteBytes, WriteJsonFile};
use super::{PageRequest, PlanOperation, Reply};
use crate::committer::{Commit, CommitResponse, PublishMetadata};
use crate::coroutine::{generator, workflow};
use crate::{EngineData, FileMeta, FileSlice, FilteredEngineData, ParquetFooter};

mod channel;

#[doc(inline)]
#[internal_api]
pub(crate) use channel::ChannelExt;

/// Operations kernel can request from a connector.
#[derive(From)]
pub enum Request {
    /// List a bounded path range in ascending order.
    ListForward(PageRequest<ForwardListing>),
    /// List page ranges from high to low, with entries ascending within each page.
    ListBackward(PageRequest<BackwardListing>),
    /// Read a whole file when the range is `None`, or exactly the half-open range otherwise.
    ReadSmallFile(FileSlice, Reply<Bytes>),
    /// Read one Parquet footer.
    ReadParquetFooter(FileMeta, Reply<ParquetFooter>),
    /// Read JSON files as ordered [`EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadJson(PageRequest<ReadJsonFiles>),
    /// Read Parquet files as ordered [`EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadParquet(PageRequest<ReadParquetFiles>),
    /// Execute a declarative plan in connector-selected pages.
    ExecutePlan(PageRequest<PlanOperation>),
    /// Materialize scalar rows as one [`EngineData`] batch.
    CreateEngineData(CreateEngineData, Reply<Box<dyn EngineData>>),
    /// Prepare an expression evaluator for repeated use.
    CreateExpressionEvaluator(CreateExpressionEvaluator, Reply<EvaluatorHandle>),
    /// Evaluate a prepared expression against one [`EngineData`] batch.
    EvaluateExpression(EvaluateExpression, Reply<Box<dyn EngineData>>),
    /// Evaluate a prepared expression while preserving a batch's row selection.
    EvaluateFilteredExpression(EvaluateFilteredExpression, Reply<FilteredEngineData>),
    /// Write one newline-delimited JSON file from streamed row batches.
    // Boxed to keep the request enum small without boxing every variant.
    WriteJson(Box<WriteJsonFile>, Reply<FileMeta>),
    /// Write one complete object.
    WriteBytes(WriteBytes, Reply<()>),
    /// Atomically copy one immutable object to a new destination.
    CopyAtomic(CopyAtomic, Reply<()>),
    /// Commit one prepared transaction through a connector-selected committer.
    Commit(Box<Commit>, Reply<CommitResponse>),
    /// Publish catalog commits through the connector's catalog committer.
    Publish(PublishMetadata, Reply<()>),
}

/// Borrowed capability used by kernel operations to post requests through their parent coroutine.
///
/// Workflow implementations receive this channel as an ordinary async-function argument. Child
/// generators use it for the lifetime of their [`Generator`].
pub type Channel = super::Channel<Request>;

/// Active streaming kernel operation using an existing request channel.
///
/// Connector requests surface through the driver that owns the channel. The `'task` lifetime
/// covers that channel and any state captured by the operation.
pub type Generator<'task, Y> = generator::Generator<'task, Y>;

/// Kernel generator that owns all captured state and its request channel.
pub type StaticGenerator<Y> = generator::StaticGenerator<Request, Y>;

/// Yield, request, or completion from a kernel [`StaticGenerator`].
///
/// Yielded items are acknowledged before this step returns. Requests carry their own replies.
pub type GeneratorStep<Y> = generator::GeneratorStep<Request, Y>;

/// Independently driven kernel operation with an owned request channel.
///
/// Process each returned request and complete its reply.
pub type Workflow<'task, O> = workflow::Workflow<'task, Request, O>;

/// Kernel workflow that owns all captured state.
pub type StaticWorkflow<O> = workflow::StaticWorkflow<Request, O>;

/// Request or completion from a kernel [`Workflow`].
pub type WorkflowStep<O> = workflow::WorkflowStep<Request, O>;

/// Unbound kernel generator that can later bind to a parent channel or become a static generator.
pub type UnboundGenerator<Y> = generator::UnboundGenerator<Request, Y>;
pub(crate) type GeneratorChannel<'call, Y> = generator::GeneratorChannel<'call, Request, Y>;
pub(crate) use generator::unbound_generator;

/// Creates a [`Workflow`] that may borrow captured state.
#[internal_api]
macro_rules! workflow {
    ($body:expr) => {
        $crate::coroutine::workflow::workflow!($crate::coroutine::kernel::Request, $body)
    };
}

/// Creates an unpolled [`StaticWorkflow`] from a static workflow body.
#[internal_api]
macro_rules! static_workflow {
    ($body:expr) => {
        $crate::coroutine::workflow::static_workflow!($crate::coroutine::kernel::Request, $body)
    };
}
