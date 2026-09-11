//! Kernel-side coroutine support.
//!
//! On the kernel side, [`Workflow`] and [`Generator`] coroutines are just async functions. They
//! communicate with the connector via a [`Channel`] whose async methods turn requests into futures
//! that kernel code `await`s to receive the connector's response. Normal rust async machinery takes
//! over from there (polling, compiler-generated stack ripping and state machines, etc).
//!
//! [`Workflow::start`], forms the boundary between kernel's public (sync) entry points and the
//! future that encapsulates a workflow's logic. Starting or resuming a coroutine
//! [polls](std::future::Future::poll) the workflow's future, allowing it to run. If that poll
//! returns [`Poll::Ready`](std::task::Poll::Ready), the coroutine completed and leads to
//! [`Workflow::Done`]; otherwise, the workflow `await`ed a [`Channel`] method future and the
//! coroutine machinery extracts the corresponding request as [`Workflow::Request`]. When the
//! connector invokes the [`Resume`], the coroutine machinery makes the response
//! available to the workflow and then resumes it by polling again. That process repeats until the
//! workflow completes or the connector abandons it.
//!
//! It is important to note that there is no async runtime; everything happens on the calling
//! thread, polling with a no-op [`Waker`](std::task::Waker::noop). Workflows only advance if the
//! connector directly polls their future by calling [`Workflow::start`] or
//! [`Resume`].
//!
//! Generators are special in that they communicate over two channels: Delegated requests go over a
//! normal [`Channel`] for the connector to handle, while yielded output items go through a
//! [`Yielder`] for the generator's owner to consume. This allows kernel to create and consume
//! generators internally while still forwarding all requests to the connector. The
//! [`GeneratorState`] wrapper provides an iterator-like surface for kernel-side generator
//! consumption, used for operations such as incremental read CRC and log replay that must
//! manipulate and consume multiple streams of connector-provided data.
use bytes::Bytes;
use delta_kernel_derive::internal_api;
use derive_more::{Deref, From};

use super::core::{
    self, DeltaFuture, GeneratorTask, IntoRequest, OutboxEntry, Pending, Step, WorkflowTask,
    YieldChannel,
};
use super::evaluation::{
    CreateEngineData, CreateExpressionEvaluator, EvaluateExpression, EvaluateFilteredExpression,
    EvaluatorHandle,
};
use super::listing::{BackwardListing, ForwardListing};
use super::read::{ReadJsonFiles, ReadParquetFiles};
use super::write::{
    CopyAtomic, PendingSinkRequest, SinkOperation, SinkWrite, WriteBytes, WriteJsonFile,
};
use super::{
    Cursor, Page, PageRequest, PagedOperation, PlanOperation, Resume, SinkRequest, YieldResume,
};
use crate::committer::{Commit, CommitResponse, PublishMetadata};
use crate::{DeltaResult, EngineData, FileMeta, FileSlice, FilteredEngineData, ParquetFooter};

mod channel;

pub(crate) use channel::EngineDataOperation;

/// A workflow that has either completed or suspended with a request for the connector.
///
/// Drive every [`Request`] until the workflow produces [`Workflow::Done`]. Dropping a suspended
/// workflow abandons the operation and records it as a failure in its tracing span.
#[must_use = "a workflow must be driven until it produces Workflow::Done"]
#[derive(From)]
pub enum Workflow<O: Send + 'static> {
    /// Final workflow output.
    Done(O),
    /// Operation the connector must perform before resuming the workflow.
    Request(Request<Self>),
}

/// A connector-facing generator that can complete, yield an item, or request connector work.
///
/// `Y` is the yielded item type. `O` is the value produced on [`Generator::Done`] and defaults
/// to `()` because most generators only yield and do not produce a final output value.
#[must_use = "a generator must be driven to produce a useful output"]
#[derive(From)]
pub enum Generator<Y: Send + 'static, O: Send + 'static = ()> {
    /// Final generator output.
    Done(O),
    /// Item offered to the generator's consumer.
    Yield(Y, YieldResume<Self>),
    /// Operation the connector must perform before resuming the generator.
    Request(Request<Self>),
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
    ReadSmallFile(FileSlice, Resume<N, Bytes>),
    /// Read one Parquet footer.
    ReadParquetFooter(FileMeta, Resume<N, ParquetFooter>),
    /// Read JSON files as ordered [`EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadJson(PageRequest<N, ReadJsonFiles>),
    /// Read Parquet files as ordered [`EngineData`] batches: Each file may produce multiple
    /// batches, but batches may not span multiple files.
    ReadParquet(PageRequest<N, ReadParquetFiles>),
    /// Execute a declarative plan in connector-selected pages.
    ExecutePlan(PageRequest<N, PlanOperation>),
    /// Materialize scalar rows as one [`EngineData`] batch.
    CreateEngineData(CreateEngineData, Resume<N, Box<dyn EngineData>>),
    /// Prepare an expression evaluator for repeated use.
    CreateExpressionEvaluator(CreateExpressionEvaluator, Resume<N, EvaluatorHandle>),
    /// Evaluate a prepared expression against one [`EngineData`] batch.
    EvaluateExpression(EvaluateExpression, Resume<N, Box<dyn EngineData>>),
    /// Evaluate a prepared expression while preserving a batch's row selection.
    EvaluateFilteredExpression(EvaluateFilteredExpression, Resume<N, FilteredEngineData>),
    /// Write one newline-delimited JSON file from streamed row batches.
    WriteJson(SinkRequest<N, WriteJsonFile>),
    /// Write one complete object.
    WriteBytes(WriteBytes, Resume<N, ()>),
    /// Atomically copy one immutable object to a new destination.
    CopyAtomic(CopyAtomic, Resume<N, ()>),
    /// Commit one prepared transaction through a connector-selected committer.
    Commit(Box<Commit>, Resume<N, CommitResponse>),
    /// Publish catalog commits through the connector's catalog committer.
    Publish(PublishMetadata, Resume<N, ()>),
}

/// Kernel-side handle for typed connector operations.
#[internal_api]
pub(crate) struct Channel(core::Channel<KernelPending>);

impl Channel {
    /// Initiate a request/response exchange with the connector.
    async fn exchange<Out: Send + 'static, In: Send + 'static, P>(
        &self,
        outbound: Out,
        pending: impl FnOnce(Pending<Out, In>) -> P + Send,
    ) -> DeltaResult<In>
    where
        P: Into<PendingRequest>,
    {
        let pending = |exchange| KernelPending(pending(exchange).into());
        self.0.exchange(outbound, pending).await
    }
}

/// Kernel-side handle passed to a generator body.
#[internal_api]
#[derive(Deref)]
pub(crate) struct Yielder<Y: Send + 'static> {
    #[deref]
    channel: Channel,
    yields: YieldChannel<Y>,
}

impl<Y: Send + 'static> Yielder<Y> {
    /// Yield one item and suspend until the consumer resumes the generator.
    ///
    /// An error supplied by the consumer is returned at this await point.
    #[internal_api]
    pub(crate) async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        self.yields.yield_item(item).await
    }
}

impl<O: Send + 'static> Workflow<O> {
    /// Start a workflow and run it until completion or its first connector request.
    #[internal_api]
    pub(crate) fn start<Fut>(workflow: impl FnOnce(Channel) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        WorkflowTask::new(|channel| workflow(Channel(channel))).step()
    }
}

impl<Y: Send + 'static, O: Send + 'static> Generator<Y, O> {
    /// Start a generator and run it until completion, its first yield, or a connector request.
    #[internal_api]
    pub(crate) fn start<Fut>(generator: impl FnOnce(Yielder<Y>) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let task = GeneratorTask::new(|channel, yields| {
            let channel = Channel(channel);
            generator(Yielder { channel, yields })
        });
        task.step()
    }
}

/// Kernel-side state for consuming a child generator.
#[internal_api]
pub(crate) enum GeneratorState<W> {
    Start(W),
    Continue(YieldResume<W>),
    Exhausted,
}

impl<Y: Send + 'static> GeneratorState<Generator<Y>> {
    /// Return the next yielded item, forwarding connector requests through `parent`.
    ///
    /// Returns `None` after the child generator completes.
    #[internal_api]
    pub(crate) async fn next(&mut self, parent: &Channel) -> DeltaResult<Option<Y>> {
        let state = std::mem::replace(self, Self::Exhausted);
        let mut generator = match state {
            Self::Start(generator) => Ok(generator),
            Self::Continue(resume) => resume(Ok(())),
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

impl<N: Send + 'static, Op: PagedOperation> PageRequest<N, Op> {
    /// Forward this paginated request through `parent`.
    async fn forward_to(self, parent: &Channel) -> DeltaResult<N>
    where
        PendingPageRequest<Op>: Into<PendingRequest>,
    {
        match self {
            Self::Start(op, resume) => resume(parent.exchange(op, PendingPageRequest::Start).await),
            Self::Prepare(op, resume) => {
                resume(parent.exchange(op, PendingPageRequest::Prepare).await)
            }
            Self::Continue(cursor, resume) => {
                resume(parent.exchange(cursor, PendingPageRequest::Continue).await)
            }
        }
    }
}

impl<N: Send + 'static, Op: SinkOperation> SinkRequest<N, Op> {
    /// Forward this sink request through `parent`.
    async fn forward_to(self, parent: &Channel) -> DeltaResult<N>
    where
        PendingSinkRequest<Op>: Into<PendingRequest>,
    {
        match self {
            Self::Start(op, resume) => resume(parent.exchange(op, PendingSinkRequest::Start).await),
            Self::Write(sink, data, resume) => {
                let outbound = SinkWrite::new(sink, data);
                resume(parent.exchange(outbound, PendingSinkRequest::Write).await)
            }
            Self::Finish(sink, resume) => {
                resume(parent.exchange(sink, PendingSinkRequest::Finish).await)
            }
        }
    }
}

impl<N: Send + 'static> Request<N> {
    /// Forward this request to the connector through the given channel.
    async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::ListForward(request) => request.forward_to(parent).await,
            Self::ListBackward(request) => request.forward_to(parent).await,
            Self::ReadSmallFile(file, resume) => {
                resume(parent.exchange(file, PendingRequest::ReadSmallFile).await)
            }
            Self::ReadParquetFooter(file, resume) => {
                let exchange = parent.exchange(file, PendingRequest::ReadParquetFooter);
                resume(exchange.await)
            }
            Self::ReadJson(request) => request.forward_to(parent).await,
            Self::ReadParquet(request) => request.forward_to(parent).await,
            Self::ExecutePlan(request) => request.forward_to(parent).await,
            Self::CreateEngineData(op, resume) => {
                let exchange = parent.exchange(op, PendingRequest::CreateEngineData);
                resume(exchange.await)
            }
            Self::CreateExpressionEvaluator(op, resume) => {
                let exchange = parent.exchange(op, PendingRequest::CreateExpressionEvaluator);
                resume(exchange.await)
            }
            Self::EvaluateExpression(op, resume) => {
                let exchange = parent.exchange(op, PendingRequest::EvaluateExpression);
                resume(exchange.await)
            }
            Self::EvaluateFilteredExpression(op, resume) => {
                let exchange = parent.exchange(op, PendingRequest::EvaluateFilteredExpression);
                resume(exchange.await)
            }
            Self::WriteJson(request) => request.forward_to(parent).await,
            Self::WriteBytes(op, resume) => {
                resume(parent.exchange(op, PendingRequest::WriteBytes).await)
            }
            Self::CopyAtomic(op, resume) => {
                resume(parent.exchange(op, PendingRequest::CopyAtomic).await)
            }
            Self::Commit(op, resume) => resume(parent.exchange(*op, PendingRequest::Commit).await),
            Self::Publish(metadata, resume) => {
                resume(parent.exchange(metadata, PendingRequest::Publish).await)
            }
        }
    }
}

/// Opaque pending-request vocabulary for kernel coroutines.
#[derive(Default)]
struct KernelPending(PendingRequest);

impl OutboxEntry for KernelPending {
    fn is_live(&self) -> bool {
        self.0.is_live()
    }
}

impl<N: Send + 'static> IntoRequest<N> for KernelPending {
    type Request = Request<N>;

    fn into_request(self, step: impl Step<N>) -> DeltaResult<Self::Request> {
        self.0.into_request(step)
    }
}

/// Kernel pending requests stored in the outbox while a coroutine is suspended.
#[derive(From)]
enum PendingRequest {
    #[from]
    ListForward(PendingPageRequest<ForwardListing>),
    #[from]
    ListBackward(PendingPageRequest<BackwardListing>),
    ReadSmallFile(Pending<FileSlice, Bytes>),
    ReadParquetFooter(Pending<FileMeta, ParquetFooter>),
    #[from]
    ReadJson(PendingPageRequest<ReadJsonFiles>),
    #[from]
    ReadParquet(PendingPageRequest<ReadParquetFiles>),
    #[from]
    ExecutePlan(PendingPageRequest<PlanOperation>),
    CreateEngineData(Pending<CreateEngineData, Box<dyn EngineData>>),
    CreateExpressionEvaluator(Pending<CreateExpressionEvaluator, EvaluatorHandle>),
    EvaluateExpression(Pending<EvaluateExpression, Box<dyn EngineData>>),
    EvaluateFilteredExpression(Pending<EvaluateFilteredExpression, FilteredEngineData>),
    #[from]
    WriteJson(PendingSinkRequest<WriteJsonFile>),
    WriteBytes(Pending<WriteBytes, ()>),
    CopyAtomic(Pending<CopyAtomic, ()>),
    Commit(Pending<Commit, CommitResponse>),
    Publish(Pending<PublishMetadata, ()>),
}

/// A suspended phase of a paginated operation.
enum PendingPageRequest<Op: PagedOperation> {
    Start(Pending<Op, Page<Op>>),
    Prepare(Pending<Op, Cursor<Op>>),
    Continue(Pending<Cursor<Op>, Page<Op>>),
}

impl Default for PendingRequest {
    fn default() -> Self {
        // Any variant with an empty weak marks an empty outbox.
        Self::ReadSmallFile(Pending::default())
    }
}

impl PendingRequest {
    fn is_live(&self) -> bool {
        match self {
            Self::ListForward(pending) => pending.is_live(),
            Self::ListBackward(pending) => pending.is_live(),
            Self::ReadSmallFile(exchange) => exchange.is_live(),
            Self::ReadParquetFooter(exchange) => exchange.is_live(),
            Self::ReadJson(pending) => pending.is_live(),
            Self::ReadParquet(pending) => pending.is_live(),
            Self::ExecutePlan(pending) => pending.is_live(),
            Self::CreateEngineData(exchange) => exchange.is_live(),
            Self::CreateExpressionEvaluator(exchange) => exchange.is_live(),
            Self::EvaluateExpression(exchange) => exchange.is_live(),
            Self::EvaluateFilteredExpression(exchange) => exchange.is_live(),
            Self::WriteJson(pending) => pending.is_live(),
            Self::WriteBytes(exchange) => exchange.is_live(),
            Self::CopyAtomic(exchange) => exchange.is_live(),
            Self::Commit(exchange) => exchange.is_live(),
            Self::Publish(exchange) => exchange.is_live(),
        }
    }
}

impl PendingRequest {
    fn into_request<N: Send + 'static>(self, step: impl Step<N>) -> DeltaResult<Request<N>> {
        match self {
            Self::ListForward(pending) => Ok(Request::ListForward(pending.into_request(step)?)),
            Self::ListBackward(pending) => Ok(Request::ListBackward(pending.into_request(step)?)),
            Self::ReadSmallFile(exchange) => exchange.into_request(step, Request::ReadSmallFile),
            Self::ReadParquetFooter(exchange) => {
                exchange.into_request(step, Request::ReadParquetFooter)
            }
            Self::ReadJson(pending) => Ok(Request::ReadJson(pending.into_request(step)?)),
            Self::ReadParquet(pending) => Ok(Request::ReadParquet(pending.into_request(step)?)),
            Self::ExecutePlan(pending) => Ok(Request::ExecutePlan(pending.into_request(step)?)),
            Self::CreateEngineData(exchange) => {
                exchange.into_request(step, Request::CreateEngineData)
            }
            Self::CreateExpressionEvaluator(exchange) => {
                exchange.into_request(step, Request::CreateExpressionEvaluator)
            }
            Self::EvaluateExpression(exchange) => {
                exchange.into_request(step, Request::EvaluateExpression)
            }
            Self::EvaluateFilteredExpression(exchange) => {
                exchange.into_request(step, Request::EvaluateFilteredExpression)
            }
            Self::WriteJson(pending) => Ok(Request::WriteJson(pending.into_request(step)?)),
            Self::WriteBytes(exchange) => exchange.into_request(step, Request::WriteBytes),
            Self::CopyAtomic(exchange) => exchange.into_request(step, Request::CopyAtomic),
            Self::Commit(exchange) => exchange.into_request(step, |operation, resume| {
                Request::Commit(Box::new(operation), resume)
            }),
            Self::Publish(exchange) => exchange.into_request(step, Request::Publish),
        }
    }
}

impl<Op: PagedOperation> PendingPageRequest<Op> {
    /// False if the underlying weak reference is empty.
    fn is_live(&self) -> bool {
        match self {
            Self::Start(exchange) => exchange.is_live(),
            Self::Prepare(exchange) => exchange.is_live(),
            Self::Continue(exchange) => exchange.is_live(),
        }
    }

    fn into_request<N: Send + 'static>(
        self,
        step: impl Step<N>,
    ) -> DeltaResult<PageRequest<N, Op>> {
        match self {
            Self::Start(exchange) => exchange.into_request(step, PageRequest::Start),
            Self::Prepare(exchange) => exchange.into_request(step, PageRequest::Prepare),
            Self::Continue(exchange) => exchange.into_request(step, PageRequest::Continue),
        }
    }
}
