//! Kernel workflows and generators post [`Request`]s through a [`Channel`] and await connector
//! replies.
//!
//! Each task owns the receiving end of its request channel. [`Yielder`] gives generator bodies the
//! same request channel plus a separate lane for yielded items.
use bytes::Bytes;
use delta_kernel_derive::internal_api;
use derive_more::{Deref, From};

use super::evaluation::{
    CreateEngineData, CreateExpressionEvaluator, EvaluateExpression, EvaluateFilteredExpression,
    EvaluatorHandle,
};
use super::listing::{BackwardListing, ForwardListing};
use super::read::{ReadJsonFiles, ReadParquetFiles};
use super::write::{CopyAtomic, WriteBytes, WriteJsonFile};
use super::{core, PageRequest, PlanOperation, Reply};
use crate::committer::{Commit, CommitResponse, PublishMetadata};
use crate::{DeltaResult, EngineData, FileMeta, FileSlice, FilteredEngineData, ParquetFooter};

mod channel;
pub mod generator;
pub mod workflow;

pub(crate) use channel::EngineDataOperation;

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

/// Borrowed capability used by kernel operations to post requests through their parent task.
///
/// Child workflows receive this channel through [`workflow::Workflow::run_with`], and child
/// generators receive it through [`generator::Generator::bind`].
#[derive(From)]
pub struct Channel(core::Channel<Request>);

impl Channel {
    async fn exchange<Out: Send + 'static, In: Send + 'static, P: Into<Request>>(
        &self,
        outbound: Out,
        make_request: impl FnOnce(Out, Reply<In>) -> P,
    ) -> DeltaResult<In> {
        self.0
            .request(|reply| make_request(outbound, reply).into())
            .await
    }
}

/// Generator access to connector requests and yielded output.
#[internal_api]
#[derive(Deref)]
pub(crate) struct Yielder<'a, Y: Send + 'static> {
    #[deref]
    channel: &'a Channel,
    yields: &'a core::Channel<YieldRequest<Y>>,
}

impl<Y: Send + 'static> Yielder<'_, Y> {
    /// Yield one item and suspend until the consumer resumes the generator.
    ///
    /// An error supplied by the consumer is returned at this await point.
    #[internal_api]
    pub(crate) async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        self.yields
            .request(|reply| YieldRequest { item, reply })
            .await
    }

    /// Forward every item from `generator`.
    pub(crate) async fn yield_all<G>(&self, generator: G) -> DeltaResult<()>
    where
        G: generator::Generator<Y>,
    {
        let mut generator = generator.bind(self.channel);
        while let Some(item) = generator.next().await? {
            self.yield_item(item).await?;
        }
        Ok(())
    }
}

pub(crate) struct YieldRequest<Y> {
    pub(crate) item: Y,
    pub(crate) reply: Reply<()>,
}
