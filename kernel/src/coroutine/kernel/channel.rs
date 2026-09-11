//! Kernel-side typed connector operations.

use std::ops::Range;

use bytes::Bytes;
use delta_kernel_derive::internal_api;
use url::Url;

use super::{Channel, PendingPageRequest, PendingRequest};
use crate::committer::{Commit, CommitMetadata, CommitResponse, PublishMetadata};
use crate::coroutine::core::DeltaFuture;
use crate::coroutine::evaluation::{
    CreateEngineData, CreateExpressionEvaluator, EvaluateExpression, EvaluateFilteredExpression,
    EvaluatorHandle,
};
use crate::coroutine::listing::{BackwardListing, ForwardListing, ListingBounds};
use crate::coroutine::read::{ReadJsonFiles, ReadParquetFiles};
use crate::coroutine::write::{
    CopyAtomic, PendingSinkRequest, SinkWrite, WriteBytes, WriteJsonFile,
};
use crate::coroutine::{Cursor, Generator, Page, PagedOperation};
use crate::engine_data::EngineData;
#[cfg(feature = "declarative-plans")]
use crate::plans::Operation as PlanOperation;
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileIndex, FileMeta, FilteredEngineData, ParquetFooter, PredicateRef};

impl Channel {
    /// Read one small file, or one half-open byte range, completely into memory.
    pub(crate) async fn read_small_file(
        &self,
        url: Url,
        range: Option<Range<FileIndex>>,
    ) -> DeltaResult<Bytes> {
        self.exchange((url, range), PendingRequest::ReadSmallFile)
            .await
    }

    /// Read a Parquet file footer.
    pub(crate) async fn read_parquet_footer(&self, file: FileMeta) -> DeltaResult<ParquetFooter> {
        self.exchange(file, PendingRequest::ReadParquetFooter).await
    }

    /// Initialize a JSON read and return its first page.
    pub(crate) async fn start_read_json(
        &self,
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Page<ReadJsonFiles>> {
        let outbound = ReadJsonFiles::new(files, physical_schema, predicate);
        self.exchange(outbound, PendingPageRequest::Start).await
    }

    /// Initialize a JSON read without fetching its first page.
    #[allow(dead_code)]
    pub(crate) async fn prepare_read_json(
        &self,
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Cursor<ReadJsonFiles>> {
        let outbound = ReadJsonFiles::new(files, physical_schema, predicate);
        self.exchange(outbound, PendingPageRequest::Prepare).await
    }

    /// Continue a JSON read from `cursor`.
    pub(crate) async fn continue_read_json(
        &self,
        cursor: Cursor<ReadJsonFiles>,
    ) -> DeltaResult<Page<ReadJsonFiles>> {
        self.exchange(cursor, PendingPageRequest::Continue).await
    }

    /// Initialize a Parquet read and return its first page.
    #[allow(dead_code)]
    pub(crate) async fn start_read_parquet(
        &self,
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Page<ReadParquetFiles>> {
        let outbound = ReadParquetFiles::new(files, physical_schema, predicate);
        self.exchange(outbound, PendingPageRequest::Start).await
    }

    /// Initialize a Parquet read without fetching its first page.
    #[allow(dead_code)]
    pub(crate) async fn prepare_read_parquet(
        &self,
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Cursor<ReadParquetFiles>> {
        let outbound = ReadParquetFiles::new(files, physical_schema, predicate);
        self.exchange(outbound, PendingPageRequest::Prepare).await
    }

    /// Continue a Parquet read from `cursor`.
    #[allow(dead_code)]
    pub(crate) async fn continue_read_parquet(
        &self,
        cursor: Cursor<ReadParquetFiles>,
    ) -> DeltaResult<Page<ReadParquetFiles>> {
        self.exchange(cursor, PendingPageRequest::Continue).await
    }

    /// Execute `plan` and return its first page.
    #[cfg(feature = "declarative-plans")]
    pub(crate) async fn start_plan(&self, plan: PlanOperation) -> DeltaResult<Page<PlanOperation>> {
        self.exchange(plan, PendingPageRequest::Start).await
    }

    /// Prepare `plan` without fetching its first page.
    #[cfg(feature = "declarative-plans")]
    #[allow(dead_code)]
    pub(crate) async fn prepare_plan(
        &self,
        plan: PlanOperation,
    ) -> DeltaResult<Cursor<PlanOperation>> {
        self.exchange(plan, PendingPageRequest::Prepare).await
    }

    /// Continue plan execution from `cursor`.
    #[cfg(feature = "declarative-plans")]
    pub(crate) async fn continue_plan(
        &self,
        cursor: Cursor<PlanOperation>,
    ) -> DeltaResult<Page<PlanOperation>> {
        self.exchange(cursor, PendingPageRequest::Continue).await
    }

    /// Initialize a forward listing and return its first page.
    pub(crate) async fn start_forward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Page<ForwardListing>> {
        let outbound = ForwardListing(Box::new(bounds));
        self.exchange(outbound, PendingPageRequest::Start).await
    }

    /// Initialize a forward listing without fetching its first page.
    #[allow(dead_code)]
    pub(crate) async fn prepare_forward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Cursor<ForwardListing>> {
        let outbound = ForwardListing(Box::new(bounds));
        self.exchange(outbound, PendingPageRequest::Prepare).await
    }

    /// Continue a forward listing from `cursor`.
    pub(crate) async fn continue_forward_listing(
        &self,
        cursor: Cursor<ForwardListing>,
    ) -> DeltaResult<Page<ForwardListing>> {
        self.exchange(cursor, PendingPageRequest::Continue).await
    }

    /// Initialize a backward listing and return its first page.
    pub(crate) async fn start_backward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Page<BackwardListing>> {
        let outbound = BackwardListing(Box::new(bounds));
        self.exchange(outbound, PendingPageRequest::Start).await
    }

    /// Initialize a backward listing without fetching its first page.
    #[allow(dead_code)]
    pub(crate) async fn prepare_backward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Cursor<BackwardListing>> {
        let outbound = BackwardListing(Box::new(bounds));
        self.exchange(outbound, PendingPageRequest::Prepare).await
    }

    /// Continue a backward listing from `cursor`.
    pub(crate) async fn continue_backward_listing(
        &self,
        cursor: Cursor<BackwardListing>,
    ) -> DeltaResult<Page<BackwardListing>> {
        self.exchange(cursor, PendingPageRequest::Continue).await
    }

    /// Start writing one newline-delimited JSON file.
    pub(crate) async fn start_json_file(
        &self,
        operation: WriteJsonFile,
    ) -> DeltaResult<Cursor<WriteJsonFile>> {
        self.exchange(operation, PendingSinkRequest::Start).await
    }

    /// Append one row batch to an open newline-delimited JSON file.
    pub(crate) async fn write_json_file(
        &self,
        sink: Cursor<WriteJsonFile>,
        data: FilteredEngineData,
    ) -> DeltaResult<Cursor<WriteJsonFile>> {
        self.exchange(SinkWrite::new(sink, data), PendingSinkRequest::Write)
            .await
    }

    /// Finish a newline-delimited JSON file and return its size.
    pub(crate) async fn finish_json_file(
        &self,
        sink: Cursor<WriteJsonFile>,
    ) -> DeltaResult<FileMeta> {
        self.exchange(sink, PendingSinkRequest::Finish).await
    }

    /// Write `data` to `url`, replacing an existing object only when `overwrite` is true.
    pub(crate) async fn write_bytes(
        &self,
        url: Url,
        data: Bytes,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let outbound = WriteBytes::new(url, data, overwrite);
        self.exchange(outbound, PendingRequest::WriteBytes).await
    }

    /// Atomically copy `source` to a new `destination`.
    #[internal_api]
    #[allow(unused)]
    pub(crate) async fn copy_atomic(&self, source: Url, destination: Url) -> DeltaResult<()> {
        self.exchange(
            CopyAtomic::new(source, destination),
            PendingRequest::CopyAtomic,
        )
        .await
    }

    /// Delegate a prepared transaction to the connector's committer.
    pub(crate) async fn commit(
        &self,
        metadata: CommitMetadata,
        actions: Generator<FilteredEngineData>,
    ) -> DeltaResult<CommitResponse> {
        self.exchange(Commit::new(metadata, actions), PendingRequest::Commit)
            .await
    }

    /// Publish catalog commits through the connector's catalog committer.
    pub(crate) async fn publish(&self, metadata: PublishMetadata) -> DeltaResult<()> {
        self.exchange(metadata, PendingRequest::Publish).await
    }

    /// Materialize one row containing a single scalar value.
    pub(crate) async fn create_row(
        &self,
        schema: SchemaRef,
        value: impl Into<crate::expressions::Scalar>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.create_many(schema, vec![vec![value.into()]]).await
    }

    /// Materialize owned scalar rows as one engine-data batch.
    pub(crate) async fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<crate::expressions::Scalar>>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let outbound = CreateEngineData::new(schema, rows);
        self.exchange(outbound, PendingRequest::CreateEngineData)
            .await
    }

    /// Prepare `expression` for repeated evaluation.
    pub(crate) async fn create_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: crate::expressions::ExpressionRef,
        output_type: crate::schema::DataType,
    ) -> DeltaResult<EvaluatorHandle> {
        let outbound = CreateExpressionEvaluator::new(input_schema, expression, output_type);
        self.exchange(outbound, PendingRequest::CreateExpressionEvaluator)
            .await
    }

    /// Evaluate a prepared expression against `input`.
    pub(crate) async fn evaluate_expression(
        &self,
        evaluator: &EvaluatorHandle,
        input: std::sync::Arc<dyn EngineData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let outbound = EvaluateExpression::new(evaluator.clone(), input);
        self.exchange(outbound, PendingRequest::EvaluateExpression)
            .await
    }

    /// Evaluate a prepared expression and preserve `input`'s selection vector on the result.
    pub(crate) async fn evaluate_filtered_expression(
        &self,
        evaluator: &EvaluatorHandle,
        input: std::sync::Arc<FilteredEngineData>,
    ) -> DeltaResult<FilteredEngineData> {
        let outbound = EvaluateFilteredExpression::new(evaluator.clone(), input);
        self.exchange(outbound, PendingRequest::EvaluateFilteredExpression)
            .await
    }
}

/// Generic kernel pagination driver for operations that return [`EngineData`].
pub(crate) trait EngineDataOperation:
    PagedOperation<Page = Vec<Box<dyn EngineData>>> + Sized
{
    /// Initialize this operation and return its first page.
    fn start(self, channel: &Channel) -> impl DeltaFuture<Page<Self>>;

    /// Initialize this operation without fetching its first page.
    #[allow(dead_code)]
    fn prepare(self, channel: &Channel) -> impl DeltaFuture<Cursor<Self>>;

    /// Continue this operation from `cursor`.
    fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> impl DeltaFuture<Page<Self>>;
}

impl<Op> EngineDataOperation for Op
where
    Op: PagedOperation<Page = Vec<Box<dyn EngineData>>>,
    PendingPageRequest<Op>: Into<PendingRequest>,
{
    fn start(self, channel: &Channel) -> impl DeltaFuture<Page<Self>> {
        channel.exchange(self, PendingPageRequest::Start)
    }

    fn prepare(self, channel: &Channel) -> impl DeltaFuture<Cursor<Self>> {
        channel.exchange(self, PendingPageRequest::Prepare)
    }

    fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> impl DeltaFuture<Page<Self>> {
        channel.exchange(cursor, PendingPageRequest::Continue)
    }
}
