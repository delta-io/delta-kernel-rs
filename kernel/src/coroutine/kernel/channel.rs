//! Kernel-side typed connector operations.
//!
//! These methods construct the corresponding [`Request`] variant and suspend until its reply is
//! completed by the task driver.

use std::ops::Range;

use bytes::Bytes;
use delta_kernel_derive::internal_api;
use url::Url;

use super::{Channel, Request};
use crate::committer::{Commit, CommitResponse, PublishMetadata};
use crate::coroutine::evaluation::{
    CreateEngineData, CreateExpressionEvaluator, EvaluateExpression, EvaluateFilteredExpression,
    EvaluatorHandle,
};
use crate::coroutine::write::{CopyAtomic, WriteBytes, WriteJsonFile};
use crate::coroutine::{ChannelExchange, Cursor, DeltaFuture, Page, PageRequest, PagedOperation};
use crate::engine_data::EngineData;
use crate::schema::SchemaRef;
use crate::{FileIndex, FileMeta, FilteredEngineData, ParquetFooter};

/// Typed kernel operations available on its request channel.
#[internal_api]
pub(crate) trait ChannelExt: ChannelExchange<Request> {
    /// Read one small file, or one half-open byte range, completely into memory.
    fn read_small_file(
        &self,
        url: Url,
        range: Option<Range<FileIndex>>,
    ) -> impl DeltaFuture<Bytes> {
        self.exchange((url, range), Request::ReadSmallFile)
    }

    /// Read a Parquet file footer.
    fn read_parquet_footer(&self, file: FileMeta) -> impl DeltaFuture<ParquetFooter> {
        self.exchange(file, Request::ReadParquetFooter)
    }

    /// Initialize `operation` and return its first page.
    fn start_paged<Op>(&self, operation: Op) -> impl DeltaFuture<Page<Op>>
    where
        Op: PagedOperation,
        PageRequest<Op>: Into<Request>,
    {
        self.exchange(operation, PageRequest::Start)
    }

    /// Initialize `operation` without fetching its first page.
    #[cfg(test)] // no prod callers yet; not allow(unused) because that could go stale
    fn prepare_paged<Op>(&self, operation: Op) -> impl DeltaFuture<Cursor<Op>>
    where
        Op: PagedOperation,
        PageRequest<Op>: Into<Request>,
    {
        self.exchange(operation, PageRequest::Prepare)
    }

    /// Continue the paged operation represented by `cursor`.
    fn continue_paged<Op>(&self, cursor: Cursor<Op>) -> impl DeltaFuture<Page<Op>>
    where
        Op: PagedOperation,
        PageRequest<Op>: Into<Request>,
    {
        self.exchange(cursor, PageRequest::Continue)
    }

    /// Write generated row batches to one newline-delimited JSON file.
    fn write_json_file(&self, operation: WriteJsonFile) -> impl DeltaFuture<FileMeta> {
        self.exchange(Box::new(operation), Request::WriteJson)
    }

    /// Write `data` to `url`, replacing an existing object only when `overwrite` is true.
    fn write_bytes(&self, url: Url, data: Bytes, overwrite: bool) -> impl DeltaFuture<()> {
        let outbound = WriteBytes::new(url, data, overwrite);
        self.exchange(outbound, Request::WriteBytes)
    }

    /// Atomically copy `source` to a new `destination`.
    #[allow(unused)]
    fn copy_atomic(&self, source: Url, destination: Url) -> impl DeltaFuture<()> {
        self.exchange(CopyAtomic::new(source, destination), Request::CopyAtomic)
    }

    /// Delegate a prepared transaction to the connector's committer.
    fn commit(&self, commit: Commit) -> impl DeltaFuture<CommitResponse> {
        self.exchange(Box::new(commit), Request::Commit)
    }

    /// Publish catalog commits through the connector's catalog committer.
    fn publish(&self, metadata: PublishMetadata) -> impl DeltaFuture<()> {
        self.exchange(metadata, Request::Publish)
    }

    /// Materialize one row containing a single scalar value.
    fn create_row(
        &self,
        schema: SchemaRef,
        value: impl Into<crate::expressions::Scalar>,
    ) -> impl DeltaFuture<Box<dyn EngineData>> {
        self.create_many(schema, vec![vec![value.into()]])
    }

    /// Materialize owned scalar rows as one engine-data batch.
    fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<crate::expressions::Scalar>>,
    ) -> impl DeltaFuture<Box<dyn EngineData>> {
        let outbound = CreateEngineData::new(schema, rows);
        self.exchange(outbound, Request::CreateEngineData)
    }

    /// Prepare `expression` for repeated evaluation.
    fn create_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: crate::expressions::ExpressionRef,
        output_type: crate::schema::DataType,
    ) -> impl DeltaFuture<EvaluatorHandle> {
        let outbound = CreateExpressionEvaluator::new(input_schema, expression, output_type);
        self.exchange(outbound, Request::CreateExpressionEvaluator)
    }

    /// Evaluate a prepared expression against `input`.
    fn evaluate_expression(
        &self,
        evaluator: &EvaluatorHandle,
        input: std::sync::Arc<dyn EngineData>,
    ) -> impl DeltaFuture<Box<dyn EngineData>> {
        let outbound = EvaluateExpression::new(evaluator.clone(), input);
        self.exchange(outbound, Request::EvaluateExpression)
    }

    /// Evaluate a prepared expression and preserve `input`'s selection vector on the result.
    fn evaluate_filtered_expression(
        &self,
        evaluator: &EvaluatorHandle,
        input: std::sync::Arc<FilteredEngineData>,
    ) -> impl DeltaFuture<FilteredEngineData> {
        let outbound = EvaluateFilteredExpression::new(evaluator.clone(), input);
        self.exchange(outbound, Request::EvaluateFilteredExpression)
    }
}

impl ChannelExt for Channel {}
