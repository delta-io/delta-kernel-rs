//! Native async driver for Delta Kernel coroutines.

use std::any::Any;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::num::{NonZero, NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::coroutine::evaluation::EvaluatorHandle;
use delta_kernel::coroutine::listing::{
    BackwardListing, BackwardListingResult, ForwardListing, ListingBounds,
};
use delta_kernel::coroutine::read::{ReadJsonFiles, ReadParquetFiles};
use delta_kernel::coroutine::write::{FileWriteMode, WriteJsonFile};
use delta_kernel::coroutine::{
    Cursor, Generator, Page, PageRequest, PagedOperation, Request, SinkRequest, Workflow,
    YieldResume,
};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::to_json_bytes;
use delta_kernel::metrics::{
    emit_json_read_completed, emit_parquet_read_completed, emit_storage_list_completed,
    emit_storage_read_completed,
};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::{
    DeltaResult, EngineData, Error, EvaluationHandler, ExpressionEvaluator, FileMeta, FileSlice,
    FilteredEngineData, ParquetFooter, Version,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::filesystem::{copy_atomic_impl, head_impl, list_from_impl, put_impl, read_files_impl};
use crate::json::read_json_files_impl;
use crate::parquet::{read_parquet_files_impl, read_parquet_footer_impl};
use crate::{DEFAULT_READ_BATCH_SIZE, DEFAULT_READ_BUFFER_SIZE};

/// Drives kernel coroutines directly through the default engine's native async I/O.
///
/// This connector does not use [`delta_kernel::Engine`] or a
/// [`crate::executor::TaskExecutor`]. Callers may handle selected [`Request`] variants themselves
/// and delegate any remaining requests to [`Self::resume`].
#[derive(Clone)]
pub struct AsyncEngineConnector {
    object_store: Arc<DynObjectStore>,
    cancellation_token: Option<CancellationToken>,
    storage_readahead: usize,
    buffer_size: NonZero<usize>,
    batch_size: NonZero<usize>,
    forward_listing_page_size: NonZero<usize>,
    backward_listing_window_size: NonZero<Version>,
}

enum GeneratorCursorState<Item: Send + 'static> {
    Prepared(Generator<Item>),
    Active(YieldResume<Generator<Item>>),
}

impl Debug for AsyncEngineConnector {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("AsyncEngineConnector")
            .field("cancellation_token", &self.cancellation_token)
            .field("storage_readahead", &self.storage_readahead)
            .field("buffer_size", &self.buffer_size)
            .field("batch_size", &self.batch_size)
            .field("forward_listing_page_size", &self.forward_listing_page_size)
            .field(
                "backward_listing_window_size",
                &self.backward_listing_window_size,
            )
            .finish_non_exhaustive()
    }
}

impl AsyncEngineConnector {
    /// Create a connector backed by `object_store` with the default read tuning.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            cancellation_token: None,
            storage_readahead: STORAGE_READAHEAD,
            buffer_size: DEFAULT_READ_BUFFER_SIZE,
            batch_size: DEFAULT_READ_BATCH_SIZE,
            forward_listing_page_size: DEFAULT_FORWARD_PAGE,
            backward_listing_window_size: DEFAULT_BACKWARD_WINDOW,
        }
    }

    /// Race each async I/O operation and stream poll against `cancellation_token`.
    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    /// Set the maximum number of storage reads performed concurrently.
    pub fn with_storage_readahead(mut self, storage_readahead: NonZero<usize>) -> Self {
        self.storage_readahead = storage_readahead.get();
        self
    }

    /// Set the maximum number of JSON or Parquet files read concurrently.
    pub fn with_buffer_size(mut self, buffer_size: NonZero<usize>) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the maximum number of rows in each JSON or Parquet batch.
    pub fn with_batch_size(mut self, batch_size: NonZero<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of listing entries in one forward-listing page.
    ///
    /// Defaults to [`ForwardListing::DEFAULT_PAGE_SIZE`].
    pub fn with_forward_listing_page_size(mut self, page_size: NonZero<usize>) -> Self {
        self.forward_listing_page_size = page_size;
        self
    }

    /// Set how many Delta versions one backward-listing request covers.
    ///
    /// Defaults to [`BackwardListing::DEFAULT_WINDOW_SIZE`].
    pub fn with_backward_listing_window_size(mut self, window_size: NonZero<Version>) -> Self {
        self.backward_listing_window_size = window_size;
        self
    }

    /// Drive `workflow` until it completes or returns an error.
    ///
    /// Kernel and I/O errors are returned to the caller. A configured cancellation token produces
    /// [`Error::Cancelled`].
    pub async fn drive<O: Send + 'static>(
        &self,
        mut workflow: DeltaResult<Workflow<O>>,
    ) -> DeltaResult<O> {
        loop {
            workflow = match workflow? {
                Workflow::Done(output) => return Ok(output),
                Workflow::Request(request) => self.resume(request).await,
            };
        }
    }

    /// Seal a kernel generator behind a cursor for paginated consumption.
    pub fn seal_generator<Op, Item>(&self, generator: Generator<Item>) -> Cursor<Op>
    where
        Op: PagedOperation<Page = Vec<Item>>,
        Item: Send + 'static,
    {
        Cursor::new(GeneratorCursorState::Prepared(generator))
    }

    /// Advance a sealed generator until it yields one item or completes.
    ///
    /// Any kernel requests encountered while advancing the generator are served by this connector.
    ///
    /// # Errors
    ///
    /// Returns an error if the cursor was not created by [`seal_generator`](Self::seal_generator),
    /// serving a kernel request fails, or advancing the generator fails.
    pub async fn continue_generator<Op, Item>(&self, cursor: Cursor<Op>) -> DeltaResult<Page<Op>>
    where
        Op: PagedOperation<Page = Vec<Item>>,
        Item: Send + 'static,
    {
        let mut generator = match cursor.into_inner()? {
            GeneratorCursorState::Prepared(generator) => Ok(generator),
            GeneratorCursorState::Active(resume) => resume(Ok(())),
        };
        loop {
            generator = match generator? {
                Generator::Done(()) => {
                    return Ok(Page::new(Vec::new(), None));
                }
                Generator::Yield(item, resume) => {
                    let cursor = Cursor::new(GeneratorCursorState::Active(resume));
                    return Ok(Page::new(vec![item], Some(cursor)));
                }
                Generator::Request(request) => self.resume(request).await,
            };
        }
    }

    /// Serve one coroutine request and resume kernel to its next suspension boundary.
    ///
    /// Connectors can intercept selected variants of `request` and delegate the remainder here.
    /// Returns kernel's next state, or an error from I/O, cancellation, or advancing kernel.
    pub async fn resume<N: Send + 'static>(&self, request: Request<N>) -> DeltaResult<N> {
        match request {
            Request::ListForward(request) => resume_paged(self, request).await,
            Request::ListBackward(request) => resume_paged(self, request).await,
            Request::ReadSmallFile(file, resume) => resume(self.read_small_file(file).await),
            Request::ReadParquetFooter(file, resume) => {
                resume(self.read_parquet_footer(file).await)
            }
            Request::ReadJson(request) => resume_paged(self, request).await,
            Request::ReadParquet(request) => resume_paged(self, request).await,
            Request::ExecutePlan(request) => {
                let err = Error::unsupported("Default engine does not execute plans");
                match request {
                    PageRequest::Start(_, resume) => resume(Err(err)),
                    PageRequest::Prepare(_, resume) => resume(Err(err)),
                    PageRequest::Continue(_, resume) => resume(Err(err)),
                }
            }
            Request::WriteBytes(operation, resume) => resume(
                self.write_bytes(operation.url, operation.data, operation.overwrite)
                    .await,
            ),
            Request::CopyAtomic(operation, resume) => resume(
                self.copy_atomic(operation.source, operation.destination)
                    .await,
            ),
            Request::WriteJson(SinkRequest::Start(operation, resume)) => {
                resume(self.start_json_file(operation))
            }
            Request::WriteJson(SinkRequest::Write(cursor, data, resume)) => {
                resume(self.write_json_file(cursor, data))
            }
            Request::WriteJson(SinkRequest::Finish(cursor, resume)) => {
                resume(self.finish_json_file(cursor).await)
            }
            Request::CreateEngineData(operation, resume) => {
                resume(ArrowEvaluationHandler.create_many(operation.schema, operation.rows))
            }
            Request::CreateExpressionEvaluator(operation, resume) => resume(
                ArrowEvaluationHandler
                    .new_expression_evaluator(
                        operation.input_schema,
                        operation.expression,
                        operation.output_type,
                    )
                    .map(|evaluator| EvaluatorHandle::Arc(Arc::new(evaluator))),
            ),
            Request::EvaluateExpression(operation, resume) => resume(
                expression_evaluator(&operation.evaluator)
                    .and_then(|evaluator| evaluator.evaluate(operation.input.as_ref())),
            ),
            Request::EvaluateFilteredExpression(operation, resume) => resume(
                expression_evaluator(&operation.evaluator).and_then(|evaluator| {
                    let selection = operation.input.selection_vector().to_vec();
                    let output = evaluator.evaluate(operation.input.data())?;
                    FilteredEngineData::try_new(output, selection)
                }),
            ),
            Request::Commit(operation, resume) => {
                let workflow = FileSystemCommitter::start_commit(*operation);
                resume(Box::pin(self.drive(workflow)).await)
            }
            Request::Publish(..) => Err(Error::unsupported(
                "the async default engine does not publish catalog commits",
            )),
        }
    }

    async fn read_small_file(&self, file: FileSlice) -> DeltaResult<Bytes> {
        let start = Instant::now();
        let stream = self
            .cancel(read_files_impl(
                self.object_store.clone(),
                vec![file],
                self.storage_readahead,
            ))
            .await?;
        let observe = |bytes: &Bytes| (1, bytes.len() as u64);
        let mut stream = metered_stream(stream, start, observe, emit_storage_read_completed);
        let Some(data) = self.next(&mut stream).await?.transpose()? else {
            return Err(Error::internal_error("single-file read returned no result"));
        };
        if self.next(&mut stream).await?.transpose()?.is_some() {
            return Err(Error::internal_error(
                "single-file read returned more than one result",
            ));
        }
        Ok(data)
    }

    async fn read_parquet_footer(&self, file: FileMeta) -> DeltaResult<ParquetFooter> {
        self.cancel(read_parquet_footer_impl(self.object_store.clone(), file))
            .await
    }

    async fn next_data<Op>(
        &self,
        mut stream: ResultStream<Box<dyn EngineData>>,
    ) -> DeltaResult<Page<Op>>
    where
        Op: PagedOperation<Page = Vec<Box<dyn EngineData>>>,
    {
        let (data, next) = match self.next(&mut stream).await?.transpose()? {
            Some(data) => (vec![data], Some(Cursor::new(stream))),
            None => (Vec::new(), None),
        };
        Ok(Page { data, next })
    }

    async fn write_bytes(&self, url: Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        let path = Path::from_url_path(url.path())?;
        self.cancel(put_impl(self.object_store.clone(), path, data, overwrite))
            .await
    }

    /// Atomically copy `source` to a new `destination`.
    ///
    /// # Errors
    ///
    /// Returns an error if either URL is invalid, reading the source fails, or creating the
    /// destination fails.
    pub async fn copy_atomic(&self, source: Url, destination: Url) -> DeltaResult<()> {
        let source = Path::from_url_path(source.path())?;
        let destination = Path::from_url_path(destination.path())?;
        let store = self.object_store.clone();
        self.cancel(copy_atomic_impl(store, source, destination))
            .await
    }

    fn start_json_file(&self, operation: WriteJsonFile) -> DeltaResult<Cursor<WriteJsonFile>> {
        Ok(Cursor::new(JsonFileSink {
            url: operation.url,
            overwrite: operation.mode == FileWriteMode::Overwrite,
            buffer: Vec::new(),
        }))
    }

    fn write_json_file(
        &self,
        cursor: Cursor<WriteJsonFile>,
        data: FilteredEngineData,
    ) -> DeltaResult<Cursor<WriteJsonFile>> {
        let mut sink: JsonFileSink = cursor.into_inner()?;
        sink.buffer
            .extend(to_json_bytes(std::iter::once(Ok(data)))?);
        Ok(Cursor::new(sink))
    }

    async fn finish_json_file(&self, cursor: Cursor<WriteJsonFile>) -> DeltaResult<FileMeta> {
        let JsonFileSink {
            url,
            overwrite,
            buffer,
        } = cursor.into_inner()?;
        self.write_bytes(url.clone(), Bytes::from(buffer), overwrite)
            .await?;
        self.cancel(head_impl(self.object_store.clone(), url)).await
    }

    async fn cancel<T>(&self, future: impl Future<Output = DeltaResult<T>>) -> DeltaResult<T> {
        let Some(token) = &self.cancellation_token else {
            return future.await;
        };
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(Error::Cancelled),
            result = future => result,
        }
    }

    async fn next<T>(&self, stream: &mut ResultStream<T>) -> DeltaResult<Option<DeltaResult<T>>> {
        let Some(token) = &self.cancellation_token else {
            return Ok(stream.next().await);
        };
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(Error::Cancelled),
            item = stream.next() => Ok(item),
        }
    }
}

const STORAGE_READAHEAD: usize = 10;
const DEFAULT_FORWARD_PAGE: NonZero<usize> = match NonZero::new(ForwardListing::DEFAULT_PAGE_SIZE) {
    Some(size) => size,
    None => NonZeroUsize::MIN,
};
const DEFAULT_BACKWARD_WINDOW: NonZero<Version> =
    match NonZero::new(BackwardListing::DEFAULT_WINDOW_SIZE) {
        Some(size) => size,
        None => NonZeroU64::MIN,
    };

type ResultStream<T> = BoxStream<'static, DeltaResult<T>>;

/// Carries native async pagination state in boxed cursors.
///
/// Duplicated from kernel's private `EnginePagination` (`coroutine/engine.rs`): that trait is
/// synchronous for the Engine-trait driver, and this one awaits. Combining them would force
/// `block_on` back onto the native async path or make the sync Engine driver async.
trait AsyncPagination<Op: PagedOperation> {
    type State: Any + Send;

    async fn initialize(&self, operation: Op) -> DeltaResult<Self::State>;

    async fn next_page(&self, state: Self::State) -> DeltaResult<Page<Op>>;

    async fn start(&self, operation: Op) -> DeltaResult<Page<Op>> {
        self.continue_from(self.prepare(operation).await?).await
    }

    async fn prepare(&self, operation: Op) -> DeltaResult<Cursor<Op>> {
        Ok(Cursor::new(self.initialize(operation).await?))
    }

    async fn continue_from(&self, cursor: Cursor<Op>) -> DeltaResult<Page<Op>> {
        self.next_page(cursor.into_inner()?).await
    }
}

async fn resume_paged<C, Op, N>(connector: &C, request: PageRequest<N, Op>) -> DeltaResult<N>
where
    C: AsyncPagination<Op>,
    Op: PagedOperation,
    N: Send + 'static,
{
    match request {
        PageRequest::Start(operation, resume) => {
            resume(AsyncPagination::start(connector, operation).await)
        }
        PageRequest::Prepare(operation, resume) => {
            resume(AsyncPagination::prepare(connector, operation).await)
        }
        PageRequest::Continue(cursor, resume) => {
            resume(AsyncPagination::continue_from(connector, cursor).await)
        }
    }
}

impl AsyncPagination<ForwardListing> for AsyncEngineConnector {
    type State = ResultStream<FileMeta>;

    async fn initialize(&self, ForwardListing(bounds): ForwardListing) -> DeltaResult<Self::State> {
        let start = Instant::now();
        let stream = self
            .cancel(list_from_impl(
                self.object_store.clone(),
                bounds.low.clone(),
            ))
            .await?;
        let stream = metered_stream(stream, start, observe_file, emit_storage_list);
        let stream = stream.take_while(move |entry| std::future::ready(bounds.contains(entry)));
        Ok(Box::pin(stream))
    }

    async fn next_page(&self, mut stream: Self::State) -> DeltaResult<Page<ForwardListing>> {
        let page_size = self.forward_listing_page_size.get();
        let mut data = Vec::with_capacity(page_size);
        while data.len() < page_size {
            let Some(entry) = self.next(&mut stream).await? else {
                break;
            };
            data.push(entry);
        }
        let next = (data.len() == page_size).then(|| Cursor::new(stream));
        Ok(Page { data, next })
    }
}

impl AsyncPagination<BackwardListing> for AsyncEngineConnector {
    type State = BackwardListingState;

    async fn initialize(&self, operation: BackwardListing) -> DeltaResult<Self::State> {
        initialize_backward_listing(operation)
    }

    async fn next_page(&self, state: Self::State) -> DeltaResult<Page<BackwardListing>> {
        let BackwardListingState { bounds, high } = state;
        let window = bounds.backward_window(high, self.backward_listing_window_size.get())?;
        let next_high = window.next_high;
        let start_time = Instant::now();
        let store = self.object_store.clone();
        let low = window.low.clone();
        let stream = self.cancel(list_from_impl(store, low)).await?;
        let stream = metered_stream(stream, start_time, observe_file, emit_storage_list);
        let stream = stream.take_while(move |entry| std::future::ready(window.contains(entry)));
        let mut stream: ResultStream<FileMeta> = Box::pin(stream);
        let mut entries = Vec::new();
        while let Some(entry) = self.next(&mut stream).await? {
            entries.push(entry);
        }
        let next = next_high.map(|high| Cursor::new(BackwardListingState { bounds, high }));
        let data = BackwardListingResult {
            entries,
            known_version_boundary: true,
        };
        Ok(Page { data, next })
    }
}

impl AsyncPagination<ReadJsonFiles> for AsyncEngineConnector {
    type State = ResultStream<Box<dyn EngineData>>;

    async fn initialize(&self, read: ReadJsonFiles) -> DeltaResult<Self::State> {
        let num_files = read.files.len() as u64;
        let bytes_read = read.files.iter().map(|file| file.size).sum();
        let stream = self
            .cancel(read_json_files_impl(
                self.object_store.clone(),
                read.files,
                read.physical_schema,
                read.predicate,
                self.batch_size.get(),
                self.buffer_size.get(),
            ))
            .await?;
        Ok(precounted_stream(
            stream,
            num_files,
            bytes_read,
            |_, num_files, bytes_read| emit_json_read_completed(num_files, bytes_read),
        ))
    }

    async fn next_page(&self, stream: Self::State) -> DeltaResult<Page<ReadJsonFiles>> {
        self.next_data(stream).await
    }
}

impl AsyncPagination<ReadParquetFiles> for AsyncEngineConnector {
    type State = ResultStream<Box<dyn EngineData>>;

    async fn initialize(&self, read: ReadParquetFiles) -> DeltaResult<Self::State> {
        let num_files = read.files.len() as u64;
        let bytes_read = read.files.iter().map(|file| file.size).sum();
        let stream = self
            .cancel(read_parquet_files_impl(
                self.object_store.clone(),
                read.files,
                read.physical_schema,
                read.predicate,
                self.buffer_size.get(),
                self.batch_size.get(),
            ))
            .await?;
        Ok(precounted_stream(
            stream,
            num_files,
            bytes_read,
            |_, num_files, bytes_read| emit_parquet_read_completed(num_files, bytes_read),
        ))
    }

    async fn next_page(&self, stream: Self::State) -> DeltaResult<Page<ReadParquetFiles>> {
        self.next_data(stream).await
    }
}

struct JsonFileSink {
    url: Url,
    overwrite: bool,
    buffer: Vec<u8>,
}

fn expression_evaluator(handle: &EvaluatorHandle) -> DeltaResult<&Arc<dyn ExpressionEvaluator>> {
    match handle {
        EvaluatorHandle::Arc(evaluator) => evaluator.downcast_ref().ok_or_else(|| {
            Error::internal_error("invalid async engine expression evaluator handle")
        }),
        EvaluatorHandle::Id(_) => Err(Error::internal_error(
            "async engine expression evaluator handle contained an id",
        )),
    }
}

struct BackwardListingState {
    bounds: Box<ListingBounds>,
    high: Version,
}

fn initialize_backward_listing(
    BackwardListing(bounds): BackwardListing,
) -> DeltaResult<BackwardListingState> {
    let high = bounds.high_version()?;
    Ok(BackwardListingState { bounds, high })
}

type Observe<T> = fn(&T) -> (u64, u64);
type EmitMetric = fn(Duration, u64, u64);

struct MeteredStream<T> {
    inner: ResultStream<T>,
    start: Instant,
    num_files: u64,
    bytes_read: u64,
    observe: Observe<T>,
    emit: EmitMetric,
}

impl<T> Stream for MeteredStream<T> {
    type Item = DeltaResult<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(ref value))) = item {
            let (files, bytes) = (self.observe)(value);
            self.num_files += files;
            self.bytes_read += bytes;
        }
        item
    }
}

impl<T> Drop for MeteredStream<T> {
    fn drop(&mut self) {
        (self.emit)(self.start.elapsed(), self.num_files, self.bytes_read);
    }
}

fn metered_stream<T: Send + 'static>(
    inner: ResultStream<T>,
    start: Instant,
    observe: Observe<T>,
    emit: EmitMetric,
) -> ResultStream<T> {
    Box::pin(MeteredStream {
        inner,
        start,
        num_files: 0,
        bytes_read: 0,
        observe,
        emit,
    })
}

fn precounted_stream<T: Send + 'static>(
    inner: ResultStream<T>,
    num_files: u64,
    bytes_read: u64,
    emit: EmitMetric,
) -> ResultStream<T> {
    Box::pin(MeteredStream {
        inner,
        start: Instant::now(),
        num_files,
        bytes_read,
        observe: |_| (0, 0),
        emit,
    })
}

fn observe_file(_: &FileMeta) -> (u64, u64) {
    (1, 0)
}

fn emit_storage_list(elapsed: Duration, num_files: u64, _bytes_read: u64) {
    emit_storage_list_completed(elapsed, num_files);
}

#[cfg(test)]
mod tests {
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::ObjectStoreExt as _;
    use delta_kernel::snapshot::Snapshot;

    use super::*;

    async fn table_store() -> Arc<DynObjectStore> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let commit = concat!(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            "\n",
            r#"{"metaData":{"id":"test-table","format":{"provider":"parquet","options":{}},"#,
            r#""schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\","#,
            r#"\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","#,
            r#""partitionColumns":[],"configuration":{},"createdTime":0}}"#,
        );
        store
            .put(
                &Path::from("table/_delta_log/00000000000000000000.json"),
                commit.into(),
            )
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn drives_snapshot_without_engine_trait_or_task_executor() {
        let connector = AsyncEngineConnector::new(table_store().await);

        let snapshot = connector
            .drive(Snapshot::builder_for("memory:///table/").start())
            .await
            .unwrap();

        assert_eq!(snapshot.version(), 0);
    }

    #[tokio::test]
    async fn native_commit_writes_json_commit_file() {
        let connector = AsyncEngineConnector::new(table_store().await);
        let snapshot = connector
            .drive(Snapshot::builder_for("memory:///table/").start())
            .await
            .unwrap();
        let txn = connector.drive(snapshot.start_transaction()).await.unwrap();
        let result = connector.drive(txn.start_commit()).await.unwrap();
        assert!(result.is_committed());

        let snapshot = connector
            .drive(Snapshot::builder_for("memory:///table/").start())
            .await
            .unwrap();
        assert_eq!(snapshot.version(), 1);
    }

    #[tokio::test]
    async fn cancellation_interrupts_snapshot_io() {
        let token = CancellationToken::new();
        token.cancel();
        let connector =
            AsyncEngineConnector::new(table_store().await).with_cancellation_token(token);

        let result = connector
            .drive(Snapshot::builder_for("memory:///table/").start())
            .await;

        assert!(matches!(result, Err(Error::Cancelled)));
    }

    #[tokio::test]
    async fn cancellation_interrupts_in_flight_future() {
        let token = CancellationToken::new();
        let cancel = token.clone();
        let connector =
            AsyncEngineConnector::new(Arc::new(InMemory::new())).with_cancellation_token(token);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel.cancel();
        });

        let result = connector
            .cancel(std::future::pending::<DeltaResult<()>>())
            .await;

        assert!(matches!(result, Err(Error::Cancelled)));
    }
}
