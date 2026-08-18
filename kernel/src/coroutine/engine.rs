//! Reference connector that serves coroutine requests through [`Engine`] handlers.
//!
//! Kernel provides `Engine`-based entry points for all public workflows, for connectors that are
//! unawae of (or opt out of) coroutines. Each entry point launches the corresponding workflow, then
//! drives it to completion using an [`EngineConnector`] that uses the caller-provided `Engine`
//! instance to serve requestse.
//!
//! Engine iterators travel in boxed cursors. [`drive_storage`] serves only listing and small-file
//! requests.

use std::any::Any;
use std::sync::Arc;

use bytes::Bytes;
use derive_more::Constructor;

use super::core::DeltaFuture;
use super::evaluation::EvaluatorHandle;
use super::listing::{BackwardListing, BackwardListingResult, ForwardListing, ListingBounds};
use super::read::{ReadJsonFiles, ReadParquetFiles};
use super::write::WriteJsonFile;
#[cfg(feature = "declarative-plans")]
use super::PlanOperation;
use super::{
    Channel, Cursor, Generator, Page, PageRequest, PagedOperation, Request, Resume, SinkRequest,
    Workflow, YieldResume,
};
use crate::cancellation::{check_cancelled, CancellationTokenRef};
use crate::committer::{Commit, Committer, FileSystemCommitter};
use crate::engine_data::{EngineData, FilteredEngineData};
#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{
    DeltaResult, DeltaResultIteratorStatic, Engine, Error, EvaluationHandler, ExpressionEvaluator,
    FileDataReadResultIterator, FileMeta, FileSlice, JsonHandler, ParquetHandler, StorageHandler,
    Version,
};

const FORWARD_LISTING_PAGE_SIZE: usize = if cfg!(test) {
    2
} else {
    ForwardListing::DEFAULT_PAGE_SIZE
};

type ListingIterator = DeltaResultIteratorStatic<FileMeta>;
type EngineDataIterator = FileDataReadResultIterator;

struct BackwardListingState {
    bounds: Box<ListingBounds>,
    high: Version,
}

/// Carries synchronous Engine pagination state in boxed cursors. Provided trait methods map the
/// three-state paged operation (create, start, continue) to `initialize` and `next_page`.
trait EnginePagination<Op: PagedOperation> {
    type State: Any + Send;

    fn initialize(&self, operation: Op) -> DeltaResult<Self::State>;

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<Op>>;

    fn start(&self, operation: Op) -> DeltaResult<Page<Op>> {
        self.continue_from(self.prepare(operation)?)
    }

    fn prepare(&self, operation: Op) -> DeltaResult<Cursor<Op>> {
        Ok(Cursor::new(self.initialize(operation)?))
    }

    fn continue_from(&self, cursor: Cursor<Op>) -> DeltaResult<Page<Op>> {
        self.next_page(cursor.into_inner()?)
    }
}

fn resume_paged<C, Op, N>(connector: &C, request: PageRequest<N, Op>) -> DeltaResult<N>
where
    C: EnginePagination<Op>,
    Op: PagedOperation,
    N: Send + 'static,
{
    match request {
        PageRequest::Start(operation, resume) => resume(C::start(connector, operation)),
        PageRequest::Prepare(operation, resume) => resume(C::prepare(connector, operation)),
        PageRequest::Continue(cursor, resume) => resume(C::continue_from(connector, cursor)),
    }
}

// Ideally, this would just take `Arc<dyn Engine>`, so we could create handlers lazily; but most
// kernel entry points only have access to `&dyn Engine` so we must eagerly instantiate them.
pub(crate) struct EngineConnector {
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
    evaluation: Arc<dyn EvaluationHandler>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
    cancellation_token: Option<CancellationTokenRef>,
}

// Some kernel entry points (and a large number of unit tests) only have access to a manually
// created `StorageHandler` instance instead of a full `EngineData`; they use this connector.
#[derive(Constructor)]
struct StorageConnector<'a> {
    storage: &'a dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
}

// Holds the kernel `Generator` between calls to `EngineGeneratorIterator::next`
enum EngineGeneratorState<Item: Send + 'static> {
    Prepared(Generator<Item>),
    Active(YieldResume<Generator<Item>>),
    Exhausted,
}

/// Engine-side iterator over a generator, which surfaces yielded items while using an
/// `EngineConnector` to drive I/O requests.
pub(crate) struct EngineGeneratorIterator<Item: Send + 'static> {
    connector: EngineConnector,
    state: EngineGeneratorState<Item>,
}

enum EngineJsonSinkState<O: Send + 'static> {
    Start(Resume<Workflow<O>, Cursor<WriteJsonFile>>),
    Write(
        Cursor<WriteJsonFile>,
        Resume<Workflow<O>, Cursor<WriteJsonFile>>,
    ),
    Finish(Resume<Workflow<O>, FileMeta>),
}

struct EngineJsonSinkIterator<'a, O: Send + 'static> {
    connector: &'a EngineConnector,
    state: Option<EngineJsonSinkState<O>>,
}

/// An almost test-only helper; the two prod uses are `LogSegment::for_timestamp_conversion` and
/// `LogSegment::for_table_changes_with_storage`. The former could be easily converted to
/// Engine-based coroutines, but the latter has many testing call sites that only have a
/// StorageHandler.
pub(crate) fn drive_storage<O: Send + 'static, Fut>(
    storage: &dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
    workflow: impl FnOnce(Channel) -> Fut,
) -> DeltaResult<O>
where
    Fut: DeltaFuture<O> + 'static,
{
    let connector = StorageConnector::new(storage, cancellation_token);
    let mut workflow = Workflow::start(workflow);
    loop {
        workflow = match workflow? {
            Workflow::Done(output) => return Ok(output),
            Workflow::Request(request) => connector.resume(request),
        };
    }
}

impl StorageConnector<'_> {
    fn resume<N: Send + 'static>(&self, request: Request<N>) -> DeltaResult<N> {
        match request {
            Request::ListForward(request) => resume_paged(self, request),
            Request::ListBackward(request) => resume_paged(self, request),
            Request::ReadSmallFile(file, resume) => resume(self.read_small_file(file)),
            Request::CopyAtomic(operation, resume) => resume(
                check_cancelled(self.cancellation_token.as_ref()).and_then(|()| {
                    self.storage
                        .copy_atomic(&operation.source, &operation.destination)
                }),
            ),
            _ => Err(Error::internal_error(
                "storage-only coroutine requested a non-storage operation",
            )),
        }
    }

    fn read_small_file(&self, file: FileSlice) -> DeltaResult<Bytes> {
        let mut reads = self
            .storage
            .read_files_with_cancellation(vec![file], self.cancellation_token.clone())?;
        let Some(data) = reads.next().transpose()? else {
            return Err(Error::internal_error("single-file read returned no result"));
        };
        if reads.next().transpose()?.is_some() {
            return Err(Error::internal_error(
                "single-file read returned more than one result",
            ));
        }
        Ok(data)
    }
}

impl EngineConnector {
    /// Create a connector from the handlers exposed by `engine`.
    pub(crate) fn new(engine: &dyn Engine) -> Self {
        Self {
            storage: engine.storage_handler(),
            json: engine.json_handler(),
            parquet: engine.parquet_handler(),
            evaluation: engine.evaluation_handler(),
            #[cfg(feature = "declarative-plans")]
            plan_executor: engine.plan_executor(),
            cancellation_token: None,
        }
    }

    /// Configure the cancellation token propagated to engine handlers.
    pub(crate) fn with_cancellation_token(
        mut self,
        cancellation_token: impl Into<Option<CancellationTokenRef>>,
    ) -> Self {
        self.cancellation_token = cancellation_token.into();
        self
    }

    /// Start and drive `workflow` to completion.
    pub(crate) fn run<O: Send + 'static, F, Fut>(&self, workflow: F) -> DeltaResult<O>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: DeltaFuture<O> + 'static,
    {
        self.drive_workflow(Workflow::start(workflow))
    }

    /// Drive a started `workflow` to completion.
    pub(crate) fn drive_workflow<O: Send + 'static>(
        &self,
        mut workflow: DeltaResult<Workflow<O>>,
    ) -> DeltaResult<O> {
        loop {
            workflow = match workflow? {
                Workflow::Done(output) => return Ok(output),
                Workflow::Request(Request::WriteJson(SinkRequest::Start(operation, resume))) => {
                    self.drive_json_sink(operation, resume)
                }
                Workflow::Request(Request::WriteJson(_)) => {
                    return Err(Error::internal_error(
                        "JSON sink workflow did not start with a Start request",
                    ))
                }
                Workflow::Request(Request::Commit(operation, resume)) => {
                    let workflow = FileSystemCommitter::start_commit(*operation);
                    resume(self.drive_workflow(workflow))
                }
                Workflow::Request(request) => self.resume(request),
            };
        }
    }

    /// Drive a workflow through `engine`, accessing its handlers only after the first request.
    pub(crate) fn drive<O: Send + 'static>(
        engine: &dyn Engine,
        workflow: DeltaResult<Workflow<O>>,
    ) -> DeltaResult<O> {
        match workflow? {
            Workflow::Done(output) => Ok(output),
            workflow @ Workflow::Request(_) => Self::new(engine).drive_workflow(Ok(workflow)),
        }
    }

    /// Drive a workflow whose catalog requests are handled by `committer`.
    pub(crate) fn drive_with_committer<O: Send + 'static>(
        &self,
        mut workflow: DeltaResult<Workflow<O>>,
        engine: &dyn Engine,
        committer: &dyn Committer,
    ) -> DeltaResult<O> {
        loop {
            workflow = match workflow? {
                Workflow::Done(output) => return Ok(output),
                Workflow::Request(Request::Publish(metadata, resume)) => {
                    resume(committer.publish(engine, metadata))
                }
                Workflow::Request(Request::WriteJson(SinkRequest::Start(operation, resume))) => {
                    self.drive_json_sink(operation, resume)
                }
                Workflow::Request(Request::WriteJson(_)) => {
                    return Err(Error::internal_error(
                        "JSON sink workflow did not start with a Start request",
                    ))
                }
                Workflow::Request(Request::Commit(operation, resume)) => {
                    let Commit { metadata, actions } = *operation;
                    let result = EngineConnector::new(engine)
                        .iterate_generator(Ok(actions))
                        .and_then(|actions| committer.commit(engine, Box::new(actions), metadata));
                    resume(result)
                }
                Workflow::Request(request) => self.resume(request),
            };
        }
    }

    /// Start and drive `workflow` through `engine`.
    pub(crate) fn run_with<O: Send + 'static, Fut>(
        engine: &dyn Engine,
        workflow: impl FnOnce(Channel) -> Fut,
    ) -> DeltaResult<O>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        Self::drive(engine, Workflow::start(workflow))
    }

    /// Convert a started generator into an iterator that drives connector requests.
    pub(crate) fn iterate_generator<Item: Send + 'static>(
        self,
        generator: DeltaResult<Generator<Item>>,
    ) -> DeltaResult<EngineGeneratorIterator<Item>> {
        Ok(EngineGeneratorIterator {
            connector: self,
            state: EngineGeneratorState::Prepared(generator?),
        })
    }

    fn storage_connector(&self) -> StorageConnector<'_> {
        StorageConnector::new(self.storage.as_ref(), self.cancellation_token.clone())
    }

    fn resume<N: Send + 'static>(&self, request: Request<N>) -> DeltaResult<N> {
        match request {
            request @ (Request::ListForward(_)
            | Request::ListBackward(_)
            | Request::ReadSmallFile(..)) => self.storage_connector().resume(request),
            Request::ReadParquetFooter(file, resume) => resume(
                self.parquet
                    .read_parquet_footer_with_cancellation(&file, self.cancellation_token.clone()),
            ),
            Request::ReadJson(request) => resume_paged(self, request),
            Request::ReadParquet(request) => resume_paged(self, request),
            #[cfg(feature = "declarative-plans")]
            Request::ExecutePlan(request) => resume_paged(self, request),
            #[cfg(not(feature = "declarative-plans"))]
            Request::ExecutePlan(request) => {
                let err = Error::unsupported("declarative plans are disabled");
                match request {
                    PageRequest::Start(_, resume) => resume(Err(err)),
                    PageRequest::Prepare(_, resume) => resume(Err(err)),
                    PageRequest::Continue(_, resume) => resume(Err(err)),
                }
            }
            Request::CreateEngineData(operation, resume) => resume(
                self.evaluation
                    .create_many(operation.schema, operation.rows),
            ),
            Request::CreateExpressionEvaluator(operation, resume) => resume(
                self.evaluation
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
            Request::WriteJson(_) => Err(Error::internal_error(
                "JSON sink request requires a workflow driver",
            )),
            Request::WriteBytes(operation, resume) => {
                resume(self.check_cancelled().and_then(|()| {
                    self.storage
                        .put(&operation.url, operation.data, operation.overwrite)
                }))
            }
            Request::CopyAtomic(operation, resume) => {
                resume(self.check_cancelled().and_then(|()| {
                    self.storage
                        .copy_atomic(&operation.source, &operation.destination)
                }))
            }
            Request::Commit(..) => Err(Error::internal_error(
                "commit request requires a workflow driver",
            )),
            Request::Publish(..) => Err(Error::internal_error(
                "publish request requires a catalog committer",
            )),
        }
    }

    fn drive_json_sink<O: Send + 'static>(
        &self,
        operation: WriteJsonFile,
        resume: Resume<Workflow<O>, Cursor<WriteJsonFile>>,
    ) -> DeltaResult<Workflow<O>> {
        let mut iterator = EngineJsonSinkIterator {
            connector: self,
            state: Some(EngineJsonSinkState::Start(resume)),
        };
        let result = self
            .json
            .write_json_file(
                &operation.url,
                Box::new(&mut iterator),
                operation.mode.overwrite(),
            )
            .and_then(|_| self.storage.head(&operation.url));
        iterator.finish(result)
    }

    fn check_cancelled(&self) -> DeltaResult<()> {
        check_cancelled(self.cancellation_token.as_ref())
    }
}

impl<O: Send + 'static> EngineJsonSinkIterator<'_, O> {
    fn finish(self, result: DeltaResult<FileMeta>) -> DeltaResult<Workflow<O>> {
        match (self.state, result) {
            (Some(EngineJsonSinkState::Finish(resume)), result) => resume(result),
            (Some(EngineJsonSinkState::Start(resume)), Err(err)) => resume(Err(err)),
            (Some(EngineJsonSinkState::Write(_, resume)), Err(err)) => resume(Err(err)),
            (None, Err(err)) => Err(err),
            (Some(EngineJsonSinkState::Start(_)), Ok(_))
            | (Some(EngineJsonSinkState::Write(_, _)), Ok(_))
            | (None, Ok(_)) => Err(Error::internal_error(
                "JSON handler completed before the sink Finish request",
            )),
        }
    }
}

impl<O: Send + 'static> Iterator for EngineJsonSinkIterator<'_, O> {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take()?;
        let mut workflow = match state {
            EngineJsonSinkState::Start(resume) => resume(Ok(Cursor::new(()))),
            EngineJsonSinkState::Write(sink, resume) => resume(Ok(sink)),
            EngineJsonSinkState::Finish(resume) => {
                self.state = Some(EngineJsonSinkState::Finish(resume));
                return None;
            }
        };

        loop {
            let next = match workflow {
                Ok(next) => next,
                Err(err) => return Some(Err(err)),
            };
            match next {
                Workflow::Done(_) => {
                    return Some(Err(Error::internal_error(
                        "workflow completed before the sink Finish request",
                    )))
                }
                Workflow::Request(Request::WriteJson(SinkRequest::Write(sink, data, resume))) => {
                    self.state = Some(EngineJsonSinkState::Write(sink, resume));
                    return Some(Ok(data));
                }
                Workflow::Request(Request::WriteJson(SinkRequest::Finish(_, resume))) => {
                    self.state = Some(EngineJsonSinkState::Finish(resume));
                    return None;
                }
                Workflow::Request(Request::WriteJson(SinkRequest::Start(..))) => {
                    return Some(Err(Error::internal_error(
                        "workflow started a nested JSON sink",
                    )))
                }
                Workflow::Request(request) => workflow = self.connector.resume(request),
            }
        }
    }
}

fn expression_evaluator(handle: &EvaluatorHandle) -> DeltaResult<&Arc<dyn ExpressionEvaluator>> {
    match handle {
        EvaluatorHandle::Arc(evaluator) => evaluator
            .downcast_ref()
            .ok_or_else(|| Error::internal_error("invalid Engine expression evaluator handle")),
        EvaluatorHandle::Id(_) => Err(Error::internal_error(
            "Engine expression evaluator handle contained an id",
        )),
    }
}

impl EnginePagination<ForwardListing> for StorageConnector<'_> {
    type State = ListingIterator;

    fn initialize(&self, ForwardListing(bounds): ForwardListing) -> DeltaResult<Self::State> {
        let listing = self
            .storage
            .list_from_with_cancellation(&bounds.low, self.cancellation_token.clone())?
            .take_while(move |entry| bounds.contains(entry));
        Ok(Box::new(listing))
    }

    fn next_page(&self, mut listing: ListingIterator) -> DeltaResult<Page<ForwardListing>> {
        let data = Vec::from_iter(listing.by_ref().take(FORWARD_LISTING_PAGE_SIZE));
        let next = (data.len() == FORWARD_LISTING_PAGE_SIZE).then(|| Cursor::new(listing));
        Ok(Page { data, next })
    }
}

impl EnginePagination<BackwardListing> for StorageConnector<'_> {
    type State = BackwardListingState;

    fn initialize(&self, BackwardListing(bounds): BackwardListing) -> DeltaResult<Self::State> {
        Ok(BackwardListingState {
            high: bounds.high_version()?,
            bounds: Box::new(*bounds),
        })
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<BackwardListing>> {
        let BackwardListingState { bounds, high } = state;
        let window = bounds.backward_window(high, BackwardListing::DEFAULT_WINDOW_SIZE)?;
        let next_high = window.next_high;
        let entries = self
            .storage
            .list_from_with_cancellation(&window.low, self.cancellation_token.clone())?
            .take_while(move |entry| window.contains(entry))
            .collect();
        let next = next_high.map(|high| Cursor::new(BackwardListingState { bounds, high }));
        let data = BackwardListingResult {
            entries,
            known_version_boundary: true,
        };
        Ok(Page { data, next })
    }
}

impl EnginePagination<ReadJsonFiles> for EngineConnector {
    type State = EngineDataIterator;

    fn initialize(&self, read: ReadJsonFiles) -> DeltaResult<Self::State> {
        self.json.read_json_files_with_cancellation(
            &read.files,
            read.physical_schema,
            read.predicate,
            self.cancellation_token.clone(),
        )
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<ReadJsonFiles>> {
        next_engine_data(state)
    }
}

impl EnginePagination<ReadParquetFiles> for EngineConnector {
    type State = EngineDataIterator;

    fn initialize(&self, read: ReadParquetFiles) -> DeltaResult<Self::State> {
        self.parquet.read_parquet_files_with_cancellation(
            &read.files,
            read.physical_schema,
            read.predicate,
            self.cancellation_token.clone(),
        )
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<ReadParquetFiles>> {
        next_engine_data(state)
    }
}

#[cfg(feature = "declarative-plans")]
impl EnginePagination<PlanOperation> for EngineConnector {
    type State = EngineDataIterator;

    fn initialize(&self, operation: PlanOperation) -> DeltaResult<Self::State> {
        self.check_cancelled()?;
        self.plan_executor
            .as_deref()
            .ok_or_else(|| Error::unsupported("this engine does not provide a PlanExecutor"))?
            .execute_op(operation)?
            .into_data()
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<PlanOperation>> {
        self.check_cancelled()?;
        next_engine_data(state)
    }
}

impl<Item: Send + 'static> Iterator for EngineGeneratorIterator<Item> {
    type Item = DeltaResult<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = std::mem::replace(&mut self.state, EngineGeneratorState::Exhausted);
        let mut generator = match state {
            EngineGeneratorState::Prepared(generator) => Ok(generator),
            EngineGeneratorState::Active(resume) => resume(Ok(())),
            EngineGeneratorState::Exhausted => return None,
        };

        loop {
            let current = match generator {
                Ok(generator) => generator,
                Err(err) => return Some(Err(err)),
            };
            match current {
                Generator::Done(()) => return None,
                Generator::Yield(item, resume) => {
                    self.state = EngineGeneratorState::Active(resume);
                    return Some(Ok(item));
                }
                Generator::Request(request) => {
                    generator = self.connector.resume(request);
                }
            }
        }
    }
}

fn next_engine_data<Op>(mut reads: EngineDataIterator) -> DeltaResult<Page<Op>>
where
    Op: PagedOperation<Page = Vec<Box<dyn EngineData>>>,
{
    let (data, next) = match reads.next().transpose()? {
        Some(data) => (vec![data], Some(Cursor::new(reads))),
        None => (Vec::new(), None),
    };
    Ok(Page { data, next })
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::sync::SyncEngine;
    use crate::unit_test_utils::TestCancellationToken;

    struct CancelAfterFirstRead {
        token: Arc<TestCancellationToken>,
    }

    impl StorageHandler for CancelAfterFirstRead {
        fn list_from(&self, _path: &Url) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
            Err(Error::generic("unused listing"))
        }

        fn read_files(
            &self,
            _files: Vec<FileSlice>,
        ) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
            let token = Arc::clone(&self.token);
            Ok(Box::new(std::iter::once_with(move || {
                token.cancel();
                Ok(Bytes::from_static(b"data"))
            })))
        }

        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
            Err(Error::generic("unused copy"))
        }

        fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> DeltaResult<()> {
            Err(Error::generic("unused write"))
        }

        fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
            Err(Error::generic("unused head"))
        }

        fn delete(&self, _path: &Url) -> DeltaResult<()> {
            Err(Error::generic("unused delete"))
        }
    }

    #[test]
    fn small_file_read_propagates_cancellation_from_exhaustion_probe() {
        let token = Arc::new(TestCancellationToken::default());
        let cancellation_token: CancellationTokenRef = token.clone();
        let storage = CancelAfterFirstRead { token };
        let file = (Url::parse("memory:///data").unwrap(), None);
        let connector = StorageConnector::new(&storage, Some(cancellation_token));

        let result = connector.read_small_file(file);

        assert!(matches!(result, Err(Error::Cancelled)));
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn plan_continuation_checks_cancellation_before_polling_iterator() {
        let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
        let engine = SyncEngine::new();
        let connector = EngineConnector::new(&engine).with_cancellation_token(token);
        let reads: EngineDataIterator = Box::new(std::iter::from_fn(|| {
            panic!("cancelled plan continuation polled its iterator")
        }));
        let cursor = Cursor::<PlanOperation>::new(reads);

        let result = EnginePagination::continue_from(&connector, cursor);

        assert!(matches!(result, Err(Error::Cancelled)));
    }
}
