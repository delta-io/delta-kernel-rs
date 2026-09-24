//! Drives coroutine workflows by serving requests through an [`Engine`].
//!
//! This adapter backs Engine-compatible entry points and generator iterators. It handles requests
//! synchronously through Engine handlers; connector-native async execution uses its own driver.

use std::any::Any;
use std::borrow::Borrow;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel_derive::internal_api;
use derive_more::Constructor;

use super::evaluation::EvaluatorHandle;
use super::kernel::{GeneratorStep, StaticGenerator, StaticWorkflow, Workflow};
use super::listing::{BackwardListing, BackwardListingResult, ForwardListing, ListingBounds};
use super::read::{ReadJsonFiles, ReadParquetFiles};
use super::workflow::drive_workflow;
use super::write::WriteJsonFile;
#[cfg(feature = "declarative-plans")]
use super::PlanOperation;
use super::{Cursor, Page, PageRequest, PagedOperation, Request};
use crate::cancellation::{check_cancelled, CancellationTokenRef};
use crate::committer::{Committer, FileSystemCommitter};
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

/// Maps paged requests onto a synchronous Engine iterator stored in an opaque cursor.
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

fn respond_paged<C, Op>(connector: &C, request: PageRequest<Op>)
where
    C: EnginePagination<Op>,
    Op: PagedOperation,
{
    match request {
        PageRequest::Start(operation, reply) => {
            reply.send(C::start(connector, operation));
        }
        PageRequest::Prepare(operation, reply) => {
            reply.send(C::prepare(connector, operation));
        }
        PageRequest::Continue(cursor, reply) => {
            reply.send(C::continue_from(connector, cursor));
        }
    }
}

/// Serves coroutine requests through handlers captured from an [`Engine`].
///
/// Engine only exposes handlers through borrowed access, so this adapter captures them eagerly.
pub(crate) struct EngineConnector {
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
    evaluation: Arc<dyn EvaluationHandler>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
    cancellation_token: Option<CancellationTokenRef>,
}

/// Serves entry points that intentionally require only storage access.
#[derive(Constructor)]
struct StorageConnector<'storage> {
    storage: &'storage dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
}

/// Iterator over yielded items that serves the generator's connector requests.
pub(crate) struct EngineGeneratorIterator<Item: Send + 'static, C = EngineConnector> {
    connector: C,
    state: GeneratorState<Item>,
}

enum GeneratorState<Item: Send + 'static> {
    Active(StaticGenerator<Item>),
    Complete,
    Failed,
}

/// Drive a workflow using only storage operations.
#[internal_api]
pub(crate) fn drive_storage<O>(
    storage: &dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
    workflow: Workflow<'_, O>,
) -> DeltaResult<O> {
    let connector = StorageConnector::new(storage, cancellation_token);
    Ok(drive_workflow!(workflow, |request| connector.respond(request)?))
}

/// Drive `workflow` through `engine`, capturing its handlers when the first request arrives.
#[internal_api]
pub(crate) fn drive_workflow<O>(engine: &dyn Engine, workflow: Workflow<'_, O>) -> DeltaResult<O> {
    let mut connector = None;
    Ok(drive_workflow!(workflow, |request| connector
        .get_or_insert_with(|| EngineConnector::new(engine))
        .respond_workflow(request)?))
}

impl StorageConnector<'_> {
    fn respond(&self, request: Request) -> DeltaResult<()> {
        match request {
            Request::ListForward(request) => respond_paged(self, request),
            Request::ListBackward(request) => respond_paged(self, request),
            Request::ReadSmallFile(file, reply) => {
                reply.send(self.read_small_file(file));
            }
            Request::CopyAtomic(operation, reply) => {
                reply.send(
                    check_cancelled(self.cancellation_token.as_ref()).and_then(|()| {
                        self.storage
                            .copy_atomic(&operation.source, &operation.destination)
                    }),
                );
            }
            _ => {
                return Err(Error::internal_error(
                    "storage-only coroutine requested a non-storage operation",
                ))
            }
        }
        Ok(())
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
    /// Capture the handlers exposed by `engine`.
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

    /// Drive `workflow` through this connector.
    pub(crate) fn run<O: Send + 'static>(&self, workflow: StaticWorkflow<O>) -> DeltaResult<O> {
        self.drive_workflow(workflow)
    }

    /// Drive an existing workflow through this connector.
    pub(crate) fn drive_workflow<O>(&self, workflow: Workflow<'_, O>) -> DeltaResult<O> {
        Ok(drive_workflow!(workflow, |request| self.respond_workflow(request)?))
    }

    /// Drive a workflow whose catalog requests are handled by `committer`.
    pub(crate) fn drive_with_committer<O>(
        &self,
        workflow: Workflow<'_, O>,
        engine: &dyn Engine,
        committer: &dyn Committer,
    ) -> DeltaResult<O> {
        Ok(drive_workflow!(workflow, |request| match request {
            Request::Publish(metadata, reply) => {
                reply.send(committer.publish(engine, metadata));
            }
            Request::Commit(operation, reply) => {
                reply.send(
                    operation
                        .metadata
                        .validate_committer(committer.is_catalog_committer())
                        .and_then(|()| committer.commit_prepared(engine, *operation)),
                );
            }
            request => self.respond(request)?,
        }))
    }

    /// Return an iterator that drives `generator` and its connector requests.
    pub(crate) fn iterate_generator<Item: Send + 'static>(
        self,
        generator: impl Into<StaticGenerator<Item>>,
    ) -> EngineGeneratorIterator<Item> {
        EngineGeneratorIterator {
            connector: self,
            state: GeneratorState::Active(generator.into()),
        }
    }

    fn storage_connector(&self) -> StorageConnector<'_> {
        StorageConnector::new(self.storage.as_ref(), self.cancellation_token.clone())
    }

    fn respond_workflow(&self, request: Request) -> DeltaResult<()> {
        match request {
            Request::Commit(operation, reply) => {
                reply.send(
                    FileSystemCommitter::commit_workflow(*operation)
                        .and_then(|workflow| self.run(workflow)),
                );
                Ok(())
            }
            request => self.respond(request),
        }
    }

    fn respond(&self, request: Request) -> DeltaResult<()> {
        match request {
            request @ (Request::ListForward(_)
            | Request::ListBackward(_)
            | Request::ReadSmallFile(..)) => self.storage_connector().respond(request)?,
            Request::ReadParquetFooter(file, reply) => {
                reply.send(
                    self.parquet.read_parquet_footer_with_cancellation(
                        &file,
                        self.cancellation_token.clone(),
                    ),
                );
            }
            Request::ReadJson(request) => respond_paged(self, request),
            Request::ReadParquet(request) => respond_paged(self, request),
            #[cfg(feature = "declarative-plans")]
            Request::ExecutePlan(request) => respond_paged(self, request),
            #[cfg(not(feature = "declarative-plans"))]
            Request::ExecutePlan(request) => {
                let err = Error::unsupported("declarative plans are disabled");
                match request {
                    PageRequest::Start(_, reply) => reply.send(Err(err)),
                    PageRequest::Prepare(_, reply) => {
                        reply.send(Err(err));
                    }
                    PageRequest::Continue(_, reply) => reply.send(Err(err)),
                }
            }
            Request::CreateEngineData(operation, reply) => {
                reply.send(
                    self.evaluation
                        .create_many(operation.schema, operation.rows),
                );
            }
            Request::CreateExpressionEvaluator(operation, reply) => {
                reply.send(
                    self.evaluation
                        .new_expression_evaluator(
                            operation.input_schema,
                            operation.expression,
                            operation.output_type,
                        )
                        .map(|evaluator| Arc::new(evaluator) as _),
                );
            }
            Request::EvaluateExpression(operation, reply) => {
                reply.send(
                    expression_evaluator(&operation.evaluator)
                        .and_then(|evaluator| evaluator.evaluate(operation.input.as_ref())),
                );
            }
            Request::EvaluateFilteredExpression(operation, reply) => {
                reply.send(
                    expression_evaluator(&operation.evaluator).and_then(|evaluator| {
                        let selection = operation.input.selection_vector().to_vec();
                        let output = evaluator.evaluate(operation.input.data())?;
                        FilteredEngineData::try_new(output, selection)
                    }),
                );
            }
            Request::WriteJson(operation, reply) => {
                reply.send(self.write_json_file(*operation));
            }
            Request::WriteBytes(operation, reply) => {
                reply.send(self.check_cancelled().and_then(|()| {
                    self.storage
                        .put(&operation.url, operation.data, operation.overwrite)
                }));
            }
            Request::CopyAtomic(operation, reply) => {
                reply.send(self.check_cancelled().and_then(|()| {
                    self.storage
                        .copy_atomic(&operation.source, &operation.destination)
                }));
            }
            Request::Commit(..) => {
                return Err(Error::internal_error(
                    "commit request requires a workflow driver",
                ))
            }
            Request::Publish(..) => {
                return Err(Error::internal_error(
                    "publish request requires a catalog committer",
                ))
            }
        }
        Ok(())
    }

    fn write_json_file(&self, operation: WriteJsonFile) -> DeltaResult<FileMeta> {
        let WriteJsonFile { url, mode, input } = operation;
        let mut iterator = EngineGeneratorIterator {
            connector: self,
            state: GeneratorState::Active(input),
        };
        self.json
            .write_json_file(&url, Box::new(&mut iterator), mode.overwrite())?;
        if !matches!(iterator.state, GeneratorState::Complete) {
            return Err(Error::internal_error(
                "JSON handler completed before exhausting its input",
            ));
        }
        self.storage.head(&url)
    }

    fn check_cancelled(&self) -> DeltaResult<()> {
        check_cancelled(self.cancellation_token.as_ref())
    }
}

/// Construct and drive a workflow through an [`Engine`].
#[internal_api]
macro_rules! run_workflow_with_engine {
    ($engine:expr, $body:expr) => {
        $crate::coroutine::drive_workflow($engine, $crate::coroutine::kernel::workflow!($body))
    };
}

/// Construct and drive a workflow through a storage handler.
#[internal_api]
macro_rules! run_workflow_with_storage {
    ($storage:expr, $cancellation_token:expr, $body:expr) => {
        $crate::coroutine::drive_storage(
            $storage,
            $cancellation_token,
            $crate::coroutine::kernel::workflow!($body),
        )
    };
}

/// Construct and drive a workflow through an [`EngineConnector`].
#[internal_api]
macro_rules! run_workflow_with_connector {
    ($connector:expr, $body:expr) => {
        $connector.drive_workflow($crate::coroutine::kernel::workflow!($body))
    };
}

fn expression_evaluator(handle: &EvaluatorHandle) -> DeltaResult<&Arc<dyn ExpressionEvaluator>> {
    handle
        .downcast_ref()
        .ok_or_else(|| Error::internal_error("invalid Engine expression evaluator handle"))
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

impl<C: Borrow<EngineConnector>, Item: Send + 'static> Iterator
    for EngineGeneratorIterator<Item, C>
{
    type Item = DeltaResult<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        next_generator(self.connector.borrow(), &mut self.state)
    }
}

fn next_generator<Item: Send + 'static>(
    connector: &EngineConnector,
    state: &mut GeneratorState<Item>,
) -> Option<DeltaResult<Item>> {
    let GeneratorState::Active(generator) = state else {
        return None;
    };
    if let Err(err) = connector.check_cancelled() {
        *state = GeneratorState::Failed;
        return Some(Err(err));
    }

    loop {
        match generator.advance() {
            Ok(GeneratorStep::Done) => {
                *state = GeneratorState::Complete;
                return None;
            }
            Ok(GeneratorStep::Yield(item)) => return Some(Ok(item)),
            Ok(GeneratorStep::Request(request)) => {
                if let Err(err) = connector.respond(request) {
                    *state = GeneratorState::Failed;
                    return Some(Err(err));
                }
            }
            Err(err) => {
                *state = GeneratorState::Failed;
                return Some(Err(err));
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

    #[cfg(feature = "default-engine-base")]
    mod json_write {
        use super::*;
        use crate::committer::CommitActions;
        use crate::coroutine::write::FileWriteMode;
        use crate::engine::sync::SyncEngine;
        use crate::engine::test_delegating::DelegatingEngine;
        use crate::schema::SchemaRef;
        use crate::{DeltaResultIterator, FileSize, PredicateRef};

        struct EarlyReturnJsonHandler;

        impl JsonHandler for EarlyReturnJsonHandler {
            fn parse_json(
                &self,
                _json_strings: Box<dyn EngineData>,
                _output_schema: SchemaRef,
            ) -> DeltaResult<Box<dyn EngineData>> {
                Err(Error::generic("unused JSON parsing"))
            }

            fn read_json_files(
                &self,
                _files: &[FileMeta],
                _physical_schema: SchemaRef,
                _predicate: Option<PredicateRef>,
            ) -> DeltaResult<FileDataReadResultIterator> {
                Err(Error::generic("unused JSON read"))
            }

            fn write_json_file(
                &self,
                _path: &Url,
                _data: DeltaResultIterator<'_, FilteredEngineData>,
                _overwrite: bool,
            ) -> DeltaResult<FileSize> {
                Ok(0)
            }
        }

        #[test]
        fn json_handler_cannot_succeed_without_exhausting_generator() {
            let inner: Arc<dyn Engine> = Arc::new(SyncEngine::new());
            let engine =
                DelegatingEngine::new(inner).with_json_handler(Arc::new(EarlyReturnJsonHandler));
            let actions: DeltaResultIteratorStatic<FilteredEngineData> =
                Box::new(std::iter::empty());
            let input = CommitActions::from_iterator(actions);
            let operation = WriteJsonFile::new(
                Url::parse("memory:///_delta_log/0.json").unwrap(),
                FileWriteMode::CreateNew,
                input,
            );

            let result = EngineConnector::new(&engine).write_json_file(operation);

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("before exhausting its input"));
        }
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
