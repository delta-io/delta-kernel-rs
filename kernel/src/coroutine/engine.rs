//! Reference connector that serves coroutine requests through [`Engine`] handlers.
//!
//! Engine iterators travel in boxed cursors. [`drive_storage`] serves only listing and small-file
//! requests.

use std::any::Any;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(test)]
use url::Url;

use super::listing::{
    backward_listing_window, is_within_listing_bounds, version_from_listing_bound,
    DEFAULT_BACKWARD_LISTING_WINDOW_SIZE, DEFAULT_FORWARD_LISTING_PAGE_SIZE,
};
#[cfg(feature = "declarative-plans")]
use super::ExecutePlan;
use super::{
    BackwardListing, BackwardListingResult, Channel, Cursor, CursorState, DeltaFuture,
    ForwardListing, Generator, ListingBounds, Page, PageRequest, PagedOperation, ReadJsonFiles,
    ReadParquetFiles, Request, Workflow, YieldResume,
};
use crate::cancellation::{check_cancelled, CancellationTokenRef};
use crate::engine_data::EngineData;
#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{
    DeltaResult, DeltaResultIteratorStatic, Engine, Error, FileDataReadResultIterator, FileMeta,
    FileSlice, JsonHandler, ParquetHandler, StorageHandler, Version,
};

const FORWARD_LISTING_PAGE_SIZE: usize = if cfg!(test) {
    2
} else {
    DEFAULT_FORWARD_LISTING_PAGE_SIZE
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

    const DESCRIPTION: &'static str;

    fn initialize(&self, operation: Op) -> DeltaResult<Self::State>;

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<Op>>;

    fn start(&self, operation: Op) -> DeltaResult<Page<Op>> {
        self.next_page(self.initialize(operation)?)
    }

    fn prepare(&self, operation: Op) -> DeltaResult<Cursor<Op>> {
        Ok(Cursor::boxed(self.initialize(operation)?))
    }

    fn continue_from(&self, cursor: Cursor<Op>) -> DeltaResult<Page<Op>> {
        let description: &str = Self::DESCRIPTION;
        let CursorState::Boxed(state) = cursor.into_state() else {
            return Err(Error::internal_error(format!(
                "coroutine Engine cursor did not contain {description} state"
            )));
        };
        let state = state.downcast().map(|state| *state).map_err(|_| {
            Error::internal_error(format!(
                "coroutine Engine cursor had invalid {description} state"
            ))
        })?;
        self.next_page(state)
    }
}

fn resume_paged<C, Op, N>(connector: &C, request: PageRequest<N, Op>) -> DeltaResult<N>
where
    C: EnginePagination<Op>,
    Op: PagedOperation,
    N: Send + 'static,
{
    match request {
        PageRequest::Start(operation, resume) => {
            resume.resume(EnginePagination::start(connector, operation))
        }
        PageRequest::Prepare(operation, resume) => {
            resume.resume(EnginePagination::prepare(connector, operation))
        }
        PageRequest::Continue(cursor, resume) => {
            resume.resume(EnginePagination::continue_from(connector, cursor))
        }
    }
}

// Ideally, this would just take `Arc<dyn Engine>`, so we could create handlers lazily; but most
// kernel entry points only have access to `&dyn Engine` so we must eagerly instantiate them.
pub(crate) struct EngineConnector {
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
    cancellation_token: Option<CancellationTokenRef>,
}

// Some kernel entry points (and a large number of unit tests) only have access to a manually
// created `StorageHandler` instance instead of a full `EngineData`; they use this connector.
struct StorageConnector<'a> {
    storage: &'a dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
}

// Holds the kernel `Generator` between calls to `EngineGeneratorIterator::next`
enum EngineGeneratorState<Item: Send + 'static> {
    Active(Generator<(), Item>),
    Yielded(YieldResume<Generator<(), Item>>),
    Exhausted,
}

/// Engine-side iterator over a generator, which surfaces yielded items while using an
/// `EngineConnector` to drive I/O requests.
pub(crate) struct EngineGeneratorIterator<Item: Send + 'static> {
    connector: EngineConnector,
    state: EngineGeneratorState<Item>,
}

/// An almost test-only helper; the two prod uses are `LogSegment::for_timestamp_conversion` and
/// `LogSegment::for_table_changes`. The former could be easily converted to Engine-based
/// coroutines, but the latter has many testing call sites that only have a StorageHandler.
pub(crate) fn drive_storage<O: Send + 'static, Fut>(
    storage: &dyn StorageHandler,
    cancellation_token: Option<CancellationTokenRef>,
    workflow: impl FnOnce(Channel) -> Fut,
) -> DeltaResult<O>
where
    Fut: DeltaFuture<O> + 'static,
{
    let connector = StorageConnector {
        storage,
        cancellation_token,
    };
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
            Request::ReadSmallFile(file, resume) => resume.resume(self.read_small_file(file)),
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
        self.drive(Workflow::start(workflow))
    }

    /// Drive a started `workflow` to completion.
    pub(crate) fn drive<O: Send + 'static>(
        &self,
        mut workflow: DeltaResult<Workflow<O>>,
    ) -> DeltaResult<O> {
        loop {
            workflow = match workflow? {
                Workflow::Done(output) => return Ok(output),
                Workflow::Request(request) => self.resume(request),
            };
        }
    }

    /// Start and drive `workflow` through `engine`.
    #[cfg(test)]
    pub(crate) fn run_with<O: Send + 'static, Fut>(
        engine: &dyn Engine,
        workflow: impl FnOnce(Channel) -> Fut,
    ) -> DeltaResult<O>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        Self::new(engine).run(workflow)
    }

    /// Convert a started generator into an iterator that drives connector requests.
    pub(crate) fn iterate_generator<Item: Send + 'static>(
        self,
        generator: DeltaResult<Generator<(), Item>>,
    ) -> DeltaResult<EngineGeneratorIterator<Item>> {
        Ok(EngineGeneratorIterator {
            connector: self,
            state: EngineGeneratorState::Active(generator?),
        })
    }

    fn storage_connector(&self) -> StorageConnector<'_> {
        StorageConnector {
            storage: self.storage.as_ref(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }

    fn resume<N: Send + 'static>(&self, request: Request<N>) -> DeltaResult<N> {
        match request {
            request @ (Request::ListForward(_)
            | Request::ListBackward(_)
            | Request::ReadSmallFile(..)) => self.storage_connector().resume(request),
            Request::ReadParquetFooter(file, resume) => resume.resume(
                self.parquet
                    .read_parquet_footer_with_cancellation(&file, self.cancellation_token.clone()),
            ),
            Request::ReadJson(request) => resume_paged(self, request),
            Request::ReadParquet(request) => resume_paged(self, request),
            #[cfg(feature = "declarative-plans")]
            Request::ExecutePlan(request) => resume_paged(self, request),
            Request::WriteBytes(operation, resume) => {
                resume.resume(self.check_cancelled().and_then(|()| {
                    self.storage
                        .put(&operation.url, operation.data, operation.overwrite)
                }))
            }
        }
    }

    fn check_cancelled(&self) -> DeltaResult<()> {
        check_cancelled(self.cancellation_token.as_ref())
    }
}

impl EnginePagination<ForwardListing> for StorageConnector<'_> {
    type State = ListingIterator;

    const DESCRIPTION: &'static str = "forward listing";

    fn initialize(&self, ForwardListing(bounds): ForwardListing) -> DeltaResult<Self::State> {
        let listing = self
            .storage
            .list_from_with_cancellation(&bounds.low, self.cancellation_token.clone())?
            .take_while(move |entry| is_within_listing_bounds(entry, &bounds.prefix, &bounds.high));
        Ok(Box::new(listing))
    }

    fn next_page(&self, mut listing: ListingIterator) -> DeltaResult<Page<ForwardListing>> {
        let data = Vec::from_iter(listing.by_ref().take(FORWARD_LISTING_PAGE_SIZE));
        let next = (data.len() == FORWARD_LISTING_PAGE_SIZE).then(|| Cursor::boxed(listing));
        Ok(Page { data, next })
    }
}

impl EnginePagination<BackwardListing> for StorageConnector<'_> {
    type State = BackwardListingState;

    const DESCRIPTION: &'static str = "backward listing";

    fn initialize(&self, BackwardListing(bounds): BackwardListing) -> DeltaResult<Self::State> {
        Ok(BackwardListingState {
            high: version_from_listing_bound(&bounds.high)?,
            bounds: Box::new(*bounds),
        })
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<BackwardListing>> {
        let BackwardListingState { bounds, high } = state;
        let window = backward_listing_window(&bounds, high, DEFAULT_BACKWARD_LISTING_WINDOW_SIZE)?;
        let entries = self
            .storage
            .list_from_with_cancellation(&window.low, self.cancellation_token.clone())?
            .take_while(|entry| is_within_listing_bounds(entry, &bounds.prefix, &window.high))
            .collect();
        let next = window
            .next_high
            .map(|high| Cursor::boxed(BackwardListingState { bounds, high }));
        let data = BackwardListingResult {
            entries,
            known_version_boundary: true,
        };
        Ok(Page { data, next })
    }
}

impl EnginePagination<ReadJsonFiles> for EngineConnector {
    type State = EngineDataIterator;

    const DESCRIPTION: &'static str = "JSON read";

    fn initialize(&self, ReadJsonFiles(read): ReadJsonFiles) -> DeltaResult<Self::State> {
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

    const DESCRIPTION: &'static str = "Parquet read";

    fn initialize(&self, ReadParquetFiles(read): ReadParquetFiles) -> DeltaResult<Self::State> {
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
impl EnginePagination<ExecutePlan> for EngineConnector {
    type State = EngineDataIterator;

    const DESCRIPTION: &'static str = "plan execution";

    fn initialize(&self, ExecutePlan(operation): ExecutePlan) -> DeltaResult<Self::State> {
        self.check_cancelled()?;
        self.plan_executor
            .as_deref()
            .ok_or_else(|| Error::unsupported("this engine does not provide a PlanExecutor"))?
            .execute_op(operation)?
            .into_data()
    }

    fn next_page(&self, state: Self::State) -> DeltaResult<Page<ExecutePlan>> {
        self.check_cancelled()?;
        next_engine_data(state)
    }
}

impl<Item: Send + 'static> Iterator for EngineGeneratorIterator<Item> {
    type Item = DeltaResult<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = std::mem::replace(&mut self.state, EngineGeneratorState::Exhausted);
        let mut generator = match state {
            EngineGeneratorState::Active(generator) => Ok(generator),
            EngineGeneratorState::Yielded(resume) => resume.resume(Ok(())),
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
                    self.state = EngineGeneratorState::Yielded(resume);
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
        Some(data) => (vec![data], Some(Cursor::boxed(reads))),
        None => (Vec::new(), None),
    };
    Ok(Page { data, next })
}

#[cfg(test)]
mod tests {
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
        let connector = StorageConnector {
            storage: &storage,
            cancellation_token: Some(cancellation_token),
        };

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
        let cursor = Cursor::<ExecutePlan>::boxed(reads);

        let result = EnginePagination::continue_from(&connector, cursor);

        assert!(matches!(result, Err(Error::Cancelled)));
    }
}
