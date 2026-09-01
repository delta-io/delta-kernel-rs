//! File and data-read requests.

use std::ops::Range;

use bytes::Bytes;
use url::Url;

use super::core::{PendingPageRequest, PendingRequest};
use super::{Channel, Cursor, DeltaFuture, Page, PageRequest, PagedOperation};
use crate::engine_data::EngineData;
#[cfg(feature = "declarative-plans")]
use crate::plans::Operation as PlanOperation;
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileIndex, FileMeta, ParquetFooter, PredicateRef};

/// Parameters shared by JSON and Parquet reads.
pub struct ReadFileFormatStart {
    /// Files to read in order; batches preserve file and row order and never span files.
    pub files: Vec<FileMeta>,
    /// Exact schema required for every returned batch, including field order and physical names.
    pub physical_schema: SchemaRef,
    /// Optional conservative push-down. Connectors may ignore it; if applied, omit data only when
    /// it cannot be true. Returned data need not satisfy it.
    pub predicate: Option<PredicateRef>,
}

/// Paginated JSON read operation.
pub struct ReadJsonFiles(pub ReadFileFormatStart);

impl PagedOperation for ReadJsonFiles {
    type Page = Vec<Box<dyn EngineData>>;
}

/// Paginated Parquet read operation.
pub struct ReadParquetFiles(pub ReadFileFormatStart);

impl PagedOperation for ReadParquetFiles {
    type Page = Vec<Box<dyn EngineData>>;
}

/// Paginated declarative-plan execution.
#[cfg(feature = "declarative-plans")]
pub struct ExecutePlan(pub PlanOperation);

#[cfg(feature = "declarative-plans")]
impl PagedOperation for ExecutePlan {
    type Page = Vec<Box<dyn EngineData>>;
}

/// Generic kernel pagination driver for operations that return [`EngineData`].
pub(crate) trait EngineDataOperation:
    PagedOperation<Page = Vec<Box<dyn EngineData>>> + Sized
{
    /// Initialize this operation and return its first page.
    fn start(self, channel: &Channel) -> impl DeltaFuture<Page<Self>>;

    /// Continue this operation from `cursor`.
    fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> impl DeltaFuture<Page<Self>>;
}

impl EngineDataOperation for ReadJsonFiles {
    async fn start(self, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.start_read_json(self.0).await
    }

    async fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.continue_read_json(cursor).await
    }
}

impl EngineDataOperation for ReadParquetFiles {
    async fn start(self, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.start_read_parquet(self.0).await
    }

    async fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.continue_read_parquet(cursor).await
    }
}

#[cfg(feature = "declarative-plans")]
impl EngineDataOperation for ExecutePlan {
    async fn start(self, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.start_plan(self.0).await
    }

    async fn continue_from(cursor: Cursor<Self>, channel: &Channel) -> DeltaResult<Page<Self>> {
        channel.continue_plan(cursor).await
    }
}

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
        read: ReadFileFormatStart,
    ) -> DeltaResult<Page<ReadJsonFiles>> {
        self.exchange(ReadJsonFiles(read), |request| {
            PendingRequest::ReadJson(PendingPageRequest::Start(request))
        })
        .await
    }

    /// Initialize a JSON read without fetching its first page.
    pub(crate) async fn prepare_read_json(
        &self,
        read: ReadFileFormatStart,
    ) -> DeltaResult<Cursor<ReadJsonFiles>> {
        self.exchange(ReadJsonFiles(read), |request| {
            PendingRequest::ReadJson(PendingPageRequest::Prepare(request))
        })
        .await
    }

    /// Continue a JSON read from `cursor`.
    pub(crate) async fn continue_read_json(
        &self,
        cursor: Cursor<ReadJsonFiles>,
    ) -> DeltaResult<Page<ReadJsonFiles>> {
        self.exchange(cursor, |request| {
            PendingRequest::ReadJson(PendingPageRequest::Continue(request))
        })
        .await
    }

    /// Initialize a Parquet read and return its first page.
    pub(crate) async fn start_read_parquet(
        &self,
        read: ReadFileFormatStart,
    ) -> DeltaResult<Page<ReadParquetFiles>> {
        self.exchange(ReadParquetFiles(read), |request| {
            PendingRequest::ReadParquet(PendingPageRequest::Start(request))
        })
        .await
    }

    /// Initialize a Parquet read without fetching its first page.
    pub(crate) async fn prepare_read_parquet(
        &self,
        read: ReadFileFormatStart,
    ) -> DeltaResult<Cursor<ReadParquetFiles>> {
        self.exchange(ReadParquetFiles(read), |request| {
            PendingRequest::ReadParquet(PendingPageRequest::Prepare(request))
        })
        .await
    }

    /// Continue a Parquet read from `cursor`.
    pub(crate) async fn continue_read_parquet(
        &self,
        cursor: Cursor<ReadParquetFiles>,
    ) -> DeltaResult<Page<ReadParquetFiles>> {
        self.exchange(cursor, |request| {
            PendingRequest::ReadParquet(PendingPageRequest::Continue(request))
        })
        .await
    }

    /// Execute `plan` and return its first page.
    #[cfg(feature = "declarative-plans")]
    pub(crate) async fn start_plan(&self, plan: PlanOperation) -> DeltaResult<Page<ExecutePlan>> {
        self.exchange(ExecutePlan(plan), |request| {
            PendingRequest::ExecutePlan(PendingPageRequest::Start(request))
        })
        .await
    }

    /// Prepare `plan` without fetching its first page.
    #[cfg(feature = "declarative-plans")]
    pub(crate) async fn prepare_plan(
        &self,
        plan: PlanOperation,
    ) -> DeltaResult<Cursor<ExecutePlan>> {
        self.exchange(ExecutePlan(plan), |request| {
            PendingRequest::ExecutePlan(PendingPageRequest::Prepare(request))
        })
        .await
    }

    /// Continue plan execution from `cursor`.
    #[cfg(feature = "declarative-plans")]
    pub(crate) async fn continue_plan(
        &self,
        cursor: Cursor<ExecutePlan>,
    ) -> DeltaResult<Page<ExecutePlan>> {
        self.exchange(cursor, |request| {
            PendingRequest::ExecutePlan(PendingPageRequest::Continue(request))
        })
        .await
    }
}

impl<N: Send + 'static> PageRequest<N, ReadJsonFiles> {
    /// Forward this JSON request through `parent`.
    pub(super) async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::Start(ReadJsonFiles(read), resume) => {
                resume.resume(parent.start_read_json(read).await)
            }
            Self::Prepare(ReadJsonFiles(read), resume) => {
                resume.resume(parent.prepare_read_json(read).await)
            }
            Self::Continue(cursor, resume) => {
                resume.resume(parent.continue_read_json(cursor).await)
            }
        }
    }
}

impl<N: Send + 'static> PageRequest<N, ReadParquetFiles> {
    /// Forward this Parquet request through `parent`.
    pub(super) async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::Start(ReadParquetFiles(read), resume) => {
                resume.resume(parent.start_read_parquet(read).await)
            }
            Self::Prepare(ReadParquetFiles(read), resume) => {
                resume.resume(parent.prepare_read_parquet(read).await)
            }
            Self::Continue(cursor, resume) => {
                resume.resume(parent.continue_read_parquet(cursor).await)
            }
        }
    }
}

#[cfg(feature = "declarative-plans")]
impl<N: Send + 'static> PageRequest<N, ExecutePlan> {
    /// Forward this plan request through `parent`.
    pub(super) async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::Start(ExecutePlan(plan), resume) => resume.resume(parent.start_plan(plan).await),
            Self::Prepare(ExecutePlan(plan), resume) => {
                resume.resume(parent.prepare_plan(plan).await)
            }
            Self::Continue(cursor, resume) => resume.resume(parent.continue_plan(cursor).await),
        }
    }
}
