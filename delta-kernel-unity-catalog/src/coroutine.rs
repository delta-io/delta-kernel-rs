//! Connector-driven Unity Catalog commit and publish workflows.

use delta_kernel::committer::{CommitMetadata, CommitResponse, PublishMetadata};
use delta_kernel::coroutine::core::{
    Channel as CoreChannel, DeltaFuture, IntoRequest, OutboxEntry, Pending, Step, WorkflowTask,
};
use delta_kernel::coroutine::write::{
    CopyAtomic, FileWriteMode, PendingSinkRequest, SinkWrite, WriteJsonFile,
};
use delta_kernel::coroutine::{Cursor, Page, PagedOperation, Resume, SinkRequest};
use delta_kernel::{DeltaResult, Error, FileMeta, FilteredEngineData};
use derive_more::{Constructor, From};
use unity_catalog_delta_client_api::{TableIdentifier, UpdateTableClient, UpdateTableRequest};
use url::Url;

use crate::UCCommitter;

/// A Unity Catalog workflow that has completed or suspended for connector work.
#[must_use = "a workflow must be driven until it produces Workflow::Done"]
#[derive(From)]
pub enum Workflow<O: Send + 'static> {
    /// Final workflow output.
    Done(O),
    /// Operation the connector must perform before resuming the workflow.
    Request(Request<Self>),
}

/// Operations Unity Catalog may request.
pub enum Request<N: Send + 'static> {
    /// Continue reading the sealed kernel commit-action stream.
    ContinueActions(Cursor<CommitActions>, Resume<N, Page<CommitActions>>),
    /// Write one newline-delimited JSON file from streamed action batches.
    WriteJson(SinkRequest<N, WriteJsonFile>),
    /// Atomically copy a staged commit into the published Delta log.
    CopyAtomic(CopyAtomic, Resume<N, ()>),
    /// Apply an atomic Unity Catalog table update.
    ///
    /// TODO: Replace this semantic UC-client operation with a transport-neutral network request.
    UpdateTable(UpdateTable, Resume<N, ()>),
}

/// Paged operation for a connector-sealed kernel commit-action stream.
pub struct CommitActions;

impl PagedOperation for CommitActions {
    type Page = Vec<FilteredEngineData>;
}

/// Parameters for one Unity Catalog `update_table` request.
#[derive(Constructor)]
pub struct UpdateTable {
    /// Table receiving the update.
    pub target: TableIdentifier,
    /// Atomic requirements and updates to apply.
    pub request: UpdateTableRequest,
}

struct Channel(CoreChannel<PendingRequest>);

impl Channel {
    async fn continue_actions(
        &self,
        cursor: Cursor<CommitActions>,
    ) -> DeltaResult<Page<CommitActions>> {
        self.0
            .exchange(cursor, PendingRequest::ContinueActions)
            .await
    }

    async fn start_json_file(
        &self,
        operation: WriteJsonFile,
    ) -> DeltaResult<Cursor<WriteJsonFile>> {
        self.0.exchange(operation, PendingSinkRequest::Start).await
    }

    async fn write_json_file(
        &self,
        sink: Cursor<WriteJsonFile>,
        data: FilteredEngineData,
    ) -> DeltaResult<Cursor<WriteJsonFile>> {
        let outbound = SinkWrite::new(sink, data);
        self.0.exchange(outbound, PendingSinkRequest::Write).await
    }

    async fn finish_json_file(&self, sink: Cursor<WriteJsonFile>) -> DeltaResult<FileMeta> {
        self.0.exchange(sink, PendingSinkRequest::Finish).await
    }

    async fn copy_atomic(&self, source: Url, destination: Url) -> DeltaResult<()> {
        let outbound = CopyAtomic::new(source, destination);
        self.0.exchange(outbound, PendingRequest::CopyAtomic).await
    }

    async fn update_table(
        &self,
        target: TableIdentifier,
        request: UpdateTableRequest,
    ) -> DeltaResult<()> {
        let outbound = UpdateTable::new(target, request);
        self.0.exchange(outbound, PendingRequest::UpdateTable).await
    }
}

impl<O: Send + 'static> Workflow<O> {
    fn start<Fut>(workflow: impl FnOnce(Channel) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        WorkflowTask::new(|channel| workflow(Channel(channel))).step()
    }
}

#[derive(From)]
enum PendingRequest {
    ContinueActions(Pending<Cursor<CommitActions>, Page<CommitActions>>),
    #[from]
    WriteJson(PendingSinkRequest<WriteJsonFile>),
    CopyAtomic(Pending<CopyAtomic, ()>),
    UpdateTable(Pending<UpdateTable, ()>),
}

impl Default for PendingRequest {
    fn default() -> Self {
        Self::ContinueActions(Pending::default())
    }
}

impl OutboxEntry for PendingRequest {
    fn is_live(&self) -> bool {
        match self {
            Self::ContinueActions(request) => request.is_live(),
            Self::WriteJson(request) => request.is_live(),
            Self::CopyAtomic(request) => request.is_live(),
            Self::UpdateTable(request) => request.is_live(),
        }
    }
}

impl<N: Send + 'static> IntoRequest<N> for PendingRequest {
    type Request = Request<N>;

    fn into_request(self, step: impl Step<N>) -> DeltaResult<Self::Request> {
        match self {
            Self::ContinueActions(request) => request.into_request(step, Request::ContinueActions),
            Self::WriteJson(request) => Ok(Request::WriteJson(request.into_request(step)?)),
            Self::CopyAtomic(request) => request.into_request(step, Request::CopyAtomic),
            Self::UpdateTable(request) => request.into_request(step, Request::UpdateTable),
        }
    }
}

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Start a connector-driven Unity Catalog commit.
    ///
    /// Version zero is written directly to the published log. Later versions are written to a
    /// staged path and ratified with `update_table`.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit metadata is invalid for Unity Catalog or the workflow cannot
    /// start.
    pub fn start_commit(
        &self,
        metadata: CommitMetadata,
        actions: Cursor<CommitActions>,
    ) -> DeltaResult<Workflow<CommitResponse>> {
        self.validate_catalog_managed_state(&metadata)?;
        if metadata.version() == 0 {
            return start_version_zero_commit(metadata, actions);
        }
        Self::validate_no_alter_table_changes(&metadata)?;
        let ratifier = self.ratifier();
        Workflow::start(async move |channel| {
            let staged_path = metadata.staged_commit_path()?;
            let file_meta = write_commit_file(&channel, actions, staged_path).await?;
            let op = ratifier.operation(&metadata, &file_meta)?;
            channel.update_table(op.target, op.request).await?;
            Ok(CommitResponse::Committed { file_meta })
        })
    }

    /// Start publishing ratified commits to the Delta log.
    ///
    /// Copies are requested in ascending version order. Existing destination files are treated as
    /// already published.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot start.
    pub fn start_publish(&self, metadata: PublishMetadata) -> DeltaResult<Workflow<()>> {
        Workflow::start(async move |channel| {
            for commit in metadata.into_commits_to_publish() {
                let result = channel
                    .copy_atomic(commit.location, commit.published_location)
                    .await;
                match result {
                    Ok(()) | Err(Error::FileAlreadyExists(_)) => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(())
        })
    }
}

fn start_version_zero_commit(
    metadata: CommitMetadata,
    actions: Cursor<CommitActions>,
) -> DeltaResult<Workflow<CommitResponse>> {
    Workflow::start(async move |channel| {
        let version = metadata.version();
        let in_commit_timestamp = metadata.in_commit_timestamp();
        let published_path = metadata.published_commit_path()?;
        match write_commit_file(&channel, actions, published_path).await {
            Ok(mut file_meta) => {
                file_meta.last_modified = in_commit_timestamp;
                Ok(CommitResponse::Committed { file_meta })
            }
            Err(Error::FileAlreadyExists(_)) => Ok(CommitResponse::Conflict { version }),
            Err(err) => Err(err),
        }
    })
}

async fn write_commit_file(
    channel: &Channel,
    actions: Cursor<CommitActions>,
    url: Url,
) -> DeltaResult<FileMeta> {
    let mut sink = channel
        .start_json_file(WriteJsonFile::new(url, FileWriteMode::CreateNew))
        .await?;
    let mut next = Some(actions);
    while let Some(cursor) = next {
        let page = channel.continue_actions(cursor).await?;
        for action in page.data {
            sink = channel.write_json_file(sink, action).await?;
        }
        next = page.next;
    }
    channel.finish_json_file(sink).await
}
