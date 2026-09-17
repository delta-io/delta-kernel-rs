//! Connector-driven Unity Catalog commit and publish workflows.
//!
//! Commit and publish tasks request catalog work from their connector. JSON writes carry a
//! generator task whose kernel requests are driven by the connector performing the write.
//! Kernel and catalog requests remain separate vocabularies: serving a catalog `WriteJson` request
//! may temporarily drive its kernel generator, but neither workflow invokes connector code.
//!
//! Connectors use [`CommitWorkflow::start`] and [`PublishWorkflow::start`] for root operations.
//! [`CommitWorkflow::run_with`] and [`PublishWorkflow::run_with`] compose work into an existing
//! catalog task when its [`Channel`] is already available.

use std::future::Future;
use std::mem::ManuallyDrop;

use delta_kernel::committer::{CommitActions, CommitMetadata, CommitResponse, PublishMetadata};
use delta_kernel::coroutine::core::{Channel as CoreChannel, Receiver, Step, Task};
use delta_kernel::coroutine::write::{CopyAtomic, FileWriteMode, WriteJsonFile};
use delta_kernel::coroutine::Reply;
use delta_kernel::{DeltaResult, Error, FileMeta};
use derive_more::{Constructor, From};
use tracing::{error, Instrument as _, Span};
use unity_catalog_delta_client_api::{TableIdentifier, UpdateTableClient, UpdateTableRequest};
use url::Url;

use crate::UCCommitter;

/// Catalog operations a Unity Catalog workflow may request from its driver.
#[derive(From)]
pub enum Request {
    /// Write one newline-delimited JSON file from streamed action batches.
    WriteJson(Box<WriteJsonFile>, Reply<FileMeta>),
    /// Atomically copy a staged commit into the published Delta log.
    CopyAtomic(CopyAtomic, Reply<()>),
    /// Apply one atomic Unity Catalog table update.
    UpdateTable(UpdateTable, Reply<()>),
}

/// Target and changes for one Unity Catalog `update_table` call.
#[derive(Constructor)]
pub struct UpdateTable {
    /// Table receiving the update.
    pub target: TableIdentifier,
    /// Atomic requirements and updates to apply.
    pub request: UpdateTableRequest,
}

/// Borrowed capability used by a UC workflow to post catalog requests to its parent task.
pub struct Channel(CoreChannel<Request>);

impl Channel {
    async fn exchange<Out: Send + 'static, In: Send + 'static, P>(
        &self,
        outbound: Out,
        make_request: impl FnOnce(Out, Reply<In>) -> P,
    ) -> DeltaResult<In>
    where
        P: Into<Request>,
    {
        self.0
            .request(|reply| make_request(outbound, reply).into())
            .await
    }

    async fn write_json_file(&self, operation: WriteJsonFile) -> DeltaResult<FileMeta> {
        self.exchange(Box::new(operation), Request::WriteJson).await
    }

    async fn copy_atomic(&self, source: Url, destination: Url) -> DeltaResult<()> {
        let outbound = CopyAtomic::new(source, destination);
        self.exchange(outbound, Request::CopyAtomic).await
    }

    pub(crate) async fn update_table(
        &self,
        target: TableIdentifier,
        request: UpdateTableRequest,
    ) -> DeltaResult<()> {
        let outbound = UpdateTable::new(target, request);
        self.exchange(outbound, Request::UpdateTable).await
    }
}

/// Lazy Unity Catalog commit that requests catalog work from its connector.
pub struct CommitWorkflow<C: UpdateTableClient> {
    committer: UCCommitter<C>,
    metadata: CommitMetadata,
    actions: CommitActions,
    span: Span,
}

impl<C: UpdateTableClient + 'static> CommitWorkflow<C> {
    /// Run this commit through an existing catalog task's borrowed channel.
    ///
    /// Requests surface from the parent task.
    pub fn run_with<'a>(
        self,
        channel: &'a Channel,
    ) -> impl Future<Output = DeltaResult<CommitResponse>> + Send + 'a {
        let span = self.span.clone();
        self.run(channel).instrument(span)
    }

    async fn run(self, channel: &Channel) -> DeltaResult<CommitResponse> {
        let Self {
            committer,
            metadata,
            actions,
            ..
        } = self;
        match metadata.version() {
            0 => {
                committer
                    .commit_version_0(channel, actions, &metadata)
                    .await
            }
            _ => {
                committer
                    .commit_version_non_zero(channel, actions, metadata)
                    .await
            }
        }
    }

    /// Start an independently driven commit task with its own catalog request channel.
    ///
    /// The caller must retain and advance the task, complete every request's reply, and stop after
    /// [`CommitStep::Done`].
    pub fn start(self) -> CommitTask {
        let span = self.span.clone();
        CommitTask(Task::single_lane(move |channel| {
            let channel = Channel(channel);
            let body = async move { self.run(&channel).await };
            track_root(body, span)
        }))
    }
}

/// Connector-owned task for one started [`CommitWorkflow`].
pub struct CommitTask(Task<'static, Receiver<Request>, CommitResponse>);

impl CommitTask {
    /// Run until the commit completes or requests catalog work.
    ///
    /// Calling after completion or before the preceding request receives a response returns an
    /// error.
    pub fn advance(&mut self) -> DeltaResult<CommitStep> {
        self.0.advance()
    }
}

/// Completion or catalog work returned by advancing a [`CommitTask`].
pub type CommitStep = Step<Request, CommitResponse>;

/// Connector-owned task for one started [`PublishWorkflow`].
pub struct PublishTask(Task<'static, Receiver<Request>, ()>);

impl PublishTask {
    /// Run until publishing completes or requests catalog work.
    ///
    /// Calling after completion or before the preceding request receives a response returns an
    /// error.
    pub fn advance(&mut self) -> DeltaResult<PublishStep> {
        self.0.advance()
    }
}

/// Completion or catalog work returned by advancing a [`PublishTask`].
pub type PublishStep = Step<Request, ()>;

/// A lazy Unity Catalog publish workflow.
pub struct PublishWorkflow {
    metadata: PublishMetadata,
    span: Span,
}

impl PublishWorkflow {
    /// Run this publish through an existing catalog task's borrowed channel.
    ///
    /// Requests surface from the parent task.
    pub fn run_with<'a>(
        self,
        channel: &'a Channel,
    ) -> impl Future<Output = DeltaResult<()>> + Send + 'a {
        let span = self.span.clone();
        self.run(channel).instrument(span)
    }

    async fn run(self, channel: &Channel) -> DeltaResult<()> {
        for commit in self.metadata.into_commits_to_publish() {
            let result = channel
                .copy_atomic(commit.location, commit.published_location)
                .await;
            match result {
                Ok(()) | Err(Error::FileAlreadyExists(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Start an independently driven publish task with its own catalog request channel.
    ///
    /// The caller must retain and advance the task, complete every request's reply, and stop after
    /// [`PublishStep::Done`].
    pub fn start(self) -> PublishTask {
        let span = self.span.clone();
        PublishTask(Task::single_lane(move |channel| {
            let channel = Channel(channel);
            let body = async move { self.run(&channel).await };
            track_root(body, span)
        }))
    }
}

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Return a lazy connector-driven Unity Catalog commit workflow.
    ///
    /// Version zero is written directly to the published log. Later versions are written to a
    /// staged path and ratified with `update_table`.
    pub fn commit_workflow(
        &self,
        metadata: CommitMetadata,
        actions: CommitActions,
    ) -> CommitWorkflow<C>
    where
        C: 'static,
    {
        CommitWorkflow {
            committer: self.clone(),
            metadata,
            actions,
            span: Span::current(),
        }
    }

    /// Return a lazy workflow that publishes ratified commits to the Delta log.
    ///
    /// Copies are requested in ascending version order. Existing destination files are treated as
    /// already published.
    pub fn publish_workflow(&self, metadata: PublishMetadata) -> PublishWorkflow {
        PublishWorkflow {
            metadata,
            span: Span::current(),
        }
    }
}

pub(crate) async fn write_commit_file(
    channel: &Channel,
    actions: CommitActions,
    url: Url,
) -> DeltaResult<FileMeta> {
    let operation = WriteJsonFile::new(url, FileWriteMode::CreateNew, actions.start());
    channel.write_json_file(operation).await
}

fn track_root<O>(
    future: impl Future<Output = DeltaResult<O>> + Send,
    span: Span,
) -> impl Future<Output = DeltaResult<O>> + Send {
    async move {
        let guard = WorkflowCompletionGuard;
        let output = future.await;
        let _ = ManuallyDrop::new(guard);
        output.inspect_err(|err| error!(error = %err, "UC coroutine workflow failed"))
    }
    .instrument(span)
}

struct WorkflowCompletionGuard;

impl Drop for WorkflowCompletionGuard {
    fn drop(&mut self) {
        error!(
            error = "abandoned",
            "UC coroutine workflow was abandoned while suspended"
        );
    }
}
