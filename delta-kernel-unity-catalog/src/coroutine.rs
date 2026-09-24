//! Connector-driven Unity Catalog commit and publish workflows.
//!
//! Commit and publish workflows request catalog work from their connector. JSON writes carry a
//! static generator whose kernel requests are driven by the connector performing the write.
//! Kernel and catalog requests remain separate vocabularies: serving a catalog `WriteJson` request
//! may temporarily drive its kernel generator, but neither workflow invokes connector code.

use delta_kernel::committer::{
    Commit as KernelCommit, CommitActions, CommitResponse, PublishMetadata,
};
use delta_kernel::coroutine::workflow::{static_workflow, StaticWorkflow, WorkflowStep};
use delta_kernel::coroutine::write::{CopyAtomic, FileWriteMode, WriteJsonFile};
use delta_kernel::coroutine::{ChannelExchange, DeltaFuture, Reply};
use delta_kernel::{DeltaResult, Error, FileMeta};
use derive_more::{Constructor, From};
use unity_catalog_delta_client_api::{TableIdentifier, UpdateTableClient, UpdateTableRequest};
use url::Url;

use crate::UCCommitter;

/// Catalog operations a Unity Catalog workflow may request from its driver.
#[derive(From)]
pub enum Request {
    /// Write one newline-delimited JSON file from streamed action batches.
    // Boxed to keep the request enum small without boxing every variant.
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

/// Catalog request channel used by Unity Catalog workflows.
pub type Channel = delta_kernel::coroutine::Channel<Request>;

/// Typed Unity Catalog operations available on [`Channel`].
pub trait ChannelExt: ChannelExchange<Request> {
    /// Write generated row batches to one newline-delimited JSON file.
    fn write_json_file(&self, operation: WriteJsonFile) -> impl DeltaFuture<FileMeta> {
        self.exchange(Box::new(operation), Request::WriteJson)
    }

    /// Atomically copy `source` to a new `destination`.
    fn copy_atomic(&self, source: Url, destination: Url) -> impl DeltaFuture<()> {
        let outbound = CopyAtomic::new(source, destination);
        self.exchange(outbound, Request::CopyAtomic)
    }

    /// Apply one atomic table update through Unity Catalog.
    fn update_table(
        &self,
        target: TableIdentifier,
        request: UpdateTableRequest,
    ) -> impl DeltaFuture<()> {
        let outbound = UpdateTable::new(target, request);
        self.exchange(outbound, Request::UpdateTable)
    }
}

impl ChannelExt for Channel {}

/// Unity Catalog commit workflow.
pub type CommitWorkflow = StaticWorkflow<Request, CommitResponse>;

/// Completion or catalog work from advancing a [`CommitWorkflow`].
pub type CommitStep = WorkflowStep<Request, CommitResponse>;

/// Unity Catalog publish workflow.
pub type PublishWorkflow = StaticWorkflow<Request, ()>;

/// Completion or catalog work from advancing a [`PublishWorkflow`].
pub type PublishStep = WorkflowStep<Request, ()>;

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Return a lazy connector-driven Unity Catalog commit workflow.
    ///
    /// Version zero is written directly to the published log. Later versions are written to a
    /// staged path and ratified with `update_table`. Advancing the workflow rejects commits that
    /// are not catalog-managed or whose protocol and metadata do not identify this UC table.
    pub fn commit_workflow(&self, commit: KernelCommit) -> CommitWorkflow
    where
        C: 'static,
    {
        let committer = self.clone();
        static_workflow!(Request, async move |channel| {
            commit.metadata.validate_committer(true)?;
            match commit.metadata.version() {
                0 => {
                    committer
                        .commit_version_0(channel, commit.actions, &commit.metadata)
                        .await
                }
                _ => {
                    committer
                        .commit_version_non_zero(channel, commit.actions, commit.metadata)
                        .await
                }
            }
        })
    }

    /// Return a lazy workflow that publishes ratified commits to the Delta log.
    ///
    /// Copies are requested in ascending version order. Existing destination files are treated as
    /// already published.
    pub fn publish_workflow(&self, metadata: PublishMetadata) -> PublishWorkflow {
        static_workflow!(Request, async move |channel| {
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

pub(crate) async fn write_commit_file(
    channel: &Channel,
    actions: CommitActions,
    url: Url,
) -> DeltaResult<FileMeta> {
    let operation = WriteJsonFile::new(url, FileWriteMode::CreateNew, actions);
    channel.write_json_file(operation).await
}
