use crate::error::Result;
use crate::models::{CommitRequest, CommitsRequest, CommitsResponse};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryCommitsClient, TableData};

/// Trait for committing new versions to a UC-managed Delta table.
///
/// Implementations of this trait are responsible for performing any necessary
/// retries on transient failures. This trait is designed to be injected into a
/// `UCCommitter` (in `delta-kernel-unity-catalog`), which itself does not
/// perform any retries and relies on the underlying client implementation to
/// handle retry logic.
#[allow(async_fn_in_trait)]
pub trait CommitClient: Send + Sync {
    /// Commit a new version to the table.
    async fn commit(&self, request: CommitRequest) -> Result<()>;
}

/// Trait for retrieving commits from a UC-managed Delta table.
///
/// Implementations of this trait are responsible for performing any necessary
/// retries on transient failures.
#[allow(async_fn_in_trait)]
pub trait GetCommitsClient: Send + Sync {
    /// Get the latest commits for the table.
    async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse>;
}
