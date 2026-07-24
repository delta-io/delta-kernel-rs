use crate::error::Result;
use crate::models::{CommitRequest, CommitsRequest, CommitsResponse, UpdateTableRequest};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryCommitsClient, TableData};

/// Trait for committing new versions to a UC-managed Delta table via the
/// `update_table` endpoint.
///
/// Implementations are responsible for any retry logic on transient failures.
#[allow(async_fn_in_trait)]
pub trait UpdateTableClient: Send + Sync {
    /// Apply the typed `requirements + updates` payload atomically against the catalog.
    async fn update_table(&self, request: UpdateTableRequest) -> Result<()>;
}

// The `CommitClient` and `GetCommitsClient` traits below back the legacy Delta-Commits path and
// are superseded by `UpdateTableClient` (above). They will be deleted once the read and commit
// paths are swapped onto the new Delta-Tables API.

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
