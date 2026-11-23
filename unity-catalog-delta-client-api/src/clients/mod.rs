use crate::error::{Error, Result};
use crate::models::{Commit, TableIdentifier, UpdateTableRequest};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryUpdateTableClient, TableData};

/// A catalog's observed ratified version and the commits it still retains.
///
/// Published commits may be absent from `commits`. Readers must combine this tail with published
/// filesystem commits through `latest_version`; staged files outside this response are not winners.
#[derive(Debug, Clone)]
pub struct CommitState {
    /// Stable identity of the table returned by the catalog.
    pub table_uuid: String,
    /// Highest version ratified at the time of this read, including data-only commits.
    pub latest_version: i64,
    /// Catalog-retained ratified commits, in any order.
    pub commits: Vec<Commit>,
}

/// Trait for committing new versions to a UC-managed Delta table via the
/// `update_table` endpoint.
///
/// The transaction layer owns retries after confirmed rejections.
/// Do not resubmit an update whose acceptance is uncertain. Return uncertain outcomes as errors
/// so callers can resolve them before resubmission.
#[allow(async_fn_in_trait)]
pub trait UpdateTableClient: Send + Sync {
    /// Apply the typed `requirements + updates` payload atomically against `target`.
    async fn update_table(
        &self,
        target: &TableIdentifier,
        request: UpdateTableRequest,
    ) -> Result<()>;

    /// Reads ratified commit state for `target` after a confirmed version conflict.
    ///
    /// Catalog clients resolve `target` by name. `expected_table_uuid` is the identity the caller
    /// opened. Return the actual identity in [`CommitState::table_uuid`]; `UCCommitter` compares it
    /// with the expected UUID and rejects a replaced table before using history.
    /// This method must not infer ratification by listing staged files.
    ///
    /// Returns an unsupported error when catalog reads are unavailable.
    async fn get_commits(
        &self,
        _target: &TableIdentifier,
        _expected_table_uuid: &str,
    ) -> Result<CommitState> {
        Err(Error::UnsupportedOperation(
            "This client does not support commit-history reads".to_string(),
        ))
    }
}
