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
/// Implementations are responsible for any retry logic on transient failures.
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
    /// `table_uuid` identifies the table the caller originally opened. The returned identity must
    /// be checked before using history, since the named table may have been replaced. This method
    /// must not infer ratification by listing staged files.
    ///
    /// Returns an unsupported error when catalog reads are unavailable.
    async fn get_commits(
        &self,
        _target: &TableIdentifier,
        _table_uuid: &str,
    ) -> Result<CommitState> {
        Err(Error::UnsupportedOperation(
            "This client does not support commit-history reads".to_string(),
        ))
    }
}
