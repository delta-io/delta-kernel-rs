use crate::error::Result;
use crate::models::{UpdateTableRequest, UpdateTableResponse};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryUpdateTableClient, TableData};

/// Trait for committing new versions to a UC-managed Delta table via the
/// `update_table` endpoint.
///
/// Under the UC API surface this is the only operation that
/// kernel-uc dispatches via trait. All other operations (`load_table`,
/// credentials, `/config`) are connector-driven against a concrete REST
/// client (or the connector's own HTTP plumbing).
///
/// Implementations are responsible for any retry logic on transient
/// failures; `UCCommitter` itself does not retry.
#[allow(async_fn_in_trait)]
pub trait UpdateTableClient: Send + Sync {
    /// Apply the typed `requirements + updates` payload atomically against
    /// the catalog and return the server's response.
    async fn update_table(&self, request: UpdateTableRequest) -> Result<UpdateTableResponse>;
}
