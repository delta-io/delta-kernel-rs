use crate::error::Result;
use crate::models::{TableName, UpdateTableRequest};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryUpdateTableClient, TableData};

/// Trait for committing new versions to a UC-managed Delta table via the
/// `update_table` endpoint.
///
/// Implementations are responsible for any retry logic on transient failures.
#[allow(async_fn_in_trait)]
pub trait UpdateTableClient: Send + Sync {
    /// Apply the typed `requirements + updates` payload atomically against `target`.
    async fn update_table(&self, target: &TableName, request: UpdateTableRequest) -> Result<()>;
}
