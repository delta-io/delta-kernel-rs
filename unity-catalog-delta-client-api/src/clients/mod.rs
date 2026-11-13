use crate::credentials::{CredentialsResponse, Operation};
use crate::error::Result;
use crate::models::{LoadTableResponse, TableIdentifier, UpdateTableRequest};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemoryUpdateTableClient, TableData};

/// Loads a UC-managed Delta table's metadata and unpublished commits.
///
/// Connectors use the response to build a kernel snapshot. Implementations are responsible for
/// any retry logic on transient failures.
#[allow(async_fn_in_trait)]
pub trait LoadTableClient: Send + Sync {
    /// Fetch the current table state, including commits not yet published to storage.
    ///
    /// Returns [`crate::Error::TableNotFound`] if the table does not exist.
    async fn load_table(&self, target: &TableIdentifier) -> Result<LoadTableResponse>;
}

/// Vends temporary storage credentials for a UC-managed Delta table.
///
/// Connectors use the credentials to configure storage access for their engine. Implementations
/// are responsible for any retry logic on transient failures.
#[allow(async_fn_in_trait)]
pub trait GetCredentialsClient: Send + Sync {
    /// Fetch credentials scoped to the table's storage and the requested operation.
    ///
    /// Use [`Operation::Read`] for reads and [`Operation::ReadWrite`] for writes. Credentials can
    /// expire; callers must refresh them according to each credential's expiration time.
    async fn get_table_credentials(
        &self,
        target: &TableIdentifier,
        operation: Operation,
    ) -> Result<CredentialsResponse>;
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
}
