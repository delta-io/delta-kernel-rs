use crate::error::Result;
use crate::models::{
    CreateIdentitySequences, DropIdentitySequences, DropIdentitySequencesResponse,
    ReserveIdentityRanges, ReserveIdentityRangesResponse, TableIdentifier, UpdateTableRequest,
};

#[cfg(any(test, feature = "test-utils"))]
mod in_memory;

#[cfg(any(test, feature = "test-utils"))]
pub use in_memory::{InMemorySequenceClient, InMemoryUpdateTableClient, TableData};

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

/// Trait for interacting with the UC Identity Sequence Service.
///
/// Mirrors the service's batch RPCs for Concurrent Identity Columns:
/// - `create_identity_sequences`: create (or idempotently get) sequences (CREATE TABLE time)
/// - `reserve_identity_ranges`: reserve ranges of identity values (INSERT time)
/// - `drop_identity_sequences`: remove sequences (DROP/REPLACE TABLE time)
#[allow(async_fn_in_trait)]
pub trait SequenceClient: Send + Sync {
    /// Create (or idempotently get) one or more identity sequences under a table. Sequence ids
    /// are minted by the caller. The batch is atomic: if any entry is rejected, no sequence is
    /// created.
    async fn create_identity_sequences(&self, req: CreateIdentitySequences) -> Result<()>;

    /// Reserve a contiguous range of values from one or more sequences. The batch is atomic.
    /// Returned ranges are positional within the batch.
    async fn reserve_identity_ranges(
        &self,
        req: ReserveIdentityRanges,
    ) -> Result<ReserveIdentityRangesResponse>;

    /// Idempotently drop one or more identity sequences under a table. Returns one result per
    /// unique requested sequence id, reporting whether each existed.
    async fn drop_identity_sequences(
        &self,
        req: DropIdentitySequences,
    ) -> Result<DropIdentitySequencesResponse>;
}
