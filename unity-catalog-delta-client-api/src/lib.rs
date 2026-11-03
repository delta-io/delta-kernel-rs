//! Unity Catalog client API traits and wire models for the UC API
//! surface.
//!
//! This crate defines the transport-agnostic [`UpdateTableClient`] trait that
//! `delta-kernel-unity-catalog`'s `UCCommitter` dispatches through, plus
//! serde-friendly wire models for the connector-driven endpoints
//! (`load_table`, credentials, `/config`). Concrete HTTP implementations live
//! in `unity-catalog-delta-rest-client`.
//!
//! `update_table` (the commit RPC) and the UC Identity Sequence Service
//! ([`SequenceClient`], for Concurrent Identity Columns) are behind traits. Read
//! and credential-vending flows are connector-driven: connectors call concrete
//! REST methods (or bring their own HTTP plumbing) and hand the responses to
//! `delta-kernel-unity-catalog` helpers.

pub mod clients;
pub mod credentials;
pub mod error;
pub mod models;

#[cfg(any(test, feature = "test-utils"))]
pub use clients::{InMemorySequenceClient, InMemoryUpdateTableClient, TableData};
pub use clients::{SequenceClient, UpdateTableClient};
pub use credentials::{CredentialsResponse, Operation, StorageCredential};
pub use error::{Error, Result};
pub use models::{
    CatalogConfig, Commit, CommitReport, CreateIdentitySequences, CreateStagingTableRequest,
    CreateStagingTableResponse, CreateTableRequest, DeltaTableRequirement, DeltaTableUpdate,
    DropIdentitySequenceResult, DropIdentitySequences, DropIdentitySequencesResponse,
    FileSizeHistogram, IdentityIdRange, IdentityReservation, IdentitySequenceSpec,
    LoadTableResponse, MetricsReport, Protocol, ReportMetricsRequest, ReserveIdentityRanges,
    ReserveIdentityRangesResponse, TableIdentifier, TableMetadata, UpdateTableRequest,
};
