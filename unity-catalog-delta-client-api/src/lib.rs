//! Unity Catalog client API traits and wire models for the UC API
//! surface.
//!
//! This crate defines the transport-agnostic [`UpdateTableClient`] trait that
//! `delta-kernel-unity-catalog`'s `UCCommitter` dispatches through, plus
//! serde-friendly wire models for the connector-driven endpoints
//! (`load_table`, credentials, `/config`). Concrete HTTP implementations live
//! in `unity-catalog-delta-rest-client`.
//!
//! Only `update_table` (the commit RPC) is behind a trait. Read and
//! credential-vending flows are connector-driven: connectors call concrete
//! REST methods (or bring their own HTTP plumbing) and hand the responses to
//! `delta-kernel-unity-catalog` helpers.

pub mod clients;
pub mod credentials;
pub mod error;
pub mod models;

pub use clients::UpdateTableClient;
#[cfg(any(test, feature = "test-utils"))]
pub use clients::{InMemoryUpdateTableClient, TableData};
pub use credentials::{CredentialsResponse, Operation, StorageCredential};
pub use error::{Error, Result};
pub use models::{
    CatalogConfig, Commit, CommitReport, CreateStagingTableRequest, CreateStagingTableResponse,
    CreateTableRequest, DeltaTableRequirement, DeltaTableUpdate, FileSizeHistogram,
    LoadTableResponse, MetricsReport, Protocol, ReportMetricsRequest, TableIdentifier,
    TableMetadata, UpdateTableRequest,
};
