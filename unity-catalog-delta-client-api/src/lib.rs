//! Unity Catalog client API traits and wire models for the UC API
//! surface.
//!
//! This crate defines the transport-agnostic [`UpdateTableClient`] trait that
//! `delta-kernel-unity-catalog`'s `UCCommitter` dispatches through, plus
//! serde-friendly wire models for the connector-driven endpoints
//! (`load_table`, credentials, `/config`). Concrete HTTP implementations live
//! in `unity-catalog-delta-rest-client`.
//!
//! The commit client also exposes optional ratified-history reads for conflict recovery. Snapshot
//! loading and credential vending remain connector-driven through concrete REST methods or
//! connector-provided transports.

pub mod clients;
pub mod credentials;
pub mod error;
pub mod models;

pub use clients::{CommitState, UpdateTableClient};
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
