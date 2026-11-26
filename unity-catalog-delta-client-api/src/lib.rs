//! Unity Catalog client API traits and wire models for the UC API
//! surface.
//!
//! Connectors use [`LoadTableClient`] and [`GetCredentialsClient`] to load tables and configure
//! storage access independently of the catalog transport. `delta-kernel-unity-catalog`'s
//! `UCCommitter` dispatches commits through [`UpdateTableClient`]. Concrete HTTP implementations
//! live in `unity-catalog-delta-rest-client`.
//!
//! The crate also provides serde-friendly wire models for the UC Delta-Tables endpoints. Reads
//! and credential vending remain connector-driven: connectors pass the responses to
//! `delta-kernel-unity-catalog` helpers and their engine's storage configuration.
//!
//! # Example
//!
//! ```no_run
//! use unity_catalog_delta_client_api::{
//!     CredentialsResponse, GetCredentialsClient, LoadTableClient, LoadTableResponse, Operation,
//!     Result, TableIdentifier,
//! };
//!
//! async fn load_for_read(
//!     client: &(impl LoadTableClient + GetCredentialsClient),
//!     target: &TableIdentifier,
//! ) -> Result<(LoadTableResponse, CredentialsResponse)> {
//!     let table = client.load_table(target).await?;
//!     let credentials = client.get_table_credentials(target, Operation::Read).await?;
//!     Ok((table, credentials))
//! }
//! ```

pub mod clients;
pub mod credentials;
pub mod error;
pub mod models;

pub use clients::{GetCredentialsClient, LoadTableClient, UpdateTableClient};
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
