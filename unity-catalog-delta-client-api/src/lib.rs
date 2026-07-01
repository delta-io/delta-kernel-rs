//! Unity Catalog client API traits and wire models for the UC API
//! surface.
//!
//! This crate defines the transport-agnostic [`UpdateTableClient`] trait that
//! kernel-uc's `UCCommitter` dispatches through, plus serde-friendly wire
//! models for the connector-driven endpoints (`load_table`, credentials,
//! `/config`). Concrete HTTP implementations live in
//! `unity-catalog-delta-rest-client`.
//!
//! Only `update_table` (the commit RPC) is behind a trait. Read and
//! credential-vending flows are connector-driven: connectors call concrete
//! REST methods (or bring their own HTTP plumbing) and hand the responses to
//! kernel-uc helpers.
//!
//! # Testing
//!
//! Enable the `test-utils` feature for an in-memory `UpdateTableClient`
//! implementation suitable for unit tests:
//!
//! ```toml
//! [dev-dependencies]
//! unity-catalog-delta-client-api = { version = "...", features = ["test-utils"] }
//! ```

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
    CatalogConfig, Commit, CreateStagingTableRequest, CreateStagingTableResponse,
    CreateTableRequest, DeltaTableRequirement, DeltaTableUpdate, LoadTableResponse, Protocol,
    TableMetadata, TableName, UpdateTableRequest,
};
