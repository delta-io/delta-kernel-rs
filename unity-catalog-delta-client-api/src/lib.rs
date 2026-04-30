//! Unity Catalog client API traits and models.
//!
//! This crate defines transport-agnostic traits for interacting with Unity
//! Catalog. Concrete implementations (e.g., REST over HTTP) live in separate
//! crates that depend on this one.
//!
//! # Traits
//!
//! - [`CommitClient`] -- commit a new version to a UC-managed Delta table
//! - [`GetCommitsClient`] -- retrieve commits from a UC-managed Delta table
//!
//! # Testing
//!
//! Enable the `test-utils` feature for an in-memory implementation of both
//! traits suitable for unit tests:
//!
//! ```toml
//! [dev-dependencies]
//! unity-catalog-delta-client-api = { version = "...", features = ["test-utils"] }
//! ```

pub mod clients;
pub mod credentials;
pub mod error;
pub mod models;

pub use clients::{CommitClient, GetCommitsClient};
#[cfg(any(test, feature = "test-utils"))]
pub use clients::{InMemoryCommitsClient, TableData};
pub use credentials::{AwsTempCredentials, Operation, TemporaryTableCredentials};
pub use error::{Error, Result};
pub use models::{Commit, CommitRequest, CommitsRequest, CommitsResponse};
