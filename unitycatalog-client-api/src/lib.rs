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
//! unitycatalog-client-api = { version = "...", features = ["test-utils"] }
//! ```

pub mod commits_client;
pub mod credentials;
pub mod error;
pub mod models;

pub use commits_client::{CommitClient, GetCommitsClient};
pub use credentials::{AwsTempCredentials, Operation, TemporaryTableCredentials};
pub use error::{Error, Result};
pub use models::{Commit, CommitRequest, CommitsRequest, CommitsResponse};

#[cfg(any(test, feature = "test-utils"))]
pub use commits_client::{InMemoryCommitsClient, TableData};
