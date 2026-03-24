//! Unity Catalog Client for Rust
//!
//! This crate provides a Rust client for interacting with Unity Catalog APIs.
//!
//! # Example
//!
//! ```no_run
//! use unitycatalog_client_rest_impl::{ClientConfig, UCCommitsRestClient, GetCommitsClient, models::CommitsRequest};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::build("uc.awesome.org", "your-token").build()?;
//!     let client = UCCommitsRestClient::new(config)?;
//!
//!     let request = CommitsRequest::new("table-id", "table-uri");
//!     let commits = client.get_commits(request).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod commits_client;
pub mod config;
pub mod error;
pub mod http;
pub mod models;

#[cfg(test)]
mod tests;

pub use client::UCClient;
pub use commits_client::{CommitClient, GetCommitsClient, UCCommitsRestClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};

#[cfg(any(test, feature = "test-utils"))]
pub use commits_client::{InMemoryCommitsClient, TableData};

#[doc(hidden)]
pub mod prelude {
    pub use crate::client::UCClient;
    pub use crate::commits_client::{CommitClient, GetCommitsClient, UCCommitsRestClient};
    pub use crate::models::tables::TablesResponse;
    pub use crate::models::{Commit, CommitsRequest, CommitsResponse};
    pub use unitycatalog_client_api::{Operation, TemporaryTableCredentials};
}
