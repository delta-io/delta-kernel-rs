//! Unity Catalog Client for Rust
//!
//! This crate provides a Rust client for interacting with Unity Catalog APIs.
//!
//! # Example
//!
//! ```no_run
//! use uc_client::{ClientConfig, UCRestCommitsClient, UCCommitsClient, models::CommitsRequest};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::build("uc.awesome.org", "your-token").build()?;
//!     let client = UCRestCommitsClient::new(config)?;
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
pub use commits_client::{UCCommitsClient, UCRestCommitsClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};

#[doc(hidden)]
pub mod prelude {
    pub use crate::client::UCClient;
    pub use crate::commits_client::{UCCommitsClient, UCRestCommitsClient};
    pub use crate::models::{
        commits::{Commit, CommitsRequest, CommitsResponse},
        credentials::{Operation, TemporaryTableCredentials},
        tables::TablesResponse,
    };
}
