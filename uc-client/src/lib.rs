//! Unity Catalog Client for Rust
//!
//! This crate provides a Rust client for interacting with Unity Catalog APIs.
//!
//! # Example
//!
//! ```no_run
//! use uc_client::{UCClient, models::CommitsRequest};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = UCClient::builder("uc.awesome.org", "your-token")
//!         .build()?;
//!
//!     let request = CommitsRequest::new("table-id", "table-uri");
//!     let commits = client.get_commits(request).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod models;

#[cfg(test)]
mod tests;

pub use client::{UCClient, UCClientBuilder};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};

#[doc(hidden)]
pub mod prelude {
    pub use crate::client::UCClient;
    pub use crate::models::{
        commits::{Commit, CommitsRequest, CommitsResponse},
        credentials::{Operation, TemporaryTableCredentials},
        tables::TablesResponse,
    };
}
