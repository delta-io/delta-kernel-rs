//! REST client implementation for Unity Catalog Delta APIs.
//!
//! This crate provides HTTP-based implementations of the traits defined in
//! [`unity_catalog_delta_client_api`].
//!
//! # Example
//!
//! ```no_run
//! use unity_catalog_delta_client_api::{CommitsRequest, GetCommitsClient};
//! use unity_catalog_delta_rest_client::{ClientConfig, UCCommitsRestClient};
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

pub mod clients;
pub mod config;
pub mod error;
pub mod http;
pub mod models;

#[cfg(test)]
mod tests;

pub use clients::{UCClient, UCCommitsRestClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};
