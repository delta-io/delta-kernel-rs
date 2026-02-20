//! Unity Catalog Client for Rust
//!
//! This crate provides a Rust client for interacting with Unity Catalog APIs,
//! including table metadata resolution and temporary credential management.
//!
//! # Example
//!
//! ```no_run
//! use unity_catalog_client::{ClientConfig, UCClient};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::build("uc.awesome.org", "your-token").build()?;
//!     let client = UCClient::new(config)?;
//!
//!     let table = client.get_table("catalog.schema.table").await?;
//!     println!("Table ID: {}", table.table_id);
//!
//!     Ok(())
//! }
//! ```

pub use delta_kernel_unity_catalog::config;
pub use delta_kernel_unity_catalog::error;
pub use delta_kernel_unity_catalog::http;

pub mod client;
pub mod models;

#[cfg(test)]
mod tests;

pub use client::UCClient;
pub use config::{ClientConfig, ClientConfigBuilder};

pub mod prelude {
    pub use crate::client::UCClient;
    pub use crate::config::{ClientConfig, ClientConfigBuilder};
    pub use crate::models::credentials::{Operation, TemporaryTableCredentials};
    pub use crate::models::tables::TablesResponse;
    pub use delta_kernel_unity_catalog::{
        Commit, CommitRequest, CommitsRequest, CommitsResponse, UCCommitClient,
        UCCommitsRestClient, UCGetCommitsClient,
    };
}
