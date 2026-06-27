//! REST client implementations for the Unity Catalog Delta APIs.
//!
//! Two REST structs:
//!
//! - [`UCClient`] holds concrete (non-trait) HTTP methods for the connector-driven endpoints
//!   (`load_table`, credentials, `/config`).
//! - [`UCUpdateTableRestClient`] implements the [`UpdateTableClient`] trait against `update_table`
//!   so kernel-uc's `UCCommitter` can dispatch through it.
//!
//! [`UpdateTableClient`]: unity_catalog_delta_client_api::UpdateTableClient
//!
//! # Example
//!
//! ```no_run
//! use unity_catalog_delta_rest_client::{ClientConfig, UCClient};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::build("uc.awesome.org", "your-token").build()?;
//!     let client = UCClient::new(config)?;
//!
//!     let resp = client.load_table("main", "default", "my_table").await?;
//!     println!("table id: {}", resp.metadata.table_uuid);
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

pub use clients::{UCClient, UCUpdateTableRestClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};
