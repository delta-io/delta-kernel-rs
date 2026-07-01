//! REST client implementation for Unity Catalog Delta APIs.
//!
//! This crate provides HTTP-based implementations of the traits defined in
//! [`unity_catalog_delta_client_api`].
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

#[cfg(test)]
mod tests;

pub use clients::{UCClient, UCUpdateTableRestClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};
