//! REST client implementation for Unity Catalog Delta APIs.
//!
//! This crate provides HTTP-based implementations of the traits defined in
//! [`unity_catalog_delta_client_api`].
//!
//! # Example
//!
//! ```no_run
//! use unity_catalog_delta_rest_client::{ClientConfig, UCDeltaTableClient};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::build("uc.awesome.org", "your-token")
//!         .with_additional_user_agent([("MyEngine", "1.0.0"), ("MyConnector", "1.0.0")])
//!         .build()?;
//!     let client = UCDeltaTableClient::new(config)?;
//!
//!     let resp = client.load_table("main", "default", "my_table").await?;
//!     println!("table id: {}", resp.metadata.table_uuid);
//!     Ok(())
//! }
//! ```

pub mod clients;
pub mod config;
pub mod error;
pub(crate) mod http;

#[cfg(test)]
mod tests;

pub use clients::{UCDeltaTableClient, UCUpdateTableRestClient};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};
pub use unity_catalog_delta_client_api as api;
pub use unity_catalog_delta_client_api::credentials::*;
pub use unity_catalog_delta_client_api::models::*;
pub use unity_catalog_delta_client_api::UpdateTableClient;
