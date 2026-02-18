//! Unity Catalog Client for Rust
//!
//! This crate provides a Rust client for interacting with Unity Catalog APIs,
//! including table metadata resolution and temporary credential management.

pub mod client;
pub mod config;
pub mod error;
pub mod http;
pub mod models;

#[cfg(test)]
mod tests;

pub use client::UCClient;
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, Result};
