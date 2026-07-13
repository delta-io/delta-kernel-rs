//! A generic REST/HTTP-backed [`ObjectStore`](delta_kernel::object_store::ObjectStore).
//!
//! [`RestObjectStore`] implements [`ObjectStore`](delta_kernel::object_store::ObjectStore) over a
//! REST "file API" -- a service that exposes list/read/write of files behind authenticated HTTP
//! endpoints rather than as a plain blob store -- so the default engine can read and write Delta
//! tables through it with no service-specific logic in the kernel.
//!
//! The caller supplies everything service-specific through two pieces:
//!
//! - [`AuthHeaderProvider`] -- headers attached to every request (auth, identity). Consulted per
//!   request, so an implementation can refresh short-lived credentials.
//! - [`RestEndpointConfig`] -- the REST dialect: path-to-URL mapping, list and write query
//!   parameters, and list-response JSON field names. HTTP `404 -> NotFound` and `409 ->
//!   AlreadyExists` are mapped in [`RestObjectStore`].
//!
//! Only the operations kernel needs are implemented (read, head, list, write, delete); the rest
//! return [`ObjectStoreError::NotSupported`].

use delta_kernel::object_store::Error as ObjectStoreError;

mod auth;
mod client;
mod config;
mod response;
mod store;
#[cfg(test)]
mod tests;

pub use auth::{
    headers_from_pairs, AuthHeaderProvider, RefreshingHeaderProvider, StaticHeaderProvider,
};
pub use client::{build_rest_client, RestClientOptions};
pub use config::RestEndpointConfig;
// Re-exported so callers can name the header type in `AuthHeaderProvider`'s public API.
pub use reqwest::header::HeaderMap;
pub use store::RestObjectStore;

/// Build a generic [`ObjectStoreError`] from a source error or message.
pub(crate) fn generic_error(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "RestObjectStore",
        source: source.into(),
    }
}
