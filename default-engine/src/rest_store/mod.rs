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
//! - [`RestEndpointConfig`] -- the REST dialect, described as data: path-to-URL mapping, list and
//!   write query parameters, list-response field names, and HTTP-status-to-[`ObjectStoreError`]
//!   mapping.
//!
//! Only the operations kernel needs are implemented (read, list, write, delete); the rest
//! return [`ObjectStoreError::NotSupported`].

use delta_kernel::object_store::Error as ObjectStoreError;

mod auth;
mod client;
mod contract;
mod store;
#[cfg(test)]
mod tests;

pub use auth::{
    headers_from_pairs, AuthHeaderProvider, RefreshingHeaderProvider, StaticHeaderProvider,
};
pub use client::{build_rest_client, RestClientOptions};
pub use contract::RestEndpointConfig;
pub use store::RestObjectStore;

/// Build a generic [`ObjectStoreError`] from a source error.
pub(crate) fn generic_err<E: std::error::Error + Send + Sync + 'static>(
    err: E,
) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "RestObjectStore",
        source: Box::new(err),
    }
}

/// Build a generic [`ObjectStoreError`] from a message.
pub(crate) fn generic_msg(
    msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "RestObjectStore",
        source: msg.into(),
    }
}
