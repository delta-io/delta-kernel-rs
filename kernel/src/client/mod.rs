//! Provides clients that implement the required interfaces. These clients can optionally be built
//! into the kernel by setting the `default-client` or `sync-client` feature flag. See the related
//! modules for more information.

#[cfg(feature = "arrow-conversion")]
pub(crate) mod arrow_conversion;

#[cfg(feature = "arrow-expression")]
pub mod arrow_expression;

#[cfg(any(feature = "default-client", feature = "sync-client"))]
pub mod arrow_data;

#[cfg(any(feature = "default-client", feature = "sync-client"))]
pub(crate) mod arrow_get_data;

#[cfg(any(feature = "default-client", feature = "sync-client"))]
pub(crate) mod arrow_utils;

#[cfg(feature = "default-client")]
pub mod default;

#[cfg(feature = "sync-client")]
pub mod sync;
