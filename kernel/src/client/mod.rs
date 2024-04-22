//! Provides clients that implement the required interfaces that are optionally built into the
//! kernel

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
