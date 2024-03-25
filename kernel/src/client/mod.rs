//! module for clients that are optionally built into the kernel

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(feature = "arrow-expression")]
pub mod arrow_expression;

#[cfg(any(feature = "default-client", feature = "sync-client"))]
pub mod arrow_data;

#[cfg(feature = "default-client")]
pub mod default;

#[cfg(feature = "sync-client")]
pub mod sync;
