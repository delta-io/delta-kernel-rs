//! module for clients that are optionally built into the kernel

#[cfg(any(feature = "default-client", feature = "sync-client"))]
pub mod arrow_data;

#[cfg(feature = "default-client")]
pub mod default;
#[cfg(feature = "default-client")]
pub use default::*;

#[cfg(feature = "sync-client")]
pub mod sync;
