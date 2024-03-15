//! module for clients that are optionally built into the kernel

#[cfg(feature = "default-client")]
pub mod default;
#[cfg(feature = "default-client")]
pub use default::*;

#[cfg(feature = "sync-client")]
pub mod sync;
