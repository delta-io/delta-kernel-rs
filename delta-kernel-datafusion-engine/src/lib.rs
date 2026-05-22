//! DataFusion execution scaffold for kernel SSA plans. See [`DataFusionExecutor`] and
//! [`exec`] / [`compile`] modules.

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;
#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

pub use executor::DataFusionExecutor;
