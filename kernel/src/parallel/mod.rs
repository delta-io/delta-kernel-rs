//! Two-phase log replay for parallel execution of checkpoint processing.

#[cfg(feature = "internal-api")]
pub mod parallel_phase;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod parallel_phase;

#[cfg(feature = "internal-api")]
pub mod sequential_phase;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod sequential_phase;

pub mod parallel_scan_metadata;
