//! Two-phase log replay for parallel execution of checkpoint processing.

use crate::internal_api;

#[cfg(feature = "internal-api")]
pub mod parallel_phase;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod parallel_phase;

#[cfg(feature = "internal-api")]
pub mod sequential_phase;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod sequential_phase;

#[cfg(feature = "internal-api")]
pub use parallel_phase::ParallelPhase;
#[cfg(not(feature = "internal-api"))]
pub(crate) use parallel_phase::ParallelPhase;

#[cfg(feature = "internal-api")]
pub use sequential_phase::{AfterSequential, SequentialPhase};
#[cfg(not(feature = "internal-api"))]
pub(crate) use sequential_phase::{AfterSequential, SequentialPhase};
