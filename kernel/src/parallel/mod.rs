//! Two-phase log replay for parallel execution of checkpoint processing.

pub mod parallel_phase;
pub mod sequential_phase;

pub use parallel_phase::ParallelPhase;
pub use sequential_phase::{AfterSequential, SequentialPhase};
