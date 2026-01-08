//! Two-phase log replay for parallel execution of checkpoint processing.

pub(crate) mod parallel_phase;
pub(crate) mod sequential_phase;

pub(crate) use parallel_phase::ParallelPhase;
pub(crate) use sequential_phase::{AfterSequential, SequentialPhase};
