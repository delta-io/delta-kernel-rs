//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM)
//! consumed by [`crate::snapshot::Snapshot::full_state_builder`].

mod file_scan;
pub mod full_state;
mod ssa_reconciliation;
mod ssa_scan;

pub use full_state::{FullState, FullStateBuilder};
pub use ssa_reconciliation::CommitFileMeta;
