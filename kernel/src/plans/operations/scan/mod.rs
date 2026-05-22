//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`Coroutine`](crate::plans::operations::framework::coroutine::driver::Coroutine) used by
//! [`Snapshot::full_state`](crate::snapshot::Snapshot::full_state).

mod action_pair;
mod checkpoint_shape;
mod dedup;
mod file_scan;
pub mod full_state;
mod plans;
mod retention;
mod schemas;
mod ssa_reconciliation;

pub use full_state::{FullState, FullStateBuilder};
pub use plans::CommitFileMeta;
