//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`CoroutineSM`] consumed by [`Snapshot::full_state_builder`].
//!
//! [`CoroutineSM`]: crate::plans::state_machines::framework::coroutine::CoroutineSM
//! [`Snapshot::full_state_builder`]: crate::snapshot::Snapshot::full_state_builder

mod file_scan;
pub mod full_state;
mod reconciliation;
mod scan_plan;
mod shape;

pub use full_state::{FullState, FullStateBuilder};
