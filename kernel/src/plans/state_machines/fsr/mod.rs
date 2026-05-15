//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM) used by
//! [`Snapshot::full_state`](crate::snapshot::Snapshot::full_state).

mod checkpoint_shape;
pub mod full_state;
mod plans;
mod schemas;

pub use checkpoint_shape::{
    checkpoint_shape_from_last_checkpoint, checkpoint_shape_from_schema, first_checkpoint_url,
    snapshot_has_checkpoint_files, CheckpointShape,
};
pub use full_state::{full_state_sm, FullState, FullStateBuilder};
pub use plans::{build_fsr_plans, CommitFileMeta};
