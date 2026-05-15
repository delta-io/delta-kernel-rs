//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM) used by
//! [`Snapshot::full_state`](crate::snapshot::Snapshot::full_state).

pub mod full_state;

pub use full_state::{
    build_fsr_plans, checkpoint_shape_from_last_checkpoint, checkpoint_shape_from_schema,
    first_checkpoint_url, full_state_sm, snapshot_has_checkpoint_files, CheckpointShape,
    CommitFileMeta, FullState, FullStateBuilder,
};
