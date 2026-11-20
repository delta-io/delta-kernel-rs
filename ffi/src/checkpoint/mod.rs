//! FFI checkpoint metadata resolved through the engine's declarative executor.

mod info;

pub use info::{
    CheckpointInfoVisitor, FfiCheckpointInfo, FfiCheckpointKind, snapshot_checkpoint_info,
};
