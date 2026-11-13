//! FFI checkpoint types and metadata resolved through the engine's declarative executor.

#[cfg(feature = "declarative-plans")]
mod info;
mod types;

#[cfg(feature = "declarative-plans")]
pub use info::{
    CheckpointInfoVisitor, FfiCheckpointInfo, FfiCheckpointKind, snapshot_checkpoint_info,
};
pub use types::FfiSelectedCheckpointType;
