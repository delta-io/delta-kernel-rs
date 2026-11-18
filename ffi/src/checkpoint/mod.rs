//! FFI checkpoint types and metadata resolved through the engine's declarative executor.

#[cfg(feature = "declarative-plans")]
mod info;
mod types;

#[cfg(all(test, feature = "declarative-plans", feature = "tracing"))]
pub(crate) use info::tests::checkpoint_handles;
#[cfg(all(feature = "declarative-plans", feature = "tracing"))]
pub(crate) use info::visit;
#[cfg(feature = "declarative-plans")]
pub use info::{
    CheckpointInfoVisitor, FfiCheckpointInfo, FfiCheckpointKind, snapshot_checkpoint_info,
    snapshot_checkpoint_type,
};
pub use types::FfiSelectedCheckpointType;
