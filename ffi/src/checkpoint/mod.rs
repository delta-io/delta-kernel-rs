//! FFI checkpoint types and metadata resolved through the engine's declarative executor.

mod info;
mod types;

#[cfg(all(test, feature = "declarative-plans", feature = "tracing"))]
pub(crate) use info::tests::checkpoint_handles;
#[cfg(feature = "tracing")]
pub(crate) use info::visit;
pub use info::{CheckpointInfoVisitor, FfiCheckpointInfo, FfiCheckpointKind};
#[cfg(feature = "declarative-plans")]
pub use info::{snapshot_checkpoint_info, snapshot_checkpoint_type};
pub use types::FfiSelectedCheckpointType;
