//! FFI checkpoint types and metadata resolved through the engine's declarative executor.

mod info;
mod metrics;
mod types;

pub use info::{CheckpointInfoVisitor, FfiCheckpointInfo, FfiCheckpointKind};
#[cfg(feature = "declarative-plans")]
pub use info::{snapshot_checkpoint_info, snapshot_checkpoint_type};
pub use metrics::{FfiCheckpointMetrics, FfiCheckpointSource};
pub use types::FfiSelectedCheckpointType;
