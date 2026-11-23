//! Observations from declarative scan-plan generation.

use std::sync::Arc;

use crate::Version;
use crate::checkpoint::{CheckpointInfo, SelectedCheckpointType};

/// Checkpoint discovery path reached by an attempt, independently of success or failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointSource {
    /// Completed checkpoint information was reused from the snapshot cache.
    SnapshotCache,
    /// Applicable `_last_checkpoint` metadata was used without checkpoint I/O.
    LastCheckpoint,
    /// At least one checkpoint footer or metadata-query invocation was attempted.
    /// Applicable hint metadata may also have been used.
    Resolved,
}

/// Checkpoint selection and work observed during one planning attempt.
#[derive(Debug, Clone, Default)]
pub struct CheckpointMetrics {
    /// Selected checkpoint version, retained on failure.
    pub version: Version,
    /// Checkpoint type known before discovery, retained on failure.
    pub checkpoint_type: Option<SelectedCheckpointType>,
    /// Shared resolved information, absent if discovery failed.
    pub info: Option<Arc<CheckpointInfo>>,
    /// Discovery path reached by this attempt, absent until a source is reached.
    pub source: Option<CheckpointSource>,
    /// Root-footer engine invocations in this attempt.
    pub root_footer_calls: u64,
    /// Sidecar-footer engine invocations in this attempt.
    pub sidecar_footer_calls: u64,
    /// Checkpoint-metadata query engine invocations in this attempt.
    pub metadata_query_calls: u64,
    /// Elapsed lookup time in nanoseconds, including waiting for shared resolution.
    pub duration_ns: u64,
}
