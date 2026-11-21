//! Observations from declarative scan-plan generation.

use std::sync::Arc;

use crate::Version;
use crate::checkpoint::{CheckpointInfo, SelectedCheckpointType};

/// Checkpoint selection and work observed during one planning attempt.
#[derive(Debug, Clone)]
pub struct CheckpointMetrics {
    /// Selected checkpoint version, retained on failure.
    pub version: Version,
    /// Checkpoint type known before discovery, retained on failure.
    pub checkpoint_type: Option<SelectedCheckpointType>,
    /// Shared resolved information, absent if discovery failed.
    pub info: Option<Arc<CheckpointInfo>>,
    /// Whether discovery reused the snapshot cache.
    pub cache_hit: Option<bool>,
    /// Root-footer engine invocations in this attempt.
    pub root_footer_calls: u64,
    /// Sidecar-footer engine invocations in this attempt.
    pub sidecar_footer_calls: u64,
    /// Checkpoint-metadata query engine invocations in this attempt.
    pub metadata_query_calls: u64,
}
