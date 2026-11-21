//! Observations from declarative scan-plan generation.

use std::sync::Arc;

use crate::Version;
use crate::checkpoint::{CheckpointInfo, SelectedCheckpointType};

/// Checkpoint selection and work observed during one planning attempt.
#[derive(Debug, Clone, Default)]
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

/// Selected commit inputs observed while building a metadata plan.
#[derive(Debug, Clone, Default)]
pub struct MetadataPlanReport {
    /// Inclusive first covered commit version, absent when no commits were selected.
    pub commit_start_version: Option<Version>,
    /// Inclusive last covered commit version, absent when no commits were selected.
    pub commit_end_version: Option<Version>,
    /// Combined size of the selected commit and compaction files.
    pub commit_total_size_bytes: u64,
    /// Selected compaction files, each counted once.
    pub compaction_file_count: u64,
}
