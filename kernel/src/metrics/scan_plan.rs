//! Observations from declarative scan-plan generation.

use std::sync::Arc;

use crate::Version;
use crate::checkpoint::CheckpointInfo;
use crate::scan::{PartitionValuesOptions, StatsOptions};

/// Starting state selected before replaying newer commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayBase {
    /// Complete live-file state from a CRC.
    Crc,
    /// A selected checkpoint.
    Checkpoint,
    /// Replay starts from commits alone.
    Empty,
}

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

/// Metrics collected during metadata planning.
#[derive(Debug, Clone, Default)]
pub struct MetadataPlanningReport {
    /// Requested statistics options.
    pub stats: StatsOptions,
    /// Requested partition-value options, including the timestamp timezone.
    pub partition_values: PartitionValuesOptions,
    /// Whether reading source collation statistics is enabled.
    pub read_collations: bool,
    /// Whether a statistics filter is configured.
    pub has_filter: bool,
    /// Starting state selected before replay, absent before source selection.
    pub replay_base: Option<ReplayBase>,
    /// Selected checkpoint or CRC version, absent before selection or for an empty base.
    pub base_version: Option<Version>,
    /// Inclusive first covered commit version, absent when no commits were selected.
    pub commit_start_version: Option<Version>,
    /// Inclusive last covered commit version, absent when no commits were selected.
    pub commit_end_version: Option<Version>,
    /// Combined size of the selected commit and compaction files.
    pub commit_total_size_bytes: u64,
    /// Selected compaction files, each counted once.
    pub compaction_file_count: u64,
}
