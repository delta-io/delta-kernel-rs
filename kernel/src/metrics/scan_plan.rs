//! Observations from declarative scan-plan generation.

use std::fmt;
use std::sync::Arc;

use super::MetricId;
use crate::Version;
use crate::checkpoint::CheckpointInfo;
use crate::scan::{PartitionValuesOptions, StatsOptions};

/// Result of one scan-plan-generation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPlanOutcome {
    /// A metadata plan was produced.
    Plan,
    /// No metadata plan was needed.
    Empty,
    /// Planning failed.
    Error,
}

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
    /// Combined size of the selected JSON inputs, including checkpoint roots.
    pub total_json_bytes: u64,
    /// Combined size of the selected Parquet inputs.
    pub total_parquet_bytes: u64,
}

/// Metrics collected during declarative scan-plan generation, including partial state on error.
#[derive(Debug, Clone)]
pub struct ScanPlanReport {
    /// Identity of this planning attempt.
    pub operation_id: MetricId,
    /// Caller-supplied correlation identity.
    pub correlation_id: Option<Arc<str>>,
    /// Terminal planning result.
    pub outcome: ScanPlanOutcome,
    /// Wall-clock planning duration.
    pub duration_ns: u64,
    /// Nodes in the returned plan, or zero when no plan is returned.
    pub num_plan_nodes: u64,
    /// Checkpoint used for replay, absent for CRC and commit-only replay.
    pub checkpoint: Option<CheckpointMetrics>,
    /// Metadata planning metrics.
    pub metadata_planning: MetadataPlanningReport,
}

impl ScanPlanReport {
    pub(crate) const SPAN_NAME: &str = "scan.plan_generation_completed";

    #[cfg(feature = "declarative-plans")]
    pub(crate) fn new() -> Self {
        Self {
            operation_id: MetricId::new(),
            correlation_id: None,
            outcome: ScanPlanOutcome::Error,
            duration_ns: 0,
            num_plan_nodes: 0,
            checkpoint: None,
            metadata_planning: MetadataPlanningReport::default(),
        }
    }
}

impl fmt::Display for ScanPlanReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScanPlanReport(operation_id={}, outcome={:?}, duration_ns={}, num_plan_nodes={}, \
             total_json_bytes={}, total_parquet_bytes={}, \
             replay_base={:?}, base_version={:?}, has_filter={}, read_collations={}, \
             commit_start_version={:?}, commit_end_version={:?}, \
             commit_total_size_bytes={}, compaction_file_count={}",
            self.operation_id,
            self.outcome,
            self.duration_ns,
            self.num_plan_nodes,
            self.metadata_planning.total_json_bytes,
            self.metadata_planning.total_parquet_bytes,
            self.metadata_planning.replay_base,
            self.metadata_planning.base_version,
            self.metadata_planning.has_filter,
            self.metadata_planning.read_collations,
            self.metadata_planning.commit_start_version,
            self.metadata_planning.commit_end_version,
            self.metadata_planning.commit_total_size_bytes,
            self.metadata_planning.compaction_file_count
        )?;
        if let Some(checkpoint) = &self.checkpoint {
            write!(
                f,
                ", checkpoint_version={}, source={:?}, root_footer_calls={}, \
                 sidecar_footer_calls={}, metadata_query_calls={}, checkpoint_duration_ns={}",
                checkpoint.version,
                checkpoint.source,
                checkpoint.root_footer_calls,
                checkpoint.sidecar_footer_calls,
                checkpoint.metadata_query_calls,
                checkpoint.duration_ns
            )?;
            if let Some(info) = &checkpoint.info {
                write!(
                    f,
                    ", checkpoint_type={:?}, leaf_files={}, checkpoint_bytes={}",
                    info.checkpoint_type(),
                    info.leaf_file_count(),
                    info.size_in_bytes()
                )?;
            }
        }
        f.write_str(")")
    }
}
