//! Observations from declarative scan-plan generation.

use std::fmt;
use std::sync::Arc;

use super::MetricId;
use crate::Version;
use crate::checkpoint::{CheckpointInfo, SelectedCheckpoint};
use crate::scan::{PartitionValuesOptions, StatsOptions};

#[cfg(test)]
mod tests;

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

/// Checkpoint selection and work observed during one planning attempt.
#[derive(Debug, Clone)]
pub struct CheckpointMetrics {
    /// Selection known before discovery, retained on failure.
    pub selected: SelectedCheckpoint,
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

impl CheckpointMetrics {
    pub(crate) fn update(update: impl FnOnce(&mut Self)) {
        ScanPlanGenerationCompleted::update(|event| {
            if let Some(checkpoint) = &mut event.checkpoint {
                update(checkpoint);
            }
        });
    }
}

/// Completion of a declarative scan-plan-generation attempt, including partial state on error.
/// Collected by [`super::ReportGeneratorLayer`] through a Registry-backed tracing subscriber.
#[derive(Debug, Clone)]
pub struct ScanPlanGenerationCompleted {
    /// Identity of this planning attempt.
    pub operation_id: MetricId,
    /// Caller-supplied correlation identity.
    pub correlation_id: Option<Arc<str>>,
    /// Terminal planning result.
    pub outcome: ScanPlanOutcome,
    /// Wall-clock planning duration.
    pub duration_ns: u64,
    /// Requested statistics options.
    pub stats: StatsOptions,
    /// Requested partition-value options.
    pub partition_values: PartitionValuesOptions,
    /// Selected replay base, absent before selection.
    pub replay_base: Option<ReplayBase>,
    /// Checkpoint used for replay, absent for CRC and commit-only replay.
    pub checkpoint: Option<CheckpointMetrics>,
    /// Inclusive first covered commit version, absent when no commits were selected.
    pub commit_start_version: Option<Version>,
    /// Inclusive last covered commit version, absent when no commits were selected.
    pub commit_end_version: Option<Version>,
    /// Combined size of the selected commit and compaction files.
    pub commit_total_size_bytes: u64,
    /// Selected compaction files, each counted once.
    pub compaction_file_count: u64,
}

impl ScanPlanGenerationCompleted {
    pub(crate) const SPAN_NAME: &str = "scan.declarative_metadata_scan_plan";

    pub(crate) fn new() -> Self {
        Self {
            operation_id: MetricId::new(),
            correlation_id: None,
            outcome: ScanPlanOutcome::Error,
            duration_ns: 0,
            stats: StatsOptions::default(),
            partition_values: PartitionValuesOptions::default(),
            replay_base: None,
            checkpoint: None,
            commit_start_version: None,
            commit_end_version: None,
            commit_total_size_bytes: 0,
            compaction_file_count: 0,
        }
    }

    pub(crate) fn update(update: impl FnOnce(&mut Self)) {
        super::reporter::update_scan_plan(update);
    }
}

impl fmt::Display for ScanPlanGenerationCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScanPlanGenerationCompleted(operation_id={}, outcome={:?}, duration_ns={}, \
             replay_base={:?}, commit_start_version={:?}, commit_end_version={:?}, \
             commit_total_size_bytes={}, compaction_file_count={}",
            self.operation_id,
            self.outcome,
            self.duration_ns,
            self.replay_base,
            self.commit_start_version,
            self.commit_end_version,
            self.commit_total_size_bytes,
            self.compaction_file_count
        )?;
        if let Some(checkpoint) = &self.checkpoint {
            write!(
                f,
                ", checkpoint={:?}, cache_hit={:?}, root_footer_calls={}, \
                 sidecar_footer_calls={}, metadata_query_calls={}",
                checkpoint.selected,
                checkpoint.cache_hit,
                checkpoint.root_footer_calls,
                checkpoint.sidecar_footer_calls,
                checkpoint.metadata_query_calls
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
