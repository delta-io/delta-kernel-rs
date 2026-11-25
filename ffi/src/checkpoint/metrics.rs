//! Borrowed checkpoint discovery metrics.

use delta_kernel::metrics::CheckpointSource;

use super::FfiCheckpointInfo;
use crate::OptionalValue;

/// Checkpoint discovery path reached by an attempt, independently of success or failure.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiCheckpointSource {
    /// Completed checkpoint information was reused from the snapshot cache.
    SnapshotCache,
    /// Applicable `_last_checkpoint` metadata was used without checkpoint I/O.
    LastCheckpoint,
    /// At least one checkpoint footer or metadata-query invocation was attempted.
    Resolved,
}

impl From<CheckpointSource> for FfiCheckpointSource {
    fn from(source: CheckpointSource) -> Self {
        match source {
            CheckpointSource::SnapshotCache => Self::SnapshotCache,
            CheckpointSource::LastCheckpoint => Self::LastCheckpoint,
            CheckpointSource::Resolved => Self::Resolved,
        }
    }
}

/// Checkpoint selection, shared information, and discovery work for one discovery attempt.
/// The info pointer and all nested values are borrowed only for the metric callback.
#[repr(C)]
pub struct FfiCheckpointMetrics {
    /// Selected version, retained if discovery fails.
    pub version: u64,
    /// Null if discovery did not produce checkpoint information.
    pub info: *const FfiCheckpointInfo,
    pub source: OptionalValue<FfiCheckpointSource>,
    pub root_footer_calls: u64,
    pub sidecar_footer_calls: u64,
    pub metadata_query_calls: u64,
    /// Elapsed checkpoint lookup time in nanoseconds, including waiting for shared resolution.
    pub duration_ns: u64,
}
