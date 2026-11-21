//! Borrowed checkpoint discovery metrics.

use super::{FfiCheckpointInfo, FfiSelectedCheckpointType};
use crate::OptionalValue;

/// Checkpoint selection, shared information, and discovery work for one discovery attempt.
/// The info pointer and all nested values are borrowed only for the metric callback.
#[repr(C)]
pub struct FfiCheckpointMetrics {
    /// Selected version, retained if discovery fails.
    pub version: u64,
    /// Type known at selection time; the resolved type is available in `info`.
    pub checkpoint_type: OptionalValue<FfiSelectedCheckpointType>,
    /// Null if discovery did not produce checkpoint information.
    pub info: *const FfiCheckpointInfo,
    pub cache_hit: OptionalValue<bool>,
    pub root_footer_calls: u64,
    pub sidecar_footer_calls: u64,
    pub metadata_query_calls: u64,
}
