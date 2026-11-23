//! Borrowed metadata planning metrics.

use crate::scan::{FfiPartitionValuesOptions, FfiStatsOptions};
use crate::{KernelStringSlice, OptionalValue};

/// Starting state selected for metadata replay.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum ReplayBase {
    Crc,
    Checkpoint,
    Empty,
}

/// Metadata planning metrics. Strings and column paths are borrowed
/// only for the enclosing metric callback; copy every retained value before returning.
#[repr(C)]
pub struct MetadataPlanningReport {
    pub stats: FfiStatsOptions,
    pub variant_min_max: bool,
    /// False when Geometry statistics support is disabled.
    pub geometry_min_max: bool,
    pub partition_values: FfiPartitionValuesOptions,
    pub partition_timezone: OptionalValue<KernelStringSlice>,
    pub replay_base: OptionalValue<ReplayBase>,
    pub commit_start_version: OptionalValue<u64>,
    pub commit_end_version: OptionalValue<u64>,
    pub commit_total_size_bytes: u64,
    pub compaction_file_count: u64,
    pub read_collations: bool,
    pub has_filter: bool,
    pub base_version: OptionalValue<u64>,
}
