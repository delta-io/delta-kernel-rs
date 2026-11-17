//! Callback-scoped views of scan-plan metrics.

use delta_kernel::checkpoint::CheckpointKind;
use delta_kernel::metrics as kernel;
use delta_kernel::scan::StructStats;
use delta_kernel::schema::DataType;

use super::{MetricEvent, MetricId, correlation_id_slice};
use crate::checkpoint::{FfiCheckpointKind, FfiSelectedCheckpointType};
use crate::delta_types::{FfiColumnName, FfiColumnNameArray};
use crate::scan::{FfiPartitionValuesOptions, FfiStatsOptions};
use crate::{FfiSlice, KernelStringSlice, OptionalValue, kernel_string_slice};

/// Layout version of the scan-plan metric and its nested records.
pub const SCAN_PLAN_GENERATION_METRIC_VERSION: u32 = 3;

/// Result of scan-plan generation.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum ScanPlanOutcome {
    Plan,
    Empty,
    Error,
    Unknown,
}

/// Starting state selected for metadata replay.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum ReplayBase {
    Crc,
    Checkpoint,
    Empty,
    Unknown,
}

/// Bounded checkpoint fields projected from the shared native checkpoint objects.
/// Fields describing resolved information must be ignored when `has_info` is false.
#[repr(C)]
pub struct CheckpointMetrics {
    pub version: u64,
    pub checkpoint_type: OptionalValue<FfiSelectedCheckpointType>,
    pub has_info: bool,
    pub kind: FfiCheckpointKind,
    pub root_file_count: u64,
    pub sidecar_count: u64,
    pub leaf_file_count: u64,
    pub size_in_bytes: u64,
    pub action_count: OptionalValue<u64>,
    pub add_file_count: OptionalValue<u64>,
    pub leaf_schema_has_json_stats: bool,
    pub leaf_schema_has_parsed_stats: bool,
    pub leaf_schema_has_parsed_partition_values: bool,
    pub cache_hit: OptionalValue<bool>,
    pub root_footer_calls: u64,
    pub sidecar_footer_calls: u64,
    pub metadata_query_calls: u64,
}

impl From<&kernel::CheckpointMetrics> for CheckpointMetrics {
    fn from(metrics: &kernel::CheckpointMetrics) -> Self {
        let info = metrics.info.as_deref();
        let hint = info.map(|info| info.last_checkpoint());
        let add = info.and_then(|info| info.leaf_schema().field("add"));
        let has_field = |name| match add.map(|field| field.data_type()) {
            Some(DataType::Struct(schema)) => schema.contains(name),
            _ => false,
        };
        Self {
            version: metrics.selected.version,
            checkpoint_type: info
                .map(|info| info.checkpoint_type())
                .or(metrics.selected.checkpoint_type)
                .map(Into::into)
                .into(),
            has_info: info.is_some(),
            kind: info.map_or(FfiCheckpointKind::Inline, |info| info.kind().into()),
            root_file_count: hint.map_or(0, |hint| hint.parts().unwrap_or(1) as u64),
            sidecar_count: info
                .filter(|info| info.kind() == CheckpointKind::Manifest)
                .map_or(0, |info| info.leaf_file_count() as u64),
            leaf_file_count: info.map_or(0, |info| info.leaf_file_count() as u64),
            size_in_bytes: info.map_or(0, |info| info.size_in_bytes()),
            action_count: hint.and_then(|hint| u64::try_from(hint.size()).ok()).into(),
            add_file_count: hint
                .and_then(|hint| hint.num_of_add_files())
                .and_then(|count| u64::try_from(count).ok())
                .into(),
            leaf_schema_has_json_stats: has_field("stats"),
            leaf_schema_has_parsed_stats: has_field("stats_parsed"),
            leaf_schema_has_parsed_partition_values: has_field("partitionValues_parsed"),
            cache_hit: metrics.cache_hit.into(),
            root_footer_calls: metrics.root_footer_calls,
            sidecar_footer_calls: metrics.sidecar_footer_calls,
            metadata_query_calls: metrics.metadata_query_calls,
        }
    }
}

/// A scan-plan-generation attempt. The record, checkpoint pointer, strings, and column paths are
/// borrowed only for the metric callback; copy every retained value before returning.
#[repr(C)]
pub struct ScanPlanGenerationCompleted {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub outcome: ScanPlanOutcome,
    pub duration_ns: u64,
    pub stats: FfiStatsOptions,
    pub variant_min_max: bool,
    /// False when Geometry statistics support is disabled.
    pub geometry_min_max: bool,
    pub partition_values: FfiPartitionValuesOptions,
    pub partition_timezone: OptionalValue<KernelStringSlice>,
    pub replay_base: OptionalValue<ReplayBase>,
    /// Null when no checkpoint was used.
    pub checkpoint: *const CheckpointMetrics,
    pub commit_start_version: OptionalValue<u64>,
    pub commit_end_version: OptionalValue<u64>,
    pub commit_total_size_bytes: u64,
    pub compaction_file_count: u64,
}

pub(super) fn with_event<R>(
    event: &kernel::ScanPlanGenerationCompleted,
    f: impl FnOnce(MetricEvent) -> R,
) -> R {
    let columns = match event.stats.struct_stats() {
        StructStats::None => &[][..],
        StructStats::AllIndexed { extra_indexed } => extra_indexed.as_slice(),
        StructStats::Columns { requested } => requested.as_slice(),
    };
    let paths: Vec<Vec<_>> = columns
        .iter()
        .map(|column| {
            column
                .path()
                .iter()
                .map(|part| kernel_string_slice!(part))
                .collect()
        })
        .collect();
    let columns: Vec<_> = paths
        .iter()
        .map(|path| FfiColumnName {
            // Safety: the paths and their borrowed strings outlive the callback.
            path: unsafe { FfiSlice::new_unsafe(path) },
        })
        .collect();
    // Safety: columns and all nested storage remain alive until the callback returns.
    let columns = unsafe { FfiColumnNameArray::new_unsafe(&columns) };
    let stats = match event.stats.struct_stats() {
        StructStats::None if event.stats.synthesize_json() => FfiStatsOptions::JsonOnly,
        StructStats::None => FfiStatsOptions::None,
        StructStats::AllIndexed { .. } if event.stats.synthesize_json() => FfiStatsOptions::All,
        StructStats::AllIndexed { extra_indexed } if extra_indexed.is_empty() => {
            FfiStatsOptions::AllStruct
        }
        StructStats::AllIndexed { .. } => FfiStatsOptions::AllStructWithExtraIndexed(columns),
        StructStats::Columns { .. } => FfiStatsOptions::StructColumns(columns),
    };
    let checkpoint = event.checkpoint.as_ref().map(CheckpointMetrics::from);
    let record = ScanPlanGenerationCompleted {
        operation_id: event.operation_id.into(),
        correlation_id: correlation_id_slice(event.correlation_id.as_deref()),
        outcome: match event.outcome {
            kernel::ScanPlanOutcome::Plan => ScanPlanOutcome::Plan,
            kernel::ScanPlanOutcome::Empty => ScanPlanOutcome::Empty,
            kernel::ScanPlanOutcome::Error => ScanPlanOutcome::Error,
            _ => ScanPlanOutcome::Unknown,
        },
        duration_ns: event.duration_ns,
        stats,
        variant_min_max: event.stats.variant_min_max(),
        geometry_min_max: {
            #[cfg(feature = "geo-type-in-dev")]
            {
                event.stats.geometry_min_max()
            }
            #[cfg(not(feature = "geo-type-in-dev"))]
            {
                false
            }
        },
        partition_values: if event.partition_values.parsed_struct() {
            FfiPartitionValuesOptions::WithStruct
        } else {
            FfiPartitionValuesOptions::StringMapOnly
        },
        partition_timezone: event
            .partition_values
            .timestamp_timezone()
            .map(|timezone| kernel_string_slice!(timezone))
            .into(),
        replay_base: event
            .replay_base
            .map(|base| match base {
                kernel::ReplayBase::Crc => ReplayBase::Crc,
                kernel::ReplayBase::Checkpoint => ReplayBase::Checkpoint,
                kernel::ReplayBase::Empty => ReplayBase::Empty,
                _ => ReplayBase::Unknown,
            })
            .into(),
        checkpoint: checkpoint
            .as_ref()
            .map_or(std::ptr::null(), std::ptr::from_ref),
        commit_start_version: event.commit_start_version.into(),
        commit_end_version: event.commit_end_version.into(),
        commit_total_size_bytes: event.commit_total_size_bytes,
        compaction_file_count: event.compaction_file_count,
    };
    f(MetricEvent::ScanPlanGenerationCompleted(&record))
}

#[cfg(test)]
mod tests {
    use delta_kernel::checkpoint::SelectedCheckpoint;
    use delta_kernel::expressions::column_name;
    use delta_kernel::scan::{PartitionValuesOptions, StatsOptions};
    use rstest::rstest;

    use super::*;
    use crate::TryFromStringSlice as _;
    use crate::ffi_metrics::with_ffi_event;

    #[rstest]
    #[case::columns(StatsOptions::struct_columns(vec![column_name!("nested", "field.with.dot")]))]
    #[case::extra(StatsOptions::all_struct_with_extra_indexed(vec![column_name!("nested", "field")]))]
    #[case::none(StatsOptions::none())]
    fn callback_preserves_borrowed_paths_and_optional_values(
        #[case] stats: StatsOptions,
        #[values(false, true)] with_checkpoint: bool,
    ) {
        let event = kernel::ScanPlanGenerationCompleted {
            operation_id: kernel::MetricId::new(),
            correlation_id: Some("scan-test".into()),
            outcome: kernel::ScanPlanOutcome::Error,
            duration_ns: 19,
            stats,
            partition_values: PartitionValuesOptions::with_struct().with_timestamp_timezone("UTC"),
            replay_base: with_checkpoint.then_some(kernel::ReplayBase::Checkpoint),
            checkpoint: with_checkpoint.then_some(kernel::CheckpointMetrics {
                selected: SelectedCheckpoint {
                    version: 0,
                    checkpoint_type: None,
                },
                info: None,
                cache_hit: Some(false),
                root_footer_calls: 1,
                sidecar_footer_calls: 0,
                metadata_query_calls: 0,
            }),
            commit_start_version: with_checkpoint.then_some(0),
            commit_end_version: with_checkpoint.then_some(0),
            commit_total_size_bytes: 0,
            compaction_file_count: 0,
        };
        let source = kernel::MetricEvent::ScanPlanGenerationCompleted(event.clone());
        with_ffi_event(&source, |ffi| {
            let MetricEvent::ScanPlanGenerationCompleted(record) = ffi else {
                panic!("expected scan-plan metric");
            };
            // Safety: the record and all nested storage are borrowed for this callback.
            let record = unsafe { &*record };
            assert_eq!(record.operation_id.bytes, event.operation_id.as_bytes());
            assert_eq!(
                unsafe { String::try_from_slice(&record.correlation_id) }.unwrap(),
                "scan-test"
            );
            assert_eq!(record.outcome, ScanPlanOutcome::Error);
            assert_eq!(record.duration_ns, 19);
            match (&record.stats, event.stats.struct_stats()) {
                (FfiStatsOptions::StructColumns(columns), StructStats::Columns { requested }) => {
                    assert_eq!(
                        &unsafe { columns.try_to_column_names() }.unwrap(),
                        requested
                    );
                }
                (
                    FfiStatsOptions::AllStructWithExtraIndexed(columns),
                    StructStats::AllIndexed { extra_indexed },
                ) => {
                    assert_eq!(
                        &unsafe { columns.try_to_column_names() }.unwrap(),
                        extra_indexed
                    );
                }
                (FfiStatsOptions::None, StructStats::None) => {}
                _ => panic!("incorrect stats options"),
            }
            assert!(matches!(
                record.partition_values,
                FfiPartitionValuesOptions::WithStruct
            ));
            let OptionalValue::Some(timezone) = &record.partition_timezone else {
                panic!("missing timezone");
            };
            assert_eq!(unsafe { String::try_from_slice(timezone) }.unwrap(), "UTC");
            assert_eq!(
                record.commit_start_version,
                with_checkpoint.then_some(0).into()
            );
            assert_eq!(
                record.commit_end_version,
                with_checkpoint.then_some(0).into()
            );
            assert_eq!(record.checkpoint.is_null(), !with_checkpoint);
            if with_checkpoint {
                let checkpoint = unsafe { &*record.checkpoint };
                assert_eq!(checkpoint.version, 0);
                assert!(!checkpoint.has_info);
                assert_eq!(checkpoint.checkpoint_type, OptionalValue::None);
                assert_eq!(checkpoint.cache_hit, OptionalValue::Some(false));
                assert_eq!(checkpoint.root_footer_calls, 1);
                assert_eq!(checkpoint.sidecar_footer_calls, 0);
            }
        });
    }
}
