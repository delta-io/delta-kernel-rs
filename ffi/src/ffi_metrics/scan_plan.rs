//! Callback-scoped views of scan-plan metrics.

use delta_kernel::metrics as kernel;
use delta_kernel::scan::StructStats;

use super::{MetricEvent, MetricId, correlation_id_slice};
use crate::checkpoint::{FfiCheckpointInfo, FfiSelectedCheckpointType, visit};
use crate::delta_types::{FfiColumnName, FfiColumnNameArray};
use crate::scan::{FfiPartitionValuesOptions, FfiStatsOptions};
use crate::{FfiSlice, KernelStringSlice, OptionalValue, kernel_string_slice};

/// Layout version of the scan-plan metric and its nested records.
pub const SCAN_PLAN_GENERATION_METRIC_VERSION: u32 = 4;

/// Result of scan-plan generation.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum ScanPlanOutcome {
    Plan,
    Empty,
    Error,
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
}

/// Checkpoint selection, shared information, and discovery work for this planning attempt.
/// The info pointer and all nested values are borrowed only for the metric callback.
#[repr(C)]
pub struct CheckpointMetrics {
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

pub(super) fn with_event(event: &kernel::ScanPlanGenerationCompleted, f: impl FnOnce(MetricEvent)) {
    let info = event
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.info.as_deref());
    if let Err(error) = visit(info, |info| {
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
        let checkpoint = event
            .checkpoint
            .as_ref()
            .map(|checkpoint| CheckpointMetrics {
                version: checkpoint.selected.version,
                checkpoint_type: checkpoint.selected.checkpoint_type.map(Into::into).into(),
                info,
                cache_hit: checkpoint.cache_hit.into(),
                root_footer_calls: checkpoint.root_footer_calls,
                sidecar_footer_calls: checkpoint.sidecar_footer_calls,
                metadata_query_calls: checkpoint.metadata_query_calls,
            });
        let record = ScanPlanGenerationCompleted {
            operation_id: event.operation_id.into(),
            correlation_id: correlation_id_slice(event.correlation_id.as_deref()),
            outcome: match event.outcome {
                kernel::ScanPlanOutcome::Plan => ScanPlanOutcome::Plan,
                kernel::ScanPlanOutcome::Empty => ScanPlanOutcome::Empty,
                kernel::ScanPlanOutcome::Error => ScanPlanOutcome::Error,
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
        f(MetricEvent::ScanPlanGenerationCompleted(&record));
    }) {
        tracing::warn!(%error, "Failed to convert scan-plan metric checkpoint information");
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::checkpoint::SelectedCheckpoint;
    use delta_kernel::expressions::column_name;
    use delta_kernel::scan::{PartitionValuesOptions, StatsOptions};
    use rstest::rstest;

    use super::*;
    use crate::checkpoint::checkpoint_handles;
    use crate::{TryFromStringSlice as _, free_engine, free_snapshot};

    #[rstest]
    #[case::columns(StatsOptions::struct_columns(vec![column_name!("nested", "field.with.dot")]))]
    #[case::extra(StatsOptions::all_struct_with_extra_indexed(vec![column_name!("nested", "field")]))]
    #[case::none(StatsOptions::none())]
    fn callback_preserves_borrowed_paths_and_optional_values(
        #[case] stats: StatsOptions,
        #[values(None, Some(false), Some(true))] resolved: Option<bool>,
    ) {
        let with_checkpoint = resolved.is_some();
        let info = resolved.filter(|resolved| *resolved).map(|_| {
            let (snapshot, engine) =
                checkpoint_handles(Some(FfiSelectedCheckpointType::V2), true, false);
            let info = unsafe { snapshot.as_ref() }
                .reader()
                .get_checkpoint_info(unsafe { engine.as_ref() }.engine().as_ref())
                .unwrap()
                .unwrap();
            unsafe {
                free_snapshot(snapshot);
                free_engine(engine);
            }
            info
        });
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
                info,
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
        MetricEvent::with_kernel_event(&source, |ffi| {
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
                assert_eq!(checkpoint.info.is_null(), resolved != Some(true));
                if let Some(info) = unsafe { checkpoint.info.as_ref() } {
                    assert_eq!(info.checkpoint_type, FfiSelectedCheckpointType::V2);
                    assert_eq!((info.leaf_file_count, info.size_in_bytes), (1, 30));
                    assert_eq!(
                        unsafe { String::try_from_slice(&info.leaf_schema) }.unwrap(),
                        r#"{"type":"struct","fields":[]}"#
                    );
                    let hint = unsafe { info.last_checkpoint.try_to_kernel() }.unwrap();
                    let sidecars = hint.v2_checkpoint().unwrap().sidecar_files().unwrap();
                    assert_eq!(sidecars.len(), 1);
                    assert_eq!(sidecars[0].path, "leaf.parquet");
                    assert_eq!(sidecars[0].size_in_bytes, 13);
                    assert_eq!(sidecars[0].modification_time, 23);
                }
                assert_eq!(checkpoint.checkpoint_type, OptionalValue::None);
                assert_eq!(checkpoint.cache_hit, OptionalValue::Some(false));
                assert_eq!(checkpoint.root_footer_calls, 1);
                assert_eq!(checkpoint.sidecar_footer_calls, 0);
            }
        });
    }
}
