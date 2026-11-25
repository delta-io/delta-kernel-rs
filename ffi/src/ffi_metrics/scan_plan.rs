//! Callback-scoped views of scan-plan metrics.

use delta_kernel::metrics as kernel;
use delta_kernel::scan::StructStats;

use super::{MetricEvent, MetricId, optional_string_slice};
use crate::checkpoint::{FfiCheckpointMetrics, visit};
use crate::delta_types::{FfiColumnName, FfiColumnNameArray};
use crate::scan::{FfiPartitionValuesOptions, FfiStatsOptions};
use crate::{FfiSlice, KernelStringSlice, OptionalValue, kernel_string_slice};

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

/// A scan-plan-generation report. Checkpoint data, strings, and column paths are borrowed only
/// for the metric callback; copy every retained value before returning.
#[repr(C)]
pub struct ScanPlanReport {
    // === Operation ===
    pub operation_id: MetricId,
    pub table_id: KernelStringSlice,
    pub table_path: KernelStringSlice,
    pub correlation_id: KernelStringSlice,
    pub outcome: ScanPlanOutcome,
    pub duration_ns: u64,

    // === Scan options ===
    pub stats: FfiStatsOptions,
    pub variant_min_max: bool,
    /// False when Geometry statistics support is disabled.
    pub geometry_min_max: bool,
    pub partition_values: FfiPartitionValuesOptions,
    pub partition_timezone: OptionalValue<KernelStringSlice>,
    pub read_collations: bool,
    pub has_filter: bool,

    // === Plan ===
    pub num_plan_nodes: u64,
    pub total_json_bytes: u64,
    pub total_parquet_bytes: u64,

    // === Log replay ===
    pub replay_base: OptionalValue<ReplayBase>,
    pub base_version: OptionalValue<u64>,
    /// Null when no checkpoint was used.
    pub checkpoint: *const FfiCheckpointMetrics,
    pub commit_start_version: OptionalValue<u64>,
    pub commit_end_version: OptionalValue<u64>,
    pub commit_total_size_bytes: u64,
    pub compaction_file_count: u64,
}

pub(super) fn with_event<F: FnOnce(MetricEvent)>(event: &kernel::ScanPlanReport, f: F) {
    let info = event
        .checkpoint
        .as_ref()
        .and_then(|metrics| metrics.info.as_deref());
    if let Err(error) = visit(info, |info| {
        let metadata = &event.metadata_planning;
        let columns = match metadata.stats.struct_stats() {
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
        let stats = FfiStatsOptions::from_kernel(&metadata.stats, columns);
        let metrics = event
            .checkpoint
            .as_ref()
            .map(|metrics| FfiCheckpointMetrics {
                version: metrics.version,
                info,
                source: metrics.source.map(Into::into).into(),
                root_footer_calls: metrics.root_footer_calls,
                sidecar_footer_calls: metrics.sidecar_footer_calls,
                metadata_query_calls: metrics.metadata_query_calls,
                duration_ns: metrics.duration_ns,
            });
        let table_id = &event.table_id;
        let table_path = &event.table_path;
        let record = ScanPlanReport {
            operation_id: event.operation_id.into(),
            table_id: kernel_string_slice!(table_id),
            table_path: kernel_string_slice!(table_path),
            correlation_id: optional_string_slice(event.correlation_id.as_deref()),
            outcome: match event.outcome {
                kernel::ScanPlanOutcome::Plan => ScanPlanOutcome::Plan,
                kernel::ScanPlanOutcome::Empty => ScanPlanOutcome::Empty,
                kernel::ScanPlanOutcome::Error => ScanPlanOutcome::Error,
            },
            duration_ns: event.duration_ns,
            stats,
            variant_min_max: metadata.stats.variant_min_max(),
            geometry_min_max: {
                #[cfg(feature = "geo-type-in-dev")]
                {
                    metadata.stats.geometry_min_max()
                }
                #[cfg(not(feature = "geo-type-in-dev"))]
                {
                    false
                }
            },
            partition_values: if metadata.partition_values.parsed_struct() {
                FfiPartitionValuesOptions::WithStruct
            } else {
                FfiPartitionValuesOptions::StringMapOnly
            },
            partition_timezone: metadata
                .partition_values
                .timestamp_timezone()
                .map(|timezone| kernel_string_slice!(timezone))
                .into(),
            read_collations: metadata.read_collations,
            has_filter: metadata.has_filter,
            num_plan_nodes: event.num_plan_nodes,
            total_json_bytes: metadata.total_json_bytes,
            total_parquet_bytes: metadata.total_parquet_bytes,
            replay_base: metadata
                .replay_base
                .map(|base| match base {
                    kernel::ReplayBase::Crc => ReplayBase::Crc,
                    kernel::ReplayBase::Checkpoint => ReplayBase::Checkpoint,
                    kernel::ReplayBase::Empty => ReplayBase::Empty,
                })
                .into(),
            base_version: metadata.base_version.into(),
            checkpoint: metrics
                .as_ref()
                .map_or(std::ptr::null(), std::ptr::from_ref),
            commit_start_version: metadata.commit_start_version.into(),
            commit_end_version: metadata.commit_end_version.into(),
            commit_total_size_bytes: metadata.commit_total_size_bytes,
            compaction_file_count: metadata.compaction_file_count,
        };
        f(MetricEvent::ScanPlanGenerationCompleted(record));
    }) {
        tracing::warn!(%error, "Failed to convert scan-plan metric checkpoint information");
    }
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use delta_kernel::expressions::column_name;
    use delta_kernel::scan::{PartitionValuesOptions, StatsOptions};
    use rstest::rstest;

    use super::*;
    use crate::checkpoint::{FfiCheckpointSource, FfiSelectedCheckpointType, checkpoint_handles};
    use crate::ffi_metrics::with_ffi_event;
    use crate::{TryFromStringSlice as _, free_engine, free_snapshot};

    #[rstest]
    #[case::columns(StatsOptions::struct_columns(vec![column_name!("nested", "field.with.dot")]))]
    #[case::extra(StatsOptions::all_struct_with_extra_indexed(vec![column_name!("nested", "field")]))]
    #[case::none(StatsOptions::none())]
    #[case::json(StatsOptions::json_only())]
    #[case::all(StatsOptions::all())]
    #[case::all_struct(StatsOptions::all_struct())]
    #[tokio::test]
    async fn callback_preserves_borrowed_paths_and_optional_values(
        #[case] stats: StatsOptions,
        #[values(None, Some(false), Some(true))] resolved: Option<bool>,
        #[values(false, true)] failed: bool,
        #[values(
            (None, None),
            (Some(kernel::ReplayBase::Crc), Some(ReplayBase::Crc)),
            (Some(kernel::ReplayBase::Checkpoint), Some(ReplayBase::Checkpoint)),
            (Some(kernel::ReplayBase::Empty), Some(ReplayBase::Empty)),
        )]
        replay_base: (Option<kernel::ReplayBase>, Option<ReplayBase>),
    ) {
        let with_checkpoint = resolved.is_some();
        let info = if resolved == Some(true) {
            let (snapshot, engine, _) =
                checkpoint_handles(Some(FfiSelectedCheckpointType::V2), true, false).await;
            let info = unsafe { snapshot.as_ref() }
                .reader()
                .get_checkpoint_info(unsafe { engine.as_ref() }.engine().as_ref())
                .unwrap()
                .unwrap();
            unsafe {
                free_snapshot(snapshot);
                free_engine(engine);
            }
            Some(info)
        } else {
            None
        };
        let event = kernel::ScanPlanReport {
            operation_id: kernel::MetricId::new(),
            table_id: "table-test".into(),
            table_path: "s3://bucket/table%20path/".into(),
            correlation_id: Some("scan-test".into()),
            outcome: if failed {
                kernel::ScanPlanOutcome::Error
            } else {
                kernel::ScanPlanOutcome::Plan
            },
            duration_ns: 19,
            num_plan_nodes: 13,
            checkpoint: with_checkpoint.then_some(kernel::CheckpointMetrics {
                version: 0,
                info,
                source: Some(kernel::CheckpointSource::Resolved),
                root_footer_calls: 1,
                sidecar_footer_calls: 0,
                metadata_query_calls: 0,
                duration_ns: 7,
            }),
            metadata_planning: kernel::MetadataPlanningReport {
                stats,
                partition_values: PartitionValuesOptions::with_struct()
                    .with_timestamp_timezone("UTC"),
                read_collations: failed,
                has_filter: !failed,
                replay_base: replay_base.0,
                base_version: with_checkpoint.then_some(0),
                commit_start_version: with_checkpoint.then_some(0),
                commit_end_version: with_checkpoint.then_some(0),
                commit_total_size_bytes: 0,
                compaction_file_count: 0,
                total_json_bytes: 31,
                total_parquet_bytes: 47,
            },
        };
        let metadata = &event.metadata_planning;
        let source = kernel::MetricEvent::ScanPlanGenerationCompleted(event.clone());
        let mut visits = 0;
        with_ffi_event(&source, |ffi| {
            visits += 1;
            let MetricEvent::ScanPlanGenerationCompleted(record) = ffi else {
                panic!("expected scan-plan metric");
            };
            assert_eq!(record.operation_id.bytes, event.operation_id.as_bytes());
            assert_eq!(
                unsafe { String::try_from_slice(&record.table_id) }.unwrap(),
                "table-test"
            );
            assert_eq!(
                unsafe { String::try_from_slice(&record.table_path) }.unwrap(),
                "s3://bucket/table%20path/"
            );
            assert_eq!(
                unsafe { String::try_from_slice(&record.correlation_id) }.unwrap(),
                "scan-test"
            );
            assert_eq!(
                record.outcome,
                if failed {
                    ScanPlanOutcome::Error
                } else {
                    ScanPlanOutcome::Plan
                }
            );
            assert_eq!(record.duration_ns, 19);
            assert_eq!(record.num_plan_nodes, 13);
            assert_eq!(record.total_json_bytes, 31);
            assert_eq!(record.total_parquet_bytes, 47);
            assert_eq!(record.read_collations, failed);
            assert_eq!(record.has_filter, !failed);
            assert_eq!(record.base_version, with_checkpoint.then_some(0).into());
            assert_eq!(record.replay_base, replay_base.1.into());
            assert_eq!(
                unsafe { record.stats.try_to_kernel() }.unwrap(),
                metadata.stats
            );
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
                assert_eq!(
                    checkpoint.source,
                    OptionalValue::Some(FfiCheckpointSource::Resolved)
                );
                assert_eq!(checkpoint.root_footer_calls, 1);
                assert_eq!(checkpoint.sidecar_footer_calls, 0);
                assert_eq!(checkpoint.duration_ns, 7);
            }
        });
        assert_eq!(visits, 1);
    }
}
