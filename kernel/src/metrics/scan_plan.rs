//! Observations from declarative scan-plan generation.

use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
#[cfg(any(feature = "declarative-plans", test))]
use serde::Serialize;
use strum::{AsRefStr, Display as StrumDisplay, EnumString, IntoStaticStr};
#[cfg(any(feature = "declarative-plans", test))]
use tracing::field::Empty;
use tracing::field::{Field, Visit};
#[cfg(any(feature = "declarative-plans", test))]
use tracing::info_span;
use tracing::span::Attributes;
use tracing::warn;

use super::MetricId;
use crate::checkpoint::{CheckpointInfo, SelectedCheckpointType};
use crate::expressions::ColumnName;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::scan::{PartitionValuesOptions, StatsOptions, StructStats};
use crate::schema::SchemaRef;
use crate::{FileMeta, Version};

/// Result of one scan-plan-generation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, StrumDisplay, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ScanPlanOutcome {
    /// A metadata plan was produced.
    Plan,
    /// No metadata plan was needed.
    Empty,
    /// Planning failed.
    Error,
}

/// Starting state selected before replaying newer commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, StrumDisplay, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ReplayBase {
    /// Complete live-file state from a CRC.
    Crc,
    /// A selected checkpoint.
    Checkpoint,
    /// Replay starts from commits alone.
    Empty,
}

/// Checkpoint discovery path reached by an attempt, independently of success or failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, StrumDisplay, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
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
    /// Table identifier from the snapshot's metadata.
    pub table_id: String,
    /// Caller-supplied correlation identity.
    pub correlation_id: Option<Arc<str>>,
    /// Terminal planning result.
    pub outcome: ScanPlanOutcome,
    /// Wall-clock planning duration.
    pub duration_ns: u64,
    /// Nodes in the returned plan, or zero when no plan is returned.
    pub num_plan_nodes: u64,
    /// Checkpoint lookup metrics, including partial information on failure.
    /// Absent when no checkpoint is selected or lookup was not reached.
    pub checkpoint: Option<CheckpointMetrics>,
    /// Metadata planning metrics.
    pub metadata_planning: MetadataPlanningReport,
}

impl ScanPlanReport {
    pub(crate) const SPAN_NAME: &str = "scan.plan_generation_completed";

    #[cfg(feature = "declarative-plans")]
    pub(crate) fn new() -> Self {
        Self::with_operation_id(MetricId::new())
    }

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut report = Self::with_operation_id(MetricId::from_attrs(attrs));
        attrs.record(&mut report);
        report
    }

    fn with_operation_id(operation_id: MetricId) -> Self {
        Self {
            operation_id,
            table_id: String::new(),
            correlation_id: None,
            outcome: ScanPlanOutcome::Error,
            duration_ns: 0,
            num_plan_nodes: 0,
            checkpoint: None,
            metadata_planning: MetadataPlanningReport::default(),
        }
    }
}

/// Emits completed planning metrics through the same span-field path as other metrics.
#[cfg(any(feature = "declarative-plans", test))]
pub(crate) fn emit_scan_plan_completed(report: &ScanPlanReport) {
    let metadata = &report.metadata_planning;
    let checkpoint = report.checkpoint.as_ref();
    let (stats_mode, columns): (_, &[ColumnName]) = match &metadata.stats.struct_stats {
        StructStats::None => ("none", &[]),
        StructStats::AllIndexed { extra_indexed } => ("all_indexed", extra_indexed),
        StructStats::Columns { requested } => ("columns", requested),
    };
    let _span = info_span!(
        ScanPlanReport::SPAN_NAME,
        report = Empty,
        operation_id = %report.operation_id,
        table_id = report.table_id.as_str(),
        correlation_id = report.correlation_id.as_deref(),
        outcome = report.outcome.as_ref(),
        duration_ns = report.duration_ns,
        num_plan_nodes = report.num_plan_nodes,
        stats_json = metadata.stats.synthesize_json,
        stats_struct = stats_mode,
        stats_columns = json_field(columns).as_deref(),
        variant_min_max = metadata.stats.variant_min_max,
        geometry_min_max = {
            #[cfg(feature = "geo-type-in-dev")]
            { metadata.stats.geometry_min_max }
            #[cfg(not(feature = "geo-type-in-dev"))]
            { false }
        },
        partition_struct = metadata.partition_values.parsed_struct,
        partition_timezone = metadata.partition_values.timestamp_timezone.as_deref(),
        read_collations = metadata.read_collations,
        has_filter = metadata.has_filter,
        replay_base = metadata.replay_base.map(<&'static str>::from),
        base_version = metadata.base_version,
        commit_start_version = metadata.commit_start_version,
        commit_end_version = metadata.commit_end_version,
        commit_total_size_bytes = metadata.commit_total_size_bytes,
        compaction_file_count = metadata.compaction_file_count,
        total_json_bytes = metadata.total_json_bytes,
        total_parquet_bytes = metadata.total_parquet_bytes,
        checkpoint_version = checkpoint.map(|metrics| metrics.version),
        checkpoint_source = checkpoint.and_then(|metrics| metrics.source).map(<&'static str>::from),
        root_footer_calls = checkpoint.map(|metrics| metrics.root_footer_calls),
        sidecar_footer_calls = checkpoint.map(|metrics| metrics.sidecar_footer_calls),
        metadata_query_calls = checkpoint.map(|metrics| metrics.metadata_query_calls),
        checkpoint_duration_ns = checkpoint.map(|metrics| metrics.duration_ns),
        checkpoint_info = checkpoint.and_then(|metrics| metrics.info.as_deref())
            .and_then(checkpoint_info_json).as_deref(),
    );
}

impl Visit for ScanPlanReport {
    fn record_u64(&mut self, field: &Field, value: u64) {
        let metadata = &mut self.metadata_planning;
        match field.name() {
            "duration_ns" => self.duration_ns = value,
            "num_plan_nodes" => self.num_plan_nodes = value,
            "base_version" => metadata.base_version = Some(value),
            "commit_start_version" => metadata.commit_start_version = Some(value),
            "commit_end_version" => metadata.commit_end_version = Some(value),
            "commit_total_size_bytes" => metadata.commit_total_size_bytes = value,
            "compaction_file_count" => metadata.compaction_file_count = value,
            "total_json_bytes" => metadata.total_json_bytes = value,
            "total_parquet_bytes" => metadata.total_parquet_bytes = value,
            "checkpoint_version" => self.checkpoint.get_or_insert_default().version = value,
            "root_footer_calls" => {
                self.checkpoint.get_or_insert_default().root_footer_calls = value
            }
            "sidecar_footer_calls" => {
                self.checkpoint.get_or_insert_default().sidecar_footer_calls = value
            }
            "metadata_query_calls" => {
                self.checkpoint.get_or_insert_default().metadata_query_calls = value
            }
            "checkpoint_duration_ns" => self.checkpoint.get_or_insert_default().duration_ns = value,
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        let metadata = &mut self.metadata_planning;
        match field.name() {
            "stats_json" => metadata.stats.synthesize_json = value,
            "variant_min_max" => metadata.stats.variant_min_max = value,
            #[cfg(feature = "geo-type-in-dev")]
            "geometry_min_max" => metadata.stats.geometry_min_max = value,
            "partition_struct" => metadata.partition_values.parsed_struct = value,
            "read_collations" => metadata.read_collations = value,
            "has_filter" => metadata.has_filter = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if self.record_text(field.name(), value).is_err() {
            warn!(field = field.name(), "Invalid scan-plan metric field");
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

impl ScanPlanReport {
    fn record_text(&mut self, name: &str, value: &str) -> Result<(), ()> {
        let metadata = &mut self.metadata_planning;
        match name {
            "table_id" => self.table_id = value.to_owned(),
            "correlation_id" => self.correlation_id = Some(value.into()),
            "outcome" => self.outcome = value.parse().map_err(|_| ())?,
            "replay_base" => metadata.replay_base = Some(value.parse().map_err(|_| ())?),
            "partition_timezone" => {
                metadata.partition_values.timestamp_timezone = Some(value.into())
            }
            "stats_struct" => {
                metadata.stats.struct_stats = match value {
                    "none" => StructStats::None,
                    "all_indexed" => StructStats::AllIndexed {
                        extra_indexed: Vec::new(),
                    },
                    "columns" => StructStats::Columns {
                        requested: Vec::new(),
                    },
                    _ => return Err(()),
                }
            }
            "stats_columns" => {
                let columns: Vec<ColumnName> = serde_json::from_str(value).map_err(|_| ())?;
                match &mut metadata.stats.struct_stats {
                    StructStats::AllIndexed { extra_indexed } => *extra_indexed = columns,
                    StructStats::Columns { requested } => *requested = columns,
                    StructStats::None if columns.is_empty() => {}
                    _ => return Err(()),
                }
            }
            "checkpoint_source" => {
                self.checkpoint.get_or_insert_default().source =
                    Some(value.parse().map_err(|_| ())?)
            }
            "checkpoint_info" => {
                self.checkpoint.get_or_insert_default().info =
                    Some(Arc::new(checkpoint_info_from_json(value).map_err(|_| ())?))
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(any(feature = "declarative-plans", test))]
fn json_field<T: Serialize + ?Sized>(value: &T) -> Option<String> {
    serde_json::to_string(value)
        .map_err(|error| {
            warn!(%error, "Failed to encode scan-plan metric field");
        })
        .ok()
}

// Only compound fields use JSON; this private tuple preserves the complete checkpoint information
// without adding serialization to its public API or introducing another checkpoint summary type.
#[cfg(any(feature = "declarative-plans", test))]
fn checkpoint_info_json(info: &CheckpointInfo) -> Option<String> {
    let sidecars: Vec<_> = info
        .sidecars
        .iter()
        .map(|file| (&file.location, file.last_modified, file.size))
        .collect();
    json_field(&(
        info.checkpoint_type() == SelectedCheckpointType::V2,
        info.leaf_schema(),
        sidecars,
        info.size_in_bytes(),
        info.last_checkpoint(),
    ))
}

fn checkpoint_info_from_json(value: &str) -> serde_json::Result<CheckpointInfo> {
    type Fields = (
        bool,
        SchemaRef,
        Vec<(url::Url, i64, u64)>,
        u64,
        Box<LastCheckpointHint>,
    );
    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.disable_recursion_limit();
    let (v2, leaf_checkpoint_schema, sidecars, size_in_bytes, last_checkpoint) =
        Fields::deserialize(serde_stacker::Deserializer::new(&mut deserializer))?;
    deserializer.end()?;
    Ok(CheckpointInfo {
        checkpoint_type: if v2 {
            SelectedCheckpointType::V2
        } else {
            SelectedCheckpointType::V1
        },
        leaf_checkpoint_schema,
        sidecars: sidecars
            .into_iter()
            .map(|(location, last_modified, size)| FileMeta {
                location,
                last_modified,
                size,
            })
            .collect(),
        size_in_bytes,
        last_checkpoint,
    })
}

impl fmt::Display for ScanPlanReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScanPlanReport(operation_id={}, table_id={:?}, correlation_id={:?}, outcome={:?}, \
             duration_ns={}, num_plan_nodes={}, \
             total_json_bytes={}, total_parquet_bytes={}, \
             replay_base={:?}, base_version={:?}, has_filter={}, read_collations={}, \
             commit_start_version={:?}, commit_end_version={:?}, \
             commit_total_size_bytes={}, compaction_file_count={}",
            self.operation_id,
            self.table_id,
            self.correlation_id,
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
