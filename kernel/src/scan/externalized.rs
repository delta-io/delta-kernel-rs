//! Request-local metadata planning from unchanged state exported by a source snapshot.

use delta_kernel_derive::internal_api;
use url::Url;

use super::scan_plan::{stats_skipping_predicate, MetadataScanPlan};
use super::{PartitionValuesOptions, PhysicalPredicate, StatsOptions};
use crate::actions::{Metadata, Protocol};
use crate::checkpoint::CheckpointShape;
use crate::expressions::Scalar;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::ir::nodes::ScanFile;
use crate::plans::ir::plan::Plan;
use crate::scan::state_info::StateInfo;
use crate::schema::SchemaRef;
use crate::snapshot::PublicationWatermark;
use crate::table_configuration::TableConfiguration;
use crate::table_features::Operation;
use crate::{Engine, KernelError, KernelResult, LogPath, Version};

/// Complete planning inputs exported by a validated source snapshot from the same Kernel build.
///
/// The caller must preserve every field unchanged. This is not a substitute for a snapshot hint
/// containing connector-authored state. All fields are consumed and dropped within one request;
/// the returned plan owns its own inputs.
#[internal_api]
pub(crate) struct SourceSnapshotScanState {
    /// Source table URL.
    pub table_root: Url,
    /// Source snapshot version.
    pub version: Version,
    /// Explicit publication observation exported by the source.
    pub publication_watermark: PublicationWatermark,
    /// Complete retained log paths.
    pub log_paths: Vec<LogPath>,
    /// Source protocol.
    pub protocol: Protocol,
    /// Source metadata; the schema is supplied separately in `logical_schema`.
    pub metadata: Metadata,
    /// Complete logical schema, including field metadata.
    pub logical_schema: SchemaRef,
    /// Matching checkpoint hint, if exported.
    pub last_checkpoint: Option<LastCheckpointHint>,
}

/// Builds a default metadata plan from complete, unchanged source-snapshot components.
///
/// Decodes each path once, constructs the complete schema/configuration, and checks current
/// table/protocol and scan-operation support. Source-proven log-segment validation is not repeated.
/// Returns an error for invalid configuration or unsupported scans. The engine may inspect
/// checkpoint metadata; this method does not execute the returned plan.
#[internal_api]
pub(crate) fn declarative_metadata_plan(
    state: SourceSnapshotScanState,
    engine: &dyn Engine,
) -> KernelResult<Option<Plan>> {
    let (segment, commits) = log_inputs(
        &state.table_root,
        state.version,
        state.publication_watermark,
        state.log_paths,
        state.last_checkpoint,
    )?;
    let configuration = TableConfiguration::try_new_from_schema(
        state.metadata,
        state.protocol,
        state.table_root,
        state.version,
        state.logical_schema,
    )?;
    configuration.ensure_operation_supported(Operation::Scan)?;
    let schema = configuration.logical_schema();
    if schema.num_fields() == 0 {
        return Err(KernelError::generic(
            "Cannot scan Delta table with empty schema; use ALTER TABLE ADD COLUMN \
             to add at least one column before scanning",
        ));
    }
    let stats = StatsOptions::default();
    let partitions = PartitionValuesOptions::default();
    let executor = engine.require_plan_executor()?;
    // With no projection, predicate, parsed stats, or parsed partitions, ordinary fields do not
    // affect this metadata plan. The full schema has still been materialized and validated.
    if schema.metadata_columns().next().is_none() {
        let shape = CheckpointShape::try_new_for_segment(executor.as_ref(), &segment, false)?;
        return MetadataScanPlan {
            log_segment: &segment,
            skip_all: false,
            pruning_predicate: None,
            physical_stats_read_schema: None,
            physical_stats_output_schema: None,
            physical_partition_schema: None,
            stats: &stats,
            partition_values: &partitions,
        }
        .build(&shape, Some(commits));
    }
    let info = StateInfo::try_new(
        schema.clone(),
        schema.clone(),
        &configuration,
        None,
        &stats,
        &partitions,
        (),
    )?;
    let needs_leaf =
        info.physical_stats_read_schema().is_some() || info.physical_partition_schema.is_some();
    let shape = CheckpointShape::try_new_for_segment(executor.as_ref(), &segment, needs_leaf)?;
    MetadataScanPlan {
        log_segment: &segment,
        skip_all: info.physical_predicate == PhysicalPredicate::StaticSkipAll,
        pruning_predicate: stats_skipping_predicate(&info),
        physical_stats_read_schema: info.physical_stats_read_schema(),
        physical_stats_output_schema: info.physical_stats_output_schema(),
        physical_partition_schema: info.physical_partition_schema.as_ref(),
        stats: &stats,
        partition_values: &partitions,
    }
    .build(&shape, Some(commits))
}

fn log_inputs(
    table_root: &Url,
    version: Version,
    watermark: PublicationWatermark,
    paths: Vec<LogPath>,
    last_checkpoint: Option<LastCheckpointHint>,
) -> KernelResult<(LogSegment, Vec<ScanFile>)> {
    let mut commits = Vec::with_capacity(paths.len());
    let mut other = Vec::new();
    for path in paths {
        let path = ParsedLogPath::from(path);
        if path.is_commit() {
            commits.push(path);
        } else {
            other.push(path);
        }
    }
    // Export orders paths by URL; staged and published URLs need not be in version order.
    commits.sort_unstable_by_key(|path| path.version);
    let mut listed = LogSegmentFiles::build_log_segment_files(
        other.into_iter().map(Ok),
        Vec::new(),
        0,
        None,
        CheckpointHandling::Adopt,
    )?;
    let checkpoint_version = listed.checkpoint_parts.first().map(|path| path.version);
    listed.max_published_version = match watermark {
        PublicationWatermark::InferFromLogPaths => commits
            .iter()
            .filter(|path| path.file_type == LogPathFileType::Commit)
            .map(|path| path.version)
            .max(),
        PublicationWatermark::NoPublishedCommits => None,
        PublicationWatermark::PublishedThrough(version) => Some(version),
    };
    listed.latest_commit_file = commits
        .iter()
        .rev()
        .find(|path| checkpoint_version.is_none_or(|version| path.version >= version))
        .cloned();
    let mut files = Vec::with_capacity(commits.len());
    for path in commits.into_iter().rev() {
        if checkpoint_version.is_some_and(|version| path.version <= version) {
            continue;
        }
        let version = path.version_as_i64()?;
        files.push(ScanFile {
            meta: path.location,
            file_constants: vec![Scalar::Long(version)],
        });
    }
    Ok((
        LogSegment {
            end_version: version,
            checkpoint_version,
            log_root: table_root.join("_delta_log/")?,
            listed,
            last_checkpoint_metadata: last_checkpoint,
        },
        files,
    ))
}
