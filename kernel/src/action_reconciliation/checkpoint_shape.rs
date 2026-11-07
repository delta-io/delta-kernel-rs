//! Probes checkpoint topology for scans and resolves complete metadata for snapshot readers.
//!
//! A checkpoint leaf directly stores file actions (`add` and `remove`):
//! - An inline single-part checkpoint stores file actions in the checkpoint itself.
//! - A multipart checkpoint has one leaf per checkpoint part.
//! - A manifest checkpoint references sidecars that hold its file actions.
//!
//! The scan probe inspects the leaf schema only when requested. Full resolution retains the
//! complete leaf list and schema. Both delegate reads to a [`PlanExecutor`].

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    CHECKPOINT_METADATA_FIELD, CHECKPOINT_METADATA_NAME, CHECKPOINT_NUM_ADD_FILES_TAG,
    CheckpointMetadata, SIDECAR_FIELD, SIDECAR_FILE_SCHEMA_TAG, SIDECAR_NAME, Sidecar,
};
use crate::checkpoint::CHECKPOINT_ACTIONS_SCHEMA_V2;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, Predicate, col};
use crate::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use crate::log_segment::LogSegment;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::ir::nodes::FileType;
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructType, ToSchema as _, schema_ref,
};
use crate::{FileMeta, KernelError, KernelResult};

/// Metadata for the checkpoint selected by a snapshot.
///
/// Obtained through [`crate::snapshot::SnapshotReader::get_checkpoint_info`]. The leaf schema
/// describes files containing Add and Remove actions: sidecars for a manifest checkpoint, or
/// the checkpoint parts themselves otherwise. JSON leaves use the canonical action read schema.
/// This summarizes the selected file layout; it does not validate all required checkpoint actions.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointInfo {
    leaf_schema: SchemaRef,
    leaf_file_count: usize,
    size_in_bytes: u64,
    last_checkpoint: LastCheckpointHint,
}

impl CheckpointInfo {
    /// Returns the schema of the files holding the checkpoint's file actions.
    pub fn leaf_schema(&self) -> &SchemaRef {
        &self.leaf_schema
    }

    /// Returns the number of sidecars, or checkpoint parts for an inline checkpoint.
    pub fn leaf_file_count(&self) -> usize {
        self.leaf_file_count
    }

    /// Returns the total bytes in the checkpoint parts and all referenced sidecars.
    pub fn size_in_bytes(&self) -> u64 {
        self.size_in_bytes
    }

    /// Returns a bounded hint for this selected checkpoint, including available writer hints.
    ///
    /// The action count is -1 when unknown. The top-level checkpoint schema can be absent even
    /// when the leaf schema is known. Optional sidecar and non-file-action lists may be omitted
    /// to bound hint size; absence of either list does not indicate an empty checkpoint. Use
    /// [`Self::leaf_file_count`] and [`Self::size_in_bytes`] for exact layout totals. The checksum
    /// is absent because the hint is derived rather than a copy of the original serialized
    /// hint.
    pub fn last_checkpoint(&self) -> &LastCheckpointHint {
        &self.last_checkpoint
    }
}

/// Where a checkpoint's file actions live.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum CheckpointType {
    /// No checkpoint files.
    #[default]
    None,
    /// File actions inline: classic V1, inline V2, and multipart V1.
    Leaf,
    /// V2 manifest: checkpoint references sidecars holding the file actions.
    Manifest,
}

/// Temporary scan-planning facts, without a complete sidecar list or checkpoint summary.
#[derive(Clone, Debug, Default)]
pub(crate) struct CheckpointProbe {
    pub(crate) checkpoint_type: CheckpointType,
    pub(crate) leaf_checkpoint_schema: Option<SchemaRef>,
}

impl CheckpointProbe {
    /// Classify the selected checkpoint, inspecting its leaf schema only when requested.
    #[tracing::instrument(
        name = "checkpoint_shape.probe",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn try_new(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
        needs_leaf_schema: bool,
    ) -> KernelResult<Self> {
        let Some(root) = segment.listed.checkpoint_parts.first() else {
            return Ok(Self::default());
        };
        let file_type = root.checkpoint_file_type();
        let mut root_schema = segment.checkpoint_hint_schema();
        let first_sidecar = match segment.checkpoint_hint_sidecars() {
            Some(sidecars) => sidecars
                .first()
                .map(|sidecar| sidecar.to_filemeta(&segment.log_root))
                .transpose()?,
            None => {
                if file_type == FileType::Parquet && root_schema.is_none() {
                    root_schema = Some(read_parquet_footer_schema(exec, root.location.clone())?);
                }
                if root_schema
                    .as_ref()
                    .is_none_or(|schema| schema.contains(SIDECAR_NAME))
                {
                    collect_single_sidecar(exec, segment, &root.location, file_type)?
                } else {
                    None
                }
            }
        };
        let checkpoint_type = if first_sidecar.is_some() {
            CheckpointType::Manifest
        } else {
            CheckpointType::Leaf
        };
        let leaf_checkpoint_schema = if !needs_leaf_schema {
            None
        } else if let Some(sidecar) = first_sidecar {
            // The probe can stop before checkpointMetadata appears. Only use a retained schema
            // tag here; complete resolution can also use tags discovered from the root.
            Some(match segment.checkpoint_hint_sidecar_file_schema() {
                Some(schema) => Arc::new(schema),
                None => read_parquet_footer_schema(exec, sidecar)?,
            })
        } else if file_type == FileType::Json {
            Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone())
        } else {
            Some(match root_schema {
                Some(schema) => schema,
                None => read_parquet_footer_schema(exec, root.location.clone())?,
            })
        };
        Ok(Self {
            checkpoint_type,
            leaf_checkpoint_schema,
        })
    }
}

/// A snapshot's resolved checkpoint topology, leaf schema, and selected-checkpoint metadata.
/// Shared by metadata access and scan planning after complete resolution.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CheckpointShape {
    pub(crate) sidecars: Vec<FileMeta>,
    pub(crate) info: Option<Arc<CheckpointInfo>>,
}

impl CheckpointShape {
    pub(crate) fn checkpoint_type(&self) -> CheckpointType {
        if self.info.is_none() {
            CheckpointType::None
        } else if self.sidecars.is_empty() {
            CheckpointType::Leaf
        } else {
            CheckpointType::Manifest
        }
    }

    pub(crate) fn leaf_checkpoint_schema(&self) -> Option<&SchemaRef> {
        self.info.as_ref().map(|info| info.leaf_schema())
    }

    #[tracing::instrument(
        name = "checkpoint_shape.try_new",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn try_new(exec: &dyn PlanExecutor, segment: &LogSegment) -> KernelResult<Self> {
        let Some(root) = segment.listed.checkpoint_parts.first() else {
            return Ok(Self::default());
        };
        let file_type = root.checkpoint_file_type();
        let hint = segment.checkpoint_hint();
        let hint_metadata = hint
            .and_then(|h| h.v2_checkpoint.as_ref())
            .and_then(|v2| v2.non_file_actions.as_ref())
            .and_then(|actions| {
                actions.iter().find_map(|a| match a {
                    HintAction::CheckpointMetadata(metadata) => Some(Cow::Borrowed(metadata)),
                    _ => None,
                })
            });
        let mut root_schema = segment.checkpoint_hint_schema();
        let (sidecars, metadata) = match segment.checkpoint_hint_sidecars() {
            Some(sidecars) => (Cow::Borrowed(sidecars.as_slice()), hint_metadata),
            None => {
                if file_type == FileType::Parquet && root_schema.is_none() {
                    root_schema = Some(read_parquet_footer_schema(exec, root.location.clone())?);
                }
                let has_metadata = root_schema.as_ref().is_none_or(|schema| {
                    schema.contains(SIDECAR_NAME) || schema.contains(CHECKPOINT_METADATA_NAME)
                });
                if has_metadata {
                    let (sidecars, metadata) =
                        collect_checkpoint_metadata(exec, &root.location, file_type)?;
                    (Cow::Owned(sidecars), metadata.map(Cow::Owned))
                } else {
                    (Cow::Borrowed([].as_slice()), None)
                }
            }
        };
        if let Some(metadata) = &metadata
            && metadata.version != root.version_as_i64()?
        {
            return Err(KernelError::invalid_checkpoint(
                "Checkpoint metadata version mismatch",
            ));
        }
        let files = sidecars
            .iter()
            .map(|s| s.to_filemeta(&segment.log_root))
            .collect::<KernelResult<Vec<_>>>()?;
        let leaf_checkpoint_schema = if let Some(first) = files.first() {
            let tagged = segment
                .checkpoint_hint_sidecar_file_schema()
                .or_else(|| {
                    metadata
                        .as_ref()
                        .and_then(|m| m.tags.as_ref())
                        .and_then(|tags| tags.get(SIDECAR_FILE_SCHEMA_TAG))
                        .and_then(|json| serde_json::from_str::<StructType>(json).ok())
                })
                .map(Arc::new);
            match tagged {
                Some(schema) => schema,
                None => read_parquet_footer_schema(exec, first.clone())?,
            }
        } else if file_type == FileType::Json {
            CHECKPOINT_ACTIONS_SCHEMA_V2.clone()
        } else {
            match &root_schema {
                Some(schema) => schema.clone(),
                None => {
                    let schema = read_parquet_footer_schema(exec, root.location.clone())?;
                    root_schema = Some(schema.clone());
                    schema
                }
            }
        };
        let size_in_bytes = segment
            .listed
            .checkpoint_parts
            .iter()
            .map(|p| p.location.size)
            .chain(files.iter().map(|f| f.size))
            .try_fold(0u64, |sum, size| {
                sum.checked_add(size)
                    .ok_or_else(|| KernelError::invalid_checkpoint("Checkpoint size overflow"))
            })?;
        let leaf_file_count = if files.is_empty() {
            segment.listed.checkpoint_parts.len()
        } else {
            files.len()
        };
        let last_checkpoint = derived_last_checkpoint(
            root,
            hint,
            sidecars,
            metadata.as_deref(),
            root_schema.as_ref(),
            size_in_bytes,
        )?;
        Ok(Self {
            sidecars: files,
            info: Some(Arc::new(CheckpointInfo {
                leaf_schema: leaf_checkpoint_schema,
                leaf_file_count,
                size_in_bytes,
                last_checkpoint,
            })),
        })
    }

    #[cfg(test)]
    pub(crate) fn mock(leaf_schema: SchemaRef, sidecars: Vec<FileMeta>) -> Self {
        Self {
            info: Some(Arc::new(CheckpointInfo {
                leaf_schema,
                leaf_file_count: sidecars.len().max(1),
                size_in_bytes: sidecars.iter().map(|file| file.size).sum(),
                last_checkpoint: LastCheckpointHint::default(),
            })),
            sidecars,
        }
    }

    /// Returns `stats_schema` when the checkpoint has compatible parsed stats.
    #[cfg(test)]
    pub(crate) fn compatible_stats_parsed_schema<'a>(
        &self,
        stats_schema: &'a SchemaRef,
    ) -> Option<&'a SchemaRef> {
        self.leaf_checkpoint_schema()
            .is_some_and(|checkpoint_schema| {
                LogSegment::schema_has_compatible_stats_parsed(checkpoint_schema, stats_schema)
            })
            .then_some(stats_schema)
    }

    /// Returns `partition_schema` when the checkpoint has compatible parsed partition values.
    #[cfg(test)]
    pub(crate) fn compatible_partition_values_parsed_schema<'a>(
        &self,
        partition_schema: &'a SchemaRef,
    ) -> Option<&'a SchemaRef> {
        self.leaf_checkpoint_schema()
            .is_some_and(|checkpoint_schema| {
                LogSegment::schema_has_compatible_partition_values_parsed(
                    checkpoint_schema,
                    partition_schema,
                )
            })
            .then_some(partition_schema)
    }
}

/// Derive a portable hint without claiming that partially inspected action lists are complete.
fn derived_last_checkpoint(
    root: &ParsedLogPath,
    hint: Option<&LastCheckpointHint>,
    sidecars: Cow<'_, [Sidecar]>,
    metadata: Option<&CheckpointMetadata>,
    root_schema: Option<&SchemaRef>,
    size_in_bytes: u64,
) -> KernelResult<LastCheckpointHint> {
    let num_of_add_files = hint
        .and_then(|h| h.num_of_add_files)
        .filter(|count| *count >= 0)
        .or_else(|| {
            metadata
                .and_then(|m| m.tags.as_ref())
                .and_then(|tags| tags.get(CHECKPOINT_NUM_ADD_FILES_TAG))
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|count| *count >= 0)
        });
    // Only UUID-named checkpoints have a v2Checkpoint entry. Identity fields come from
    // the selected root, not the hint.
    let v2_checkpoint = if matches!(root.file_type, LogPathFileType::UuidCheckpoint) {
        Some(LastCheckpointV2 {
            path: root.filename.clone(),
            size_in_bytes: Some(
                i64::try_from(root.location.size)
                    .map_err(|_| KernelError::invalid_checkpoint("Checkpoint size exceeds i64"))?,
            ),
            modification_time: Some(root.location.last_modified),
            sidecar_files: Some(sidecars.into_owned()),
            // Discovery projects only checkpointMetadata and sidecars, so it cannot
            // supply a complete non-file-action list. Preserve only the retained list.
            non_file_actions: hint
                .and_then(|h| h.v2_checkpoint.as_ref())
                .and_then(|v2| v2.non_file_actions.clone()),
        })
    } else {
        None
    };
    let parts = match root.file_type {
        LogPathFileType::MultiPartCheckpoint { num_parts, .. } => Some(num_parts as usize),
        _ => None,
    };
    let mut derived = LastCheckpointHint::from_parts(
        root.version,
        hint.map_or(-1, |h| h.size),
        parts,
        Some(
            i64::try_from(size_in_bytes)
                .map_err(|_| KernelError::invalid_checkpoint("Checkpoint size exceeds i64"))?,
        ),
        num_of_add_files,
        None,
        // This is derived metadata, so the original hint's checksum no longer applies.
        None,
        hint.and_then(|h| h.tags.clone()),
        v2_checkpoint,
    )?;
    // The schema is already parsed and can be shared with the cached hint.
    derived.checkpoint_schema = root_schema.cloned();
    Ok(derived)
}

#[tracing::instrument(
    name = "checkpoint_shape.read_parquet_footer_schema",
    skip_all,
    fields(enable_call_frame),
    err
)]
fn read_parquet_footer_schema(exec: &dyn PlanExecutor, file: FileMeta) -> KernelResult<SchemaRef> {
    Ok(exec.read_parquet_footer(file)?.schema)
}

/// Finding one sidecar establishes topology, but does not establish a complete leaf list.
#[tracing::instrument(
    name = "checkpoint_shape.collect_single_sidecar",
    skip_all,
    fields(enable_call_frame),
    err
)]
fn collect_single_sidecar(
    exec: &dyn PlanExecutor,
    segment: &LogSegment,
    file: &FileMeta,
    file_type: FileType,
) -> KernelResult<Option<FileMeta>> {
    let schema = LogSegment::sidecar_read_schema();
    let plan = match file_type {
        FileType::Parquet => PlanBuilder::scan_parquet([file.clone()], &[], schema),
        FileType::Json => PlanBuilder::scan_json([file.clone()], &[], schema),
    }?
    .filter(col!(SIDECAR_NAME, "path").is_not_null())?
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;
    let mut visitor = SidecarVisitor::default();
    for batch in data {
        visitor.visit_rows_of(batch?.as_ref())?;
        if let Some(sidecar) = visitor.sidecars.first() {
            return sidecar.to_filemeta(&segment.log_root).map(Some);
        }
    }
    Ok(None)
}

#[tracing::instrument(
    name = "checkpoint_shape.collect_checkpoint_metadata",
    skip_all,
    fields(enable_call_frame),
    err
)]
fn collect_checkpoint_metadata(
    exec: &dyn PlanExecutor,
    file: &FileMeta,
    file_type: FileType,
) -> KernelResult<(Vec<Sidecar>, Option<CheckpointMetadata>)> {
    let schema = schema_ref! { (&SIDECAR_FIELD), (&CHECKPOINT_METADATA_FIELD) };
    let plan = match file_type {
        FileType::Parquet => PlanBuilder::scan_parquet([file.clone()], &[], schema),
        FileType::Json => PlanBuilder::scan_json([file.clone()], &[], schema),
    }?
    .filter(Predicate::or(
        col!(SIDECAR_NAME, "path").is_not_null(),
        col!(CHECKPOINT_METADATA_NAME, "version").is_not_null(),
    ))?
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;
    let mut sidecars = SidecarVisitor::default();
    let mut metadata = CheckpointMetadataVisitor::default();
    for batch in data {
        let batch = batch?;
        sidecars.visit_rows_of(batch.as_ref())?;
        metadata.visit_rows_of(batch.as_ref())?;
    }
    Ok((sidecars.sidecars, metadata.0))
}

#[derive(Default)]
struct CheckpointMetadataVisitor(Option<CheckpointMetadata>);

impl RowVisitor for CheckpointMetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| CheckpointMetadata::to_schema().leaves(CHECKPOINT_METADATA_NAME));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [version, tags] = getters else {
            return Err(KernelError::internal_error(
                "Expected checkpoint metadata getters",
            ));
        };
        for row in 0..row_count {
            if let Some(version) = version.get_opt(row, "checkpointMetadata.version")? {
                if self.0.is_some() {
                    return Err(KernelError::invalid_checkpoint(
                        "Duplicate checkpointMetadata action",
                    ));
                }
                self.0 = Some(CheckpointMetadata {
                    version,
                    tags: tags.get_opt(row, "checkpointMetadata.tags")?,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use rstest::rstest;

    use super::*;
    use crate::actions::{
        ADD_NAME, CheckpointMetadata, MAX_VALUES, MIN_VALUES, NUM_RECORDS, SIDECAR_FILE_SCHEMA_TAG,
        STATS_PARSED, Sidecar,
    };
    use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
    use crate::engine::sync::SyncEngine;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
    use crate::log_segment_files::LogSegmentFiles;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::{IoOperation, PlanResult};
    use crate::schema::{schema, schema_ref};
    use crate::unit_test_utils::{
        copy_test_table, create_log_path, create_log_path_with_size, load_test_table,
    };
    use crate::{KernelResult, Snapshot};

    /// Counts I/O operations and verifies that sidecar discovery queries filter out null paths.
    struct CountingExecutor {
        inner: SyncPlanExecutor,
        query_scans: AtomicUsize,
        footer_reads: AtomicUsize,
        batches_read: Arc<AtomicUsize>,
        fail_query_once: AtomicBool,
        panic_once: AtomicBool,
    }

    impl CountingExecutor {
        fn new() -> Self {
            Self {
                inner: SyncPlanExecutor::default(),
                query_scans: AtomicUsize::new(0),
                footer_reads: AtomicUsize::new(0),
                batches_read: Arc::new(AtomicUsize::new(0)),
                fail_query_once: AtomicBool::new(false),
                panic_once: AtomicBool::new(false),
            }
        }
    }

    impl PlanExecutor for CountingExecutor {
        fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
            match &op {
                Operation::QueryPlan(plan) => {
                    let predicate = plan.nodes.iter().find_map(|node| match &node.op {
                        Operator::Filter(filter) => Some(filter.predicate.as_ref()),
                        _ => None,
                    });
                    assert!(
                        predicate == Some(&col!(SIDECAR_NAME, "path").is_not_null())
                            || predicate
                                == Some(&Predicate::or(
                                    col!(SIDECAR_NAME, "path").is_not_null(),
                                    col!(CHECKPOINT_METADATA_NAME, "version").is_not_null()
                                ))
                    );
                    _ = self.query_scans.fetch_add(1, Ordering::Relaxed);
                }
                Operation::IoOperation(IoOperation::ParquetFooter { .. }) => {
                    _ = self.footer_reads.fetch_add(1, Ordering::Relaxed)
                }
                _ => {}
            }
            assert!(
                !self.panic_once.swap(false, Ordering::Relaxed),
                "injected executor panic"
            );
            match self.inner.execute_op(op)? {
                PlanResult::Data(data) => {
                    let mut rows = Vec::new();
                    for batch in data {
                        let batch = batch?.try_into_record_batch()?;
                        for row in 0..batch.num_rows() {
                            rows.push(Ok(Box::new(ArrowEngineData::new(batch.slice(row, 1)))
                                as Box<dyn crate::EngineData>));
                        }
                    }
                    if self.fail_query_once.swap(false, Ordering::Relaxed) {
                        rows.insert(2, Err(KernelError::generic("injected batch failure")));
                    }
                    let batches_read = self.batches_read.clone();
                    Ok(PlanResult::Data(Box::new(rows.into_iter().inspect(
                        move |_| {
                            batches_read.fetch_add(1, Ordering::Relaxed);
                        },
                    ))))
                }
                other => Ok(other),
            }
        }
    }

    /// Resolves checkpoint shape and, when requested, parsed-stats availability across fixtures.
    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint", CheckpointType::None, None)]
    #[case::no_checkpoint_with_stats("app-txn-no-checkpoint", CheckpointType::None, Some(false))]
    #[case::leaf_parquet("with_checkpoint_no_last_checkpoint", CheckpointType::Leaf, None)]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        CheckpointType::Manifest,
        Some(false)
    )]
    #[case::manifest_json("v2-checkpoints-json-with-sidecars", CheckpointType::Manifest, None)]
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        CheckpointType::Leaf,
        Some(false)
    )]
    // Regression: an all-null `sidecar` column is still a leaf.
    #[case::leaf_parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        CheckpointType::Leaf,
        None
    )]
    #[case::leaf_multipart("v1-multi-part-struct-stats-only", CheckpointType::Leaf, Some(true))]
    #[case::json_stats_classic(
        "v2-classic-checkpoint-parquet",
        CheckpointType::Manifest,
        Some(false)
    )]
    #[case::struct_stats_leaf(
        "v2-classic-parquet-struct-stats-only",
        CheckpointType::Leaf,
        Some(true)
    )]
    #[case::struct_stats_json_manifest(
        "v2-json-sidecars-struct-stats-only",
        CheckpointType::Manifest,
        Some(true)
    )]
    #[case::struct_stats_parquet_manifest(
        "v2-parquet-sidecars-struct-stats-only",
        CheckpointType::Manifest,
        Some(true)
    )]
    fn resolve_checkpoint_and_stats(
        #[case] table: &str,
        #[case] expected_checkpoint: CheckpointType,
        #[case] expect_parsed: Option<bool>,
    ) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = CountingExecutor::new();
        let stats_schema = expect_parsed.map(|_| probe_stats_schema());

        let shape = CheckpointShape::try_new(&exec, snapshot.log_segment()).unwrap();
        let probe =
            CheckpointProbe::try_new(&exec, snapshot.log_segment(), expect_parsed.is_some())
                .unwrap();
        assert_eq!(probe.checkpoint_type, expected_checkpoint);
        assert_eq!(
            probe.leaf_checkpoint_schema.as_ref(),
            expect_parsed.and(shape.leaf_checkpoint_schema())
        );
        let parsed_stats_schema = stats_schema
            .as_ref()
            .and_then(|stats_schema| shape.compatible_stats_parsed_schema(stats_schema));

        assert_eq!(
            shape.checkpoint_type(),
            expected_checkpoint,
            "{table}: checkpoint type"
        );

        match expect_parsed {
            // Stats not requested: no parsed-stats schema regardless of the checkpoint.
            None => assert!(
                parsed_stats_schema.is_none(),
                "{table}: stats not requested"
            ),
            // Requested with compatible parsed stats: the requested schema is echoed back.
            Some(true) => assert_eq!(
                parsed_stats_schema,
                stats_schema.as_ref(),
                "{table}: parsed stats available, schema echoed"
            ),
            // Requested but no compatible parsed stats: `None`.
            Some(false) => assert!(
                parsed_stats_schema.is_none(),
                "{table}: no compatible stats"
            ),
        }

        assert_eq!(
            shape.leaf_checkpoint_schema().is_some(),
            expected_checkpoint != CheckpointType::None,
            "{table}: checkpoint leaf schema"
        );
        if let Some(schema) = shape.leaf_checkpoint_schema() {
            assert!(schema.contains("add"), "{table}: retained add action");
            assert!(schema.contains("remove"), "{table}: retained remove action");
        }
    }

    /// Requested stats schema for the `*-struct-stats-only` fixtures (`id: long`, `value: string`),
    /// so compatibility does real per-column matching.
    fn probe_stats_schema() -> SchemaRef {
        let columns = || schema! { nullable "id": LONG, nullable "value": STRING };
        schema_ref! {
            nullable NUM_RECORDS: LONG,
            nullable MIN_VALUES: (columns()),
            nullable MAX_VALUES: (columns()),
        }
    }

    fn probe_partition_schema() -> SchemaRef {
        schema_ref! { nullable "part": INTEGER }
    }

    #[test]
    fn incompatible_parsed_stats_schema_is_rejected() {
        let (_engine, snapshot, _tempdir) =
            load_test_table("v2-classic-parquet-struct-stats-only").unwrap();
        let columns = || schema! { nullable "value": LONG };
        let incompatible = schema_ref! {
            nullable NUM_RECORDS: LONG,
            nullable MIN_VALUES: (columns()),
            nullable MAX_VALUES: (columns()),
        };

        let shape =
            CheckpointShape::try_new(&SyncPlanExecutor::default(), snapshot.log_segment()).unwrap();

        assert!(
            shape
                .compatible_stats_parsed_schema(&incompatible)
                .is_none()
        );
    }

    #[rstest]
    #[case::matching(probe_partition_schema(), true)]
    #[case::missing_column(schema_ref! {
        nullable "part": INTEGER,
        nullable "missing": STRING,
    }, true)]
    #[case::incompatible_type(schema_ref! { nullable "part": STRING }, false)]
    fn parsed_partition_values_schema_compatibility(
        #[case] partition_schema: SchemaRef,
        #[case] expected_compatible: bool,
    ) {
        let (_engine, snapshot, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only").unwrap();
        let shape =
            CheckpointShape::try_new(&SyncPlanExecutor::default(), snapshot.log_segment()).unwrap();

        assert_eq!(
            shape.compatible_partition_values_parsed_schema(&partition_schema),
            expected_compatible.then_some(&partition_schema)
        );
    }

    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars")]
    #[case::parquet("v2-checkpoints-parquet-with-sidecars")]
    fn sidecars_hint_skips_checkpoint_drain(
        #[case] table: &str,
        #[values(false, true)] omit_non_file_actions: bool,
    ) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        if omit_non_file_actions {
            segment
                .last_checkpoint_metadata
                .as_mut()
                .unwrap()
                .v2_checkpoint
                .as_mut()
                .unwrap()
                .non_file_actions = None;
        }
        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new(&exec, &segment).unwrap();
        assert_eq!(shape.checkpoint_type(), CheckpointType::Manifest);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 1);
        assert!(shape.leaf_checkpoint_schema().is_some());
    }

    #[rstest]
    #[case::json_manifest(
        "v2-checkpoints-json-with-sidecars",
        CheckpointType::Manifest,
        1,
        false
    )]
    #[case::parquet_manifest(
        "v2-parquet-sidecars-struct-stats-only",
        CheckpointType::Manifest,
        1,
        true
    )]
    #[case::classic_v1("with_checkpoint_no_last_checkpoint", CheckpointType::Leaf, 0, false)]
    #[case::parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        CheckpointType::Leaf,
        1,
        false
    )]
    #[case::json_inline("v2-checkpoints-json-without-sidecars", CheckpointType::Leaf, 1, false)]
    fn resolve_checkpoint_without_hint(
        #[case] table: &str,
        #[case] checkpoint_type: CheckpointType,
        #[case] query_scans: usize,
        #[case] expect_parsed_stats: bool,
    ) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new(&exec, &segment).unwrap();
        assert_eq!(shape.checkpoint_type(), checkpoint_type);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), query_scans);
        assert_eq!(
            shape
                .compatible_stats_parsed_schema(&probe_stats_schema())
                .is_some(),
            expect_parsed_stats
        );
        for needs_schema in [false, true] {
            let probe = CheckpointProbe::try_new(&exec, &segment, needs_schema).unwrap();
            assert_eq!(probe.checkpoint_type, checkpoint_type);
            assert_eq!(
                probe.leaf_checkpoint_schema.as_ref(),
                needs_schema
                    .then(|| shape.leaf_checkpoint_schema())
                    .flatten()
            );
        }
    }

    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars", 0)]
    #[case::parquet("v2-checkpoints-parquet-with-sidecars", 1)]
    fn scan_probe_stops_early_and_does_not_initialize_cache(
        #[case] table: &str,
        #[case] root_footer_reads: usize,
        #[values(false, true)] needs_schema: bool,
    ) {
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let snapshot =
            Arc::new(Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap());
        let exec = Arc::new(CountingExecutor::new());
        // The probe must not request the failing batch after the first sidecar reference.
        exec.fail_query_once.store(true, Ordering::Relaxed);
        let counted = crate::engine::test_delegating::DelegatingEngine::new(engine)
            .with_plan_executor(exec.clone());
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_stats(if needs_schema {
                crate::scan::StatsOptions::all()
            } else {
                crate::scan::StatsOptions::none()
            })
            .build()
            .unwrap();
        let plan = scan
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();
        assert!(
            plan.nodes
                .iter()
                .any(|node| matches!(node.op, Operator::DynamicScan(_)))
        );
        assert!(snapshot.cached_checkpoint_shape().is_none());
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            root_footer_reads + usize::from(needs_schema)
        );

        let info = snapshot
            .reader()
            .get_checkpoint_info(&counted)
            .unwrap()
            .unwrap();
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
        assert!(exec.batches_read.load(Ordering::Relaxed) > 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            2 * root_footer_reads + usize::from(needs_schema) + 1
        );
        let calls = (
            exec.query_scans.load(Ordering::Relaxed),
            exec.footer_reads.load(Ordering::Relaxed),
        );
        let cached = snapshot
            .reader()
            .get_checkpoint_info(&counted)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&info, &cached));
        let plan = scan
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        assert_eq!(
            calls,
            (
                exec.query_scans.load(Ordering::Relaxed),
                exec.footer_reads.load(Ordering::Relaxed),
            )
        );
    }

    /// Builds a `LogSegment` whose applicable hint carries `v2Checkpoint.sidecarFiles == Some([])`
    /// (empty, not absent) for a checkpoint with the given extension. No real fixture carries an
    /// empty sidecar list, so this synthetic segment is the only way to exercise the leaf fast
    /// path.
    fn segment_with_empty_sidecars_hint(extension: &str, include_metadata: bool) -> LogSegment {
        let (_store, log_root) = crate::checkpoint::tests::new_in_memory_store();
        let selected = format!(
            "00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.{extension}"
        );
        let checkpoint_file = log_root.join(&selected).unwrap().to_string();
        let commit = create_log_path(log_root.join("00000000000000000002.json").unwrap().as_str());
        LogSegment::try_new(
            LogSegmentFiles {
                checkpoint_parts: vec![create_log_path_with_size(&checkpoint_file, 1)],
                ascending_commit_files: vec![commit.clone()],
                latest_commit_file: Some(commit),
                ..Default::default()
            },
            log_root,
            None,
            Some(LastCheckpointHint {
                version: 1,
                checkpoint_schema: Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone()),
                v2_checkpoint: Some(LastCheckpointV2 {
                    path: selected,
                    sidecar_files: Some(vec![]),
                    non_file_actions: include_metadata.then(|| {
                        vec![HintAction::CheckpointMetadata(CheckpointMetadata {
                            version: 1,
                            tags: None,
                        })]
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .unwrap()
    }

    #[rstest]
    #[case::json("json")]
    #[case::parquet("parquet")]
    fn empty_sidecars_hint_classifies_leaf_without_drain(
        #[case] extension: &str,
        #[values(false, true)] include_metadata: bool,
    ) {
        let segment = segment_with_empty_sidecars_hint(extension, include_metadata);
        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new(&exec, &segment).unwrap();
        assert_eq!(shape.checkpoint_type(), CheckpointType::Leaf);
        assert_eq!(
            shape.leaf_checkpoint_schema(),
            Some(&*CHECKPOINT_ACTIONS_SCHEMA_V2)
        );
        assert_eq!(shape.info.as_ref().unwrap().leaf_file_count(), 1);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn empty_sidecars_hint_reads_and_caches_missing_parquet_schema() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-without-sidecars").unwrap();
        let mut segment = snapshot.log_segment().clone();
        let root = segment.listed.checkpoint_parts[0].clone();
        let expected_schema =
            read_parquet_footer_schema(&SyncPlanExecutor::default(), root.location.clone())
                .unwrap();
        segment.last_checkpoint_metadata = Some(LastCheckpointHint {
            version: root.version,
            size: -1,
            v2_checkpoint: Some(LastCheckpointV2 {
                path: root.filename,
                sidecar_files: Some(vec![]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let snapshot = Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap();
        let probe_exec = CountingExecutor::new();
        let probe = CheckpointProbe::try_new(&probe_exec, snapshot.log_segment(), false).unwrap();
        assert_eq!(probe.checkpoint_type, CheckpointType::Leaf);
        assert!(probe.leaf_checkpoint_schema.is_none());
        assert_eq!(probe_exec.footer_reads.load(Ordering::Relaxed), 0);
        let probe = CheckpointProbe::try_new(&probe_exec, snapshot.log_segment(), true).unwrap();
        assert_eq!(
            probe.leaf_checkpoint_schema.as_ref(),
            Some(&expected_schema)
        );
        assert_eq!(probe_exec.footer_reads.load(Ordering::Relaxed), 1);
        assert_eq!(probe_exec.query_scans.load(Ordering::Relaxed), 0);
        assert!(snapshot.cached_checkpoint_shape().is_none());
        let exec = Arc::new(CountingExecutor::new());
        let engine = crate::engine::test_delegating::DelegatingEngine::new(engine)
            .with_plan_executor(exec.clone());

        let info = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        let cached = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&info, &cached));
        assert_eq!(info.leaf_schema(), &expected_schema);
        assert_eq!(
            info.last_checkpoint().checkpoint_schema(),
            Some(&expected_schema)
        );
        assert!(Arc::ptr_eq(
            info.leaf_schema(),
            info.last_checkpoint().checkpoint_schema().unwrap()
        ));
        assert_eq!(info.leaf_file_count(), 1);
        assert_eq!(info.size_in_bytes(), root.location.size);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 1);
    }

    /// Builds a segment whose applicable hint lists one sidecar and carries a `checkpointMetadata`
    /// action with a synthetic `sidecarFileSchema` tag, exercising the footer-read short-circuit.
    /// When `compatible`, the tagged schema's `add.stats_parsed` matches [`probe_stats_schema`];
    /// otherwise `add` has no `stats_parsed` field at all, so the compatibility check fails.
    fn segment_with_manifest_hint(extension: &str, compatible: bool) -> LogSegment {
        let columns = || schema! { nullable "id": LONG, nullable "value": STRING };
        let stats_parsed = schema! {
            nullable NUM_RECORDS: LONG,
            nullable MIN_VALUES: (columns()),
            nullable MAX_VALUES: (columns()),
        };
        let add = if compatible {
            schema! { nullable STATS_PARSED: (stats_parsed) }
        } else {
            schema! { nullable "path": STRING }
        };
        let sidecar_file_schema =
            serde_json::to_string(&schema! { nullable ADD_NAME: (add) }).unwrap();
        let tags = std::collections::HashMap::from([(
            SIDECAR_FILE_SCHEMA_TAG.to_string(),
            sidecar_file_schema,
        )]);

        let (_store, log_root) = crate::checkpoint::tests::new_in_memory_store();
        let selected = format!(
            "00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.{extension}"
        );
        let checkpoint_file = log_root.join(&selected).unwrap().to_string();
        let commit = create_log_path(log_root.join("00000000000000000002.json").unwrap().as_str());
        LogSegment::try_new(
            LogSegmentFiles {
                checkpoint_parts: vec![create_log_path_with_size(&checkpoint_file, 1)],
                ascending_commit_files: vec![commit.clone()],
                latest_commit_file: Some(commit),
                ..Default::default()
            },
            log_root,
            None,
            Some(LastCheckpointHint {
                version: 1,
                v2_checkpoint: Some(LastCheckpointV2 {
                    path: selected,
                    sidecar_files: Some(vec![Sidecar {
                        path: "sidecar-1.parquet".to_string(),
                        size_in_bytes: 1,
                        modification_time: 0,
                        tags: None,
                    }]),
                    non_file_actions: Some(vec![HintAction::CheckpointMetadata(
                        CheckpointMetadata {
                            version: 1,
                            tags: Some(tags),
                        },
                    )]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .unwrap()
    }

    /// A manifest hint carrying a `sidecarFileSchema` answers the stats-compatibility question from
    /// the hint alone: the checkpoint classifies as a manifest, parsed stats are reported per the
    /// hint schema's compatibility, and no sidecar footer is read (nor is the checkpoint drained).
    #[rstest]
    #[case::compatible_parquet("parquet", true, true)]
    #[case::compatible_json("json", true, true)]
    #[case::incompatible_parquet("parquet", false, false)]
    #[case::incompatible_json("json", false, false)]
    fn manifest_hint_sidecar_file_schema_skips_footer_read(
        #[case] extension: &str,
        #[case] compatible: bool,
        #[case] expect_parsed: bool,
    ) {
        let segment = segment_with_manifest_hint(extension, compatible);
        let stats_schema = probe_stats_schema();
        let exec = CountingExecutor::new();

        let shape = CheckpointShape::try_new(&exec, &segment).unwrap();

        assert_eq!(shape.checkpoint_type(), CheckpointType::Manifest);
        let parsed_stats_schema = shape.compatible_stats_parsed_schema(&stats_schema);
        assert_eq!(parsed_stats_schema.is_some(), expect_parsed);
        if expect_parsed {
            assert_eq!(parsed_stats_schema, Some(&stats_schema));
        }
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            0,
            "sidecarFileSchema hint must skip the sidecar footer read"
        );
        assert_eq!(
            exec.query_scans.load(Ordering::Relaxed),
            0,
            "the hint fast path must not drain the checkpoint"
        );
    }

    /// The hint schema must match the sidecar's actual parquet footer, so substituting it is
    /// behavior-preserving. Uses a real table whose parquet sidecars carry compatible struct stats.
    #[test]
    fn manifest_hint_schema_matches_footer_read_result() {
        let (_engine, snapshot, _tempdir) =
            load_test_table("v2-parquet-sidecars-struct-stats-only").unwrap();
        let exec = SyncPlanExecutor::default();
        let segment = snapshot.log_segment();
        let sidecar = segment
            .checkpoint_hint_sidecars()
            .and_then(|s| s.first())
            .expect("table's hint lists sidecars")
            .to_filemeta(&segment.log_root)
            .unwrap();
        let hint_schema = segment
            .checkpoint_hint_sidecar_file_schema()
            .expect("table's hint carries the sidecar schema");
        let footer = read_parquet_footer_schema(&exec, sidecar).unwrap();
        assert_eq!(&hint_schema, footer.as_ref());
    }

    #[rstest]
    #[case::hint_wins(Some(7), Some("123"), Some(7))]
    #[case::negative_hint_uses_tag(Some(-1), Some("123"), Some(123))]
    #[case::negative_counts(Some(-1), Some("-2"), None)]
    #[case::malformed_tag(None, Some("not a count"), None)]
    #[case::missing_counts(None, None, None)]
    fn add_file_count_uses_nonnegative_hint_or_protocol_tag(
        #[case] hint_count: Option<i64>,
        #[case] tag: Option<&str>,
        #[case] expected: Option<i64>,
    ) {
        let mut segment = segment_with_manifest_hint("json", true);
        let hint = segment.last_checkpoint_metadata.as_mut().unwrap();
        hint.num_of_add_files = hint_count;
        hint.checksum = Some("original serialized hint checksum".to_owned());
        let metadata = hint
            .v2_checkpoint
            .as_mut()
            .unwrap()
            .non_file_actions
            .as_mut()
            .unwrap();
        let HintAction::CheckpointMetadata(metadata) = &mut metadata[0] else {
            panic!("fixture metadata action");
        };
        if let Some(tag) = tag {
            metadata
                .tags
                .as_mut()
                .unwrap()
                .insert(CHECKPOINT_NUM_ADD_FILES_TAG.to_owned(), tag.to_owned());
        }
        let shape = CheckpointShape::try_new(&CountingExecutor::new(), &segment).unwrap();
        let derived = shape.info.as_ref().unwrap().last_checkpoint();
        assert_eq!(derived.num_of_add_files(), expected);
        assert_eq!(derived.checksum(), None);
        assert_eq!(
            derived.v2_checkpoint().unwrap().non_file_actions(),
            segment
                .checkpoint_hint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .non_file_actions()
        );
    }

    #[test]
    fn root_schema_without_sidecars_exports_verified_empty_list() {
        let mut segment = segment_with_empty_sidecars_hint("parquet", false);
        let hint = segment.last_checkpoint_metadata.as_mut().unwrap();
        hint.v2_checkpoint.as_mut().unwrap().sidecar_files = None;
        hint.checkpoint_schema = Some(schema_ref! { nullable ADD_NAME: STRING });
        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new(&exec, &segment).unwrap();
        assert_eq!(shape.checkpoint_type(), CheckpointType::Leaf);
        assert_eq!(
            shape
                .info
                .as_ref()
                .unwrap()
                .last_checkpoint()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files(),
            Some([].as_slice())
        );
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case::manifest("v2-checkpoints-parquet-with-sidecars")]
    #[case::multipart("v1-multi-part-struct-stats-only")]
    fn snapshot_readers_share_complete_metadata(
        #[case] table: &str,
        #[values(false, true)] scan_first: bool,
    ) {
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let counted = crate::engine::test_delegating::DelegatingEngine::new(engine)
            .with_plan_executor(exec.clone());
        let cold_heap = snapshot.estimated_owned_heap_size_bytes();
        if scan_first {
            snapshot
                .clone()
                .scan_builder()
                .build()
                .unwrap()
                .declarative_metadata_scan_plan(&counted)
                .unwrap();
        }
        assert!(snapshot.cached_checkpoint_shape().is_none());
        let probe_calls = (
            exec.query_scans.load(Ordering::Relaxed),
            exec.footer_reads.load(Ordering::Relaxed),
        );
        let infos = std::thread::scope(|scope| {
            let readers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        snapshot
                            .reader()
                            .get_checkpoint_info(&counted)
                            .unwrap()
                            .unwrap()
                    })
                })
                .collect();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        for info in &infos {
            assert!(Arc::ptr_eq(info, &infos[0]));
        }
        let calls = (
            exec.query_scans.load(Ordering::Relaxed),
            exec.footer_reads.load(Ordering::Relaxed),
        );
        assert_eq!(
            (calls.0 - probe_calls.0, calls.1 - probe_calls.1),
            if table.starts_with("v2") {
                (0, 1)
            } else {
                (0, 0)
            }
        );
        let shape = snapshot.checkpoint_shape(&counted).unwrap();
        assert!(
            snapshot.estimated_owned_heap_size_bytes()
                >= cold_heap + shape.sidecars.len() * size_of::<FileMeta>()
        );

        let roots = &snapshot.log_segment().listed.checkpoint_parts;
        let expected_bytes = roots
            .iter()
            .map(|root| root.location.size)
            .chain(shape.sidecars.iter().map(|file| file.size))
            .sum::<u64>();
        assert_eq!(infos[0].size_in_bytes(), expected_bytes);
        assert_eq!(
            infos[0].leaf_file_count(),
            if shape.sidecars.is_empty() {
                roots.len()
            } else {
                shape.sidecars.len()
            }
        );
        assert_eq!(infos[0].last_checkpoint().version(), roots[0].version);
        let expected_parts = match roots[0].file_type {
            LogPathFileType::MultiPartCheckpoint { num_parts, .. } => Some(num_parts as usize),
            _ => None,
        };
        assert_eq!(infos[0].last_checkpoint().parts(), expected_parts);
        assert_eq!(
            infos[0].last_checkpoint().v2_checkpoint().is_some(),
            matches!(roots[0].file_type, LogPathFileType::UuidCheckpoint)
        );
        assert_eq!(infos[0].last_checkpoint().checksum(), None);
        assert_eq!(
            infos[0].leaf_schema(),
            shape.leaf_checkpoint_schema().unwrap()
        );
        snapshot
            .clone()
            .scan_builder()
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&counted)
            .unwrap();
        assert_eq!(
            calls,
            (
                exec.query_scans.load(Ordering::Relaxed),
                exec.footer_reads.load(Ordering::Relaxed)
            )
        );
    }

    #[test]
    fn failed_resolution_can_retry_and_new_snapshots_resolve_independently() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let missing = crate::engine::test_delegating::DelegatingEngine::new(engine.clone())
            .without_plan_executor();
        assert!(snapshot.reader().get_checkpoint_info(&missing).is_err());
        let first = snapshot
            .reader()
            .get_checkpoint_info(engine.as_ref())
            .unwrap()
            .unwrap();
        let cached = snapshot
            .reader()
            .get_checkpoint_info(&missing)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &cached));
        snapshot
            .clone()
            .scan_builder()
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&missing)
            .unwrap();
        let new_snapshot = Snapshot::new(
            snapshot.log_segment().clone(),
            snapshot.table_configuration().clone(),
        )
        .unwrap();
        assert!(new_snapshot.reader().get_checkpoint_info(&missing).is_err());
        let scan = Arc::new(new_snapshot).scan_builder().build().unwrap();
        let error = scan.declarative_metadata_scan_plan(&missing).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not provide a PlanExecutor")
        );
    }

    #[test]
    fn checkpoint_info_tracks_checkpoint_selected_by_builder_from() -> KernelResult<()> {
        let (engine, latest, _tempdir) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let old = Snapshot::builder_for(latest.table_root().clone())
            .at_version(5)
            .build(engine.as_ref())?;
        let before = old.reader().get_checkpoint_info(engine.as_ref())?.unwrap();
        assert_eq!(
            (before.last_checkpoint().version(), before.leaf_file_count()),
            (5, 1)
        );
        assert_eq!(before.size_in_bytes(), 16_101);

        let updated = Snapshot::builder_from(old.clone())
            .at_version(6)
            .build(engine.as_ref())?;
        let after = updated
            .reader()
            .get_checkpoint_info(engine.as_ref())?
            .unwrap();
        assert_eq!(
            (after.last_checkpoint().version(), after.leaf_file_count()),
            (6, 2)
        );
        assert_eq!(after.size_in_bytes(), 25_454);
        let selected = after.last_checkpoint().v2_checkpoint().unwrap();
        assert_ne!(
            before.last_checkpoint().v2_checkpoint().unwrap().path(),
            selected.path(),
        );
        let fresh = latest
            .reader()
            .get_checkpoint_info(engine.as_ref())?
            .unwrap();
        let fresh_selected = fresh.last_checkpoint().v2_checkpoint().unwrap();
        assert_eq!(after.leaf_schema(), fresh.leaf_schema());
        assert_eq!(selected.path(), fresh_selected.path());
        assert_eq!(selected.sidecar_files(), fresh_selected.sidecar_files());
        assert_eq!(
            old.reader().get_checkpoint_info(engine.as_ref())?.unwrap(),
            before
        );
        Ok(())
    }

    #[rstest]
    fn panicking_resolution_can_retry(#[values(false, true)] scan_first: bool) {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let exec = Arc::new(CountingExecutor::new());
        exec.panic_once.store(true, Ordering::Relaxed);
        let engine = crate::engine::test_delegating::DelegatingEngine::new(engine)
            .with_plan_executor(exec.clone());
        let scan = snapshot.clone().scan_builder().build().unwrap();
        let read = || {
            if scan_first {
                scan.declarative_metadata_scan_plan(&engine).map(|_| ())
            } else {
                snapshot.reader().get_checkpoint_info(&engine).map(|_| ())
            }
        };
        assert!(catch_unwind(AssertUnwindSafe(read)).is_err());
        let info = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        scan.declarative_metadata_scan_plan(&engine).unwrap();
        let cached = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&info, &cached));
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[case::duplicate(true, "Duplicate checkpointMetadata action")]
    #[case::version_mismatch(false, "Checkpoint metadata version mismatch")]
    fn invalid_checkpoint_metadata_is_rejected(
        #[case] duplicate: bool,
        #[case] expected_error: &str,
    ) {
        let (table_root, _tempdir) = copy_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let snapshot = Snapshot::builder_for(table_root)
            .build(&SyncEngine::new())
            .unwrap();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let root = &segment.listed.checkpoint_parts[0];
        let path = root.location.location.to_file_path().unwrap();
        let source = std::fs::read_to_string(&path).unwrap();
        let mut lines: Vec<serde_json::Value> = source
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let metadata = lines
            .iter_mut()
            .find_map(|action| action.get_mut(CHECKPOINT_METADATA_NAME))
            .unwrap();
        if duplicate {
            let duplicate = serde_json::json!({"checkpointMetadata": metadata.clone()});
            lines.push(duplicate);
        } else {
            metadata["version"] = serde_json::json!(root.version + 1);
        }
        std::fs::write(
            &path,
            lines
                .iter()
                .map(|action| action.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let exec = CountingExecutor::new();
        let error = CheckpointShape::try_new(&exec, &segment).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    fn checkpoint_metadata_rejects_wrong_getter_count(#[values(0, 1, 3)] count: usize) {
        let getters = vec![&() as &dyn GetData<'_>; count];
        let mut visitor = CheckpointMetadataVisitor::default();
        let error = visitor.visit(1, &getters).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Expected checkpoint metadata getters"),
            "{error}"
        );
        assert!(visitor.0.is_none());
    }

    #[rstest]
    #[case::total_u64_overflow(u64::MAX, 1, "Checkpoint size overflow")]
    #[case::root_exceeds_i64(i64::MAX as u64 + 1, 1, "Checkpoint size exceeds i64")]
    #[case::total_exceeds_i64(i64::MAX as u64, 1, "Checkpoint size exceeds i64")]
    fn checkpoint_size_overflow_is_rejected(
        #[case] root_bytes: u64,
        #[case] sidecar_bytes: i64,
        #[case] expected_error: &str,
    ) {
        let mut segment = segment_with_manifest_hint("json", true);
        segment.listed.checkpoint_parts[0].location.size = root_bytes;
        segment
            .last_checkpoint_metadata
            .as_mut()
            .unwrap()
            .v2_checkpoint
            .as_mut()
            .unwrap()
            .sidecar_files
            .as_mut()
            .unwrap()[0]
            .size_in_bytes = sidecar_bytes;
        let exec = CountingExecutor::new();
        let error = CheckpointShape::try_new(&exec, &segment).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn no_checkpoint_needs_no_executor() {
        let (engine, snapshot, _tempdir) = load_test_table("app-txn-no-checkpoint").unwrap();
        let engine =
            crate::engine::test_delegating::DelegatingEngine::new(engine).without_plan_executor();
        assert!(
            snapshot
                .reader()
                .get_checkpoint_info(&engine)
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    #[case::missing("missing")]
    #[case::stale("stale")]
    #[case::other_checkpoint_at_same_version("other")]
    fn discovered_checkpoint_metadata_supplies_counts_and_schema(
        #[case] hint_kind: &str,
        #[values(false, true)] malformed_tags: bool,
    ) {
        let (table_root, _tempdir) = copy_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
        let root = &snapshot.log_segment().listed.checkpoint_parts[0];
        let path = root.location.location.to_file_path().unwrap();
        let source = std::fs::read_to_string(&path).unwrap();
        let mut lines: Vec<serde_json::Value> = source
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let tagged_schema = schema! { nullable ADD_NAME: (schema! { nullable "path": STRING }) };
        // Exceed the portable hint's list cap and split discovery into one-row batches.
        // Only the first sidecar is probed for its schema, so the extra descriptors need no data.
        let template = lines
            .iter()
            .find(|action| action.get(SIDECAR_NAME).is_some())
            .unwrap()
            .clone();
        for index in 0..32 {
            let mut sidecar = template.clone();
            sidecar[SIDECAR_NAME]["path"] = serde_json::json!(format!("extra-{index}.parquet"));
            sidecar[SIDECAR_NAME]["sizeInBytes"] = serde_json::json!(index + 1);
            lines.push(sidecar);
        }

        for action in &mut lines {
            if let Some(metadata) = action.get_mut(CHECKPOINT_METADATA_NAME) {
                metadata["tags"] = serde_json::json!({
                    "numOfAddFiles": if malformed_tags { "-1" } else { "123" },
                    "sidecarFileSchema": if malformed_tags { "invalid schema".to_string() } else { serde_json::to_string(&tagged_schema).unwrap() },
                });
            }
        }
        let source = lines
            .iter()
            .map(|action| serde_json::to_string(action).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, source).unwrap();
        let hint_path = snapshot
            .log_segment()
            .log_root
            .join("_last_checkpoint")
            .unwrap()
            .to_file_path()
            .unwrap();
        if hint_kind != "missing" {
            let mut hint: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&hint_path).unwrap()).unwrap();
            if hint_kind == "stale" {
                hint["version"] = serde_json::json!(0);
            } else {
                hint["v2Checkpoint"]["path"] = serde_json::json!(format!(
                    "{:020}.checkpoint.11111111-1111-1111-1111-111111111111.json",
                    root.version
                ));
            }
            std::fs::write(&hint_path, serde_json::to_string(&hint).unwrap()).unwrap();
        } else {
            std::fs::remove_file(&hint_path).unwrap();
        }
        let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new(&exec, snapshot.log_segment()).unwrap();
        let info = shape.info.unwrap();
        assert_eq!(
            info.last_checkpoint().num_of_add_files(),
            (!malformed_tags).then_some(123)
        );
        assert_eq!(info.last_checkpoint().size(), -1);
        assert_eq!(info.last_checkpoint().checksum(), None);
        // A projected metadata read cannot promise a complete non-file-action list.
        assert!(
            info.last_checkpoint()
                .v2_checkpoint()
                .unwrap()
                .non_file_actions()
                .is_none()
        );
        if malformed_tags {
            let footer =
                read_parquet_footer_schema(&SyncPlanExecutor::default(), shape.sidecars[0].clone())
                    .unwrap();
            assert_eq!(info.leaf_schema(), &footer);
        } else {
            assert_eq!(info.leaf_schema().as_ref(), &tagged_schema);
        }
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            usize::from(malformed_tags)
        );
        let expected_sidecars = lines
            .iter()
            .filter(|action| action.get(SIDECAR_NAME).is_some())
            .count();
        assert_eq!(info.leaf_file_count(), expected_sidecars);
        assert_eq!(shape.sidecars.len(), expected_sidecars);
        assert!(expected_sidecars > 30);
        assert!(
            info.last_checkpoint()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files()
                .is_none()
        );
        let sidecar_bytes: u64 = lines
            .iter()
            .filter_map(|action| action.get(SIDECAR_NAME))
            .map(|sidecar| sidecar["sizeInBytes"].as_u64().unwrap())
            .sum();
        let root_bytes = std::fs::metadata(path).unwrap().len();
        assert_eq!(info.size_in_bytes(), root_bytes + sidecar_bytes);
    }

    #[test]
    fn batch_failure_does_not_cache_partial_sidecars() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let expected = snapshot
            .log_segment()
            .checkpoint_hint_sidecars()
            .unwrap()
            .clone();
        assert_eq!(expected.len(), 2);
        std::fs::remove_file(
            snapshot
                .log_segment()
                .log_root
                .join("_last_checkpoint")
                .unwrap()
                .to_file_path()
                .unwrap(),
        )
        .unwrap();
        let snapshot = Snapshot::builder_for(snapshot.table_root().clone())
            .build(engine.as_ref())
            .unwrap();
        let exec = Arc::new(CountingExecutor::new());
        exec.fail_query_once.store(true, Ordering::Relaxed);
        let engine = crate::engine::test_delegating::DelegatingEngine::new(engine)
            .with_plan_executor(exec.clone());
        assert!(
            snapshot
                .reader()
                .get_checkpoint_info(&engine)
                .unwrap_err()
                .to_string()
                .contains("injected batch failure")
        );
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), 3);
        assert!(snapshot.cached_checkpoint_shape().is_none());
        let info = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
        assert_eq!(info.leaf_file_count(), 2);
        assert_eq!(
            info.last_checkpoint()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files(),
            Some(expected.as_slice()),
        );
        let cached = snapshot
            .reader()
            .get_checkpoint_info(&engine)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&info, &cached));
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
    }
}
