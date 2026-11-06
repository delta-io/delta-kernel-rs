//! Resolves a checkpoint's layout and leaf schema for scan planning and metadata access.
//!
//! Scan planning can stop after finding a sidecar. Complete metadata resolution enumerates all
//! sidecars; only complete lists may replace manifest discovery during scan execution.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    CHECKPOINT_METADATA_FIELD, CHECKPOINT_METADATA_NAME, CHECKPOINT_NUM_ADD_FILES_TAG,
    CheckpointMetadata, SIDECAR_FIELD, SIDECAR_FILE_SCHEMA_TAG, SIDECAR_NAME, Sidecar,
};
use crate::checkpoint::{CHECKPOINT_ACTIONS_SCHEMA_V2, CheckpointShape};
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
use crate::snapshot::Snapshot;
use crate::{Engine, FileMeta, KernelError, KernelResult};

/// Topology of a checkpoint: where the `add` / `remove` actions live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointType {
    /// No checkpoint files.
    None,
    /// File actions inline. Classic V1, inline V2, and multi-part V1.
    Leaf,
    /// V2 manifest: checkpoint references sidecar files holding the file actions.
    Manifest,
}

/// How much checkpoint information the caller needs; this does not change snapshot cache state.
#[derive(Clone, Copy, PartialEq)]
enum DiscoveryMode {
    Scan { needs_leaf_schema: bool },
    Complete,
}

impl CheckpointShape {
    /// Reuses completed snapshot metadata, otherwise probes only the facts needed by this scan.
    /// Does not wait for or initialize the snapshot's complete checkpoint metadata cache.
    pub(crate) fn for_scan(
        snapshot: &Snapshot,
        engine: &dyn Engine,
        needs_leaf_schema: bool,
    ) -> KernelResult<Arc<Self>> {
        if let Some(cached) = snapshot.cached_checkpoint_info() {
            return Ok(cached.clone().unwrap_or_else(|| Arc::new(Self::empty())));
        }
        let exec = engine.require_plan_executor()?;
        Self::discover(
            exec.as_ref(),
            snapshot.log_segment(),
            DiscoveryMode::Scan { needs_leaf_schema },
        )
        .map(|shape| Arc::new(shape.unwrap_or_else(Self::empty)))
    }

    /// Resolves the selected checkpoint, retaining its complete layout alongside portable fields.
    #[tracing::instrument(
        name = "checkpoint_info.resolve",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn resolve(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
    ) -> KernelResult<Option<Self>> {
        Self::discover(exec, segment, DiscoveryMode::Complete)
    }

    fn empty() -> Self {
        Self {
            checkpoint_type: CheckpointType::None,
            leaf_checkpoint_schema: None,
            sidecars: None,
            last_checkpoint: None,
        }
    }

    /// Resolve the selected checkpoint from an applicable hint or its files. A scan stops after
    /// the first sidecar batch; complete discovery also collects and validates checkpoint metadata.
    #[tracing::instrument(
        name = "checkpoint_shape.resolve",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    fn discover(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
        mode: DiscoveryMode,
    ) -> KernelResult<Option<Self>> {
        let Some(root) = segment.listed.checkpoint_parts.first() else {
            return Ok(None);
        };
        let file_type = if root.is_json() {
            FileType::Json
        } else {
            FileType::Parquet
        };
        let complete = mode == DiscoveryMode::Complete;
        let needs_leaf_schema = match mode {
            DiscoveryMode::Scan { needs_leaf_schema } => needs_leaf_schema,
            DiscoveryMode::Complete => true,
        };
        let hint_metadata = segment
            .checkpoint_hint()
            .and_then(|hint| hint.v2_checkpoint.as_ref())
            .and_then(|v2| v2.non_file_actions.as_ref())
            .and_then(|actions| {
                actions.iter().find_map(|action| match action {
                    HintAction::CheckpointMetadata(metadata) => Some(Cow::Borrowed(metadata)),
                    _ => None,
                })
            });
        let mut root_schema = segment.checkpoint_hint_schema();
        let (sidecars, metadata, complete_sidecars) = match segment.checkpoint_hint_sidecars() {
            Some(sidecars) => (Cow::Borrowed(sidecars.as_slice()), hint_metadata, true),
            None => {
                if file_type == FileType::Parquet && root_schema.is_none() {
                    root_schema = Some(read_parquet_footer_schema(exec, root.location.clone())?);
                }
                let needs_root_read = (file_type == FileType::Json && !complete)
                    || root_schema.as_ref().is_none_or(|schema| {
                        schema.contains(SIDECAR_NAME)
                            || (complete && schema.contains(CHECKPOINT_METADATA_NAME))
                    });
                if needs_root_read {
                    let (sidecars, metadata) =
                        collect_checkpoint_metadata(exec, &root.location, file_type, mode)?;
                    // A shallow probe returns immediately on finding a sidecar. Even if its batch
                    // contains all sidecars, only reaching EOF proves the list complete.
                    let complete_sidecars = complete || sidecars.is_empty();
                    (
                        Cow::Owned(sidecars),
                        metadata.map(Cow::Owned),
                        complete_sidecars,
                    )
                } else {
                    (Cow::Borrowed([].as_slice()), None, true)
                }
            }
        };
        if complete
            && let Some(metadata) = &metadata
            && metadata.version != root.version_as_i64()?
        {
            return Err(KernelError::invalid_checkpoint(
                "Checkpoint metadata version mismatch",
            ));
        }
        let files = complete_sidecars
            .then(|| {
                sidecars
                    .iter()
                    .map(|sidecar| sidecar.to_filemeta(&segment.log_root))
                    .collect::<KernelResult<Arc<[FileMeta]>>>()
            })
            .transpose()?;
        let first = match &files {
            Some(files) => files.first().cloned(),
            None => sidecars
                .first()
                .map(|sidecar| sidecar.to_filemeta(&segment.log_root))
                .transpose()?,
        };
        let checkpoint_type = if first.is_some() {
            CheckpointType::Manifest
        } else {
            CheckpointType::Leaf
        };
        let leaf_checkpoint_schema = if !needs_leaf_schema {
            None
        } else if let Some(first) = first {
            let tagged = segment.checkpoint_hint_sidecar_file_schema().or_else(|| {
                metadata
                    .as_ref()
                    .and_then(|m| m.tags.as_ref())
                    .and_then(|tags| tags.get(SIDECAR_FILE_SCHEMA_TAG))
                    .and_then(|json| serde_json::from_str::<StructType>(json).ok())
            });
            Some(match tagged {
                Some(schema) => Arc::new(schema),
                None => read_parquet_footer_schema(exec, first)?,
            })
        } else if file_type == FileType::Json {
            Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone())
        } else {
            if root_schema.is_none() {
                root_schema = Some(read_parquet_footer_schema(exec, root.location.clone())?);
            }
            root_schema.clone()
        };
        let last_checkpoint = if complete {
            let size_in_bytes = segment
                .listed
                .checkpoint_parts
                .iter()
                .map(|part| part.location.size)
                .chain(
                    files
                        .iter()
                        .flat_map(|files| files.iter())
                        .map(|file| file.size),
                )
                .try_fold(0u64, |sum, size| {
                    sum.checked_add(size)
                        .ok_or_else(|| KernelError::invalid_checkpoint("Checkpoint size overflow"))
                })?;
            Some(Box::new(derived_last_checkpoint(
                root,
                segment.checkpoint_hint(),
                sidecars,
                metadata.as_deref(),
                root_schema.as_ref(),
                size_in_bytes,
            )?))
        } else {
            None
        };
        Ok(Some(Self {
            checkpoint_type,
            leaf_checkpoint_schema,
            sidecars: files,
            last_checkpoint,
        }))
    }
}

/// Derive a portable hint from complete sidecars, retaining the existing hint's `non_file_actions`.
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
    // Existing _last_checkpoint readers interpret v2Checkpoint as UUID-named identity.
    // Classic-named checkpoints can still contain V2 actions and reference sidecars.
    // Identity fields come from the selected root, not the hint.
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
    mode: DiscoveryMode,
) -> KernelResult<(Vec<Sidecar>, Option<CheckpointMetadata>)> {
    let complete = mode == DiscoveryMode::Complete;
    let schema = if complete {
        schema_ref! { (&SIDECAR_FIELD), (&CHECKPOINT_METADATA_FIELD) }
    } else {
        LogSegment::sidecar_read_schema()
    };
    let predicate = if complete {
        Predicate::or(
            col!(SIDECAR_NAME, "path").is_not_null(),
            col!(CHECKPOINT_METADATA_NAME, "version").is_not_null(),
        )
    } else {
        col!(SIDECAR_NAME, "path").is_not_null()
    };
    let plan = match file_type {
        FileType::Parquet => PlanBuilder::scan_parquet([file.clone()], &[], schema),
        FileType::Json => PlanBuilder::scan_json([file.clone()], &[], schema),
    }?
    .filter(predicate)?
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;
    let mut sidecars = SidecarVisitor::default();
    let mut metadata = CheckpointMetadataVisitor::default();
    for batch in data {
        let batch = batch?;
        sidecars.visit_rows_of(batch.as_ref())?;
        if complete {
            metadata.visit_rows_of(batch.as_ref())?;
        } else if !sidecars.sidecars.is_empty() {
            break;
        }
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
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use rstest::rstest;

    use super::*;
    use crate::actions::{
        ADD_NAME, CHECKPOINT_METADATA_NAME, CHECKPOINT_NUM_ADD_FILES_TAG, CheckpointMetadata,
        MAX_VALUES, MIN_VALUES, NUM_RECORDS, SIDECAR_FILE_SCHEMA_TAG, STATS_PARSED, Sidecar,
    };
    use crate::checkpoint::CheckpointKind;
    use crate::engine::DelegatingEngine;
    use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
    use crate::engine::sync::SyncEngine;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::expressions::Predicate;
    use crate::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
    use crate::log_segment_files::LogSegmentFiles;
    use crate::path::LogPathFileType;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::{IoOperation, PlanResult};
    use crate::schema::{schema, schema_ref};
    use crate::unit_test_utils::{
        copy_test_table, create_log_path, create_log_path_with_size, load_test_table,
    };
    use crate::{Engine, KernelError, KernelResult, Result, Snapshot};

    /// Counts discovery I/O and batches, checks query filters, and can split batches or inject
    /// a one-shot batch failure or executor panic.
    struct CountingExecutor {
        inner: SyncPlanExecutor,
        query_scans: AtomicUsize,
        footer_reads: AtomicUsize,
        batches_read: Arc<AtomicUsize>,
        rows_in_batch: Option<usize>,
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
                rows_in_batch: Some(1),
                fail_query_once: AtomicBool::new(false),
                panic_once: AtomicBool::new(false),
            }
        }

        /// Returns checkpoint-query and Parquet-footer read counts.
        fn io_counts(&self) -> (usize, usize) {
            (
                self.query_scans.load(Ordering::Relaxed),
                self.footer_reads.load(Ordering::Relaxed),
            )
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
                    let mut batches = Vec::new();
                    for batch in data {
                        let batch = batch?;
                        if let Some(rows_in_batch) = self.rows_in_batch {
                            let batch = batch.try_into_record_batch()?;
                            for row in (0..batch.num_rows()).step_by(rows_in_batch) {
                                let len = rows_in_batch.min(batch.num_rows() - row);
                                batches.push(Ok(Box::new(ArrowEngineData::new(
                                    batch.slice(row, len),
                                ))
                                    as Box<dyn crate::EngineData>));
                            }
                        } else {
                            batches.push(Ok(batch));
                        }
                    }
                    if self.fail_query_once.swap(false, Ordering::Relaxed) {
                        assert!(batches.len() >= 2, "failure injection needs two batches");
                        batches.insert(2, Err(KernelError::generic("injected batch failure")));
                    }
                    let batches_read = self.batches_read.clone();
                    Ok(PlanResult::Data(Box::new(batches.into_iter().inspect(
                        move |_| {
                            batches_read.fetch_add(1, Ordering::Relaxed);
                        },
                    ))))
                }
                other => Ok(other),
            }
        }
    }

    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint", CheckpointType::None, false, 0)]
    #[case::leaf_parquet("with_checkpoint_no_last_checkpoint", CheckpointType::Leaf, false, 0)]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        CheckpointType::Manifest,
        false,
        1
    )]
    #[case::manifest_json(
        "v2-checkpoints-json-with-sidecars",
        CheckpointType::Manifest,
        false,
        1
    )]
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        CheckpointType::Leaf,
        false,
        1
    )]
    // Regression: an all-null `sidecar` column is still a leaf.
    #[case::leaf_parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        CheckpointType::Leaf,
        false,
        1
    )]
    #[case::leaf_multipart("v1-multi-part-struct-stats-only", CheckpointType::Leaf, true, 0)]
    #[case::json_stats_classic("v2-classic-checkpoint-parquet", CheckpointType::Manifest, false, 1)]
    #[case::struct_stats_leaf(
        "v2-classic-parquet-struct-stats-only",
        CheckpointType::Leaf,
        true,
        0
    )]
    #[case::struct_stats_json_manifest(
        "v2-json-sidecars-struct-stats-only",
        CheckpointType::Manifest,
        true,
        1
    )]
    #[case::struct_stats_parquet_manifest(
        "v2-parquet-sidecars-struct-stats-only",
        CheckpointType::Manifest,
        true,
        1
    )]
    fn resolve_checkpoint_and_stats(
        #[case] table: &str,
        #[case] expected_checkpoint: CheckpointType,
        #[case] expect_parsed: bool,
        #[case] unhinted_query_scans: usize,
        #[values(false, true)] omit_hint: bool,
        #[values(false, true)] needs_schema: bool,
    ) {
        // Given: a checkpoint layout with or without a hint and an optional schema request.
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        if omit_hint {
            segment.last_checkpoint_metadata = None;
        }
        let snapshot = Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap();
        let exec = CountingExecutor::new();

        // When: scan discovery inspects the requested checkpoint facts.
        let shape = CheckpointShape::discover(
            &exec,
            snapshot.log_segment(),
            DiscoveryMode::Scan {
                needs_leaf_schema: needs_schema,
            },
        )
        .unwrap()
        .unwrap_or_else(CheckpointShape::empty);

        // Then: only requested, compatible metadata is retained.
        assert_eq!(
            shape.checkpoint_type, expected_checkpoint,
            "{table}: checkpoint type"
        );

        assert_eq!(
            shape.leaf_checkpoint_schema.is_some(),
            needs_schema && expected_checkpoint != CheckpointType::None,
            "{table}: checkpoint leaf schema"
        );
        if let Some(schema) = &shape.leaf_checkpoint_schema {
            assert!(schema.contains("add"), "{table}: retained add action");
            assert!(schema.contains("remove"), "{table}: retained remove action");
            assert_eq!(
                LogSegment::schema_has_compatible_stats_parsed(schema, &probe_stats_schema()),
                expect_parsed,
                "{table}: parsed stats compatibility"
            );
        }

        // When: full resolution independently discovers all checkpoint metadata.
        let exec = CountingExecutor::new();
        let resolved = CheckpointShape::resolve(&exec, snapshot.log_segment()).unwrap();

        // Then: it agrees with the probe and retains the schema even when the scan did not need it.
        assert_eq!(
            resolved.as_ref().map(|info| info.checkpoint_type),
            (expected_checkpoint != CheckpointType::None).then_some(expected_checkpoint),
        );
        assert_eq!(
            resolved
                .as_ref()
                .and_then(CheckpointShape::leaf_schema)
                .is_some(),
            expected_checkpoint != CheckpointType::None,
        );
        if let Some(schema) = &shape.leaf_checkpoint_schema {
            assert_eq!(
                resolved.as_ref().and_then(CheckpointShape::leaf_schema),
                Some(schema)
            );
        }
        if let Some(schema) = resolved.as_ref().and_then(CheckpointShape::leaf_schema) {
            assert_eq!(
                LogSegment::schema_has_compatible_stats_parsed(schema, &probe_stats_schema()),
                expect_parsed
            );
        }
        if omit_hint {
            assert_eq!(
                exec.query_scans.load(Ordering::Relaxed),
                unhinted_query_scans
            );
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
    fn classic_named_manifest_keeps_layout_without_v2_hint() {
        // Given: a classic-named Parquet checkpoint referencing one sidecar.
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-classic-checkpoint-parquet").unwrap();
        let root = &snapshot.log_segment().listed.checkpoint_parts[0];
        assert_eq!(root.file_type, LogPathFileType::ClassicCheckpoint);

        // When: the public getter resolves the selected checkpoint.
        let info = resolve_checkpoint_info(&snapshot, engine.as_ref());

        // Then: its manifest layout is explicit even though the portable hint has no V2 entry.
        assert_eq!(info.kind().unwrap(), CheckpointKind::Manifest);
        assert_eq!(info.leaf_file_count().unwrap(), 1);
        assert_eq!(info.size_in_bytes().unwrap(), 18_768 + 9_363);
        assert_eq!(info.last_checkpoint().unwrap().version(), 1);
        assert!(info.last_checkpoint().unwrap().v2_checkpoint().is_none());
        assert!(info.last_checkpoint().unwrap().parts().is_none());
        let shape = snapshot.checkpoint_info(engine.as_ref()).unwrap().unwrap();
        let sidecar_schema = read_parquet_footer_schema(
            &SyncPlanExecutor::default(),
            shape.sidecars.as_ref().unwrap()[0].clone(),
        )
        .unwrap();
        assert_eq!(info.leaf_schema().unwrap(), &sidecar_schema);
    }

    #[test]
    fn incompatible_parsed_stats_schema_is_rejected() {
        // Given: a requested stats column whose type differs from the checkpoint.
        let (_engine, snapshot, _tempdir) =
            load_test_table("v2-classic-parquet-struct-stats-only").unwrap();
        let columns = || schema! { nullable "value": LONG };
        let incompatible = schema_ref! {
            nullable NUM_RECORDS: LONG,
            nullable MIN_VALUES: (columns()),
            nullable MAX_VALUES: (columns()),
        };

        // When: the checkpoint schema is resolved.
        let shape = CheckpointShape::resolve(&SyncPlanExecutor::default(), snapshot.log_segment())
            .unwrap()
            .unwrap();

        // Then: incompatible parsed statistics are unavailable.
        assert!(!LogSegment::schema_has_compatible_stats_parsed(
            shape.leaf_checkpoint_schema.as_ref().unwrap(),
            &incompatible,
        ));
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
        // Given: a partitioned checkpoint and a requested partition schema.
        let (_engine, snapshot, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only").unwrap();
        // When: its parsed partition schema is resolved.
        let shape = CheckpointShape::resolve(&SyncPlanExecutor::default(), snapshot.log_segment())
            .unwrap()
            .unwrap();

        // Then: missing columns are allowed, but incompatible column types are rejected.
        assert_eq!(
            LogSegment::schema_has_compatible_partition_values_parsed(
                shape.leaf_checkpoint_schema.as_ref().unwrap(),
                &partition_schema,
            ),
            expected_compatible
        );
    }

    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars")]
    #[case::parquet("v2-checkpoints-parquet-with-sidecars")]
    fn sidecars_hint_skips_checkpoint_drain(
        #[case] table: &str,
        #[values(false, true)] omit_non_file_actions: bool,
    ) {
        // Given: a complete sidecar hint, with or without non-file actions.
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

        // When: checkpoint metadata is resolved from the hint.
        let shape = CheckpointShape::resolve(&exec, &segment).unwrap().unwrap();

        // Then: only the missing sidecar schema requires I/O.
        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 1);
        assert!(shape.leaf_schema().is_some());

        // When: a scan needs only the layout from the same hint.
        let snapshot = Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap();
        let probe = CheckpointShape::discover(
            &exec,
            snapshot.log_segment(),
            DiscoveryMode::Scan {
                needs_leaf_schema: false,
            },
        )
        .unwrap()
        .unwrap();

        // Then: it carries every hinted sidecar without requesting another schema or caching.
        assert_eq!(
            probe.sidecars.as_deref(),
            Some(shape.sidecars.as_ref().unwrap().as_ref())
        );
        assert!(probe.leaf_checkpoint_schema.is_none());
        assert_eq!(exec.io_counts(), (0, 1));
        assert!(snapshot.cached_checkpoint_info().is_none());
    }

    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars", 0)]
    #[case::parquet("v2-checkpoints-parquet-with-sidecars", 1)]
    fn scan_probe_stops_early_and_does_not_initialize_cache(
        #[case] table: &str,
        #[case] root_footer_reads: usize,
        #[values(false, true)] needs_schema: bool,
    ) {
        // Given: an unhinted manifest with a failure after its first sidecar reference.
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let snapshot =
            Arc::new(Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap());
        let exec = Arc::new(CountingExecutor::new());
        exec.fail_query_once.store(true, Ordering::Relaxed);
        let counted = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
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
        // When: a scan plans with or without a requested leaf schema.
        let plan = scan
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();
        // Then: it stops before the failure and leaves complete metadata unresolved.
        assert!(
            plan.nodes
                .iter()
                .any(|node| matches!(node.op, Operator::DynamicScan(_)))
        );
        assert!(snapshot.cached_checkpoint_info().is_none());
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            root_footer_reads + usize::from(needs_schema)
        );

        // When: the getter resolves the complete manifest independently.
        let info = resolve_checkpoint_info(&snapshot, &counted);

        // Then: it drains the manifest and reads the schema missing from the hint.
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
        assert!(exec.batches_read.load(Ordering::Relaxed) > 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            2 * root_footer_reads + usize::from(needs_schema) + 1
        );
        let calls = exec.io_counts();

        // When: another getter and scan reuse the completed resolution.
        let cached = resolve_checkpoint_info(&snapshot, &counted);
        let plan = scan
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();

        // Then: the getter shares its value and the scan uses static files without more I/O.
        assert!(Arc::ptr_eq(&info, &cached));
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        assert_eq!(exec.io_counts(), calls);
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
        #[values(false, true)] omit_sidecar_list: bool,
    ) {
        // Given: an empty sidecar list, or a root schema proving that no sidecars exist.
        let mut segment = segment_with_empty_sidecars_hint(extension, include_metadata);
        if omit_sidecar_list {
            let hint = segment.last_checkpoint_metadata.as_mut().unwrap();
            hint.v2_checkpoint.as_mut().unwrap().sidecar_files = None;
            hint.checkpoint_schema = Some(schema_ref! { nullable ADD_NAME: STRING });
        }
        let expected_schema = if extension == "json" {
            CHECKPOINT_ACTIONS_SCHEMA_V2.clone()
        } else {
            segment.checkpoint_hint_schema().unwrap()
        };
        let exec = CountingExecutor::new();

        // When: full metadata is resolved.
        let shape = CheckpointShape::resolve(&exec, &segment).unwrap().unwrap();

        // Then: the checkpoint is a leaf and neither its root nor a footer is read.
        assert_eq!(shape.checkpoint_type, CheckpointType::Leaf);
        assert_eq!(shape.leaf_schema(), Some(&expected_schema));
        assert_eq!(shape.leaf_file_count().unwrap(), 1);
        assert_eq!(
            shape
                .last_checkpoint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files(),
            Some([].as_slice())
        );
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn empty_sidecars_hint_reads_and_caches_missing_parquet_schema() {
        // Given: an inline Parquet checkpoint whose empty-sidecar hint omits its schema.
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

        // When / Then: probing without a schema request leaves the footer unread.
        let probe = CheckpointShape::discover(
            &probe_exec,
            snapshot.log_segment(),
            DiscoveryMode::Scan {
                needs_leaf_schema: false,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(probe.checkpoint_type, CheckpointType::Leaf);
        assert!(probe.leaf_checkpoint_schema.is_none());
        assert_eq!(probe_exec.footer_reads.load(Ordering::Relaxed), 0);
        // When / Then: requesting the schema reads one footer without populating the cache.
        let probe = CheckpointShape::discover(
            &probe_exec,
            snapshot.log_segment(),
            DiscoveryMode::Scan {
                needs_leaf_schema: true,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            probe.leaf_checkpoint_schema.as_ref(),
            Some(&expected_schema)
        );
        assert_eq!(probe_exec.footer_reads.load(Ordering::Relaxed), 1);
        assert_eq!(probe_exec.query_scans.load(Ordering::Relaxed), 0);
        assert!(snapshot.cached_checkpoint_info().is_none());
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());

        // When: repeated getters resolve the complete checkpoint.
        let info = resolve_checkpoint_info(&snapshot, &engine);
        let cached = resolve_checkpoint_info(&snapshot, &engine);

        // Then: schema, totals, and the derived hint share one cached footer read.
        assert!(Arc::ptr_eq(&info, &cached));
        assert_eq!(info.leaf_schema().unwrap(), &expected_schema);
        assert_eq!(
            info.last_checkpoint().unwrap().checkpoint_schema(),
            Some(&expected_schema)
        );
        assert!(Arc::ptr_eq(
            info.leaf_schema().unwrap(),
            info.last_checkpoint().unwrap().checkpoint_schema().unwrap()
        ));
        assert_eq!(info.leaf_file_count().unwrap(), 1);
        assert_eq!(
            info.size_in_bytes(),
            Some(i64::try_from(root.location.size).unwrap())
        );
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

    #[rstest]
    #[case::parquet("parquet")]
    #[case::json("json")]
    fn manifest_hint_sidecar_file_schema_skips_footer_read(
        #[case] extension: &str,
        #[values(false, true)] compatible: bool,
    ) {
        // Given: a complete manifest hint with a compatible or incompatible sidecar schema.
        let segment = segment_with_manifest_hint(extension, compatible);
        let stats_schema = probe_stats_schema();
        let exec = CountingExecutor::new();

        // When: metadata is resolved using that hint.
        let shape = CheckpointShape::resolve(&exec, &segment).unwrap().unwrap();

        // Then: stats compatibility follows the hinted schema without any I/O.
        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert_eq!(
            LogSegment::schema_has_compatible_stats_parsed(
                shape.leaf_schema().unwrap(),
                &stats_schema,
            ),
            compatible
        );
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

    #[test]
    fn manifest_hint_schema_matches_footer_read_result() {
        // Given: a real sidecar and its hinted schema.
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
        // When: the actual Parquet footer is read.
        let footer = read_parquet_footer_schema(&exec, sidecar).unwrap();

        // Then: the hint is a faithful substitute for the footer schema.
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
        // Given: a writer hint and checkpoint tag with varying count validity.
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
        // When: selected-checkpoint metadata is derived.
        let resolved = CheckpointShape::resolve(&CountingExecutor::new(), &segment)
            .unwrap()
            .unwrap();
        let derived = resolved.last_checkpoint().unwrap();

        // Then: a valid hint wins; otherwise the tag is used, and serialized checksums are dropped.
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

    #[rstest]
    #[case::manifest("v2-checkpoints-parquet-with-sidecars")]
    #[case::multipart("v1-multi-part-struct-stats-only")]
    fn snapshot_readers_share_complete_metadata(
        #[case] table: &str,
        #[values(false, true)] scan_first: bool,
    ) {
        // Given: an unresolved snapshot, optionally preceded by scan planning.
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let counted = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
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
        assert!(snapshot.cached_checkpoint_info().is_none());
        let probe_calls = exec.io_counts();

        // When: concurrent readers request complete metadata from the same snapshot.
        let infos = std::thread::scope(|scope| {
            let readers: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| resolve_checkpoint_info(&snapshot, &counted)))
                .collect();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });

        // Then: all readers share one result and pay for only one full resolution.
        for info in &infos {
            assert!(Arc::ptr_eq(info, &infos[0]));
        }
        let info = &infos[0];
        let calls = exec.io_counts();
        assert_eq!(
            (calls.0 - probe_calls.0, calls.1 - probe_calls.1),
            if table.starts_with("v2") {
                (0, 1)
            } else {
                (0, 0)
            }
        );
        let shape = CheckpointShape::for_scan(&snapshot, &counted, false).unwrap();
        assert!(Arc::ptr_eq(info, &shape));
        assert!(
            snapshot.estimated_owned_heap_size_bytes()
                >= cold_heap + shape.sidecars.as_ref().unwrap().len() * size_of::<FileMeta>()
        );

        let roots = &snapshot.log_segment().listed.checkpoint_parts;
        let expected_bytes = roots
            .iter()
            .map(|root| root.location.size)
            .chain(
                shape
                    .sidecars
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|file| file.size),
            )
            .sum::<u64>();
        assert_eq!(
            info.size_in_bytes(),
            Some(i64::try_from(expected_bytes).unwrap())
        );
        assert_eq!(
            info.leaf_file_count().unwrap(),
            if shape.sidecars.as_ref().unwrap().is_empty() {
                roots.len()
            } else {
                shape.sidecars.as_ref().unwrap().len()
            }
        );
        assert_eq!(info.last_checkpoint().unwrap().version(), roots[0].version);
        let expected_parts = match roots[0].file_type {
            LogPathFileType::MultiPartCheckpoint { num_parts, .. } => Some(num_parts as usize),
            _ => None,
        };
        assert_eq!(info.last_checkpoint().unwrap().parts(), expected_parts);
        assert_eq!(
            info.last_checkpoint().unwrap().v2_checkpoint().is_some(),
            matches!(roots[0].file_type, LogPathFileType::UuidCheckpoint)
        );
        assert_eq!(info.last_checkpoint().unwrap().checksum(), None);
        assert_eq!(info.leaf_schema().unwrap(), shape.leaf_schema().unwrap());

        // When / Then: another scan reuses those facts without additional I/O.
        snapshot
            .clone()
            .scan_builder()
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&counted)
            .unwrap();
        assert_eq!(exec.io_counts(), calls);
    }

    #[test]
    fn failed_resolution_can_retry_and_new_snapshots_resolve_independently() {
        // Given: a snapshot whose first getter has no executor available.
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let missing = DelegatingEngine::new(engine.clone()).without_plan_executor();
        assert!(snapshot.reader().get_checkpoint_info(&missing).is_err());

        // When: a working executor retries the getter.
        let first = resolve_checkpoint_info(&snapshot, engine.as_ref());

        // Then: readers and scans can reuse the cache even without an executor.
        let cached = resolve_checkpoint_info(&snapshot, &missing);
        assert!(Arc::ptr_eq(&first, &cached));
        snapshot
            .clone()
            .scan_builder()
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&missing)
            .unwrap();
        // When: another snapshot is constructed from the same log segment.
        let new_snapshot = Snapshot::new(
            snapshot.log_segment().clone(),
            snapshot.table_configuration().clone(),
        )
        .unwrap();

        // Then: neither its getter nor its scan inherits the original cache.
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
    fn checkpoint_info_tracks_checkpoint_selected_by_builder_from() -> Result<()> {
        // Given: resolved checkpoint metadata for an older table version.
        let (engine, latest, _tempdir) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let old = Snapshot::builder_for(latest.table_root().clone())
            .at_version(5)
            .build(engine.as_ref())?;
        let before = old.reader().get_checkpoint_info(engine.as_ref())?.unwrap();
        assert_eq!(
            (
                before.last_checkpoint().unwrap().version(),
                before.leaf_file_count().unwrap()
            ),
            (5, 1)
        );
        assert_eq!(before.size_in_bytes().unwrap(), 16_101);

        // When: incremental construction selects a newer checkpoint.
        let updated = Snapshot::builder_from(old.clone())
            .at_version(6)
            .build(engine.as_ref())?;
        let after = updated
            .reader()
            .get_checkpoint_info(engine.as_ref())?
            .unwrap();

        // Then: the new layout agrees with a fresh load, while the old snapshot stays unchanged.
        assert_eq!(
            (
                after.last_checkpoint().unwrap().version(),
                after.leaf_file_count().unwrap()
            ),
            (6, 2)
        );
        assert_eq!(after.size_in_bytes().unwrap(), 25_454);
        let selected = after.last_checkpoint().unwrap().v2_checkpoint().unwrap();
        assert_ne!(
            before
                .last_checkpoint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .path(),
            selected.path(),
        );
        let fresh = latest
            .reader()
            .get_checkpoint_info(engine.as_ref())?
            .unwrap();
        let fresh_selected = fresh.last_checkpoint().unwrap().v2_checkpoint().unwrap();
        assert_eq!(after.leaf_schema().unwrap(), fresh.leaf_schema().unwrap());
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
        // Given: an executor that panics once, during either a scan probe or full resolution.
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let exec = Arc::new(CountingExecutor::new());
        exec.panic_once.store(true, Ordering::Relaxed);
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        let scan = snapshot.clone().scan_builder().build().unwrap();
        let read = || {
            if scan_first {
                scan.declarative_metadata_scan_plan(&engine).map(|_| ())
            } else {
                snapshot.reader().get_checkpoint_info(&engine).map(|_| ())
            }
        };
        // When: the first access panics and the getter retries.
        assert!(catch_unwind(AssertUnwindSafe(read)).is_err());
        let info = resolve_checkpoint_info(&snapshot, &engine);
        scan.declarative_metadata_scan_plan(&engine).unwrap();
        let cached = resolve_checkpoint_info(&snapshot, &engine);

        // Then: initialization recovers and the successful value is cached.
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
        // Given: an unhinted root with duplicate metadata or a mismatched metadata version.
        let (table_root, _tempdir) = copy_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let snapshot = Snapshot::builder_for(table_root)
            .build(&SyncEngine::new())
            .unwrap();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let root = &segment.listed.checkpoint_parts[0];
        let path = root.location.location.to_file_path().unwrap();
        let mut lines = read_checkpoint_actions(&path);
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
        write_checkpoint_actions(&path, &lines);
        let exec = CountingExecutor::new();

        // When: discovery reads the malformed metadata.
        let error = CheckpointShape::resolve(&exec, &segment).unwrap_err();

        // Then: resolution reports the specific metadata error.
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[case::total_u64_overflow(u64::MAX, 1, "Checkpoint size overflow")]
    #[case::root_exceeds_i64(i64::MAX as u64 + 1, 1, "Checkpoint size exceeds i64")]
    #[case::total_exceeds_i64(i64::MAX as u64, 1, "Checkpoint size exceeds i64")]
    #[case::negative_sidecar_size(1, -1, "sidecar size -1")]
    fn invalid_checkpoint_sizes_are_rejected_without_io_or_caching(
        #[case] root_bytes: u64,
        #[case] sidecar_bytes: i64,
        #[case] expected_error: &str,
    ) {
        // Given: hinted file sizes that are negative or exceed the individual or combined limit.
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
        let (engine, original, _tempdir) =
            load_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let snapshot = Snapshot::new(segment, original.table_configuration().clone()).unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());

        for _ in 0..2 {
            // When: the getter attempts resolution, including after a previous failure.
            let error = snapshot.reader().get_checkpoint_info(&engine).unwrap_err();

            // Then: the specific error is returned before I/O and no result is cached.
            if sidecar_bytes < 0 {
                assert!(matches!(error, KernelError::Generic(_)), "{error}");
            } else {
                assert!(
                    matches!(error, KernelError::InvalidCheckpoint(_)),
                    "{error}"
                );
            }
            assert!(error.to_string().contains(expected_error), "{error}");
            assert_eq!(exec.io_counts(), (0, 0));
            assert!(snapshot.cached_checkpoint_info().is_none());
        }
    }

    #[test]
    fn no_checkpoint_needs_no_executor() {
        // Given: a snapshot with no checkpoint and no executor.
        let (engine, snapshot, _tempdir) = load_test_table("app-txn-no-checkpoint").unwrap();
        let engine = DelegatingEngine::new(engine).without_plan_executor();
        // When: its checkpoint metadata is requested.
        let info = snapshot.reader().get_checkpoint_info(&engine).unwrap();

        // Then: absence is returned without requiring an executor.
        assert!(info.is_none());
        assert!(matches!(snapshot.cached_checkpoint_info(), Some(None)));
    }

    #[rstest]
    #[case::missing("missing")]
    #[case::stale("stale")]
    #[case::other_checkpoint_at_same_version("other")]
    fn discovered_checkpoint_metadata_supplies_counts_and_schema(
        #[case] hint_kind: &str,
        #[values(false, true)] malformed_tags: bool,
        #[values(None, Some(1), Some(2))] rows_in_batch: Option<usize>,
    ) {
        // Given: a manifest larger than the portable hint cap, with an unusable writer hint.
        let (table_root, _tempdir) = copy_test_table("v2-checkpoints-json-with-sidecars").unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
        let root = &snapshot.log_segment().listed.checkpoint_parts[0];
        let path = root.location.location.to_file_path().unwrap();
        let mut lines = read_checkpoint_actions(&path);
        let tagged_schema = schema! { nullable ADD_NAME: (schema! { nullable "path": STRING }) };
        // Exceed the portable hint's list cap to distinguish complete layout from bounded hints.
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
        write_checkpoint_actions(&path, &lines);
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
        let exec = Arc::new(CountingExecutor {
            rows_in_batch,
            ..CountingExecutor::new()
        });
        let counted = DelegatingEngine::new(Arc::new(engine)).with_plan_executor(exec.clone());

        // When: the getter drains the root and derives its summary from checkpoint actions.
        let info = resolve_checkpoint_info(&snapshot, &counted);
        let shape = snapshot.cached_checkpoint_info().unwrap().as_ref().unwrap();

        // Then: valid tags supply counts and schema; malformed tags fall back to the footer.
        assert_eq!(
            info.last_checkpoint().unwrap().num_of_add_files(),
            (!malformed_tags).then_some(123)
        );
        assert_eq!(info.last_checkpoint().unwrap().size(), -1);
        assert_eq!(info.last_checkpoint().unwrap().checksum(), None);
        // A projected metadata read cannot promise a complete non-file-action list.
        assert!(
            info.last_checkpoint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .non_file_actions()
                .is_none()
        );
        if malformed_tags {
            let footer = read_parquet_footer_schema(
                &SyncPlanExecutor::default(),
                shape.sidecars.as_ref().unwrap()[0].clone(),
            )
            .unwrap();
            assert_eq!(info.leaf_schema().unwrap(), &footer);
        } else {
            assert_eq!(info.leaf_schema().unwrap().as_ref(), &tagged_schema);
        }
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            usize::from(malformed_tags)
        );
        // Then: bounded lists may be absent while the retained layout and totals remain exact.
        let expected_sidecars = lines
            .iter()
            .filter(|action| action.get(SIDECAR_NAME).is_some())
            .count();
        // Exercise metadata and sidecars together, separately, and across smaller mixed batches.
        assert_eq!(
            exec.batches_read.load(Ordering::Relaxed),
            rows_in_batch.map_or(1, |rows| (expected_sidecars + 1).div_ceil(rows))
        );
        assert_eq!(info.leaf_file_count().unwrap(), expected_sidecars);
        assert_eq!(info.kind().unwrap(), CheckpointKind::Manifest);
        assert_eq!(shape.sidecars.as_ref().unwrap().len(), expected_sidecars);
        assert!(expected_sidecars > 30);
        assert!(
            info.last_checkpoint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files()
                .is_none()
        );
        let portable = snapshot.to_snapshot_hint().unwrap();
        let parsed: LastCheckpointHint = serde_json::from_value(
            serde_json::to_value(portable.last_checkpoint_hint().unwrap()).unwrap(),
        )
        .unwrap();
        assert!(parsed.v2_checkpoint().unwrap().sidecar_files().is_none());
        assert_eq!(parsed.size_in_bytes(), info.size_in_bytes());
        let sidecar_bytes: u64 = lines
            .iter()
            .filter_map(|action| action.get(SIDECAR_NAME))
            .map(|sidecar| sidecar["sizeInBytes"].as_u64().unwrap())
            .sum();
        let root_bytes = std::fs::metadata(path).unwrap().len();
        assert_eq!(
            info.size_in_bytes(),
            Some(i64::try_from(root_bytes + sidecar_bytes).unwrap())
        );

        // When: a subsequent scan plans from the cache whose exported hint omitted the list.
        let calls = exec.io_counts();
        let plan = snapshot
            .clone()
            .scan_builder()
            .with_stats(crate::scan::StatsOptions::none())
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();

        // Then: every sidecar is retained in the static plan, without rediscovery or footer reads.
        let planned_files: Vec<_> = plan
            .nodes
            .iter()
            .flat_map(|node| match &node.op {
                Operator::ScanParquet(scan) => scan.files.iter().map(|file| &file.meta).collect(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(
            planned_files,
            shape.sidecars.as_ref().unwrap().iter().collect::<Vec<_>>()
        );
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        assert_eq!(exec.io_counts(), calls);
    }

    #[test]
    fn batch_failure_does_not_cache_partial_sidecars() {
        // Given: an unhinted manifest whose reader fails after consuming one sidecar.
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
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());

        // When / Then: the partial read fails without publishing a cached result.
        assert!(
            snapshot
                .reader()
                .get_checkpoint_info(&engine)
                .unwrap_err()
                .to_string()
                .contains("injected batch failure")
        );
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), 3);
        assert!(snapshot.cached_checkpoint_info().is_none());
        // When: a second getter retries discovery.
        let info = resolve_checkpoint_info(&snapshot, &engine);

        // Then: it retains the complete sidecar list, and subsequent getters perform no I/O.
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
        assert_eq!(info.leaf_file_count().unwrap(), 2);
        assert_eq!(
            info.last_checkpoint()
                .unwrap()
                .v2_checkpoint()
                .unwrap()
                .sidecar_files(),
            Some(expected.as_slice()),
        );
        let cached = resolve_checkpoint_info(&snapshot, &engine);
        assert!(Arc::ptr_eq(&info, &cached));
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
    }

    fn resolve_checkpoint_info(snapshot: &Snapshot, engine: &dyn Engine) -> Arc<CheckpointShape> {
        snapshot
            .reader()
            .get_checkpoint_info(engine)
            .unwrap()
            .unwrap()
    }

    fn read_checkpoint_actions(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn write_checkpoint_actions(path: &Path, actions: &[serde_json::Value]) {
        let content = actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path, content).unwrap();
    }
    #[rstest]
    fn checkpoint_metadata_rejects_wrong_getter_count(#[values(0, 1, 3)] count: usize) {
        // Given: a metadata batch with the wrong number of getters.
        let getters = vec![&() as &dyn GetData<'_>; count];
        let mut visitor = CheckpointMetadataVisitor::default();

        // When: the visitor attempts to consume it.
        let error = visitor.visit(1, &getters).unwrap_err();

        // Then: validation fails before retaining metadata.
        assert!(
            error
                .to_string()
                .contains("Expected checkpoint metadata getters"),
            "{error}"
        );
        assert!(visitor.0.is_none());
    }
}
