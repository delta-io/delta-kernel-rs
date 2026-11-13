//! Resolves a checkpoint's layout and leaf schema for scan planning and metadata access.
//!
//! Scan planning and metadata access share snapshot-cached discovery of the complete sidecar list,
//! leaf schema, and portable hint.

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
use crate::{FileMeta, KernelError, KernelResult};

impl CheckpointShape {
    /// Resolves the selected checkpoint's complete layout, leaf schema, and portable hint.
    #[tracing::instrument(
        name = "checkpoint_shape.resolve",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn resolve(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
    ) -> KernelResult<Option<Self>> {
        let Some(root) = segment.listed.checkpoint_parts.first() else {
            return Ok(None);
        };
        let file_type = if root.is_json() {
            FileType::Json
        } else {
            FileType::Parquet
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
        let (sidecars, metadata) = match segment.checkpoint_hint_sidecars() {
            Some(sidecars) => (Cow::Borrowed(sidecars.as_slice()), hint_metadata),
            None => {
                if file_type == FileType::Parquet && root_schema.is_none() {
                    root_schema = Some(read_parquet_footer_schema(exec, root.location.clone())?);
                }
                let needs_root_read = root_schema.as_ref().is_none_or(|schema| {
                    schema.contains(SIDECAR_NAME) || schema.contains(CHECKPOINT_METADATA_NAME)
                });
                if needs_root_read {
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
            .map(|sidecar| sidecar.to_filemeta(&segment.log_root))
            .collect::<KernelResult<Arc<[FileMeta]>>>()?;
        let leaf_checkpoint_schema = if let Some(first) = files.first() {
            let tagged = segment.checkpoint_hint_sidecar_file_schema().or_else(|| {
                metadata
                    .as_ref()
                    .and_then(|m| m.tags.as_ref())
                    .and_then(|tags| tags.get(SIDECAR_FILE_SCHEMA_TAG))
                    .and_then(|json| serde_json::from_str::<StructType>(json).ok())
            });
            match tagged {
                Some(schema) => Arc::new(schema),
                None => read_parquet_footer_schema(exec, first.clone())?,
            }
        } else if file_type == FileType::Json {
            CHECKPOINT_ACTIONS_SCHEMA_V2.clone()
        } else {
            let schema = match root_schema {
                Some(ref schema) => schema.clone(),
                None => read_parquet_footer_schema(exec, root.location.clone())?,
            };
            root_schema = Some(schema.clone());
            schema
        };
        let size_in_bytes = segment
            .listed
            .checkpoint_parts
            .iter()
            .map(|part| part.location.size)
            .chain(files.iter().map(|file| file.size))
            .try_fold(0u64, |sum, size| {
                sum.checked_add(size)
                    .ok_or_else(|| KernelError::invalid_checkpoint("Checkpoint size overflow"))
            })?;
        let last_checkpoint = Box::new(derived_last_checkpoint(
            root,
            segment.checkpoint_hint(),
            sidecars,
            metadata.as_deref(),
            root_schema.as_ref(),
            size_in_bytes,
        )?);
        Ok(Some(Self {
            leaf_checkpoint_schema,
            sidecars: files,
            size_in_bytes,
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
) -> KernelResult<(Vec<Sidecar>, Option<CheckpointMetadata>)> {
    let schema = schema_ref! { (&SIDECAR_FIELD), (&CHECKPOINT_METADATA_FIELD) };
    let predicate = Predicate::or(
        col!(SIDECAR_NAME, "path").is_not_null(),
        col!(CHECKPOINT_METADATA_NAME, "version").is_not_null(),
    );
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
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;

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
        footer_pause: Option<(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>)>,
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
                footer_pause: None,
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
                    assert_eq!(
                        predicate,
                        Some(&Predicate::or(
                            col!(SIDECAR_NAME, "path").is_not_null(),
                            col!(CHECKPOINT_METADATA_NAME, "version").is_not_null(),
                        ))
                    );
                    _ = self.query_scans.fetch_add(1, Ordering::Relaxed);
                }
                Operation::IoOperation(IoOperation::ParquetFooter { .. }) => {
                    _ = self.footer_reads.fetch_add(1, Ordering::Relaxed);
                    if let Some((entered, release)) = &self.footer_pause {
                        entered.send(()).unwrap();
                        release
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(10))
                            .unwrap();
                    }
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
    #[case::no_checkpoint("app-txn-no-checkpoint", None, false, 0)]
    #[case::leaf_parquet(
        "with_checkpoint_no_last_checkpoint",
        Some(CheckpointKind::Inline),
        false,
        0
    )]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        Some(CheckpointKind::Manifest),
        false,
        1
    )]
    #[case::manifest_json(
        "v2-checkpoints-json-with-sidecars",
        Some(CheckpointKind::Manifest),
        false,
        1
    )]
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        Some(CheckpointKind::Inline),
        false,
        1
    )]
    // Regression: an all-null `sidecar` column is still a leaf.
    #[case::leaf_parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        Some(CheckpointKind::Inline),
        false,
        1
    )]
    #[case::leaf_multipart(
        "v1-multi-part-struct-stats-only",
        Some(CheckpointKind::Inline),
        true,
        0
    )]
    #[case::json_stats_classic(
        "v2-classic-checkpoint-parquet",
        Some(CheckpointKind::Manifest),
        false,
        1
    )]
    #[case::struct_stats_leaf(
        "v2-classic-parquet-struct-stats-only",
        Some(CheckpointKind::Inline),
        true,
        0
    )]
    #[case::struct_stats_json_manifest(
        "v2-json-sidecars-struct-stats-only",
        Some(CheckpointKind::Manifest),
        true,
        1
    )]
    #[case::struct_stats_parquet_manifest(
        "v2-parquet-sidecars-struct-stats-only",
        Some(CheckpointKind::Manifest),
        true,
        1
    )]
    fn resolve_checkpoint_and_stats(
        #[case] table: &str,
        #[case] expected_checkpoint: Option<CheckpointKind>,
        #[case] expect_parsed: bool,
        #[case] unhinted_query_scans: usize,
        #[values(false, true)] omit_hint: bool,
    ) {
        // Given: a checkpoint layout with or without a matching hint.
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let mut segment = snapshot.log_segment().clone();
        if omit_hint {
            segment.last_checkpoint_metadata = None;
        }
        let exec = CountingExecutor::new();

        // When: the shared resolver discovers the complete checkpoint.
        let resolved = CheckpointShape::resolve(&exec, &segment).unwrap();

        // Then: absence stays explicit; selected checkpoints always include their leaf schema.
        assert_eq!(
            resolved.as_ref().map(CheckpointShape::kind),
            expected_checkpoint
        );
        if let Some(shape) = resolved {
            let schema = shape.leaf_schema();
            assert!(schema.contains("add"), "{table}: retained add action");
            assert!(schema.contains("remove"), "{table}: retained remove action");
            assert_eq!(
                LogSegment::schema_has_compatible_stats_parsed(schema, &probe_stats_schema()),
                expect_parsed,
                "{table}: parsed stats compatibility"
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
        assert_eq!(info.kind(), CheckpointKind::Manifest);
        assert_eq!(info.leaf_file_count(), 1);
        assert_eq!(info.size_in_bytes(), 18_768 + 9_363);
        assert_eq!(info.last_checkpoint().version(), 1);
        assert!(info.last_checkpoint().v2_checkpoint().is_none());
        assert!(info.last_checkpoint().parts().is_none());
        let shape = snapshot.checkpoint_info(engine.as_ref()).unwrap().unwrap();
        let sidecar_schema = read_parquet_footer_schema(
            &SyncPlanExecutor::default(),
            shape.sidecars.as_ref()[0].clone(),
        )
        .unwrap();
        assert_eq!(info.leaf_schema(), &sidecar_schema);
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
            shape.leaf_checkpoint_schema.as_ref(),
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
                shape.leaf_checkpoint_schema.as_ref(),
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
        assert_eq!(shape.kind(), CheckpointKind::Manifest);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 1);
        assert!(!shape.sidecars.is_empty());
    }

    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars", 0)]
    #[case::parquet("v2-checkpoints-parquet-with-sidecars", 1)]
    fn scan_caches_complete_metadata_for_getters_and_later_scans(
        #[case] table: &str,
        #[case] root_footer_reads: usize,
        #[values(false, true)] include_stats: bool,
    ) {
        // Given: an unhinted manifest and a scan with or without requested statistics.
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let expected: Vec<_> = snapshot
            .log_segment()
            .checkpoint_hint_sidecars()
            .unwrap()
            .iter()
            .map(|sidecar| {
                sidecar
                    .to_filemeta(&snapshot.log_segment().log_root)
                    .unwrap()
            })
            .collect();
        let mut segment = snapshot.log_segment().clone();
        segment.last_checkpoint_metadata = None;
        let snapshot =
            Arc::new(Snapshot::new(segment, snapshot.table_configuration().clone()).unwrap());
        let exec = Arc::new(CountingExecutor::new());
        let counted = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_stats(if include_stats {
                crate::scan::StatsOptions::all()
            } else {
                crate::scan::StatsOptions::none()
            })
            .build()
            .unwrap();

        // When: scan planning resolves the checkpoint.
        let plan = scan
            .declarative_metadata_scan_plan(&counted)
            .unwrap()
            .unwrap();

        // Then: every sidecar is planned statically, even when statistics are disabled.
        let planned: Vec<_> = plan
            .nodes
            .iter()
            .flat_map(|node| match &node.op {
                Operator::ScanParquet(scan) => scan.files.iter().map(|file| &file.meta).collect(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(planned, expected.iter().collect::<Vec<_>>());
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        let cached = snapshot.cached_checkpoint_info().unwrap().as_ref().unwrap();
        assert_eq!(exec.io_counts(), (1, root_footer_reads + 1));
        let batches = exec.batches_read.load(Ordering::Relaxed);
        assert!(batches > 1);

        // When: the getter and another scan request the same checkpoint metadata.
        let info = resolve_checkpoint_info(&snapshot, &counted);
        scan.declarative_metadata_scan_plan(&counted).unwrap();

        // Then: the cached result is shared and no further discovery is performed.
        assert!(Arc::ptr_eq(&info, cached));
        assert_eq!(exec.io_counts(), (1, root_footer_reads + 1));
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), batches);
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
            None,
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
        assert_eq!(shape.kind(), CheckpointKind::Inline);
        assert_eq!(shape.leaf_schema(), &expected_schema);
        assert_eq!(shape.leaf_file_count(), 1);
        assert_eq!(
            shape
                .last_checkpoint()
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
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());

        // When: repeated getters resolve the complete checkpoint.
        let info = resolve_checkpoint_info(&snapshot, &engine);
        let cached = resolve_checkpoint_info(&snapshot, &engine);

        // Then: schema, totals, and the derived hint share one cached footer read.
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
            None,
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
        assert_eq!(shape.kind(), CheckpointKind::Manifest);
        assert_eq!(
            LogSegment::schema_has_compatible_stats_parsed(shape.leaf_schema(), &stats_schema,),
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
        let derived = resolved.last_checkpoint();

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
    fn concurrent_checkpoint_access_waits_for_complete_result(
        #[values(false, true)] waiting_scan: bool,
    ) {
        // Given: a getter paused inside its footer read while holding initialization.
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let timeout = Duration::from_secs(10);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let exec = Arc::new(CountingExecutor {
            footer_pause: Some((entered_tx, Mutex::new(release_rx))),
            ..CountingExecutor::new()
        });
        let counted = DelegatingEngine::new(engine.clone()).with_plan_executor(exec.clone());
        let without_executor = DelegatingEngine::new(engine).without_plan_executor();
        let scan = snapshot.clone().scan_builder().build().unwrap();

        std::thread::scope(|scope| {
            let first = scope.spawn(|| snapshot.reader().get_checkpoint_info(&counted));
            entered_rx.recv_timeout(timeout).unwrap();

            // When: a scan or another getter enters without an executor during initialization.
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let snapshot = snapshot.as_ref();
            let waiting_engine = &without_executor;
            scope.spawn(move || {
                started_tx.send(()).unwrap();
                let result = (|| {
                    if waiting_scan {
                        assert!(
                            scan.declarative_metadata_scan_plan(waiting_engine)?
                                .is_some()
                        );
                    }
                    snapshot.reader().get_checkpoint_info(waiting_engine)
                })();
                done_tx.send(result).unwrap();
            });
            started_rx.recv_timeout(timeout).unwrap();
            let before_release = done_rx.recv_timeout(Duration::from_millis(100));
            release_tx.send(()).unwrap();

            // Then: it waits, rechecks the cache, and shares the result without an executor.
            assert!(matches!(
                before_release,
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            let first = first.join().unwrap().unwrap().unwrap();
            let second = done_rx.recv_timeout(timeout).unwrap().unwrap().unwrap();
            assert!(Arc::ptr_eq(&first, &second));
        });
        assert_eq!(exec.io_counts(), (0, 1));
    }

    #[rstest]
    #[case::manifest("v2-checkpoints-parquet-with-sidecars")]
    #[case::multipart("v1-multi-part-struct-stats-only")]
    fn snapshot_caches_complete_checkpoint_metadata(#[case] table: &str) {
        // Given: an unresolved checkpoint with manifest or multipart layout.
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let counted = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        let cold_heap = snapshot.estimated_owned_heap_size_bytes();

        // When: a getter initializes metadata, followed by another reader.
        let info = resolve_checkpoint_info(&snapshot, &counted);
        let cached = resolve_checkpoint_info(&snapshot, &counted);

        // Then: readers share one complete result, whose allocations count toward snapshot size.
        assert!(Arc::ptr_eq(&info, &cached));
        let calls = exec.io_counts();
        assert_eq!(
            calls,
            if table.starts_with("v2") {
                (0, 1)
            } else {
                (0, 0)
            }
        );
        assert!(
            snapshot.estimated_owned_heap_size_bytes()
                >= cold_heap + std::mem::size_of_val(info.sidecars.as_ref())
        );

        let roots = &snapshot.log_segment().listed.checkpoint_parts;
        let expected_bytes = roots
            .iter()
            .map(|root| root.location.size)
            .chain(info.sidecars.iter().map(|file| file.size))
            .sum::<u64>();
        assert_eq!(info.size_in_bytes(), expected_bytes);
        assert_eq!(
            info.leaf_file_count(),
            if info.sidecars.is_empty() {
                roots.len()
            } else {
                info.sidecars.len()
            }
        );
        assert_eq!(info.last_checkpoint().version(), roots[0].version);
        let expected_parts = match roots[0].file_type {
            LogPathFileType::MultiPartCheckpoint { num_parts, .. } => Some(num_parts as usize),
            _ => None,
        };
        assert_eq!(info.last_checkpoint().parts(), expected_parts);
        assert_eq!(
            info.last_checkpoint().v2_checkpoint().is_some(),
            matches!(roots[0].file_type, LogPathFileType::UuidCheckpoint)
        );
        assert_eq!(info.last_checkpoint().checksum(), None);

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
            (before.last_checkpoint().version(), before.leaf_file_count()),
            (5, 1)
        );
        assert_eq!(before.size_in_bytes(), 16_101);

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
        // Given: an executor that panics once, during either scan planning or a getter.
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
        #[values(false, true)] scan_first: bool,
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
        let snapshot =
            Arc::new(Snapshot::new(segment, original.table_configuration().clone()).unwrap());
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        let scan = snapshot.clone().scan_builder().build().unwrap();

        for _ in 0..2 {
            // When: scan planning or a getter attempts resolution, including after a failure.
            let error = if scan_first {
                scan.declarative_metadata_scan_plan(&engine).unwrap_err()
            } else {
                snapshot.reader().get_checkpoint_info(&engine).unwrap_err()
            };

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
        let scan = snapshot.clone().scan_builder().build().unwrap();

        // When: scan planning resolves checkpoint absence without an executor.
        assert!(
            scan.declarative_metadata_scan_plan(&engine)
                .unwrap()
                .is_some()
        );
        assert!(matches!(snapshot.cached_checkpoint_info(), Some(None)));

        // Then: the getter shares the cached absence without requiring an executor.
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
            let footer = read_parquet_footer_schema(
                &SyncPlanExecutor::default(),
                shape.sidecars.as_ref()[0].clone(),
            )
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
        assert_eq!(info.leaf_file_count(), expected_sidecars);
        assert_eq!(info.kind(), CheckpointKind::Manifest);
        assert_eq!(shape.sidecars.as_ref().len(), expected_sidecars);
        assert!(expected_sidecars > 30);
        assert!(
            info.last_checkpoint()
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
        assert_eq!(
            parsed.size_in_bytes(),
            Some(i64::try_from(info.size_in_bytes()).unwrap())
        );
        let sidecar_bytes: u64 = lines
            .iter()
            .filter_map(|action| action.get(SIDECAR_NAME))
            .map(|sidecar| sidecar["sizeInBytes"].as_u64().unwrap())
            .sum();
        let root_bytes = std::fs::metadata(path).unwrap().len();
        assert_eq!(info.size_in_bytes(), root_bytes + sidecar_bytes);

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
            shape.sidecars.as_ref().iter().collect::<Vec<_>>()
        );
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        assert_eq!(exec.io_counts(), calls);
    }

    #[rstest]
    fn batch_failure_does_not_cache_partial_sidecars(#[values(false, true)] scan_first: bool) {
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

        // When: either scan planning or a getter encounters the failure after one sidecar.
        let error = if scan_first {
            snapshot
                .clone()
                .scan_builder()
                .build()
                .unwrap()
                .declarative_metadata_scan_plan(&engine)
                .unwrap_err()
        } else {
            snapshot.reader().get_checkpoint_info(&engine).unwrap_err()
        };

        // Then: the partial result is discarded, so the getter can retry the complete read.
        assert!(error.to_string().contains("injected batch failure"));
        assert_eq!(exec.batches_read.load(Ordering::Relaxed), 3);
        assert!(snapshot.cached_checkpoint_info().is_none());
        // When: a second getter retries discovery.
        let info = resolve_checkpoint_info(&snapshot, &engine);

        // Then: it retains the complete sidecar list, and subsequent getters perform no I/O.
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 2);
        assert_eq!(info.leaf_file_count(), 2);
        assert_eq!(
            info.last_checkpoint()
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
