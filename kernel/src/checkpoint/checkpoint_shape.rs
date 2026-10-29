//! Resolves a checkpoint shape:
//! - A single-part checkpoint has one checkpoint leaf storing file actions (`add` and `remove`).
//! - A multipart checkpoint has one checkpoint leaf per checkpoint part.
//! - A manifest checkpoint has sidecars as its checkpoint leaves.
//!
//! A checkpoint leaf directly stores file actions. When requested, this module also retains the
//! checkpoint leaf schema.
//! Driven through a [`PlanExecutor`].

#![allow(dead_code)]

use std::sync::{Arc, OnceLock};

use url::Url;

use super::CHECKPOINT_ACTIONS_SCHEMA_V2;
use crate::actions::visitors::SidecarVisitor;
use crate::actions::SIDECAR_NAME;
use crate::engine_data::RowVisitor;
use crate::expressions::col;
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::FileType;
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::{SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::{FileMeta, KernelResult};

/// Topology of a checkpoint: where the `add` / `remove` actions live.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CheckpointType {
    /// No checkpoint files.
    None,
    /// File actions inline. Classic V1, inline V2, and multi-part V1.
    Leaf,
    /// V2 manifest: checkpoint references sidecar files holding the file actions.
    Manifest,
}

/// A snapshot's resolved checkpoint type and checkpoint leaf schema.
#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointShape {
    /// What kind of checkpoint this is.
    pub(crate) checkpoint_type: CheckpointType,
    /// Schema of the checkpoint leaves, when requested. An empty cell means uninspected.
    pub(crate) leaf_checkpoint_schema: OnceLock<Option<SchemaRef>>,
    pub(crate) sidecars: Vec<FileMeta>,
    pub(crate) is_json_leaf: bool,
    /// Number of files carrying file actions, excluding the manifest when sidecars are present.
    /// Zero when the snapshot has no checkpoint.
    pub num_files: u64,
    /// Total bytes of all checkpoint parts and referenced sidecars.
    pub size_in_bytes: u64,
}

impl CheckpointShape {
    /// Physical Parquet schema when requested during snapshot inspection.
    /// Returns `None` before schema resolution, without a checkpoint, or for an inline JSON
    /// checkpoint. All checkpoint parts or sidecars share this schema.
    pub fn parquet_schema(&self) -> Option<&SchemaRef> {
        if self.is_json_leaf {
            None
        } else {
            self.leaf_checkpoint_schema()
        }
    }

    pub(crate) fn resolve(exec: &dyn PlanExecutor, snapshot: &Snapshot) -> KernelResult<Self> {
        let mut shape = Self::try_new(exec, snapshot)?;
        let parts = &snapshot.log_segment().listed.checkpoint_parts;
        shape.num_files = if shape.sidecars.is_empty() {
            parts.len()
        } else {
            shape.sidecars.len()
        } as u64;
        shape.is_json_leaf =
            shape.sidecars.is_empty() && parts.first().is_some_and(|part| part.is_json());
        shape.size_in_bytes = parts
            .iter()
            .map(|part| &part.location)
            .chain(shape.sidecars.iter())
            .try_fold(0u64, |total, file| {
                total.checked_add(file.size).ok_or_else(|| {
                    crate::KernelError::generic("Checkpoint file size exceeds u64::MAX")
                })
            })?;
        Ok(shape)
    }

    pub(crate) fn leaf_checkpoint_schema(&self) -> Option<&SchemaRef> {
        self.leaf_checkpoint_schema.get().and_then(Option::as_ref)
    }

    pub(crate) fn leaf_schema_resolved(&self) -> bool {
        self.leaf_checkpoint_schema.get().is_some()
    }

    // Called under Snapshot's initialization lock so schema discovery is also retryable and shared.
    pub(crate) fn resolve_leaf_schema(
        &self,
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
    ) -> KernelResult<()> {
        if self.leaf_schema_resolved() {
            return Ok(());
        }
        let segment = snapshot.log_segment();
        let schema = match self.checkpoint_type {
            CheckpointType::None => None,
            CheckpointType::Leaf if segment.listed.checkpoint_parts[0].is_json() => {
                Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone())
            }
            CheckpointType::Leaf | CheckpointType::Manifest => {
                let (file, hint) = if let Some(sidecar) = self.sidecars.first() {
                    (
                        sidecar,
                        segment.checkpoint_hint_sidecar_file_schema().map(Arc::new),
                    )
                } else {
                    (
                        &segment.listed.checkpoint_parts[0].location,
                        segment.checkpoint_hint_schema(),
                    )
                };
                Some(match hint {
                    Some(schema) => schema,
                    None => read_parquet_footer_schema(exec, file.clone())?,
                })
            }
        };
        let _ = self.leaf_checkpoint_schema.set(schema);
        Ok(())
    }

    /// Resolves `snapshot`'s checkpoint topology, retaining any leaf schema already read.
    ///
    /// Returns an error if checkpoint metadata is invalid or required checkpoint data cannot be
    /// read.
    #[tracing::instrument(
        name = "checkpoint_shape.try_new",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn try_new(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
    ) -> KernelResult<CheckpointShape> {
        Self::try_new_impl(exec, snapshot, false)
    }

    /// Resolves `snapshot`'s checkpoint topology and retains the checkpoint leaf schema.
    ///
    /// Returns an error if checkpoint metadata is invalid or required checkpoint data cannot be
    /// read.
    #[tracing::instrument(
        name = "checkpoint_shape.try_new_with_leaf_schema",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn try_new_with_leaf_schema(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
    ) -> KernelResult<CheckpointShape> {
        Self::try_new_impl(exec, snapshot, true)
    }

    fn try_new_impl(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
        needs_leaf_schema: bool,
    ) -> KernelResult<CheckpointShape> {
        let segment = snapshot.log_segment();

        let (root_checkpoint, file_type) = match segment.listed.checkpoint_parts.first() {
            Some(checkpoint) if checkpoint.is_json() => (&checkpoint.location, FileType::Json),
            Some(checkpoint) => (&checkpoint.location, FileType::Parquet),
            None => {
                return Ok(CheckpointShape {
                    checkpoint_type: CheckpointType::None,
                    leaf_checkpoint_schema: None.into(),
                    sidecars: vec![],
                    size_in_bytes: 0,
                    num_files: 0,
                    is_json_leaf: false,
                })
            }
        };

        // Classify from a V2 checkpoint's `_last_checkpoint` hint when possible, else inspect the
        // file.
        if let Some(shape) = Self::from_v2_checkpoint_hint(
            exec,
            segment,
            root_checkpoint,
            file_type,
            needs_leaf_schema,
        )? {
            return Ok(shape);
        }

        // A checkpoint with sidecars is a manifest, one without is a leaf.
        match file_type {
            FileType::Parquet => {
                let cp_schema = match segment.checkpoint_hint_schema() {
                    Some(schema) => schema,
                    None => read_parquet_footer_schema(exec, root_checkpoint.clone())?,
                };
                // No `sidecar` column means the file actions are inline, so this is a leaf.
                if !cp_schema.contains(SIDECAR_NAME) {
                    return Ok(Self::new_leaf(Some(cp_schema)));
                }
                // The `sidecar` column may still be all-null (not a manifest), so scan it to
                // confirm whether a sidecar is actually present.
                match collect_sidecars(exec, root_checkpoint, file_type, &segment.log_root)? {
                    sidecars if !sidecars.is_empty() => Self::try_new_manifest(
                        exec,
                        sidecars,
                        needs_leaf_schema,
                        segment.checkpoint_hint_sidecar_file_schema(),
                    ),
                    _ => Ok(Self::new_leaf(Some(cp_schema))),
                }
            }
            // A JSON checkpoint has no footer schema to inspect, so try to collect a sidecar to
            // decide if it is a manifest or a leaf.
            FileType::Json => {
                match collect_sidecars(exec, root_checkpoint, file_type, &segment.log_root)? {
                    sidecars if !sidecars.is_empty() => Self::try_new_manifest(
                        exec,
                        sidecars,
                        needs_leaf_schema,
                        segment.checkpoint_hint_sidecar_file_schema(),
                    ),
                    _ => Ok(Self::new_leaf(Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone()))),
                }
            }
        }
    }

    /// Classify the checkpoint from its `_last_checkpoint` sidecar hint, without reading the
    /// checkpoint file. Returns `None` when the hint is absent (or was trimmed away), leaving the
    /// caller to inspect the file. A non-empty sidecar list is a manifest; an empty list is a leaf
    /// (the writer emits an empty list only for a leaf, and trims an oversized manifest to absent,
    /// never to empty).
    #[tracing::instrument(
        name = "checkpoint_shape.from_v2_checkpoint_hint",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    fn from_v2_checkpoint_hint(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
        root_checkpoint: &FileMeta,
        file_type: FileType,
        needs_leaf_schema: bool,
    ) -> KernelResult<Option<CheckpointShape>> {
        match segment.checkpoint_hint_sidecars().map(Vec::as_slice) {
            Some(sidecars @ [_, ..]) => {
                let sidecar_meta = sidecars
                    .iter()
                    .map(|sidecar| sidecar.to_filemeta(&segment.log_root))
                    .collect::<KernelResult<Vec<_>>>()?;
                let result = Self::try_new_manifest(
                    exec,
                    sidecar_meta,
                    needs_leaf_schema,
                    segment.checkpoint_hint_sidecar_file_schema(),
                )?;
                Ok(Some(result))
            }
            Some([]) => {
                let leaf_schema = match file_type {
                    FileType::Json => Some(CHECKPOINT_ACTIONS_SCHEMA_V2.clone()),
                    FileType::Parquet => match segment.checkpoint_hint_schema() {
                        Some(schema) => Some(schema),
                        None if needs_leaf_schema => {
                            Some(read_parquet_footer_schema(exec, root_checkpoint.clone())?)
                        }
                        None => None,
                    },
                };
                Ok(Some(Self::new_leaf(leaf_schema)))
            }
            None => Ok(None),
        }
    }

    /// Build the shape for a manifest checkpoint. Its file actions live in the sidecars. All
    /// sidecars of a checkpoint share one schema, so probing the first is sufficient.
    ///
    /// If the `_last_checkpoint` hint carries a `sidecarFileSchema`, use it directly. Otherwise,
    /// read the sidecar's footer when the leaf schema is requested.
    #[tracing::instrument(
        name = "checkpoint_shape.try_new_manifest",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    fn try_new_manifest(
        exec: &dyn PlanExecutor,
        sidecars: Vec<FileMeta>,
        needs_leaf_schema: bool,
        hint_sidecar_schema: Option<StructType>,
    ) -> KernelResult<CheckpointShape> {
        let leaf_checkpoint_schema = match (needs_leaf_schema, hint_sidecar_schema) {
            (_, Some(schema)) => Some(Arc::new(schema)),
            (true, None) => Some(read_parquet_footer_schema(exec, sidecars[0].clone())?),
            (false, None) => None,
        };
        Ok(CheckpointShape {
            checkpoint_type: CheckpointType::Manifest,
            leaf_checkpoint_schema: leaf_checkpoint_schema
                .map(|schema| Some(schema).into())
                .unwrap_or_default(),
            sidecars,
            size_in_bytes: 0,
            num_files: 0,
            is_json_leaf: false,
        })
    }

    fn new_leaf(leaf_checkpoint_schema: Option<SchemaRef>) -> CheckpointShape {
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: leaf_checkpoint_schema
                .map(|schema| Some(schema).into())
                .unwrap_or_default(),
            sidecars: vec![],
            size_in_bytes: 0,
            num_files: 0,
            is_json_leaf: false,
        }
    }

    /// Returns `stats_schema` when the checkpoint has compatible parsed stats.
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

#[tracing::instrument(
    name = "checkpoint_shape.read_parquet_footer_schema",
    skip_all,
    fields(enable_call_frame),
    err
)]
fn read_parquet_footer_schema(exec: &dyn PlanExecutor, file: FileMeta) -> KernelResult<SchemaRef> {
    Ok(exec.read_parquet_footer(file)?.schema)
}

/// Read the checkpoint `file`'s `sidecar` column, returning every referenced sidecar's
/// [`FileMeta`].
#[tracing::instrument(
    name = "checkpoint_shape.collect_sidecars",
    skip_all,
    fields(enable_call_frame),
    err
)]
fn collect_sidecars(
    exec: &dyn PlanExecutor,
    file: &FileMeta,
    file_format: FileType,
    log_root: &Url,
) -> KernelResult<Vec<FileMeta>> {
    let read_schema = LogSegment::sidecar_read_schema();
    // No file-constant columns: the sidecar column is read directly from each file.
    let plan = match file_format {
        FileType::Parquet => PlanBuilder::scan_parquet([file.clone()], &[], read_schema),
        FileType::Json => PlanBuilder::scan_json([file.clone()], &[], read_schema),
    }?
    .filter(col!(SIDECAR_NAME, "path").is_not_null())?
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;

    let mut visitor = SidecarVisitor::default();
    for batch in data {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    visitor
        .sidecars
        .iter()
        .map(|sidecar| sidecar.to_filemeta(log_root))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Barrier;

    use rstest::rstest;

    use super::*;
    use crate::actions::{
        CheckpointMetadata, Sidecar, ADD_NAME, MAX_VALUES, MIN_VALUES, NUM_RECORDS,
        SIDECAR_FILE_SCHEMA_TAG, STATS_PARSED,
    };
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
    use crate::log_segment_files::LogSegmentFiles;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::{IoOperation, PlanResult};
    use crate::scan::StatsOptions;
    use crate::schema::{schema, schema_ref};
    use crate::unit_test_utils::{
        copy_test_table, create_log_path, create_log_path_with_size, load_test_table,
    };
    use crate::KernelResult;

    /// Counts I/O operations and verifies that sidecar discovery queries filter out null paths.
    struct CountingExecutor {
        inner: SyncPlanExecutor,
        query_scans: AtomicUsize,
        footer_reads: AtomicUsize,
        fail_next_footer: AtomicBool,
    }

    impl CountingExecutor {
        fn new() -> Self {
            Self {
                inner: SyncPlanExecutor::default(),
                query_scans: AtomicUsize::new(0),
                footer_reads: AtomicUsize::new(0),
                fail_next_footer: AtomicBool::new(false),
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
                    assert_eq!(predicate, Some(&col!(SIDECAR_NAME, "path").is_not_null()));
                    _ = self.query_scans.fetch_add(1, Ordering::Relaxed);
                }
                Operation::IoOperation(IoOperation::ParquetFooter { .. }) => {
                    _ = self.footer_reads.fetch_add(1, Ordering::Relaxed);
                    if self.fail_next_footer.swap(false, Ordering::Relaxed) {
                        return Err(crate::KernelError::generic("injected footer failure"));
                    }
                }
                _ => {}
            }
            self.inner.execute_op(op)
        }
    }

    #[rstest]
    #[case("app-txn-no-checkpoint")]
    #[case("with_checkpoint_no_last_checkpoint")]
    #[case("v1-multi-part-struct-stats-only")]
    #[case("v2-checkpoints-json-with-sidecars")]
    #[case("v2-checkpoints-parquet-with-sidecars")]
    #[case("v2-checkpoints-json-without-sidecars")]
    fn checkpoint_inspection_counts_all_bytes_and_is_reused(
        #[case] table: &str,
        #[values(false, true)] scan_first: bool,
    ) {
        let (table_root, _tempdir) = copy_test_table(table).unwrap();
        let engine = Arc::new(SyncEngine::new());
        let snapshot = Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .unwrap();
        let plan = || {
            snapshot
                .clone()
                .scan_builder()
                .with_stats(StatsOptions::all_struct())
                .build()
                .unwrap()
                .declarative_metadata_scan_plan(engine.as_ref())
                .unwrap()
        };
        if scan_first {
            plan();
        }
        let shape = snapshot.checkpoint_shape(engine.as_ref(), true).unwrap();
        let paths: Vec<_> = snapshot
            .log_segment()
            .listed
            .checkpoint_parts
            .iter()
            .map(|p| &p.location)
            .chain(shape.sidecars.iter())
            .map(|file| file.location.to_file_path().unwrap())
            .collect();
        let expected: u64 = paths
            .iter()
            .map(|path| std::fs::metadata(path).unwrap().len())
            .sum();
        assert_eq!(
            snapshot.checkpoint_size_in_bytes(engine.as_ref()).unwrap(),
            expected
        );
        for path in paths {
            std::fs::remove_file(path).unwrap();
        }
        assert!(std::ptr::eq(
            snapshot.checkpoint_shape(engine.as_ref(), true).unwrap(),
            shape
        ));
        assert_eq!(
            snapshot.checkpoint_size_in_bytes(engine.as_ref()).unwrap(),
            expected
        );
        plan();
    }

    #[test]
    fn snapshot_adds_leaf_schema_after_a_scan_without_stats() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        let plan = |stats| {
            snapshot
                .clone()
                .scan_builder()
                .with_stats(stats)
                .build()
                .unwrap()
                .declarative_metadata_scan_plan(&engine)
                .unwrap()
        };
        plan(StatsOptions::none());
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
        assert!(!snapshot
            .checkpoint_shape(&engine, false)
            .unwrap()
            .leaf_schema_resolved());
        let bytes = snapshot.checkpoint_size_in_bytes(&engine).unwrap();
        assert!(bytes > 0);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 0);
        plan(StatsOptions::all_struct());
        let shape = snapshot.checkpoint_shape(&engine, true).unwrap();
        assert!(shape.parquet_schema().is_some());
        plan(StatsOptions::all_struct());
        assert_eq!(snapshot.checkpoint_size_in_bytes(&engine).unwrap(), bytes);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 1);
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case("with_checkpoint_no_last_checkpoint")]
    #[case("v2-checkpoints-parquet-with-sidecars")]
    fn snapshot_retries_failed_checkpoint_inspection(#[case] table: &str) {
        let (engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(engine).with_plan_executor(exec.clone());
        exec.fail_next_footer.store(true, Ordering::Relaxed);
        assert!(snapshot.checkpoint_shape(&engine, true).is_err());
        let shape = snapshot.checkpoint_shape(&engine, true).unwrap();
        assert!(shape.parquet_schema().is_some());
        assert!(std::ptr::eq(
            snapshot.checkpoint_shape(&engine, true).unwrap(),
            shape
        ));
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn concurrent_inspection_and_scans_share_checkpoint_io_without_a_hint() {
        let (root, _tempdir) = copy_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let hint = Url::parse(root.as_ref())
            .unwrap()
            .join("_delta_log/_last_checkpoint")
            .unwrap();
        std::fs::remove_file(hint.to_file_path().unwrap()).unwrap();
        let base_engine = Arc::new(SyncEngine::new());
        let snapshot = Snapshot::builder_for(root)
            .build(base_engine.as_ref())
            .unwrap();
        let exec = Arc::new(CountingExecutor::new());
        let engine = DelegatingEngine::new(base_engine).with_plan_executor(exec.clone());
        let barrier = Barrier::new(8);
        std::thread::scope(|scope| {
            for i in 0..8 {
                let snapshot = &snapshot;
                let engine = &engine;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    match i % 3 {
                        0 => {
                            snapshot.checkpoint_shape(engine, true).unwrap();
                        }
                        1 => {
                            snapshot.checkpoint_size_in_bytes(engine).unwrap();
                        }
                        _ => {
                            snapshot
                                .clone()
                                .scan_builder()
                                .with_stats(StatsOptions::all_struct())
                                .build()
                                .unwrap()
                                .declarative_metadata_scan_plan(engine)
                                .unwrap();
                        }
                    }
                });
            }
        });
        assert_eq!(exec.query_scans.load(Ordering::Relaxed), 1);
        assert_eq!(exec.footer_reads.load(Ordering::Relaxed), 2);
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

        let shape = if stats_schema.is_some() {
            CheckpointShape::try_new_with_leaf_schema(&exec, snapshot.as_ref())
        } else {
            CheckpointShape::try_new(&exec, snapshot.as_ref())
        }
        .unwrap();
        let parsed_stats_schema = stats_schema
            .as_ref()
            .and_then(|stats_schema| shape.compatible_stats_parsed_schema(stats_schema));

        assert_eq!(
            shape.checkpoint_type, expected_checkpoint,
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

        if stats_schema.is_some() {
            assert_eq!(
                shape.leaf_checkpoint_schema().is_some(),
                expected_checkpoint != CheckpointType::None,
                "{table}: checkpoint leaf schema"
            );
        }
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

        let shape = CheckpointShape::try_new_with_leaf_schema(
            &SyncPlanExecutor::default(),
            snapshot.as_ref(),
        )
        .unwrap();

        assert!(shape
            .compatible_stats_parsed_schema(&incompatible)
            .is_none());
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
        let shape = CheckpointShape::try_new_with_leaf_schema(
            &SyncPlanExecutor::default(),
            snapshot.as_ref(),
        )
        .unwrap();

        assert_eq!(
            shape.compatible_partition_values_parsed_schema(&partition_schema),
            expected_compatible.then_some(&partition_schema)
        );
    }

    /// Fast path on a manifest hint: one sidecar footer read, no drain (`query_scans == 0`). Guards
    /// against the optimization silently not firing (result-only checks pass via the drain too).
    #[rstest]
    #[case::without_leaf_schema(false, 0)]
    #[case::with_leaf_schema(true, 1)]
    fn fast_path_skips_checkpoint_drain_when_hint_lists_sidecars(
        #[case] needs_leaf_schema: bool,
        #[case] expected_footer_reads: usize,
    ) {
        let (_engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let exec = CountingExecutor::new();

        let shape = if needs_leaf_schema {
            CheckpointShape::try_new_with_leaf_schema(&exec, snapshot.as_ref())
        } else {
            CheckpointShape::try_new(&exec, snapshot.as_ref())
        }
        .unwrap();

        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert_eq!(
            exec.query_scans.load(Ordering::Relaxed),
            0,
            "fast path must not drain the checkpoint sidecar column"
        );
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            expected_footer_reads,
            "fast path reads the sidecar footer only when its schema is needed"
        );
        assert_eq!(shape.leaf_checkpoint_schema().is_some(), needs_leaf_schema);
    }

    /// Without a hint, a manifest must drain the `sidecar` column (`query_scans >= 1`).
    #[rstest]
    #[case::json("v2-checkpoints-json-with-sidecars", false)]
    #[case::parquet("v2-parquet-sidecars-struct-stats-only", true)]
    fn resolve_manifest_via_drain_without_hint(
        #[case] table: &str,
        #[case] expect_parsed_stats: bool,
    ) {
        let (table_root, _tempdir) = copy_test_table(table).unwrap();
        let engine = Arc::new(SyncEngine::new());
        let snapshot = Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .unwrap();
        // Remove the hint so resolve must drain.
        let hint = snapshot
            .log_segment()
            .log_root
            .join("_last_checkpoint")
            .unwrap();
        std::fs::remove_file(hint.to_file_path().unwrap()).unwrap();
        let table_root = snapshot
            .table_configuration()
            .table_root()
            .as_str()
            .to_string();
        let snapshot = Snapshot::builder_for(&table_root)
            .build(engine.as_ref())
            .unwrap();

        let exec = CountingExecutor::new();
        let shape = CheckpointShape::try_new_with_leaf_schema(&exec, snapshot.as_ref()).unwrap();

        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert!(
            exec.query_scans.load(Ordering::Relaxed) >= 1,
            "must drain, not fast-path"
        );
        assert_eq!(
            shape
                .compatible_stats_parsed_schema(&probe_stats_schema())
                .is_some(),
            expect_parsed_stats
        );
    }

    /// Builds a `LogSegment` whose applicable hint carries `v2Checkpoint.sidecarFiles == Some([])`
    /// (empty, not absent) for a checkpoint with the given extension. No real fixture carries an
    /// empty sidecar list, so this synthetic segment is the only way to exercise the leaf fast
    /// path.
    fn segment_with_empty_sidecars_hint(extension: &str) -> LogSegment {
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
                    sidecar_files: Some(vec![]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .unwrap()
    }

    /// An empty-sidecars hint (`Some([])`) classifies as a leaf without inspecting the checkpoint.
    /// A JSON leaf uses the canonical action schema; a parquet leaf reads its schema only when
    /// requested. `None` here means the hint short-circuited (a fall-through would return `Some`).
    #[rstest]
    // JSON leaf: use the canonical schema without I/O.
    #[case::json_without_leaf_schema("json", false, None, 0)]
    #[case::json_with_leaf_schema("json", true, None, 0)]
    // Parquet leaf: schema not retained -> no footer read or parsed stats.
    #[case::parquet_without_leaf_schema("parquet", false, None, 0)]
    fn empty_sidecars_hint_classifies_leaf_without_drain(
        #[case] extension: &str,
        #[case] needs_leaf_schema: bool,
        #[case] expect_parsed: Option<bool>,
        #[case] expected_footer_reads: usize,
    ) {
        let segment = segment_with_empty_sidecars_hint(extension);
        let root = &segment.listed.checkpoint_parts[0].location;
        let file_type = match extension {
            "json" => FileType::Json,
            _ => FileType::Parquet,
        };
        let stats_schema = needs_leaf_schema.then(probe_stats_schema);
        let exec = CountingExecutor::new();

        let shape = CheckpointShape::from_v2_checkpoint_hint(
            &exec,
            &segment,
            root,
            file_type,
            needs_leaf_schema,
        )
        .unwrap()
        .expect("an empty-sidecars hint must classify without falling through");

        assert_eq!(shape.checkpoint_type, CheckpointType::Leaf);
        if file_type == FileType::Json && needs_leaf_schema {
            assert_eq!(
                shape.leaf_checkpoint_schema(),
                Some(&*CHECKPOINT_ACTIONS_SCHEMA_V2)
            );
        }
        let parsed_stats_schema = stats_schema
            .as_ref()
            .and_then(|stats_schema| shape.compatible_stats_parsed_schema(stats_schema));
        assert_eq!(parsed_stats_schema.is_some(), expect_parsed == Some(true));
        assert_eq!(
            exec.query_scans.load(Ordering::Relaxed),
            0,
            "empty-sidecars leaf must never drain the checkpoint"
        );
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            expected_footer_reads
        );
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
        let root = &segment.listed.checkpoint_parts[0].location;
        let file_type = match extension {
            "json" => FileType::Json,
            _ => FileType::Parquet,
        };
        let stats_schema = probe_stats_schema();
        let exec = CountingExecutor::new();

        let shape =
            CheckpointShape::from_v2_checkpoint_hint(&exec, &segment, root, file_type, true)
                .unwrap()
                .expect("a sidecar-listing hint must classify as a manifest");

        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
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
        let footer_shape =
            CheckpointShape::try_new_manifest(&exec, vec![sidecar.clone()], true, None).unwrap();
        let hinted =
            CheckpointShape::try_new_manifest(&exec, vec![sidecar], true, Some(hint_schema))
                .unwrap();

        assert_eq!(
            hinted.leaf_checkpoint_schema(),
            footer_shape.leaf_checkpoint_schema(),
            "hint schema must match the footer schema"
        );
    }
}
