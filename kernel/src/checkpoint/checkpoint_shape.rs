//! Resolves a checkpoint shape. This falls into the following cases: no checkpoint,
//! leaf (file actions inline, including multi-part), or manifest (which references sidecar files).
//! When stats are requested, also reports whether the checkpoint has compatible parsed stats.
//! Driven through a [`PlanExecutor`].

// No in-crate caller yet; following PRs will use this.
#![allow(dead_code)]

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::SIDECAR_NAME;
use crate::engine_data::RowVisitor;
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::FileType;
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::{DeltaResult, FileMeta};

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

/// A snapshot's resolved checkpoint type and parsed-stats schema.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CheckpointShape {
    /// What kind of checkpoint this is.
    pub(crate) checkpoint_type: CheckpointType,
    /// The requested stats schema when the checkpoint has a compatible `add.stats_parsed` struct
    /// to read it from; `None` when stats were not requested or no compatible parsed stats exist.
    pub(crate) parsed_stats_schema: Option<SchemaRef>,
}

impl CheckpointShape {
    /// Resolve `snapshot`'s checkpoint shape. Determines the checkpoint type and, when
    /// `stats_schema` is `Some`, whether the checkpoint contains parsed stats compatible with it.
    pub(crate) fn try_new(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
    ) -> DeltaResult<CheckpointShape> {
        let segment = snapshot.log_segment();

        let (root_checkpoint, file_type) = match segment.listed.checkpoint_parts.first() {
            Some(checkpoint) if checkpoint.is_json() => (&checkpoint.location, FileType::Json),
            Some(checkpoint) => (&checkpoint.location, FileType::Parquet),
            None => {
                return Ok(CheckpointShape {
                    checkpoint_type: CheckpointType::None,
                    parsed_stats_schema: None,
                })
            }
        };

        // Classify from a V2 checkpoint's `_last_checkpoint` hint when possible, else inspect the
        // file.
        if let Some(shape) =
            Self::from_v2_checkpoint_hint(exec, segment, root_checkpoint, file_type, stats_schema)?
        {
            return Ok(shape);
        }

        // A checkpoint with sidecars is a manifest, one without is a leaf.
        match file_type {
            FileType::Parquet => {
                let cp_schema = match segment.checkpoint_hint_schema() {
                    Some(schema) => schema,
                    None => exec.read_parquet_footer(root_checkpoint.clone())?.schema,
                };
                // No `sidecar` column means the file actions are inline, so this is a leaf.
                if !cp_schema.contains(SIDECAR_NAME) {
                    return Ok(Self::try_new_leaf(Some(cp_schema), stats_schema));
                }
                // The `sidecar` column may still be all-null (not a manifest), so scan it to
                // confirm whether a sidecar is actually present.
                match collect_single_sidecar(exec, root_checkpoint, file_type, &segment.log_root)? {
                    Some(sidecar) => Self::try_new_manifest(exec, sidecar, stats_schema),
                    None => Ok(Self::try_new_leaf(Some(cp_schema), stats_schema)),
                }
            }
            // A JSON checkpoint has no footer schema to inspect, so try to collect a sidecar to
            // decide if it is a manifest or a leaf. A JSON leaf has no readable schema,
            // hence no parsed stats.
            FileType::Json => {
                match collect_single_sidecar(exec, root_checkpoint, file_type, &segment.log_root)? {
                    Some(sidecar) => Self::try_new_manifest(exec, sidecar, stats_schema),
                    None => Ok(Self::try_new_leaf(None, stats_schema)),
                }
            }
        }
    }

    /// Classify the checkpoint from its `_last_checkpoint` sidecar hint, without reading the
    /// checkpoint file. Returns `None` when the hint is absent (or was trimmed away), leaving the
    /// caller to inspect the file. A non-empty sidecar list is a manifest; an empty list is a leaf
    /// (the writer emits an empty list only for a leaf, and trims an oversized manifest to absent,
    /// never to empty).
    fn from_v2_checkpoint_hint(
        exec: &dyn PlanExecutor,
        segment: &LogSegment,
        root_checkpoint: &FileMeta,
        file_type: FileType,
        stats_schema: Option<&SchemaRef>,
    ) -> DeltaResult<Option<CheckpointShape>> {
        match segment.checkpoint_hint_sidecars().map(Vec::as_slice) {
            Some([sidecar, ..]) => {
                let sidecar_meta = sidecar.to_filemeta(&segment.log_root)?;
                let result = Self::try_new_manifest(exec, sidecar_meta, stats_schema)?;
                Ok(Some(result))
            }
            Some([]) => {
                // A parquet leaf's stats live in its own schema; read it only when stats are
                // requested. A JSON leaf has no readable schema.
                let leaf_schema = match file_type {
                    FileType::Parquet if stats_schema.is_some() => {
                        Some(match segment.checkpoint_hint_schema() {
                            Some(schema) => schema,
                            None => exec.read_parquet_footer(root_checkpoint.clone())?.schema,
                        })
                    }
                    _ => None,
                };
                Ok(Some(Self::try_new_leaf(leaf_schema, stats_schema)))
            }
            None => Ok(None),
        }
    }

    /// Build the shape for a manifest checkpoint. Its file actions and their stats live in the
    /// sidecars, so probe a sidecar's (parquet) footer -- but only when stats were requested. All
    /// sidecars of a checkpoint share one schema, so probing the first is sufficient.
    fn try_new_manifest(
        exec: &dyn PlanExecutor,
        sidecar: FileMeta,
        stats_schema: Option<&SchemaRef>,
    ) -> DeltaResult<CheckpointShape> {
        let parsed_stats_schema = match stats_schema {
            Some(stats_schema) => {
                let footer_schema = exec.read_parquet_footer(sidecar)?.schema;
                LogSegment::schema_has_compatible_stats_parsed(footer_schema.as_ref(), stats_schema)
                    .then(|| stats_schema.clone())
            }
            None => None,
        };
        Ok(CheckpointShape {
            checkpoint_type: CheckpointType::Manifest,
            parsed_stats_schema,
        })
    }

    /// Build the shape for a leaf checkpoint. Its file actions are inline, so `leaf_schema` carries
    /// their stats (`None` for a JSON leaf, which has no readable footer schema).
    fn try_new_leaf(
        leaf_schema: Option<SchemaRef>,
        stats_schema: Option<&SchemaRef>,
    ) -> CheckpointShape {
        let parsed_stats_schema = stats_schema.filter(|stats_schema| {
            leaf_schema.as_ref().is_some_and(|leaf_schema| {
                LogSegment::schema_has_compatible_stats_parsed(leaf_schema.as_ref(), stats_schema)
            })
        });
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            parsed_stats_schema: parsed_stats_schema.cloned(),
        }
    }
}

/// Read the checkpoint `file`'s `sidecar` column, returning the first referenced sidecar's
/// [`FileMeta`] (enough to classify and probe; not a full enumeration).
fn collect_single_sidecar(
    exec: &dyn PlanExecutor,
    file: &FileMeta,
    file_format: FileType,
    log_root: &Url,
) -> DeltaResult<Option<FileMeta>> {
    let read_schema = LogSegment::sidecar_read_schema();
    // No file-constant columns: the sidecar column is read directly from each file.
    let plan = match file_format {
        FileType::Parquet => PlanBuilder::scan_parquet([file.clone()], &[], read_schema),
        FileType::Json => PlanBuilder::scan_json([file.clone()], &[], read_schema),
    }?
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;

    let mut visitor = SidecarVisitor::default();
    for batch in data {
        visitor.visit_rows_of(batch?.as_ref())?;
        if !visitor.sidecars.is_empty() {
            break;
        }
    }
    match visitor.sidecars.first() {
        Some(sidecar) => Ok(Some(sidecar.to_filemeta(log_root)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::actions::{MAX_VALUES, MIN_VALUES, NUM_RECORDS};
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::plans::{IoOperation, PlanResult};
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::load_test_table;

    /// Counts ops by kind and delegates to `SyncPlanExecutor`, to assert which I/O the fast path
    /// performs.
    struct CountingExecutor {
        inner: SyncPlanExecutor,
        query_scans: AtomicUsize,
        footer_reads: AtomicUsize,
    }

    impl CountingExecutor {
        fn new() -> Self {
            Self {
                inner: SyncPlanExecutor::default(),
                query_scans: AtomicUsize::new(0),
                footer_reads: AtomicUsize::new(0),
            }
        }
    }

    impl PlanExecutor for CountingExecutor {
        fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
            match &op {
                Operation::QueryPlan(_) => _ = self.query_scans.fetch_add(1, Ordering::Relaxed),
                Operation::IoOperation(IoOperation::ParquetFooter { .. }) => {
                    _ = self.footer_reads.fetch_add(1, Ordering::Relaxed)
                }
                _ => {}
            }
            self.inner.execute_op(op)
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
        let exec = SyncPlanExecutor::default();
        let stats_schema = expect_parsed.map(|_| probe_stats_schema());

        let shape =
            CheckpointShape::try_new(&exec, snapshot.as_ref(), stats_schema.as_ref()).unwrap();

        assert_eq!(
            shape.checkpoint_type, expected_checkpoint,
            "{table}: checkpoint type"
        );

        match expect_parsed {
            // Stats not requested: no parsed-stats schema regardless of the checkpoint.
            None => assert!(
                shape.parsed_stats_schema.is_none(),
                "{table}: stats not requested"
            ),
            // Requested with compatible parsed stats: the requested schema is echoed back.
            Some(true) => assert_eq!(
                shape.parsed_stats_schema.as_ref(),
                stats_schema.as_ref(),
                "{table}: parsed stats available, schema echoed"
            ),
            // Requested but no compatible parsed stats: `None`.
            Some(false) => assert!(
                shape.parsed_stats_schema.is_none(),
                "{table}: no compatible parsed stats"
            ),
        }
    }

    /// Requested stats schema for the `*-struct-stats-only` fixtures (`id: long`, `value: string`),
    /// so compatibility does real per-column matching.
    fn probe_stats_schema() -> SchemaRef {
        let columns = || {
            StructType::new_unchecked([
                StructField::nullable("id", DataType::LONG),
                StructField::nullable("value", DataType::STRING),
            ])
        };
        Arc::new(StructType::new_unchecked([
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(MIN_VALUES, columns()),
            StructField::nullable(MAX_VALUES, columns()),
        ]))
    }

    /// Fast path on a manifest hint: one sidecar footer read, no drain (`query_scans == 0`). Guards
    /// against the optimization silently not firing (result-only checks pass via the drain too).
    #[test]
    fn fast_path_skips_checkpoint_drain_when_hint_lists_sidecars() {
        let (_engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let exec = CountingExecutor::new();

        let shape = CheckpointShape::try_new(&exec, snapshot.as_ref(), Some(&probe_stats_schema()))
            .unwrap();

        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert_eq!(
            exec.query_scans.load(Ordering::Relaxed),
            0,
            "fast path must not drain the checkpoint sidecar column"
        );
        assert_eq!(
            exec.footer_reads.load(Ordering::Relaxed),
            1,
            "fast path footer-reads exactly the hint's first sidecar"
        );
    }

    /// With the hint removed, a JSON manifest must drain the `sidecar` column (`query_scans >= 1`).
    #[test]
    fn resolve_json_manifest_via_drain_without_hint() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-json-with-sidecars").unwrap();
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
        let shape = CheckpointShape::try_new(&exec, snapshot.as_ref(), Some(&probe_stats_schema()))
            .unwrap();

        assert_eq!(shape.checkpoint_type, CheckpointType::Manifest);
        assert!(
            exec.query_scans.load(Ordering::Relaxed) >= 1,
            "must drain, not fast-path"
        );
    }

    /// Builds a `LogSegment` whose applicable hint carries `v2Checkpoint.sidecarFiles == Some([])`
    /// (empty, not absent) for a checkpoint with the given extension. No real fixture carries an
    /// empty sidecar list, so this synthetic segment is the only way to exercise the leaf fast
    /// path.
    fn segment_with_empty_sidecars_hint(extension: &str) -> LogSegment {
        use crate::last_checkpoint_hint::{LastCheckpointHint, LastCheckpointV2};
        use crate::log_segment_files::LogSegmentFiles;
        use crate::utils::test_utils::{create_log_path, create_log_path_with_size};

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
    /// A JSON leaf never reports parsed stats; a parquet leaf reads its schema only when stats are
    /// requested. `None` here means the hint short-circuited (a fall-through would return `Some`).
    #[rstest]
    // JSON leaf: no readable schema, so never any parsed stats, and never any I/O.
    #[case::json_no_stats("json", false, None, 0)]
    #[case::json_with_stats("json", true, None, 0)]
    // Parquet leaf: no stats requested -> no schema read, no parsed stats.
    #[case::parquet_no_stats("parquet", false, None, 0)]
    fn empty_sidecars_hint_classifies_leaf_without_drain(
        #[case] extension: &str,
        #[case] request_stats: bool,
        #[case] expect_parsed: Option<bool>,
        #[case] expected_footer_reads: usize,
    ) {
        let segment = segment_with_empty_sidecars_hint(extension);
        let root = &segment.listed.checkpoint_parts[0].location;
        let file_type = match extension {
            "json" => FileType::Json,
            _ => FileType::Parquet,
        };
        let stats_schema = request_stats.then(probe_stats_schema);
        let exec = CountingExecutor::new();

        let shape = CheckpointShape::from_v2_checkpoint_hint(
            &exec,
            &segment,
            root,
            file_type,
            stats_schema.as_ref(),
        )
        .unwrap()
        .expect("an empty-sidecars hint must classify without falling through");

        assert_eq!(shape.checkpoint_type, CheckpointType::Leaf);
        assert_eq!(
            shape.parsed_stats_schema.is_some(),
            expect_parsed == Some(true)
        );
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
}
