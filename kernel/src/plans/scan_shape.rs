//! Resolves a snapshot's checkpoint shape for scanning. This falls into the following cases: no
//! checkpoint, leaf (file actions inline, including multi-part), or manifest (which references
//! sidecar files) . When stats are requested, also reports whether the checkpoint has compatible
//! parsed stats. Driven through a [`PlanExecutor`].

// Public surface for the FSR scan builder; no other in-crate caller.
#![allow(unused)]

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::SIDECAR_NAME;
use crate::engine_data::RowVisitor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::ir::nodes::FileType;
use crate::plans::{IoOperation, Operation, PlanBuilder, PlanExecutor};
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::{DeltaResult, FileMeta};

/// Topology of a snapshot's checkpoint(s): where the `add` / `remove` actions live.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CheckpointShape {
    /// No checkpoint files.
    None,
    /// File actions inline. Classic V1, inline V2, and multi-part V1.
    Leaf,
    /// V2 manifest: checkpoint references sidecar files holding the file actions.
    Manifest,
}

/// Stats wiring for a scan that requested stats.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StatsInfo {
    /// The requested stats schema, unchanged.
    pub(crate) schema: SchemaRef,
    /// `true` if the checkpoint has an `add.stats_parsed` struct compatible with [`Self::schema`].
    pub(crate) has_parsed_stats: bool,
}

/// A scan's resolved checkpoint topology and stats wiring.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanShape {
    /// What kind of checkpoint the snapshot has.
    pub(crate) checkpoint: CheckpointShape,
    /// Stats wiring, or `None` when the caller didn't request stats.
    pub(crate) stats: Option<StatsInfo>,
}

impl ScanShape {
    /// Resolve `snapshot`'s scan shape. Determines the checkpoint topology and, when `stats_schema`
    /// is `Some`, whether the checkpoint contains parsed stats compatible with it.
    pub(crate) fn resolve(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
    ) -> DeltaResult<ScanShape> {
        let seg = snapshot.log_segment();

        let make_info = |checkpoint, has_parsed_stats| ScanShape {
            checkpoint,
            stats: stats_schema.cloned().map(|schema| StatsInfo {
                schema,
                has_parsed_stats,
            }),
        };

        let parts = &seg.listed.checkpoint_parts;
        let Some(first) = parts.first() else {
            return Ok(make_info(CheckpointShape::None, false));
        };
        let file_format = checkpoint_file_type(first);
        let hint_sidecar = seg
            .checkpoint_sidecars()
            .and_then(|sidecars| sidecars.first());
        // Classify by locating a sidecar reference: a checkpoint with sidecars is a manifest, one
        // without is a leaf. Prefer the `_last_checkpoint` hint to avoid reading the checkpoint;
        // otherwise inspect the checkpoint file itself. Alongside the sidecar, capture the schema
        // for a leaf checkpiont, since that schema carries the parsed stats.
        let (sidecar, leaf_checkpoint_schema) = match (file_format, hint_sidecar) {
            // The `_last_checkpoint` hint lists a sidecar: a manifest, no checkpoint read needed.
            (_, Some(sidecar)) => (Some(sidecar.to_filemeta(&seg.log_root)?), None),
            (FileType::Parquet, None) => {
                // Prefer the `_last_checkpoint` schema hint to avoid a footer read.
                let cp_schema = match seg.checkpoint_schema() {
                    Some(schema) => schema,
                    None => read_footer_schema(exec, first.location.clone())?,
                };
                // A checkpoint may list the `sidecar` column without being a manifest, so scan it
                // to confirm a sidecar is actually present.
                let sidecar = match cp_schema.contains(SIDECAR_NAME) {
                    true => {
                        collect_sidecar(exec, first.location.clone(), file_format, &seg.log_root)?
                    }
                    false => None,
                };
                // A leaf holds its file actions inline, so `cp_schema` carries their stats.
                let leaf_checkpoint_schema = sidecar.is_none().then_some(cp_schema);
                (sidecar, leaf_checkpoint_schema)
            }
            // A JSON checkpoint has no footer schema to inspect, so drain it for sidecars.
            (FileType::Json, None) => (
                collect_sidecar(exec, first.location.clone(), file_format, &seg.log_root)?,
                None,
            ),
        };

        let checkpoint_shape = match sidecar.is_some() {
            true => CheckpointShape::Manifest,
            false => CheckpointShape::Leaf,
        };

        // Parsed stats come from the schema of the file holding the add actions: the sidecar footer
        // for a manifest, or the leaf checkpoint schema for a parquet leaf. Probe it once, and only
        // when a stats schema was requested. A JSON leaf has no such schema (its stats are unparsed
        // strings), so `leaf_schema` is `None` and it never reports parsed stats.
        let has_parsed_stats = match stats_schema {
            Some(reqd) => {
                let data_schema = match sidecar {
                    Some(sidecar) => Some(read_footer_schema(exec, sidecar)?),
                    None => leaf_checkpoint_schema,
                };
                data_schema.is_some_and(|schema| {
                    LogSegment::schema_has_compatible_stats_parsed(schema.as_ref(), reqd.as_ref())
                })
            }
            None => false,
        };
        Ok(make_info(checkpoint_shape, has_parsed_stats))
    }
}

/// Footer-read a parquet file's schema through the executor.
fn read_footer_schema(exec: &dyn PlanExecutor, file: FileMeta) -> DeltaResult<SchemaRef> {
    let footer = exec
        .execute_op(Operation::IoOperation(IoOperation::ParquetFooter { file }))?
        .into_parquet_footer()?;
    Ok(footer.schema)
}

/// Read the checkpoint `file`'s `sidecar` column, returning the first reference to a sidecar's [`FileMeta`]
fn collect_sidecar(
    exec: &dyn PlanExecutor,
    file: FileMeta,
    file_format: FileType,
    log_root: &Url,
) -> DeltaResult<Option<FileMeta>> {
    let read_schema = LogSegment::sidecar_read_schema();
    // No file-constant columns: the sidecar column is read directly from each file.
    let plan = match file_format {
        FileType::Parquet => PlanBuilder::scan_parquet(vec![file], &[], read_schema),
        FileType::Json => PlanBuilder::scan_json(vec![file], &[], read_schema),
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

/// A checkpoint part's scan encoding, from the extension: `LogPathFileType` does not distinguish a
/// JSON from a Parquet `UuidCheckpoint`. V2 is JSON or Parquet; classic and multi-part are Parquet.
fn checkpoint_file_type(part: &ParsedLogPath) -> FileType {
    match part.extension.as_str() {
        "json" => FileType::Json,
        _ => FileType::Parquet,
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
    use crate::plans::PlanResult;
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
                inner: SyncPlanExecutor::new(),
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
    #[case::no_checkpoint("app-txn-no-checkpoint", CheckpointShape::None, None)]
    #[case::no_checkpoint_with_stats("app-txn-no-checkpoint", CheckpointShape::None, Some(false))]
    #[case::leaf_parquet("with_checkpoint_no_last_checkpoint", CheckpointShape::Leaf, None)]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        CheckpointShape::Manifest,
        Some(false)
    )]
    #[case::manifest_json("v2-checkpoints-json-with-sidecars", CheckpointShape::Manifest, None)]
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        CheckpointShape::Leaf,
        Some(false)
    )]
    // Regression: an all-null `sidecar` column is still a leaf.
    #[case::leaf_parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        CheckpointShape::Leaf,
        None
    )]
    #[case::leaf_multipart("v1-multi-part-struct-stats-only", CheckpointShape::Leaf, Some(true))]
    #[case::json_stats_classic(
        "v2-classic-checkpoint-parquet",
        CheckpointShape::Manifest,
        Some(false)
    )]
    #[case::struct_stats_leaf(
        "v2-classic-parquet-struct-stats-only",
        CheckpointShape::Leaf,
        Some(true)
    )]
    #[case::struct_stats_json_manifest(
        "v2-json-sidecars-struct-stats-only",
        CheckpointShape::Manifest,
        Some(true)
    )]
    #[case::struct_stats_parquet_manifest(
        "v2-parquet-sidecars-struct-stats-only",
        CheckpointShape::Manifest,
        Some(true)
    )]
    fn resolve_checkpoint_and_stats(
        #[case] table: &str,
        #[case] expected_checkpoint: CheckpointShape,
        #[case] expect_parsed: Option<bool>,
    ) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = SyncPlanExecutor::new();
        let stats_schema = expect_parsed.map(|_| probe_stats_schema());

        let shape = ScanShape::resolve(&exec, snapshot.as_ref(), stats_schema.as_ref()).unwrap();

        assert_eq!(
            shape.checkpoint, expected_checkpoint,
            "{table}: checkpoint shape"
        );

        match expect_parsed {
            None => assert!(shape.stats.is_none(), "{table}: stats not requested"),
            Some(parsed) => {
                let stats = shape.stats.expect("stats requested");
                assert_eq!(
                    stats.schema,
                    stats_schema.unwrap(),
                    "{table}: schema passed through verbatim"
                );
                assert_eq!(
                    stats.has_parsed_stats, parsed,
                    "{table}: parsed-stats detection"
                );
            }
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

        let shape =
            ScanShape::resolve(&exec, snapshot.as_ref(), Some(&probe_stats_schema())).unwrap();

        assert_eq!(shape.checkpoint, CheckpointShape::Manifest);
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
        let shape =
            ScanShape::resolve(&exec, snapshot.as_ref(), Some(&probe_stats_schema())).unwrap();

        assert_eq!(shape.checkpoint, CheckpointShape::Manifest);
        assert!(
            exec.query_scans.load(Ordering::Relaxed) >= 1,
            "must drain, not fast-path"
        );
    }
}
