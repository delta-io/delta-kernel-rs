//! Checkpoint-shape resolution for scan / FSR log replay.
//!
//! Probes a snapshot's checkpoint topology -- no checkpoint, an inline *leaf* checkpoint, or a
//! V2 *manifest* that references sidecars -- and, when stats are requested, resolves how those
//! stats are surfaced (native `add.stats_parsed` struct vs JSON `add.stats` string). The probe is
//! driven synchronously through a [`PlanExecutor`]. A V2 checkpoint is identified by a
//! `checkpointMetadata` column in its schema (the footer or `_last_checkpoint` hint); only V2
//! checkpoints can reference sidecars, so a parquet checkpoint lacking that column is a classic V1
//! leaf outright. For a V2 (or any JSON) checkpoint the `sidecar` column is drained through
//! [`SidecarVisitor`] -- no references means an inline leaf, one or more a manifest. For a
//! manifest, that first sidecar then answers the parsed-stats probe via its footer.
//!
//! This is the IO-bearing front half of log-replay planning. It is deliberately independent of
//! the plan-construction surface and the reconciliation pipeline: a caller resolves the shape
//! here, then feeds it into a (separately built) reconciliation plan.
//!
//! Sibling: `LogSegment::get_file_actions_schema_and_sidecars` reaches the same leaf/manifest
//! outcome for the engine-driven read path, though it detects V2 via the `sidecar` column rather
//! than `checkpointMetadata` (the latter is the spec-mandated V2 marker -- every V2 checkpoint
//! carries it, classic V1 never does). Keep their behavior aligned.

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::CHECKPOINT_METADATA_NAME;
use crate::engine_data::RowVisitor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::ir::nodes::FileType;
use crate::plans::{IoOperation, Operation, PlanExecutor, QueryPlanBuilder};
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::{DeltaResult, FileMeta};

/// Topology of a snapshot's checkpoint(s).
///
/// The two non-empty shapes differ in where the leaf `add` / `remove` rows live: `Leaf` keeps
/// them in the checkpoint files themselves; `Manifest` keeps them in sidecar parquet files
/// referenced by the checkpoint.
#[derive(Clone, Debug, PartialEq)]
pub enum CheckpointShape {
    /// No checkpoint files: log replay degenerates to commit-only.
    None,
    /// The checkpoint files ARE the leaves -- `add` / `remove` rows are stored inline. Covers
    /// classic V1 checkpoints and V2 checkpoints that inline their actions (no sidecars).
    Leaf,
    /// V2 manifest -- the checkpoint files reference sidecar parquet files that hold the leaf
    /// `add` / `remove` rows (resolved lazily by the reconciliation plan, not stored here).
    Manifest,
}

/// Stats wiring resolved for a scan that requested stats: the projected stats schema plus
/// whether the snapshot's checkpoint files surface column stats as a native `add.stats_parsed`
/// struct (`true`) or as a JSON `add.stats` string (`false`).
#[derive(Clone, Debug, PartialEq)]
pub struct StatsInfo {
    /// The requested (projected) stats schema the reconciliation plan should produce.
    pub schema: SchemaRef,
    /// `true` iff the snapshot surfaces `add.stats_parsed` compatible with [`Self::schema`] --
    /// read from the checkpoint footer for a leaf, or a sidecar footer for a manifest.
    pub has_parsed_stats: bool,
}

/// Resolved reconciliation shape: checkpoint topology, stats wiring, and partition schema.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanShape {
    /// What kind of checkpoint the snapshot has.
    pub checkpoint: CheckpointShape,
    /// Stats wiring. `None` when the caller didn't request stats; otherwise the projected stats
    /// schema and whether the checkpoint surfaces it natively.
    pub stats: Option<StatsInfo>,
    /// Partition schema to surface as `partitionValues_parsed`, when partitioned.
    pub partition_schema: Option<SchemaRef>,
}

impl ScanShape {
    /// Resolve `snapshot`'s scan shape, driving all IO synchronously through `exec`.
    ///
    /// Determines the checkpoint topology and, when `stats_schema` is `Some`, whether the
    /// checkpoint surfaces native parsed stats compatible with it. For a leaf the parsed-stats
    /// answer comes from the checkpoint footer; for a manifest it comes from a sidecar footer.
    /// The `_last_checkpoint` hint describes the manifest rather than the leaf rows, so it is not
    /// consulted for the parsed-stats decision.
    ///
    /// # Errors
    ///
    /// Propagates executor failures from the footer reads or the sidecar-drain scan, errors
    /// visiting the `sidecar` column, and any error resolving a sidecar reference to a
    /// [`FileMeta`].
    pub fn resolve(
        exec: &dyn PlanExecutor,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<SchemaRef>,
    ) -> DeltaResult<ScanShape> {
        let seg = snapshot.log_segment();

        let make_info = |checkpoint, has_parsed_stats| ScanShape {
            checkpoint,
            stats: stats_schema.cloned().map(|schema| StatsInfo {
                schema,
                has_parsed_stats,
            }),
            partition_schema: partition_schema.clone(),
        };

        let parts = &seg.listed.checkpoint_parts;
        let Some(first) = parts.first() else {
            return Ok(make_info(CheckpointShape::None, false));
        };
        let file_format = checkpoint_file_type(first);

        // Classify leaf vs manifest. Only V2 checkpoints can reference sidecars, and a V2
        // checkpoint is marked by a `checkpointMetadata` column in its schema -- so a
        // parquet checkpoint whose schema (footer or `_last_checkpoint` hint) lacks that
        // column is a classic V1 leaf with no sidecars to drain, and that schema IS the
        // leaf schema we probe for parsed stats. For a V2 checkpoint we drain the `sidecar`
        // column: an empty drain is an inline leaf (V2 checkpoints carry an all-null
        // `sidecar` column even when inlining), >=1 a manifest. JSON checkpoints are always
        // V2 and have no footer schema, so we always drain.
        let mut leaf_parsed_stats = false;
        let sidecar = match file_format {
            FileType::Parquet => {
                // Prefer the `_last_checkpoint` schema hint to avoid a footer read; it describes
                // this same top-level checkpoint file (for a leaf, that is the leaf schema).
                let cp_schema = match seg.checkpoint_schema() {
                    Some(schema) => schema,
                    None => read_footer_schema(exec, first.location.clone())?,
                };
                let sidecar = if cp_schema.contains(CHECKPOINT_METADATA_NAME) {
                    collect_sidecar(exec, first.location.clone(), file_format, &seg.log_root)?
                } else {
                    None
                };
                // No `checkpointMetadata` column (classic V1 leaf) or a V2 checkpoint that inlines
                // its actions (drain found no sidecars): either way `cp_schema` is the leaf schema,
                // so probe it for parsed stats when the caller requested them.
                if sidecar.is_none() {
                    leaf_parsed_stats = stats_schema.is_some_and(|reqd| {
                        LogSegment::schema_has_compatible_stats_parsed(
                            cp_schema.as_ref(),
                            reqd.as_ref(),
                        )
                    });
                }
                sidecar
            }
            FileType::Json => {
                collect_sidecar(exec, first.location.clone(), file_format, &seg.log_root)?
            }
        };

        let Some(sidecar) = sidecar else {
            // A JSON leaf has no footer to probe and surfaces stats only as JSON strings, so its
            // parsed answer stays `false`.
            return Ok(make_info(CheckpointShape::Leaf, leaf_parsed_stats));
        };

        // Manifest: the leaf rows -- and their stats -- live in the sidecars, so the sidecar
        // footer is authoritative for parsed stats. The `_last_checkpoint` hint describes the
        // manifest, not the leaves, so we deliberately do NOT consult it. Probe the sidecar only
        // when stats were requested.
        let has_parsed_stats = match stats_schema {
            Some(reqd) => {
                let side_schema = read_footer_schema(exec, sidecar)?;
                LogSegment::schema_has_compatible_stats_parsed(side_schema.as_ref(), reqd.as_ref())
            }
            None => false,
        };

        Ok(make_info(CheckpointShape::Manifest, has_parsed_stats))
    }
}

/// Footer-read a parquet file's schema through the executor.
fn read_footer_schema(exec: &dyn PlanExecutor, file: FileMeta) -> DeltaResult<SchemaRef> {
    let footer = exec
        .execute_op(Operation::IoOperation(IoOperation::ParquetFooter { file }))?
        .into_parquet_footer()?;
    Ok(footer.schema)
}

/// Scan the checkpoint `file` and drain its `sidecar` column through [`SidecarVisitor`], resolving
/// the first reference to a [`FileMeta`] under `log_root`.
///
/// This mimics a streaming reducer kernel-side: it pulls each batch from the executor's result
/// stream and visits its rows, stopping as soon as a sidecar reference appears. The scan projects
/// only the `sidecar` column, so manifest action rows (`add` / `remove`) read as null and are
/// skipped by the visitor.
fn collect_sidecar(
    exec: &dyn PlanExecutor,
    file: FileMeta,
    file_format: FileType,
    log_root: &Url,
) -> DeltaResult<Option<FileMeta>> {
    let read_schema = LogSegment::sidecar_read_schema();
    let plan = match file_format {
        FileType::Parquet => QueryPlanBuilder::scan_parquet(vec![file], read_schema),
        FileType::Json => QueryPlanBuilder::scan_json(vec![file], read_schema),
    }
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

/// A checkpoint part's scan encoding (JSON vs Parquet). The file extension is the authoritative
/// signal: `LogPathFileType` does not distinguish a JSON from a Parquet `UuidCheckpoint`.
/// Multi-part V1 checkpoints are always Parquet and carry no `sidecar` column, so they classify as
/// leaves without a drain.
fn checkpoint_file_type(part: &ParsedLogPath) -> FileType {
    if part.extension == "json" {
        FileType::Json
    } else {
        FileType::Parquet
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::load_test_table;

    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint", CheckpointShape::None)]
    #[case::leaf_parquet("with_checkpoint_no_last_checkpoint", CheckpointShape::Leaf)]
    #[case::manifest_parquet("v2-checkpoints-parquet-with-sidecars", CheckpointShape::Manifest)]
    #[case::manifest_json("v2-checkpoints-json-with-sidecars", CheckpointShape::Manifest)]
    // A V2 JSON checkpoint with no sidecar references inlines its actions: a leaf, not a manifest.
    #[case::leaf_json_inline("v2-checkpoints-json-without-sidecars", CheckpointShape::Leaf)]
    // A V2 parquet checkpoint carries a `sidecar` column even when it inlines its actions; with
    // zero sidecar references it is still a leaf, so the column alone must not classify it.
    #[case::leaf_parquet_inline("v2-checkpoints-parquet-without-sidecars", CheckpointShape::Leaf)]
    // A multi-part V1 checkpoint is always parquet and carries no `sidecar` column: a leaf.
    #[case::leaf_multipart("v1-multi-part-struct-stats-only", CheckpointShape::Leaf)]
    fn resolve_classifies_checkpoint(#[case] table: &str, #[case] expected: CheckpointShape) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = SyncPlanExecutor::new();
        // No stats requested: topology only.
        let shape = ScanShape::resolve(&exec, snapshot.as_ref(), None, None).unwrap();
        assert!(shape.stats.is_none(), "{table}: stats not requested");
        assert_eq!(shape.checkpoint, expected, "{table}: checkpoint shape");
    }

    /// When stats are requested, `StatsInfo` carries the requested schema verbatim and reports
    /// whether the checkpoint surfaces it as native parsed stats: `true` for the
    /// struct-stats-only tables, `false` for the JSON-stats tables. An empty `minValues` /
    /// `maxValues` request is trivially compatible, so this isolates "does the checkpoint have
    /// an `add.stats_parsed` struct at all".
    #[rstest]
    #[case::json_stats_classic_leaf("v2-classic-checkpoint-parquet", false)]
    #[case::json_stats_sidecars("v2-checkpoints-parquet-with-sidecars", false)]
    #[case::struct_stats_leaf("v2-classic-parquet-struct-stats-only", true)]
    // Manifest sidecar probe: the parsed-stats answer comes from a sidecar footer, exercising the
    // JSON drain-then-probe path (JSON) and the lazy parquet drain (parquet).
    #[case::struct_stats_json_manifest("v2-json-sidecars-struct-stats-only", true)]
    #[case::struct_stats_parquet_manifest("v2-parquet-sidecars-struct-stats-only", true)]
    // A multi-part V1 leaf surfaces struct stats via its checkpoint footer / `_last_checkpoint`
    // hint.
    #[case::struct_stats_multipart_leaf("v1-multi-part-struct-stats-only", true)]
    // A JSON leaf has no footer, so it never reports native parsed stats even when requested.
    #[case::json_leaf_no_parsed("v2-checkpoints-json-without-sidecars", false)]
    fn resolve_reports_parsed_stats(#[case] table: &str, #[case] expect_parsed: bool) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = SyncPlanExecutor::new();
        let stats_schema = probe_stats_schema();

        let shape =
            ScanShape::resolve(&exec, snapshot.as_ref(), Some(&stats_schema), None).unwrap();
        let stats = shape.stats.expect("stats requested");
        assert_eq!(
            stats.schema, stats_schema,
            "{table}: schema passed through verbatim"
        );
        assert_eq!(
            stats.has_parsed_stats, expect_parsed,
            "{table}: parsed-stats detection"
        );
    }

    /// `partition_schema` is threaded through verbatim, independent of checkpoint topology.
    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint")]
    #[case::manifest("v2-checkpoints-parquet-with-sidecars")]
    fn resolve_passes_partition_schema_through(#[case] table: &str) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = SyncPlanExecutor::new();
        let partition_schema: SchemaRef =
            Arc::new(StructType::new_unchecked([StructField::nullable(
                "p",
                DataType::INTEGER,
            )]));
        let shape = ScanShape::resolve(
            &exec,
            snapshot.as_ref(),
            None,
            Some(partition_schema.clone()),
        )
        .unwrap();
        assert_eq!(shape.partition_schema, Some(partition_schema), "{table}");
    }

    /// A minimal requested stats schema: empty `minValues` / `maxValues` structs. Compatibility
    /// then reduces to whether the checkpoint carries an `add.stats_parsed` struct.
    fn probe_stats_schema() -> SchemaRef {
        let empty = || StructType::new_unchecked([]);
        Arc::new(StructType::new_unchecked([
            StructField::nullable("minValues", empty()),
            StructField::nullable("maxValues", empty()),
        ]))
    }
}
