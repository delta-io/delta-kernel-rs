//! Checkpoint-shape resolution for scan / FSR log replay.
//!
//! Probes a snapshot's checkpoint topology -- no checkpoint, an inline *leaf* checkpoint, or a
//! V2 *manifest* that references sidecars -- and, when stats are requested, resolves how those
//! stats are surfaced (native `add.stats_parsed` struct vs JSON `add.stats` string). The probe is
//! driven synchronously through a [`PlanExecutor`]. A checkpoint is a manifest only if it actually
//! references sidecars: a parquet footer with no `sidecar` column is a leaf outright, but otherwise
//! (a `sidecar` column present, or any JSON checkpoint) the column is drained through
//! [`SidecarVisitor`] -- no references means an inline leaf, one or more a manifest. For a
//! manifest, that same first sidecar then answers the parsed-stats probe via its footer.
//!
//! This is the IO-bearing front half of log-replay planning. It is deliberately independent of
//! the plan-construction surface and the reconciliation pipeline: a caller resolves the shape
//! here, then feeds it into a (separately built) reconciliation plan.

use std::sync::Arc;

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{Sidecar, SIDECAR_NAME};
use crate::engine_data::RowVisitor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::ir::nodes::FileType;
use crate::plans::{IoOperation, Operation, PlanExecutor, QueryPlanBuilder};
use crate::schema::{SchemaRef, StructField, StructType, ToSchema};
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
    /// V2 multipart manifest -- `files` are the manifest parts; the leaf rows live in sidecar
    /// parquet files the manifest references (resolved lazily by the reconciliation plan, not
    /// stored here).
    Manifest,
}

/// Stats wiring resolved for a scan that requested stats: the projected stats schema plus
/// whether the snapshot's checkpoint files surface column stats as a native `add.stats_parsed`
/// struct (`true`) or as a JSON `add.stats` string (`false`).
#[derive(Clone, Debug, PartialEq)]
pub struct StatsInfo {
    /// The requested (projected) stats schema the reconciliation plan should produce.
    pub schema: SchemaRef,
    /// `true` iff the checkpoint surfaces `add.stats_parsed` compatible with [`Self::schema`].
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
    /// Propagates executor failures from the footer reads or the manifest scan, and any error
    /// resolving a sidecar reference to a [`FileMeta`].
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
        let files: Vec<FileMeta> = parts.iter().map(|p| p.location.clone()).collect();

        // Classify leaf vs manifest by whether the checkpoint actually references sidecars. A
        // `sidecar` *column* in the schema is necessary but NOT sufficient: a V2 checkpoint that
        // inlines its actions still carries an all-null `sidecar` column. So we drain that column
        // whenever it could be populated, and treat an empty drain as an inline leaf.
        //
        // Parquet exposes its schema in the footer: no `sidecar` column means a V1 leaf, and that
        // footer IS the leaf schema we probe for parsed stats. JSON has no footer, so we always
        // drain. Either way: >=1 sidecar => manifest, 0 => leaf.
        let mut leaf_parsed_stats = None;
        let sidecar = match file_format {
            FileType::Parquet => {
                let cp_schema = read_footer_schema(exec, first.location.clone())?;
                let sidecar = if cp_schema.contains(SIDECAR_NAME) {
                    collect_sidecar(exec, files, file_format, &seg.log_root)?
                } else {
                    None
                };
                match sidecar {
                    Some(sidecar) => Some(sidecar),
                    // No `sidecar` column (V1 leaf) or column present but unreferenced (V2 inline
                    // leaf): either way the footer we read is the leaf schema, so probe it here.
                    _ => {
                        leaf_parsed_stats = stats_probe(Some(&cp_schema), stats_schema);
                        None
                    }
                }
            }
            FileType::Json => collect_sidecar(exec, files, file_format, &seg.log_root)?,
        };

        if sidecar.is_none() {
            // A JSON leaf has no footer to probe and surfaces stats only as JSON strings, so its
            // parsed answer falls through to `false`.
            return Ok(make_info(
                CheckpointShape::Leaf,
                leaf_parsed_stats.unwrap_or(false),
            ));
        }

        // Manifest: the leaf rows -- and their stats -- live in the sidecars, so the sidecar
        // footer is authoritative for parsed stats. The `_last_checkpoint` hint describes the
        // manifest, not the leaves, so we deliberately do NOT consult it. Probe the first sidecar
        // only when stats were requested.
        // Classification drained these sidecars; the leaf path returned above, so a manifest
        // always carries at least one. An empty list would simply leave parsed stats `false`.
        let has_parsed_stats = match (stats_schema, sidecar) {
            (Some(reqd), Some(sidecar)) => {
                let side_schema = read_footer_schema(exec, sidecar.clone())?;
                LogSegment::schema_has_compatible_stats_parsed(side_schema.as_ref(), reqd.as_ref())
            }
            _ => false,
        };

        Ok(make_info(CheckpointShape::Manifest, has_parsed_stats))
    }
}

/// Compare a checkpoint/leaf `schema` against a `requested` stats schema, returning whether the
/// checkpoint surfaces compatible native parsed stats. Returns `None` when either input is
/// absent (no schema to probe, or stats not requested).
fn stats_probe(schema: Option<&SchemaRef>, requested: Option<&SchemaRef>) -> Option<bool> {
    let (schema, requested) = (schema?, requested?);
    Some(LogSegment::schema_has_compatible_stats_parsed(
        schema.as_ref(),
        requested.as_ref(),
    ))
}

/// Footer-read a parquet file's schema through the executor.
fn read_footer_schema(exec: &dyn PlanExecutor, file: FileMeta) -> DeltaResult<SchemaRef> {
    let footer = exec
        .execute_op(Operation::IoOperation(IoOperation::ParquetFooter { file }))?
        .into_parquet_footer()?;
    Ok(footer.schema)
}

/// Scan the manifest `files` and drain their `sidecar` column through [`SidecarVisitor`],
/// resolving each reference to a [`FileMeta`] under `log_root`.
///
/// This mimics a streaming reducer kernel-side: it pulls each batch from the executor's result
/// stream and visits its rows, rather than crossing the engine boundary with a reducer sink.
/// The scan projects only the `sidecar` column, so manifest action rows (`add` / `remove`) read
/// as null and are skipped by the visitor.
fn collect_sidecar(
    exec: &dyn PlanExecutor,
    files: Vec<FileMeta>,
    file_format: FileType,
    log_root: &Url,
) -> DeltaResult<Option<FileMeta>> {
    let plan = match file_format {
        FileType::Parquet => QueryPlanBuilder::scan_parquet(files, sidecar_scan_schema()),
        FileType::Json => QueryPlanBuilder::scan_json(files, sidecar_scan_schema()),
    }
    .build()?;
    let data = exec.execute_op(Operation::QueryPlan(plan))?.into_data()?;

    let mut visitor = SidecarVisitor::default();
    for batch in data {
        if !visitor.sidecars.is_empty() {
            break;
        }
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    match visitor.sidecars.pop() {
        Some(sidecar) => Ok(Some(sidecar.to_filemeta(log_root)?)),
        None => Ok(None),
    }
}

/// Manifest scan schema: just the `sidecar` column. Narrowing to that one column avoids paying
/// the read cost for the action columns we never inspect here.
fn sidecar_scan_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        SIDECAR_NAME,
        Sidecar::to_schema(),
    )]))
}

/// A checkpoint part's encoding, derived from its file extension.
fn checkpoint_file_type(part: &ParsedLogPath) -> FileType {
    if part.extension == "json" {
        FileType::Json
    } else {
        FileType::Parquet
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::utils::test_utils::load_test_table;

    /// The checkpoint variant a table is expected to resolve to.
    #[derive(Debug)]
    enum Expected {
        None,
        Leaf(FileType),
        Manifest(FileType),
    }

    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint", Expected::None)]
    #[case::leaf_parquet(
        "with_checkpoint_no_last_checkpoint",
        Expected::Leaf(FileType::Parquet)
    )]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        Expected::Manifest(FileType::Parquet)
    )]
    #[case::manifest_json(
        "v2-checkpoints-json-with-sidecars",
        Expected::Manifest(FileType::Json)
    )]
    // A V2 JSON checkpoint with no sidecar references inlines its actions: a leaf, not a manifest.
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        Expected::Leaf(FileType::Json)
    )]
    // A V2 parquet checkpoint carries a `sidecar` column even when it inlines its actions; with
    // zero sidecar references it is still a leaf, so the column alone must not classify it.
    #[case::leaf_parquet_inline(
        "v2-checkpoints-parquet-without-sidecars",
        Expected::Leaf(FileType::Parquet)
    )]
    fn resolve_classifies_checkpoint(#[case] table: &str, #[case] expected: Expected) {
        let (_engine, snapshot, _tempdir) = load_test_table(table).unwrap();
        let exec = SyncPlanExecutor::new();
        // No stats requested: topology only.
        let shape = ScanShape::resolve(&exec, snapshot.as_ref(), None, None).unwrap();
        assert!(shape.stats.is_none(), "{table}: stats not requested");
        match (expected, &shape.checkpoint) {
            (Expected::None, CheckpointShape::None) => {}
            (Expected::Leaf(fmt), CheckpointShape::Leaf { files, file_format }) => {
                assert!(
                    !files.is_empty(),
                    "{table}: leaf must have checkpoint files"
                );
                assert_eq!(*file_format, fmt, "{table}: file format mismatch");
            }
            (Expected::Manifest(fmt), CheckpointShape::Manifest { files, file_format }) => {
                assert!(!files.is_empty(), "{table}: manifest must have parts");
                assert_eq!(*file_format, fmt, "{table}: file format mismatch");
            }
            (expected, actual) => panic!("{table}: expected {expected:?}, got {actual:?}"),
        }
    }

    /// When stats are requested, `StatsInfo` carries the requested schema verbatim and reports
    /// whether the checkpoint surfaces it as native parsed stats: `true` for the
    /// struct-stats-only tables, `false` for the JSON-stats tables. An empty `minValues` /
    /// `maxValues` request is trivially compatible, so this isolates "does the checkpoint have
    /// an `add.stats_parsed` struct at all".
    #[rstest]
    #[case::json_stats_manifest("v2-classic-checkpoint-parquet", false)]
    #[case::json_stats_sidecars("v2-checkpoints-parquet-with-sidecars", false)]
    #[case::struct_stats_leaf("v2-classic-parquet-struct-stats-only", true)]
    // Manifest sidecar probe: the parsed-stats answer comes from a sidecar footer, exercising the
    // JSON drain-then-probe path (JSON) and the lazy parquet drain (parquet).
    #[case::struct_stats_json_manifest("v2-json-sidecars-struct-stats-only", true)]
    #[case::struct_stats_parquet_manifest("v2-parquet-sidecars-struct-stats-only", true)]
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
