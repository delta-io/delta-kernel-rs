//! Checkpoint-shape resolution for scan
//!
//! Probes a snapshot's checkpoint topology, categorizing it into one of:
//!     1) no checkpoint
//!     2) leaf-level checkpoints, containing only file contents. This includes single-part and
//!        legacy multi-part checkpoints.
//!     3) manifest-level checkpoints, containing references to sidecar files that hold the file
//!        contents.
//! When stats are requested, it also resolves how those stats are surfaced (a structured
//! `add.stats_parsed` struct vs a JSON `add.stats` string). The probe is driven through a
//! [`PlanExecutor`].

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::SIDECAR_NAME;
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

/// Resolved reconciliation shape: checkpoint topology and stats wiring.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanShape {
    /// What kind of checkpoint the snapshot has.
    pub checkpoint: CheckpointShape,
    /// Stats wiring. `None` when the caller didn't request stats; otherwise the projected stats
    /// schema and whether the checkpoint surfaces it natively.
    pub stats: Option<StatsInfo>,
}

impl ScanShape {
    /// Resolve `snapshot`'s scan shape. Determines the checkpoint topology and, when `stats_schema`
    /// is `Some`, whether the checkpoint surfaces native parsed stats compatible with it. For a
    /// leaf the parsed-stats answer comes from the checkpoint footer; for a manifest it comes from
    /// a sidecar footer.
    pub fn resolve(
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

        // Classify leaf vs manifest by the `sidecar` column -- the direct indicator of a
        // manifest. Only V2 checkpoints carry it; a parquet checkpoint whose schema (footer or
        // `_last_checkpoint` hint) lacks it has no sidecars and is a leaf, and that schema IS the
        // leaf schema we probe for parsed stats. When present we drain it: empty -> inline leaf,
        // non-empty -> manifest. JSON checkpoints have no footer schema, so we always drain.
        //
        // TODO(#2770): when `_last_checkpoint` embeds the V2 sidecar references, classify from
        // those to skip this footer read / drain, falling back to draining when absent.
        let mut leaf_parsed_stats = false;
        let sidecar = match file_format {
            FileType::Parquet => {
                // Prefer the `_last_checkpoint` schema hint to avoid a footer read.
                let cp_schema = match seg.checkpoint_schema() {
                    Some(schema) => schema,
                    None => read_footer_schema(exec, first.location.clone())?,
                };
                let sidecar = if cp_schema.contains(SIDECAR_NAME) {
                    collect_sidecar(exec, first.location.clone(), file_format, &seg.log_root)?
                } else {
                    None
                };
                // `cp_schema` is the leaf schema here, so probe it for parsed stats when requested.
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
                // JSON checkpoints have no footer schema, so drain the `sidecar` column to
                // classify: empty -> inline leaf, non-empty -> manifest.
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
    use crate::actions::{MAX_VALUES, MIN_VALUES, NUM_RECORDS};
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::load_test_table;

    /// Resolves checkpoint shape and, when stats are requested, whether parsed stats are
    /// available. `expect_parsed = None` skips the stats probe (checkpoint-only cases).
    #[rstest]
    #[case::no_checkpoint("app-txn-no-checkpoint", CheckpointShape::None, None)]
    #[case::leaf_parquet("with_checkpoint_no_last_checkpoint", CheckpointShape::Leaf, None)]
    #[case::manifest_parquet(
        "v2-checkpoints-parquet-with-sidecars",
        CheckpointShape::Manifest,
        Some(false)
    )]
    #[case::manifest_json("v2-checkpoints-json-with-sidecars", CheckpointShape::Manifest, None)]
    // A V2 JSON checkpoint with no sidecar references inlines its actions: a leaf, not a manifest.
    #[case::leaf_json_inline(
        "v2-checkpoints-json-without-sidecars",
        CheckpointShape::Leaf,
        Some(false)
    )]
    // Regression guard: a V2 parquet checkpoint's all-null `sidecar` column alone must not
    // classify it as a manifest -- only a non-empty drain does.
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
    // Manifest sidecar probe: the parsed-stats answer comes from a sidecar footer, exercising the
    // JSON drain-then-probe path (JSON) and the lazy parquet drain (parquet).
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

    /// A realistic requested stats schema mirroring the `*-struct-stats-only` fixtures
    /// (`id: long, value: string`): `numRecords` plus `minValues` / `maxValues` over the data
    /// columns. Compatibility then exercises real per-column type matching against the
    /// checkpoint's `add.stats_parsed`, not just the presence of the struct.
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
}
