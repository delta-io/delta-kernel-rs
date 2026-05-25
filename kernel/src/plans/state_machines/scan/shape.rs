//! Shape resolution for the scan reconciliation pipeline.
//!
//! Probes the snapshot's checkpoint topology (none / inline / V2 manifest), discovers the
//! checkpoint's on-disk stats layout (parsed struct vs JSON string), and packages those
//! findings as a [`ScanShape`] consumed by [`super::reconciliation::build_reconciliation`].
//!
//! [`ScanShape::resolve`] yields through the plan-construction dispatch surface
//! ([`Context::reduce`] / [`Context::schema_query`]); the build phase is purely synchronous.
//! At most one top-level `SchemaQuery`, one `Reduce` (V2 manifest sidecar URL extraction),
//! and one sidecar `SchemaQuery` are emitted.
//!
//! All types are `pub(super)` -- this module is internal IR for the scan/FSR pipeline.

use crate::actions::{Sidecar, SIDECAR_NAME};
use crate::expressions::col;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::FileFormat;
use crate::plans::kernel_reducers::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::Engine;
use crate::plans::state_machines::framework::plan_context::Context;
use crate::schema::{arc_schema, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::FileMeta;

/// Topology of a snapshot's checkpoint(s).
///
/// Two non-empty shapes, distinguished by where the leaf `add`/`remove` rows live:
/// `Leaf` keeps them in the checkpoint files themselves; `Manifest` keeps them in sidecar
/// parquet files referenced by the checkpoint.
#[derive(Clone, Debug)]
pub(super) enum CheckpointShape {
    /// No checkpoint files: reconciliation degenerates to commit-only replay.
    None,
    /// Classic checkpoint -- the checkpoint files ARE the leaves. `add`/`remove` rows
    /// are stored inline; the reconciliation re-scans `files` directly.
    Leaf {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
    /// V2 multipart manifest -- `files` are the manifest parts, the leaves live in
    /// sidecars. The reconciliation re-scans the manifest, filters down to sidecar
    /// pointer rows, and lazily loads the referenced sidecar parquet files via
    /// `NodeKind::Load`.
    Manifest {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
}

/// Configured reconciliation shape: checkpoint topology + stats + partitioning.
#[derive(Clone, Debug)]
pub(super) struct ScanShape {
    pub(super) checkpoint: CheckpointShape,
    /// Stats wiring. `None` means the caller didn't request stats. `Some` carries the
    /// projected stats schema along with whether the snapshot's checkpoint files surface
    /// column stats as a parsed struct (`add.stats_parsed`) vs a JSON string (`add.stats`).
    pub(super) stats: Option<StatsInfo>,
    pub(super) partition_schema: Option<SchemaRef>,
}

/// Stats wiring: the projected stats schema plus a flag indicating whether the snapshot's
/// checkpoint files store column stats as a parsed struct (`add.stats_parsed`, true) or as
/// a JSON string (`add.stats`, false).
#[derive(Clone, Debug)]
pub(super) struct StatsInfo {
    pub(super) schema: SchemaRef,
    pub(super) has_parsed_stats: bool,
}

impl ScanShape {
    /// Resolve the scan shape via a sequence of `EngineRequest::Reduce` (sidecar URL
    /// extraction) and `EngineRequest::SchemaQuery` (layout / stats probes) yields against
    /// `engine`.
    ///
    /// Yields through the plan-construction dispatch surface (`Context::reduce` /
    /// `Context::schema_query`). At most one top-level `SchemaQuery`, one `Reduce` (V2
    /// manifest sidecar URL extraction), and one sidecar `SchemaQuery` are emitted.
    pub(super) async fn resolve(
        ctx: &Context,
        engine: &mut Engine,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<SchemaRef>,
    ) -> Result<ScanShape, DeltaError> {
        let seg = snapshot.log_segment();
        let checkpoint_parts = &seg.listed.checkpoint_parts;

        let make_info = |checkpoint, has_parsed_stats| ScanShape {
            checkpoint,
            stats: stats_schema.cloned().map(|schema| StatsInfo {
                schema,
                has_parsed_stats,
            }),
            partition_schema: partition_schema.clone(),
        };

        if checkpoint_parts.is_empty() {
            return Ok(make_info(CheckpointShape::None, false));
        }

        let file_format = checkpoint_format_from_path(&checkpoint_parts[0]);
        let files: Vec<FileMeta> = checkpoint_parts
            .iter()
            .map(|p| p.location.clone())
            .collect();

        // Hint probe is authoritative -- `_last_checkpoint` describes the leaf file.
        let mut parsed_stats: Option<bool> =
            stats_probe(seg.checkpoint_schema().as_ref(), stats_schema);

        let is_manifest = match file_format {
            FileFormat::Parquet => {
                let url = checkpoint_parts[0].location.location.as_str().to_string();
                let cp_schema = ctx
                    .schema_query(engine, url, "ScanShape::resolve::checkpoint_schema")
                    .await?;
                let is_mfst = cp_schema.contains(SIDECAR_NAME);
                if !is_mfst && parsed_stats.is_none() {
                    parsed_stats = stats_probe(Some(&cp_schema), stats_schema);
                }
                is_mfst
            }
            // V2 spec only defines JSON for the top-level checkpoint manifest -- the
            // referenced sidecars are always Parquet. A JSON checkpoint part is therefore
            // unconditionally a manifest (no JSON-leaf code path exists). If a future
            // V2 revision introduces JSON sidecars this constant needs to gain the same
            // schema-based `SIDECAR_NAME` probe as the Parquet arm.
            FileFormat::Json => true,
        };

        if !is_manifest {
            return Ok(make_info(
                CheckpointShape::Leaf { files, file_format },
                parsed_stats.unwrap_or(false),
            ));
        }

        // V2 manifest: drain a `SidecarCollector` over (manifest_scan -> filter SIDECAR not
        // null) to recover sidecar URLs. The manifest is re-scanned in the build phase;
        // this scan is for the sidecar SchemaQuery probe only.
        let manifest_chain = match file_format {
            FileFormat::Parquet => ctx.scan_parquet(files.clone(), manifest_probe_schema())?,
            FileFormat::Json => ctx.scan_json(files.clone(), manifest_probe_schema())?,
        };
        let sidecar_chain = manifest_chain.filter(col([SIDECAR_NAME]).is_not_null())?;
        let sidecar_files = ctx
            .reduce(
                engine,
                sidecar_chain,
                SidecarCollector::new(snapshot.log_segment().log_root.clone()),
                "ScanShape::resolve::sidecar_extract",
            )
            .await?;

        // Sidecar SchemaQuery probe (only if stats are requested AND hint didn't already
        // answer).
        if parsed_stats.is_none() {
            if let (Some(reqd), Some(first)) = (stats_schema, sidecar_files.first()) {
                let side_schema = ctx
                    .schema_query(
                        engine,
                        first.location.as_str().to_string(),
                        "ScanShape::resolve::sidecar_schema",
                    )
                    .await?;
                parsed_stats = Some(LogSegment::schema_has_compatible_stats_parsed(
                    side_schema.as_ref(),
                    reqd.as_ref(),
                ));
            }
        }

        Ok(make_info(
            CheckpointShape::Manifest { files, file_format },
            parsed_stats.unwrap_or(false),
        ))
    }
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    if cp.extension == "json" {
        FileFormat::Json
    } else {
        FileFormat::Parquet
    }
}

fn stats_probe(leaf: Option<&SchemaRef>, requested: Option<&SchemaRef>) -> Option<bool> {
    let (l, r) = (leaf?, requested?);
    Some(LogSegment::schema_has_compatible_stats_parsed(
        l.as_ref(),
        r.as_ref(),
    ))
}

/// Manifest scan schema for the V2-multipart `is_manifest` probe in [`ScanShape::resolve`].
/// Only the `sidecar` column is consumed downstream (`SidecarCollector` reads
/// `sidecar.path` / `sidecar.sizeInBytes`); narrowing the scan to that one column avoids
/// paying the read cost for the action columns.
fn manifest_probe_schema() -> SchemaRef {
    arc_schema([StructField::nullable(SIDECAR_NAME, Sidecar::to_schema())])
}
