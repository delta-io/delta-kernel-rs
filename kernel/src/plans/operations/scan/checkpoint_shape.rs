//! Scan-shape resolution: turns a [`Snapshot`] into a fully-populated [`ScanShapeInfo`]
//! (checkpoint topology + stats config + partition schema) via [`ScanShapeInfo::resolve`].
//!
//! The `_last_checkpoint` hint's `checkpointSchema` describes the **leaf** file (classic
//! checkpoint, or V2 sidecar) — never the manifest. So a hint-based stats probe is authoritative
//! whenever it fires (the sidecar SchemaQuery becomes redundant and is elided), but layout
//! (classic vs V2 manifest) must still be read: parquet via `SchemaQuery`, JSON assumed manifest
//! (`SIDECAR_NAME IS NOT NULL` filter downstream sorts out inline-row classic-JSON).

use std::sync::Arc;

use super::plans::CHECKPOINT_TOP;
use super::schemas::action_schema;
use crate::actions::{Sidecar, SIDECAR_NAME};
use crate::expressions::col;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::nodes::{FileFormat, RelationHandle};
use crate::plans::ir::PlanBuilder;
use crate::plans::kernel_consumers::SidecarCollector;
use crate::plans::operations::framework::coroutine::context::Context;
use crate::plans::operations::framework::step::{SchemaQueryNode, Step};
use crate::schema::{arc_schema, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::{delta_error, FileMeta};

// === Public types ========================================================================

/// Topology of a snapshot's checkpoint(s); drives the checkpoint half of
/// [`super::plans::register_reconciliation`].
#[derive(Clone, Debug)]
pub(super) enum CheckpointShape {
    /// No checkpoint files: reconciliation degenerates to commit-only replay.
    NoCheckpoint,
    /// Classic checkpoint with `add`/`remove` rows inline: scanned directly via `files`.
    Scan {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
    /// V2 multipart: manifest already published as `CHECKPOINT_TOP`; downstream stages
    /// derive sidecar URLs declaratively from `manifest_relation`.
    Manifest {
        // Read by SchemaQuery downstream (consumed by future PRs that wire it through).
        #[allow(dead_code)]
        manifest_relation: RelationHandle,
    },
}

/// Stats wiring. `has_parsed_stats` is `true` when the checkpoint leaf (classic checkpoint or
/// first V2 sidecar) carries a compatible `add.stats_parsed` column — letting `augment_add`
/// pass the column through instead of running `parse_json` per row.
#[derive(Clone, Debug)]
pub(super) struct StatsInfo {
    pub stats_schema: Option<SchemaRef>,
    pub has_parsed_stats: bool,
}

/// Configured pipeline shape. `partition_schema` is the union of partition columns needed by
/// the predicate and projection; when `Some`, `augment_add` derives `partitionValues_parsed`
/// via `map_to_struct` (Delta checkpoints never carry it natively).
#[derive(Clone, Debug)]
pub(super) struct ScanShapeInfo {
    pub checkpoint: CheckpointShape,
    pub stats: StatsInfo,
    pub partition_schema: Option<SchemaRef>,
}

impl ScanShapeInfo {
    /// Resolve the scan shape. Yields at most: one top-level `SchemaQuery` (parquet layout
    /// detection), one `Plans` (V2 manifest publish + sidecar URL extraction), and one sidecar
    /// `SchemaQuery` (elided when the `_last_checkpoint` hint already answered).
    pub(super) async fn resolve(
        ctx: &mut Context<'_>,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<SchemaRef>,
    ) -> Result<Self, DeltaError> {
        let seg = snapshot.log_segment();
        let checkpoint_parts = &seg.listed.checkpoint_parts;

        let make_info = |checkpoint, has_parsed_stats| Self {
            checkpoint,
            stats: StatsInfo {
                stats_schema: stats_schema.cloned(),
                has_parsed_stats,
            },
            partition_schema: partition_schema.clone(),
        };

        if checkpoint_parts.is_empty() {
            return Ok(make_info(CheckpointShape::NoCheckpoint, false));
        }

        let file_format = checkpoint_format_from_path(&checkpoint_parts[0]);
        let checkpoint_files: Vec<FileMeta> = checkpoint_parts
            .iter()
            .map(|p| p.location.clone())
            .collect();

        // Hint probe is authoritative — `_last_checkpoint` describes the leaf file.
        let mut parsed_stats: Option<bool> =
            stats_probe(seg.checkpoint_schema().as_ref(), stats_schema);

        // Layout detection: hint has no layout info, so probe parquet via SchemaQuery; JSON has
        // no parquet footer and is always treated as a manifest.
        let is_manifest = match file_format {
            FileFormat::Parquet => {
                let url = first_checkpoint_url(snapshot)?;
                let state = ctx
                    .execute(
                        Step::SchemaQuery(SchemaQueryNode::new(url)),
                        "ScanShapeInfo::resolve::checkpoint_schema",
                    )
                    .await
                    .map_err(|e| e.into_delta_typed())?;
                let cp_schema: SchemaRef = state.take_schema()?;
                let is_mfst = cp_schema.contains(SIDECAR_NAME);
                // On a classic leaf, refine the stats probe from the live schema if the hint
                // didn't already answer.
                if !is_mfst && parsed_stats.is_none() {
                    parsed_stats = stats_probe(Some(&cp_schema), stats_schema);
                }
                is_mfst
            }
            FileFormat::Json => true,
        };

        if !is_manifest {
            return Ok(make_info(
                CheckpointShape::Scan {
                    files: checkpoint_files,
                    file_format,
                },
                parsed_stats.unwrap_or(false),
            ));
        }

        let (manifest_relation, sidecar_files) =
            publish_v2_manifest_and_extract_sidecars(ctx, snapshot, file_format, &checkpoint_files)
                .await?;

        // Probe a sidecar only when the hint hasn't already answered — hint IS the leaf, and so
        // is the sidecar.
        if parsed_stats.is_none() {
            if let (Some(reqd), Some(first)) = (stats_schema, sidecar_files.first()) {
                let state = ctx
                    .execute(
                        Step::SchemaQuery(SchemaQueryNode::new(first.location.as_str())),
                        "ScanShapeInfo::resolve::sidecar_schema",
                    )
                    .await
                    .map_err(|e| e.into_delta_typed())?;
                let side_schema = state.take_schema()?;
                parsed_stats = Some(LogSegment::schema_has_compatible_stats_parsed(
                    side_schema.as_ref(),
                    reqd.as_ref(),
                ));
            }
        }

        Ok(make_info(
            CheckpointShape::Manifest { manifest_relation },
            parsed_stats.unwrap_or(false),
        ))
    }
}

// === Module-private helpers ==============================================================

fn first_checkpoint_url(snapshot: &Snapshot) -> Result<String, DeltaError> {
    snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .first()
        .map(|p| p.location.location.as_str().to_string())
        .ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "first_checkpoint_url: no checkpoint parts",
            )
        })
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    if cp.extension == "json" {
        FileFormat::Json
    } else {
        FileFormat::Parquet
    }
}

/// `Some(true|false)` when both a leaf schema and a requested schema are present; `None`
/// otherwise. Caller treats `None` as "not yet known".
fn stats_probe(leaf: Option<&SchemaRef>, requested: Option<&SchemaRef>) -> Option<bool> {
    let (l, r) = (leaf?, requested?);
    Some(LogSegment::schema_has_compatible_stats_parsed(
        l.as_ref(),
        r.as_ref(),
    ))
}

/// Manifest scan schema: six action slots (for inline `add`/`remove` rows) plus a `sidecar`
/// field for the sidecar URL extraction filter downstream.
fn checkpoint_manifest_scan_schema() -> SchemaRef {
    let mut fields: Vec<StructField> = action_schema().fields().cloned().collect();
    fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    arc_schema(fields)
}

/// Publish the V2 manifest as [`CHECKPOINT_TOP`] and extract sidecar URLs in one `Plans` phase.
async fn publish_v2_manifest_and_extract_sidecars(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    file_format: FileFormat,
    checkpoint_files: &[FileMeta],
) -> Result<(RelationHandle, Vec<FileMeta>), DeltaError> {
    let manifest_schema = checkpoint_manifest_scan_schema();
    let manifest_scan = match file_format {
        FileFormat::Parquet => {
            PlanBuilder::scan_parquet(checkpoint_files.to_vec(), manifest_schema.clone())
        }
        FileFormat::Json => {
            PlanBuilder::scan_json(checkpoint_files.to_vec(), manifest_schema.clone())
        }
    };
    let manifest_handle = manifest_scan.into_relation(CHECKPOINT_TOP, &mut *ctx)?;

    // Sidecar extraction reuses the manifest relation in the same `Plans` phase.
    let sidecar_chain = ctx
        .relation_ref(CHECKPOINT_TOP)?
        .filter(Arc::new(col([SIDECAR_NAME]).is_not_null().into()));
    let sidecar_files = ctx
        .consume_phase(
            sidecar_chain,
            SidecarCollector::new(snapshot.log_segment().log_root.clone()),
            "ScanShapeInfo::resolve::publish_manifest_and_extract",
        )
        .await?;

    Ok((manifest_handle, sidecar_files))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_probe_requires_both_inputs() {
        assert_eq!(stats_probe(None, None), None);
        // Missing requested -> None.
        let schema = crate::schema::arc_schema([crate::schema::StructField::not_null(
            "x",
            crate::schema::DataType::LONG,
        )]);
        assert_eq!(stats_probe(Some(&schema), None), None);
        assert_eq!(stats_probe(None, Some(&schema)), None);
    }
}
