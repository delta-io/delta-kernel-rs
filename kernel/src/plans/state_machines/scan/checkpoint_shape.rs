//! Scan-shape resolution.
//!
//! [`ScanShapeInfo::resolve`] is the canonical entry: an SM-driven async coroutine that turns
//! a [`Snapshot`]'s log segment + `_last_checkpoint` hint into a fully-populated
//! [`ScanShapeInfo`] (checkpoint topology + stats config + partition schema). The function
//! prefers the `_last_checkpoint` hint and falls back to engine-yielded
//! [`SchemaQuery`](crate::plans::state_machines::framework::phase_operation::PhaseOperation::SchemaQuery)
//! / [`Plans`](crate::plans::state_machines::framework::phase_operation::PhaseOperation::Plans)
//! phases only when the hint can't answer.
//!
//! ### Hint semantics (load-bearing)
//!
//! `_last_checkpoint`'s `checkpointSchema` describes the **leaf** file (the classic checkpoint, or
//! the V2 sidecar). It never carries layout info — a hint with `SIDECAR_NAME` would be meaningless
//! (sidecars contain pure `add`/`remove`, not sidecar pointers). Two consequences:
//!
//! - A hint-based stats probe is authoritative whenever it fires. The hint IS the leaf, so the
//!   sidecar SchemaQuery is redundant when the hint already answered (and we elide it).
//! - Layout (classic vs V2 manifest) is **not** in the hint. We must read the file: a top-level
//!   `SchemaQuery` for parquet; for JSON we assume manifest and let the `SIDECAR_NAME IS NOT NULL`
//!   filter in `register_reconciliation` sort out classic-JSON-with-inline-rows.

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
use crate::plans::kdf::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::schema::{arc_schema, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::{delta_error, FileMeta};

// === Public types ========================================================================

/// Topology of a snapshot's checkpoint(s), if any. Drives the checkpoint half of
/// [`super::plans::register_reconciliation`].
///
/// Variants are mutually exclusive and exhaustive — invariants that today's struct shape
/// has to defend at the call site (e.g. "sidecars present but manifest missing") are simply
/// unrepresentable here.
#[derive(Clone, Debug)]
pub(super) enum CheckpointShape {
    /// No checkpoint files in the segment. The reconciliation pipeline degenerates to
    /// commit-only replay.
    NoCheckpoint,
    /// Classic single- or multipart checkpoint with `add`/`remove` rows inline. The
    /// reconciliation pipeline scans `files` directly with the
    /// [`super::plans::CHECKPOINT_TOP`] schema.
    Scan {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
    /// V2 multipart: the manifest is already published as a relation, with sidecar URLs
    /// extracted in the same `Plans` phase. `register_reconciliation` `RelationRef`s the
    /// manifest and derives the sidecar load declaratively.
    Manifest { manifest_relation: RelationHandle },
}

/// Stats wiring for the action pipeline.
#[derive(Clone, Debug)]
pub(super) struct StatsInfo {
    pub stats_schema: Option<SchemaRef>,
    /// `true` when the checkpoint leaf (classic checkpoint or first V2 sidecar) carries a
    /// compatible `add.stats_parsed` column — so `augment_add` can pass the column through
    /// instead of running `parse_json(add.stats, stats_schema)` per row.
    pub has_parsed_stats: bool,
}

/// Configured shape of a scan/FSR pipeline: checkpoint topology + stats wiring +
/// partition schema. Resolved once per pipeline via [`Self::resolve`].
#[derive(Clone, Debug)]
pub(super) struct ScanShapeInfo {
    pub checkpoint: CheckpointShape,
    pub stats: StatsInfo,
    /// Union of (predicate partition columns) and (projection partition columns). When
    /// `Some`, `augment_add` always derives `partitionValues_parsed` via
    /// `map_to_struct(add.partitionValues)` — there's no native form to probe for (Delta
    /// checkpoint files never carry `partitionValues_parsed`), so no `has_parsed_partitions`
    /// flag exists.
    pub partition_schema: Option<SchemaRef>,
}

impl ScanShapeInfo {
    /// Canonical SM-driven scan-shape resolver shared by FSR and Scan plan builders.
    ///
    /// Yields at most:
    /// - one top-level [`PhaseOperation::SchemaQuery`] (parquet only, for layout detection),
    /// - one [`PhaseOperation::Plans`] (V2 multipart: publishes the manifest as [`CHECKPOINT_TOP`]
    ///   and extracts sidecar URLs in the same phase, so downstream stages reuse the relation
    ///   instead of re-scanning),
    /// - one [`PhaseOperation::SchemaQuery`] (sidecar probe — elided when the `_last_checkpoint`
    ///   hint already answered, since the hint IS the leaf and the sidecar IS the leaf for V2).
    pub(super) async fn resolve(
        ctx: &mut Context<'_>,
        snapshot: &Snapshot,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<SchemaRef>,
    ) -> Result<Self, DeltaError> {
        let seg = snapshot.log_segment();
        let checkpoint_parts = &seg.listed.checkpoint_parts;

        // (1) Empty -> NoCheckpoint, zero engine round-trips.
        if checkpoint_parts.is_empty() {
            return Ok(Self {
                checkpoint: CheckpointShape::NoCheckpoint,
                stats: StatsInfo {
                    stats_schema: stats_schema.cloned(),
                    has_parsed_stats: false,
                },
                partition_schema,
            });
        }

        let file_format = checkpoint_format_from_path(&checkpoint_parts[0]);
        let checkpoint_files: Vec<FileMeta> = checkpoint_parts
            .iter()
            .map(|p| p.location.clone())
            .collect();

        // (2) Hint probe -- leaf-level, hence authoritative whenever it fires.
        let mut parsed_stats: Option<bool> =
            stats_probe(seg.checkpoint_schema().as_ref(), stats_schema);

        // (3) Layout detection. The hint doesn't carry layout info; for parquet we
        //     SchemaQuery the checkpoint file, for JSON (no parquet footer) we assume manifest.
        //     If the parquet schema query happens to land on a classic leaf AND the hint
        //     didn't already answer, we refine the stats probe from the live schema.
        let is_manifest = match file_format {
            FileFormat::Parquet => {
                let url = first_checkpoint_url(snapshot)?;
                let state = ctx
                    .execute(
                        PhaseOperation::SchemaQuery(SchemaQueryNode::new(url)),
                        "ScanShapeInfo::resolve::checkpoint_schema",
                    )
                    .await
                    .map_err(|e| e.into_delta_typed())?;
                let cp_schema: SchemaRef = state.take_schema()?;
                let is_mfst = cp_schema.contains(SIDECAR_NAME);
                if !is_mfst && parsed_stats.is_none() {
                    parsed_stats = stats_probe(Some(&cp_schema), stats_schema);
                }
                is_mfst
            }
            FileFormat::Json => true,
        };

        // (4) Scan variant -- done.
        if !is_manifest {
            return Ok(Self {
                checkpoint: CheckpointShape::Scan {
                    files: checkpoint_files,
                    file_format,
                },
                stats: StatsInfo {
                    stats_schema: stats_schema.cloned(),
                    has_parsed_stats: parsed_stats.unwrap_or(false),
                },
                partition_schema,
            });
        }

        // (5) Manifest variant: always publish the manifest as `CHECKPOINT_TOP`. The relation
        //     handle is required by `register_reconciliation`, which derives sidecar URLs
        //     declaratively from it.
        let (manifest_relation, sidecar_files) =
            publish_v2_manifest_and_extract_sidecars(ctx, snapshot, file_format, &checkpoint_files)
                .await?;

        // (6) Sidecar stats probe -- ONLY when the hint hasn't already answered. The hint IS
        //     the leaf, and the sidecar IS the leaf for V2, so re-querying would be redundant.
        if parsed_stats.is_none() {
            if let (Some(reqd), Some(first)) = (stats_schema, sidecar_files.first()) {
                let state = ctx
                    .execute(
                        PhaseOperation::SchemaQuery(SchemaQueryNode::new(first.location.as_str())),
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

        Ok(Self {
            checkpoint: CheckpointShape::Manifest { manifest_relation },
            stats: StatsInfo {
                stats_schema: stats_schema.cloned(),
                has_parsed_stats: parsed_stats.unwrap_or(false),
            },
            partition_schema,
        })
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
                "ScanShapeInfo::resolve::first_checkpoint_url: snapshot has no checkpoint parts \
                 but layout probing was requested",
            )
        })
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    match cp.extension.as_str() {
        "json" => FileFormat::Json,
        _ => FileFormat::Parquet,
    }
}

/// Pure helper: `Some(true|false)` when both `leaf` (a checkpoint-leaf schema, e.g. from the
/// `_last_checkpoint` hint or a live `SchemaQuery`) and `requested` are present; `None` when
/// either is missing (i.e. "not yet known").
fn stats_probe(leaf: Option<&SchemaRef>, requested: Option<&SchemaRef>) -> Option<bool> {
    match (leaf, requested) {
        (Some(l), Some(r)) => Some(LogSegment::schema_has_compatible_stats_parsed(
            l.as_ref(),
            r.as_ref(),
        )),
        _ => None,
    }
}

/// Manifest scan schema used when publishing a V2 manifest as `CHECKPOINT_TOP`. Combines the
/// six action slots (so any inline `add`/`remove` rows in the manifest are still read) with
/// a top-level `sidecar` field so the sidecar filter in `register_reconciliation` can extract
/// sidecar URLs declaratively.
pub(super) fn checkpoint_manifest_scan_schema() -> SchemaRef {
    let mut fields: Vec<StructField> = action_schema().fields().cloned().collect();
    fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    arc_schema(fields)
}

/// V2 multipart: publish the manifest scan as [`CHECKPOINT_TOP`] and extract sidecar URLs in
/// one `Plans` phase. Returns `(manifest_handle, sidecar_files)`. Caller is responsible for
/// any follow-up sidecar SchemaQuery (e.g. the stats probe).
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

    // Sidecar extraction reads from the just-registered manifest relation. `consume_phase`
    // drains the registry's accumulated plans (the manifest publish plan) plus the consume
    // plan into a `PhaseOperation::Plans`, yields it, and extracts the typed output.
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
