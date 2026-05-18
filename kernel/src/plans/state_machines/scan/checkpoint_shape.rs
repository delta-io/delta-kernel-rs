//! Checkpoint shape resolution.
//!
//! [`resolve_checkpoint_shape`] is the canonical entry: an SM-driven async coroutine that
//! turns a [`Snapshot`]'s log segment + `_last_checkpoint` hint into a fully-populated
//! [`CheckpointShape`] (including `requested_stats_schema`, `has_stats_parsed`, and
//! `manifest_relation` for V2 multipart). The function prefers the `_last_checkpoint`
//! hint and falls back to engine-yielded
//! [`SchemaQuery`](crate::plans::state_machines::framework::phase_operation::PhaseOperation::SchemaQuery)
//! / [`Plans`](crate::plans::state_machines::framework::phase_operation::PhaseOperation::Plans)
//! phases only when the hint is insufficient.
//!
//! Sync helpers ([`checkpoint_shape_from_last_checkpoint`],
//! [`checkpoint_shape_from_schema`]) populate the cheap shape fields and leave
//! `(requested_stats_schema, has_stats_parsed, manifest_relation)` at their defaults; the
//! resolver enriches them.

use std::sync::Arc;

use super::plans::FSR_CHECKPOINT_TOP;
use super::schemas::action_schema;
use crate::actions::{
    Sidecar, ADD_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::expressions::col;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{FileFormat, RelationHandle, SinkType};
use crate::plans::ir::PlanBuilder;
use crate::plans::kdf::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::schema::{arc_schema, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::Snapshot;
use crate::{delta_error, FileMeta};

/// Resolved checkpoint encoding + schema hints. `actions_schema_subset` keys the top-level
/// checkpoint scan in [`super::plans::build_fsr_plans`]; `has_sidecars` decides whether
/// the FSR sidecar Load is appended to the plan vector.
///
/// `requested_stats_schema` carries the per-scan physical stats schema (the projection that
/// `data_skipping` would consume); `has_stats_parsed` records whether the leaf parquet has a
/// compatible native `add.stats_parsed` field, letting plan builders pick `col(["add",
/// "stats_parsed"])` instead of `parse_json(col(["add", "stats"]))`. `manifest_relation` is
/// the handle of the V2 multipart manifest scan published by
/// [`resolve_checkpoint_shape`], so downstream plan builders can `RelationRef` it instead of
/// re-scanning.
#[derive(Clone, Debug)]
pub struct CheckpointShape {
    pub file_format: FileFormat,
    pub has_sidecars: bool,
    pub actions_schema_subset: SchemaRef,
    pub requested_stats_schema: Option<SchemaRef>,
    pub has_stats_parsed: bool,
    pub manifest_relation: Option<RelationHandle>,
}

pub fn snapshot_has_checkpoint_files(snapshot: &Snapshot) -> bool {
    !snapshot.log_segment().listed.checkpoint_parts.is_empty()
}

pub fn first_checkpoint_url(snapshot: &Snapshot) -> Result<String, DeltaError> {
    snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .first()
        .map(|p| p.location.location.as_str().to_string())
        .ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::first_checkpoint_url: snapshot has no checkpoint parts but schema prelude \
                 was requested",
            )
        })
}

pub fn checkpoint_shape_from_schema(schema: &SchemaRef) -> Result<CheckpointShape, DeltaError> {
    // SchemaQuery is executed against an on-disk checkpoint part today — treat as Parquet-shaped
    // reads.
    let subset = checkpoint_actions_schema_projection(schema)?;
    Ok(CheckpointShape {
        file_format: FileFormat::Parquet,
        has_sidecars: schema.contains(SIDECAR_NAME),
        actions_schema_subset: subset,
        requested_stats_schema: None,
        has_stats_parsed: false,
        manifest_relation: None,
    })
}

pub fn checkpoint_shape_from_last_checkpoint(
    snapshot: &Snapshot,
) -> Result<CheckpointShape, DeltaError> {
    let seg = snapshot.log_segment();
    let has_checkpoint_parts = !seg.listed.checkpoint_parts.is_empty();
    let fmt = seg
        .listed
        .checkpoint_parts
        .first()
        .map(checkpoint_format_from_path)
        .unwrap_or(FileFormat::Json);

    let full_schema = seg.checkpoint_schema().unwrap_or_else(action_schema);
    let subset = checkpoint_actions_schema_projection(&full_schema)?;
    Ok(CheckpointShape {
        file_format: fmt,
        has_sidecars: full_schema.contains(SIDECAR_NAME)
            || (has_checkpoint_parts && matches!(fmt, FileFormat::Json)),
        actions_schema_subset: subset,
        requested_stats_schema: None,
        has_stats_parsed: false,
        manifest_relation: None,
    })
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    match cp.extension.as_str() {
        "json" => FileFormat::Json,
        _ => FileFormat::Parquet,
    }
}

fn checkpoint_actions_schema_projection(full: &SchemaRef) -> Result<SchemaRef, DeltaError> {
    const WANT: &[&str] = &[
        ADD_NAME,
        REMOVE_NAME,
        PROTOCOL_NAME,
        METADATA_NAME,
        DOMAIN_METADATA_NAME,
        SET_TRANSACTION_NAME,
    ];
    let names: Vec<_> = WANT.iter().copied().filter(|n| full.contains(*n)).collect();
    if names.is_empty() {
        Ok(action_schema())
    } else {
        full.project(names.as_slice())
            .map_err(|e| e.into_delta_default())
    }
}

fn sidecar_only_schema() -> SchemaRef {
    arc_schema([StructField::nullable(SIDECAR_NAME, Sidecar::to_schema())])
}

pub(super) fn checkpoint_manifest_scan_schema(include_sidecar: bool) -> SchemaRef {
    let mut fields: Vec<StructField> = action_schema().fields().cloned().collect();
    if include_sidecar {
        fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    }
    arc_schema(fields)
}

/// Canonical SM-driven checkpoint shape resolver. Used by both FSR and Scan plan builders.
///
/// Always derives the cheap fields from the snapshot's `_last_checkpoint` hint first; yields
/// at most one [`PhaseOperation::SchemaQuery`] (when the hint lacks a schema) and at most one
/// [`PhaseOperation::Plans`] (V2 multipart, to publish the manifest as a relation and extract
/// sidecar URLs in a single phase). Sets `requested_stats_schema = physical_stats_schema`
/// and populates `has_stats_parsed` from the most authoritative schema we observe (hint,
/// probed top-level parquet, or probed first sidecar parquet).
///
/// For V2 multipart, registers the manifest scan as the [`FSR_CHECKPOINT_TOP`] relation in
/// the context's registry so [`super::plans::build_fsr_plans`] reuses it (via
/// [`crate::plans::ir::relation_registry::RelationRegistry::relation_ref`]) instead of
/// scanning the manifest a second time.
pub(super) async fn resolve_checkpoint_shape(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    physical_stats_schema: Option<&SchemaRef>,
) -> Result<CheckpointShape, DeltaError> {
    let mut shape = checkpoint_shape_from_last_checkpoint(snapshot)?;
    shape.requested_stats_schema = physical_stats_schema.cloned();

    let seg = snapshot.log_segment();
    let hint_schema = seg.checkpoint_schema();

    // Hint-first stats detection: zero engine round-trips when `_last_checkpoint` carries the
    // schema. The hint reflects what the writer recorded for this checkpoint's actions, so a
    // compatible `add.stats_parsed` here means the leaf parquet has it too.
    if let (Some(hint), Some(reqd)) = (hint_schema.as_ref(), physical_stats_schema) {
        shape.has_stats_parsed = check_stats_parsed_compat(hint.as_ref(), reqd.as_ref());
    }

    // Fall back to a top-level SchemaQuery when the hint is missing but checkpoint files
    // exist. JSON checkpoints have no parquet footer to probe, so we skip the query and
    // conservatively assume sidecar-aware replay.
    let needs_top_level_query = snapshot_has_checkpoint_files(snapshot) && hint_schema.is_none();
    if needs_top_level_query {
        if shape.file_format == FileFormat::Json {
            // JSON checkpoints are ambiguous; treat as possibly-manifest.
            shape.has_sidecars = true;
        } else {
            let checkpoint_url = first_checkpoint_url(snapshot)?;
            let checkpoint_state = ctx
                .execute(
                    PhaseOperation::SchemaQuery(SchemaQueryNode::new(checkpoint_url)),
                    "fsr::resolve_checkpoint_shape::checkpoint_schema",
                )
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            let checkpoint_schema = checkpoint_state.take_schema()?;
            shape.has_sidecars = checkpoint_schema.contains(SIDECAR_NAME);
            shape.actions_schema_subset =
                checkpoint_actions_schema_projection(&checkpoint_schema)?;
            if let Some(reqd) = physical_stats_schema {
                shape.has_stats_parsed =
                    check_stats_parsed_compat(checkpoint_schema.as_ref(), reqd.as_ref());
            }
        }
    }

    // V2 multipart: publish the manifest as a reusable relation, extract sidecar URLs in the
    // same phase, then probe the first sidecar for `add.stats_parsed` when stats requested.
    if shape.has_sidecars {
        let manifest_handle =
            publish_v2_manifest_and_probe_sidecar(ctx, snapshot, &mut shape, physical_stats_schema)
                .await?;
        shape.manifest_relation = Some(manifest_handle);
    }

    Ok(shape)
}

/// Returns `true` iff the leaf parquet `add.stats_parsed` is type-compatible with the
/// requested stats schema. The compatibility check accepts widening (int→long,
/// long→timestamp, etc.) and missing/extra columns.
fn check_stats_parsed_compat(checkpoint_schema: &StructType, requested: &StructType) -> bool {
    LogSegment::schema_has_compatible_stats_parsed(checkpoint_schema, requested)
}

/// Yield a single [`PhaseOperation::Plans`] that (a) scans the V2 manifest into a registered
/// relation under [`FSR_CHECKPOINT_TOP`] and (b) extracts sidecar URLs via
/// [`SidecarCollector`]. Optionally yields a follow-up [`PhaseOperation::SchemaQuery`] on
/// the first sidecar to refresh `has_stats_parsed` from the actual sidecar parquet (sidecars
/// are always parquet, even when the manifest is JSON).
async fn publish_v2_manifest_and_probe_sidecar(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    shape: &mut CheckpointShape,
    physical_stats_schema: Option<&SchemaRef>,
) -> Result<RelationHandle, DeltaError> {
    let checkpoint_files: Vec<FileMeta> = snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();
    let manifest_schema = checkpoint_manifest_scan_schema(true);
    let manifest_scan = match shape.file_format {
        FileFormat::Parquet => {
            PlanBuilder::scan_parquet(checkpoint_files.clone(), manifest_schema.clone())
        }
        FileFormat::Json => {
            PlanBuilder::scan_json(checkpoint_files.clone(), manifest_schema.clone())
        }
    };
    let manifest_publish_plan = manifest_scan.into_relation(FSR_CHECKPOINT_TOP, &mut *ctx)?;
    let manifest_handle = match &manifest_publish_plan.sink {
        SinkType::Relation(h) => h.clone(),
        other => {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::resolve_checkpoint_shape: manifest_publish_plan must end in Relation sink, got {other:?}",
            ));
        }
    };

    // Sidecar extraction reads from the just-registered manifest relation.
    let (sidecar_extract_plan, extract_sidecars) = ctx
        .relation_ref(FSR_CHECKPOINT_TOP)?
        .filter(Arc::new(col([SIDECAR_NAME]).is_not_null().into()))
        .consume(SidecarCollector::new(
            snapshot.log_segment().log_root.clone(),
        ));

    let sidecar_state = ctx
        .execute(
            PhaseOperation::Plans(vec![manifest_publish_plan, sidecar_extract_plan]),
            "fsr::resolve_checkpoint_shape::publish_manifest_and_extract",
        )
        .await
        .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
    let sidecar_files = extract_sidecars.extract(&sidecar_state).map_err(|e| {
        let detail = e.display_with_source_chain();
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            source = e,
            "fsr::resolve_checkpoint_shape::extract_sidecars: {detail}",
        )
    })?;

    // Probe the first sidecar parquet for `add.stats_parsed` if stats requested. Sidecars
    // override the manifest's stats-parsed assessment because the actual `add` rows live
    // there for V2 multipart.
    if let (Some(reqd), Some(first_sidecar)) = (physical_stats_schema, sidecar_files.first()) {
        let sidecar_state = ctx
            .execute(
                PhaseOperation::SchemaQuery(SchemaQueryNode::new(first_sidecar.location.as_str())),
                "fsr::resolve_checkpoint_shape::sidecar_schema",
            )
            .await
            .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
        let sidecar_schema = sidecar_state.take_schema()?;
        shape.has_stats_parsed = check_stats_parsed_compat(sidecar_schema.as_ref(), reqd.as_ref());
    }

    Ok(manifest_handle)
}
