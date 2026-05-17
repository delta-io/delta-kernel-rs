//! Checkpoint shape resolution helpers.
//!
//! Encapsulates the [`CheckpointShape`] descriptor and the discovery functions that turn a
//! [`Snapshot`]'s log segment / `_last_checkpoint` hints into a concrete shape. The async
//! [`resolve_checkpoint_shape_for_scan`] runs schema-query and sidecar-discovery phases
//! when `_last_checkpoint` does not carry sufficient hints; the synchronous functions are
//! the static counterparts used by
//! [`FullStateBuilder::build`](super::full_state::FullStateBuilder).

use std::sync::Arc;

use super::schemas::action_schema;
use crate::actions::{
    Sidecar, ADD_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::expressions::Expression;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::FileFormat;
use crate::plans::ir::{Extractor, Plan, PlanBuilder};
use crate::plans::kdf::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::scan::Scan;
use crate::schema::{arc_schema, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::{delta_error, FileMeta};

/// Resolved checkpoint encoding + schema hints. `actions_schema_subset` keys the top-level
/// checkpoint scan in [`super::plans::build_fsr_plans`]; `has_sidecars` decides whether
/// the FSR sidecar Load is appended to the plan vector.
#[derive(Clone, Debug)]
pub struct CheckpointShape {
    pub file_format: FileFormat,
    pub has_sidecars: bool,
    pub actions_schema_subset: SchemaRef,
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

pub(super) async fn resolve_checkpoint_shape_for_scan(
    phase: &mut Phase<'_>,
    scan: &Scan,
) -> Result<CheckpointShape, DeltaError> {
    let snapshot = scan.snapshot();
    let mut shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref())?;

    // SchemaQuery is needed only when checkpoint files exist but `_last_checkpoint` did not
    // include a schema hint.
    let needs_schema_query = snapshot_has_checkpoint_files(snapshot.as_ref())
        && snapshot.log_segment().checkpoint_schema().is_none();
    if needs_schema_query {
        if shape.file_format == FileFormat::Json {
            // JSON checkpoints are ambiguous without `_last_checkpoint` schema hints:
            // they may be manifest rows that carry sidecar pointers. SchemaQuery reads
            // parquet footers only, so for JSON we must conservatively treat this as
            // possibly-manifest and proceed through sidecar-aware replay.
            shape.has_sidecars = true;
        } else {
            let checkpoint_url = first_checkpoint_url(snapshot.as_ref())?;
            let checkpoint_state = phase
                .execute(
                    PhaseOperation::SchemaQuery(SchemaQueryNode::new(checkpoint_url)),
                    "scan::resolve_checkpoint_shape_for_scan::checkpoint_schema",
                )
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            let checkpoint_schema = checkpoint_state.take_schema().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "scan::resolve_checkpoint_shape_for_scan::checkpoint_schema: schema query \
                     phase returned no schema",
                )
            })?;
            shape = checkpoint_shape_from_schema(&checkpoint_schema)?;
        }

        if shape.has_sidecars {
            let (discover_sidecars, extract_sidecars) =
                build_sidecar_discovery_plan(snapshot.as_ref(), shape.file_format)?;
            let sidecar_state = phase
                .execute(
                    PhaseOperation::Plans(vec![discover_sidecars]),
                    "scan::resolve_checkpoint_shape_for_scan::sidecar_discovery",
                )
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            let sidecar_files = extract_sidecars.extract(&sidecar_state).map_err(|e| {
                let detail = e.display_with_source_chain();
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    source = e,
                    "scan::resolve_checkpoint_shape_for_scan::extract_sidecars: {detail}",
                )
            })?;
            if let Some(first_sidecar) = sidecar_files.first() {
                let sidecar_state = phase
                    .execute(
                        PhaseOperation::SchemaQuery(SchemaQueryNode::new(
                            first_sidecar.location.as_str(),
                        )),
                        "scan::resolve_checkpoint_shape_for_scan::sidecar_schema",
                    )
                    .await
                    .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
                let sidecar_schema = sidecar_state.take_schema().ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "scan::resolve_checkpoint_shape_for_scan::sidecar_schema: sidecar schema \
                         query phase returned no schema",
                    )
                })?;
                let mut sidecar_shape = checkpoint_shape_from_schema(&sidecar_schema)?;
                // We are in the v2 sidecar branch by construction.
                sidecar_shape.has_sidecars = true;
                shape = sidecar_shape;
            }
        }
    }

    Ok(shape)
}

pub(super) fn build_sidecar_discovery_plan(
    snapshot: &Snapshot,
    checkpoint_format: FileFormat,
) -> Result<(Plan, Extractor<Vec<FileMeta>>), DeltaError> {
    let checkpoint_files: Vec<FileMeta> = snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();
    let sidecar_scan = match checkpoint_format {
        FileFormat::Parquet => PlanBuilder::scan_parquet(checkpoint_files, sidecar_only_schema()),
        FileFormat::Json => PlanBuilder::scan_json(checkpoint_files, sidecar_only_schema()),
    }
    .filter(Arc::new(
        Expression::column(["sidecar"]).is_not_null().into(),
    ));
    Ok(sidecar_scan.consume(SidecarCollector::new(
        snapshot.log_segment().log_root.clone(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::ir::nodes::SinkType;
    use crate::utils::test_utils::load_test_table;

    #[test]
    fn sidecar_discovery_plan_uses_consume_sink() {
        let (_engine, snapshot, _tmp) =
            load_test_table("v2-parquet-sidecars-struct-stats-only").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref()).unwrap();
        assert!(shape.has_sidecars);
        let (plan, _extract) =
            build_sidecar_discovery_plan(snapshot.as_ref(), shape.file_format).unwrap();
        assert!(matches!(plan.sink, SinkType::Consume(_)));
    }
}
