//! SSA-flavored scan plan builder.
//!
//! Mirrors [`super::file_scan::Scan::build_plans`] / [`super::file_scan::Scan::do_data_stage`]
//! but builds against the [`super::super::framework::plan_context`] (`Context` / `PlanBuilder`) API
//! and the SSA IR. The scan-side terminal projection (action_pair -> flat `scan_file_row`) and
//! the data-phase Load + logical projection are expressed as builder chains; reconciliation
//! upstream of the terminal is shared with FSR via [`super::ssa_reconciliation`].
//!
//! After PR8 deletes the legacy registry-based pipeline, the SMs in [`super::file_scan`]
//! will be retired in favor of the SSA SMs declared on `Scan` here.

use std::sync::Arc;

use super::action_pair::{project_scan_file_row, SCAN_BASE};
use super::dedup::scan_file_dedup_key;
use super::file_scan::scan_data_projection;
use super::ssa_reconciliation::execute_reconciliation_ssa;
use crate::expressions::ColumnName;
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{DvRef, FileType, ScanFileColumns};
use crate::plans::state_machines::framework::coroutine::context::Engine;
use crate::plans::state_machines::framework::plan_context::{Context, LoadSpec, PlanBuilder};
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;

// ============================================================================
// SSA scan builder
// ============================================================================

/// Build the SSA scan pipeline against `ctx`. Mirrors [`Scan::build_plans`] -- runs the
/// shared SSA reconciliation, then projects to the flat scan-file row shape and (when
/// `with_data`) appends the data phase.
///
/// Returns a [`PlanBuilder`] terminating on either:
/// - the live-actions stream (`with_data == false`), or
/// - the per-row data stream after parquet Load + logical-schema projection (`with_data == true`).
///
/// The caller wraps the returned builder with [`Context::into_result_plan`] to mint the
/// SM's terminal value.
pub(super) async fn build_scan_ssa(
    ctx: &Context,
    engine: &mut Engine,
    scan: &Scan,
    with_data: bool,
) -> Result<PlanBuilder, DeltaError> {
    let stats = scan.physical_stats_schema();
    // The data stage needs the full partition schema (see `data_stage_partition_schema`
    // doc on [`Scan`]); the predicate-narrowed `physical_partition_schema()` only works
    // for metadata-only execution.
    let parts = if with_data {
        scan.data_stage_partition_schema()
    } else {
        scan.physical_partition_schema()
    };

    // === Stages 1-5: shared reconciliation -> reconciled builder =========================
    let reconciled = execute_reconciliation_ssa(
        ctx,
        engine,
        scan.snapshot().as_ref(),
        &SCAN_BASE,
        stats,
        parts.clone(),
        Arc::new(scan_file_dedup_key()),
    )
    .await?;

    // === Scan-specific terminal projection: reconciled -> flat scan_file_row ==========
    let live_actions = project_scan_file_row(reconciled, parts.as_ref())?;

    // === Stage 6 (optional): data phase =================================================
    if with_data {
        do_data_stage_ssa(scan, live_actions)
    } else {
        Ok(live_actions)
    }
}

/// Append the SSA data stage onto the live-actions builder and return the data-stream
/// builder. Mirrors [`Scan::do_data_stage`].
///
/// Splits per-file: the upstream builder emits one row per surviving file (flat
/// `scan_file_row` shape), and [`PlanBuilder::load`] expands each row into the file's
/// per-record stream while broadcasting `path` and the `fileConstantValues` struct via
/// `passthrough_columns`. The trailing projection translates physical -> logical column
/// names per the scan's column-mapping mode.
fn do_data_stage_ssa(scan: &Scan, live_actions: PlanBuilder) -> Result<PlanBuilder, DeltaError> {
    let logical_schema = scan.logical_schema().clone();
    let logical_projection = scan_data_projection(scan.state_info())?;

    // Per-file parquet read; broadcasts the file-constant struct and `path` to every
    // emitted record-row. Output schema = physical_schema ++ {path, fileConstantValues}.
    let raw_data = live_actions.load(LoadSpec {
        file_schema: scan.physical_schema().clone(),
        file_type: FileType::Parquet,
        base_url: Some(scan.snapshot().table_root().clone()),
        passthrough_columns: vec![
            ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
            ColumnName::new(["path"]),
        ],
        file_meta: ScanFileColumns {
            path: ColumnName::new(["path"]),
            size: Some(ColumnName::new(["size"])),
            record_count: None,
        },
        dv_ref: Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
    })?;

    // Surviving projection: physical -> logical (column-mapping rename + metadata-column
    // synthesis). The kernel evaluator validates the expressions against the declared
    // `logical_schema` at lower time -- inference here would be insufficient (Transform
    // / RowId coalesce / etc. fall outside narrow inference's supported set).
    raw_data.project_with_schema(logical_projection, logical_schema)
}

// Helper module-internal tests are exercised via the engine's scan integration tests; the
// `scan_data_projection` logic itself is unit-tested in [`super::file_scan::tests`].
