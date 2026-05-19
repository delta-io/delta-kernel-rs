//! Scan-time plan builders and state machines.
//!
//! Hosts the `impl Scan { ... }` block that builds the two-phase scan plans
//! ([`Scan::build_plans`] = metadata pipeline + optional data phase,
//! [`Scan::do_data_stage`] = data phase only). All public entry points are coroutine
//! state machines (`*_state_machine`); there is no sync API surface for scan plan building.

use std::collections::HashSet;
use std::sync::Arc;

use super::action_pair::scan_file_row_pair;
use super::dedup::scan_file_dedup_key;
use super::plans::{execute_reconciliation, RECONCILED};
use crate::delta_error;
use crate::expressions::{col, BinaryExpressionOp, ColumnName, Expression, Transform};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::nodes::{DvRef, FileType, RelationHandle, ScanFileColumns};
use crate::plans::ir::{RelationRegistry, ResultPlan};
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;
use crate::schema::{DataType, MetadataColumnSpec, SchemaRef, StructField};

// === Scan-side stage names ================================================================
//
// The pipeline shares COMMIT_RAW / COMMIT_DEDUP / CHECKPOINT_TOP / SIDECAR_ACTIONS / RECONCILED
// with the FSR pipeline via the bare-name convention; only the SM-side terminal names below
// are scan-specific.
/// Scan-side metadata terminal: post-reconciliation rows projected to the flat
/// `scan_file_row_pair` shape. Read by the data phase via `relation_ref(LIVE_ACTIONS)`.
pub(super) const LIVE_ACTIONS: &str = "live_actions";
/// Raw per-file rows emitted by the data-phase Load sink before logical projection.
const DATA_ROWS_RAW: &str = "data_rows_raw";
/// Final scan-data relation in the scan's logical schema.
const DATA: &str = "data";

impl Scan {
    /// Build the scan plans. When `with_data: false`, terminates at [`LIVE_ACTIONS`].
    /// When `with_data: true`, appends the data phase ([`Self::do_data_stage`]) and
    /// terminates at `data`.
    pub(super) async fn build_plans(
        &self,
        ctx: &mut Context<'_>,
        with_data: bool,
    ) -> Result<ResultPlan, DeltaError> {
        let stats = self.physical_stats_schema();
        let parts = self.physical_partition_schema();

        // === Stage 1-5: shared reconciliation -> RECONCILED ==============================
        execute_reconciliation(
            ctx,
            self.snapshot().as_ref(),
            &super::action_pair::SCAN_BASE,
            stats,
            parts.clone(),
            Arc::new(scan_file_dedup_key()),
        )
        .await?;

        // === Scan-specific terminal projection: RECONCILED -> LIVE_ACTIONS ===============
        // Project the action_pair-shaped reconciled rows into the flat scan file row.
        ctx.relation_ref(RECONCILED)?
            .project_pair(scan_file_row_pair(parts.as_ref()))
            .into_relation(LIVE_ACTIONS, &mut *ctx)?;

        // === Stage 6 (optional): data phase ===============================================
        if with_data {
            self.do_data_stage(ctx, LIVE_ACTIONS)
        } else {
            ctx.into_result_plan(LIVE_ACTIONS)
        }
    }

    /// Append the data stage: read parquet via Load, project to the scan's logical schema,
    /// terminate at `data`. `live_actions_name` is the registered name of the materialized
    /// live-actions relation (typically [`LIVE_ACTIONS`] within `execute`, or whatever name
    /// was used to `adopt` an externally-minted handle in the data-only SM).
    pub(super) fn do_data_stage(
        &self,
        ctx: &mut Context<'_>,
        live_actions_name: &str,
    ) -> Result<ResultPlan, DeltaError> {
        let snapshot = self.snapshot();
        let partition_columns: HashSet<String> = snapshot
            .table_configuration()
            .partition_columns()
            .iter()
            .filter(|name| self.logical_schema().contains(name.as_str()))
            .cloned()
            .collect();
        let logical_schema = self.logical_schema().clone();
        let logical_projection = scan_data_projection(
            &logical_schema,
            self.physical_schema(),
            &partition_columns,
            snapshot.table_configuration().column_mapping_mode(),
        )?;

        // Per-file parquet read keyed by the live-actions relation. Broadcasts the
        // file-constant struct and the path through to the projected output.
        ctx.relation_ref(live_actions_name)?.load(
            DATA_ROWS_RAW,
            self.physical_schema().clone(),
            FileType::Parquet,
            Some(snapshot.table_root().clone()),
            vec![
                ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
                ColumnName::new(["path"]),
            ],
            ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: Some(ColumnName::new(["size"])),
                record_count: None,
            },
            Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
            &mut *ctx,
        )?;
        // Final projection: physical → logical. The terminal `into_result_plan` registers
        // the projected output under `DATA` and drains the registry's accumulated plans
        // into a `ResultPlan` whose terminal relation is `DATA`.
        ctx.relation_ref(DATA_ROWS_RAW)?
            .project(logical_projection, logical_schema)
            .into_result_plan(DATA, &mut *ctx)
    }
}

// === SM entry points ======================================================================

impl Scan {
    /// Coroutine SM for metadata-only scan execution. Terminates at the live-actions
    /// relation.
    pub fn scan_metadata_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan_metadata", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id, "scan"));
            scan.build_plans(&mut ctx, /*with_data=*/ false).await
        })
    }

    /// Coroutine SM for data-only scan execution given a previously-materialized
    /// `live_actions_relation` handle (typically produced by `scan_metadata_state_machine`).
    /// Adopts the handle under [`LIVE_ACTIONS`] in the new registry, then runs the data
    /// phase via [`Self::do_data_stage`].
    pub fn scan_data_from_metadata_state_machine(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan_data", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id, "scan"));
            ctx.adopt(LIVE_ACTIONS, live_actions_relation)?;
            scan.do_data_stage(&mut ctx, LIVE_ACTIONS)
        })
    }

    /// Coroutine SM for combined metadata + data scan execution. Terminates at `data`.
    pub fn scan_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id, "scan"));
            scan.build_plans(&mut ctx, /*with_data=*/ true).await
        })
    }
}

// === Data-phase projection helpers =======================================================

pub(super) fn scan_data_projection(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    column_mapping_mode: crate::table_features::ColumnMappingMode,
) -> Result<Vec<Arc<Expression>>, DeltaError> {
    let row_index_name = StructField::default_row_index_column().name.clone();
    let mut physical_fields = physical_schema.fields();
    logical_schema
        .fields()
        .map(|field| {
            let expr = match field.get_metadata_column_spec() {
                Some(MetadataColumnSpec::RowIndex) => col([row_index_name.as_str()]),
                Some(MetadataColumnSpec::RowCommitVersion) => {
                    col([FILE_CONSTANT_VALUES_NAME, "defaultRowCommitVersion"])
                }
                Some(MetadataColumnSpec::RowId) => Expression::binary(
                    BinaryExpressionOp::Plus,
                    col([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
                    col([row_index_name.as_str()]),
                ),
                Some(MetadataColumnSpec::FilePath) => col(["path"]),
                None if partition_columns.contains(field.name()) => col([
                    FILE_CONSTANT_VALUES_NAME,
                    "partitionValues_parsed",
                    field.physical_name(column_mapping_mode),
                ]),
                None => {
                    let physical_field = physical_fields.next().ok_or_else(|| {
                        delta_error!(
                            DeltaErrorCode::DeltaCommandInvariantViolation,
                            "fsr::scan_data_projection: missing physical field for logical field `{}`",
                            field.name(),
                        )
                    })?;
                    // For struct-shaped logical fields, emit an identity `Transform` rooted at the
                    // physical column. This is the kernel IR primitive for "give me this struct,
                    // then reshape to the projection's declared output schema" -- it is exactly the
                    // case `Expression::Transform(t) if t.is_identity()` that
                    // `engine/arrow_expression::evaluate` already handles via `apply_schema`. Engines
                    // that lower to other runtimes (e.g. DataFusion) translate this identity
                    // transform into their native struct-construction primitive against the
                    // projection's output schema. Non-struct fields keep the bare `col(...)` form
                    // because their top-level rename is handled by the engine's per-field cast.
                    if matches!(field.data_type(), DataType::Struct(_)) {
                        Expression::Transform(Transform::new_nested([physical_field
                            .name()
                            .as_str()]))
                    } else {
                        col([physical_field.name().as_str()])
                    }
                }
            };
            Ok(expr.into())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::StructType;

    /// Helper: build a logical schema from `fields`, then derive the physical schema via
    /// `make_physical(Name)`. Returns `(logical, physical)`.
    fn schemas_with_cm_name(fields: Vec<StructField>) -> (SchemaRef, SchemaRef) {
        use crate::table_features::ColumnMappingMode;
        let logical = Arc::new(StructType::try_new(fields).unwrap());
        let physical = Arc::new(logical.make_physical(ColumnMappingMode::Name).unwrap());
        (logical, physical)
    }

    /// Helper: build a `StructField` annotated with the given physical name + a fresh column ID
    /// suitable for `make_physical(Name)`.
    fn annotated_field(
        logical_name: &str,
        physical_name: &str,
        column_id: i64,
        data_type: DataType,
        nullable: bool,
    ) -> StructField {
        use crate::schema::{ColumnMetadataKey, MetadataValue};
        StructField::new(logical_name, data_type, nullable).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(physical_name.to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(column_id),
            ),
        ])
    }

    /// Verifies the per-shape projection emitted by `scan_data_projection` for non-metadata,
    /// non-partition columns:
    ///
    /// - Primitive / list / map fields: a single top-level `col([physical_root])` reference.
    /// - Struct fields: an identity [`Expression::Transform`] rooted at the physical column.
    #[rstest::rstest]
    #[case::primitive(DataType::LONG, false)]
    #[case::nested_struct(DataType::Struct(Box::new(StructType::try_new(vec![
        annotated_field("inner1", "phys-inner1", 91, DataType::STRING, true),
        annotated_field("inner2", "phys-inner2", 92, DataType::INTEGER, true),
    ]).unwrap())), true)]
    fn scan_data_projection_emits_expected_shape(
        #[case] data_type: DataType,
        #[case] expect_transform: bool,
    ) {
        use crate::table_features::ColumnMappingMode;
        let (logical, physical) = schemas_with_cm_name(vec![annotated_field(
            "field", "col-phys", 1, data_type, true,
        )]);
        let exprs = scan_data_projection(
            &logical,
            &physical,
            &HashSet::new(),
            ColumnMappingMode::Name,
        )
        .unwrap();
        assert_eq!(exprs.len(), 1);
        if expect_transform {
            let expected = Expression::Transform(Transform::new_nested(["col-phys"]));
            assert_eq!(exprs[0].as_ref(), &expected);
        } else {
            assert_eq!(exprs[0].as_ref(), &col(["col-phys"]));
        }
    }
}
