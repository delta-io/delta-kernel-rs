//! Scan-time state machines and the shared data-phase projection helper.
//!
//! Hosts the `impl Scan { ... }` block that exposes the coroutine state machines for
//! metadata-only and combined metadata + data scans. The actual scan plan body is built
//! by [`super::scan_plan::build_scan_plan`].
//!
//! # Behavioral parity gap
//!
//! The visitor path (`Scan::scan_metadata` / `Scan::scan_data`) row-group-skips both the
//! checkpoint reads and the per-commit reads via the engine-provided meta predicate
//! returned by `Scan::build_actions_meta_predicate()`. The declarative pipeline does NOT
//! yet wire that predicate through; it reconciles every live add. This is intentional for
//! the initial slice but means predicated scans against very wide tables will read more
//! data than the visitor path. Tracked: wire `build_actions_meta_predicate()` into the
//! checkpoint/commit `LoadNode`s once the engine PR consumes the SM; the contract is
//! preserved in the visitor path until then.

use std::collections::HashSet;
use std::sync::Arc;

use super::scan_plan::build_scan_plan;
use crate::expressions::{col, Expression, Transform};
use crate::plans::errors::DeltaError;
use crate::plans::ir::plan::ResultPlan;
use crate::plans::state_machines::framework::coroutine::CoroutineSM;
use crate::plans::state_machines::framework::plan_context::Context;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::state_info::StateInfo;
use crate::scan::transform_spec::{row_id_coalesce_expr, FieldTransformSpec};
use crate::scan::Scan;
use crate::schema::{DataType, MetadataColumnSpec, StructField};

// === SM entry points ======================================================================

#[cfg(feature = "declarative-plans")]
impl Scan {
    /// Coroutine state machine for metadata-only scan execution.
    ///
    /// Builds the canonical scan pipeline (FSR reconciliation + flat `scan_file_row`
    /// projection) as a single plan against the builder API and yields a single
    /// [`ResultPlan`]. Engines drive this through `drive_to_dataframe`.
    pub fn scan_metadata_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan_metadata", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let live_actions =
                build_scan_plan(&ctx, &mut engine, &scan, /* with_data= */ false).await?;
            ctx.into_result_plan(live_actions)
        })
    }

    /// Coroutine state machine for combined metadata + data scan execution.
    ///
    /// Pipeline: shape-resolution yields, then reconciliation + flat scan-file
    /// projection + per-file Load + logical projection are appended into a single plan.
    pub fn scan_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let data = build_scan_plan(&ctx, &mut engine, &scan, /* with_data= */ true).await?;
            ctx.into_result_plan(data)
        })
    }
}

// === Data-phase projection ===============================================================

/// Build the per-logical-field projection list that converts raw Load output (physical schema +
/// broadcast file-constant struct + parquet-synthesized metadata columns) into the scan's
/// logical schema. Emits one expression per logical field, in order.
///
/// Classification is driven by [`StateInfo::transform_spec`] (the same source of truth as the
/// visitor path's `get_transform_expr`) plus each field's [`MetadataColumnSpec`]:
///
/// - Partition columns ([`FieldTransformSpec::MetadataDerivedColumn`]) ->
///   `fileConstantValues.partitionValues_parsed.<physical_name>`.
/// - `RowId` (paired with [`FieldTransformSpec::GenerateRowId`]) -> `coalesce(materialized,
///   baseRowId + row_index)` via [`row_id_coalesce_expr`], with `baseRowId` read from the
///   file-constant struct (vs. a literal in the visitor path).
/// - `RowIndex` -> `col([field.physical_name])`, the parquet-synthesized column under the user's
///   chosen name (not the kernel default `_metadata.row_index`).
/// - Regular fields -> `col([physical_name])`, except struct-typed fields which emit an identity
///   [`Expression::Transform`] for engines to lower to struct-reshape against the projection's
///   declared output schema.
///
/// Returns [`DeltaCommandInvariantViolation`](crate::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation) for shapes the SM data stage
/// does not support: [`FieldTransformSpec::DynamicColumn`] (CDF-only),
/// [`MetadataColumnSpec::FilePath`] (no DF synthesizer), and
/// [`MetadataColumnSpec::RowCommitVersion`] (rejected at `StateInfo::try_new`).
pub(super) fn scan_data_projection(
    state_info: &StateInfo,
) -> Result<Vec<Arc<Expression>>, DeltaError> {
    let logical_schema = &state_info.logical_schema;
    let column_mapping_mode = state_info.column_mapping_mode;

    // Index the spec once for O(1) per-field dispatch. Partition columns are keyed by
    // `field_index`; the at-most-one `GenerateRowId` entry feeds the `RowId` arm.
    // `StaticInsert` / `StaticDrop` don't affect the logical projection (the scan classifier
    // emits neither, and `StaticDrop` targets columns this projection doesn't read).
    let mut partition_indices: HashSet<usize> = HashSet::new();
    let mut row_id_spec: Option<&FieldTransformSpec> = None;
    if let Some(spec) = state_info.transform_spec.as_deref() {
        for entry in spec {
            match entry {
                FieldTransformSpec::MetadataDerivedColumn { field_index, .. } => {
                    partition_indices.insert(*field_index);
                }
                FieldTransformSpec::GenerateRowId { .. } => row_id_spec = Some(entry),
                FieldTransformSpec::StaticInsert { .. } | FieldTransformSpec::StaticDrop { .. } => {
                }
                FieldTransformSpec::DynamicColumn { .. } => {
                    return Err(DeltaError::invariant(
                        "fsr::scan_data_projection: DynamicColumn entries are emitted only \
                         by the CDF classifier; the scan data stage does not run for CDF",
                    ));
                }
            }
        }
    }

    logical_schema
        .fields()
        .enumerate()
        .map(|(field_index, field)| {
            let expr = match field.get_metadata_column_spec() {
                Some(MetadataColumnSpec::RowIndex) => {
                    // Metadata columns aren't subject to column mapping, so `physical_name`
                    // returns the user-supplied field name -- the same name the parquet
                    // reader stamps onto the synthesized column.
                    col([field.physical_name(column_mapping_mode)])
                }
                Some(MetadataColumnSpec::RowId) => row_id_expr(field, row_id_spec)?,
                Some(
                    spec @ (MetadataColumnSpec::FilePath | MetadataColumnSpec::RowCommitVersion),
                ) => return Err(unsupported_metadata_column(field, spec)),
                None if partition_indices.contains(&field_index) => col([
                    FILE_CONSTANT_VALUES_NAME,
                    "partitionValues_parsed",
                    field.physical_name(column_mapping_mode),
                ]),
                None => {
                    // Struct fields wrap in identity `Transform` so engines reshape to the
                    // declared output schema (logical names + nullability/order). Non-structs
                    // are renamed by the engine's per-field cast.
                    let physical_name = field.physical_name(column_mapping_mode);
                    if matches!(field.data_type(), DataType::Struct(_)) {
                        Expression::Transform(Transform::new_nested([physical_name]))
                    } else {
                        col([physical_name])
                    }
                }
            };
            Ok(expr.into())
        })
        .collect()
}

/// Build the `RowId` projection expression for `field`, matched against the spec's at-most-one
/// [`FieldTransformSpec::GenerateRowId`] entry. `baseRowId` is read from the per-file
/// file-constant struct (the SM pipeline doesn't know the value at projection-build time).
fn row_id_expr(
    field: &StructField,
    row_id_spec: Option<&FieldTransformSpec>,
) -> Result<Expression, DeltaError> {
    let Some(FieldTransformSpec::GenerateRowId {
        field_name,
        row_index_field_name,
    }) = row_id_spec
    else {
        return Err(DeltaError::invariant(format!(
            "fsr::scan_data_projection: RowId column `{}` has no GenerateRowId entry in \
             the transform spec",
            field.name(),
        )));
    };
    Ok(row_id_coalesce_expr(
        field_name,
        row_index_field_name,
        col([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
    ))
}

/// Typed error for metadata-column specs the scan SM data stage does not support; see
/// [`scan_data_projection`]'s doc for the full rationale.
fn unsupported_metadata_column(field: &StructField, spec: MetadataColumnSpec) -> DeltaError {
    DeltaError::invariant(format!(
        "fsr::scan_data_projection: metadata column `{}` ({:?}) is not supported in the \
         scan data stage",
        field.name(),
        spec,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::expressions::BinaryExpressionOp;
    use crate::plans::errors::DeltaErrorCode;
    use crate::scan::state_info::tests::{
        get_simple_state_info, get_state_info, ROW_TRACKING_FEATURES,
    };
    use crate::schema::{ColumnMetadataKey, MetadataValue, StructField, StructType};

    // === Helpers ==========================================================================

    /// Build a `StructField` annotated with the given physical name + column ID, suitable for
    /// `column_mapping_mode = Name`.
    fn annotated_field(
        logical_name: &str,
        physical_name: &str,
        column_id: i64,
        data_type: DataType,
        nullable: bool,
    ) -> StructField {
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

    /// Metadata configuration that turns on column mapping mode `Name`.
    fn cm_name_metadata() -> HashMap<String, String> {
        HashMap::from([("delta.columnMapping.mode".to_string(), "name".to_string())])
    }

    /// Metadata configuration required by `StateInfo::try_new` to accept a `RowId` metadata
    /// column, parameterised by the materialized row-id column name.
    fn row_tracking_metadata(materialized_row_id_col: &str) -> HashMap<String, String> {
        HashMap::from([
            ("delta.enableRowTracking".to_string(), "true".to_string()),
            (
                "delta.rowTracking.materializedRowIdColumnName".to_string(),
                materialized_row_id_col.to_string(),
            ),
            (
                "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
                "some_row_commit_version_col".to_string(),
            ),
        ])
    }

    // === Tests ============================================================================

    /// Verifies the per-shape projection emitted for non-metadata, non-partition columns:
    ///
    /// - Primitive / list / map fields: a single top-level `col([physical_root])` reference.
    /// - Struct fields: an identity [`Expression::Transform`] rooted at the physical column.
    #[rstest::rstest]
    #[case::primitive(DataType::LONG, false)]
    #[case::nested_struct(StructType::try_new(vec![
        annotated_field("inner1", "phys-inner1", 91, DataType::STRING, true),
        annotated_field("inner2", "phys-inner2", 92, DataType::INTEGER, true),
    ]).unwrap().into(), true)]
    fn scan_data_projection_emits_expected_shape(
        #[case] data_type: DataType,
        #[case] expect_transform: bool,
    ) {
        let fields = vec![annotated_field("field", "col-phys", 1, data_type, true)];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let state_info =
            get_state_info(schema, vec![], None, &[], cm_name_metadata(), vec![]).unwrap();
        let exprs = scan_data_projection(&state_info).unwrap();
        assert_eq!(exprs.len(), 1);
        if expect_transform {
            let expected = Expression::Transform(Transform::new_nested(["col-phys"]));
            assert_eq!(exprs[0].as_ref(), &expected);
        } else {
            assert_eq!(exprs[0].as_ref(), &col(["col-phys"]));
        }
    }

    #[test]
    fn scan_data_projection_partition_column() {
        let fields = vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("date", DataType::DATE),
        ];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let state_info = get_simple_state_info(schema, vec!["date".to_string()]).unwrap();
        let exprs = scan_data_projection(&state_info).unwrap();
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0].as_ref(), &col(["id"]));
        assert_eq!(
            exprs[1].as_ref(),
            &col([FILE_CONSTANT_VALUES_NAME, "partitionValues_parsed", "date"]),
        );
    }

    #[test]
    fn scan_data_projection_file_path_errors() {
        let fields = vec![StructField::nullable("id", DataType::STRING)];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let metadata_cols = vec![("my_path", MetadataColumnSpec::FilePath)];
        let state_info =
            get_state_info(schema, vec![], None, &[], HashMap::new(), metadata_cols).unwrap();
        let err = scan_data_projection(&state_info)
            .expect_err("FilePath metadata column should not be projected");
        assert_eq!(err.code, DeltaErrorCode::DeltaCommandInvariantViolation);
        assert!(
            err.message.contains("metadata column `my_path` (FilePath)"),
            "unexpected error message: {}",
            err.message,
        );
    }

    /// Regression: row index must use the user-supplied field name, not `_metadata.row_index`.
    #[test]
    fn scan_data_projection_user_named_row_index() {
        let fields = vec![StructField::nullable("id", DataType::STRING)];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let metadata_cols = vec![("my_row_idx", MetadataColumnSpec::RowIndex)];
        let state_info =
            get_state_info(schema, vec![], None, &[], HashMap::new(), metadata_cols).unwrap();
        let exprs = scan_data_projection(&state_info).unwrap();
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0].as_ref(), &col(["id"]));
        assert_eq!(exprs[1].as_ref(), &col(["my_row_idx"]));
    }

    /// Regression: `RowId` must use `coalesce(materialized, baseRowId + row_index)` with the
    /// classifier-synthesized row-index column name from the spec.
    #[test]
    fn scan_data_projection_row_id_synthesized_index() {
        let fields = vec![StructField::nullable("id", DataType::STRING)];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let metadata_cols = vec![("row_id", MetadataColumnSpec::RowId)];
        let metadata = row_tracking_metadata("some_row_id_col");
        let state_info = get_state_info(
            schema,
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            metadata,
            metadata_cols,
        )
        .unwrap();
        let exprs = scan_data_projection(&state_info).unwrap();
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0].as_ref(), &col(["id"]));
        let expected_row_id = Expression::coalesce([
            col(["some_row_id_col"]),
            Expression::binary(
                BinaryExpressionOp::Plus,
                col([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
                col(["row_indexes_for_row_id_0"]),
            ),
        ]);
        assert_eq!(exprs[1].as_ref(), &expected_row_id);
    }

    /// `RowId` reuses the user's `RowIndex` column when both are requested -- the
    /// `GenerateRowId` spec carries the same `row_index_field_name`.
    #[test]
    fn scan_data_projection_row_id_with_explicit_index() {
        let fields = vec![StructField::nullable("id", DataType::STRING)];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let metadata_cols = vec![
            ("row_id", MetadataColumnSpec::RowId),
            ("row_index", MetadataColumnSpec::RowIndex),
        ];
        let metadata = row_tracking_metadata("some_row_id_col");
        let state_info = get_state_info(
            schema,
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            metadata,
            metadata_cols,
        )
        .unwrap();
        let exprs = scan_data_projection(&state_info).unwrap();
        assert_eq!(exprs.len(), 3);
        assert_eq!(exprs[0].as_ref(), &col(["id"]));
        let expected_row_id = Expression::coalesce([
            col(["some_row_id_col"]),
            Expression::binary(
                BinaryExpressionOp::Plus,
                col([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
                col(["row_index"]),
            ),
        ]);
        assert_eq!(exprs[1].as_ref(), &expected_row_id);
        assert_eq!(exprs[2].as_ref(), &col(["row_index"]));
    }
}
