//! SSA-flavored scan plan builder.
//!
//! Mirrors [`super::file_scan::Scan::build_plans`] / [`super::file_scan::Scan::do_data_stage`]
//! but builds against the [`super::super::framework::plan_context`] (`Context` / `PlanBuilder`) API
//! and the SSA IR. The scan-side terminal projection (reconciled rows -> flat `scan_file_row`)
//! and the data-phase Load + logical projection are expressed as builder chains; reconciliation
//! upstream of the terminal is shared with FSR via [`super::ssa_reconciliation`].
//!
//! After PR8 deletes the legacy registry-based pipeline, the SMs in [`super::file_scan`]
//! will be retired in favor of the SSA SMs declared on `Scan` here.

use std::sync::Arc;

use super::file_scan::scan_data_projection;
use super::ssa_reconciliation::{execute_reconciliation_ssa, scan_file_dedup_key, SCAN_BASE};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::ADD_NAME;
use crate::expressions::{col, ColumnName, Expression};
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{default_scan_file_columns, DvRef, FileType};
use crate::plans::state_machines::framework::coroutine::context::Engine;
use crate::plans::state_machines::framework::plan_context::{Context, LoadSpec, PlanBuilder};
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;
use crate::schema::{DataType, MapType, SchemaRef, StructField, StructType, ToSchema};

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
        file_meta: default_scan_file_columns(),
        dv_ref: Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
    })?;

    // Surviving projection: physical -> logical (column-mapping rename + metadata-column
    // synthesis). The kernel evaluator validates the expressions against the declared
    // `logical_schema` at lower time -- inference here would be insufficient (Transform
    // / RowId coalesce / etc. fall outside narrow inference's supported set).
    raw_data.project_with_schema(logical_projection, logical_schema)
}

// ============================================================================
// Scan terminal: reconciled builder -> flat scan_file_row
// ============================================================================

/// Project the reconciled action stream into the flat `scan_file_row` shape consumed by
/// the SSA scan data stage:
///
/// ```text
/// {
///   path: STRING NOT NULL,
///   size: LONG NOT NULL,
///   deletionVector: DV?,
///   fileConstantValues: STRUCT<
///     baseRowId: LONG?,
///     defaultRowCommitVersion: LONG?,
///     tags: MAP<STRING,STRING>?,
///     clusteringProvider: STRING?,
///     partitionValues_parsed?: STRUCT<...>,  // present iff `partitions` is Some
///   >?,
/// }
/// ```
///
/// **Invariant:** when `partitions` is `Some(parts)`, the upstream reconciliation pipeline
/// must have already replaced `add.partitionValues` with `add.partitionValues_parsed`
/// (see `ssa_reconciliation::ReconciliationPlanBuilder::with_partitions_parsed`). The terminal
/// reads `col(["add", "partitionValues_parsed"])` directly -- no per-row
/// `map_to_struct(add.partitionValues)` here. The raw Map form has no downstream consumer
/// in the SSA path, so it is omitted from `fileConstantValues` entirely (parsing happens
/// once upstream, not per file row in the data phase).
fn project_scan_file_row(
    builder: PlanBuilder,
    partitions: Option<&SchemaRef>,
) -> Result<PlanBuilder, DeltaError> {
    let tags = MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    let mut file_constant_exprs: Vec<Arc<Expression>> = vec![
        col([ADD_NAME, "baseRowId"]).into(),
        col([ADD_NAME, "defaultRowCommitVersion"]).into(),
        col([ADD_NAME, "tags"]).into(),
        col([ADD_NAME, "clusteringProvider"]).into(),
    ];
    if let Some(parts) = partitions {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            parts.as_ref().clone(),
        ));
        file_constant_exprs.push(col([ADD_NAME, "partitionValues_parsed"]).into());
    }

    let schema = Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(
            FILE_CONSTANT_VALUES_NAME,
            StructType::new_unchecked(file_constant_fields),
        ),
    ]));
    let exprs: Vec<Arc<Expression>> = vec![
        col([ADD_NAME, "path"]).into(),
        col([ADD_NAME, "size"]).into(),
        col([ADD_NAME, "deletionVector"]).into(),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ];
    builder.project_with_schema(exprs, schema)
}

// Helper module-internal tests are exercised via the engine's scan integration tests; the
// `scan_data_projection` logic itself is unit-tested in [`super::file_scan::tests`].

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{Remove, REMOVE_NAME};
    use crate::schema::arc_schema;

    /// Build a synthetic input builder whose schema mimics the reconciled action stream
    /// (post-`with_partitions_parsed`): `add` carries `path`/`size`/`deletionVector`/
    /// `baseRowId`/etc., plus `partitionValues_parsed` when partitions are present. The
    /// terminal projection reads only those field names regardless of how the rest of the
    /// upstream looks.
    fn make_input_builder(parts: Option<&SchemaRef>) -> (Context, PlanBuilder) {
        let tags = MapType::new(DataType::STRING, DataType::STRING, true);
        let mut add_fields = vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
            StructField::nullable("baseRowId", DataType::LONG),
            StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            StructField::nullable("tags", tags),
            StructField::nullable("clusteringProvider", DataType::STRING),
        ];
        if let Some(p) = parts {
            add_fields.push(StructField::nullable(
                "partitionValues_parsed",
                p.as_ref().clone(),
            ));
        }
        let schema = arc_schema([
            StructField::nullable(ADD_NAME, StructType::new_unchecked(add_fields)),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        ]);
        let ctx = Context::new();
        let builder = ctx
            .values(schema, Vec::<Vec<crate::expressions::Scalar>>::new())
            .unwrap();
        (ctx, builder)
    }

    /// Without partitions: the terminal emits the four-field `fileConstantValues` and
    /// drops the `partitionValues` Map slot.
    #[test]
    fn project_scan_file_row_drops_partition_values_map_when_no_parts() {
        let (_ctx, builder) = make_input_builder(None);
        let out = project_scan_file_row(builder, None).unwrap();
        let schema = out.schema().unwrap();
        let fields: Vec<_> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(
            fields,
            ["path", "size", "deletionVector", FILE_CONSTANT_VALUES_NAME]
        );
        let DataType::Struct(fcv) = schema.field(FILE_CONSTANT_VALUES_NAME).unwrap().data_type()
        else {
            panic!("fileConstantValues must be a struct");
        };
        let fcv_fields: Vec<_> = fcv.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(
            fcv_fields,
            [
                "baseRowId",
                "defaultRowCommitVersion",
                "tags",
                "clusteringProvider",
            ],
            "partitionValues Map slot must be dropped when no partitions",
        );
    }

    /// With partitions: `fileConstantValues.partitionValues_parsed` is appended as a
    /// passthrough column ref (not `map_to_struct`); the Map slot remains absent.
    #[test]
    fn project_scan_file_row_appends_partitions_parsed_when_some() {
        let parts = arc_schema([StructField::nullable("p", DataType::STRING)]);
        let (_ctx, builder) = make_input_builder(Some(&parts));
        let out = project_scan_file_row(builder, Some(&parts)).unwrap();
        let schema = out.schema().unwrap();
        let DataType::Struct(fcv) = schema.field(FILE_CONSTANT_VALUES_NAME).unwrap().data_type()
        else {
            panic!("fileConstantValues must be a struct");
        };
        let fcv_fields: Vec<_> = fcv.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(
            fcv_fields,
            [
                "baseRowId",
                "defaultRowCommitVersion",
                "tags",
                "clusteringProvider",
                "partitionValues_parsed",
            ],
            "partitions present => partitionValues_parsed appended; Map slot stays absent",
        );
    }
}
