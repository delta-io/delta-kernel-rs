//! Scan plan builder.
//!
//! Builds the scan pipeline against [`Context`] / [`PlanBuilder`] and the kernel plan IR:
//! shared reconciliation, terminal projection to flat `scan_file_row`, and an optional data
//! phase (Load + logical projection). Reconciliation upstream of the terminal is shared with
//! FSR via [`super::reconciliation`].

use std::sync::Arc;

use super::file_scan::scan_data_projection;
use super::reconciliation::{execute_reconciliation, scan_file_dedup_key, SCAN_BASE};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::ADD_NAME;
use crate::expressions::{col, ColumnName, Expression};
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{default_scan_file_columns, DvRef, FileType, LoadNode};
use crate::plans::state_machines::framework::coroutine::Engine;
use crate::plans::state_machines::framework::plan_context::{Context, PlanBuilder};
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;
use crate::schema::{DataType, MapType, SchemaRef, StructField, StructType, ToSchema};

// ============================================================================
// Scan plan builder
// ============================================================================

/// Build the scan plan against `ctx`: shared reconciliation, terminal projection to flat
/// `scan_file_row`, and (when `with_data`) the data phase.
///
/// Returns a [`PlanBuilder`] terminating on either:
/// - the live-actions stream (`with_data == false`), or
/// - the per-row data stream after parquet Load + logical-schema projection (`with_data == true`).
///
/// The caller wraps the returned builder with [`Context::into_result_plan`] to mint the
/// SM's terminal value.
pub(super) async fn build_scan_plan(
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
    let reconciled = execute_reconciliation(
        ctx,
        engine,
        scan.snapshot().as_ref(),
        &SCAN_BASE,
        stats,
        parts.clone(),
        Arc::new(scan_file_dedup_key()),
    )
    .await?;

    // The reconciled stream still carries surviving `remove` rows (they participate in
    // dedup but never produce scan output). The terminal projection below reads
    // `col(["add", "path"])` directly and declares `path` as `not_null`, so we MUST drop
    // remove-only rows before projecting -- otherwise the kernel evaluator either fails
    // the not_null contract or emits invalid all-null rows downstream.
    let live_adds = reconciled.filter(col([ADD_NAME]).is_not_null())?;

    // === Scan-specific terminal projection: live_adds -> flat scan_file_row ==========
    let live_actions = project_scan_file_row(live_adds, parts.as_ref())?;

    // === Stage 6 (optional): data phase =================================================
    if with_data {
        do_data_stage(scan, live_actions)
    } else {
        Ok(live_actions)
    }
}

/// Append the data stage onto the live-actions builder and return the data-stream
/// builder.
///
/// Splits per-file: the upstream builder emits one row per surviving file (flat
/// `scan_file_row` shape), and [`PlanBuilder::load`] expands each row into the file's
/// per-record stream while broadcasting `path` and the `fileConstantValues` struct via
/// `passthrough_columns`. The trailing projection translates physical -> logical column
/// names per the scan's column-mapping mode.
fn do_data_stage(scan: &Scan, live_actions: PlanBuilder) -> Result<PlanBuilder, DeltaError> {
    let logical_schema = scan.logical_schema().clone();
    let logical_projection = scan_data_projection(scan.state_info())?;

    // Per-file parquet read; broadcasts the file-constant struct and `path` to every
    // emitted record-row. Output schema = physical_schema ++ {path, fileConstantValues}.
    let passthrough_columns = vec![
        ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
        ColumnName::new(["path"]),
    ];
    let load = LoadNode {
        file_schema: scan.physical_schema().clone(),
        file_type: FileType::Parquet,
        base_url: Some(scan.snapshot().table_root().clone()),
        passthrough_columns,
        file_meta: default_scan_file_columns(),
        dv_ref: Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
    };
    let raw_data = live_actions.load(load)?;

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
/// the scan data stage:
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
/// (see `reconciliation::ReconciliationPlanBuilder::with_partitions_parsed`). The terminal
/// reads `col(["add", "partitionValues_parsed"])` directly -- no per-row
/// `map_to_struct(add.partitionValues)` here. The raw Map form has no downstream consumer
/// in the scan plan, so it is omitted from `fileConstantValues` entirely (parsing happens
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

    let file_constants = StructType::new_unchecked(file_constant_fields);
    let fields = [
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(FILE_CONSTANT_VALUES_NAME, file_constants),
    ];
    let schema = Arc::new(StructType::new_unchecked(fields));
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
    use crate::expressions::Scalar;
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
        let builder = ctx.values(schema, Vec::<Vec<Scalar>>::new()).unwrap();
        (ctx, builder)
    }

    fn assert_fcv_field_names(out: PlanBuilder, expected_fcv_fields: &[&str]) {
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
        assert_eq!(fcv_fields, expected_fcv_fields);
    }

    #[rstest::rstest]
    #[case::no_partitions(
        None,
        &["baseRowId", "defaultRowCommitVersion", "tags", "clusteringProvider"],
    )]
    #[case::with_partitions(
        Some(arc_schema([StructField::nullable("p", DataType::STRING)])),
        &[
            "baseRowId",
            "defaultRowCommitVersion",
            "tags",
            "clusteringProvider",
            "partitionValues_parsed",
        ],
    )]
    fn project_scan_file_row_partitions(
        #[case] parts: Option<SchemaRef>,
        #[case] expected_fcv_fields: &[&str],
    ) {
        let parts_ref = parts.as_ref();
        let (_ctx, builder) = make_input_builder(parts_ref);
        let out = project_scan_file_row(builder, parts_ref).unwrap();
        assert_fcv_field_names(out, expected_fcv_fields);
    }
}
