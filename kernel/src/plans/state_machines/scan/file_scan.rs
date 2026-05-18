//! Scan-time plan builders and state machines.
//!
//! Hosts the `impl Scan { ... }` block that builds the two-phase scan plans
//! (metadata then data) and exposes them as coroutine state machines. The
//! supporting [`scan_metadata_plans_with_shape`] composer and the
//! partition-column projection helpers used by both halves live here too.
//!
//! Naming convention:
//! - `*_plans` builders return the raw `Vec<Plan>` (plus any handle the downstream consumer needs);
//!   they are `pub(super)` because the public surface is the SM wrappers.
//! - `*_state_machine` returns a [`CoroutineSM`]; this is the canonical entry point external
//!   engines drive.

use std::collections::HashSet;
use std::sync::Arc;

use uuid::Uuid;

use super::checkpoint_shape::{
    checkpoint_shape_from_last_checkpoint, resolve_checkpoint_shape_for_scan, CheckpointShape,
};
use super::dedup::ADD_PATH;
use super::plans::build_fsr_plans;
use super::schemas::{action_schema, action_schema_with_augmented_add};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    Add, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::delta_error;
use crate::expressions::{
    col, BinaryExpressionOp, ColumnName, Expression, IntoColumnName, Predicate, Transform,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{DvRef, FileType, RelationHandle, ScanFileColumns, SinkType};
use crate::plans::ir::{Plan, PlanBuilder, RelationRegistry, ResultPlan};
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;
use crate::schema::{DataType, MetadataColumnSpec, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::Snapshot;

fn replay_partition_columns(scan: &Scan) -> HashSet<String> {
    scan.snapshot()
        .table_configuration()
        .partition_columns()
        .iter()
        .filter(|name| scan.logical_schema().contains(name.as_str()))
        .cloned()
        .collect()
}

impl Scan {
    /// Build the metadata-phase [`ResultPlan`].
    ///
    /// [`ResultPlan::result_relation`] is the materialized live-actions
    /// relation that the data phase consumes.
    pub(super) fn scan_metadata_plans(&self) -> Result<ResultPlan, DeltaError> {
        let checkpoint_shape = checkpoint_shape_from_last_checkpoint(self.snapshot().as_ref())?;
        scan_metadata_plans_with_shape(self, checkpoint_shape)
    }

    /// Build the data-phase [`ResultPlan`] given the live-actions relation
    /// handle materialized by the metadata phase.
    ///
    /// [`ResultPlan::result_relation`] publishes the final scan output rows in
    /// the scan's logical schema.
    pub fn scan_data_from_metadata_plans(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<ResultPlan, DeltaError> {
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let snapshot = self.snapshot();
        let partition_columns = replay_partition_columns(self);
        let _file_schema = scan_data_file_schema(self.physical_schema(), self.logical_schema())?;
        let logical_schema = self.logical_schema().clone();
        let logical_projection = scan_data_projection(
            &logical_schema,
            self.physical_schema(),
            &partition_columns,
            snapshot.table_configuration().column_mapping_mode(),
        )?;
        // === data load: per-file parquet read keyed by the live-actions relation ===
        let load_plan = PlanBuilder::from_relation_handle(live_actions_relation).load(
            FSR_SCAN_DATA_ROWS_RAW,
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
            &mut registry,
        )?;
        // === final projection: physical -> logical schema, materialized into the result handle ===
        // The handle publishes the scan's logical schema directly; the projection chain feeds
        // matching rows.
        let project_plan = registry
            .relation_ref(FSR_SCAN_DATA_ROWS_RAW)?
            .project(logical_projection, logical_schema)
            .into_relation(FSR_SCAN_DATA, &mut registry)?;
        let result_handle = relation_output_handle(&project_plan)?;

        Ok(ResultPlan::new(
            vec![load_plan, project_plan],
            result_handle,
        ))
    }

    /// Build the full composed scan [`ResultPlan`]: metadata phase followed by
    /// data phase, sharing the live-actions relation handle.
    ///
    /// [`ResultPlan::result_relation`] is the data-phase result relation
    /// publishing the scan's logical schema.
    pub fn scan_plans(&self) -> Result<ResultPlan, DeltaError> {
        let metadata = self.scan_metadata_plans()?;
        let live_actions_relation = metadata.result_relation;
        let data = self.scan_data_from_metadata_plans(live_actions_relation)?;
        let mut plans = metadata.plans;
        plans.extend(data.plans);
        Ok(ResultPlan::new(plans, data.result_relation))
    }

    /// Coroutine SM for metadata-only scan execution.
    ///
    /// Resolves checkpoint shape (running schema-query phases when the
    /// `_last_checkpoint` hint is missing) and returns the metadata
    /// [`ResultPlan`] whose result relation is the materialized live-actions
    /// relation. The caller drives the SM, executes the result plan's plans,
    /// and feeds [`ResultPlan::result_relation`] into
    /// [`Self::scan_data_from_metadata_state_machine`].
    pub fn scan_metadata_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan_metadata", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id));
            let shape = resolve_checkpoint_shape_for_scan(&mut ctx, &scan).await?;
            scan_metadata_plans_with_shape(&scan, shape)
        })
    }

    /// Coroutine SM for data-only scan execution.
    ///
    /// The caller provides the live-actions relation produced by metadata
    /// replay. The SM returns the data-phase [`ResultPlan`]; the caller
    /// executes its plans and reads its result relation to obtain the final
    /// rows.
    pub fn scan_data_from_metadata_state_machine(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan_data", move |_co, _sm_id| async move {
            scan.scan_data_from_metadata_plans(live_actions_relation)
        })
    }

    /// Coroutine SM for combined metadata + data scan execution.
    ///
    /// When `_last_checkpoint` does not provide schema hints, the SM runs a
    /// checkpoint-schema query first, and for V2 checkpoints with sidecars it
    /// runs sidecar discovery (`ConsumeByKdf`) followed by a sidecar schema
    /// query before building plans. The returned [`ResultPlan`]'s result
    /// relation is the data-phase output relation publishing the scan's
    /// logical schema.
    pub fn scan_state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new("scan", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id));
            let shape = resolve_checkpoint_shape_for_scan(&mut ctx, &scan).await?;
            let metadata = scan_metadata_plans_with_shape(&scan, shape)?;
            let live_actions_relation = metadata.result_relation;
            let data = scan.scan_data_from_metadata_plans(live_actions_relation)?;
            let mut plans = metadata.plans;
            plans.extend(data.plans);
            Ok(ResultPlan::new(plans, data.result_relation))
        })
    }
}

/// Materialized live-actions relation name produced by metadata-phase replay.
const FSR_SCAN_LIVE_ACTIONS: &str = "scan.live_actions";
/// Raw per-file rows emitted by the data-phase Load sink before logical projection.
const FSR_SCAN_DATA_ROWS_RAW: &str = "scan.data_rows_raw";
/// Final scan-data relation in the scan's logical schema.
const FSR_SCAN_DATA: &str = "scan.data";

fn relation_output_handle(plan: &Plan) -> Result<RelationHandle, DeltaError> {
    match &plan.sink {
        SinkType::Relation(h) => Ok(h.clone()),
        other => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "internal: expected Relation sink, got {other:?}",
        )),
    }
}

/// Compose the metadata-phase [`ResultPlan`] for `scan` using the resolved
/// checkpoint shape.
///
/// Builds on top of [`build_fsr_plans`] by consuming the FSR result relation
/// as the input of an actions-side projection / predicate / dedup chain, and
/// materializes the live-actions relation that the data phase consumes.
fn scan_metadata_plans_with_shape(
    scan: &Scan,
    checkpoint_shape: CheckpointShape,
) -> Result<ResultPlan, DeltaError> {
    let snapshot = scan.snapshot();
    let partition_columns = replay_partition_columns(scan);
    let predicate_stats_schema = scan.physical_stats_schema();
    let predicate_partition_schema = scan.physical_partition_schema();
    let partition_values_schema =
        scan_partition_values_physical_schema(snapshot.as_ref(), scan.logical_schema())?;

    // === FSR plans: build the FSR result plan, then layer the actions chain on top ===
    // The terminal FSR plan is rebound under `scan.fsr_results` in this layer's registry so the
    // downstream actions chain can `relation_ref` it. The rebind reuses the strict
    // `action_schema()` -- the same contract `build_fsr_plans` publishes -- since the engine
    // realigns the relaxed JSON/Parquet output to that schema at the scan boundary. Both layers
    // share one registry so every minted handle shares the same `sm_id` namespace.
    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let mut plans: Vec<Plan> = Vec::new();
    let fsr = build_fsr_plans(snapshot.as_ref(), checkpoint_shape, &mut registry)?;
    let fsr_terminal_id = fsr.result_relation.id;
    let mut terminal_plan: Option<crate::plans::ir::Plan> = None;
    for plan in fsr.plans {
        let is_terminal = matches!(
            &plan.sink,
            SinkType::Relation(h) if h.id == fsr_terminal_id
        );
        if is_terminal {
            terminal_plan = Some(plan);
        } else {
            plans.push(plan);
        }
    }
    let terminal_plan = terminal_plan.ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "fsr::scan::scan_metadata: terminal FSR plan missing from plan vector",
        )
    })?;
    let fsr_results_handle =
        registry.register_relation_sink("scan.fsr_results", action_schema())?;
    let terminal_rebound_plan =
        Plan::new(terminal_plan.root, SinkType::Relation(fsr_results_handle));
    plans.push(terminal_rebound_plan);

    // === actions chain: filter live add paths, optionally augmenting with parsed stats /
    // partitionValues columns when a data-skipping predicate is in play ===
    let actions_root = registry.relation_ref("scan.fsr_results")?;
    let live_actions_node = match scan.build_actions_meta_predicate() {
        Some(predicate) => {
            let needs_augmented_add =
                predicate_stats_schema.is_some() || predicate_partition_schema.is_some();
            let projected = if needs_augmented_add {
                let (projection, schema) = scan_actions_with_parsed_projection_and_schema(
                    predicate_stats_schema.as_ref(),
                    predicate_partition_schema.as_ref(),
                )?;
                actions_root.project(projection, schema)
            } else {
                actions_root
            };
            // Data-skipping predicates are best-effort: keep rows when the predicate evaluates to
            // NULL (unknown) to avoid over-pruning files with partial stats coverage.
            let add_path_present = Expression::Column(ADD_PATH.into_column_name()).is_not_null();
            let pred = predicate.as_ref().clone();
            let skip_or_unknown = Predicate::or(pred.clone(), Predicate::is_null(pred));
            projected.filter(Arc::new(
                Predicate::and(add_path_present, skip_or_unknown).into(),
            ))
        }
        None => actions_root.filter(Arc::new(
            Expression::Column(ADD_PATH.into_column_name())
                .is_not_null()
                .into(),
        )),
    }
    .project(
        scan_live_actions_projection(!partition_columns.is_empty()),
        scan_live_actions_schema(partition_values_schema.as_ref()),
    );

    let live_actions_plan =
        live_actions_node.into_relation(FSR_SCAN_LIVE_ACTIONS, &mut registry)?;
    let live_actions_relation = relation_output_handle(&live_actions_plan)?;
    plans.push(live_actions_plan);
    Ok(ResultPlan::new(plans, live_actions_relation))
}

pub(super) fn scan_partition_values_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Option<SchemaRef> {
    let partition_columns = snapshot.table_configuration().partition_columns();
    if partition_columns.is_empty() {
        return None;
    }
    let fields = logical_schema
        .fields()
        .filter(|f| partition_columns.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    if fields.is_empty() {
        None
    } else {
        Some(Arc::new(StructType::new_unchecked(fields)))
    }
}

pub(super) fn scan_partition_values_physical_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Result<Option<SchemaRef>, DeltaError> {
    let Some(logical_partition_schema) = scan_partition_values_schema(snapshot, logical_schema)
    else {
        return Ok(None);
    };
    let mode = snapshot.table_configuration().column_mapping_mode();
    let physical_partition_schema = logical_partition_schema
        .as_ref()
        .make_physical(mode)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())?;
    Ok(Some(physical_partition_schema))
}

pub(super) fn scan_live_actions_schema(
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let partition_values = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    if let Some(schema) = partition_values_parsed_schema {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            schema.as_ref().clone(),
        ));
    }
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(
            FILE_CONSTANT_VALUES_NAME,
            StructType::new_unchecked(file_constant_fields),
        ),
    ]))
}

pub(super) fn scan_live_actions_projection(
    with_partition_values_parsed: bool,
) -> Vec<Arc<Expression>> {
    let mut file_constant_exprs = vec![
        col(["add", "partitionValues"]),
        col(["add", "baseRowId"]),
        col(["add", "defaultRowCommitVersion"]),
        col(["add", "tags"]),
        col(["add", "clusteringProvider"]),
    ];
    if with_partition_values_parsed {
        file_constant_exprs.push(Expression::map_to_struct(col(["add", "partitionValues"])));
    }
    vec![
        col(["add", "path"]).into(),
        col(["add", "size"]).into(),
        col(["add", "deletionVector"]).into(),
        Expression::struct_from(file_constant_exprs).into(),
    ]
}

pub(super) fn scan_actions_with_parsed_projection(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Vec<Arc<Expression>> {
    let mut add_exprs: Vec<Arc<Expression>> = Add::to_schema()
        .fields()
        .map(|f| col(["add", f.name().as_str()]).into())
        .collect();
    if let Some(schema) = stats_parsed_schema {
        add_exprs.push(Expression::parse_json(col(["add", "stats"]), schema.clone()).into());
    }
    if partition_values_parsed_schema.is_some() {
        add_exprs.push(Expression::map_to_struct(col(["add", "partitionValues"])).into());
    }
    vec![
        Expression::struct_from(add_exprs).into(),
        col([REMOVE_NAME]).into(),
        col([PROTOCOL_NAME]).into(),
        col([METADATA_NAME]).into(),
        col([DOMAIN_METADATA_NAME]).into(),
        col([SET_TRANSACTION_NAME]).into(),
    ]
}

fn scan_actions_with_parsed_projection_and_schema(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Result<(Vec<Arc<Expression>>, SchemaRef), DeltaError> {
    Ok((
        scan_actions_with_parsed_projection(stats_parsed_schema, partition_values_parsed_schema),
        action_schema_with_augmented_add(stats_parsed_schema, partition_values_parsed_schema),
    ))
}

pub(super) fn scan_data_file_schema(
    physical_schema: &SchemaRef,
    logical_schema: &SchemaRef,
) -> Result<SchemaRef, DeltaError> {
    if logical_schema.contains_metadata_column(&MetadataColumnSpec::RowId)
        && !physical_schema.contains_metadata_column(&MetadataColumnSpec::RowIndex)
    {
        let row_index_field = StructField::default_row_index_column().clone();
        if physical_schema.contains(row_index_field.name.as_str()) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::scan::scan_data_file_schema: row-id projection requires row-index metadata field `{}` but schema already contains a field with that name",
                row_index_field.name,
            ));
        }
        return physical_schema
            .as_ref()
            .add([row_index_field])
            .map(Arc::new)
            .map_err(|e| e.into_delta_default());
    }
    Ok(physical_schema.clone())
}

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

    use rstest::rstest;

    use super::*;
    use crate::scan::ScanBuilder;
    use crate::utils::test_utils::load_test_table;

    #[rstest]
    #[case::app_txn_no_checkpoint("app-txn-no-checkpoint", false)]
    #[case::app_txn_checkpoint("app-txn-checkpoint", false)]
    #[case::v2_without_sidecars("v2-checkpoints-parquet-without-sidecars", true)]
    #[case::v2_with_sidecars("v2-checkpoints-parquet-with-sidecars", true)]
    fn scan_builder_with_stats_supports_metadata_data_and_combined_modes(
        #[case] table: &str,
        #[case] with_predicate: bool,
    ) {
        let (_engine, snapshot, _tmp) = load_test_table(table).unwrap();
        let mut builder = ScanBuilder::new(Arc::clone(&snapshot)).with_stats();
        if with_predicate {
            builder = builder.with_predicate(Arc::new(Expression::column(["id"]).is_not_null()));
        }
        let scan = builder.build_replay().unwrap();
        let metadata = scan.scan_metadata_plans().unwrap();
        assert!(
            !metadata.plans.is_empty(),
            "metadata plans must be non-empty for {table}"
        );
        let data = scan
            .scan_data_from_metadata_plans(metadata.result_relation.clone())
            .unwrap();
        assert_eq!(
            data.plans.len(),
            2,
            "data phase should remain two plans for {table}"
        );
        let combined = scan.scan_plans().unwrap();
        assert_eq!(
            combined.plans.len(),
            metadata.plans.len() + data.plans.len(),
            "combined replay should equal metadata + data for {table}"
        );
    }

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
    /// - Primitive / list / map fields: a single top-level `col([physical_root])` reference. The
    ///   physical-to-logical name rename and per-field metadata are then applied by the engine via
    ///   its per-field cast (which uses the projection's declared output schema).
    /// - Struct fields: an identity [`Expression::Transform`] rooted at the physical column. This
    ///   IR primitive carries the "reshape to the projection's output schema" intent that
    ///   [`crate::engine::arrow_expression`] handles natively via `apply_schema`, and that other
    ///   engines (e.g. DataFusion) lower into their native struct-construction primitive. Emitting
    ///   `Expression::Struct` here would lose the source struct's null bitmap.
    #[rstest]
    #[case::primitive(DataType::LONG, false)]
    #[case::nested_struct(DataType::Struct(Box::new(StructType::try_new(vec![
        annotated_field("inner1", "phys-inner1", 91, DataType::STRING, true),
        annotated_field("inner2", "phys-inner2", 92, DataType::INTEGER, true),
    ]).unwrap())), true)]
    #[case::deeply_nested_struct(DataType::Struct(Box::new(StructType::try_new(vec![
        annotated_field("mid", "phys-mid", 93, DataType::Struct(Box::new(
            StructType::try_new(vec![annotated_field(
                "leaf", "phys-leaf", 94, DataType::LONG, true,
            )]).unwrap()
        )), true),
    ]).unwrap())), true)]
    #[case::list_of_struct(DataType::Array(Box::new(crate::schema::ArrayType::new(
        DataType::Struct(Box::new(StructType::try_new(vec![annotated_field(
            "x", "phys-x", 95, DataType::INTEGER, true,
        )]).unwrap())),
        true,
    ))), false)]
    #[case::map_of_struct(DataType::Map(Box::new(crate::schema::MapType::new(
        DataType::STRING,
        DataType::Struct(Box::new(StructType::try_new(vec![annotated_field(
            "v", "phys-v", 96, DataType::STRING, true,
        )]).unwrap())),
        true,
    ))), false)]
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
