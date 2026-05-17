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

use super::checkpoint_shape::{
    checkpoint_shape_from_last_checkpoint, resolve_checkpoint_shape_for_scan, CheckpointShape,
};
use super::dedup::ADD_PATH;
use super::plans::build_fsr_plans;
use super::schemas::{
    action_output_schema, action_schema_with_augmented_add, load_materialized_schema,
};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    Add, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::delta_error;
use crate::expressions::{
    col, BinaryExpressionOp, ColumnName, Expression, IntoColumnName, Predicate,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    DvRef, FileType, LoadSink, RelationHandle, ScanFileColumns, SinkType,
};
use crate::plans::ir::{identity_project_struct, plan, PlanCollector, ResultPlan};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
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
        let snapshot = self.snapshot();
        let partition_columns = replay_partition_columns(self);
        let file_schema = scan_data_file_schema(self.physical_schema(), self.logical_schema())?;
        let logical_schema = self.logical_schema().clone();
        let logical_projection = scan_data_projection(
            &logical_schema,
            self.physical_schema(),
            &partition_columns,
            snapshot.table_configuration().column_mapping_mode(),
        )?;
        let materialized_schema = load_materialized_schema(
            &file_schema,
            &scan_live_actions_schema(
                scan_partition_values_physical_schema(snapshot.as_ref(), self.logical_schema())?
                    .as_ref(),
            ),
            &[FILE_CONSTANT_VALUES_NAME, "path"],
        )?;

        // === data load: per-file parquet read keyed by the live-actions relation ===
        // The load sink is built directly because the materialized schema is computed by
        // load_materialized_schema (file_schema + named passthroughs): the row-id projection
        // augments file_schema beyond the load's physical read schema.
        let data_rows_raw_relation =
            RelationHandle::fresh(FSR_SCAN_DATA_ROWS_RAW, materialized_schema);
        let load_sink = LoadSink {
            output_relation: data_rows_raw_relation.clone(),
            file_schema: self.physical_schema().clone(),
            base_url: Some(snapshot.table_root().clone()),
            file_meta: ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: Some(ColumnName::new(["size"])),
                record_count: None,
            },
            dv_ref: Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
            passthrough_columns: vec![
                ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
                ColumnName::new(["path"]),
            ],
            file_type: FileType::Parquet,
        };
        let load_plan = plan::relation_ref(&live_actions_relation).into_load(load_sink);

        // === final projection: physical -> logical schema, materialized into the result handle ===
        // The handle publishes the scan's logical schema directly; the projection chain feeds
        // matching rows.
        let result_handle = RelationHandle::fresh(FSR_SCAN_DATA, logical_schema.clone());
        let project_plan = plan::relation_ref(&data_rows_raw_relation)
            .project(logical_projection, logical_schema)
            .into_relation(result_handle.clone());

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
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
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
        CoroutineSM::new(move |_co| async move {
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
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
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
    // `build_fsr_plans` publishes its terminal relation with the all-nullable
    // `action_read_schema()`. The downstream actions chain references nested fields by name and
    // expects the strict `action_output_schema()` contract, so the FSR terminal plan is rebound
    // to a fresh `scan.fsr_results` handle that publishes the strict schema. The chain's actual
    // row data is still all-nullable; the strict handle is purely a name-binding for downstream
    // projection consumers.
    let mut p = PlanCollector::new();
    let fsr = build_fsr_plans(snapshot.as_ref(), checkpoint_shape)?;
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
            p.push_plan(plan);
        }
    }
    let terminal_plan = terminal_plan.ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "fsr::scan::scan_metadata: terminal FSR plan missing from plan vector",
        )
    })?;
    let fsr_results_relation = RelationHandle::fresh("scan.fsr_results", action_output_schema());
    p.push_plan(crate::plans::ir::Plan::new(
        terminal_plan.root,
        SinkType::Relation(fsr_results_relation.clone()),
    ));

    // === actions chain: filter live add paths, optionally augmenting with parsed stats /
    // partitionValues columns when a data-skipping predicate is in play ===
    let actions_root = plan::relation_ref(&fsr_results_relation);
    let live_actions_node = match scan.build_actions_meta_predicate() {
        Some(predicate) => {
            let projected =
                if predicate_stats_schema.is_some() || predicate_partition_schema.is_some() {
                    actions_root.project(
                        scan_actions_with_parsed_projection(
                            predicate_stats_schema.as_ref(),
                            predicate_partition_schema.as_ref(),
                        ),
                        action_schema_with_augmented_add(
                            predicate_stats_schema.as_ref(),
                            predicate_partition_schema.as_ref(),
                        ),
                    )
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

    let live_actions_relation = p.add_relation(FSR_SCAN_LIVE_ACTIONS, live_actions_node)?;
    Ok(ResultPlan::new(p.into_vec(), live_actions_relation))
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
    let mut add_exprs = identity_project_struct("add", &Add::to_schema());
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
                    col([physical_field.name().as_str()])
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
    use crate::expressions::Scalar;
    use crate::plans::ir::nodes::SinkType;
    use crate::scan::ScanBuilder;
    use crate::snapshot::Snapshot;
    use crate::utils::test_utils::load_test_table;

    #[derive(Clone, Copy, Debug)]
    struct ReplayCoverageConfig {
        table: &'static str,
        with_predicate: bool,
    }

    fn replay_coverage_configs() -> [ReplayCoverageConfig; 4] {
        [
            ReplayCoverageConfig {
                table: "app-txn-no-checkpoint",
                with_predicate: false,
            },
            ReplayCoverageConfig {
                table: "app-txn-checkpoint",
                with_predicate: false,
            },
            ReplayCoverageConfig {
                table: "v2-checkpoints-parquet-without-sidecars",
                with_predicate: true,
            },
            ReplayCoverageConfig {
                table: "v2-checkpoints-parquet-with-sidecars",
                with_predicate: true,
            },
        ]
    }

    fn build_scan_for_config(
        snapshot: Arc<Snapshot>,
        cfg: ReplayCoverageConfig,
    ) -> Result<Scan, DeltaError> {
        let mut builder = ScanBuilder::new(snapshot).with_stats();
        if cfg.with_predicate {
            builder = builder.with_predicate(Arc::new(Expression::column(["id"]).is_not_null()));
        }
        builder.build_replay()
    }

    #[test]
    fn scan_builder_with_stats_supports_metadata_data_and_combined_modes_across_configs() {
        for cfg in replay_coverage_configs() {
            let (_engine, snapshot, _tmp) = load_test_table(cfg.table).unwrap();
            let scan = build_scan_for_config(Arc::clone(&snapshot), cfg).unwrap();
            let metadata = scan.scan_metadata_plans().unwrap();
            assert!(
                !metadata.plans.is_empty(),
                "metadata plans must be non-empty for {}",
                cfg.table
            );
            let data = scan
                .scan_data_from_metadata_plans(metadata.result_relation.clone())
                .unwrap();
            assert_eq!(
                data.plans.len(),
                2,
                "data phase should remain two plans for {}",
                cfg.table
            );
            let combined = scan.scan_plans().unwrap();
            assert_eq!(
                combined.plans.len(),
                metadata.plans.len() + data.plans.len(),
                "combined replay should equal metadata + data for {}",
                cfg.table
            );
        }
    }

    #[test]
    fn scan_metadata_with_predicate_materializes_parsed_stats() {
        let (_engine, snapshot, _tmp) = load_test_table("basic_partitioned").unwrap();
        let predicate = Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(Scalar::Long(1)),
        ));
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_predicate(predicate)
            .with_stats()
            .build_replay()
            .unwrap();
        let metadata = scan.scan_metadata_plans().unwrap();
        let debug = format!("{:#?}", metadata.plans);
        assert!(
            debug.contains("ParseJson"),
            "metadata replay should materialize ParseJson before predicate filtering:\n{debug}"
        );
        assert!(
            debug.contains("stats_parsed"),
            "metadata replay should project stats_parsed for predicate filtering:\n{debug}"
        );
    }

    #[test]
    fn scan_data_load_uses_flat_live_actions_columns() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let metadata = scan.scan_metadata_plans().unwrap();
        let data = scan
            .scan_data_from_metadata_plans(metadata.result_relation)
            .unwrap();
        let load = match &data.plans[0].sink {
            SinkType::Load(load) => load,
            other => panic!("expected load sink, got {other:?}"),
        };
        assert_eq!(load.file_meta.path, ColumnName::new(["path"]));
        assert_eq!(load.file_meta.size, Some(ColumnName::new(["size"])));
        assert_eq!(
            load.dv_ref,
            Some(DvRef::skip(ColumnName::new(["deletionVector"])))
        );
        assert_eq!(
            load.passthrough_columns,
            vec![
                ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
                ColumnName::new(["path"])
            ]
        );
    }
}
