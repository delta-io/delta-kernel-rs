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

use super::action::{
    action_output_schema, action_schema_with_augmented_add, load_materialized_schema, ADD_PATH,
};
use super::checkpoint_shape::{
    checkpoint_shape_from_last_checkpoint, resolve_checkpoint_shape_for_scan, CheckpointShape,
};
use super::plans::build_fsr_plans;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::delta_error;
use crate::expressions::{
    col, BinaryExpressionOp, ColumnName, Expression, IntoColumnName, Predicate,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{DvRef, FileType, LoadSink, RelationHandle, ScanFileColumns};
use crate::plans::ir::{plan, Plan, PlanCollector};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::plans::state_machines::framework::phase_state::PhaseState;
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

/// Run a single SM phase, wrapping any phase-execution error as a
/// [`DeltaError`] with a stable `{label}` site tag. Used by every SM body
/// in this module so the call sites stay one-liners.
async fn run_phase(
    phase: &mut Phase<'_>,
    op: PhaseOperation,
    label: &'static str,
) -> Result<PhaseState, DeltaError> {
    phase.execute(op, label).await.map_err(|e| {
        let detail = e.display_with_source_chain();
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            source = e,
            "scan::{label}: {detail}",
        )
    })
}

impl Scan {
    /// Build the metadata-phase plans plus the live-actions handle the
    /// data-phase plan consumes.
    ///
    /// Internal helper used by both the metadata-only SM and the combined SM;
    /// not exposed on the public surface.
    pub(super) fn scan_metadata_plans(&self) -> Result<(Vec<Plan>, RelationHandle), DeltaError> {
        let checkpoint_shape = checkpoint_shape_from_last_checkpoint(self.snapshot().as_ref())?;
        scan_metadata_plans_with_shape(self, checkpoint_shape)
    }

    /// Build the data-phase plans given the live-actions relation handle
    /// materialized by the metadata phase.
    pub fn scan_data_from_metadata_plans(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<Vec<Plan>, DeltaError> {
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
        // PlanCollector::add_load does not accept a dv_ref, so the LoadSink is built
        // directly. The materialized schema is computed by load_materialized_schema
        // (file_schema + named passthroughs) rather than the spec's resolver because
        // the row-id projection augments file_schema beyond the load's physical read
        // schema.
        let data_rows_raw_relation =
            RelationHandle::fresh("scan.data_rows_raw", materialized_schema);
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

        // === final projection: physical -> logical schema, terminating in Results ===
        let results_plan = plan::relation_ref(&data_rows_raw_relation)
            .project(logical_projection, logical_schema.clone())
            .into_results_with_schema(logical_schema);

        Ok(vec![load_plan, results_plan])
    }

    /// Build the full composed scan plan vector: metadata phase followed by
    /// data phase, sharing the live-actions relation handle.
    pub fn scan_plans(&self) -> Result<Vec<Plan>, DeltaError> {
        let (mut plans, live_actions_relation) = self.scan_metadata_plans()?;
        plans.extend(self.scan_data_from_metadata_plans(live_actions_relation)?);
        Ok(plans)
    }

    /// Coroutine SM for metadata-only scan execution.
    ///
    /// Resolves checkpoint shape (running schema-query phases when the
    /// `_last_checkpoint` hint is missing), executes the metadata replay
    /// plans, and returns the materialized live-actions relation handle so
    /// the caller can feed it into [`Self::scan_data_from_metadata_state_machine`].
    pub fn scan_metadata_state_machine(&self) -> Result<CoroutineSM<RelationHandle>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
            let (metadata, live_actions_relation) = scan_metadata_plans_with_shape(&scan, shape)?;
            run_phase(
                &mut phase,
                PhaseOperation::Plans(metadata),
                "scan.replay.metadata",
            )
            .await?;
            Ok(live_actions_relation)
        })
    }

    /// Coroutine SM for data-only scan execution.
    ///
    /// The caller provides the live-actions relation produced by metadata
    /// replay. The SM executes only the data-phase plans.
    pub fn scan_data_from_metadata_state_machine(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let data = scan.scan_data_from_metadata_plans(live_actions_relation)?;
            run_phase(&mut phase, PhaseOperation::Plans(data), "scan.replay.data").await?;
            Ok(())
        })
    }

    /// Coroutine SM for combined metadata + data scan execution.
    ///
    /// This is the canonical replay path when checkpoint shape is ambiguous:
    /// if `_last_checkpoint` does not provide schema hints, the SM runs a
    /// checkpoint-schema query first, and for V2 checkpoints with sidecars
    /// it runs sidecar discovery (`ConsumeByKdf`) followed by a sidecar
    /// schema query before building metadata/data plans.
    pub fn scan_state_machine(&self) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
            let (metadata, live_actions_relation) =
                scan_metadata_plans_with_shape(&scan, shape.clone())?;
            run_phase(
                &mut phase,
                PhaseOperation::Plans(metadata),
                "scan.replay.metadata",
            )
            .await?;
            let data = scan.scan_data_from_metadata_plans(live_actions_relation)?;
            run_phase(&mut phase, PhaseOperation::Plans(data), "scan.replay.data").await?;
            Ok(())
        })
    }
}

/// Compose the metadata-phase plans for `scan` using the resolved checkpoint
/// shape.
///
/// Builds on top of [`build_fsr_plans`] by rebinding the FSR-results plan to
/// a fresh relation handle, layering on the actions-side projection /
/// predicate / dedup chain, and finally materializing the `scan.live_actions`
/// relation that the data phase consumes.
fn scan_metadata_plans_with_shape(
    scan: &Scan,
    checkpoint_shape: CheckpointShape,
) -> Result<(Vec<Plan>, RelationHandle), DeltaError> {
    let snapshot = scan.snapshot();
    let partition_columns = replay_partition_columns(scan);
    let predicate_stats_schema = scan.physical_stats_schema();
    let predicate_partition_schema = scan.physical_partition_schema();
    let partition_values_schema =
        scan_partition_values_physical_schema(snapshot.as_ref(), scan.logical_schema())?;

    // === FSR plans: build, rebind the terminal Results plan to a fresh relation ===
    // build_fsr_plans returns a vector ending in a Results sink. Inside the scan SM the
    // terminal output is the actions-projected live-actions relation, so pop the FSR
    // results plan and rebind its root to a named handle the actions chain can reference.
    // The handle's published schema is action_output_schema() (strict per-action ToSchema)
    // because the downstream actions chain references nested fields by name; some FSR
    // results branches emit action_read_schema() (all-nullable nested), so we pin the
    // handle to the strict schema to keep the actions chain's input shape stable.
    let mut p = PlanCollector::new();
    let mut fsr_plans = build_fsr_plans(snapshot.as_ref(), checkpoint_shape)?;
    let fsr_results_plan = fsr_plans.pop().ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "fsr::scan::scan_metadata: expected at least one plan from build_fsr_plans",
        )
    })?;
    for prior in fsr_plans {
        p.push_plan(prior);
    }
    let fsr_results_relation = RelationHandle::fresh("scan.fsr_results", action_output_schema());
    p.push_plan(
        fsr_results_plan
            .root
            .into_relation(fsr_results_relation.clone()),
    );

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

    let live_actions_relation = p.add_relation("scan.live_actions", live_actions_node);
    Ok((p.into_vec(), live_actions_relation))
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
        Arc::new(col(["add", "path"])),
        Arc::new(col(["add", "size"])),
        Arc::new(col(["add", "deletionVector"])),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ]
}

pub(super) fn scan_actions_with_parsed_projection(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Vec<Arc<Expression>> {
    let mut add_exprs = vec![
        Arc::new(col(["add", "path"])),
        Arc::new(col(["add", "partitionValues"])),
        Arc::new(col(["add", "size"])),
        Arc::new(col(["add", "modificationTime"])),
        Arc::new(col(["add", "dataChange"])),
        Arc::new(col(["add", "stats"])),
        Arc::new(col(["add", "tags"])),
        Arc::new(col(["add", "deletionVector"])),
        Arc::new(col(["add", "baseRowId"])),
        Arc::new(col(["add", "defaultRowCommitVersion"])),
        Arc::new(col(["add", "clusteringProvider"])),
    ];
    if let Some(schema) = stats_parsed_schema {
        add_exprs.push(Arc::new(Expression::parse_json(
            col(["add", "stats"]),
            schema.clone(),
        )));
    }
    if partition_values_parsed_schema.is_some() {
        add_exprs.push(Arc::new(Expression::map_to_struct(col([
            "add",
            "partitionValues",
        ]))));
    }
    vec![
        Arc::new(Expression::struct_from(add_exprs)),
        Arc::new(col([REMOVE_NAME])),
        Arc::new(col([PROTOCOL_NAME])),
        Arc::new(col([METADATA_NAME])),
        Arc::new(col([DOMAIN_METADATA_NAME])),
        Arc::new(col([SET_TRANSACTION_NAME])),
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
            Ok(Arc::new(expr))
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
            let (metadata, live_actions) = scan.scan_metadata_plans().unwrap();
            assert!(
                !metadata.is_empty(),
                "metadata plans must be non-empty for {}",
                cfg.table
            );
            let data = scan.scan_data_from_metadata_plans(live_actions).unwrap();
            assert_eq!(
                data.len(),
                2,
                "data phase should remain two plans for {}",
                cfg.table
            );
            let combined = scan.scan_plans().unwrap();
            assert_eq!(
                combined.len(),
                metadata.len() + data.len(),
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
        let (metadata, _) = scan.scan_metadata_plans().unwrap();
        let debug = format!("{metadata:#?}");
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
        let (_, live_actions) = scan.scan_metadata_plans().unwrap();
        let data = scan.scan_data_from_metadata_plans(live_actions).unwrap();
        let load = match &data[0].sink.sink_type {
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
