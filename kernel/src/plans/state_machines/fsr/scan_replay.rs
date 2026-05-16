//! Scan replay state machines.
//!
//! Hosts the `impl Scan { ... }` block that builds replay plans from a
//! [`Scan`] (the two-phase metadata + data flow), the supporting
//! [`scan_metadata_plans_with_shape`] composer, and the partition-column
//! projection helper used by both halves.

use std::collections::HashSet;
use std::sync::Arc;

use super::checkpoint_shape::{
    checkpoint_shape_from_last_checkpoint, resolve_checkpoint_shape_for_scan, CheckpointShape,
};
use super::plans::{build_fsr_plans, load_materialized_schema};
use super::schemas::{
    action_output_schema, action_schema_with_augmented_add, scan_actions_with_parsed_projection,
    scan_data_file_schema, scan_data_projection, scan_live_actions_projection,
    scan_live_actions_schema, scan_partition_values_physical_schema, ADD_PATH,
};
use crate::delta_error;
use crate::expressions::{ColumnName, Predicate};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::expr_ext::{col, PredicateExt};
use crate::plans::ir::nodes::{DvRef, FileType, LoadSink, RelationHandle, ScanFileColumns};
use crate::plans::ir::{DeclarativePlanNode, Plan};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;

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
    /// Build replay plans for the metadata phase.
    ///
    /// Returns the metadata plans and the live-actions relation handle that must be fed
    /// into [`Self::replay_scan_data_plans`].
    pub fn scan_metadata_state_machine(&self) -> Result<(Vec<Plan>, RelationHandle), DeltaError> {
        let checkpoint_shape = checkpoint_shape_from_last_checkpoint(self.snapshot().as_ref())?;
        scan_metadata_plans_with_shape(self, checkpoint_shape)
    }

    /// Build replay plans for the data phase.
    pub fn replay_scan_data_plans(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<Vec<Plan>, DeltaError> {
        let snapshot = self.snapshot();
        let partition_columns = replay_partition_columns(self);
        let file_schema = scan_data_file_schema(self.physical_schema(), self.logical_schema())?;
        let data_rows_raw_relation = RelationHandle::fresh(
            "scan.data_rows_raw",
            load_materialized_schema(
                &file_schema,
                &scan_live_actions_schema(
                    scan_partition_values_physical_schema(
                        snapshot.as_ref(),
                        self.logical_schema(),
                    )?
                    .as_ref(),
                ),
                &[FILE_CONSTANT_VALUES_NAME, "path"],
            )?,
        );
        let load = LoadSink {
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
        let f1 = DeclarativePlanNode::relation_ref(live_actions_relation).into_load(load);
        let logical_schema = self.logical_schema().clone();
        let logical_projection = scan_data_projection(
            &logical_schema,
            self.physical_schema(),
            &partition_columns,
            self.snapshot().table_configuration().column_mapping_mode(),
        )?;
        let f2 = DeclarativePlanNode::relation_ref(data_rows_raw_relation)
            .project(logical_projection, logical_schema.clone())
            .into_results_with_schema(logical_schema);
        Ok(vec![f1, f2])
    }

    /// Build replay plans for the full composed scan.
    pub fn replay_scan_plans(&self) -> Result<Vec<Plan>, DeltaError> {
        let (mut plans, live_actions_relation) = self.scan_metadata_state_machine()?;
        plans.extend(self.replay_scan_data_plans(live_actions_relation)?);
        Ok(plans)
    }

    /// Build a coroutine SM for metadata-only replay scan execution.
    ///
    /// The SM resolves checkpoint shape hints the same way as
    /// [`Self::replay_scan_state_machine`], executes metadata replay plans, and
    /// returns the materialized live-actions relation handle.
    pub fn replay_scan_metadata_state_machine(
        &self,
    ) -> Result<CoroutineSM<RelationHandle>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
            let (metadata, live_actions_relation) = scan_metadata_plans_with_shape(&scan, shape)?;
            let _metadata_state = phase
                .execute(PhaseOperation::Plans(metadata), "scan.replay.metadata")
                .await
                .map_err(|e| {
                    let detail = e.display_with_source_chain();
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        source = e,
                        "scan::replay_scan_metadata_state_machine::metadata_phase: {detail}",
                    )
                })?;
            Ok(live_actions_relation)
        })
    }

    /// Build a coroutine SM for data-only replay scan execution.
    ///
    /// The caller provides the live-actions relation produced by metadata
    /// replay. The SM executes only the data-phase plans.
    pub fn replay_scan_data_state_machine(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let data = scan.replay_scan_data_plans(live_actions_relation)?;
            let _data_state = phase
                .execute(PhaseOperation::Plans(data), "scan.replay.data")
                .await
                .map_err(|e| {
                    let detail = e.display_with_source_chain();
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        source = e,
                        "scan::replay_scan_data_state_machine::data_phase: {detail}",
                    )
                })?;
            Ok(())
        })
    }

    /// Build a coroutine SM for replay scan execution.
    ///
    /// This is the canonical replay path when checkpoint shape is ambiguous: if `_last_checkpoint`
    /// does not provide schema hints, the SM runs a checkpoint-schema query first, and for V2
    /// checkpoints with sidecars it runs sidecar discovery (`ConsumeByKdf`) followed by a sidecar
    /// schema query before building metadata/data plans.
    pub fn replay_scan_state_machine(&self) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;

            let (metadata, live_actions_relation) =
                scan_metadata_plans_with_shape(&scan, shape.clone())?;
            let _metadata_state = phase
                .execute(PhaseOperation::Plans(metadata), "scan.replay.metadata")
                .await
                .map_err(|e| {
                    let detail = e.display_with_source_chain();
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        source = e,
                        "scan::replay_scan_state_machine::metadata_phase: {detail}",
                    )
                })?;
            let data = scan.replay_scan_data_plans(live_actions_relation)?;
            let _data_state = phase
                .execute(PhaseOperation::Plans(data), "scan.replay.data")
                .await
                .map_err(|e| {
                    let detail = e.display_with_source_chain();
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        source = e,
                        "scan::replay_scan_state_machine::data_phase: {detail}",
                    )
                })?;
            Ok(())
        })
    }
}

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
    let live_actions_relation = RelationHandle::fresh(
        "scan.live_actions",
        scan_live_actions_schema(partition_values_schema.as_ref()),
    );
    let mut plans = build_fsr_plans(snapshot.as_ref(), checkpoint_shape)?;
    let last = plans.pop().ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "fsr::scan::scan_metadata: expected at least one plan from build_fsr_plans",
        )
    })?;
    let fsr_results_relation = RelationHandle::fresh("scan.fsr_results", action_output_schema());
    plans.push(last.root.into_relation(fsr_results_relation.clone()));
    let mut metadata_root = DeclarativePlanNode::relation_ref(fsr_results_relation);
    if let Some(predicate) = scan.build_actions_meta_predicate() {
        let needs_augmented_projection =
            predicate_stats_schema.is_some() || predicate_partition_schema.is_some();
        if needs_augmented_projection {
            metadata_root = metadata_root.project(
                scan_actions_with_parsed_projection(
                    predicate_stats_schema.as_ref(),
                    predicate_partition_schema.as_ref(),
                ),
                action_schema_with_augmented_add(
                    predicate_stats_schema.as_ref(),
                    predicate_partition_schema.as_ref(),
                ),
            );
        }
        let add_path_present = col(ADD_PATH).is_not_null();
        // Data-skipping predicates are best-effort and must never introduce false negatives.
        // Keep rows when the skipping predicate evaluates to NULL (unknown) to avoid over-pruning
        // files with incomplete / partially-missing stats coverage.
        let pred = predicate.as_ref().clone();
        let skip_or_unknown = pred.clone().or(Predicate::is_null(pred));
        let combined = add_path_present.and(skip_or_unknown);
        metadata_root = metadata_root.filter(Arc::new(combined.into()));
    } else {
        metadata_root = metadata_root.filter(Arc::new(col(ADD_PATH).is_not_null().into()));
    }
    let live_actions = metadata_root.project(
        scan_live_actions_projection(!partition_columns.is_empty()),
        scan_live_actions_schema(partition_values_schema.as_ref()),
    );
    plans.push(live_actions.into_relation(live_actions_relation.clone()));
    Ok((plans, live_actions_relation))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expressions::{Expression, Scalar};
    use crate::plans::ir::nodes::SinkType;
    use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
    use crate::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
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
            builder = builder.with_predicate(Arc::new(col("id").is_not_null()));
        }
        builder.build_replay()
    }

    #[test]
    fn scan_builder_with_stats_supports_metadata_data_and_combined_modes_across_configs() {
        for cfg in replay_coverage_configs() {
            let (_engine, snapshot, _tmp) = load_test_table(cfg.table).unwrap();
            let scan = build_scan_for_config(Arc::clone(&snapshot), cfg).unwrap();
            let (metadata, live_actions) = scan.scan_metadata_state_machine().unwrap();
            assert!(
                !metadata.is_empty(),
                "metadata plans must be non-empty for {}",
                cfg.table
            );
            let data = scan.replay_scan_data_plans(live_actions).unwrap();
            assert_eq!(
                data.len(),
                2,
                "data phase should remain two plans for {}",
                cfg.table
            );
            let combined = scan.replay_scan_plans().unwrap();
            assert_eq!(
                combined.len(),
                metadata.len() + data.len(),
                "combined replay should equal metadata + data for {}",
                cfg.table
            );
        }
    }

    #[test]
    fn scan_replay_metadata_only_sm_returns_live_actions_handle() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-no-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let mut sm = scan.replay_scan_metadata_state_machine().unwrap();
        assert_eq!(sm.phase_name(), "scan.replay.metadata");
        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::Plans(_)));
        let done = sm.advance(Ok(
            crate::plans::state_machines::framework::phase_state::PhaseState::empty(),
        ));
        match done.unwrap() {
            AdvanceResult::Done(handle) => assert_eq!(handle.name, "scan.live_actions"),
            AdvanceResult::Continue => panic!("metadata-only scan SM should finish in one phase"),
        }
    }

    #[test]
    fn scan_replay_data_only_sm_runs_single_data_phase() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let mut sm = scan.replay_scan_data_state_machine(live_actions).unwrap();
        assert_eq!(sm.phase_name(), "scan.replay.data");
        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 2),
            _ => panic!("expected data-only scan SM to yield plans"),
        }
        let done = sm.advance(Ok(
            crate::plans::state_machines::framework::phase_state::PhaseState::empty(),
        ));
        assert!(matches!(done.unwrap(), AdvanceResult::Done(())));
    }

    #[test]
    fn replay_scan_state_machine_queries_checkpoint_schema_when_hint_missing() {
        let (_engine, snapshot, _tmp) =
            load_test_table("with_checkpoint_no_last_checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let mut sm = scan.replay_scan_state_machine().unwrap();
        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::SchemaQuery(_)));
    }

    #[test]
    fn scan_metadata_terminal_materializes_live_actions_relation() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        let last = metadata.last().expect("scan metadata plans");
        match &last.sink.sink_type {
            SinkType::Relation(h) => assert_eq!(h.name, "scan.live_actions"),
            other => panic!("expected terminal relation sink, got {other:?}"),
        }
    }

    #[test]
    fn scan_data_emits_load_then_results() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let data = scan.replay_scan_data_plans(live_actions).unwrap();
        assert_eq!(data.len(), 2);
        assert!(matches!(data[0].sink.sink_type, SinkType::Load(_)));
        assert!(matches!(data[1].sink.sink_type, SinkType::Results(_)));
    }

    #[test]
    fn scan_metadata_with_predicate_materializes_parsed_stats() {
        let (_engine, snapshot, _tmp) = load_test_table("basic_partitioned").unwrap();
        let predicate = Arc::new(Predicate::gt(
            col(["number"]),
            Expression::literal(Scalar::Long(1)),
        ));
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_predicate(predicate)
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
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

    #[rstest::rstest]
    #[case::classic_checkpoint("app-txn-checkpoint", false)]
    #[case::v2_checkpoint("v2-checkpoints-parquet-without-sidecars", true)]
    #[case::multipart_checkpoint("v2-checkpoints-parquet-with-sidecars", true)]
    fn scan_metadata_with_stats_terminates_in_relation_sink(
        #[case] table: &str,
        #[case] with_predicate: bool,
    ) {
        let (_engine, snapshot, _tmp) = load_test_table(table).unwrap();
        let mut builder = ScanBuilder::new(Arc::clone(&snapshot)).with_stats();
        if with_predicate {
            builder = builder.with_predicate(Arc::new(col("id").is_not_null()));
        }
        let scan = builder.build_replay().unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        assert!(!metadata.is_empty());
        let terminal = metadata.last().expect("metadata plans should be non-empty");
        assert!(matches!(terminal.sink.sink_type, SinkType::Relation(_)));
    }

    #[test]
    fn scan_data_load_uses_flat_live_actions_columns() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let data = scan.replay_scan_data_plans(live_actions).unwrap();
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
