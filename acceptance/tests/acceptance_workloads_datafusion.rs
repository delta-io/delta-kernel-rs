//! DataFusion-specific acceptance workload harness.
//!
//! This harness is intentionally separate from `acceptance_workloads_reader` so DataFusion
//! coverage can evolve independently from the default kernel engine workload suite.

use std::path::Path;
use std::sync::Arc;

use acceptance::acceptance_workloads::validation::{validate_read_result, validate_snapshot};
use acceptance::acceptance_workloads::workload::{
    execute_and_validate_workload, ReadResult, SnapshotResult,
};
use acceptance::acceptance_workloads::TestCase;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use delta_kernel_benchmarks::models::{ReadSpec, SnapshotConstructionSpec, Spec, TimeTravel};
use delta_kernel_benchmarks::predicate_parser::parse_predicate;
use delta_kernel_datafusion_engine::DataFusionExecutor;

/// Harness-level skips that are not about DataFusion read semantics.
const HARNESS_SKIP_PATTERNS: &[(&str, &str)] =
    &[("DV-017/", "Huge table (2B rows) causes OOM/hang")];

fn should_skip_harness_path(test_path: &str) -> Option<&'static str> {
    for (pattern, reason) in HARNESS_SKIP_PATTERNS {
        if test_path.contains(pattern) {
            return Some(reason);
        }
    }
    None
}

fn workload_spec_type(spec_path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(spec_path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    json.get("type")?.as_str().map(str::to_owned)
}

fn should_skip_datafusion_spec_type(spec_type: Option<&str>) -> Option<&'static str> {
    match spec_type.unwrap_or_default() {
        "read" | "snapshot" | "snapshotConstruction" | "snapshot_construction" => None,
        "cdf" => Some("CDF workload type not supported in this DataFusion read harness"),
        "txn" => Some("Transaction workload type not supported in this DataFusion read harness"),
        other if other.contains("domain_metadata") => {
            Some("Domain metadata workload type not supported in this DataFusion read harness")
        }
        _ => Some("Unsupported workload type for DataFusion read harness"),
    }
}

#[derive(Clone, Copy, Debug)]
struct ReadReplayConfig {
    name: &'static str,
    split_phases: bool,
}

const READ_REPLAY_CONFIGS: &[ReadReplayConfig] = &[
    ReadReplayConfig {
        name: "combined_scan_sm",
        split_phases: false,
    },
    ReadReplayConfig {
        name: "split_scan_sm",
        split_phases: true,
    },
];

#[derive(Clone, Copy, Debug)]
struct SnapshotReplayConfig {
    name: &'static str,
    use_full_state_builder: bool,
}

const SNAPSHOT_REPLAY_CONFIGS: &[SnapshotReplayConfig] = &[
    SnapshotReplayConfig {
        name: "snapshot_full_state_sm",
        use_full_state_builder: false,
    },
    SnapshotReplayConfig {
        name: "snapshot_full_state_builder",
        use_full_state_builder: true,
    },
];

async fn execute_snapshot_workload_datafusion(
    engine: Arc<dyn Engine>,
    table_root: &url::Url,
    snapshot_spec: &SnapshotConstructionSpec,
    config: SnapshotReplayConfig,
) -> DeltaResult<SnapshotResult> {
    let version = snapshot_spec
        .time_travel
        .as_ref()
        .map(TimeTravel::as_version)
        .transpose()
        .map_err(Error::generic)?;

    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(version) = version {
        builder = builder.at_version(version);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let executor = DataFusionExecutor::try_new_with_engine(engine)
        .map_err(|e| Error::generic(format!("create DataFusionExecutor: {e}")))?;
    if config.use_full_state_builder {
        let plans = snapshot
            .full_state_builder()
            .with_stats()
            .build()
            .map_err(|e| Error::generic(format!("build full_state plans via builder: {e}")))?;
        let _state = executor
            .execute_phase_operation(PhaseOperation::Plans(plans))
            .await
            .map_err(|e| {
                Error::generic(format!(
                    "execute full_state builder plans via DataFusionExecutor ({}): {e}",
                    config.name
                ))
            })?;
    } else {
        let sm = snapshot.full_state()?;
        let ((), _) = executor
            .drive_coroutine_sm_collecting_results(sm)
            .await
            .map_err(|e| {
                Error::generic(format!(
                    "execute full_state via DataFusionExecutor ({}): {e}",
                    config.name
                ))
            })?;
    }
    let table_configuration = snapshot.table_configuration();
    Ok(SnapshotResult {
        version: snapshot.version(),
        protocol: table_configuration.protocol().clone(),
        metadata: table_configuration.metadata().clone(),
    })
}

fn filter_batches_with_predicate(
    batches: Vec<RecordBatch>,
    predicate: Option<&Predicate>,
) -> DeltaResult<Vec<RecordBatch>> {
    let Some(predicate) = predicate else {
        return Ok(batches);
    };
    batches
        .into_iter()
        .map(|batch| {
            let selection = evaluate_predicate(predicate, &batch, false)?;
            let filtered = filter_record_batch(&batch, &selection)?;
            Ok(filtered)
        })
        .collect()
}

async fn execute_read_workload_datafusion(
    engine: Arc<dyn Engine>,
    table_root: &url::Url,
    read_spec: &ReadSpec,
    config: ReadReplayConfig,
) -> DeltaResult<ReadResult> {
    let version = read_spec
        .time_travel
        .as_ref()
        .map(TimeTravel::as_version)
        .transpose()
        .map_err(Error::generic)?;
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(version) = version {
        builder = builder.at_version(version);
    }
    let snapshot = builder.build(engine.as_ref())?;
    let table_schema = snapshot.schema();
    let predicate = if let Some(predicate_string) = read_spec.predicate.as_ref() {
        let predicate = parse_predicate(predicate_string, &table_schema).map_err(Error::generic)?;
        Some(Arc::new(predicate))
    } else {
        None
    };

    let schema = if let Some(cols) = read_spec.columns.as_ref() {
        table_schema.project(cols)?
    } else {
        table_schema.clone()
    };

    let executor = DataFusionExecutor::try_new_with_engine(engine)
        .map_err(|e| Error::generic(format!("create DataFusionExecutor: {e}")))?;
    let mut scan_builder = snapshot.scan_builder().with_schema(schema.clone());
    if let Some(predicate_ref) = predicate.clone() {
        scan_builder = scan_builder.with_predicate(predicate_ref);
    }
    let replay_scan = scan_builder
        .build_replay()
        .map_err(|e| Error::generic(format!("build replay scan: {e}")))?;
    let batches = if config.split_phases {
        let metadata_sm = replay_scan
            .replay_scan_metadata_state_machine()
            .map_err(|e| Error::generic(format!("build metadata-only replay scan SM: {e}")))?;
        let (live_actions, _) = executor
            .drive_coroutine_sm_collecting_results(metadata_sm)
            .await
            .map_err(|e| {
                Error::generic(format!(
                    "execute metadata-only replay scan SM via DataFusionExecutor ({}): {e:?}",
                    config.name
                ))
            })?;
        let data_sm = replay_scan
            .replay_scan_data_state_machine(live_actions)
            .map_err(|e| Error::generic(format!("build data-only replay scan SM: {e}")))?;
        let (_done, batches) = executor
            .drive_coroutine_sm_collecting_results(data_sm)
            .await
            .map_err(|e| {
                Error::generic(format!(
                    "execute data-only replay scan SM via DataFusionExecutor ({}): {e:?}",
                    config.name
                ))
            })?;
        batches
    } else {
        let replay_sm = replay_scan
            .replay_scan_state_machine()
            .map_err(|e| Error::generic(format!("build replay scan SM: {e}")))?;
        let (_done, batches) = executor
            .drive_coroutine_sm_collecting_results(replay_sm)
            .await
            .map_err(|e| {
                Error::generic(format!(
                    "execute combined replay scan SM via DataFusionExecutor ({}): {e:?}",
                    config.name
                ))
            })?;
        batches
    };
    let batches = filter_batches_with_predicate(batches, predicate.as_deref())?;
    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();

    Ok(ReadResult {
        batches,
        schema: schema.clone(),
        row_count,
    })
}

fn acceptance_workloads_datafusion_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();
    #[cfg(windows)]
    let spec_path_str = spec_path_str.replace('\\', "/");

    if should_skip_harness_path(&spec_path_str).is_some() {
        return Ok(());
    }
    if should_skip_datafusion_spec_type(workload_spec_type(&spec_path_abs).as_deref()).is_some() {
        return Ok(());
    }

    let test_case = TestCase::from_spec_path(&spec_path_abs);
    let table_root = test_case.table_root().expect("Failed to get table URL");
    let engine: Arc<dyn Engine> =
        test_utils::create_default_engine(&table_root).expect("Failed to create engine");

    // Keep DataFusion comparison scoped to workloads that pass in kernel's default engine harness.
    // This makes DataFusion ignore the same failing set as kernel. Log the skip so output remains
    // diagnosable when DataFusion silently regresses on workloads kernel handles.
    if let Err(e) = execute_and_validate_workload(
        Arc::clone(&engine),
        &table_root,
        &test_case.spec,
        &test_case.expected_dir(),
    ) {
        eprintln!(
            "SKIP DataFusion workload '{}': kernel-default-engine also fails ({e})",
            test_case.workload_name
        );
        return Ok(());
    }

    match &test_case.spec {
        Spec::Read(read_spec) => {
            let expected = match read_spec.expected.as_ref() {
                Some(expected) => expected,
                None => return Ok(()),
            };
            for config in READ_REPLAY_CONFIGS {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(execute_read_workload_datafusion(
                        Arc::clone(&engine),
                        &table_root,
                        read_spec,
                        *config,
                    ));
                if let Err(e) = validate_read_result(result, &test_case.expected_dir(), expected) {
                    let rendered = e.to_string();
                    return Err(Box::new(std::io::Error::other(format!(
                        "DataFusion workload '{}' failed for read replay config '{}': {rendered}",
                        test_case.workload_name, config.name
                    ))));
                }
            }
        }
        Spec::SnapshotConstruction(snapshot_spec) => {
            let expected = match snapshot_spec.expected.as_ref() {
                Some(expected) => expected,
                None => return Ok(()),
            };
            for config in SNAPSHOT_REPLAY_CONFIGS {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(execute_snapshot_workload_datafusion(
                        Arc::clone(&engine),
                        &table_root,
                        snapshot_spec.as_ref(),
                        *config,
                    ));
                if let Err(rendered) = validate_snapshot(result, expected) {
                    return Err(Box::new(std::io::Error::other(format!(
                        "DataFusion snapshot workload '{}' failed for snapshot replay config '{}': {rendered}",
                        test_case.workload_name, config.name
                    ))));
                }
            }
        }
    }
    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_datafusion_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
