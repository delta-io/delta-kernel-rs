//! Executes checkpoint specs on isolated copies and checks the persisted table state.

use std::path::Path;
use std::sync::Arc;

use delta_kernel::actions::SetTransaction;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
#[cfg(not(feature = "declarative-plans"))]
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
#[cfg(not(feature = "declarative-plans"))]
use delta_kernel::snapshot::CheckpointWriteResult;
#[cfg(not(feature = "declarative-plans"))]
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Engine, KernelResult, Snapshot, Version};
#[cfg(feature = "declarative-plans")]
use delta_kernel::{
    engine::arrow_conversion::TryFromArrow as _,
    engine::arrow_data::ArrowEngineData,
    engine::{plans::PlanBasedEngine, sync::plan::SyncPlanExecutor},
    expressions::{ColumnName, lit, null_lit},
    plans::{Operation, PlanExecutor as _, ir::nodes::FileType},
    schema::StructType,
    struct_patch::ProjectionStructPatchBuilder,
};
use delta_kernel_workloads::models::Spec;
use test_utils::copy_directory;
use url::Url;

use super::workload::{execute_and_validate_workload, execute_read_workload};
use super::{LoadedTestCase, TestCase};
use crate::data::assert_data_matches;

pub(super) fn execute_checkpoint_workload(
    source_engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Version,
    expected_dir: &Path,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let source = table_root
        .to_file_path()
        .map_err(|_| "Checkpoint specs require a local table")?;
    let temp = tempfile::tempdir()?;
    copy_directory(&source, temp.path())?;
    let written_root =
        Url::from_directory_path(temp.path()).map_err(|_| "Invalid temporary path")?;
    let log = temp.path().join("_delta_log");
    // Remove fixture checkpoints and CRCs so the operation must reconstruct and write new state.
    for entry in std::fs::read_dir(&log)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy();
        if name == "_last_checkpoint" || name.contains(".checkpoint.") || name.ends_with(".crc") {
            std::fs::remove_file(path)?;
        }
    }
    // Checkpoint writes pull their input while writing; use a runtime with multiple workers.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let _guard = runtime.enter();
    let engine: Arc<dyn Engine> = test_utils::create_default_engine_mt_executor(&written_root)?;
    let snapshot = Snapshot::builder_for(&written_root)
        .at_version(version)
        .build(engine.as_ref())?;
    let expected = checkpoint_actions(&snapshot, engine.as_ref())?;
    #[cfg(feature = "declarative-plans")]
    let expected = {
        // Compare Runtime's checkpoint payload rules with the unchanged imperative reference.
        let input = Arc::new(StructType::try_from_arrow(expected.schema())?);
        let mut patch = ProjectionStructPatchBuilder::new(&input)
            .replace_expr_at(["add"], "dataChange", lit(false))
            .replace_expr_at(["remove"], "dataChange", lit(false));
        for name in ["stats", "tags"] {
            let field = input.field_at(&ColumnName::new(["remove", name]))?;
            patch = patch.replace_expr_at(["remove"], name, null_lit(field.data_type().clone()));
        }
        let (output, expression) = patch.build()?;
        engine
            .evaluation_handler()
            .new_expression_evaluator(input, expression, output.into())?
            .evaluate(&ArrowEngineData::new(expected))?
            .try_into_record_batch()?
    };

    write_checkpoint(snapshot, engine.clone(), &log)?;

    // Keep the target commit for timestamp lookup; older JSON cannot mask a broken checkpoint.
    for entry in std::fs::read_dir(&log)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json")
            && path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<Version>().ok())
                .is_some_and(|commit| commit < version)
        {
            std::fs::remove_file(path)?;
        }
    }
    let reloaded = Snapshot::builder_for(&written_root)
        .at_version(version)
        .build(engine.as_ref())?;
    assert_eq!(reloaded.log_segment().checkpoint_version, Some(version));
    let actual = checkpoint_actions(&reloaded, engine.as_ref())?;
    assert_data_matches(vec![actual], &expected.schema(), expected)?;

    let case = expected_dir
        .parent()
        .and_then(Path::parent)
        .ok_or("Invalid checkpoint expected directory")?;
    for entry in std::fs::read_dir(case.join("specs"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json")
            && let LoadedTestCase::Supported(test) = TestCase::load(&path)?
        {
            match &test.spec {
                Spec::Read(spec) => {
                    // Compare successful reads directly; golden data may have timestamp precision
                    // divergences already covered by the standalone read specs.
                    let expected = execute_read_workload(source_engine.clone(), table_root, spec)?;
                    let schema = Arc::new(ArrowSchema::try_from_kernel(expected.schema.as_ref())?);
                    let expected = concat_batches(&schema, &expected.batches)?;
                    let actual = execute_read_workload(engine.clone(), &written_root, spec)?;
                    assert_data_matches(actual.batches, &schema, expected)?;
                }
                Spec::SnapshotConstruction(_) => {
                    execute_and_validate_workload(
                        engine.clone(),
                        &written_root,
                        &test.spec,
                        &test.expected_dir(),
                    )?;
                    let spec: serde_json::Value =
                        serde_json::from_reader(std::fs::File::open(path)?)?;
                    let expected = &spec["expected"];
                    for txn in expected["setTransactions"].as_array().into_iter().flatten() {
                        let txn: SetTransaction = serde_json::from_value(txn.clone())?;
                        assert_eq!(
                            reloaded
                                .reader()
                                .get_app_id_version(txn.app_id(), engine.as_ref())?,
                            Some(txn.version())
                        );
                    }
                    for domain in expected["domainMetadata"].as_array().into_iter().flatten() {
                        assert_eq!(
                            reloaded.reader().get_domain_metadata(
                                domain["domain"].as_str().ok_or("Missing domain")?,
                                engine.as_ref(),
                            )?,
                            domain["configuration"].as_str().map(str::to_owned)
                        );
                    }
                }
                Spec::Checkpoint { .. } => {}
            }
        }
    }
    Ok(())
}

fn write_checkpoint(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    log: &Path,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "declarative-plans")]
    {
        let executor = Arc::new(SyncPlanExecutor::new(None));
        let planning_engine = PlanBasedEngine::new(Some(engine.clone()), executor.clone());
        let writer = snapshot.create_checkpoint_writer(&planning_engine)?;
        let plan = writer.declarative_checkpoint_plan(&planning_engine, FileType::Parquet)?;
        let bytes = writer
            .last_checkpoint_bytes_from_result(executor.execute_op(Operation::QueryPlan(plan))?)?;
        std::fs::write(log.join("_last_checkpoint"), bytes)?;
        Ok(())
    }
    #[cfg(not(feature = "declarative-plans"))]
    {
        let v2 = snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::V2Checkpoint);
        let spec = v2.then_some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }));
        assert_eq!(
            snapshot.checkpoint(engine.as_ref(), spec.as_ref())?.0,
            CheckpointWriteResult::Written
        );
        assert!(log.join("_last_checkpoint").is_file());
        Ok(())
    }
}

fn checkpoint_actions(
    snapshot: &Arc<Snapshot>,
    engine: &dyn Engine,
) -> KernelResult<RecordBatch, Box<dyn std::error::Error>> {
    let batches = snapshot
        .clone()
        .create_checkpoint_writer(engine)?
        .checkpoint_data(engine)?
        .map(|batch| batch?.apply_selection_vector()?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    Ok(concat_batches(&batches[0].schema(), &batches)?)
}
