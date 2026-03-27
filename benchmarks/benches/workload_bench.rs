use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::local::LocalFileSystem;

use delta_kernel_benchmarks::models::{
    default_read_configs, ParallelScan, ReadConfig, ReadOperation, Spec,
};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;
use test_utils::CountingReporter;

fn setup_engine() -> (
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    Arc<CountingReporter>,
) {
    let store = Arc::new(LocalFileSystem::new());
    let reporter = Arc::new(CountingReporter::new());
    let executor = Arc::new(
        TokioMultiThreadExecutor::new_owned_runtime(None, None)
            .expect("Failed to create tokio multi-thread executor"),
    );
    let engine = DefaultEngine::builder(store)
        .with_task_executor(executor)
        .with_metrics_reporter(reporter.clone())
        .build();

    (Arc::new(engine), reporter)
}

// Loads all workloads and sets up a shared engine, then registers each as a top-level benchmark.
// For each workload, builds a runner that encapsulates the state (table info, engine, config, etc.)
// and execution logic. After each Criterion timing pass, runs one IO-profiling iteration and
// prints per-call storage and log-replay counts.
fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let (engine, reporter) = setup_engine();

    for workload in &workloads {
        match &workload.spec {
            Spec::Read(read_spec) => {
                for operation in [ReadOperation::ReadMetadata] {
                    for config in build_read_configs(&workload.table_info.name) {
                        let runner = create_read_runner(
                            &workload.table_info,
                            &workload.case_name,
                            read_spec,
                            operation,
                            config,
                            engine.clone(),
                        )
                        .expect("Failed to create read runner");
                        run_benchmark(c, runner.as_ref(), &reporter);
                    }
                }
            }
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                let runner = SnapshotConstructionRunner::setup(
                    &workload.table_info,
                    &workload.case_name,
                    snapshot_construction_spec,
                    engine.clone(),
                )
                .expect("Failed to create snapshot construction runner");
                run_benchmark(c, &runner, &reporter);
            }
        }
    }
}

// Registers a workload with Criterion and benchmarks its `execute()` function.
// After timing completes, runs one IO-profiling iteration and prints per-call storage and
// log-replay counts. The IO profile is skipped entirely when Criterion filters out the benchmark,
// since Criterion never calls the closure for filtered benchmarks.
fn run_benchmark(c: &mut Criterion, runner: &dyn WorkloadRunner, reporter: &CountingReporter) {
    let bench_ran = AtomicBool::new(false);
    c.bench_function(runner.name(), |b| {
        bench_ran.store(true, Ordering::Relaxed);
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
    if bench_ran.load(Ordering::Relaxed) {
        reporter.reset();
        runner.execute().expect("IO profiling iteration failed");
        reporter.print_summary(runner.name());
    }
}

fn build_read_configs(table_name: &str) -> Vec<ReadConfig> {
    // Choose which benchmark configurations to run for a given table
    // TODO: This function will take in table info to choose the appropriate configs for a given table
    let mut configs = default_read_configs();
    if table_name.contains("V2Chkpt") {
        configs.push(ReadConfig {
            name: "parallel2".into(),
            parallel_scan: ParallelScan::Enabled { num_threads: 2 },
        });
    }
    configs
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
