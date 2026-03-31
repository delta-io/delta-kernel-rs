use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use delta_kernel_benchmarks::models::{
    default_read_configs, ParallelScan, ReadConfig, ReadOperation, Spec,
};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;

// Loads all workloads, then registers each as a top-level benchmark.
// For each workload, builds a runner that encapsulates the state (table info, engine, config, etc.)
// and execution logic.
fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let runtime =
        Arc::new(tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"));

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
                            runtime.clone(),
                        )
                        .expect("Failed to create read runner");
                        run_benchmark(c, runner.as_ref());
                    }
                }
            }
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                let runner = SnapshotConstructionRunner::setup(
                    &workload.table_info,
                    &workload.case_name,
                    snapshot_construction_spec,
                    runtime.clone(),
                )
                .expect("Failed to create snapshot construction runner");
                run_benchmark(c, &runner);
            }
        }
    }
}

// Registers a workload with Criterion and benchmarks its `execute()` function.
// TODO: Wire CountingReporter into engines for IO profiling (removed during engine refactor).
fn run_benchmark(c: &mut Criterion, runner: &dyn WorkloadRunner) {
    c.bench_function(runner.name(), |b| {
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
}

fn build_read_configs(table_name: &str) -> Vec<ReadConfig> {
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
