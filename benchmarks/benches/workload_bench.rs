use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use delta_kernel_benchmarks::registry::BenchRegistry;
use delta_kernel_benchmarks::runners::{
    benchmark_name, configured_benchmark_name, create_read_runner, SnapshotConstructionRunner,
    WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;
use delta_kernel_workloads::models::{ReadOperation, Spec};
use test_utils::CountingReporter;

// Checked-in registry mapping each benchmark to its harness configs. Lives under the crate root
// (not the gitignored, downloaded `workloads/` dir), so it is loaded relative to
// CARGO_MANIFEST_DIR.
const REGISTRY_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/bench-registry.json");

// Loads all workloads and sets up a shared runtime, then registers each as a top-level benchmark.
// For each workload, builds a runner that encapsulates the state (table info, engine, config, etc.)
// and execution logic. After each Criterion timing pass, runs one IO-profiling iteration and
// prints per-call storage and log-replay counts.
fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let registry = BenchRegistry::load_from_path(Path::new(REGISTRY_PATH))
        .expect("Failed to load bench-registry.json");

    let reporter = Arc::new(CountingReporter::new());
    let runtime = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"));

    for workload in &workloads {
        let case_name = &workload.case_name;
        match &workload.spec {
            Spec::Read(read_spec) => {
                let configs = registry
                    .read_configs(workload)
                    .expect("loaded workload must have a registry table key");
                for operation in [ReadOperation::ReadMetadata] {
                    for config in &configs {
                        let name = configured_benchmark_name(
                            &workload.table_info,
                            case_name,
                            &config.name,
                        );
                        let runner = create_read_runner(
                            name,
                            read_spec,
                            operation,
                            config.clone(),
                            &workload.table_info,
                            runtime.clone(),
                        )
                        .expect("Failed to create read runner");
                        run_benchmark(c, runner.as_ref(), &reporter);
                    }
                }
            }
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                let name = benchmark_name(&workload.table_info, case_name);
                let runner = SnapshotConstructionRunner::setup(
                    name,
                    snapshot_construction_spec,
                    &workload.table_info,
                    runtime.clone(),
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

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
