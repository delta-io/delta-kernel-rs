use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

use delta_kernel::benchmarks::models::{default_read_configs, ReadConfig, ReadOperation, Spec};
use delta_kernel::benchmarks::runners::{create_read_runner, WorkloadRunner};
use delta_kernel::benchmarks::utils::load_all_workloads;

fn setup_engine() -> Arc<DefaultEngine<TokioBackgroundExecutor>> {
    use object_store::local::LocalFileSystem;

    let store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngine::builder(store).build();

    Arc::new(engine)
}

fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {}", e),
    };

    let engine = setup_engine();
    let mut group = c.benchmark_group("workload_benchmarks");

    for workload in workloads {
        match &workload.spec {
            Spec::Read { .. } => {
                for operation in [ReadOperation::ReadMetadata] {
                    let configs = choose_config();
                    for config in configs {
                        let runner =
                            create_read_runner(workload.clone(), operation, config, engine.clone())
                                .expect("Failed to create read runner");
                        run_benchmark(&mut group, runner.as_ref());
                    }
                }
            }
        }
    }

    group.finish();
}

fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    runner: &dyn WorkloadRunner,
) {
    let name = runner.name();

    group.bench_function(&name, |b| {
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
}

fn choose_config() -> Vec<ReadConfig> {
    //Choose which benchmark configurations to run for a given table
    //This function will take in table info to return the appropriate configs
    default_read_configs()
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
