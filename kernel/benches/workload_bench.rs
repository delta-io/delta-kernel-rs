use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

use delta_kernel::benchmarks::models::{
    default_read_configs, ReadConfig, ReadOperation, Spec, WorkloadVariant,
};
use delta_kernel::benchmarks::runners::ReadMetadataRunner;
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
        Ok(_) => {
            panic!("No workloads found");
        }
        Err(e) => {
            panic!("Failed to load workloads: {}", e);
        }
    };

    let engine = setup_engine();
    let mut read_metadata_group = c.benchmark_group("read_metadata_benchmarks");

    for workload in workloads {
        match &workload.spec {
            Spec::Read { .. } => {
                for operation in [ReadOperation::ReadMetadata] {
                    let configs = choose_config();
                    for config in configs {
                        let variant = WorkloadVariant {
                            table_info: workload.table_info.clone(),
                            case_name: workload.case_name.clone(),
                            spec: workload.spec.clone(),
                            operation: Some(operation),
                            config,
                        };
                        run_benchmark(&mut read_metadata_group, variant, &engine);
                    }
                }
            }
        }
    }

    read_metadata_group.finish();
}

fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    variant: WorkloadVariant,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
) {
    let runner = ReadMetadataRunner::setup(variant, engine.clone())
        .expect("Failed to setup benchmark runner");

    let name = runner.name().expect("Failed to get benchmark name");

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
