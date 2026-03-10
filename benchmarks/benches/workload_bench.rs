use criterion::{criterion_group, criterion_main, Criterion};

use delta_kernel_benchmarks::models::{
    default_read_configs, ParallelScan, ReadConfig, ReadOperation, Spec,
};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;

fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

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
                        )
                        .expect("Failed to create read runner");
                        c.bench_function(runner.name(), |b| {
                            b.iter(|| runner.execute().expect("Benchmark execution failed"))
                        });
                    }
                }
            }
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                let runner = SnapshotConstructionRunner::setup(
                    &workload.table_info,
                    &workload.case_name,
                    snapshot_construction_spec,
                )
                .expect("Failed to create snapshot construction runner");
                c.bench_function(runner.name(), |b| {
                    b.iter(|| runner.execute().expect("Benchmark execution failed"))
                });
            }
        }
    }
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
