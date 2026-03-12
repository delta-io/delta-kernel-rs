//! Runs a single benchmark workload once, for clean profiling with samply or perf.
//!
//! Usage:
//!   run_once <filter>
//!
//! The filter matches against workload names (table/spec). Only the first match is run.
//! Example:
//!   run_once "10k_checkpoint_with_parsed_stats/read_highly_selective"

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use object_store::local::LocalFileSystem;

use delta_kernel_benchmarks::models::{ReadOperation, Spec};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;

fn setup_engine() -> Arc<DefaultEngine<TokioBackgroundExecutor>> {
    let store = Arc::new(LocalFileSystem::new());
    Arc::new(DefaultEngine::builder(store).build())
}

fn main() {
    let filter = std::env::args().nth(1).unwrap_or_default();
    let iters: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let workloads = load_all_workloads().expect("Failed to load workloads");
    let engine = setup_engine();

    let mut matched = false;
    for workload in &workloads {
        let workload_id = format!("{}/{}", workload.table_info.name, workload.case_name);
        if !filter.is_empty() && !workload_id.contains(&filter) {
            continue;
        }

        eprintln!("Running: {} ({} iterations)", workload_id, iters);
        for _ in 0..iters {
            match &workload.spec {
                Spec::Read(read_spec) => {
                    let config = delta_kernel_benchmarks::models::ReadConfig {
                        name: "serial".into(),
                        parallel_scan: delta_kernel_benchmarks::models::ParallelScan::Disabled,
                    };
                    let runner = create_read_runner(
                        &workload.table_info,
                        &workload.case_name,
                        read_spec,
                        ReadOperation::ReadMetadata,
                        config,
                        engine.clone(),
                    )
                    .expect("Failed to create runner");
                    runner.execute().expect("Runner failed");
                }
                Spec::SnapshotConstruction(spec) => {
                    let runner = SnapshotConstructionRunner::setup(
                        &workload.table_info,
                        &workload.case_name,
                        spec,
                        engine.clone(),
                    )
                    .expect("Failed to create runner");
                    runner.execute().expect("Runner failed");
                }
            }
        }
        matched = true;
        break;
    }

    if !matched {
        eprintln!("No workload matched filter: {:?}", filter);
        eprintln!("Available workloads:");
        for w in &workloads {
            eprintln!("  {}/{}", w.table_info.name, w.case_name);
        }
        std::process::exit(1);
    }
}
