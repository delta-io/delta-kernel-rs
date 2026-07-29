use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, Ordering as AtomicOrdering};
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

struct TrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static BASELINE_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            record_allocation(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            record_allocation(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size(), AtomicOrdering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, AtomicOrdering::Relaxed);
            }
        }
        new_ptr
    }
}

fn record_allocation(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, AtomicOrdering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(live, AtomicOrdering::Relaxed);
}

fn reset_allocation_peak() {
    let baseline = LIVE_BYTES.load(AtomicOrdering::Relaxed);
    BASELINE_BYTES.store(baseline, AtomicOrdering::Relaxed);
    PEAK_BYTES.store(baseline, AtomicOrdering::Relaxed);
}

fn allocation_peak_delta() -> usize {
    PEAK_BYTES
        .load(AtomicOrdering::Relaxed)
        .saturating_sub(BASELINE_BYTES.load(AtomicOrdering::Relaxed))
}

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
    registry
        .validate(&workloads)
        .expect("bench-registry.json must match the loaded workload types");

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
                        let concurrent_base_name = name.clone();
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
                        if case_name == "readMetadataLatest" && config.name == "serial" {
                            for query_count in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
                                let concurrent_name =
                                    format!("{concurrent_base_name}/concurrent{query_count}");
                                let peak_bytes = AtomicUsize::new(0);
                                c.bench_function(&concurrent_name, |b| {
                                    b.iter(|| {
                                        reset_allocation_peak();
                                        runner
                                            .execute_concurrent(query_count)
                                            .expect("Concurrent execution failed");
                                        peak_bytes.store(
                                            allocation_peak_delta(),
                                            AtomicOrdering::Relaxed,
                                        );
                                    })
                                });
                                println!(
                                    "[alloc] {concurrent_name}: peak +{} KiB",
                                    peak_bytes.load(AtomicOrdering::Relaxed) / 1024
                                );
                            }
                        }
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
