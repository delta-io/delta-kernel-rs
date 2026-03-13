use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion};

use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::Engine;

use delta_kernel_benchmarks::models::{
    default_read_configs, ParallelScan, ReadConfig, ReadOperation, Spec, TableInfo,
};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;

/// Resolves a table's engine based on its configuration:
/// 1. `catalog_managed_info` present -> UC credential vending (gets S3 path + temp creds from UC)
/// 2. `table_path` starts with s3:// or s3a:// -> direct S3 access (uses AWS env vars)
/// 3. Otherwise -> local filesystem (existing behavior)
///
/// Also returns the resolved table path (for UC tables, this is the S3 URI from UC).
fn setup_engine(table_info: &TableInfo) -> (Arc<dyn Engine>, Option<String>) {
    if let Some(cm) = &table_info.catalog_managed_info {
        setup_uc_engine(cm)
    } else {
        let table_root = table_info.resolved_table_root();
        if table_root.starts_with("s3://") || table_root.starts_with("s3a://") {
            (setup_s3_engine(&table_root), None)
        } else {
            (setup_local_engine(), None)
        }
    }
}

fn build_engine(store: Arc<dyn object_store::ObjectStore>) -> Arc<dyn Engine> {
    let executor = TokioMultiThreadExecutor::new_owned_runtime(None, None)
        .expect("Failed to create multi-threaded tokio runtime");
    let engine = DefaultEngineBuilder::new(store)
        .with_task_executor(Arc::new(executor))
        .build();
    Arc::new(engine)
}

/// Creates an engine via UC credential vending.
/// Requires UC_WORKSPACE (e.g. "https://my-workspace.cloud.databricks.com") and UC_TOKEN.
fn setup_uc_engine(
    cm: &delta_kernel_benchmarks::models::CatalogManagedInfo,
) -> (Arc<dyn Engine>, Option<String>) {
    use uc_client::prelude::*;

    let endpoint =
        std::env::var("UC_WORKSPACE").expect("UC_WORKSPACE required for catalog-managed tables");
    let token = std::env::var("UC_TOKEN").expect("UC_TOKEN required for catalog-managed tables");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let config = uc_client::ClientConfig::build(&endpoint, &token)
        .build()
        .expect("Failed to build UC client config");
    let uc_client = UCClient::new(config).expect("Failed to create UC client");

    let (table_id, table_uri) = rt.block_on(async {
        let res = uc_client
            .get_table(&cm.table_name)
            .await
            .expect("Failed to get table from UC");
        (res.table_id, res.storage_location)
    });

    let creds = rt.block_on(async {
        uc_client
            .get_credentials(&table_id, Operation::Read)
            .await
            .expect("Failed to get credentials from UC")
    });

    let aws_creds = creds
        .aws_temp_credentials
        .expect("No AWS credentials from UC");

    let options = [
        ("region", "us-west-2"),
        ("access_key_id", aws_creds.access_key_id.as_str()),
        ("secret_access_key", aws_creds.secret_access_key.as_str()),
        ("session_token", aws_creds.session_token.as_str()),
    ];

    let table_url =
        url::Url::parse(&table_uri).expect("Failed to parse table URI from UC response");
    let (store, _) =
        object_store::parse_url_opts(&table_url, options).expect("Failed to create object store");

    (build_engine(store.into()), Some(table_uri))
}

/// Creates an engine for direct S3 access.
/// Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN from environment.
/// AWS_REGION defaults to us-west-2 if not set.
fn setup_s3_engine(table_root: &str) -> Arc<dyn Engine> {
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    let table_url = url::Url::parse(table_root).expect("Failed to parse S3 table path");

    let mut options: Vec<(String, String)> = vec![("region".to_string(), region)];

    // Forward AWS credentials from environment if present
    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
        options.push(("access_key_id".to_string(), key));
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        options.push(("secret_access_key".to_string(), secret));
    }
    if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
        options.push(("session_token".to_string(), token));
    }

    let (store, _) = object_store::parse_url_opts(&table_url, options)
        .expect("Failed to create S3 object store");
    build_engine(store.into())
}

/// Creates a local filesystem engine (existing behavior).
fn setup_local_engine() -> Arc<dyn Engine> {
    use object_store::local::LocalFileSystem;
    let store = Arc::new(LocalFileSystem::new());
    build_engine(store)
}

// Loads all workloads and sets up per-table engines.
// For each workload, builds a runner that encapsulates the state (table info, engine, config, etc.) and execution logic
fn workload_benchmarks(c: &mut Criterion) {
    let mut workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let mut group = c.benchmark_group("workload_benchmarks");

    for workload in &mut workloads {
        let (engine, resolved_path) = setup_engine(&workload.table_info);

        // For UC tables, set table_path to the resolved S3 URI so runners can use it
        if let Some(uri) = resolved_path {
            workload.table_info.table_path = Some(uri);
        }

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
                        run_benchmark(&mut group, runner.as_ref());
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
                run_benchmark(&mut group, &runner);
            }
        }
    }

    group.finish();
}

fn run_benchmark(group: &mut BenchmarkGroup<WallTime>, runner: &dyn WorkloadRunner) {
    group.bench_function(runner.name(), |b| {
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
}

fn build_read_configs(table_name: &str) -> Vec<ReadConfig> {
    let mut configs = default_read_configs();

    // KERNEL_BENCH_PARALLEL: comma-separated thread counts (e.g. "2,4,8")
    // Adds parallel configs for all tables when set.
    if let Ok(val) = std::env::var("KERNEL_BENCH_PARALLEL") {
        for s in val.split(',') {
            if let Ok(n) = s.trim().parse::<usize>() {
                if n > 0 {
                    configs.push(ReadConfig {
                        name: format!("parallel_{}", n),
                        parallel_scan: ParallelScan::Enabled { num_threads: n },
                    });
                }
            }
        }
    } else if table_name.contains("v2_checkpoint") {
        configs.push(ReadConfig {
            name: "parallel_2".into(),
            parallel_scan: ParallelScan::Enabled { num_threads: 2 },
        });
    }

    configs
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
