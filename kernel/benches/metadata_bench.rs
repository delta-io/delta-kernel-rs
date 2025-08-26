//! Regression test for metadata (snapshot and scan) performance. This currently reads a table with
//! only JSON commits (checkpoints TODO) and measures (1) time to create a snapshot and (2) time to
//! create scan metadata.
//!
//! You can run this regression test with `cargo bench`.
//!
//! To compare your changes vs. latest main, you can:
//! ```bash
//! # checkout baseline branch (upstream/main) and save as baseline
//! git checkout main # or upstream/main, another branch, etc.
//! cargo bench --bench metadata_bench -- --save-baseline main
//!
//! # switch back to your changes, and compare against baseline
//! git checkout your-branch
//! cargo bench --bench metadata_bench -- --baseline main
//! ```
//!
//! Follow-ups: <https://github.com/delta-io/delta-kernel-rs/issues/1185>

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::try_parse_uri;

use delta_kernel::Predicate;
use test_utils::load_test_data;
use test_utils::workload_runner::*;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;
use url::Url;

// force scan metadata bench to use smaller sample size so test runs faster (100 -> 20)
const SCAN_METADATA_BENCH_SAMPLE_SIZE: usize = 20;

fn setup() -> (TempDir, Url, Arc<DefaultEngine<TokioBackgroundExecutor>>) {
    // note this table _only_ has a _delta_log, no data files (can only do metadata reads)
    let table = "300k-add-files-100-col-partitioned";
    let tempdir = load_test_data("./tests/data", table).unwrap();
    let table_path = tempdir.path().join(table);
    let url = try_parse_uri(table_path.to_str().unwrap()).expect("Failed to parse table path");
    // TODO: use multi-threaded executor
    let executor = Arc::new(TokioBackgroundExecutor::new());
    let engine = DefaultEngine::try_new(&url, HashMap::<String, String>::new(), executor)
        .expect("Failed to create engine");

    (tempdir, url, Arc::new(engine))
}

fn create_snapshot_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();

    c.bench_function("create_snapshot", |b| {
        b.iter(|| {
            Snapshot::try_new(url.clone(), engine.as_ref(), None)
                .expect("Failed to create snapshot")
        })
    });
}

fn scan_metadata_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();

    let snapshot = Arc::new(
        Snapshot::try_new(url.clone(), engine.as_ref(), None).expect("Failed to create snapshot"),
    );

    let mut group = c.benchmark_group("scan_metadata");
    group.sample_size(SCAN_METADATA_BENCH_SAMPLE_SIZE);
    group.bench_function("scan_metadata", |b| {
        b.iter(|| {
            let scan = snapshot
                .clone() // arc
                .scan_builder()
                .build()
                .expect("Failed to build scan");
            let metadata_iter = scan
                .scan_metadata(engine.as_ref())
                .expect("Failed to get scan metadata");
            // kernel scans are lazy, we must consume iterator to do the work we want to test
            for result in metadata_iter {
                result.expect("Failed to process scan metadata");
            }
        })
    });
    group.finish();
}

fn get_workloads() -> Vec<(WorkloadSpec, String)> {
    let workloads_dir = "./benches/workloads/";
    println!("current: {:?}", std::env::current_dir());

    // Check if the workloads directory exists
    if !Path::new(workloads_dir).exists() {
        panic!("Workloads directory '{}' does not exist", workloads_dir);
    }

    // Read all entries in the workloads directory
    let entries = fs::read_dir(workloads_dir).unwrap();

    let mut workloads = Vec::new();

    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();

        // Only process JSON files
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            println!("Processing file: {:?}", path);

            match fs::read_to_string(&path) {
                Ok(json_content) => match serde_json::from_str::<WorkloadSpec>(&json_content) {
                    Ok(workload) => {
                        let filename = path.file_name().unwrap().to_str().unwrap().to_string();
                        println!("Successfully parsed workload from: {:?}", filename);
                        workloads.push((workload, filename));
                    }
                    Err(e) => {
                        eprintln!("Failed to parse JSON from {:?}: {}", path, e);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to read file {:?}: {}", path, e);
                }
            }
        }
    }
    workloads
}

fn temp_benchmark(c: &mut Criterion) {
    // let file_path = "./benches/workloads/basic_metadata_workload.json";
    // println!("current: {:?}", std::env::current_dir());
    // let json_content = fs::read_to_string(file_path).unwrap();

    // Parse the JSON into our struct
    let workloads = get_workloads();

    // Print the parsed data
    println!("Parsed workload: {:#?}", workloads);

    let mut group = c.benchmark_group("scan_metadata");
    group.sample_size(SCAN_METADATA_BENCH_SAMPLE_SIZE);
    for workload in workloads {
        // Access the data
        match workload {
            (WorkloadSpec::ReadMetadata(read_metadata), name) => {
                println!("Table root: {}", read_metadata.table_root);
                println!("Version: {:?}", read_metadata.version);
                println!("Predicate: {:?}", read_metadata.predicate);
                println!(
                    "Expected scan metadata: {}",
                    read_metadata.expected_scan_metadata
                );

                let executor = Arc::new(TokioBackgroundExecutor::new());
                let engine = Arc::new(
                    DefaultEngine::try_new(
                        &read_metadata.table_root,
                        HashMap::<String, String>::new(),
                        executor,
                    )
                    .expect("Failed to create engine"),
                );

                let snapshot = Arc::new(
                    Snapshot::try_new(
                        read_metadata.table_root,
                        engine.as_ref(),
                        read_metadata.version,
                    )
                    .expect("Failed to create snapshot"),
                );
                println!("snapshot schema: {:?}", snapshot.schema());
                let predicate = read_metadata
                    .predicate
                    .as_ref()
                    .map(|pred| Arc::new(Predicate::BooleanExpression(pred.0.clone())));
                group.bench_function(format!("scan_metadata: {name}"), |b| {
                    b.iter(|| {
                        let scan = snapshot
                            .clone() // arc
                            .scan_builder()
                            .with_predicate(predicate.clone())
                            .build()
                            .expect("Failed to build scan");
                        let metadata_iter = scan
                            .scan_metadata(engine.as_ref())
                            .expect("Failed to get scan metadata");
                        // kernel scans are lazy, we must consume iterator to do the work we want to test
                        for result in metadata_iter {
                            result.expect("Failed to process scan metadata");
                        }
                    })
                });
            }
        }
    }
}

criterion_group!(benches, temp_benchmark);
criterion_main!(benches);
