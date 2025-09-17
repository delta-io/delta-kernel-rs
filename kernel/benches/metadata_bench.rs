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
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::try_parse_uri;

use test_utils::load_test_data;

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
            Snapshot::builder_for(url.clone())
                .build(engine.as_ref())
                .expect("Failed to create snapshot")
        })
    });
}

fn scan_metadata_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

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

criterion_group!(benches, create_snapshot_benchmark, scan_metadata_benchmark);
criterion_main!(benches);
