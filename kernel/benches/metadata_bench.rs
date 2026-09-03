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

use std::num::NonZeroUsize;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::try_parse_uri;
use tempfile::TempDir;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::storage::store_from_url;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::load_test_data;
use url::Url;

// force scan metadata bench to use smaller sample size so test runs faster (100 -> 20)
const SCAN_METADATA_BENCH_SAMPLE_SIZE: usize = 20;

fn setup(
    parallel_chunks: Option<NonZeroUsize>,
) -> (TempDir, Url, Arc<DefaultEngine<TokioMultiThreadExecutor>>) {
    // note this table _only_ has a _delta_log, no data files (can only do metadata reads)
    let table = "300k-add-files-100-col-partitioned";
    let tempdir = load_test_data("./tests/data", table).unwrap();
    let table_path = tempdir.path().join(table);
    let url = try_parse_uri(table_path.to_str().unwrap()).expect("Failed to parse table path");
    let store = store_from_url(&url).expect("Failed to create store");
    let executor = Arc::new(
        TokioMultiThreadExecutor::new_owned_runtime(None, None).expect("Failed to create runtime"),
    );
    let engine = DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .with_parallel_chunks(parallel_chunks)
        .build();

    (tempdir, url, Arc::new(engine))
}

fn create_snapshot_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_snapshot");
    for (id, chunks) in [("serial", None), ("parallel_chunks", NonZeroUsize::new(10))] {
        let (_tempdir, url, engine) = setup(chunks);
        group.bench_function(id, |b| {
            b.iter(|| {
                Snapshot::builder_for(url.clone())
                    .build(engine.as_ref())
                    .expect("Failed to create snapshot")
            })
        });
    }
    group.finish();
}

fn scan_metadata_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup(None);

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let mut group = c.benchmark_group("scan_metadata");
    group.sample_size(SCAN_METADATA_BENCH_SAMPLE_SIZE);
    // Benchmark both the default path (kernel builds a per-file transform expression for this
    // partitioned table) and the `without_row_transforms` opt-out (which skips that build and the
    // per-row partition-value parse that feeds it).
    for without_row_transforms in [false, true] {
        let id = if without_row_transforms {
            "scan_metadata_without_row_transforms"
        } else {
            "scan_metadata"
        };
        group.bench_function(id, |b| {
            b.iter(|| {
                let mut builder = snapshot.clone().scan_builder();
                if without_row_transforms {
                    builder = builder.without_row_transforms();
                }
                let scan = builder.build().expect("Failed to build scan");
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
    group.finish();
}

criterion_group!(benches, create_snapshot_benchmark, scan_metadata_benchmark);
criterion_main!(benches);
