//! Smoke tests for [`DataFusionExecutor::scan_metadata`] / [`DataFusionExecutor::scan_data`]
//! routed through the SSA scan SMs.
//!
//! Cross-checks the SSA scan path's `scan_metadata` row count against the kernel
//! default-engine reference (`Scan::scan_metadata` add-path set, the same source of
//! truth used by the FSR golden tests). `scan_data` is asserted to drive without
//! error and produce some rows; the per-row data correctness is implicitly covered
//! by parity with the metadata add-path set + the engine-level scan-correctness
//! tests in `scan_correctness.rs`.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::Scan;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use rstest::rstest;
use tempfile::TempDir;
use url::Url;

struct FixtureTable {
    _tmp: Option<TempDir>,
    url: Url,
}

fn fixture_table(name: &str) -> FixtureTable {
    let data_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../kernel/tests/data");
    let direct = data_root.join(name);
    if direct.is_dir() {
        return FixtureTable {
            _tmp: None,
            url: Url::from_directory_path(direct.canonicalize().expect("fixture path"))
                .expect("table url"),
        };
    }
    let tmp = test_utils::load_test_data("../kernel/tests/data", name)
        .unwrap_or_else(|e| panic!("load archived fixture {name}: {e}"));
    let extracted = tmp.path().join(name);
    assert!(
        extracted.is_dir(),
        "archived fixture extraction missing directory: {}",
        extracted.display()
    );
    FixtureTable {
        _tmp: Some(tmp),
        url: Url::from_directory_path(extracted.canonicalize().expect("extracted fixture path"))
            .expect("table url"),
    }
}

fn default_engine() -> Arc<dyn KernelEngine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

fn open_scan(table: &str) -> (Arc<dyn KernelEngine>, Scan) {
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(fixture_table(table).url)
        .build(engine.as_ref())
        .expect("snapshot");
    let scan = snapshot.scan_builder().build().expect("scan builder build");
    (engine, scan)
}

/// Reference live-file count via the kernel default-engine scan_metadata path.
fn kernel_reference_live_file_count(scan: &Scan, engine: &dyn KernelEngine) -> usize {
    let mut total = 0usize;
    for batch in scan.scan_metadata(engine).expect("scan_metadata") {
        let metadata = batch.expect("scan_metadata batch");
        for selected in metadata.scan_files.selection_vector() {
            if *selected {
                total += 1;
            }
        }
    }
    total
}

/// SSA `scan_metadata` row count matches the kernel default-engine reference live-file
/// count across the FSR golden fixtures. Mirrors the fixture set used by `fsr_real.rs`
/// so any disagreement points at the scan-specific terminal (action_pair -> flat
/// scan_file_row), not the shared reconciliation.
#[rstest]
#[case::commit_only("app-txn-no-checkpoint")]
#[case::v1_checkpoint("app-txn-checkpoint")]
#[case::dv_small("table-with-dv-small")]
#[case::no_dv_small("table-without-dv-small")]
#[case::v2_classic_parquet("v2-classic-parquet-struct-stats-only")]
#[case::v2_json_sidecars("v2-json-sidecars-struct-stats-only")]
#[case::v2_parquet_sidecars("v2-parquet-sidecars-struct-stats-only")]
#[tokio::test]
async fn scan_metadata_ssa_row_count_matches_kernel_reference(#[case] fixture: &str) {
    let (engine, scan) = open_scan(fixture);
    let expected = kernel_reference_live_file_count(&scan, engine.as_ref());

    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let df = executor.scan_metadata(&scan).await.expect("scan_metadata");
    let batches = df.collect().await.expect("collect scan_metadata");
    let ssa_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        ssa_rows, expected,
        "SSA scan_metadata row count vs kernel reference for {fixture}"
    );
}

/// SSA `scan_data` drives end-to-end without error and produces at least one row.
/// The detailed per-row data correctness is covered by `scan_correctness.rs` plus
/// kernel parity in the metadata test above; this case asserts the full data-stage
/// pipeline (Load + logical projection on top of reconciliation) wires up.
#[rstest]
#[case::commit_only("app-txn-no-checkpoint")]
#[case::v1_checkpoint("app-txn-checkpoint")]
#[case::dv_small("table-with-dv-small")]
#[case::no_dv_small("table-without-dv-small")]
#[tokio::test]
async fn scan_data_ssa_drives_without_error(#[case] fixture: &str) {
    let (engine, scan) = open_scan(fixture);
    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let df = executor.scan_data(&scan).await.expect("scan_data");
    let batches = df.collect().await.expect("collect scan_data");
    let row_total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        row_total > 0,
        "SSA scan_data produced zero rows for {fixture}"
    );
}
