//! Scan execution metrics tests.
//!
//! Covers how `scan.execute()` contributes to I/O metrics: parquet data-file reads
//! through `DefaultParquetHandler::read_parquet_files` and the JSON log replay that
//! `scan.execute()` performs internally to collect Add/Remove scan metadata.

use super::{measuring_engine, simple_schema};
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use test_utils::insert_data;
use url::Url;

// ---------------------------------------------------------------------------
// Scenario 10: scan.execute() contributes parquet data-file reads
// ---------------------------------------------------------------------------

/// `scan.execute()` reads the actual parquet data files written during inserts.
/// These go through `DefaultParquetHandler::read_parquet_files` and appear in
/// `parquet_read_calls`, separately from any checkpoint reads. Resetting the
/// reporter after snapshot construction isolates the scan I/O.
///
/// Note: `scan.execute()` also does its own log replay (to collect Add/Remove
/// actions for scan metadata), so `json_read_calls` is non-zero even after the
/// reporter reset.
#[tokio::test]
async fn scan_execute_contributes_parquet_data_file_reads() -> DeltaResult<()> {
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///").unwrap();
    let setup_engine = Arc::new(DefaultEngineBuilder::new(store.clone() as Arc<_>).build());

    let _ = create_table("memory:///", simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    // Two inserts, each writing one parquet data file
    for val in [1i32, 2] {
        let snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
        let _ = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?;
    }

    let (engine, reporter) = measuring_engine(store);
    let snap = Snapshot::builder_for(table_url).build(&engine)?;

    // Reset after snapshot build to isolate scan I/O
    reporter.reset();

    let engine: Arc<dyn Engine> = Arc::new(engine);
    let mut batches_seen = 0usize;
    for result in snap.scan_builder().build()?.execute(engine)? {
        result?;
        batches_seen += 1;
    }
    assert!(batches_seen > 0, "scan should return rows");

    // scan calls read_parquet_files once per data file (not batched), so 2 calls for 2 files
    assert_eq!(reporter.parquet_read_calls.get(), 2);
    assert_eq!(reporter.parquet_files_read.get(), 2);
    assert!(reporter.parquet_bytes_read.get() > 0);
    // scan.execute() does its own log replay for Add/Remove scan metadata
    assert_eq!(reporter.json_read_calls.get(), 1);

    Ok(())
}
