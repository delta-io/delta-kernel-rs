//! Scan execution metrics tests.
//!
//! Covers how `scan.execute()` contributes to I/O metrics: parquet data-file reads
//! through `DefaultParquetHandler::read_parquet_files` and the JSON log replay that
//! `scan.execute()` performs internally to collect Add/Remove scan metadata.

use super::{measuring_engine, LogState, TestTableBuilder};
use std::sync::Arc;

use delta_kernel::{DeltaResult, Engine, Snapshot};

// ============================================================================
// scan.execute() contributes parquet data-file reads
// ============================================================================

/// `scan.execute()` reads the actual parquet data files written during inserts.
/// These go through `DefaultParquetHandler::read_parquet_files` and appear in
/// `parquet_read_calls`, separately from any checkpoint reads. Resetting the
/// reporter after snapshot construction isolates the scan I/O.
///
/// Note: `scan.execute()` also does its own log replay (to collect Add/Remove
/// actions for scan metadata), so `json_read_calls` is non-zero even after the
/// reporter reset.
#[test]
fn scan_execute_contributes_parquet_data_file_reads() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_commits(3))
        .with_data(1, 1)
        .build()?;

    let (engine, reporter, _guard) = measuring_engine(table.store().clone());
    let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;

    // Reset after snapshot build to isolate scan I/O
    reporter.reset();

    let engine: Arc<dyn Engine> = Arc::new(engine);
    let mut batches_seen = 0usize;
    for result in snap.scan_builder().build()?.execute(engine)? {
        result?;
        batches_seen += 1;
    }
    assert_eq!(
        batches_seen, 2,
        "scan should return one batch per data file"
    );

    // scan calls read_parquet_files once per data file (not batched), so 2 calls for 2 files
    assert_eq!(reporter.parquet_read_calls.get(), 2);
    assert_eq!(reporter.parquet_files_read.get(), 2);
    // On Windows (NTFS), listing a recently written file can return size=0 because the OS
    // has not yet flushed size metadata to the directory entry.
    #[cfg(not(windows))]
    assert!(reporter.parquet_bytes_read.get() > 0);
    // scan.execute() does its own log replay for Add/Remove scan metadata
    assert_eq!(reporter.json_read_calls.get(), 1);

    Ok(())
}
