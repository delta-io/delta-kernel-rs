//! Scan execution metrics tests.
//!
//! Covers how `scan.execute()` contributes to I/O metrics: parquet data-file reads
//! through `DefaultParquetHandler::read_parquet_files` and the JSON log replay that
//! `scan.execute()` performs internally to collect Add/Remove scan metadata.

#[cfg(feature = "internal-api")]
use std::cell::Cell;
#[cfg(feature = "internal-api")]
use std::fs;
use std::sync::Arc;

#[cfg(feature = "internal-api")]
use delta_kernel::arrow::array::Int32Array;
#[cfg(feature = "internal-api")]
use delta_kernel::object_store::memory::InMemory;
#[cfg(feature = "internal-api")]
use delta_kernel::Error;
use delta_kernel::{DeltaResult, Engine, Snapshot};
#[cfg(feature = "internal-api")]
use rstest::rstest;
#[cfg(feature = "internal-api")]
use test_utils::{
    actions_to_string, add_commit, assert_result_error_with_message, create_default_engine,
    create_default_engine_with_batch, install_thread_local_metrics_reporter, into_record_batch,
    CountingReporter, TestAction,
};
#[cfg(feature = "internal-api")]
use url::Url;

use super::{measuring_engine, LogState, TestTableBuilder};

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
        .with_log_state(LogState::with_latest_version(2))
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

#[cfg(feature = "internal-api")]
#[rstest]
#[case::rejected(Ok(false))]
#[case::callback_error(Err("file assignment failed"))]
fn execute_file_filter_skips_data_and_persisted_dv_io(
    #[case] include: Result<bool, &'static str>,
) -> DeltaResult<()> {
    let path = fs::canonicalize("./tests/data/table-with-dv-small/")?;
    let url = Url::from_directory_path(path).expect("canonical path must form a file URL");
    let reporter = Arc::new(CountingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    reporter.reset();

    let callback_count = Cell::new(0);
    let engine: Arc<dyn Engine> = engine;
    let results = scan
        .execute_with_file_filter(engine, |_| {
            callback_count.set(callback_count.get() + 1);
            include.map_err(Error::generic)
        })?
        .collect::<DeltaResult<Vec<_>>>();

    match include {
        Ok(false) => assert!(results?.is_empty()),
        Err(message) => assert_result_error_with_message(results, message),
        Ok(true) => panic!("test requires a rejected file or callback error"),
    }
    assert_eq!(callback_count.get(), 1);
    assert!(
        reporter.json_read_calls.get() > 0,
        "scan metadata must still replay the transaction log"
    );
    assert_eq!(
        reporter.storage_read_calls.get(),
        0,
        "DV read must be skipped"
    );
    assert_eq!(
        reporter.parquet_read_calls.get(),
        0,
        "data-file read must be skipped"
    );
    Ok(())
}

#[cfg(feature = "internal-api")]
#[rstest]
#[case::included(Ok(true), Some("invalid IPv6 address"))]
#[case::rejected(Ok(false), None)]
#[case::callback_error(Err("file assignment failed"), Some("file assignment failed"))]
#[tokio::test]
async fn execute_file_filter_runs_before_path_resolution(
    #[case] include: Result<bool, &'static str>,
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table_root = "memory:///";
    add_commit(
        table_root,
        store.as_ref(),
        0,
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add("http://[".into()),
        ]),
    )
    .await?;
    let (engine, reporter, _guard) = measuring_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    reporter.reset();
    let callback_count = Cell::new(0);
    let results = scan
        .execute_with_file_filter(Arc::new(engine), |_| {
            callback_count.set(callback_count.get() + 1);
            include.map_err(Error::generic)
        })?
        .collect::<DeltaResult<Vec<_>>>();

    if let Some(message) = expected_error {
        assert_result_error_with_message(results, message);
    } else {
        assert!(results?.is_empty());
    }
    assert_eq!(callback_count.get(), 1);
    assert_eq!(reporter.storage_read_calls.get(), 0);
    assert_eq!(reporter.parquet_read_calls.get(), 0);
    Ok(())
}

#[cfg(feature = "internal-api")]
#[tokio::test]
async fn execute_file_filter_preserves_scan_metadata_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let metadata = actions_to_string(vec![TestAction::Metadata]);
    let add = serde_json::json!({"add": {
        "path": "missing-size.parquet",
        "partitionValues": {},
        "modificationTime": 0,
        "dataChange": true
    }});
    add_commit(table_root, store.as_ref(), 0, format!("{metadata}\n{add}")).await?;
    let (engine, reporter, _guard) = measuring_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    reporter.reset();
    let callback_count = Cell::new(0);
    let results = scan
        .execute_with_file_filter(Arc::new(engine), |_| {
            callback_count.set(callback_count.get() + 1);
            Ok(false)
        })?
        .collect::<DeltaResult<Vec<_>>>();

    assert_result_error_with_message(results, "non-nullable StructArray child");
    assert_eq!(callback_count.get(), 0);
    assert_eq!(reporter.storage_read_calls.get(), 0);
    assert_eq!(reporter.parquet_read_calls.get(), 0);
    Ok(())
}

#[cfg(feature = "internal-api")]
#[test]
fn execute_file_filter_applies_persisted_dv_across_multiple_batches() -> DeltaResult<()> {
    let path = fs::canonicalize("./tests/data/table-with-dv-small/")?;
    let url = Url::from_directory_path(path).expect("canonical path must form a file URL");
    let engine = create_default_engine_with_batch(&url, Some(3))?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let engine: Arc<dyn Engine> = engine;
    let mut batch_count = 0;
    let mut values = Vec::new();
    for result in scan.execute_with_file_filter(engine, |_| Ok(true))? {
        let batch = into_record_batch(result?);
        batch_count += 1;
        let column = batch
            .column_by_name("value")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .expect("table must contain an INTEGER value column");
        values.extend(column.values().iter().copied());
    }

    assert!(batch_count > 1, "the data file must span multiple batches");
    assert_eq!(values, (1..=8).collect::<Vec<_>>());
    Ok(())
}
