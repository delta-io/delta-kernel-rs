//! This tests our memory usage for reading tables with large data files.
//!
//! run with `cargo test -p mem-test dhat_large_table_data -- --ignored --nocapture`

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::parquet::arrow::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::Snapshot;

use arrow::compute::filter_record_batch;
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::tempdir;
use url::Url;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const NUM_ROWS: u64 = 1_000_000;

/// write a 1M row parquet file that is 1GB in size
fn write_large_parquet_to(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let path = path.join("1.parquet");
    let file = File::create(&path)?;

    let i_col = Arc::new(Int64Array::from_iter_values(0..NUM_ROWS as i64)) as ArrayRef;
    let s_col = (0..NUM_ROWS).map(|i| format!("val_{}_{}", i, "XYZ".repeat(350)));
    let s_col = Arc::new(StringArray::from_iter_values(s_col)) as ArrayRef;
    let rb = RecordBatch::try_from_iter(vec![("i", i_col.clone()), ("s", s_col.clone())])?;

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, rb.schema(), Some(props))?;
    writer.write(&rb)?;
    let parquet_metadata = writer.close()?;

    // read to show file sizes
    let metadata = std::fs::metadata(&path)?;
    let file_size = metadata.len();
    let total_row_group_size: i64 = parquet_metadata
        .row_groups
        .iter()
        .map(|rg| rg.total_byte_size)
        .sum();
    println!("File size (compressed file size):    {} bytes", file_size);
    println!(
        "Total size (uncompressed file size): {} bytes",
        total_row_group_size
    );

    Ok(())
}

/// create a _delta_log/00000000000000000000.json file with a single add file for our 1.parquet
/// above
fn create_commit(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let delta_log_path = path.join("_delta_log");
    create_dir_all(&delta_log_path)?;
    let commit_path = delta_log_path.join("00000000000000000000.json");
    let mut file = File::create(&commit_path)?;

    let actions = vec![
        json!({
            "metaData": {
                "id": "00000000000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"i\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {}
            }
        }),
        json!({
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 1
            }
        }),
        json!({
            "add": {
                "path": "1.parquet",
                "partitionValues": {},
                "size": 1000000000,
                "modificationTime": 0,
                "dataChange": true
            }
        }),
    ];

    for action in actions {
        writeln!(file, "{}", action)?;
    }

    Ok(())
}

#[ignore = "mem-test - run manually"]
#[test]
fn test_dhat_large_table_data() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let table_path = dir.path();
    let _profiler = dhat::Profiler::builder().testing().build();

    // Step 1: Write the large parquet file
    write_large_parquet_to(table_path)?;
    let stats = dhat::HeapStats::get();
    println!("Heap stats after writing parquet:\n{:?}", stats);

    // Step 2: Create the Delta log
    create_commit(table_path)?;

    // Step 3: Create engine and snapshot
    let store = Arc::new(LocalFileSystem::new());
    let url = Url::from_directory_path(table_path).unwrap();
    let engine = Arc::new(DefaultEngine::new(
        store,
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let stats = dhat::HeapStats::get();
    println!("Heap stats after creating snapshot:\n{:?}", stats);

    // Step 4: Build and execute scan
    let scan = snapshot
        .scan_builder()
        .build()
        .expect("Failed to build scan");

    let stats = dhat::HeapStats::get();
    println!("Heap stats after building scan:\n{:?}", stats);

    // Step 5: Execute the scan and read data
    let mut row_count = 0;
    for scan_result in scan.execute(engine)? {
        let scan_result = scan_result?;
        let mask = scan_result.full_mask();
        let data = scan_result.raw_data?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into();

        let batch = if let Some(mask) = mask {
            filter_record_batch(&record_batch, &mask.into())?
        } else {
            record_batch
        };
        row_count += batch.num_rows();
    }

    let stats = dhat::HeapStats::get();
    println!("Heap stats after scan execution:\n{:?}", stats);
    println!("Total rows read: {}", row_count);

    Ok(())
}
