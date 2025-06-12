//! This tests our memory usage for reading tables with large data files.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::parquet::arrow::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;

use serde_json::json;
use tempfile::tempdir;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const NUM_ROWS: u64 = 1_000_000; // need 100M

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
    let path = path.join("_delta_log/00000000000000000000.json");
    let mut file = File::create(&path)?;

    let actions = vec![
        json!({
            "metadata": {
                "id": "00000000000000000000",
                "format": "parquet",
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"i\",\"type\":{\"type\":\"long\"},\"nullable\":true,\"metadata\":{}},{\"name\":\"s\",\"type\":{\"type\":\"string\"},\"nullable\":true,\"metadata\":{}}]}",
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

#[test]
fn test_dhat_large_table_data() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let table_path = dir.path();
    let _profiler = dhat::Profiler::builder().testing().build();
    write_large_parquet_to(table_path)?;

    let stats = dhat::HeapStats::get();
    println!("Heap stats after scan::execute: {:?}", stats);

    Ok(())
}
