//! Shared read helpers for integration tests.

use std::fs::File;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn read_parquet_file(path: &std::path::Path) -> RecordBatch {
    let file = File::open(path).expect("failed to open parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to create parquet reader")
        .build()
        .expect("failed to build parquet reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&batches[0].schema(), &batches).expect("failed to concat batches")
}
