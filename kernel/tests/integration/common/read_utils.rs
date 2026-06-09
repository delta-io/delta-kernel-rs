//! Shared read helpers for integration tests.
//!
//! Each integration test file compiles as its own test binary and uses only a subset of these
//! helpers; the blanket `#![allow(dead_code)]` suppresses the per-binary dead-code warnings that
//! the unused subset would otherwise generate.

#![allow(dead_code)]

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn read_parquet_file(path: &std::path::Path) -> RecordBatch {
    let file = std::fs::File::open(path).expect("failed to open parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to create parquet reader")
        .build()
        .expect("failed to build parquet reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&batches[0].schema(), &batches).expect("failed to concat batches")
}
