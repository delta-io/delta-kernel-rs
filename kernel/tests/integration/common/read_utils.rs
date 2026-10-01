//! Shared read helpers for integration tests.

use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::MetadataColumnSpec;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use test_utils::read_scan;

/// Reads table data with the requested Row Tracking metadata columns.
pub fn read_row_tracking_scan(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    metadata_columns: impl IntoIterator<Item = MetadataColumnSpec>,
) -> DeltaResult<Vec<RecordBatch>> {
    let scan_schema = metadata_columns.into_iter().try_fold(
        snapshot.schema().as_ref().clone(),
        |schema, metadata_column| {
            schema.add_metadata_column(metadata_column.text_value(), metadata_column)
        },
    )?;
    let scan = snapshot
        .scan_builder()
        .with_schema(Arc::new(scan_schema))
        .build()?;
    read_scan(&scan, engine)
}

pub fn read_parquet_file(path: &std::path::Path) -> RecordBatch {
    let file = File::open(path).expect("failed to open parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to create parquet reader")
        .build()
        .expect("failed to build parquet reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&batches[0].schema(), &batches).expect("failed to concat batches")
}
