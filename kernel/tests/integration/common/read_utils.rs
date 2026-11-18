//! Shared read helpers for integration tests.

use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata, Scan};
use delta_kernel::schema::MetadataColumnSpec;
use delta_kernel::{Engine, Result, Snapshot};
use test_utils::read_scan;

/// Reads table data with the requested Row Tracking metadata columns.
pub fn read_row_tracking_scan(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    metadata_columns: impl IntoIterator<Item = MetadataColumnSpec>,
) -> Result<Vec<RecordBatch>> {
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
    Ok(read_scan(&scan, engine)?)
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

/// Collects scan-file paths through sequential or parallel scan metadata.
///
/// Parallel execution assigns one file to each worker. Neither path guarantees result order.
pub fn collect_scan_paths(
    scan: &Scan,
    engine: Arc<dyn Engine>,
    use_parallel: bool,
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error>> {
    fn push_path(paths: &mut Vec<String>, scan_file: ScanFile) {
        paths.push(scan_file.path);
    }

    let mut paths: Vec<String> = Vec::new();
    if use_parallel {
        let mut sequential = scan.parallel_scan_metadata(engine.clone())?;
        for sm in sequential.by_ref() {
            paths = sm?.visit_scan_files(paths, push_path)?;
        }
        if let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()? {
            let state = Arc::from(state);
            for file in files {
                let parallel =
                    ParallelScanMetadata::try_new(engine.clone(), Arc::clone(&state), vec![file])?;
                for sm in parallel {
                    paths = sm?.visit_scan_files(paths, push_path)?;
                }
            }
            state.log_metrics();
        }
    } else {
        for sm in scan.scan_metadata(engine.as_ref())? {
            paths = sm?.visit_scan_files(paths, push_path)?;
        }
    }
    Ok(paths)
}
