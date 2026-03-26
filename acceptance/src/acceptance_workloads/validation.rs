//! Result validation for acceptance workload test cases.
//!
//! Compares actual kernel results against expected outcomes from the spec. For read workloads,
//! expected data is loaded from Parquet files in `expected_data/` and compared order-independently.
//! For snapshot workloads, protocol and metadata are compared directly.

use std::fs::{self, File};
use std::path::Path;

use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use itertools::Itertools;
use tracing::debug;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::DeltaResult;
use delta_kernel_benchmarks::models::{ReadExpected, SnapshotExpected};

use crate::data::assert_data_matches;

use super::workload::{ReadResult, SnapshotResult};

/// Read expected data from parquet files in expected_dir/expected_data/.
fn read_expected_data(expected_dir: &Path) -> Result<RecordBatch, String> {
    let expected_data_dir = expected_dir.join("expected_data");
    if !expected_data_dir.exists() {
        return Err(format!(
            "Expected data directory not found: {}",
            expected_data_dir.display()
        ));
    }

    let parquet_paths = fs::read_dir(&expected_data_dir)
        .map_err(|e| format!("Failed to read expected_data dir: {e}"))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let filename = path.file_name()?.to_str()?;

            if filename.starts_with('.') || filename.starts_with('_') {
                return None;
            }

            if path.extension()?.to_str()? == "parquet" {
                Some(path)
            } else {
                None
            }
        })
        .collect_vec();

    let mut batches = vec![];
    let mut schema = None;

    for path in parquet_paths {
        let file = File::open(&path)
            .map_err(|e| format!("Failed to open parquet file {}: {e}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("Failed to create parquet reader: {e}"))?;

        if schema.is_none() {
            schema = Some(builder.schema().clone());
        }

        let reader = builder
            .build()
            .map_err(|e| format!("Failed to build parquet reader: {e}"))?;

        for batch in reader {
            let batch = batch.map_err(|e| format!("Failed to read batch: {e}"))?;
            batches.push(batch);
        }
    }

    let schema = schema
        .ok_or_else(|| format!("No parquet files found in {}", expected_data_dir.display()))?;
    let all_data =
        concat_batches(&schema, &batches).map_err(|e| format!("Failed to concat batches: {e}"))?;
    Ok(all_data)
}

/// Validate read results against expected outcome.
pub fn validate_read_result(
    result: DeltaResult<ReadResult>,
    expected_dir: &Path,
    expected: &ReadExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(read_result), ReadExpected::Success { expected: exp }) => {
            // Log file count mismatch (not a failure since data skipping implementations may be
            // different)
            if let Some(expected_file_count) = exp.file_count {
                if read_result.file_count != expected_file_count {
                    debug!(
                        "File count mismatch: expected {}, got {}. Note: different data skipping implementations may lead to mismatches",
                        expected_file_count, read_result.file_count
                    );
                }
            }

            // Validate data content
            let expected_data = read_expected_data(expected_dir)?;

            let schema = ArrowSchema::try_from_kernel(read_result.schema.as_ref())
                .map_err(|e| e.to_string())?;
            let schema = std::sync::Arc::new(schema);
            assert_data_matches(read_result.batches, &schema, expected_data)
                .map_err(|e| e.to_string())?;

            // Validate row count against spec's expected row counts
            if read_result.row_count != exp.row_count {
                return Err(format!(
                    "Row count mismatch: expected {}, got {}",
                    exp.row_count, read_result.row_count
                ));
            }

            Ok(())
        }
        (Err(kernel_err), ReadExpected::Error { error }) => {
            debug!(
                "Got expected error '{}' with message: {:?}\nKernel error: {}",
                error.error_code, error.error_message, kernel_err
            );
            Ok(())
        }
        (Ok(_), ReadExpected::Error { error }) => Err(format!(
            "Expected error '{}' but succeeded",
            error.error_code
        )),
        (Err(e), ReadExpected::Success { .. }) => {
            Err(format!("Expected success but got error: {}", e))
        }
    }
}

/// Validate snapshot result against expected outcome.
pub fn validate_snapshot(
    result: DeltaResult<SnapshotResult>,
    expected: &SnapshotExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(snapshot_result), SnapshotExpected::Success { expected }) => {
            if snapshot_result.protocol != *expected.protocol {
                return Err(format!(
                    "Expected protocol to match:\n{:?}\n{:?}",
                    snapshot_result.protocol, expected.protocol
                ));
            }
            if snapshot_result.metadata != *expected.metadata {
                return Err(format!(
                    "Expected metadata to match:\n{:?}\n{:?}",
                    snapshot_result.metadata, expected.metadata
                ));
            }
            Ok(())
        }
        (Err(kernel_err), SnapshotExpected::Error { error }) => {
            debug!(
                "Got expected error '{}' with message: {:?}\nKernel error: {}",
                error.error_code, error.error_message, kernel_err
            );
            Ok(())
        }
        (Ok(_), SnapshotExpected::Error { error }) => Err(format!(
            "Expected error '{}' but succeeded",
            error.error_code
        )),
        (Err(e), SnapshotExpected::Success { .. }) => {
            Err(format!("Expected success but got error: {}", e))
        }
    }
}
