//! Result validation for acceptance workload test cases.

use std::fs::{self, File};
use std::path::Path;

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

    let parquet_paths: Vec<_> = fs::read_dir(&expected_data_dir)
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
        .collect();

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

// ── Validation ───────────────────────────────────────────────────────────────

/// Validate read results against expected outcome.
pub fn validate_read_result(
    result: DeltaResult<ReadResult>,
    expected_dir: &Path,
    expected: &ReadExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(read_result), ReadExpected::Success { expected }) => {
            let expected_data = read_expected_data(expected_dir)?;
            assert_data_matches(
                read_result.batches,
                &read_result.schema,
                expected_data,
                Some(expected.row_count),
            )
            .map_err(|e| e.to_string())
        }
        (Err(e), ReadExpected::Error { error }) => {
            println!("  Got expected error '{}': {}", error.error_code, e);
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
            if snapshot_result.protocol != expected.protocol {
                return Err(format!(
                    "Expected protocol to match:\n{:?}\n{:?}",
                    snapshot_result.protocol, expected.protocol
                ));
            }
            if snapshot_result.metadata != expected.metadata {
                return Err(format!(
                    "Expected metadata to match:\n{:?}\n{:?}",
                    snapshot_result.metadata, expected.metadata
                ));
            }
            Ok(())
        }
        (Err(e), SnapshotExpected::Error { error }) => {
            println!("  Got expected error '{}': {}", error.error_code, e);
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
