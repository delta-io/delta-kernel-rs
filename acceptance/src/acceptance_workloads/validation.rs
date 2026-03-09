//! Result validation for acceptance workload test cases.

use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, RecordBatch};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::DataType;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::DeltaResult;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use super::types::{ExpectedSummary, ReadExpected, SnapshotExpected};
use super::workload::SnapshotResult;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn print_mismatch(label: &str, expected: &str, actual: &str) {
    eprintln!("\n=== {label} MISMATCH ===");
    eprintln!("expected:\n{expected}");
    eprintln!("actual:\n{actual}");
    eprintln!("=== END MISMATCH ===\n");
}

fn format_batch(batch: &RecordBatch) -> String {
    pretty_format_batches(std::slice::from_ref(batch))
        .map(|d| d.to_string())
        .unwrap_or_else(|_| "Failed to format".to_string())
}

/// Normalize a column for comparison (handle timezone/precision equivalence).
///
/// Spark may write Timestamp(Nanosecond, None) while kernel produces
/// Timestamp(Microsecond, Some("UTC")). Normalize all timestamps to
/// Timestamp(Microsecond, Some("UTC")).
fn normalize_col(col: Arc<dyn Array>) -> Arc<dyn Array> {
    match col.data_type() {
        DataType::Timestamp(_unit, tz) => {
            let target = DataType::Timestamp(
                delta_kernel::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            );
            let needs_cast = match tz {
                None => true,
                Some(z) if **z == *"+00:00" => true,
                Some(z) if **z == *"UTC" => !matches!(
                    col.data_type(),
                    DataType::Timestamp(delta_kernel::arrow::datatypes::TimeUnit::Microsecond, _)
                ),
                _ => false,
            };
            if needs_cast {
                return delta_kernel::arrow::compute::cast(&col, &target)
                    .expect("Could not normalize timestamp column");
            }
            col
        }
        _ => col,
    }
}

fn columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    for (actual, expected) in actual.iter().zip(expected) {
        let actual = normalize_col(actual.clone());
        let expected = normalize_col(expected.clone());
        if actual != expected {
            return false;
        }
    }
    true
}

// ── Expected data loading ───────────────────────────────────────────────────

async fn read_expected_data(expected_dir: &Path) -> DeltaResult<Option<RecordBatch>> {
    let expected_data_dir = expected_dir.join("expected_data");
    if !expected_data_dir.exists() {
        return Ok(None);
    }

    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_data_dir)?);
    let files: Vec<_> = store
        .list(None)
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    let mut batches = vec![];
    let mut schema = None;

    for meta in files {
        let path_str = meta.location.to_string();
        let filename = path_str.rsplit('/').next().unwrap_or(&path_str);
        if filename.starts_with('.') || filename.starts_with('_') {
            continue;
        }
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta.location);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
    }

    if let Some(schema) = schema {
        let all_data = concat_batches(&schema, &batches)?;
        Ok(Some(all_data))
    } else {
        Ok(None)
    }
}

fn read_expected_summary(expected_dir: &Path) -> Option<ExpectedSummary> {
    let path = expected_dir.join("summary.json");
    if !path.exists() {
        return None;
    }
    let file = std::fs::File::open(path).ok()?;
    serde_json::from_reader(file).ok()
}

fn read_expected_protocol(expected_dir: &Path) -> Option<super::types::Protocol> {
    let path = expected_dir.join("protocol.json");
    if !path.exists() {
        return None;
    }
    let file = std::fs::File::open(path).ok()?;
    let wrapper: super::types::ProtocolWrapper = serde_json::from_reader(file).ok()?;
    Some(wrapper.protocol)
}

fn read_expected_metadata(expected_dir: &Path) -> Option<super::types::Metadata> {
    let path = expected_dir.join("metadata.json");
    if !path.exists() {
        return None;
    }
    let file = std::fs::File::open(path).ok()?;
    let wrapper: super::types::MetadataWrapper = serde_json::from_reader(file).ok()?;
    Some(wrapper.meta_data)
}

// ── Validation (panics on mismatch) ─────────────────────────────────────────

/// Validate read results against expected data. Panics on mismatch.
pub async fn validate_read_result(
    actual: RecordBatch,
    expected_dir: &Path,
    inline_expected: Option<&ReadExpected>,
) {
    let actual_row_count = actual.num_rows() as u64;

    let expected = read_expected_data(expected_dir)
        .await
        .expect("Failed to read expected data");

    if let Some(expected) = expected {
        let actual = crate::data::sort_record_batch(actual).expect("Failed to sort actual");
        let expected = crate::data::sort_record_batch(expected).expect("Failed to sort expected");

        if actual.num_rows() != expected.num_rows() {
            print_mismatch(
                "ROW COUNT",
                &format!("{} rows\n{}", expected.num_rows(), format_batch(&expected)),
                &format!("{} rows\n{}", actual.num_rows(), format_batch(&actual)),
            );
            panic!(
                "Row count mismatch: expected {}, got {}",
                expected.num_rows(),
                actual.num_rows()
            );
        }

        if !columns_match(actual.columns(), expected.columns()) {
            print_mismatch(
                "DATA",
                &format!("{:#?}\n{}", expected.schema(), format_batch(&expected)),
                &format!("{:#?}\n{}", actual.schema(), format_batch(&actual)),
            );
            panic!("Data content does not match");
        }
    }

    // Validate row count: inline expected takes priority over summary.json
    let expected_row_count = inline_expected
        .map(|e| Some(e.row_count))
        .unwrap_or_else(|| read_expected_summary(expected_dir).map(|s| s.actual_row_count));

    if let Some(expected) = expected_row_count {
        assert_eq!(
            actual_row_count, expected,
            "Row count mismatch: expected {expected}, got {actual_row_count}"
        );
    }
}

/// Validate snapshot result against expected protocol and metadata. Panics on mismatch.
pub fn validate_snapshot(
    result: &SnapshotResult,
    expected_dir: &Path,
    inline: Option<&SnapshotExpected>,
) {
    // Protocol: inline takes priority over file
    let expected_protocol = inline
        .and_then(|e| e.protocol.clone())
        .or_else(|| read_expected_protocol(expected_dir));
    if let Some(ref expected) = expected_protocol {
        if result.protocol != *expected {
            print_mismatch(
                "PROTOCOL",
                &serde_json::to_string_pretty(expected).unwrap(),
                &serde_json::to_string_pretty(&result.protocol).unwrap(),
            );
            panic!("Protocol does not match");
        }
    }

    // Metadata: inline takes priority over file
    let expected_metadata = inline
        .and_then(|e| e.metadata.clone())
        .or_else(|| read_expected_metadata(expected_dir));
    if let Some(ref expected) = expected_metadata {
        if result.metadata != *expected {
            print_mismatch(
                "METADATA",
                &serde_json::to_string_pretty(expected).unwrap(),
                &serde_json::to_string_pretty(&result.metadata).unwrap(),
            );
            panic!("Metadata does not match");
        }
    }
}
