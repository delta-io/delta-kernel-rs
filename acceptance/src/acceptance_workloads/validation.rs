//! Result validation for acceptance workload test cases.

use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
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

/// Compare two record batches column-by-column.
/// Returns Ok(()) if they match, or Err with a detailed per-column diff message.
fn columns_match(actual: &RecordBatch, expected: &RecordBatch) -> Result<(), String> {
    if actual.num_columns() != expected.num_columns() {
        return Err(format!(
            "Column count mismatch: expected {} columns ({:?}), got {} columns ({:?})",
            expected.num_columns(),
            expected
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
            actual.num_columns(),
            actual
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
        ));
    }
    let mut mismatches = Vec::new();
    let act_schema = actual.schema();
    let exp_schema = expected.schema();
    for i in 0..actual.num_columns() {
        let act = actual.column(i);
        let exp = expected.column(i);
        if act.as_ref() != exp.as_ref() {
            let act_name = act_schema.field(i).name();
            let exp_name = exp_schema.field(i).name();
            let act_type = act_schema.field(i).data_type();
            let exp_type = exp_schema.field(i).data_type();
            // Show first few differing rows
            let mut diffs = Vec::new();
            let n = act.len().min(exp.len()).min(10);
            for row in 0..n {
                let a_slice = act.slice(row, 1);
                let e_slice = exp.slice(row, 1);
                let a = format!("{:?}", a_slice);
                let e = format!("{:?}", e_slice);
                if a != e {
                    diffs.push(format!("  row {row}: expected {e}, got {a}"));
                }
            }
            let name_info = if act_name != exp_name {
                format!(" (name: expected '{}', got '{}')", exp_name, act_name)
            } else {
                String::new()
            };
            let type_info = if act_type != exp_type {
                format!(" (type: expected {:?}, got {:?})", exp_type, act_type)
            } else {
                String::new()
            };
            mismatches.push(format!(
                "column[{i}] '{}'{}{}: {} differing rows{}",
                act_name,
                name_info,
                type_info,
                diffs.len(),
                if diffs.is_empty() {
                    String::new()
                } else {
                    format!("\n{}", diffs.join("\n"))
                }
            ));
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(mismatches.join("\n"))
    }
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

// ── Validation ───────────────────────────────────────────────────────────────

/// Validate read results against expected data. Returns Err with details on mismatch.
pub async fn validate_read_result(
    actual: RecordBatch,
    expected_dir: &Path,
    inline_expected: Option<&ReadExpected>,
) -> Result<(), String> {
    let actual_row_count = actual.num_rows() as u64;

    let expected = read_expected_data(expected_dir)
        .await
        .map_err(|e| format!("Failed to read expected data: {e}"))?;

    if let Some(expected) = expected {
        // Sort both batches for order-independent comparison. If sort fails (e.g., struct-only
        // schemas that Arrow can't sort), fall back to comparing unsorted.
        let actual = crate::data::sort_record_batch(actual.clone()).unwrap_or(actual);
        let expected = crate::data::sort_record_batch(expected.clone()).unwrap_or(expected);

        if actual.num_rows() != expected.num_rows() {
            print_mismatch(
                "ROW COUNT",
                &format!("{} rows\n{}", expected.num_rows(), format_batch(&expected)),
                &format!("{} rows\n{}", actual.num_rows(), format_batch(&actual)),
            );
            return Err(format!(
                "Row count mismatch: expected {}, got {}",
                expected.num_rows(),
                actual.num_rows()
            ));
        }

        if let Err(diff) = columns_match(&actual, &expected) {
            print_mismatch("DATA", &format_batch(&expected), &format_batch(&actual));
            return Err(format!("Data content does not match:\n{diff}"));
        }
    }

    // Validate row count: inline expected takes priority over summary.json
    let expected_row_count = inline_expected
        .map(|e| Some(e.row_count))
        .unwrap_or_else(|| read_expected_summary(expected_dir).map(|s| s.actual_row_count));

    if let Some(expected) = expected_row_count {
        if actual_row_count != expected {
            return Err(format!(
                "Row count mismatch: expected {expected}, got {actual_row_count}"
            ));
        }
    }

    Ok(())
}

/// Validate snapshot result against expected protocol and metadata. Returns Err on mismatch.
pub fn validate_snapshot(
    result: &SnapshotResult,
    expected_dir: &Path,
    inline: Option<&SnapshotExpected>,
) -> Result<(), String> {
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
            return Err("Protocol does not match".to_string());
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
            return Err("Metadata does not match".to_string());
        }
    }

    Ok(())
}
