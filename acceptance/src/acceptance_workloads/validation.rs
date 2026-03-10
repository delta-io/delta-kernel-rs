//! Result validation for acceptance workload test cases.

use std::fs::{self, File};
use std::path::Path;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel_benchmarks::models::{ReadExpected, SnapshotExpected};

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

/// Read expected data from parquet files in expected_dir/expected_data/.
fn read_expected_data(expected_dir: &Path) -> Result<Option<RecordBatch>, String> {
    let expected_data_dir = expected_dir.join("expected_data");
    if !expected_data_dir.exists() {
        return Ok(None);
    }

    let entries = fs::read_dir(&expected_data_dir)
        .map_err(|e| format!("Failed to read expected_data dir: {e}"))?;

    let mut batches = vec![];
    let mut schema = None;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read dir entry: {e}"))?;
        let path = entry.path();
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        if filename.starts_with('.') || filename.starts_with('_') {
            continue;
        }

        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
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
    }

    if let Some(schema) = schema {
        let all_data = concat_batches(&schema, &batches)
            .map_err(|e| format!("Failed to concat batches: {e}"))?;
        Ok(Some(all_data))
    } else {
        Ok(None)
    }
}

// ── Validation ───────────────────────────────────────────────────────────────

/// Validate read results against expected data.
pub fn validate_read_result(
    actual: RecordBatch,
    expected_dir: &Path,
    expected: Option<&ReadExpected>,
) -> Result<(), String> {
    let actual_row_count = actual.num_rows() as u64;

    let expected_data = read_expected_data(expected_dir)?;

    if let Some(expected_data) = expected_data {
        // Sort both batches for order-independent comparison. If sort fails (e.g., struct-only
        // schemas that Arrow can't sort), fall back to comparing unsorted.
        let actual = crate::data::sort_record_batch(actual.clone()).unwrap_or(actual);
        let expected_data =
            crate::data::sort_record_batch(expected_data.clone()).unwrap_or(expected_data);

        if actual.num_rows() != expected_data.num_rows() {
            print_mismatch(
                "ROW COUNT",
                &format!(
                    "{} rows\n{}",
                    expected_data.num_rows(),
                    format_batch(&expected_data)
                ),
                &format!("{} rows\n{}", actual.num_rows(), format_batch(&actual)),
            );
            return Err(format!(
                "Row count mismatch: expected {}, got {}",
                expected_data.num_rows(),
                actual.num_rows()
            ));
        }

        if let Err(diff) = columns_match(&actual, &expected_data) {
            print_mismatch(
                "DATA",
                &format_batch(&expected_data),
                &format_batch(&actual),
            );
            return Err(format!("Data content does not match:\n{diff}"));
        }
    }

    // Validate row count from expected
    if let Some(expected) = expected {
        if actual_row_count != expected.row_count {
            return Err(format!(
                "Row count mismatch: expected {}, got {actual_row_count}",
                expected.row_count
            ));
        }
    }

    Ok(())
}

/// Validate snapshot result against expected protocol and metadata.
pub fn validate_snapshot(
    result: &SnapshotResult,
    expected: &SnapshotExpected,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if result.protocol != expected.protocol {
        print_mismatch(
            "PROTOCOL",
            &format!("{:?}", expected.protocol),
            &format!("{:?}", result.protocol),
        );
        errors.push("Protocol mismatch".to_string());
    }

    if result.metadata != expected.metadata {
        print_mismatch(
            "METADATA",
            &format!("{:?}", expected.metadata),
            &format!("{:?}", result.metadata),
        );
        errors.push("Metadata mismatch".to_string());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
