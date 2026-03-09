//! Result validation for improved_dat test cases.

use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, RecordBatch};
use delta_kernel::arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use delta_kernel::arrow::datatypes::DataType;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::DeltaResult;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use super::types::{ExpectedSummary, Metadata, Protocol, ReadExpected, SnapshotExpected};
use super::workload::SnapshotResult;

/// Validation error
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Row count mismatch: expected {expected}, got {actual}")]
    RowCountMismatch { expected: u64, actual: u64 },
    #[error("Column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("Data mismatch at column {column}: {message}")]
    DataMismatch { column: String, message: String },
    #[error("Protocol mismatch: {message}")]
    ProtocolMismatch { message: String },
    #[error("Metadata mismatch: {message}")]
    MetadataMismatch { message: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Kernel error: {0}")]
    Kernel(#[from] delta_kernel::Error),
}

/// Sort a record batch lexicographically by all sortable columns.
/// Returns (sorted_batch, was_sorted) where was_sorted indicates if any sorting was possible.
pub fn sort_record_batch(batch: RecordBatch) -> DeltaResult<(RecordBatch, bool)> {
    let mut sort_columns = vec![];
    for col in batch.columns() {
        match col.data_type() {
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                // can't sort structs, lists, or maps
            }
            _ => sort_columns.push(SortColumn {
                values: col.clone(),
                options: None,
            }),
        }
    }

    if sort_columns.is_empty() {
        return Ok((batch, false));
    }

    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    Ok((RecordBatch::try_new(batch.schema(), columns)?, true))
}

/// Normalize all struct columns in a record batch to have fields in alphabetical order.
fn normalize_batch_struct_order(batch: &RecordBatch) -> RecordBatch {
    use delta_kernel::arrow::datatypes::Field;

    let new_columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .map(|col| normalize_struct_field_order(col.clone()))
        .collect();
    let new_fields: Vec<Arc<Field>> = batch
        .schema()
        .fields()
        .iter()
        .zip(new_columns.iter())
        .map(|(f, col)| {
            Arc::new(Field::new(
                f.name(),
                col.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect();
    let new_schema = Arc::new(delta_kernel::arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).unwrap()
}

/// Compare two record batches as multisets (order-independent) using string representations.
/// Used when the batch contains only struct/list/map columns that can't be sorted.
fn multiset_match(actual: &RecordBatch, expected: &RecordBatch) -> bool {
    use std::collections::HashMap;

    // Normalize struct field order before comparing string representations
    let actual_normalized = normalize_batch_struct_order(actual);
    let expected_normalized = normalize_batch_struct_order(expected);

    let actual_str = pretty_format_batches(&[actual_normalized])
        .map(|d| d.to_string())
        .unwrap_or_default();
    let expected_str = pretty_format_batches(&[expected_normalized])
        .map(|d| d.to_string())
        .unwrap_or_default();

    // Parse rows from formatted output (skip header lines)
    fn row_multiset(formatted: &str) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for line in formatted.lines() {
            let trimmed = line.trim();
            // Skip header/separator lines (start with +, or contain column names)
            if trimmed.starts_with('+') || trimmed.is_empty() {
                continue;
            }
            // Data rows start with '|'
            if trimmed.starts_with('|') {
                *counts.entry(trimmed.to_string()).or_insert(0) += 1;
            }
        }
        counts
    }

    row_multiset(&actual_str) == row_multiset(&expected_str)
}

/// Normalize a column for comparison (handle timezone/precision equivalence)
///
/// Handles differences between how Spark writes expected parquet and how
/// kernel reads delta tables:
/// - Spark may write Timestamp(Nanosecond, None) while kernel produces
///   Timestamp(Microsecond, Some("UTC"))
/// - Normalize all timestamps to Timestamp(Microsecond, Some("UTC"))
fn normalize_col(col: Arc<dyn Array>) -> Arc<dyn Array> {
    match col.data_type() {
        DataType::Timestamp(_unit, tz) => {
            // Normalize timezone: None and "+00:00" both become "UTC"
            let target = DataType::Timestamp(
                delta_kernel::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            );
            let needs_cast = match tz {
                None => true,
                Some(z) if **z == *"+00:00" => true,
                Some(z) if **z == *"UTC" => {
                    // Already UTC, but may need precision change
                    !matches!(
                        col.data_type(),
                        DataType::Timestamp(
                            delta_kernel::arrow::datatypes::TimeUnit::Microsecond,
                            _
                        )
                    )
                }
                _ => false, // Other timezones, leave as-is
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

/// Strip field metadata from a data type (recursive for structs)
fn strip_metadata_from_datatype(dt: &DataType) -> DataType {
    use delta_kernel::arrow::datatypes::Field;

    match dt {
        DataType::Struct(fields) => {
            let new_fields: Vec<_> = fields
                .iter()
                .map(|f| {
                    let new_dt = strip_metadata_from_datatype(f.data_type());
                    Arc::new(Field::new(f.name(), new_dt, f.is_nullable()))
                })
                .collect();
            DataType::Struct(new_fields.into())
        }
        DataType::List(field) => {
            let new_dt = strip_metadata_from_datatype(field.data_type());
            DataType::List(Arc::new(Field::new(
                field.name(),
                new_dt,
                field.is_nullable(),
            )))
        }
        DataType::Map(field, sorted) => {
            let new_dt = strip_metadata_from_datatype(field.data_type());
            DataType::Map(
                Arc::new(Field::new(field.name(), new_dt, field.is_nullable())),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

/// Cast an array to strip field metadata (for struct comparisons)
fn strip_metadata_from_array(arr: &Arc<dyn Array>) -> Arc<dyn Array> {
    let target_type = strip_metadata_from_datatype(arr.data_type());
    if &target_type == arr.data_type() {
        arr.clone()
    } else {
        delta_kernel::arrow::compute::cast(arr, &target_type)
            .expect("Failed to cast array to strip metadata")
    }
}

/// Reorder struct fields in an array to match a canonical (sorted by name) order.
/// This handles cases where kernel and Spark produce struct fields in different orders
/// (e.g., variant columns: kernel produces {metadata, value}, Spark produces {value, metadata}).
/// Also recursively normalizes nested structs and updates field type declarations to match.
fn normalize_struct_field_order(arr: Arc<dyn Array>) -> Arc<dyn Array> {
    use delta_kernel::arrow::array::{GenericListArray, MapArray, StructArray};
    use delta_kernel::arrow::datatypes::Field;

    match arr.data_type() {
        DataType::Struct(fields) => {
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();

            // Build (name, index) pairs and sort by name
            let mut indexed_fields: Vec<(usize, &Arc<Field>)> = fields.iter().enumerate().collect();
            indexed_fields.sort_by(|a, b| a.1.name().cmp(b.1.name()));

            // Reorder columns and fields, recursing into children
            let new_columns: Vec<Arc<dyn Array>> = indexed_fields
                .iter()
                .map(|(orig_idx, _)| {
                    normalize_struct_field_order(struct_arr.column(*orig_idx).clone())
                })
                .collect();
            // Rebuild field declarations from the normalized child columns' actual data types
            let new_fields: Vec<Arc<Field>> = indexed_fields
                .iter()
                .zip(new_columns.iter())
                .map(|((_, orig_field), col)| {
                    Arc::new(Field::new(
                        orig_field.name(),
                        col.data_type().clone(),
                        orig_field.is_nullable(),
                    ))
                })
                .collect();

            Arc::new(
                StructArray::try_new(new_fields.into(), new_columns, struct_arr.nulls().cloned())
                    .unwrap(),
            )
        }
        DataType::List(field) => {
            let list_arr = arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            let normalized_values = normalize_struct_field_order(list_arr.values().clone());
            let new_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            Arc::new(
                GenericListArray::try_new(
                    new_field,
                    list_arr.offsets().clone(),
                    normalized_values,
                    list_arr.nulls().cloned(),
                )
                .unwrap(),
            )
        }
        DataType::LargeList(field) => {
            let list_arr = arr
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();
            let normalized_values = normalize_struct_field_order(list_arr.values().clone());
            let new_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            Arc::new(
                GenericListArray::try_new(
                    new_field,
                    list_arr.offsets().clone(),
                    normalized_values,
                    list_arr.nulls().cloned(),
                )
                .unwrap(),
            )
        }
        DataType::Map(field, sorted) => {
            let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
            // Map entries are stored as a struct with "key" and "value" fields
            let entries: Arc<dyn Array> = Arc::new(map_arr.entries().clone());
            let normalized_entries = normalize_struct_field_order(entries);
            let new_field = Arc::new(Field::new(
                field.name(),
                normalized_entries.data_type().clone(),
                field.is_nullable(),
            ));
            Arc::new(
                MapArray::try_new(
                    new_field,
                    map_arr.offsets().clone(),
                    normalized_entries
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap()
                        .clone(),
                    map_arr.nulls().cloned(),
                    *sorted,
                )
                .unwrap(),
            )
        }
        _ => arr,
    }
}

/// Compare two sets of columns for equality (ignoring field metadata and struct field order)
pub fn columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    for (actual, expected) in actual.iter().zip(expected) {
        let actual =
            normalize_struct_field_order(strip_metadata_from_array(&normalize_col(actual.clone())));
        let expected = normalize_struct_field_order(strip_metadata_from_array(&normalize_col(
            expected.clone(),
        )));
        if actual != expected {
            return false;
        }
    }
    true
}

/// Assert that two sets of columns match
pub fn assert_columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Column count mismatch: actual={}, expected={}",
        actual.len(),
        expected.len()
    );
    for (i, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let actual = normalize_col(actual.clone());
        let expected = normalize_col(expected.clone());
        assert_eq!(
            &actual, &expected,
            "Column {} data mismatch. Got {:?}, expected {:?}",
            i, actual, expected
        );
    }
}

/// Read expected data from parquet files in the given directory
pub async fn read_expected_data(expected_dir: &Path) -> DeltaResult<Option<RecordBatch>> {
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
        // Skip hidden files (Spark .crc checksums) and metadata files (_SUCCESS, _committed, etc.)
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

/// Read and parse the summary.json file
pub fn read_expected_summary(
    expected_dir: &Path,
) -> Result<Option<ExpectedSummary>, ValidationError> {
    let summary_path = expected_dir.join("summary.json");
    if !summary_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(summary_path)?;
    let summary: ExpectedSummary = serde_json::from_reader(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(summary))
}

/// Read and parse the protocol.json file
pub fn read_expected_protocol(
    expected_dir: &Path,
) -> Result<Option<Protocol>, ValidationError> {
    let protocol_path = expected_dir.join("protocol.json");
    if !protocol_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(protocol_path)?;
    let wrapper: super::types::ProtocolWrapper = serde_json::from_reader(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(wrapper.protocol))
}

/// Read and parse the metadata.json file
pub fn read_expected_metadata(
    expected_dir: &Path,
) -> Result<Option<Metadata>, ValidationError> {
    let metadata_path = expected_dir.join("metadata.json");
    if !metadata_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(metadata_path)?;
    let wrapper: super::types::MetadataWrapper = serde_json::from_reader(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(wrapper.meta_data))
}

/// Validate read results against expected data
pub async fn validate_read_result(
    actual: RecordBatch,
    expected_dir: &Path,
    _has_predicate: bool,
    inline_expected: Option<&ReadExpected>,
) -> Result<(), ValidationError> {
    // Get actual row count before we potentially move the batch
    let actual_row_count = actual.num_rows() as u64;

    // Read expected data
    let expected = read_expected_data(expected_dir).await?;

    if let Some(expected) = expected {
        // Sort both for comparison
        let (actual, _actual_sorted) = sort_record_batch(actual)?;
        let (expected, _) = sort_record_batch(expected)?;

        // Check row count
        if actual.num_rows() != expected.num_rows() {
            eprintln!("\n=== DATA MISMATCH ===");
            eprintln!(
                "Expected {} rows, got {} rows",
                expected.num_rows(),
                actual.num_rows()
            );
            eprintln!("\n--- Expected Data ---");
            eprintln!(
                "{}",
                pretty_format_batches(std::slice::from_ref(&expected))
                    .map(|d| d.to_string())
                    .unwrap_or_else(|_| "Failed to format".to_string())
            );
            eprintln!("\n--- Actual Data ---");
            eprintln!(
                "{}",
                pretty_format_batches(std::slice::from_ref(&actual))
                    .map(|d| d.to_string())
                    .unwrap_or_else(|_| "Failed to format".to_string())
            );
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::RowCountMismatch {
                expected: expected.num_rows() as u64,
                actual: actual.num_rows() as u64,
            });
        }

        // Check columns match (try element-by-element first, fall back to multiset)
        // Multiset fallback is needed even for sorted data because tied sort keys
        // produce non-deterministic row ordering within equal groups.
        let data_matches = columns_match(actual.columns(), expected.columns())
            || multiset_match(&actual, &expected);

        if !data_matches {
            eprintln!("\n=== DATA MISMATCH ===");
            eprintln!("Column data does not match");
            eprintln!("\n--- Expected Schema ---");
            eprintln!("{:#?}", expected.schema());
            eprintln!("\n--- Actual Schema ---");
            eprintln!("{:#?}", actual.schema());
            eprintln!("\n--- Expected Data ---");
            eprintln!(
                "{}",
                pretty_format_batches(std::slice::from_ref(&expected))
                    .map(|d| d.to_string())
                    .unwrap_or_else(|_| "Failed to format".to_string())
            );
            eprintln!("\n--- Actual Data ---");
            eprintln!(
                "{}",
                pretty_format_batches(std::slice::from_ref(&actual))
                    .map(|d| d.to_string())
                    .unwrap_or_else(|_| "Failed to format".to_string())
            );
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::DataMismatch {
                column: "unknown".to_string(),
                message: "Data content does not match".to_string(),
            });
        }
    }

    // Validate row count: inline expected takes priority over summary.json
    let expected_row_count = inline_expected
        .map(|e| Some(e.row_count))
        .unwrap_or_else(|| {
            read_expected_summary(expected_dir)
                .ok()
                .flatten()
                .map(|s| s.actual_row_count)
        });

    if let Some(expected) = expected_row_count {
        if actual_row_count != expected {
            return Err(ValidationError::RowCountMismatch {
                expected,
                actual: actual_row_count,
            });
        }
    }

    Ok(())
}

/// Validate snapshot result against expected protocol and metadata.
///
/// Loads expected values from `protocol.json` and `metadata.json` in the expected dir,
/// or uses inline expected values from the spec. Comparison uses `PartialEq` on the
/// shared `Protocol`/`Metadata` types. On mismatch, prints both as JSON.
pub fn validate_snapshot(
    result: &SnapshotResult,
    expected_dir: &Path,
    inline: Option<&SnapshotExpected>,
) -> Result<(), ValidationError> {
    // Protocol: inline takes priority over file
    let expected_protocol = inline
        .and_then(|e| e.protocol.clone())
        .or_else(|| read_expected_protocol(expected_dir).ok().flatten());
    if let Some(ref expected) = expected_protocol {
        if result.protocol != *expected {
            eprintln!("\n=== PROTOCOL MISMATCH ===");
            eprintln!(
                "expected: {}",
                serde_json::to_string_pretty(expected).unwrap()
            );
            eprintln!(
                "actual:   {}",
                serde_json::to_string_pretty(&result.protocol).unwrap()
            );
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::ProtocolMismatch {
                message: "protocol does not match".to_string(),
            });
        }
    }

    // Metadata: inline takes priority over file
    let expected_metadata = inline
        .and_then(|e| e.metadata.clone())
        .or_else(|| read_expected_metadata(expected_dir).ok().flatten());
    if let Some(ref expected) = expected_metadata {
        if result.metadata != *expected {
            eprintln!("\n=== METADATA MISMATCH ===");
            eprintln!(
                "expected: {}",
                serde_json::to_string_pretty(expected).unwrap()
            );
            eprintln!(
                "actual:   {}",
                serde_json::to_string_pretty(&result.metadata).unwrap()
            );
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::MetadataMismatch {
                message: "metadata does not match".to_string(),
            });
        }
    }

    Ok(())
}
