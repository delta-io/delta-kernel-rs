//! Statistics extraction from Parquet file metadata.
//!
//! This module provides functionality to extract Delta Lake statistics (numRecords, nullCount,
//! minValues, maxValues) from Parquet file footer metadata after writing.
//!
//! Supports nested struct fields with recursive stat collection.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use crate::arrow::datatypes::{DataType, Field};
use crate::parquet::file::metadata::ParquetMetaData;
use crate::parquet::file::statistics::Statistics;
use crate::parquet::schema::types::ColumnDescriptor;
use crate::schema::{DataType as KernelDataType, PrimitiveType, SchemaRef, StructField};
use crate::{DeltaResult, Error};

/// Maximum length for string statistics (Delta protocol requirement).
const STRING_PREFIX_LENGTH: usize = 32;

/// Collected statistics for a single column.
struct ColumnStats {
    null_count: i64,
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
}

/// Collected statistics for a set of fields (returned by recursive field traversal).
struct FieldStatsArrays {
    null_counts: Option<ArrayRef>,
    min_values: Option<ArrayRef>,
    max_values: Option<ArrayRef>,
}

/// Collects statistics from Parquet file metadata for the specified columns.
///
/// Extracts numRecords, nullCount, minValues, and maxValues from the Parquet footer,
/// aggregating across all row groups. Supports nested struct fields.
pub(crate) fn collect_stats_from_parquet(
    parquet_metadata: &ParquetMetaData,
    stats_columns: &[String],
    schema: &SchemaRef,
) -> DeltaResult<StructArray> {
    let stats_set: HashSet<String> = stats_columns.iter().cloned().collect();

    // Build column path to parquet column index mapping (e.g., "outer.inner" -> idx)
    let column_indices = build_column_indices(parquet_metadata);

    // Aggregate statistics across all row groups
    let num_records: i64 = parquet_metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum();

    // Collect all stats (nullCount, min, max) in a single pass through row groups
    let field_stats = collect_all_field_stats(
        parquet_metadata,
        &column_indices,
        schema.fields().collect::<Vec<_>>().as_slice(),
        &stats_set,
        "",
    )?;

    // Build final stats struct
    let mut fields = vec![Field::new("numRecords", DataType::Int64, true)];
    let mut arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![num_records]))];

    if let Some(arr) = field_stats.null_counts {
        fields.push(Field::new("nullCount", arr.data_type().clone(), true));
        arrays.push(arr);
    }
    if let Some(arr) = field_stats.min_values {
        fields.push(Field::new("minValues", arr.data_type().clone(), true));
        arrays.push(arr);
    }
    if let Some(arr) = field_stats.max_values {
        fields.push(Field::new("maxValues", arr.data_type().clone(), true));
        arrays.push(arr);
    }

    fields.push(Field::new("tightBounds", DataType::Boolean, true));
    arrays.push(Arc::new(BooleanArray::from(vec![true])));

    StructArray::try_new(fields.into(), arrays, None)
        .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
}

/// Builds a mapping from column path to parquet column index.
/// Parquet stores nested columns with dotted paths like "outer.inner".
fn build_column_indices(metadata: &ParquetMetaData) -> HashMap<String, usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            // Build the full path from column descriptor
            let path = col.path().parts().join(".");
            (path, idx)
        })
        .collect()
}

/// Collects all statistics (nullCount, min, max) for fields in a single pass.
fn collect_all_field_stats(
    metadata: &ParquetMetaData,
    column_indices: &HashMap<String, usize>,
    fields: &[&StructField],
    stats_columns: &HashSet<String>,
    path_prefix: &str,
) -> DeltaResult<FieldStatsArrays> {
    let mut null_fields: Vec<Field> = Vec::new();
    let mut null_arrays: Vec<ArrayRef> = Vec::new();
    let mut min_fields: Vec<Field> = Vec::new();
    let mut min_arrays: Vec<ArrayRef> = Vec::new();
    let mut max_fields: Vec<Field> = Vec::new();
    let mut max_arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        let field_name = field.name();

        // Build the full path for this field
        let field_path = if path_prefix.is_empty() {
            field_name.to_string()
        } else {
            format!("{}.{}", path_prefix, field_name)
        };

        // Check if this field or any nested field is in stats_columns
        let should_include = stats_columns
            .iter()
            .any(|col| col == &field_path || col.starts_with(&format!("{}.", field_path)));

        if !should_include {
            continue;
        }

        match field.data_type() {
            KernelDataType::Struct(struct_type) => {
                // Recurse into nested struct
                let nested_fields: Vec<&StructField> = struct_type.fields().collect();
                let nested_stats = collect_all_field_stats(
                    metadata,
                    column_indices,
                    &nested_fields,
                    stats_columns,
                    &field_path,
                )?;

                if let Some(arr) = nested_stats.null_counts {
                    null_fields.push(Field::new(field_name, arr.data_type().clone(), true));
                    null_arrays.push(arr);
                }
                if let Some(arr) = nested_stats.min_values {
                    min_fields.push(Field::new(field_name, arr.data_type().clone(), true));
                    min_arrays.push(arr);
                }
                if let Some(arr) = nested_stats.max_values {
                    max_fields.push(Field::new(field_name, arr.data_type().clone(), true));
                    max_arrays.push(arr);
                }
            }
            _ => {
                // Primitive field - collect all stats in one pass
                if let Some(&col_idx) = column_indices.get(&field_path) {
                    let stats = collect_column_stats(metadata, col_idx, field)?;

                    // Add null count
                    null_fields.push(Field::new(field_name, DataType::Int64, true));
                    null_arrays.push(Arc::new(Int64Array::from(vec![stats.null_count])));

                    // Add min value if present
                    if let Some(min_val) = stats.min_value {
                        let arr = scalar_to_array(&min_val, field)?;
                        min_fields.push(Field::new(field_name, arr.data_type().clone(), true));
                        min_arrays.push(arr);
                    }

                    // Add max value if present
                    if let Some(max_val) = stats.max_value {
                        let arr = scalar_to_array(&max_val, field)?;
                        max_fields.push(Field::new(field_name, arr.data_type().clone(), true));
                        max_arrays.push(arr);
                    }
                }
            }
        }
    }

    // Build result structs
    let null_struct = if null_fields.is_empty() {
        None
    } else {
        Some(Arc::new(
            StructArray::try_new(null_fields.into(), null_arrays, None)
                .map_err(|e| Error::generic(format!("Failed to create nullCount struct: {e}")))?,
        ) as ArrayRef)
    };

    let min_struct = if min_fields.is_empty() {
        None
    } else {
        Some(Arc::new(
            StructArray::try_new(min_fields.into(), min_arrays, None)
                .map_err(|e| Error::generic(format!("Failed to create minValues struct: {e}")))?,
        ) as ArrayRef)
    };

    let max_struct = if max_fields.is_empty() {
        None
    } else {
        Some(Arc::new(
            StructArray::try_new(max_fields.into(), max_arrays, None)
                .map_err(|e| Error::generic(format!("Failed to create maxValues struct: {e}")))?,
        ) as ArrayRef)
    };

    Ok(FieldStatsArrays {
        null_counts: null_struct,
        min_values: min_struct,
        max_values: max_struct,
    })
}

/// Collects all statistics for a single column in one pass through row groups.
fn collect_column_stats(
    metadata: &ParquetMetaData,
    col_idx: usize,
    field: &StructField,
) -> DeltaResult<ColumnStats> {
    let col_desc = metadata.file_metadata().schema_descr().column(col_idx);

    let mut null_count: i64 = 0;
    let mut mins: Vec<Option<ScalarValue>> = Vec::new();
    let mut maxs: Vec<Option<ScalarValue>> = Vec::new();

    // Single pass through all row groups
    for rg in metadata.row_groups() {
        if let Some(stats) = rg.column(col_idx).statistics() {
            // Collect null count
            if let Some(count) = get_null_count(stats) {
                null_count += count;
            }

            // Collect min/max values
            mins.push(extract_stat_value(stats, &col_desc, field, true));
            maxs.push(extract_stat_value(stats, &col_desc, field, false));
        }
    }

    // Find overall min (minimum of all mins) and max (maximum of all maxs)
    let min_value = find_extreme(&mins, |a, b| a < b);
    let max_value = find_extreme(&maxs, |a, b| a > b);

    Ok(ColumnStats {
        null_count,
        min_value,
        max_value,
    })
}

/// Extracts null count from Parquet statistics.
fn get_null_count(stats: &Statistics) -> Option<i64> {
    let count = match stats {
        Statistics::Boolean(s) => s.null_count_opt(),
        Statistics::Int32(s) => s.null_count_opt(),
        Statistics::Int64(s) => s.null_count_opt(),
        Statistics::Int96(s) => s.null_count_opt(),
        Statistics::Float(s) => s.null_count_opt(),
        Statistics::Double(s) => s.null_count_opt(),
        Statistics::ByteArray(s) => s.null_count_opt(),
        Statistics::FixedLenByteArray(s) => s.null_count_opt(),
    };
    count.map(|c| c as i64)
}

/// Intermediate representation of a scalar value extracted from Parquet stats.
#[derive(Debug, Clone, PartialEq)]
enum ScalarValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Date32(i32),
    TimestampMicros(i64, Option<String>),
    Decimal128(i128, u8, u8),
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a.partial_cmp(b),
            (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a.partial_cmp(b),
            (ScalarValue::Int16(a), ScalarValue::Int16(b)) => a.partial_cmp(b),
            (ScalarValue::Int32(a), ScalarValue::Int32(b)) => a.partial_cmp(b),
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a.partial_cmp(b),
            (ScalarValue::Float32(a), ScalarValue::Float32(b)) => a.partial_cmp(b),
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => a.partial_cmp(b),
            (ScalarValue::String(a), ScalarValue::String(b)) => a.partial_cmp(b),
            (ScalarValue::Date32(a), ScalarValue::Date32(b)) => a.partial_cmp(b),
            (ScalarValue::TimestampMicros(a, _), ScalarValue::TimestampMicros(b, _)) => {
                a.partial_cmp(b)
            }
            (ScalarValue::Decimal128(a, _, _), ScalarValue::Decimal128(b, _, _)) => {
                a.partial_cmp(b)
            }
            _ => None,
        }
    }
}

/// Finds the extreme value (min or max) from a list of optional values.
fn find_extreme<F>(values: &[Option<ScalarValue>], cmp: F) -> Option<ScalarValue>
where
    F: Fn(&ScalarValue, &ScalarValue) -> bool,
{
    values
        .iter()
        .filter_map(|v| v.as_ref())
        .fold(None, |acc, val| match acc {
            None => Some(val.clone()),
            Some(ref current) if cmp(val, current) => Some(val.clone()),
            _ => acc,
        })
}

/// Extracts a stat value (min or max) from Parquet statistics.
fn extract_stat_value(
    stats: &Statistics,
    _col_desc: &ColumnDescriptor,
    field: &StructField,
    is_min: bool,
) -> Option<ScalarValue> {
    let kernel_type = field.data_type();
    let prim_type = kernel_type.as_primitive_opt()?;

    use PrimitiveType::*;
    let value = match (prim_type, stats) {
        (Boolean, Statistics::Boolean(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Boolean(*v)
        }
        (Byte, Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Int8(*v as i8)
        }
        (Short, Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Int16(*v as i16)
        }
        (Integer, Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Int32(*v)
        }
        (Long, Statistics::Int64(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Int64(*v)
        }
        (Long, Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Int64(*v as i64)
        }
        (Float, Statistics::Float(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Float32(*v)
        }
        (Double, Statistics::Double(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Float64(*v)
        }
        (Double, Statistics::Float(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Float64(*v as f64)
        }
        (String, Statistics::ByteArray(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            let str_val = v.as_utf8().ok()?;
            ScalarValue::String(truncate_string(str_val, is_min))
        }
        (String, Statistics::FixedLenByteArray(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            let str_val = v.as_utf8().ok()?;
            ScalarValue::String(truncate_string(str_val, is_min))
        }
        (Date, Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Date32(*v)
        }
        (Timestamp, Statistics::Int64(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::TimestampMicros(*v, Some("UTC".to_string()))
        }
        (TimestampNtz, Statistics::Int64(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::TimestampMicros(*v, None)
        }
        (Decimal(decimal_type), Statistics::Int32(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Decimal128(*v as i128, decimal_type.precision(), decimal_type.scale())
        }
        (Decimal(decimal_type), Statistics::Int64(s)) => {
            let v = if is_min { s.min_opt()? } else { s.max_opt()? };
            ScalarValue::Decimal128(*v as i128, decimal_type.precision(), decimal_type.scale())
        }
        (Decimal(decimal_type), Statistics::FixedLenByteArray(s)) => {
            let bytes = if is_min {
                s.min_bytes_opt()?
            } else {
                s.max_bytes_opt()?
            };
            let value = decimal_from_bytes(bytes)?;
            ScalarValue::Decimal128(value, decimal_type.precision(), decimal_type.scale())
        }
        _ => return None,
    };
    Some(value)
}

/// Converts bytes to decimal i128 value (big-endian, sign-extended).
fn decimal_from_bytes(bytes: &[u8]) -> Option<i128> {
    if bytes.len() > 16 {
        return None;
    }
    // Sign-extend: if high bit is set, fill with 0xFF, otherwise 0x00
    let sign_byte = if bytes.first().map_or(false, |&b| b & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut padded = [sign_byte; 16];
    padded[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(padded))
}

/// Truncates a string to the maximum length for Delta statistics.
fn truncate_string(s: &str, is_min: bool) -> String {
    if s.len() <= STRING_PREFIX_LENGTH {
        return s.to_string();
    }

    // Find a valid UTF-8 boundary at or before STRING_PREFIX_LENGTH
    let mut end = STRING_PREFIX_LENGTH;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }

    if is_min {
        // For min, simple truncation is safe
        s[..end].to_string()
    } else {
        // For max, we need to increment the last character to ensure correctness
        // This is a simplified version - a full implementation would handle
        // Unicode edge cases more carefully
        let prefix = &s[..end];
        let mut chars: Vec<char> = prefix.chars().collect();

        // Try to increment the last character
        if let Some(last) = chars.last_mut() {
            if *last < char::MAX {
                *last = char::from_u32(*last as u32 + 1).unwrap_or(*last);
            }
        }
        chars.into_iter().collect()
    }
}

/// Converts a ScalarValue to an Arrow array with a single element.
fn scalar_to_array(value: &ScalarValue, field: &StructField) -> DeltaResult<ArrayRef> {
    let array: ArrayRef = match value {
        ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v])),
        ScalarValue::Int8(v) => Arc::new(Int8Array::from(vec![*v])),
        ScalarValue::Int16(v) => Arc::new(Int16Array::from(vec![*v])),
        ScalarValue::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
        ScalarValue::Float32(v) => Arc::new(Float32Array::from(vec![*v])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
        ScalarValue::String(v) => Arc::new(StringArray::from(vec![v.as_str()])),
        ScalarValue::Date32(v) => Arc::new(Date32Array::from(vec![*v])),
        ScalarValue::TimestampMicros(v, tz) => {
            let arr = TimestampMicrosecondArray::from(vec![*v]);
            if let Some(tz_str) = tz {
                Arc::new(arr.with_timezone(tz_str.clone()))
            } else {
                Arc::new(arr)
            }
        }
        ScalarValue::Decimal128(v, precision, scale) => Arc::new(
            Decimal128Array::from(vec![*v])
                .with_precision_and_scale(*precision, *scale as i8)
                .map_err(|e| {
                    Error::generic(format!(
                        "Failed to create decimal for field {}: {e}",
                        field.name()
                    ))
                })?,
        ),
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::schema::{DataType as KernelDataType, StructField, StructType};

    fn write_parquet_and_get_metadata(batch: &RecordBatch) -> Arc<ParquetMetaData> {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let reader =
            ArrowReaderMetadata::load(&bytes::Bytes::from(buffer), Default::default()).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn test_collect_stats_basic() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])) as ArrayRef,
            ),
        ])
        .unwrap();

        let metadata = write_parquet_and_get_metadata(&batch);
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::new("number", KernelDataType::LONG, true),
                StructField::new("name", KernelDataType::STRING, true),
            ])
            .unwrap(),
        );

        let stats = collect_stats_from_parquet(
            &metadata,
            &["number".to_string(), "name".to_string()],
            &schema,
        )
        .unwrap();

        // Verify numRecords
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 3);

        // Verify nullCount
        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let number_nulls = null_count
            .column_by_name("number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(number_nulls.value(0), 0);

        // Verify minValues
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let number_min = min_values
            .column_by_name("number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(number_min.value(0), 1);

        let name_min = min_values
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_min.value(0), "alice");

        // Verify maxValues
        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let number_max = max_values
            .column_by_name("number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(number_max.value(0), 3);

        let name_max = max_values
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_max.value(0), "charlie");

        // Verify tightBounds
        let tight_bounds = stats
            .column_by_name("tightBounds")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(tight_bounds.value(0));
    }

    #[test]
    fn test_collect_stats_with_nulls() {
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        )])
        .unwrap();

        let metadata = write_parquet_and_get_metadata(&batch);
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("value", KernelDataType::LONG, true)])
                .unwrap(),
        );

        let stats = collect_stats_from_parquet(&metadata, &["value".to_string()], &schema).unwrap();

        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let value_nulls = null_count
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_nulls.value(0), 1);

        // Min/max should still be computed from non-null values
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let value_min = min_values
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_min.value(0), 1);
    }

    #[test]
    fn test_collect_stats_nested_struct() {
        // Create a nested struct: outer { inner: i64 }
        let inner_field = Field::new("inner", DataType::Int64, true);
        let outer_field = Field::new(
            "outer",
            DataType::Struct(vec![inner_field.clone()].into()),
            true,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![outer_field]));

        let inner_array = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let outer_array = Arc::new(
            StructArray::try_new(
                vec![inner_field].into(),
                vec![inner_array as ArrayRef],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(arrow_schema, vec![outer_array as ArrayRef]).unwrap();

        let metadata = write_parquet_and_get_metadata(&batch);

        // Create kernel schema for nested struct
        let kernel_schema = Arc::new(
            StructType::try_new(vec![StructField::new(
                "outer",
                KernelDataType::try_struct_type(vec![StructField::new(
                    "inner",
                    KernelDataType::LONG,
                    true,
                )])
                .unwrap(),
                true,
            )])
            .unwrap(),
        );

        // Request stats for the nested field
        let stats =
            collect_stats_from_parquet(&metadata, &["outer.inner".to_string()], &kernel_schema)
                .unwrap();

        // Verify nested minValues: { outer: { inner: 10 } }
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let outer_min = min_values
            .column_by_name("outer")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let inner_min = outer_min
            .column_by_name("inner")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(inner_min.value(0), 10);

        // Verify nested maxValues: { outer: { inner: 30 } }
        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let outer_max = max_values
            .column_by_name("outer")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let inner_max = outer_max
            .column_by_name("inner")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(inner_max.value(0), 30);
    }

    #[test]
    fn test_string_truncation() {
        // Test min truncation (simple prefix)
        let long_string = "a".repeat(50);
        let truncated = truncate_string(&long_string, true);
        assert_eq!(truncated.len(), STRING_PREFIX_LENGTH);
        assert_eq!(truncated, "a".repeat(STRING_PREFIX_LENGTH));

        // Short strings should not be truncated
        let short = "hello";
        assert_eq!(truncate_string(short, true), "hello");
        assert_eq!(truncate_string(short, false), "hello");
    }

    #[test]
    fn test_decimal_from_bytes() {
        // Test positive number
        let bytes = [0x00, 0x00, 0x00, 0x01]; // 1 in big-endian
        assert_eq!(decimal_from_bytes(&bytes), Some(1));

        // Test negative number (two's complement)
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF]; // -1 in big-endian
        assert_eq!(decimal_from_bytes(&bytes), Some(-1));
    }

    #[test]
    fn test_empty_stats_columns() {
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let metadata = write_parquet_and_get_metadata(&batch);
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("value", KernelDataType::LONG, true)])
                .unwrap(),
        );

        // Empty stats columns means no column-level stats
        let stats = collect_stats_from_parquet(&metadata, &[], &schema).unwrap();

        // Should still have numRecords and tightBounds
        assert!(stats.column_by_name("numRecords").is_some());
        assert!(stats.column_by_name("tightBounds").is_some());

        // But no nullCount, minValues, maxValues (they would be empty structs, which we skip)
        assert!(stats.column_by_name("nullCount").is_none());
        assert!(stats.column_by_name("minValues").is_none());
        assert!(stats.column_by_name("maxValues").is_none());
    }
}
