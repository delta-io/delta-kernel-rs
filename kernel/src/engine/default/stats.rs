//! Statistics collection for Delta Lake file writes.
//!
//! Provides `collect_stats` to compute min, max, and null count statistics
//! for a single RecordBatch during file writes.

use std::borrow::Cow;
use std::sync::Arc;

use crate::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Int64Array, LargeStringArray,
    PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use crate::arrow::compute::kernels::aggregate::{max, max_string, min, min_string};
use crate::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Field, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use crate::column_trie::ColumnTrie;
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

/// Maximum prefix length for string statistics (Delta protocol requirement).
const STRING_PREFIX_LENGTH: usize = 32;

/// Maximum expansion when searching for a valid max truncation point.
const STRING_EXPANSION_LIMIT: usize = STRING_PREFIX_LENGTH * 2;

/// ASCII DEL character (0x7F) - used as tie-breaker for max values when truncated char is ASCII.
const ASCII_MAX_CHAR: char = '\x7F';

/// Maximum Unicode code point - used as tie-breaker for max values when truncated char is non-ASCII.
const UTF8_MAX_CHAR: char = '\u{10FFFF}';

// ============================================================================
// String truncation for Delta statistics
// ============================================================================

/// Truncate a string for min statistics.
///
/// For min values, we simply truncate at the prefix length. The truncated value will always
/// be <= the original, which is correct for min statistics.
///
/// Returns the original string if it's already within the limit.
fn truncate_min_string(s: &str) -> &str {
    if s.len() <= STRING_PREFIX_LENGTH {
        return s;
    }
    // Find char boundary at or before STRING_PREFIX_LENGTH
    let end = s
        .char_indices()
        .take(STRING_PREFIX_LENGTH + 1)
        .last()
        .map(|(i, _)| i)
        .unwrap_or(s.len());

    // Take exactly STRING_PREFIX_LENGTH chars
    let truncated_end = s
        .char_indices()
        .nth(STRING_PREFIX_LENGTH)
        .map(|(i, _)| i)
        .unwrap_or(end);

    &s[..truncated_end]
}

/// Truncate a string for max statistics.
///
/// For max values, we need to ensure the truncated value is >= all actual values in the column.
/// We do this by appending a "tie-breaker" character after truncation:
/// - ASCII_MAX_CHAR (0x7F) if the character at the truncation point is ASCII (< 0x7F)
/// - UTF8_MAX_CHAR (U+10FFFF) otherwise
///
/// This ensures correct data skipping behavior: any string starting with the truncated prefix
/// will compare <= the truncated max + tie-breaker.
///
/// Returns `Cow::Borrowed` if no truncation needed (avoiding allocation), `Cow::Owned` when
/// truncation is performed, or `None` if the string is too long to truncate safely.
fn truncate_max_string(s: &str) -> Option<Cow<'_, str>> {
    if s.len() <= STRING_PREFIX_LENGTH {
        return Some(Cow::Borrowed(s));
    }

    // Start at STRING_PREFIX_LENGTH chars
    let char_indices: Vec<(usize, char)> = s.char_indices().collect();

    // We can expand up to STRING_EXPANSION_LIMIT chars looking for a valid truncation point
    let max_chars = char_indices.len().min(STRING_EXPANSION_LIMIT);

    // Start from STRING_PREFIX_LENGTH and look for a valid truncation point
    for len in STRING_PREFIX_LENGTH..=max_chars {
        if len >= char_indices.len() {
            // Reached end of string - return original
            return Some(Cow::Borrowed(s));
        }

        let (_, next_char) = char_indices[len];

        // If the character being truncated is U+10FFFF (max Unicode code point), we cannot
        // use this position. The tie-breaker must be >= the truncated char, but nothing is
        // greater than U+10FFFF. Include it in the prefix and check the next character.
        // (In Scala/Java this is a surrogate pair requiring substring check; in Rust it's one char)
        if next_char == UTF8_MAX_CHAR {
            continue;
        }

        let truncation_byte_idx = char_indices[len].0;
        let truncated = &s[..truncation_byte_idx];

        // Choose tie-breaker based on the character being truncated
        let tie_breaker = if next_char < ASCII_MAX_CHAR {
            ASCII_MAX_CHAR
        } else {
            UTF8_MAX_CHAR
        };

        return Some(Cow::Owned(format!("{}{}", truncated, tie_breaker)));
    }

    // Could not find a valid truncation point within expansion limit
    None
}

// ============================================================================
// Min/Max computation using Arrow compute kernels
// ============================================================================

/// Aggregation type selector.
#[derive(Clone, Copy)]
enum Agg {
    Min,
    Max,
}

/// Compute aggregation for a primitive array.
fn agg_primitive<T>(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    let array = column.as_primitive_opt::<T>().ok_or_else(|| {
        Error::generic(format!(
            "Failed to downcast column to PrimitiveArray<{}>",
            std::any::type_name::<T>()
        ))
    })?;
    let result = match agg {
        Agg::Min => min(array),
        Agg::Max => max(array),
    };
    Ok(result.map(|v| Arc::new(PrimitiveArray::<T>::from(vec![Some(v)])) as ArrayRef))
}

/// Compute aggregation for a timestamp array, preserving timezone.
fn agg_timestamp<T>(
    column: &ArrayRef,
    tz: Option<Arc<str>>,
    agg: Agg,
) -> DeltaResult<Option<ArrayRef>>
where
    T: crate::arrow::datatypes::ArrowTimestampType,
    PrimitiveArray<T>: From<Vec<Option<i64>>>,
{
    let array = column.as_primitive_opt::<T>().ok_or_else(|| {
        Error::generic(format!(
            "Failed to downcast column to PrimitiveArray<{}>",
            std::any::type_name::<T>()
        ))
    })?;
    let result = match agg {
        Agg::Min => min(array),
        Agg::Max => max(array),
    };
    Ok(result.map(|v| {
        Arc::new(PrimitiveArray::<T>::from(vec![Some(v)]).with_timezone_opt(tz)) as ArrayRef
    }))
}

/// Compute aggregation for a decimal128 array, preserving precision and scale.
fn agg_decimal(
    column: &ArrayRef,
    precision: u8,
    scale: i8,
    agg: Agg,
) -> DeltaResult<Option<ArrayRef>> {
    let array = column
        .as_primitive_opt::<Decimal128Type>()
        .ok_or_else(|| Error::generic("Failed to downcast column to Decimal128Array"))?;
    let result = match agg {
        Agg::Min => min(array),
        Agg::Max => max(array),
    };
    result
        .map(|v| {
            Decimal128Array::from(vec![Some(v)])
                .with_precision_and_scale(precision, scale)
                .map(|arr| Arc::new(arr) as ArrayRef)
        })
        .transpose()
        .map_err(|e| Error::generic(format!("Invalid decimal precision/scale: {e}")))
}

/// Compute aggregation for a string array with truncation.
fn agg_string(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    let array = column
        .as_string_opt::<i32>()
        .ok_or_else(|| Error::generic("Failed to downcast column to StringArray"))?;
    let result = match agg {
        Agg::Min => min_string(array),
        Agg::Max => max_string(array),
    };
    match (result, agg) {
        (Some(s), Agg::Min) => {
            let truncated = truncate_min_string(s);
            Ok(Some(
                Arc::new(StringArray::from(vec![Some(truncated)])) as ArrayRef
            ))
        }
        (Some(s), Agg::Max) => Ok(truncate_max_string(s)
            .map(|t| Arc::new(StringArray::from(vec![Some(&*t)])) as ArrayRef)),
        (None, _) => Ok(None),
    }
}

/// Compute aggregation for a large string array with truncation.
///
/// Unlike StringArray, Arrow's compute kernels don't provide min/max for LargeStringArray,
/// so we iterate manually. `iter()` yields `Option<&str>` per element (None for nulls),
/// and `flatten()` filters out nulls so we only compare non-null values.
fn agg_large_string(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    let array = column
        .as_string_opt::<i64>()
        .ok_or_else(|| Error::generic("Failed to downcast column to LargeStringArray"))?;
    let result = match agg {
        Agg::Min => array.iter().flatten().min(),
        Agg::Max => array.iter().flatten().max(),
    };
    match (result, agg) {
        (Some(s), Agg::Min) => {
            let truncated = truncate_min_string(s);
            Ok(Some(
                Arc::new(LargeStringArray::from(vec![Some(truncated)])) as ArrayRef,
            ))
        }
        (Some(s), Agg::Max) => Ok(truncate_max_string(s)
            .map(|t| Arc::new(LargeStringArray::from(vec![Some(&*t)])) as ArrayRef)),
        (None, _) => Ok(None),
    }
}

/// Compute aggregation for a string view array with truncation.
///
/// Like LargeStringArray, Arrow's compute kernels don't provide min/max for StringViewArray.
/// See `agg_large_string` for explanation of `iter().flatten()`.
fn agg_string_view(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    let array = column
        .as_string_view_opt()
        .ok_or_else(|| Error::generic("Failed to downcast column to StringViewArray"))?;
    let result: Option<&str> = match agg {
        Agg::Min => array.iter().flatten().min(),
        Agg::Max => array.iter().flatten().max(),
    };
    match (result, agg) {
        (Some(s), Agg::Min) => {
            let truncated = truncate_min_string(s);
            Ok(Some(
                Arc::new(StringViewArray::from(vec![Some(truncated)])) as ArrayRef
            ))
        }
        (Some(s), Agg::Max) => Ok(truncate_max_string(s)
            .map(|t| Arc::new(StringViewArray::from(vec![Some(&*t)])) as ArrayRef)),
        (None, _) => Ok(None),
    }
}

/// Compute min or max for a leaf column based on its data type.
fn compute_leaf_agg(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    match column.data_type() {
        // Integer types
        DataType::Int8 => agg_primitive::<Int8Type>(column, agg),
        DataType::Int16 => agg_primitive::<Int16Type>(column, agg),
        DataType::Int32 => agg_primitive::<Int32Type>(column, agg),
        DataType::Int64 => agg_primitive::<Int64Type>(column, agg),
        DataType::UInt8 => agg_primitive::<UInt8Type>(column, agg),
        DataType::UInt16 => agg_primitive::<UInt16Type>(column, agg),
        DataType::UInt32 => agg_primitive::<UInt32Type>(column, agg),
        DataType::UInt64 => agg_primitive::<UInt64Type>(column, agg),

        // Float types
        DataType::Float32 => agg_primitive::<Float32Type>(column, agg),
        DataType::Float64 => agg_primitive::<Float64Type>(column, agg),

        // Date types
        DataType::Date32 => agg_primitive::<Date32Type>(column, agg),
        DataType::Date64 => agg_primitive::<Date64Type>(column, agg),

        // Timestamp types (preserve timezone)
        DataType::Timestamp(TimeUnit::Second, tz) => {
            agg_timestamp::<TimestampSecondType>(column, tz.clone(), agg)
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            agg_timestamp::<TimestampMillisecondType>(column, tz.clone(), agg)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            agg_timestamp::<TimestampMicrosecondType>(column, tz.clone(), agg)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            agg_timestamp::<TimestampNanosecondType>(column, tz.clone(), agg)
        }

        // Decimal type (preserve precision/scale)
        DataType::Decimal128(p, s) => agg_decimal(column, *p, *s, agg),

        // String types (with truncation)
        DataType::Utf8 => agg_string(column, agg),
        DataType::LargeUtf8 => agg_large_string(column, agg),
        DataType::Utf8View => agg_string_view(column, agg),

        // Unsupported types (structs handled separately, others return no min/max)
        _ => Ok(None),
    }
}

// ============================================================================
// Combined stats computation (single traversal)
// ============================================================================

/// Statistics computed for a column (leaf or nested struct).
#[derive(Default)]
struct ColumnStats {
    null_count: Option<ArrayRef>,
    min_value: Option<ArrayRef>,
    max_value: Option<ArrayRef>,
}

/// Compute all statistics for a column in a single traversal.
///
/// Returns `ColumnStats` containing null_count, min, and max for this column.
/// For struct columns, these are nested StructArrays. For leaf columns, these are scalar arrays.
/// Map, List, and other complex types are skipped (returns default empty stats).
fn compute_column_stats(
    column: &ArrayRef,
    path: &mut Vec<String>,
    filter: &ColumnTrie<'_>,
) -> DeltaResult<ColumnStats> {
    match column.data_type() {
        DataType::Struct(fields) => {
            let struct_array = column
                .as_struct_opt()
                .ok_or_else(|| Error::generic("Failed to downcast column to StructArray"))?;

            // Accumulators for each stat type
            let mut null_fields: Vec<Field> = Vec::new();
            let mut null_arrays: Vec<ArrayRef> = Vec::new();
            let mut min_fields: Vec<Field> = Vec::new();
            let mut min_arrays: Vec<ArrayRef> = Vec::new();
            let mut max_fields: Vec<Field> = Vec::new();
            let mut max_arrays: Vec<ArrayRef> = Vec::new();

            for (i, field) in fields.iter().enumerate() {
                path.push(field.name().to_string());

                let child_stats = compute_column_stats(struct_array.column(i), path, filter)?;

                if let Some(arr) = child_stats.null_count {
                    null_fields.push(Field::new(field.name(), arr.data_type().clone(), true));
                    null_arrays.push(arr);
                }
                if let Some(arr) = child_stats.min_value {
                    min_fields.push(Field::new(field.name(), arr.data_type().clone(), true));
                    min_arrays.push(arr);
                }
                if let Some(arr) = child_stats.max_value {
                    max_fields.push(Field::new(field.name(), arr.data_type().clone(), true));
                    max_arrays.push(arr);
                }

                path.pop();
            }

            // Build result structs (None if empty)
            let build_struct =
                |fields: Vec<Field>, arrays: Vec<ArrayRef>| -> DeltaResult<Option<ArrayRef>> {
                    if fields.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(Arc::new(
                            StructArray::try_new(fields.into(), arrays, None)
                                .map_err(|e| Error::generic(format!("stats struct: {e}")))?,
                        ) as ArrayRef))
                    }
                };

            Ok(ColumnStats {
                null_count: build_struct(null_fields, null_arrays)?,
                min_value: build_struct(min_fields, min_arrays)?,
                max_value: build_struct(max_fields, max_arrays)?,
            })
        }
        // Skip complex types that don't support statistics
        DataType::Map(_, _)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => Ok(ColumnStats::default()),
        _ => {
            // Leaf: check filter, compute all stats together
            if !filter.contains_prefix_of(path) {
                return Ok(ColumnStats::default());
            }

            Ok(ColumnStats {
                null_count: Some(Arc::new(Int64Array::from(vec![column.null_count() as i64]))),
                min_value: compute_leaf_agg(column, Agg::Min)?,
                max_value: compute_leaf_agg(column, Agg::Max)?,
            })
        }
    }
}

/// Accumulates (field_name, array) pairs for building a stats struct.
struct StatsAccumulator {
    name: &'static str,
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
}

impl StatsAccumulator {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            fields: Vec::new(),
            arrays: Vec::new(),
        }
    }

    fn push(&mut self, field_name: &str, array: ArrayRef) {
        self.fields
            .push(Field::new(field_name, array.data_type().clone(), true));
        self.arrays.push(array);
    }

    fn build(self) -> DeltaResult<Option<(Field, Arc<dyn Array>)>> {
        if self.fields.is_empty() {
            return Ok(None);
        }
        let struct_arr = StructArray::try_new(self.fields.into(), self.arrays, None)
            .map_err(|e| Error::generic(format!("Failed to create {}: {e}", self.name)))?;
        let field = Field::new(self.name, struct_arr.data_type().clone(), true);
        Ok(Some((field, Arc::new(struct_arr) as Arc<dyn Array>)))
    }
}

/// Collect statistics from a RecordBatch for Delta Lake file statistics.
///
/// Returns a StructArray with the following fields:
/// - `numRecords`: total row count
/// - `nullCount`: nested struct with null counts per column
/// - `minValues`: nested struct with min values per column
/// - `maxValues`: nested struct with max values per column
/// - `tightBounds`: always true for new file writes
///
/// # Arguments
/// * `batch` - The RecordBatch to collect statistics from
/// * `stats_columns` - Column names that should have statistics collected (allowlist).
///   Only these columns will appear in nullCount/minValues/maxValues.
pub(crate) fn collect_stats(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
) -> DeltaResult<StructArray> {
    let filter = ColumnTrie::from_columns(stats_columns);
    let schema = batch.schema();

    // Collect all stats in a single traversal
    let mut null_counts = StatsAccumulator::new("nullCount");
    let mut min_values = StatsAccumulator::new("minValues");
    let mut max_values = StatsAccumulator::new("maxValues");

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut path = vec![field.name().to_string()];
        let column = batch.column(col_idx);

        // Single traversal computes all three stats
        let stats = compute_column_stats(column, &mut path, &filter)?;

        if let Some(arr) = stats.null_count {
            null_counts.push(field.name(), arr);
        }
        if let Some(arr) = stats.min_value {
            min_values.push(field.name(), arr);
        }
        if let Some(arr) = stats.max_value {
            max_values.push(field.name(), arr);
        }
    }

    // Build output struct
    let mut fields = vec![Field::new("numRecords", DataType::Int64, true)];
    let mut arrays: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(vec![batch.num_rows() as i64]))];

    for acc in [null_counts, min_values, max_values] {
        if let Some((field, array)) = acc.build()? {
            fields.push(field);
            arrays.push(array);
        }
    }

    // tightBounds
    fields.push(Field::new("tightBounds", DataType::Boolean, true));
    arrays.push(Arc::new(BooleanArray::from(vec![true])));

    StructArray::try_new(fields.into(), arrays, None)
        .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array, StringArray};
    use crate::arrow::datatypes::{Fields, Schema};
    use crate::expressions::column_name;

    #[test]
    fn test_collect_stats_single_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("id")]).unwrap();

        assert_eq!(stats.len(), 1);
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 3);
    }

    #[test]
    fn test_collect_stats_null_counts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("id"), column_name!("value")]).unwrap();

        // Check nullCount struct
        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // id has 0 nulls
        let id_null_count = null_count
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_null_count.value(0), 0);

        // value has 1 null
        let value_null_count = null_count
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_null_count.value(0), 1);
    }

    #[test]
    fn test_collect_stats_respects_stats_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        // Only collect stats for "id", not "value"
        let stats = collect_stats(&batch, &[column_name!("id")]).unwrap();

        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Only id should be present
        assert!(null_count.column_by_name("id").is_some());
        assert!(null_count.column_by_name("value").is_none());
    }

    #[test]
    fn test_collect_stats_min_max() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![5, 1, 9, 3])),
                Arc::new(StringArray::from(vec![
                    Some("banana"),
                    Some("apple"),
                    Some("cherry"),
                    None,
                ])),
            ],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("number"), column_name!("name")]).unwrap();

        // Check minValues
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
        assert_eq!(name_min.value(0), "apple");

        // Check maxValues
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
        assert_eq!(number_max.value(0), 9);

        let name_max = max_values
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_max.value(0), "cherry");
    }

    #[test]
    fn test_collect_stats_all_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                None as Option<i64>,
                None,
                None,
            ]))],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("value")]).unwrap();

        // numRecords should be 3
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 3);

        // nullCount should be 3
        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let value_null_count = null_count
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_null_count.value(0), 3);

        // minValues/maxValues should not have "value" field (all nulls)
        if let Some(min_values) = stats.column_by_name("minValues") {
            let min_struct = min_values.as_any().downcast_ref::<StructArray>().unwrap();
            assert!(min_struct.column_by_name("value").is_none());
        }
    }

    #[test]
    fn test_collect_stats_empty_stats_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        // No stats columns requested
        let stats = collect_stats(&batch, &[]).unwrap();

        // Should still have numRecords and tightBounds
        assert!(stats.column_by_name("numRecords").is_some());
        assert!(stats.column_by_name("tightBounds").is_some());

        // Should not have nullCount, minValues, maxValues
        assert!(stats.column_by_name("nullCount").is_none());
        assert!(stats.column_by_name("minValues").is_none());
        assert!(stats.column_by_name("maxValues").is_none());
    }

    #[test]
    fn test_collect_stats_string_truncation_ascii() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        // Create an ASCII string longer than 32 characters
        let long_string = "a".repeat(50);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![long_string.as_str()]))],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("text")]).unwrap();

        // Check minValues - should be truncated to exactly 32 chars
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let text_min = min_values
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(text_min.value(0).len(), 32);
        assert_eq!(text_min.value(0), "a".repeat(32));

        // Check maxValues - should be 32 chars + 0x7F tie-breaker (since 'a' < 0x7F)
        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let text_max = max_values
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let expected_max = format!("{}\x7F", "a".repeat(32));
        assert_eq!(text_max.value(0), expected_max);
    }

    #[test]
    fn test_collect_stats_string_truncation_non_ascii() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        // Create a string where the character BEING TRUNCATED (at position 32) is non-ASCII.
        // The tie-breaker is chosen based on the first char being removed, not the last kept.
        // 32 'a's followed by 'À' (>= 0x7F) followed by more chars
        let long_string = format!("{}À{}", "a".repeat(32), "b".repeat(20));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![long_string.as_str()]))],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("text")]).unwrap();

        // Check maxValues - should use UTF8_MAX_CHAR since 'À' (the truncated char) >= 0x7F
        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let text_max = max_values
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Should be 32 'a's + U+10FFFF (tie-breaker for non-ASCII truncated char)
        let expected_max = format!("{}\u{10FFFF}", "a".repeat(32));
        assert_eq!(text_max.value(0), expected_max);
    }

    #[test]
    fn test_collect_stats_string_no_truncation_needed() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        // String within 32 chars - should not be truncated
        let short_string = "hello world";
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![short_string]))],
        )
        .unwrap();

        let stats = collect_stats(&batch, &[column_name!("text")]).unwrap();

        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let text_min = min_values
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(text_min.value(0), short_string);

        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let text_max = max_values
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(text_max.value(0), short_string);
    }

    #[test]
    fn test_truncate_min_string() {
        // Short string - no truncation
        assert_eq!(truncate_min_string("hello"), "hello");

        // Exactly 32 chars - no truncation
        let s32 = "a".repeat(32);
        assert_eq!(truncate_min_string(&s32), s32);

        // Long string - truncated to 32 chars
        let s50 = "a".repeat(50);
        assert_eq!(truncate_min_string(&s50), "a".repeat(32));

        // Multi-byte characters
        let multi = format!("{}À", "a".repeat(35)); // 'À' is 2 bytes in UTF-8
        assert_eq!(truncate_min_string(&multi).chars().count(), 32);
    }

    #[test]
    fn test_truncate_max_string() {
        // Short string - no truncation, returns Cow::Borrowed
        assert_eq!(truncate_max_string("hello").as_deref(), Some("hello"));

        // Exactly 32 chars - no truncation
        let s32 = "a".repeat(32);
        assert_eq!(truncate_max_string(&s32).as_deref(), Some(s32.as_str()));

        // Long ASCII string - truncated with 0x7F tie-breaker
        // The 33rd char ('a') is < 0x7F, so we use 0x7F
        let s50 = "a".repeat(50);
        let expected = format!("{}\x7F", "a".repeat(32));
        assert_eq!(
            truncate_max_string(&s50).as_deref(),
            Some(expected.as_str())
        );

        // Non-ASCII at truncation point - uses UTF8_MAX_CHAR
        // 32 'a's then 'À' (which is >= 0x7F), so we use UTF8_MAX
        let non_ascii = format!("{}À{}", "a".repeat(32), "b".repeat(20));
        let expected = format!("{}\u{10FFFF}", "a".repeat(32));
        assert_eq!(
            truncate_max_string(&non_ascii).as_deref(),
            Some(expected.as_str())
        );

        // U+10FFFF at truncation point - must skip past it
        // 32 'a's then U+10FFFF then 'b' - we can't truncate at U+10FFFF (no tie-breaker > it)
        // so we include U+10FFFF in prefix and use 'b' to determine tie-breaker
        let with_max_char = format!("{}\u{10FFFF}b{}", "a".repeat(32), "c".repeat(10));
        let expected = format!("{}\u{10FFFF}\x7F", "a".repeat(32)); // 'b' < 0x7F
        assert_eq!(
            truncate_max_string(&with_max_char).as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn test_collect_stats_nested_struct() {
        // Schema: { nested: { a: int64, b: string } }
        let nested_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(nested_fields.clone()),
            false,
        )]));

        // Build nested struct data
        let a_array = Arc::new(Int64Array::from(vec![10, 5, 20]));
        let b_array = Arc::new(StringArray::from(vec![Some("zebra"), Some("apple"), None]));
        let nested_struct = StructArray::try_new(
            nested_fields,
            vec![a_array as ArrayRef, b_array as ArrayRef],
            None,
        )
        .unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(nested_struct) as ArrayRef]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("nested")]).unwrap();

        // Check nullCount.nested.a = 0, nullCount.nested.b = 1
        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let nested_null = null_count
            .column_by_name("nested")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let a_null = nested_null
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_null.value(0), 0);

        let b_null = nested_null
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(b_null.value(0), 1);

        // Check minValues.nested.a = 5, minValues.nested.b = "apple"
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let nested_min = min_values
            .column_by_name("nested")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let a_min = nested_min
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_min.value(0), 5);

        let b_min = nested_min
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b_min.value(0), "apple");

        // Check maxValues.nested.a = 20, maxValues.nested.b = "zebra"
        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let nested_max = max_values
            .column_by_name("nested")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let a_max = nested_max
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_max.value(0), 20);

        let b_max = nested_max
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b_max.value(0), "zebra");
    }

    #[test]
    fn test_collect_stats_skips_complex_types() {
        use crate::arrow::array::ListArray;
        use crate::arrow::buffer::OffsetBuffer;

        // Schema with list column - should be skipped for statistics
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));

        // Build list array: [[1, 2], [3], [4, 5, 6]]
        let values = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 6].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            None,
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(list_array),
            ],
        )
        .unwrap();

        // Request stats for both columns
        let stats = collect_stats(&batch, &[column_name!("id"), column_name!("list_col")]).unwrap();

        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // id should have null count
        assert!(null_count.column_by_name("id").is_some());

        // list_col should NOT have null count (complex type skipped)
        assert!(null_count.column_by_name("list_col").is_none());

        // Same for minValues/maxValues
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(min_values.column_by_name("id").is_some());
        assert!(min_values.column_by_name("list_col").is_none());
    }
}
