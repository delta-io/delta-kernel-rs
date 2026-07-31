//! Statistics collection for Delta Lake file writes: min, max, and null count per column.
//!
//! [`FileStatsAccumulator`] merges per-row-group statistics into one file-level statistics struct.

use std::borrow::Cow;
use std::sync::Arc;

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS};
use delta_kernel::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Int64Array,
    LargeStringArray, PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use delta_kernel::arrow::compute::concat;
use delta_kernel::arrow::compute::kernels::aggregate::{
    bool_and, max, max_string, min, min_string, sum_checked,
};
use delta_kernel::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Field, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef as ArrowSchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use delta_kernel::column_trie::ColumnTrie;
use delta_kernel::engine::arrow_utils::fix_nested_null_masks;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType as KernelDataType, StructType};
use delta_kernel::{DeltaResult, Error};

/// Maximum prefix length for string statistics (Delta protocol requirement).
const STRING_PREFIX_LENGTH: usize = 32;

/// Maximum expansion when searching for a valid max truncation point.
const STRING_EXPANSION_LIMIT: usize = STRING_PREFIX_LENGTH * 2;

/// ASCII DEL character (0x7F) - used as tie-breaker for max values when truncated char is ASCII.
const ASCII_MAX_CHAR: char = '\x7F';

/// Maximum Unicode code point - used as tie-breaker for max values when truncated char is
/// non-ASCII.
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

        return Some(Cow::Owned(format!("{truncated}{tie_breaker}")));
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
    T: delta_kernel::arrow::datatypes::ArrowTimestampType,
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

/// Compute aggregation for a string array.
fn agg_string(column: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    let array = column
        .as_string_opt::<i32>()
        .ok_or_else(|| Error::generic("Failed to downcast column to StringArray"))?;
    let result = match agg {
        Agg::Min => min_string(array),
        Agg::Max => max_string(array),
    };
    Ok(result.map(|v| Arc::new(StringArray::from(vec![Some(v)])) as ArrayRef))
}

/// Compute aggregation for a large string array.
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
    Ok(result.map(|v| Arc::new(LargeStringArray::from(vec![Some(v)])) as ArrayRef))
}

/// Compute aggregation for a string view array.
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
    Ok(result.map(|v| Arc::new(StringViewArray::from(vec![Some(v)])) as ArrayRef))
}

/// Compute min or max for a leaf column based on its data type.
///
/// The result is the raw aggregate; [`truncate_stats_bound`] truncates string bounds.
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

        // String types
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
/// Complex types (Map, List, Variant) get nullCount only because they have no meaningful
/// ordering for min/max data skipping, but null counts are still useful for tracking nullability.
fn compute_column_stats(
    column: &ArrayRef,
    path: &mut Vec<String>,
    filter: &ColumnTrie<'_>,
    null_count_only_filter: &ColumnTrie<'_>,
) -> DeltaResult<ColumnStats> {
    match column.data_type() {
        // A struct column that the filter marks as a terminal leaf (e.g. Variant, which is a
        // struct at the Arrow level but a leaf for stats purposes) gets nullCount only, no
        // recursion into sub-fields and no min/max.
        DataType::Struct(_) if filter.is_terminal(path) => Ok(ColumnStats {
            null_count: Some(Arc::new(Int64Array::from(vec![column.null_count() as i64]))),
            min_value: None,
            max_value: None,
        }),
        DataType::Struct(fields) => {
            let struct_array = column
                .as_struct_opt()
                .ok_or_else(|| Error::generic("Failed to downcast column to StructArray"))?;

            // Propagate struct-level nulls to all descendants
            let fixed_struct = fix_nested_null_masks(struct_array.clone());

            // Accumulators for each stat type
            let mut null_fields: Vec<Field> = Vec::new();
            let mut null_arrays: Vec<ArrayRef> = Vec::new();
            let mut min_fields: Vec<Field> = Vec::new();
            let mut min_arrays: Vec<ArrayRef> = Vec::new();
            let mut max_fields: Vec<Field> = Vec::new();
            let mut max_arrays: Vec<ArrayRef> = Vec::new();

            for (i, field) in fields.iter().enumerate() {
                path.push(field.name().to_string());

                let child_stats = compute_column_stats(
                    fixed_struct.column(i),
                    path,
                    filter,
                    null_count_only_filter,
                )?;

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
        // Void columns (Arrow `Null` / kernel `VOID`): every value is null by definition,
        // and the column has no parquet representation. We still need to publish nullCount
        // for IS NULL / IS NOT NULL data skipping, so synthesize it from the array length.
        // Use `column.len()` rather than `column.null_count()` because `NullArray` has no
        // null buffer and the inherited `Array::null_count` default returns 0.
        DataType::Null => {
            if !filter.contains_prefix_of(path) {
                return Ok(ColumnStats::default());
            }
            Ok(ColumnStats {
                null_count: Some(Arc::new(Int64Array::from(vec![column.len() as i64]))),
                min_value: None,
                max_value: None,
            })
        }
        _ => {
            // Leaf: check filter, compute all stats together
            if !filter.contains_prefix_of(path) {
                return Ok(ColumnStats::default());
            }

            let null_count: Option<ArrayRef> =
                Some(Arc::new(Int64Array::from(vec![column.null_count() as i64])));

            let complex_type = matches!(
                column.data_type(),
                DataType::Map(_, _)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
            );
            if complex_type || null_count_only_filter.contains_prefix_of(path) {
                return Ok(ColumnStats {
                    null_count,
                    min_value: None,
                    max_value: None,
                });
            }

            // When min/max is None (all nulls or unsupported type), emit a null-valued
            // single-element array to keep the field present in the stats struct. This
            // allows downstream consumers (like StatsColumnVerifier) to find the column and
            // check nullCount == numRecords. The JSON serializer omits null fields, so
            // the on-disk format still matches Spark's ignoreNullFields behavior.
            let null_fallback = || -> ArrayRef { Arc::new(new_null_array(column.data_type(), 1)) };
            Ok(ColumnStats {
                null_count,
                min_value: Some(compute_leaf_agg(column, Agg::Min)?.unwrap_or_else(&null_fallback)),
                max_value: Some(compute_leaf_agg(column, Agg::Max)?.unwrap_or_else(null_fallback)),
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
/// - `minValues`: nested struct with min values per column (null when all values are null)
/// - `maxValues`: nested struct with max values per column (null when all values are null)
/// - `tightBounds`: always true for new file writes
///
/// String min/max values are truncated to a 32-character prefix with appropriate tie-breaker
/// characters for max values. See the `stats_schema` module documentation for the full stats
/// value rules.
///
/// # Parameters
///
/// - `batch`: The record batch to collect statistics from.
/// - `stats_columns`: The columns to include in `nullCount`, `minValues`, and `maxValues`.
/// - `physical_schema`: The kernel physical schema, used to preserve logical type distinctions that
///   are erased in Arrow arrays.
///
/// # Returns
///
/// A single-row struct array containing the collected file statistics.
///
/// # Errors
///
/// Returns an error if a column cannot be converted to its expected Arrow type or the output stats
/// array cannot be constructed.
pub fn collect_stats(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
    physical_schema: &StructType,
) -> DeltaResult<StructArray> {
    let null_count_only_columns = interval_column_names(physical_schema, stats_columns);
    reduce_stats(&collect_stats_raw(
        batch,
        stats_columns,
        &null_count_only_columns,
    )?)
}

#[cfg(test)]
pub(crate) fn collect_stats_for_test(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
) -> DeltaResult<StructArray> {
    reduce_stats(&collect_stats_raw(batch, stats_columns, &[])?)
}

/// Collect one batch's raw (untruncated) statistics as a single-row struct. [`reduce_stats`] turns
/// one or more such rows into the publishable statistics for a file.
fn collect_stats_raw(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
    null_count_only_columns: &[ColumnName],
) -> DeltaResult<StructArray> {
    let filter = ColumnTrie::from_columns(stats_columns);
    let null_count_only_filter = ColumnTrie::from_columns(null_count_only_columns);
    let schema = batch.schema();

    // Collect all stats in a single traversal
    let mut null_counts = StatsAccumulator::new(NULL_COUNT);
    let mut min_values = StatsAccumulator::new(MIN_VALUES);
    let mut max_values = StatsAccumulator::new(MAX_VALUES);

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut path = vec![field.name().to_string()];
        let column = batch.column(col_idx);

        // Single traversal computes all three stats
        let stats = compute_column_stats(column, &mut path, &filter, &null_count_only_filter)?;

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
    let mut fields = vec![Field::new(NUM_RECORDS, DataType::Int64, true)];
    let mut arrays: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(vec![batch.num_rows() as i64]))];

    for acc in [null_counts, min_values, max_values] {
        if let Some((field, array)) = acc.build()? {
            fields.push(field);
            arrays.push(array);
        }
    }

    // tightBounds
    fields.push(Field::new(TIGHT_BOUNDS, DataType::Boolean, true));
    arrays.push(Arc::new(BooleanArray::from(vec![true])));

    StructArray::try_new(fields.into(), arrays, None)
        .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
}

// ============================================================================
// File-level stats aggregation
// ============================================================================

/// Accumulates per-row-group Delta statistics into a single file-level statistics struct.
///
/// A connector that writes one file as multiple row groups feeds each row group's [`RecordBatch`]
/// to [`FileStatsAccumulator::merge`] as the row group closes, then calls
/// [`FileStatsAccumulator::finish`] once the file is complete to get the file-level statistics for
/// the Add action. Only one single-row statistics struct per row group is retained, never the row
/// groups themselves.
///
/// Per-row-group statistics are accumulated raw and reduced once by
/// [`FileStatsAccumulator::finish`], which truncates string bounds as part of that reduction. Raw
/// bounds compose across row groups where truncated ones would not, so the file-level statistics
/// are byte-identical to collecting statistics over the whole file at once.
///
/// Every batch must have the same Arrow schema, since the statistics shape derives from it:
/// [`FileStatsAccumulator::merge`] rejects a batch whose Arrow schema differs from that of the
/// first batch merged. A failed merge is terminal: every later [`merge`] and [`finish`] returns an
/// error, so a file whose row groups did not all merge cannot publish partial statistics.
///
/// [`merge`]: FileStatsAccumulator::merge
/// [`finish`]: FileStatsAccumulator::finish
///
/// # Example
///
/// The merged statistics go into [`DataFileMetadata::new`](crate::parquet::DataFileMetadata::new)
/// to build the written file's Add action.
///
/// ```
/// # use std::sync::Arc;
/// # use delta_kernel::arrow::array::{Array, AsArray, Int64Array, RecordBatch, StructArray};
/// # use delta_kernel::arrow::datatypes::{DataType, Field, Int64Type, Schema};
/// # use delta_kernel::expressions::column_name;
/// # use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};
/// # use delta_kernel_default_engine::stats::FileStatsAccumulator;
/// # fn main() -> delta_kernel::DeltaResult<()> {
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let row_group = |ids: Vec<i64>| {
///     RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])
/// };
/// let physical_schema = StructType::try_new([StructField::not_null("id", KernelDataType::LONG)])?;
///
/// let mut acc = FileStatsAccumulator::new(&[column_name!("id")], &physical_schema);
/// acc.merge(&row_group(vec![1, 2])?)?;
/// acc.merge(&row_group(vec![3, 4])?)?;
/// let stats = acc.finish()?.expect("two row groups were merged");
///
/// let id_stat = |section: &str| -> i64 {
///     stats.column_by_name(section).unwrap().as_struct().column_by_name("id").unwrap()
///         .as_primitive::<Int64Type>().value(0)
/// };
/// assert_eq!(
///     stats.column_by_name("numRecords").unwrap().as_primitive::<Int64Type>().value(0),
///     4,
/// );
/// assert_eq!(id_stat("minValues"), 1); // from the first row group
/// assert_eq!(id_stat("maxValues"), 4); // from the second
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FileStatsAccumulator {
    stats_columns: Vec<ColumnName>,
    null_count_only_columns: Vec<ColumnName>,
    batch_schema: Option<ArrowSchemaRef>,
    state: AccumulatorState,
}

/// Makes the terminal state representable, so a failed merge cannot be forgotten.
#[derive(Debug)]
enum AccumulatorState {
    /// One raw statistics row per row group merged so far.
    Open(Vec<StructArray>),
    /// A merge failed, so the row groups merged so far can never be published.
    Failed,
}

const FAILED: &str = "statistics accumulator failed a previous merge";

impl FileStatsAccumulator {
    /// Create an empty accumulator that collects statistics for `stats_columns` (the same
    /// allowlist used for every row group of the file). `physical_schema` is the physical schema
    /// of the data being written, used to distinguish interval columns (which get `nullCount`
    /// only) from integer columns.
    pub fn new(stats_columns: &[ColumnName], physical_schema: &StructType) -> Self {
        Self {
            null_count_only_columns: interval_column_names(physical_schema, stats_columns),
            stats_columns: stats_columns.to_vec(),
            batch_schema: None,
            state: AccumulatorState::Open(Vec::new()),
        }
    }

    /// Collect one row group's raw statistics and add them to the accumulator.
    ///
    /// Returns an error if the accumulator already failed a merge, or if this one fails: `batch`
    /// has a different Arrow schema than the first batch merged, it produces a different statistics
    /// shape, or statistics collection fails.
    pub fn merge(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        // Catch every error path in one place, so no failure can leave publishable state behind.
        self.try_merge(batch)
            .inspect_err(|_| self.state = AccumulatorState::Failed)
    }

    fn try_merge(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        let AccumulatorState::Open(row_groups) = &mut self.state else {
            return Err(Error::stats_validation(FAILED));
        };
        // Comparing derived stats shapes instead would collapse every nullCount-only leaf to the
        // same Int64.
        let first_schema = self.batch_schema.get_or_insert_with(|| batch.schema());
        if first_schema != &batch.schema() {
            // `Debug`: schema equality covers the schema's metadata map, which `Display` omits.
            return Err(Error::schema(format!(
                "all row groups in a file must have the same schema; expected {:?} but got {:?}",
                first_schema,
                batch.schema(),
            )));
        }
        let batch_stats =
            collect_stats_raw(batch, &self.stats_columns, &self.null_count_only_columns)?;
        // Schema equality can ignore a struct column's nested field names, which statistics
        // collection filters on, so equal schemas can still derive different stats shapes.
        if let Some(first) = row_groups.first() {
            if first.data_type() != batch_stats.data_type() {
                return Err(Error::schema(format!(
                    "all row groups must produce the same statistics shape; batch schemas compare \
                     equal but their nested field names differ: {} vs {}",
                    first.data_type(),
                    batch_stats.data_type(),
                )));
            }
        }
        row_groups.push(batch_stats);
        Ok(())
    }

    /// Consume the accumulator and return the file-level statistics, or `None` if no row group was
    /// merged. A zero-row row group is still a merged row group and yields statistics with a
    /// `numRecords` of 0.
    ///
    /// Publishing statistics for a file with no rows therefore requires merging a zero-row batch:
    /// an accumulator that merged nothing yields `None`, and an Add action carrying no
    /// `numRecords` is rejected by tables that require it (IcebergCompat tables, and any file
    /// carrying a deletion vector).
    ///
    /// Returns `Err` if a merge failed, or if the accumulated statistics cannot be reduced into a
    /// single row.
    pub fn finish(self) -> DeltaResult<Option<StructArray>> {
        let AccumulatorState::Open(row_groups) = self.state else {
            return Err(Error::stats_validation(FAILED));
        };
        if row_groups.is_empty() {
            return Ok(None);
        }
        let rows: Vec<&dyn Array> = row_groups.iter().map(|s| s as &dyn Array).collect();
        let combined = concat(&rows)
            .map_err(|e| Error::generic(format!("concat per-row-group stats: {e}")))?;
        let combined = combined
            .as_struct_opt()
            .ok_or_else(|| Error::internal_error("concatenated stats are not a struct"))?;
        reduce_stats(combined).map(Some)
    }
}

/// Reduce a raw statistics struct of one row per row group into the single publishable row for the
/// file: `numRecords` and `nullCount` summed, `minValues`/`maxValues` aggregated element-wise and
/// truncated per Delta's string rules, `tightBounds` AND-ed.
///
/// Returns `Err` if `stats` is not shaped like the output of `collect_stats_raw`.
fn reduce_stats(stats: &StructArray) -> DeltaResult<StructArray> {
    let fields = stats.fields().clone();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(stats.num_columns());
    for (field, col) in fields.iter().zip(stats.columns()) {
        cols.push(match field.name().as_str() {
            NUM_RECORDS => reduce_count_leaf(col)?,
            NULL_COUNT => reduce_stats_children(col, &reduce_count_leaf)?,
            MIN_VALUES => reduce_stats_children(col, &|c| reduce_minmax_leaf(c, Agg::Min))?,
            MAX_VALUES => reduce_stats_children(col, &|c| reduce_minmax_leaf(c, Agg::Max))?,
            TIGHT_BOUNDS => reduce_bool_and_leaf(col)?,
            // Keep in sync with the sections `collect_stats_raw` produces. User columns live inside
            // the sub-structs, so an unrecognized top-level name is a kernel bug, not bad input.
            other => {
                return Err(Error::internal_error(format!(
                    "cannot reduce unknown stats section: {other}"
                )))
            }
        });
    }
    StructArray::try_new(fields, cols, None)
        .map_err(|e| Error::generic(format!("rebuilding reduced stats struct: {e}")))
}

/// Reduce each child of a stats sub-struct (`nullCount`/`minValues`/`maxValues`) to one row,
/// recursing into nested structs so nested-column stats reduce correctly. `reduce` handles a leaf.
fn reduce_stats_children(
    array: &ArrayRef,
    reduce: &dyn Fn(&ArrayRef) -> DeltaResult<ArrayRef>,
) -> DeltaResult<ArrayRef> {
    let struct_array = array
        .as_struct_opt()
        .ok_or_else(|| Error::internal_error("expected struct in stats sub-tree"))?;
    let fields = struct_array.fields().clone();
    let cols = struct_array
        .columns()
        .iter()
        .map(|col| match col.as_struct_opt() {
            Some(_) => reduce_stats_children(col, reduce),
            None => reduce(col),
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(fields, cols, None).map_err(
        |e| Error::generic(format!("rebuilding reduced stats sub-struct: {e}")),
    )?))
}

/// Sum an Int64 count leaf (`numRecords` or a `nullCount` leaf) to one row.
///
/// A null count is unknown, not zero, and the protocol defines no default for it, so a null leaf is
/// an error rather than a silently invented count.
fn reduce_count_leaf(array: &ArrayRef) -> DeltaResult<ArrayRef> {
    let arr = array
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| Error::internal_error("expected Int64 count leaf in stats"))?;
    if arr.null_count() != 0 {
        return Err(Error::internal_error("null count leaf in stats"));
    }
    let sum = sum_checked(arr)
        .map_err(|e| Error::generic(format!("summing stats count leaf: {e}")))?
        .unwrap_or(0);
    Ok(Arc::new(Int64Array::from(vec![sum])))
}

/// AND a Boolean `tightBounds` leaf to one row: tight only if every row group was tight.
///
/// A null is an error rather than an assumed bound tightness.
fn reduce_bool_and_leaf(array: &ArrayRef) -> DeltaResult<ArrayRef> {
    let arr = array
        .as_boolean_opt()
        .ok_or_else(|| Error::internal_error("expected Boolean tightBounds leaf in stats"))?;
    if arr.null_count() != 0 {
        return Err(Error::internal_error("null tightBounds leaf in stats"));
    }
    let all = bool_and(arr).unwrap_or(true);
    Ok(Arc::new(BooleanArray::from(vec![all])))
}

/// Reduce a min (or max) leaf to the one bound published for the file, null if none is
/// representable.
///
/// Reusing `compute_leaf_agg` gives the bound the same ordering a single-pass collection uses (NaN,
/// decimal precision/scale, timestamp timezone, string byte order).
fn reduce_minmax_leaf(array: &ArrayRef, agg: Agg) -> DeltaResult<ArrayRef> {
    let bound = match compute_leaf_agg(array, agg)? {
        Some(bound) => truncate_stats_bound(&bound, agg)?,
        None => None,
    };
    Ok(bound.unwrap_or_else(|| new_null_array(array.data_type(), 1)))
}

/// Truncate a single-row min/max bound per Delta's string rules, passing non-string bounds through.
///
/// `None` when a max string has no representable truncated upper bound: no upper bound may be
/// published in that case.
fn truncate_stats_bound(bound: &ArrayRef, agg: Agg) -> DeltaResult<Option<ArrayRef>> {
    let s = match bound.data_type() {
        DataType::Utf8 => bound.as_string_opt::<i32>().and_then(|a| a.iter().next()),
        DataType::LargeUtf8 => bound.as_string_opt::<i64>().and_then(|a| a.iter().next()),
        DataType::Utf8View => bound.as_string_view_opt().and_then(|a| a.iter().next()),
        _ => return Ok(Some(bound.clone())),
    };
    let s = s
        .flatten()
        .ok_or_else(|| Error::generic("expected a single non-null string stats bound"))?;
    let Some(truncated) = (match agg {
        Agg::Min => Some(Cow::Borrowed(truncate_min_string(s))),
        Agg::Max => truncate_max_string(s),
    }) else {
        return Ok(None);
    };
    let v = Some(&*truncated);
    Ok(Some(match bound.data_type() {
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![v])) as ArrayRef,
        DataType::Utf8View => Arc::new(StringViewArray::from(vec![v])) as ArrayRef,
        _ => Arc::new(StringArray::from(vec![v])) as ArrayRef,
    }))
}

fn is_interval_type(data_type: &KernelDataType) -> bool {
    let KernelDataType::Primitive(primitive_type) = data_type else {
        return false;
    };
    primitive_type.is_interval()
}

fn interval_column_names(schema: &StructType, stats_columns: &[ColumnName]) -> Vec<ColumnName> {
    stats_columns
        .iter()
        .filter(|col| {
            schema
                .fields_of_path(col)
                .ok()
                .and_then(|fields| fields.last().map(|field| field.data_type()))
                .is_some_and(is_interval_type)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{
        Array, AsArray, BinaryArray, Decimal128Array, Float64Array, Int32Array, Int64Array,
        ListArray, MapArray, NullArray, RecordBatchOptions, StringArray,
    };
    use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
    use delta_kernel::arrow::compute::concat_batches;
    use delta_kernel::arrow::datatypes::{Fields, Int32Type, Int64Type, Schema};
    use delta_kernel::engine::arrow_conversion::TryFromArrow as _;
    use delta_kernel::engine::arrow_expression::evaluate_expression::to_json;
    use delta_kernel::expressions::column_name;
    use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use delta_kernel::schema::StructField;
    use test_utils::assert_result_error_with_message;

    use super::*;

    fn collect_stats(
        batch: &RecordBatch,
        stats_columns: &[ColumnName],
    ) -> DeltaResult<StructArray> {
        super::collect_stats_for_test(batch, stats_columns)
    }

    #[test]
    fn test_collect_stats_single_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("id")]).unwrap();

        assert_eq!(stats.len(), 1);
        let num_records = stats
            .column_by_name(NUM_RECORDS)
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
            .column_by_name(NULL_COUNT)
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
            .column_by_name(NULL_COUNT)
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
            .column_by_name(MIN_VALUES)
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
            .column_by_name(MAX_VALUES)
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
            .column_by_name(NUM_RECORDS)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 3);

        // nullCount should be 3
        let null_count = stats
            .column_by_name(NULL_COUNT)
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

        // All-null columns are present in minValues/maxValues but with null values.
        // The field must exist so that StatsColumnVerifier can find it via visit_rows and
        // check nullCount == numRecords. The JSON serializer omits null fields, so
        // the on-disk format still matches Spark's ignoreNullFields behavior.
        let min_values = stats
            .column_by_name(MIN_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let min_col = min_values.column_by_name("value").unwrap();
        assert!(min_col.is_null(0));

        let max_values = stats
            .column_by_name(MAX_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let max_col = max_values.column_by_name("value").unwrap();
        assert!(max_col.is_null(0));
    }

    // A void column reaches stats only if a connector or direct caller bypasses the
    // kernel-side physical write schema, which strips void columns. Even so, we must
    // publish nullCount = numRecords rather than 0, because `NullArray` has no null
    // buffer and the inherited `Array::null_count` default returns 0. Min/max are not
    // meaningful for void.
    #[rstest::rstest]
    #[case::non_empty(5)]
    #[case::empty(0)]
    fn test_collect_stats_void_column_synthesizes_full_null_count(#[case] length: usize) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(length))]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("v")]).unwrap();

        let null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let v_null_count = null_count
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v_null_count.value(0), length as i64);

        // Void columns do not participate in min/max stats. With void as the only stats
        // column, both struct accumulators stay empty and the fields are omitted entirely.
        assert!(stats.column_by_name("minValues").is_none());
        assert!(stats.column_by_name("maxValues").is_none());
    }

    #[test]
    fn test_collect_stats_void_column_nested_in_struct() {
        // Worst realistic shape for the latent bug: void buried inside a struct alongside
        // a non-void sibling. The recursion must reach the `DataType::Null` arm so that
        // `s.v` records `nullCount = numRecords`, while `s.a` still gets full min/max.
        let inner_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("v", DataType::Null, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(inner_fields.clone()),
            false,
        )]));
        let inner = StructArray::try_new(
            inner_fields,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(NullArray::new(4)) as ArrayRef,
            ],
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(inner) as ArrayRef]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("s.a"), column_name!("s.v")]).unwrap();

        let s_null_count = stats
            .column_by_name("nullCount")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let v_null_count = s_null_count
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v_null_count.value(0), 4);

        // `s.a` keeps its min/max; `s.v` does not appear under min/max because void has no
        // ordering.
        let s_min = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(s_min.column_by_name("a").is_some());
        assert!(s_min.column_by_name("v").is_none());
    }

    #[test]
    fn test_collect_stats_empty_stats_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        // No stats columns requested
        let stats = collect_stats(&batch, &[]).unwrap();

        // Should still have numRecords and tightBounds
        assert!(stats.column_by_name(NUM_RECORDS).is_some());
        assert!(stats.column_by_name(TIGHT_BOUNDS).is_some());

        // Should not have nullCount, minValues, maxValues
        assert!(stats.column_by_name(NULL_COUNT).is_none());
        assert!(stats.column_by_name(MIN_VALUES).is_none());
        assert!(stats.column_by_name(MAX_VALUES).is_none());
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
            .column_by_name(MIN_VALUES)
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
            .column_by_name(MAX_VALUES)
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
            .column_by_name(MAX_VALUES)
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
            .column_by_name(MIN_VALUES)
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
            .column_by_name(MAX_VALUES)
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

        let stats = collect_stats(
            &batch,
            &[column_name!("nested.a"), column_name!("nested.b")],
        )
        .unwrap();

        // Check nullCount.nested.a = 0, nullCount.nested.b = 1
        let null_count = stats
            .column_by_name(NULL_COUNT)
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
            .column_by_name(MIN_VALUES)
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
            .column_by_name(MAX_VALUES)
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
    fn test_collect_stats_complex_types_null_count_only() {
        // Schema with list column - should have nullCount but no min/max
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));

        // Build list array: [[1, 2], null, [4, 5, 6]]
        let values = Int64Array::from(vec![1, 2, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            Some(vec![true, false, true].into()), // second element is null
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
            .column_by_name(NULL_COUNT)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // id should have null count = 0
        let id_nulls = null_count
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_nulls.value(0), 0);

        // list_col should have null count = 1
        let list_nulls = null_count
            .column_by_name("list_col")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(list_nulls.value(0), 1);

        // minValues should have id but NOT list_col
        let min_values = stats
            .column_by_name(MIN_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(min_values.column_by_name("id").is_some());
        assert!(min_values.column_by_name("list_col").is_none());

        // maxValues should have id but NOT list_col
        let max_values = stats
            .column_by_name(MAX_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(max_values.column_by_name("id").is_some());
        assert!(max_values.column_by_name("list_col").is_none());
    }

    #[test]
    fn test_collect_stats_map_null_count_only() {
        // === GIVEN: a batch with an id column and a map column (1 null out of 3 rows) ===
        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "map_col",
                DataType::Map(Arc::new(map_field.clone()), false),
                true,
            ),
        ]));
        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int64Array::from(vec![1, 2]);
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let map_array = MapArray::new(
            Arc::new(map_field),
            OffsetBuffer::new(vec![0, 1, 1, 2].into()),
            entries,
            Some(vec![true, false, true].into()), // second element is null
            false,
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(map_array),
            ],
        )
        .unwrap();

        // === WHEN: collecting stats for both columns ===
        let stats = collect_stats(&batch, &[column_name!("id"), column_name!("map_col")]).unwrap();

        // === THEN: map_col has nullCount=1 but no min/max ===
        let null_count = child_struct(&stats, NULL_COUNT);
        let map_nulls = null_count
            .column_by_name("map_col")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(map_nulls.value(0), 1);

        let min_values = child_struct(&stats, MIN_VALUES);
        assert!(min_values.column_by_name("id").is_some());
        assert!(min_values.column_by_name("map_col").is_none());
    }

    #[test]
    fn test_collect_stats_variant_terminal_struct_null_count_only() {
        // Variant is Struct { metadata: Binary, value: Binary } at the Arrow level, but
        // stats_column_names lists it as a terminal leaf ["v"]. The is_terminal guard in
        // compute_column_stats must produce nullCount at the struct level without recursing
        // into metadata/value sub-fields.

        // === GIVEN: a batch with an id column and a Variant column (1 null out of 3 rows) ===
        let variant_fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Struct(variant_fields.clone()), true),
        ]));
        let variant_array = StructArray::new(
            variant_fields,
            vec![
                Arc::new(BinaryArray::from(vec![
                    Some([0x01, 0x00, 0x00].as_slice()),
                    None,
                    Some([0x01, 0x00, 0x00].as_slice()),
                ])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    Some([0x0C].as_slice()),
                    None,
                    Some([0x0C].as_slice()),
                ])) as ArrayRef,
            ],
            Some(NullBuffer::from_iter([true, false, true])), // row 1 is null
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(variant_array),
            ],
        )
        .unwrap();

        // === WHEN: collecting stats with "v" as a terminal column ===
        let stats = collect_stats(&batch, &[column_name!("id"), column_name!("v")]).unwrap();

        // === THEN: v has nullCount=1 at the struct level, no recursion, no min/max ===
        let null_count = child_struct(&stats, NULL_COUNT);
        let v_nulls = null_count
            .column_by_name("v")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(v_nulls.value(0), 1);

        // v must NOT be recursed into, no nested metadata/value in nullCount
        assert!(null_count.column_by_name("metadata").is_none());
        assert!(null_count.column_by_name("value").is_none());

        let min_values = child_struct(&stats, MIN_VALUES);
        assert!(min_values.column_by_name("id").is_some());
        assert!(min_values.column_by_name("v").is_none());
    }

    #[test]
    fn test_collect_stats_struct_with_nulls_at_struct_level() {
        // Schema: { my_struct: { a: int32, b: int32 (nullable) } }
        // Test both struct-level nulls and field-level nulls
        let child_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);

        let a_values = Int32Array::from(vec![1, 2, 3, 4]);
        // b has field-level nulls at rows 0 and 2
        let b_values = Int32Array::from(vec![None, Some(20), None, Some(40)]);

        // Struct validity: [false, true, true, false]
        // In Arrow: false = null, true = valid
        // So rows 0 and 3 have null structs (entire struct is null)
        let nulls = NullBuffer::from(vec![false, true, true, false]);

        let struct_array = StructArray::new(
            child_fields.clone(),
            vec![Arc::new(a_values), Arc::new(b_values)],
            Some(nulls),
        );

        let schema = Schema::new(vec![Field::new(
            "my_struct",
            DataType::Struct(child_fields),
            true,
        )]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)]).unwrap();

        let stats = collect_stats(
            &batch,
            &[column_name!("my_struct.a"), column_name!("my_struct.b")],
        )
        .unwrap();

        // Visualizing the data:
        // Row 0: struct=NULL,  (a=1, b=None are "invisible")
        // Row 1: struct=VALID, a=2, b=20
        // Row 2: struct=VALID, a=3, b=None
        // Row 3: struct=NULL,  (a=4, b=40 are "invisible")
        //
        // Expected behavior (struct nulls propagate to children):
        // - a: visible values are [2, 3], nullCount = 2 (rows 0, 3 are struct-null)
        // - b: visible values are [20, None], nullCount = 3 (rows 0, 3 struct-null + row 2
        //   field-null)
        // - a: min=2, max=3
        // - b: min=20, max=20

        // nullCount includes struct-level nulls
        assert_eq!(
            get_stat::<Int64Type>(&stats, NULL_COUNT, "my_struct", "a"),
            2
        );
        assert_eq!(
            get_stat::<Int64Type>(&stats, NULL_COUNT, "my_struct", "b"),
            3
        );

        // minValues excludes values from null struct rows
        assert_eq!(
            get_stat::<Int32Type>(&stats, MIN_VALUES, "my_struct", "a"),
            2
        );
        assert_eq!(
            get_stat::<Int32Type>(&stats, MIN_VALUES, "my_struct", "b"),
            20
        );

        // maxValues excludes values from null struct rows
        assert_eq!(
            get_stat::<Int32Type>(&stats, MAX_VALUES, "my_struct", "a"),
            3
        );
        assert_eq!(
            get_stat::<Int32Type>(&stats, MAX_VALUES, "my_struct", "b"),
            20
        );
    }

    #[test]
    fn test_file_stats_accumulator_empty_returns_none() {
        let physical_schema = StructType::try_from_arrow(&Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))
        .unwrap();
        let acc = FileStatsAccumulator::new(&[column_name!("id")], &physical_schema);
        assert!(acc.finish().unwrap().is_none());
    }

    #[test]
    fn test_file_stats_accumulator_single_batch_matches_collect_stats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let cols = vec![column_name!("id"), column_name!("value")];
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();

        let single = collect_stats(&batch, &cols).unwrap();

        let mut acc = FileStatsAccumulator::new(&cols, &physical_schema);
        acc.merge(&batch).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(
            to_json(&merged).unwrap().as_string::<i32>().value(0),
            to_json(&single).unwrap().as_string::<i32>().value(0),
        );
    }

    /// Extracts a named child column from a StructArray, downcasting it to StructArray.
    fn child_struct<'a>(parent: &'a StructArray, name: &str) -> &'a StructArray {
        parent
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
    }

    /// Reads a primitive leaf of a nested column: `stats[stat_name][struct_name][field_name]`.
    fn get_stat<T>(
        stats: &StructArray,
        stat_name: &str,
        struct_name: &str,
        field_name: &str,
    ) -> T::Native
    where
        T: delta_kernel::arrow::datatypes::ArrowPrimitiveType,
    {
        flat_stat::<T>(child_struct(stats, stat_name), struct_name, field_name)
    }

    /// Reads a primitive leaf of a top-level column: `stats[stat_name][field_name]`.
    fn flat_stat<T>(stats: &StructArray, stat_name: &str, field_name: &str) -> T::Native
    where
        T: delta_kernel::arrow::datatypes::ArrowPrimitiveType,
    {
        child_struct(stats, stat_name)
            .column_by_name(field_name)
            .unwrap()
            .as_primitive::<T>()
            .value(0)
    }

    /// Reads a string leaf directly under `struct_array`, `None` if the bound is null.
    fn string_leaf<'a>(struct_array: &'a StructArray, field_name: &str) -> Option<&'a str> {
        let leaf = struct_array
            .column_by_name(field_name)
            .unwrap()
            .as_string_opt::<i32>()
            .unwrap();
        leaf.is_valid(0).then(|| leaf.value(0))
    }

    #[test]
    fn test_file_stats_accumulator_merges_two_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![5, 1, 9])),
                Arc::new(StringArray::from(vec![Some("banana"), Some("apple"), None])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 12, 0])),
                Arc::new(StringArray::from(vec![Some("cherry"), None, Some("date")])),
            ],
        )
        .unwrap();
        let cols = vec![column_name!("n"), column_name!("s")];

        let mut acc = FileStatsAccumulator::new(&cols, &physical_schema);
        acc.merge(&b1).unwrap();
        acc.merge(&b2).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(
            merged
                .column_by_name(NUM_RECORDS)
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            6
        );
        assert_eq!(flat_stat::<Int64Type>(&merged, NULL_COUNT, "s"), 2);
        assert_eq!(flat_stat::<Int64Type>(&merged, MIN_VALUES, "n"), 0);
        assert_eq!(flat_stat::<Int64Type>(&merged, MAX_VALUES, "n"), 12);
        let leaf = |section| string_leaf(child_struct(&merged, section), "s");
        assert_eq!(leaf(MIN_VALUES), Some("apple"));
        assert_eq!(leaf(MAX_VALUES), Some("date"));
        assert!(merged
            .column_by_name(TIGHT_BOUNDS)
            .unwrap()
            .as_boolean()
            .value(0));
    }

    #[test]
    fn test_file_stats_accumulator_min_max_skips_all_null_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let all_null = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None as Option<i64>, None]))],
        )
        .unwrap();
        let valued = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(7), Some(4)]))],
        )
        .unwrap();

        let mut acc = FileStatsAccumulator::new(&[column_name!("v")], &physical_schema);
        acc.merge(&all_null).unwrap();
        acc.merge(&valued).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(flat_stat::<Int64Type>(&merged, MIN_VALUES, "v"), 4);
        assert_eq!(flat_stat::<Int64Type>(&merged, MAX_VALUES, "v"), 7);
        assert_eq!(flat_stat::<Int64Type>(&merged, NULL_COUNT, "v"), 2);
    }

    /// Recursively extracts leaf column names from an Arrow schema for stats collection.
    fn extract_leaf_columns(fields: &Fields, prefix: &[String]) -> Vec<ColumnName> {
        let mut columns = Vec::new();
        for field in fields.iter() {
            let mut path = prefix.to_vec();
            path.push(field.name().clone());
            match field.data_type() {
                DataType::Struct(sub_fields) => {
                    columns.extend(extract_leaf_columns(sub_fields, &path));
                }
                _ => {
                    columns.push(ColumnName::new(path));
                }
            }
        }
        columns
    }

    /// Recursively compares Spark stats JSON against kernel stats JSON.
    /// Only checks keys present in `spark_val`; kernel may have extra keys.
    fn assert_stats_match(
        spark_val: &serde_json::Value,
        kernel_val: &serde_json::Value,
        path: &str,
    ) {
        match (spark_val, kernel_val) {
            (serde_json::Value::Object(spark_map), serde_json::Value::Object(kernel_map)) => {
                for (key, spark_child) in spark_map {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    let kernel_child = kernel_map
                        .get(key)
                        .unwrap_or_else(|| panic!("Kernel stats missing key: {child_path}"));
                    assert_stats_match(spark_child, kernel_child, &child_path);
                }
            }
            (serde_json::Value::Number(s), serde_json::Value::Number(k)) => {
                let sv = s.as_f64().unwrap();
                let kv = k.as_f64().unwrap();
                assert!(
                    (sv - kv).abs() < 1e-6,
                    "Numeric mismatch at {path}: spark={sv}, kernel={kv}"
                );
            }
            (serde_json::Value::String(s), serde_json::Value::String(k)) => {
                // Spark uses "Z" suffix for TZ timestamps and no suffix for NTZ.
                let s_normalized = s.trim_end_matches('Z').trim_end_matches("+00:00");
                let k_normalized = k.trim_end_matches('Z').trim_end_matches("+00:00");

                // Spark (Jackson) always includes fractional seconds (e.g., ".000") while Arrow's
                // JSON encoder omits them when they are zero. Strip zero-only fractional parts
                // so both formats compare equal.
                if s_normalized.contains('T') && k_normalized.contains('T') {
                    let normalize_ts = |ts: &str| -> String {
                        if let Some(dot_pos) = ts.rfind('.') {
                            let frac = &ts[dot_pos + 1..];
                            if frac.chars().all(|c| c == '0') {
                                return ts[..dot_pos].to_string();
                            }
                            let trimmed = frac.trim_end_matches('0');
                            return format!("{}.{trimmed}", &ts[..dot_pos]);
                        }
                        ts.to_string()
                    };
                    let s_norm = normalize_ts(s_normalized);
                    let k_norm = normalize_ts(k_normalized);
                    assert_eq!(
                        s_norm, k_norm,
                        "Timestamp mismatch at {path}: spark={s}, kernel={k}"
                    );
                } else {
                    assert_eq!(s, k, "String mismatch at {path}: spark={s}, kernel={k}");
                }
            }
            _ => {
                assert_eq!(
                    spark_val, kernel_val,
                    "Value mismatch at {path}: spark={spark_val}, kernel={kernel_val}"
                );
            }
        }
    }

    // Verify that the `assert_stats_match` test helper correctly accepts equivalent values.
    #[test]
    fn test_assert_stats_match_accepts_equivalent_values() {
        // Extra kernel keys are ignored
        let spark = serde_json::json!({"a": 1, "b": "hello"});
        let kernel = serde_json::json!({"a": 1, "b": "hello", "extra": true});
        assert_stats_match(&spark, &kernel, "");

        // Nested objects with extra kernel keys
        let spark = serde_json::json!({"outer": {"inner": 42}});
        let kernel = serde_json::json!({"outer": {"inner": 42, "extra": 0}});
        assert_stats_match(&spark, &kernel, "");

        // Timestamp with trailing ".000Z" vs no fractional part
        let spark = serde_json::json!({"ts": "2023-06-15T12:30:00.000Z"});
        let kernel = serde_json::json!({"ts": "2023-06-15T12:30:00Z"});
        assert_stats_match(&spark, &kernel, "");

        // Timestamp NTZ (no Z suffix) with trailing ".000"
        let spark = serde_json::json!({"ts": "2023-06-15T12:30:00.000"});
        let kernel = serde_json::json!({"ts": "2023-06-15T12:30:00"});
        assert_stats_match(&spark, &kernel, "");

        // Non-zero fractional seconds with different trailing zeros
        let spark = serde_json::json!({"ts": "2023-06-15T12:30:00.500Z"});
        let kernel = serde_json::json!({"ts": "2023-06-15T12:30:00.5Z"});
        assert_stats_match(&spark, &kernel, "");
    }

    // Verify that the `assert_stats_match` test helper correctly rejects mismatched values.
    #[test]
    fn test_assert_stats_match_rejects_mismatches() {
        let result = std::panic::catch_unwind(|| {
            let spark = serde_json::json!({"a": 1});
            let kernel = serde_json::json!({"b": 1});
            assert_stats_match(&spark, &kernel, "");
        });
        assert!(result.is_err(), "should panic on missing key");

        let result = std::panic::catch_unwind(|| {
            let spark = serde_json::json!({"val": 1.0});
            let kernel = serde_json::json!({"val": 2.0});
            assert_stats_match(&spark, &kernel, "");
        });
        assert!(result.is_err(), "should panic on numeric mismatch");

        let result = std::panic::catch_unwind(|| {
            let spark = serde_json::json!({"s": "alpha"});
            let kernel = serde_json::json!({"s": "beta"});
            assert_stats_match(&spark, &kernel, "");
        });
        assert!(result.is_err(), "should panic on string mismatch");
    }

    /// Loads the PySpark-generated `stats-writing-all-types` fixture: the whole parquet file as one
    /// batch, and Spark's own stats for it, taken from the `add` action of commit 1. The fixture
    /// covers every supported stat type.
    fn load_spark_all_types_fixture() -> (RecordBatch, serde_json::Value) {
        let test_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../kernel/tests/data/stats-writing-all-types/delta");
        let commit_path = test_path
            .join("_delta_log")
            .join("00000000000000000001.json");
        let commit_data = std::fs::read_to_string(&commit_path).expect("read commit 1 json");

        let add = commit_data
            .lines()
            .filter_map(|line| {
                let action: serde_json::Value =
                    serde_json::from_str(line).expect("parse JSON line");
                action.get("add").cloned()
            })
            .next()
            .expect("commit 1 has an add action");
        let spark_stats: serde_json::Value =
            serde_json::from_str(add["stats"].as_str().expect("stats str"))
                .expect("parse Spark stats JSON");

        let parquet_file_path = test_path.join(add["path"].as_str().expect("path str"));
        let file = std::fs::File::open(&parquet_file_path).expect("open parquet file");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader builder");
        let schema = builder.schema().clone();
        let batches: Vec<RecordBatch> = builder
            .build()
            .expect("build parquet reader")
            .map(|b| b.expect("read batch"))
            .collect();
        let whole = concat_batches(&schema, &batches).expect("concat batches");
        (whole, spark_stats)
    }

    /// Asserts kernel's stats agree with Spark's on numRecords and every key Spark published.
    fn assert_matches_spark_stats(kernel_stats: &StructArray, spark_stats: &serde_json::Value) {
        let json_array = to_json(kernel_stats).expect("convert stats to JSON");
        let json_strings = json_array.as_string::<i32>();
        assert_eq!(json_strings.len(), 1, "should have exactly one stats row");
        let kernel_stats: serde_json::Value =
            serde_json::from_str(json_strings.value(0)).expect("parse kernel stats JSON");

        assert_eq!(
            spark_stats[NUM_RECORDS], kernel_stats[NUM_RECORDS],
            "numRecords mismatch"
        );
        for section in &[NULL_COUNT, MIN_VALUES, MAX_VALUES] {
            if let Some(spark_section) = spark_stats.get(*section) {
                let kernel_section = kernel_stats
                    .get(*section)
                    .unwrap_or_else(|| panic!("Kernel stats missing {section}"));
                assert_stats_match(spark_section, kernel_section, section);
            }
        }
    }

    #[test]
    fn test_collect_stats_matches_spark() {
        let (whole, spark_stats) = load_spark_all_types_fixture();
        let stats_columns = extract_leaf_columns(whole.schema().fields(), &[]);
        let stats = collect_stats(&whole, &stats_columns).expect("collect stats");
        assert_matches_spark_stats(&stats, &spark_stats);
    }

    #[test]
    fn test_file_stats_accumulator_nested_struct() {
        let inner = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(inner.clone()),
            false,
        )]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |a: Vec<i64>, b: Vec<Option<&str>>| {
            let st = StructArray::try_new(
                inner.clone(),
                vec![
                    Arc::new(Int64Array::from(a)) as ArrayRef,
                    Arc::new(StringArray::from(b)) as ArrayRef,
                ],
                None,
            )
            .unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(st) as ArrayRef]).unwrap()
        };
        let b1 = mk(vec![10, 5], vec![Some("m"), Some("z")]);
        let b2 = mk(vec![2, 8], vec![Some("a"), None]);

        let mut acc = FileStatsAccumulator::new(
            &[column_name!("s.a"), column_name!("s.b")],
            &physical_schema,
        );
        acc.merge(&b1).unwrap();
        acc.merge(&b2).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(get_stat::<Int64Type>(&merged, MIN_VALUES, "s", "a"), 2);
        assert_eq!(get_stat::<Int64Type>(&merged, MAX_VALUES, "s", "a"), 10);
        assert_eq!(get_stat::<Int64Type>(&merged, NULL_COUNT, "s", "b"), 1);
        let nested_b =
            |section| string_leaf(child_struct(child_struct(&merged, section), "s"), "b");
        assert_eq!(nested_b(MIN_VALUES), Some("a"));
        assert_eq!(nested_b(MAX_VALUES), Some("z"));
    }

    #[test]
    fn test_file_stats_accumulator_preserves_decimal() {
        let dt = DataType::Decimal128(10, 2);
        let schema = Arc::new(Schema::new(vec![Field::new("d", dt.clone(), false)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |vals: Vec<i128>| {
            let arr = Decimal128Array::from(vals)
                .with_precision_and_scale(10, 2)
                .unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef]).unwrap()
        };
        let mut acc = FileStatsAccumulator::new(&[column_name!("d")], &physical_schema);
        acc.merge(&mk(vec![500, 100])).unwrap();
        acc.merge(&mk(vec![50, 900])).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(
            child_struct(&merged, MIN_VALUES)
                .column_by_name("d")
                .unwrap()
                .data_type(),
            &dt
        );
        assert_eq!(flat_stat::<Decimal128Type>(&merged, MIN_VALUES, "d"), 50);
        assert_eq!(flat_stat::<Decimal128Type>(&merged, MAX_VALUES, "d"), 900);
    }

    // NaN wins max and loses min, matching `compute_leaf_agg`.
    #[test]
    fn test_file_stats_accumulator_float_nan_ordering() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |vals: Vec<f64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Float64Array::from(vals)) as ArrayRef],
            )
            .unwrap()
        };
        let mut acc = FileStatsAccumulator::new(&[column_name!("f")], &physical_schema);
        acc.merge(&mk(vec![1.0, 2.0])).unwrap();
        acc.merge(&mk(vec![f64::NAN, 0.5])).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(flat_stat::<Float64Type>(&merged, MIN_VALUES, "f"), 0.5);
        assert!(flat_stat::<Float64Type>(&merged, MAX_VALUES, "f").is_nan());
    }

    #[rstest::rstest]
    #[case::ascii(
        "a".repeat(50),
        "b".repeat(50),
        Some("a".repeat(32)),
        Some(format!("{}\x7F", "b".repeat(32)))
    )]
    // Over 32 bytes but under 32 chars: `truncate_max_string` gates on byte length but searches for
    // the truncation point by char count, so the range is empty and no max is published.
    #[case::no_max_bound_published(
        "\u{6f22}".repeat(11),
        "a".to_string(),
        Some("a".to_string()),
        None
    )]
    #[case::multibyte_min(
        "a".repeat(32),
        format!("{}\u{3042}", "a".repeat(31)),
        Some("a".repeat(32)),
        Some(format!("{}\u{3042}", "a".repeat(31)))
    )]
    fn test_file_stats_accumulator_string_stats_match_single_shot(
        #[case] v1: String,
        #[case] v2: String,
        #[case] expected_min: Option<String>,
        #[case] expected_max: Option<String>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |vals: Vec<&str>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vals)) as ArrayRef],
            )
            .unwrap()
        };
        let whole = mk(vec![v1.as_str(), v2.as_str()]);
        let single = collect_stats(&whole, &[column_name!("t")]).unwrap();

        let mut acc = FileStatsAccumulator::new(&[column_name!("t")], &physical_schema);
        acc.merge(&mk(vec![v1.as_str()])).unwrap();
        acc.merge(&mk(vec![v2.as_str()])).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        let value = |st: &StructArray, sect: &str| -> Option<String> {
            string_leaf(child_struct(st, sect), "t").map(str::to_string)
        };
        assert_eq!(value(&merged, MIN_VALUES), value(&single, MIN_VALUES));
        assert_eq!(value(&merged, MAX_VALUES), value(&single, MAX_VALUES));
        assert_eq!(value(&merged, MIN_VALUES), expected_min);
        assert_eq!(value(&merged, MAX_VALUES), expected_max);

        if let Some(min) = value(&merged, MIN_VALUES) {
            assert!(min.as_str() <= v1.as_str() && min.as_str() <= v2.as_str());
        }
        if let Some(max) = value(&merged, MAX_VALUES) {
            assert!(max.as_str() >= v1.as_str() && max.as_str() >= v2.as_str());
        }
    }

    // Splits past the fixture's row count exercise merging empty row groups.
    #[rstest::rstest]
    fn test_file_stats_accumulator_matches_spark(#[values(1, 2, 3, 4, 6)] n_row_groups: usize) {
        let (whole, spark_stats) = load_spark_all_types_fixture();
        let schema = whole.schema();
        let stats_columns = extract_leaf_columns(schema.fields(), &[]);
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();

        let mut acc = FileStatsAccumulator::new(&stats_columns, &physical_schema);
        let rows_per_group = whole.num_rows().div_ceil(n_row_groups);
        for i in 0..n_row_groups {
            let offset = (i * rows_per_group).min(whole.num_rows());
            let len = rows_per_group.min(whole.num_rows() - offset);
            acc.merge(&whole.slice(offset, len))
                .unwrap_or_else(|e| panic!("merge row group {i}: {e}"));
        }
        let merged = acc.finish().unwrap().expect("merged stats");

        assert_matches_spark_stats(&merged, &spark_stats);
        // Spark's JSON omits bounds it did not publish and never carries tightBounds, so only
        // single-shot collection covers those.
        let single = super::collect_stats(&whole, &stats_columns, &physical_schema).unwrap();
        assert_eq!(
            to_json(&merged).unwrap().as_string::<i32>().value(0),
            to_json(&single).unwrap().as_string::<i32>().value(0),
        );
    }

    // A zero-row row group still counts as merged: it contributes numRecords and null bounds, so
    // only an accumulator that merged nothing yields `None`.
    #[rstest::rstest]
    #[case::no_row_groups(vec![], None, None)]
    #[case::one_row_group(vec![vec![1i64, 2, 3]], Some(3), Some((1, 3)))]
    #[case::two_row_groups(vec![vec![1i64], vec![2i64]], Some(2), Some((1, 2)))]
    #[case::only_zero_row_group(vec![vec![]], Some(0), None)]
    #[case::zero_row_group_between(vec![vec![1i64], vec![], vec![2i64]], Some(2), Some((1, 2)))]
    fn test_file_stats_accumulator_finish_counts_and_bounds(
        #[case] row_groups: Vec<Vec<i64>>,
        #[case] expected_num_records: Option<i64>,
        #[case] expected_bounds: Option<(i64, i64)>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mut acc = FileStatsAccumulator::new(&[column_name!("n")], &physical_schema);
        for rows in &row_groups {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(rows.clone()))],
            )
            .unwrap();
            acc.merge(&batch).expect("merge must succeed");
        }
        let merged = acc.finish().expect("finish must succeed");
        let num_records = merged.as_ref().map(|stats| {
            stats
                .column_by_name(NUM_RECORDS)
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0)
        });
        assert_eq!(num_records, expected_num_records);

        let Some(stats) = merged else { return };
        let bound = |section: &str| {
            let leaf = child_struct(&stats, section)
                .column_by_name("n")
                .unwrap()
                .as_primitive::<Int64Type>();
            leaf.is_valid(0).then(|| leaf.value(0))
        };
        assert_eq!(bound(MIN_VALUES), expected_bounds.map(|(min, _)| min));
        assert_eq!(bound(MAX_VALUES), expected_bounds.map(|(_, max)| max));
        assert_eq!(flat_stat::<Int64Type>(&stats, NULL_COUNT, "n"), 0);
        assert!(stats
            .column_by_name(TIGHT_BOUNDS)
            .unwrap()
            .as_boolean()
            .value(0));
    }

    // Every Arrow string type takes its own arm in `compute_leaf_agg`.
    #[rstest::rstest]
    fn test_file_stats_accumulator_string_types_match_single_shot(
        #[values(DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View)] string_type: DataType,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            string_type.clone(),
            true,
        )]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |vals: Vec<Option<&str>>| {
            let array: ArrayRef = match string_type {
                DataType::Utf8 => Arc::new(StringArray::from(vals)),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vals)),
                DataType::Utf8View => Arc::new(StringViewArray::from(vals)),
                ref other => panic!("unexpected string type {other}"),
            };
            RecordBatch::try_new(schema.clone(), vec![array]).unwrap()
        };
        // Long enough to be truncated, so the merge must compose raw values, not truncated ones.
        let long_max = "z".repeat(40);
        let rg1 = vec![Some("apple"), None];
        let rg2 = vec![Some(long_max.as_str()), Some("banana")];

        let mut acc = FileStatsAccumulator::new(&[column_name!("t")], &physical_schema);
        acc.merge(&mk(rg1.clone())).unwrap();
        acc.merge(&mk(rg2.clone())).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        let whole = mk([rg1, rg2].concat());
        let single = collect_stats(&whole, &[column_name!("t")]).unwrap();

        assert_eq!(
            to_json(&merged).unwrap().as_string::<i32>().value(0),
            to_json(&single).unwrap().as_string::<i32>().value(0),
        );
    }

    // A malformed stats tree is a kernel bug, not bad caller input, so the guards report
    // `InternalError`.
    fn assert_internal_error(result: DeltaResult<impl std::fmt::Debug>, needle: &str) {
        let err = result.expect_err("must be rejected");
        // `Error::internal_error` captures a backtrace, which wraps the variant in `Backtraced`.
        let mut variant = &err;
        while let Error::Backtraced { source, .. } = variant {
            variant = source;
        }
        assert!(
            matches!(variant, Error::InternalError(_)),
            "expected an internal error, got: {err}"
        );
        assert!(
            err.to_string().contains(needle),
            "error {err} does not mention {needle}"
        );
    }

    #[test]
    fn test_reduce_stats_rejects_unexpected_field() {
        let stats = StructArray::try_new(
            vec![Field::new("bogusSection", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
            None,
        )
        .unwrap();
        assert_internal_error(reduce_stats(&stats), "unknown stats section");
    }

    #[test]
    fn test_reduce_stats_children_rejects_non_struct() {
        let leaf = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        assert_internal_error(
            reduce_stats_children(&leaf, &reduce_count_leaf),
            "expected struct in stats sub-tree",
        );
    }

    #[test]
    fn test_leaf_reducers_reject_wrong_type() {
        let strings = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
        assert_internal_error(reduce_count_leaf(&strings), "expected Int64 count leaf");
        assert_internal_error(
            reduce_bool_and_leaf(&strings),
            "expected Boolean tightBounds leaf",
        );
    }

    // Neither reducer may invent a value: a null count is unknown, not zero, and a null
    // tightBounds must not be assumed tight.
    #[test]
    fn test_leaf_reducers_reject_null_leaves() {
        let counts = Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef;
        assert_internal_error(reduce_count_leaf(&counts), "null count leaf");
        let tight = Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef;
        assert_internal_error(reduce_bool_and_leaf(&tight), "null tightBounds leaf");
    }

    #[rstest::rstest]
    #[case::all_tight(vec![true, true], true)]
    #[case::one_wide(vec![true, false, true], false)]
    #[case::all_wide(vec![false, false], false)]
    fn test_reduce_bool_and_leaf_ands_across_row_groups(
        #[case] row_groups: Vec<bool>,
        #[case] expected: bool,
    ) {
        let leaf = Arc::new(BooleanArray::from(row_groups)) as ArrayRef;
        let merged = reduce_bool_and_leaf(&leaf).unwrap();
        assert_eq!(merged.as_boolean().value(0), expected);
    }

    fn batch_of(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn i64s(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    // A one-row batch with one struct column `s` whose fields are named in the given order.
    fn nested_batch(field_names: [&str; 2], values: [i64; 2]) -> RecordBatch {
        let fields: Fields = field_names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect();
        let columns = values.iter().map(|v| i64s(&[*v])).collect();
        let struct_array = Arc::new(StructArray::try_new(fields.clone(), columns, None).unwrap());
        batch_of(
            vec![Field::new("s", DataType::Struct(fields), true)],
            vec![struct_array],
        )
    }

    fn i64_list_batch(values: &[i64]) -> RecordBatch {
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let offsets = OffsetBuffer::new(vec![0, values.len() as i32].into());
        let list = ListArray::new(item.clone(), offsets, i64s(values), None);
        batch_of(
            vec![Field::new("c", DataType::List(item), true)],
            vec![Arc::new(list)],
        )
    }

    // Each case differs only in ways the derived stats shape would not catch.
    #[rstest::rstest]
    #[case::reordered_columns(
        batch_of(
            vec![Field::new("a", DataType::Int64, true), Field::new("b", DataType::Int64, true)],
            vec![i64s(&[1, 2]), i64s(&[100, 200])],
        ),
        batch_of(
            vec![Field::new("b", DataType::Int64, true), Field::new("a", DataType::Int64, true)],
            vec![i64s(&[100, 200]), i64s(&[1, 2])],
        ),
        vec![column_name!("a"), column_name!("b")]
    )]
    #[case::reordered_nested_fields(
        nested_batch(["x", "y"], [1, 2]),
        nested_batch(["y", "x"], [3, 4]),
        vec![column_name!("s", "x"), column_name!("s", "y")]
    )]
    #[case::extra_non_stats_column(
        batch_of(vec![Field::new("a", DataType::Int64, true)], vec![i64s(&[1, 2])]),
        batch_of(
            vec![Field::new("a", DataType::Int64, true), Field::new("b", DataType::Utf8, true)],
            vec![i64s(&[3, 4]), Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef],
        ),
        vec![column_name!("a")]
    )]
    #[case::nullability_only(
        batch_of(vec![Field::new("a", DataType::Int64, true)], vec![i64s(&[1, 2])]),
        batch_of(vec![Field::new("a", DataType::Int64, false)], vec![i64s(&[3, 4])]),
        vec![column_name!("a")]
    )]
    // Both derive an Int64 nullCount leaf, but `Null` counts rows while a list counts nulls.
    #[case::void_column_vs_list_column(
        batch_of(
            vec![Field::new("c", DataType::Null, true)],
            vec![Arc::new(NullArray::new(3)) as ArrayRef],
        ),
        i64_list_batch(&[1, 2, 3]),
        vec![column_name!("c")]
    )]
    fn test_file_stats_accumulator_rejects_differing_batch_schema(
        #[case] first: RecordBatch,
        #[case] second: RecordBatch,
        #[case] stats_columns: Vec<ColumnName>,
    ) {
        let physical_schema = StructType::try_from_arrow(first.schema().as_ref()).unwrap();
        let mut acc = FileStatsAccumulator::new(&stats_columns, &physical_schema);
        acc.merge(&first).expect("first merge must succeed");
        let err = acc
            .merge(&second)
            .expect_err("a batch whose schema differs must be rejected");
        assert!(
            matches!(err, Error::Schema(_)),
            "expected a schema error, got: {err}"
        );
        // A failed merge is terminal, so the first row group can never be published on its own.
        assert_result_error_with_message(acc.merge(&first), FAILED);
        let err = acc
            .finish()
            .expect_err("an accumulator that failed a merge must not publish statistics");
        // Its own variant, so callers can match the failure without matching on the message.
        assert!(
            matches!(&err, Error::StatsValidation(msg) if msg == FAILED),
            "expected a stats validation error, got: {err}"
        );
    }

    #[test]
    fn test_file_stats_accumulator_rejects_differing_stats_shape() {
        let declared = nested_batch(["x", "y"], [1, 2]);
        // `with_match_field_names(false)` is the only way to get a schema-equal batch whose nested
        // field names differ.
        let renamed = RecordBatch::try_new_with_options(
            declared.schema(),
            nested_batch(["p", "q"], [3, 4]).columns().to_vec(),
            &RecordBatchOptions::new().with_match_field_names(false),
        )
        .unwrap();
        assert_eq!(declared.schema(), renamed.schema());

        let physical_schema = StructType::try_from_arrow(declared.schema().as_ref()).unwrap();
        let mut acc = FileStatsAccumulator::new(
            &[column_name!("s", "x"), column_name!("s", "y")],
            &physical_schema,
        );
        acc.merge(&declared).expect("first merge must succeed");
        let err = acc
            .merge(&renamed)
            .expect_err("a batch deriving a different stats shape must be rejected");
        assert!(
            matches!(err, Error::Schema(_)),
            "expected a schema error, got: {err}"
        );
        assert_result_error_with_message(acc.finish(), FAILED);
    }

    // Exists to compile: a connector holds the accumulator in its writer struct, so `merge` must be
    // callable through `&mut self`.
    #[test]
    fn test_file_stats_accumulator_usable_as_struct_field() {
        struct Writer {
            acc: FileStatsAccumulator,
        }
        impl Writer {
            fn write_row_group(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
                self.acc.merge(batch)
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mut writer = Writer {
            acc: FileStatsAccumulator::new(&[column_name!("n")], &physical_schema),
        };
        let batch = RecordBatch::try_new(schema, vec![i64s(&[1, 2])]).unwrap();
        writer.write_row_group(&batch).unwrap();
        assert!(writer.acc.finish().unwrap().is_some());
    }

    // Arrow erases interval columns to plain integers, so only the kernel schema can classify them.
    #[rstest::rstest]
    fn test_file_stats_accumulator_interval_column_gets_null_count_only(
        #[values(KernelDataType::INTERVAL_YEAR_MONTH, KernelDataType::INTERVAL_DAY_TIME)]
        interval_type: KernelDataType,
    ) {
        // Year-month intervals are Int32 in Arrow, day-time intervals Int64.
        let arrow_type = if interval_type == KernelDataType::INTERVAL_YEAR_MONTH {
            DataType::Int32
        } else {
            DataType::Int64
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("span", arrow_type.clone(), true),
            Field::new("n", DataType::Int64, true),
        ]));
        let physical_schema = StructType::try_new([
            StructField::nullable("span", interval_type),
            StructField::nullable("n", KernelDataType::LONG),
        ])
        .unwrap();
        let mk = |spans: &[i64], ns: &[i64]| {
            let span: ArrayRef = match arrow_type {
                DataType::Int32 => Arc::new(Int32Array::from(
                    spans.iter().map(|v| *v as i32).collect::<Vec<_>>(),
                )),
                _ => i64s(spans),
            };
            RecordBatch::try_new(schema.clone(), vec![span, i64s(ns)]).unwrap()
        };

        let cols = vec![column_name!("span"), column_name!("n")];
        let mut acc = FileStatsAccumulator::new(&cols, &physical_schema);
        acc.merge(&mk(&[5, 1], &[10, 20])).unwrap();
        acc.merge(&mk(&[9], &[30])).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        assert_eq!(flat_stat::<Int64Type>(&merged, NULL_COUNT, "span"), 0);
        for section in [MIN_VALUES, MAX_VALUES] {
            assert!(
                child_struct(&merged, section)
                    .column_by_name("span")
                    .is_none(),
                "interval column must not appear in {section}"
            );
            // The sibling column still gets bounds, so the exclusion is targeted.
            assert!(child_struct(&merged, section).column_by_name("n").is_some());
        }
    }

    #[test]
    fn test_file_stats_accumulator_nested_string_truncates_and_handles_all_nulls() {
        let inner = Fields::from(vec![
            Field::new("long", DataType::Utf8, true),
            Field::new("empty", DataType::Utf8, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(inner.clone()),
            true,
        )]));
        let physical_schema = StructType::try_from_arrow(schema.as_ref()).unwrap();
        let mk = |long: Vec<&str>| {
            let nulls = vec![None as Option<&str>; long.len()];
            let struct_array = StructArray::try_new(
                inner.clone(),
                vec![
                    Arc::new(StringArray::from(long)) as ArrayRef,
                    Arc::new(StringArray::from(nulls)) as ArrayRef,
                ],
                None,
            )
            .unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array) as ArrayRef]).unwrap()
        };
        // Both exceed the 32-char prefix and the file-level max comes from the second row group, so
        // truncating before reducing would give the wrong bound.
        let short_prefix = format!("{}zzz", "a".repeat(40));
        let long_prefix = format!("{}aaa", "b".repeat(40));
        let cols = vec![column_name!("s", "long"), column_name!("s", "empty")];

        let mut acc = FileStatsAccumulator::new(&cols, &physical_schema);
        acc.merge(&mk(vec![short_prefix.as_str()])).unwrap();
        acc.merge(&mk(vec![long_prefix.as_str()])).unwrap();
        let merged = acc.finish().unwrap().unwrap();

        let whole = mk(vec![short_prefix.as_str(), long_prefix.as_str()]);
        let single = collect_stats(&whole, &cols).unwrap();
        let leaf = |stats: &StructArray, section, field| {
            string_leaf(child_struct(child_struct(stats, section), "s"), field).map(str::to_string)
        };

        assert_eq!(
            leaf(&merged, MIN_VALUES, "long").as_deref(),
            Some("a".repeat(32).as_str())
        );
        assert_eq!(
            leaf(&merged, MAX_VALUES, "long"),
            Some(format!("{}\x7F", "b".repeat(32)))
        );
        assert_eq!(leaf(&merged, MIN_VALUES, "empty"), None);
        assert_eq!(leaf(&merged, MAX_VALUES, "empty"), None);
        assert_eq!(get_stat::<Int64Type>(&merged, NULL_COUNT, "s", "empty"), 2);

        for section in [MIN_VALUES, MAX_VALUES] {
            for field in ["long", "empty"] {
                assert_eq!(
                    leaf(&merged, section, field),
                    leaf(&single, section, field),
                    "{section}.s.{field} must match single-shot collection"
                );
            }
        }
    }
}
