//! Statistics collection for Delta Lake file writes.
//!
//! This module provides `StatisticsCollector` which accumulates statistics
//! across multiple Arrow RecordBatches during file writes.

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Int64Array, LargeStringArray, PrimitiveArray,
    RecordBatch, StringArray, StringViewArray, StructArray,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Field, Fields,
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::{DeltaResult, Error};

// ============================================================================
// Trait-based min/max aggregation
// ============================================================================

/// Trait for min/max aggregation operations.
/// Implementations define whether we're computing min or max.
trait MinMaxAggregator {
    /// Aggregate two values, returning the min or max.
    fn aggregate<T: PartialOrd>(a: T, b: T) -> T;

    /// Aggregate two floats with proper NaN handling.
    fn aggregate_float<T: num_traits::Float>(a: T, b: T) -> T;
}

/// Min aggregation - returns the smaller value.
struct MinAgg;

impl MinMaxAggregator for MinAgg {
    fn aggregate<T: PartialOrd>(a: T, b: T) -> T {
        if a <= b {
            a
        } else {
            b
        }
    }

    fn aggregate_float<T: num_traits::Float>(a: T, b: T) -> T {
        a.min(b)
    }
}

/// Max aggregation - returns the larger value.
struct MaxAgg;

impl MinMaxAggregator for MaxAgg {
    fn aggregate<T: PartialOrd>(a: T, b: T) -> T {
        if a >= b {
            a
        } else {
            b
        }
    }

    fn aggregate_float<T: num_traits::Float>(a: T, b: T) -> T {
        a.max(b)
    }
}

/// Compute aggregation for primitive array types, optionally filtered by mask.
fn aggregate_primitive<T, Agg>(
    array: &PrimitiveArray<T>,
    mask: Option<&NullBuffer>,
) -> Option<T::Native>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    Agg: MinMaxAggregator,
{
    if let Some(m) = mask {
        array
            .iter()
            .enumerate()
            .filter_map(|(i, opt_val)| if m.is_valid(i) { opt_val } else { None })
            .reduce(|acc, val| Agg::aggregate(acc, val))
    } else {
        array
            .iter()
            .flatten()
            .reduce(|acc, val| Agg::aggregate(acc, val))
    }
}

/// Helper to downcast an array reference to a specific type.
fn downcast<T: 'static>(column: &ArrayRef) -> DeltaResult<&T> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::generic(format!(
            "Failed to downcast from {} to {}",
            std::any::type_name_of_val(column.as_ref()),
            std::any::type_name::<T>(),
        ))
    })
}

/// Compute aggregation for a column, returning a single-element array.
/// If mask is provided, only masked-in rows are considered.
fn compute_agg<Agg: MinMaxAggregator>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
) -> DeltaResult<ArrayRef> {
    match column.data_type() {
        DataType::Int8 => agg_primitive::<Int8Type, Agg>(column, mask),
        DataType::Int16 => agg_primitive::<Int16Type, Agg>(column, mask),
        DataType::Int32 => agg_primitive::<Int32Type, Agg>(column, mask),
        DataType::Int64 => agg_primitive::<Int64Type, Agg>(column, mask),
        DataType::UInt8 => agg_primitive::<UInt8Type, Agg>(column, mask),
        DataType::UInt16 => agg_primitive::<UInt16Type, Agg>(column, mask),
        DataType::UInt32 => agg_primitive::<UInt32Type, Agg>(column, mask),
        DataType::UInt64 => agg_primitive::<UInt64Type, Agg>(column, mask),
        DataType::Float32 => agg_float::<Float32Type, Agg>(column, mask),
        DataType::Float64 => agg_float::<Float64Type, Agg>(column, mask),
        DataType::Date32 => agg_primitive::<Date32Type, Agg>(column, mask),
        DataType::Date64 => agg_primitive::<Date64Type, Agg>(column, mask),
        DataType::Timestamp(TimeUnit::Second, tz) => {
            agg_timestamp::<TimestampSecondType, Agg>(column, mask, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            agg_timestamp::<TimestampMillisecondType, Agg>(column, mask, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            agg_timestamp::<TimestampMicrosecondType, Agg>(column, mask, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            agg_timestamp::<TimestampNanosecondType, Agg>(column, mask, tz.clone())
        }
        DataType::Decimal128(p, s) => agg_decimal128::<Agg>(column, mask, *p, *s),
        DataType::Utf8 => agg_string::<Agg>(column, mask),
        DataType::LargeUtf8 => agg_large_string::<Agg>(column, mask),
        DataType::Utf8View => agg_string_view::<Agg>(column, mask),
        // Types without meaningful min/max stats
        _ => Ok(new_null_array(column.data_type(), 1)),
    }
}

/// Aggregate primitive types (integers, dates).
fn agg_primitive<T, Agg>(column: &ArrayRef, mask: Option<&NullBuffer>) -> DeltaResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    Agg: MinMaxAggregator,
    PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    let array = downcast::<PrimitiveArray<T>>(column)?;
    let result = aggregate_primitive::<T, Agg>(array, mask);
    Ok(Arc::new(PrimitiveArray::<T>::from(vec![result])))
}

/// Aggregate float types with NaN handling.
fn agg_float<T, Agg>(column: &ArrayRef, mask: Option<&NullBuffer>) -> DeltaResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::Float,
    Agg: MinMaxAggregator,
    PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    let array = downcast::<PrimitiveArray<T>>(column)?;
    let result = if let Some(m) = mask {
        array
            .iter()
            .enumerate()
            .filter_map(|(i, opt_val)| if m.is_valid(i) { opt_val } else { None })
            .reduce(|acc, val| Agg::aggregate_float(acc, val))
    } else {
        array
            .iter()
            .flatten()
            .reduce(|acc, val| Agg::aggregate_float(acc, val))
    };
    Ok(Arc::new(PrimitiveArray::<T>::from(vec![result])))
}

/// Aggregate timestamp types, preserving timezone.
fn agg_timestamp<T, Agg>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
    tz: Option<Arc<str>>,
) -> DeltaResult<ArrayRef>
where
    T: crate::arrow::datatypes::ArrowTimestampType,
    Agg: MinMaxAggregator,
    PrimitiveArray<T>: From<Vec<Option<i64>>>,
{
    let array = downcast::<PrimitiveArray<T>>(column)?;
    let result = aggregate_primitive::<T, Agg>(array, mask);
    Ok(Arc::new(
        PrimitiveArray::<T>::from(vec![result]).with_timezone_opt(tz),
    ))
}

/// Aggregate Decimal128 types, preserving precision and scale.
fn agg_decimal128<Agg: MinMaxAggregator>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
    precision: u8,
    scale: i8,
) -> DeltaResult<ArrayRef> {
    use crate::arrow::array::Decimal128Array;
    let array = downcast::<Decimal128Array>(column)?;
    let result = aggregate_primitive::<Decimal128Type, Agg>(array, mask);
    let arr = Decimal128Array::from(vec![result])
        .with_precision_and_scale(precision, scale)
        .map_err(|e| Error::generic(format!("Invalid decimal precision/scale: {e}")))?;
    Ok(Arc::new(arr))
}

/// Aggregate Utf8 strings with truncation.
fn agg_string<Agg: MinMaxAggregator>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
) -> DeltaResult<ArrayRef> {
    let array = downcast::<StringArray>(column)?;
    let result = if let Some(m) = mask {
        array
            .iter()
            .enumerate()
            .filter_map(|(i, opt_val)| if m.is_valid(i) { opt_val } else { None })
            .reduce(|acc, val| Agg::aggregate(acc, val))
            .map(truncate_string)
    } else {
        array
            .iter()
            .flatten()
            .reduce(|acc, val| Agg::aggregate(acc, val))
            .map(truncate_string)
    };
    Ok(Arc::new(StringArray::from(vec![result])))
}

/// Aggregate LargeUtf8 strings with truncation.
fn agg_large_string<Agg: MinMaxAggregator>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
) -> DeltaResult<ArrayRef> {
    let array = downcast::<LargeStringArray>(column)?;
    let result = if let Some(m) = mask {
        array
            .iter()
            .enumerate()
            .filter_map(|(i, opt_val)| if m.is_valid(i) { opt_val } else { None })
            .reduce(|acc, val| Agg::aggregate(acc, val))
            .map(truncate_string)
    } else {
        array
            .iter()
            .flatten()
            .reduce(|acc, val| Agg::aggregate(acc, val))
            .map(truncate_string)
    };
    Ok(Arc::new(LargeStringArray::from(vec![result])))
}

/// Aggregate StringView with truncation.
fn agg_string_view<Agg: MinMaxAggregator>(
    column: &ArrayRef,
    mask: Option<&NullBuffer>,
) -> DeltaResult<ArrayRef> {
    let array = downcast::<StringViewArray>(column)?;
    let result: Option<String> = if let Some(m) = mask {
        array
            .iter()
            .enumerate()
            .filter_map(|(i, opt_val)| if m.is_valid(i) { opt_val } else { None })
            .map(|s| s.to_string())
            .reduce(|acc, val| {
                if Agg::aggregate(acc.as_str(), val.as_str()) == acc.as_str() {
                    acc
                } else {
                    val
                }
            })
            .map(|s| truncate_string(&s))
    } else {
        array
            .iter()
            .flatten()
            .map(|s| s.to_string())
            .reduce(|acc, val| {
                if Agg::aggregate(acc.as_str(), val.as_str()) == acc.as_str() {
                    acc
                } else {
                    val
                }
            })
            .map(|s| truncate_string(&s))
    };
    Ok(Arc::new(StringViewArray::from(vec![result])))
}

/// Truncate a string to the max prefix length for stats.
fn truncate_string(s: &str) -> String {
    s.chars().take(STRING_PREFIX_LENGTH).collect()
}

/// Compare two single-element arrays and select min or max.
fn compare_and_select<Agg: MinMaxAggregator>(a: &ArrayRef, b: &ArrayRef) -> DeltaResult<ArrayRef> {
    match a.data_type() {
        DataType::Int8 => compare_primitive::<Int8Type, Agg>(a, b),
        DataType::Int16 => compare_primitive::<Int16Type, Agg>(a, b),
        DataType::Int32 => compare_primitive::<Int32Type, Agg>(a, b),
        DataType::Int64 => compare_primitive::<Int64Type, Agg>(a, b),
        DataType::UInt8 => compare_primitive::<UInt8Type, Agg>(a, b),
        DataType::UInt16 => compare_primitive::<UInt16Type, Agg>(a, b),
        DataType::UInt32 => compare_primitive::<UInt32Type, Agg>(a, b),
        DataType::UInt64 => compare_primitive::<UInt64Type, Agg>(a, b),
        DataType::Date32 => compare_primitive::<Date32Type, Agg>(a, b),
        DataType::Date64 => compare_primitive::<Date64Type, Agg>(a, b),
        DataType::Float32 => compare_float::<Float32Type, Agg>(a, b),
        DataType::Float64 => compare_float::<Float64Type, Agg>(a, b),
        DataType::Utf8 => compare_string::<Agg>(a, b),
        _ => Ok(a.clone()), // Fallback
    }
}

fn compare_primitive<T, Agg>(a: &ArrayRef, b: &ArrayRef) -> DeltaResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    Agg: MinMaxAggregator,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let a_arr = downcast::<PrimitiveArray<T>>(a)?;
    let b_arr = downcast::<PrimitiveArray<T>>(b)?;
    let result = Agg::aggregate(a_arr.value(0), b_arr.value(0));
    Ok(Arc::new(PrimitiveArray::<T>::from(vec![result])))
}

fn compare_float<T, Agg>(a: &ArrayRef, b: &ArrayRef) -> DeltaResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::Float,
    Agg: MinMaxAggregator,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let a_arr = downcast::<PrimitiveArray<T>>(a)?;
    let b_arr = downcast::<PrimitiveArray<T>>(b)?;
    let result = Agg::aggregate_float(a_arr.value(0), b_arr.value(0));
    Ok(Arc::new(PrimitiveArray::<T>::from(vec![result])))
}

fn compare_string<Agg: MinMaxAggregator>(a: &ArrayRef, b: &ArrayRef) -> DeltaResult<ArrayRef> {
    let a_arr = downcast::<StringArray>(a)?;
    let b_arr = downcast::<StringArray>(b)?;
    let result = Agg::aggregate(a_arr.value(0), b_arr.value(0));
    Ok(Arc::new(StringArray::from(vec![result])))
}

/// Maximum prefix length for string statistics (Delta protocol requirement).
const STRING_PREFIX_LENGTH: usize = 32;

/// Collects statistics from RecordBatches for Delta Lake file statistics.
/// Supports streaming accumulation across multiple batches.
#[allow(dead_code)]
pub(crate) struct StatisticsCollector {
    /// Total number of records across all batches.
    num_records: i64,
    /// Column names from the data schema.
    column_names: Vec<String>,
    /// Column names that should have stats collected.
    stats_columns: HashSet<String>,
    /// Null counts per column.
    null_counts: Vec<ArrayRef>,
    /// Min values per column (single-element arrays).
    min_values: Vec<ArrayRef>,
    /// Max values per column (single-element arrays).
    max_values: Vec<ArrayRef>,
}

#[allow(dead_code)]
impl StatisticsCollector {
    /// Create a new statistics collector.
    ///
    /// # Arguments
    /// * `data_schema` - The Arrow schema of the data being written
    /// * `stats_columns` - Column names that should have statistics collected
    pub(crate) fn new(
        data_schema: Arc<crate::arrow::datatypes::Schema>,
        stats_columns: &[String],
    ) -> DeltaResult<Self> {
        let stats_set: HashSet<String> = stats_columns.iter().cloned().collect();

        let mut column_names = Vec::with_capacity(data_schema.fields().len());
        let mut null_counts = Vec::with_capacity(data_schema.fields().len());
        let mut min_values = Vec::with_capacity(data_schema.fields().len());
        let mut max_values = Vec::with_capacity(data_schema.fields().len());

        for field in data_schema.fields() {
            column_names.push(field.name().clone());
            null_counts.push(Self::create_zero_null_count(field.data_type())?);
            let null_array = Self::create_null_array(field.data_type());
            min_values.push(null_array.clone());
            max_values.push(null_array);
        }

        Ok(Self {
            num_records: 0,
            column_names,
            stats_columns: stats_set,
            null_counts,
            min_values,
            max_values,
        })
    }

    /// Check if a column should have stats collected.
    fn should_collect_stats(&self, column_name: &str) -> bool {
        self.stats_columns.contains(column_name)
    }

    /// Create a zero-initialized null count structure for the given data type.
    fn create_zero_null_count(data_type: &DataType) -> DeltaResult<ArrayRef> {
        match data_type {
            DataType::Struct(fields) => {
                let children: Vec<ArrayRef> = fields
                    .iter()
                    .map(|f| Self::create_zero_null_count(f.data_type()))
                    .collect::<DeltaResult<_>>()?;
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| {
                        let child_type = Self::null_count_data_type(f.data_type());
                        Field::new(f.name(), child_type, true)
                    })
                    .collect();
                Ok(Arc::new(
                    StructArray::try_new(null_count_fields, children, None).map_err(|e| {
                        Error::generic(format!("Failed to create null count struct: {e}"))
                    })?,
                ))
            }
            _ => Ok(Arc::new(Int64Array::from(vec![0i64]))),
        }
    }

    /// Get the data type for null counts of a given data type.
    fn null_count_data_type(data_type: &DataType) -> DataType {
        match data_type {
            DataType::Struct(fields) => {
                let null_count_fields: Vec<Field> = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                DataType::Struct(null_count_fields.into())
            }
            _ => DataType::Int64,
        }
    }

    /// Compute null counts for a column, respecting the optional mask.
    fn compute_null_counts(column: &ArrayRef, mask: Option<&NullBuffer>) -> DeltaResult<ArrayRef> {
        match column.data_type() {
            DataType::Struct(fields) => {
                let struct_array = column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| Error::generic("Expected StructArray for struct column"))?;
                let children: Vec<ArrayRef> = (0..fields.len())
                    .map(|i| Self::compute_null_counts(struct_array.column(i), mask))
                    .collect::<DeltaResult<Vec<_>>>()?;
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Ok(Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .map_err(|e| Error::generic(format!("null count struct: {e}")))?,
                ))
            }
            _ => {
                let null_count = match mask {
                    Some(m) => {
                        // Count nulls only for masked-in rows
                        (0..column.len())
                            .filter(|&i| m.is_valid(i) && column.is_null(i))
                            .count() as i64
                    }
                    None => column.null_count() as i64,
                };
                Ok(Arc::new(Int64Array::from(vec![null_count])))
            }
        }
    }

    /// Merge two null count structures by adding them together.
    fn merge_null_counts(existing: &ArrayRef, new: &ArrayRef) -> DeltaResult<ArrayRef> {
        match existing.data_type() {
            DataType::Struct(fields) => {
                let existing_struct =
                    existing
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            Error::generic("Expected StructArray for existing null count")
                        })?;
                let new_struct = new
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| Error::generic("Expected StructArray for new null count"))?;

                let children: Vec<ArrayRef> = (0..fields.len())
                    .map(|i| {
                        Self::merge_null_counts(existing_struct.column(i), new_struct.column(i))
                    })
                    .collect::<DeltaResult<_>>()?;

                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Ok(Arc::new(
                    StructArray::try_new(null_count_fields, children, None).map_err(|e| {
                        Error::generic(format!("Failed to merge null count struct: {e}"))
                    })?,
                ))
            }
            _ => {
                let existing_val = existing
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| Error::generic("Expected Int64Array for existing null count"))?
                    .value(0);
                let new_val = new
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| Error::generic("Expected Int64Array for new null count"))?
                    .value(0);
                Ok(Arc::new(Int64Array::from(vec![existing_val + new_val])))
            }
        }
    }

    /// Create a null array of the given type with length 1.
    fn create_null_array(data_type: &DataType) -> ArrayRef {
        new_null_array(data_type, 1)
    }

    /// Compute min/max for a column, returning (min, max) as single-element arrays.
    /// If mask is provided, only masked-in rows are considered.
    fn compute_min_max(
        column: &ArrayRef,
        mask: Option<&NullBuffer>,
    ) -> DeltaResult<(ArrayRef, ArrayRef)> {
        let min_val = compute_agg::<MinAgg>(column, mask)?;
        let max_val = compute_agg::<MaxAgg>(column, mask)?;
        Ok((min_val, max_val))
    }

    /// Merge min values, keeping the smaller one.
    fn merge_min(existing: &ArrayRef, new: &ArrayRef) -> DeltaResult<ArrayRef> {
        if existing.is_null(0) {
            return Ok(new.clone());
        }
        if new.is_null(0) {
            return Ok(existing.clone());
        }
        let new_min = compute_agg::<MinAgg>(new, None)?;
        let existing_min = compute_agg::<MinAgg>(existing, None)?;
        compare_and_select::<MinAgg>(&existing_min, &new_min)
    }

    /// Merge max values, keeping the larger one.
    fn merge_max(existing: &ArrayRef, new: &ArrayRef) -> DeltaResult<ArrayRef> {
        if existing.is_null(0) {
            return Ok(new.clone());
        }
        if new.is_null(0) {
            return Ok(existing.clone());
        }
        let new_max = compute_agg::<MaxAgg>(new, None)?;
        let existing_max = compute_agg::<MaxAgg>(existing, None)?;
        compare_and_select::<MaxAgg>(&existing_max, &new_max)
    }

    /// Update statistics with data from a RecordBatch.
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch to accumulate statistics from
    /// * `mask` - Optional mask indicating which rows to include (true = include)
    ///           Used for deletion vector support where masked-out rows should not
    ///           contribute to statistics.
    pub(crate) fn update(
        &mut self,
        batch: &RecordBatch,
        mask: Option<&NullBuffer>,
    ) -> DeltaResult<()> {
        // Count rows, respecting mask if present
        let row_count = match mask {
            Some(m) => m.iter().filter(|&valid| valid).count() as i64,
            None => batch.num_rows() as i64,
        };
        self.num_records += row_count;

        for (col_idx, column) in batch.columns().iter().enumerate() {
            let col_name = &self.column_names[col_idx];
            if self.should_collect_stats(col_name) {
                // Update null counts
                let batch_null_counts = Self::compute_null_counts(column, mask)?;
                self.null_counts[col_idx] =
                    Self::merge_null_counts(&self.null_counts[col_idx], &batch_null_counts)?;

                // Update min/max
                let (batch_min, batch_max) = Self::compute_min_max(column, mask)?;
                self.min_values[col_idx] = Self::merge_min(&self.min_values[col_idx], &batch_min)?;
                self.max_values[col_idx] = Self::merge_max(&self.max_values[col_idx], &batch_max)?;
            }
        }

        Ok(())
    }

    /// Finalize and return the collected statistics as a StructArray.
    pub(crate) fn finalize(&self) -> DeltaResult<StructArray> {
        let mut fields = Vec::new();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        // numRecords
        fields.push(Field::new("numRecords", DataType::Int64, true));
        arrays.push(Arc::new(Int64Array::from(vec![self.num_records])));

        // nullCount - nested struct matching data schema
        let null_count_fields: Vec<Field> = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(_, name)| self.should_collect_stats(name))
            .map(|(idx, name)| Field::new(name, self.null_counts[idx].data_type().clone(), true))
            .collect();

        if !null_count_fields.is_empty() {
            let null_count_arrays: Vec<ArrayRef> = self
                .column_names
                .iter()
                .enumerate()
                .filter(|(_, name)| self.should_collect_stats(name))
                .map(|(idx, _)| self.null_counts[idx].clone())
                .collect();

            let null_count_struct =
                StructArray::try_new(null_count_fields.into(), null_count_arrays, None)
                    .map_err(|e| Error::generic(format!("Failed to create nullCount: {e}")))?;

            fields.push(Field::new(
                "nullCount",
                null_count_struct.data_type().clone(),
                true,
            ));
            arrays.push(Arc::new(null_count_struct));
        }

        // minValues - nested struct with min values
        let min_fields: Vec<Field> = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(idx, name)| {
                self.should_collect_stats(name) && !self.min_values[*idx].is_null(0)
            })
            .map(|(idx, name)| Field::new(name, self.min_values[idx].data_type().clone(), true))
            .collect();

        if !min_fields.is_empty() {
            let min_arrays: Vec<ArrayRef> = self
                .column_names
                .iter()
                .enumerate()
                .filter(|(idx, name)| {
                    self.should_collect_stats(name) && !self.min_values[*idx].is_null(0)
                })
                .map(|(idx, _)| self.min_values[idx].clone())
                .collect();

            let min_struct = StructArray::try_new(min_fields.into(), min_arrays, None)
                .map_err(|e| Error::generic(format!("Failed to create minValues: {e}")))?;

            fields.push(Field::new(
                "minValues",
                min_struct.data_type().clone(),
                true,
            ));
            arrays.push(Arc::new(min_struct));
        }

        // maxValues - nested struct with max values
        let max_fields: Vec<Field> = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(idx, name)| {
                self.should_collect_stats(name) && !self.max_values[*idx].is_null(0)
            })
            .map(|(idx, name)| Field::new(name, self.max_values[idx].data_type().clone(), true))
            .collect();

        if !max_fields.is_empty() {
            let max_arrays: Vec<ArrayRef> = self
                .column_names
                .iter()
                .enumerate()
                .filter(|(idx, name)| {
                    self.should_collect_stats(name) && !self.max_values[*idx].is_null(0)
                })
                .map(|(idx, _)| self.max_values[idx].clone())
                .collect();

            let max_struct = StructArray::try_new(max_fields.into(), max_arrays, None)
                .map_err(|e| Error::generic(format!("Failed to create maxValues: {e}")))?;

            fields.push(Field::new(
                "maxValues",
                max_struct.data_type().clone(),
                true,
            ));
            arrays.push(Arc::new(max_struct));
        }

        // tightBounds
        fields.push(Field::new("tightBounds", DataType::Boolean, true));
        arrays.push(Arc::new(BooleanArray::from(vec![true])));

        StructArray::try_new(fields.into(), arrays, None)
            .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array, StringArray};
    use crate::arrow::datatypes::Schema;

    #[test]
    fn test_statistics_collector_single_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]).unwrap();
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

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
    fn test_statistics_collector_null_counts() {
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

        let mut collector =
            StatisticsCollector::new(schema, &["id".to_string(), "value".to_string()]).unwrap();
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

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
    fn test_statistics_collector_multiple_batches_null_counts() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![None, None, Some("b")]))],
        )
        .unwrap();

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]).unwrap();
        collector.update(&batch1, None).unwrap();
        collector.update(&batch2, None).unwrap();
        let stats = collector.finalize().unwrap();

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
        // 1 null in batch1 + 2 nulls in batch2 = 3 total
        assert_eq!(value_null_count.value(0), 3);
    }

    #[test]
    fn test_statistics_collector_respects_stats_columns() {
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

        // Only collect stats for "id", not "value"
        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]).unwrap();
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

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
    fn test_statistics_collector_min_max() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
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

        let mut collector =
            StatisticsCollector::new(schema, &["number".to_string(), "name".to_string()]).unwrap();
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

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
    fn test_statistics_collector_min_max_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![5, 10, 3]))],
        )
        .unwrap();

        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 8]))])
                .unwrap();

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]).unwrap();
        collector.update(&batch1, None).unwrap();
        collector.update(&batch2, None).unwrap();
        let stats = collector.finalize().unwrap();

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
        assert_eq!(value_min.value(0), 1); // min across both batches

        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let value_max = max_values
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_max.value(0), 10); // max across both batches
    }

    #[test]
    fn test_statistics_collector_with_mask() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        // Batch with values [1, 2, 3, 4, 5]
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ]))],
        )
        .unwrap();

        // Mask: only include rows 1, 3 (values 2, 4)
        let mask = NullBuffer::from(vec![false, true, false, true, false]);

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]).unwrap();
        collector.update(&batch, Some(&mask)).unwrap();
        let stats = collector.finalize().unwrap();

        // numRecords should be 2 (only masked-in rows)
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 2);

        // min should be 2, max should be 4
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
        assert_eq!(value_min.value(0), 2);

        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let value_max = max_values
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_max.value(0), 4);
    }

    #[test]
    fn test_statistics_collector_with_mask_null_count() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        // Batch with values [1, null, 3, null, 5]
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                None,
                Some(5),
            ]))],
        )
        .unwrap();

        // Mask: include rows 0, 1, 2 (values 1, null, 3)
        let mask = NullBuffer::from(vec![true, true, true, false, false]);

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]).unwrap();
        collector.update(&batch, Some(&mask)).unwrap();
        let stats = collector.finalize().unwrap();

        // nullCount should be 1 (only the null at index 1 is in masked rows)
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
        assert_eq!(value_null_count.value(0), 1);
    }
}
