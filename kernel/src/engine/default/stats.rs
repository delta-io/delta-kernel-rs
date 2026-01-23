//! Statistics collection for Delta Lake file writes.
//!
//! This module provides `StatisticsCollector` which accumulates statistics (min, max, null count)
//! across multiple Arrow RecordBatches during file writes.
//!
//! Type coverage matches Shinkansen's stats module for parity.
//! Supports optional mask for deletion vector handling (masked rows are excluded from stats).

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    LargeStringArray, RecordBatch, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::compute::{max, min};
use crate::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use crate::{DeltaResult, Error};

/// Collects statistics from RecordBatches for Delta Lake file statistics.
/// Supports streaming accumulation across multiple batches.
///
/// Only collects statistics for columns specified in `stats_columns`.
pub(crate) struct StatisticsCollector {
    num_records: usize,
    /// Column names from the data schema, indexed by column position.
    column_names: Vec<String>,
    /// Column names that should have stats collected.
    stats_columns: HashSet<String>,
    /// Collected min values per column (single-element arrays).
    min_values: Vec<ArrayRef>,
    /// Collected max values per column (single-element arrays).
    max_values: Vec<ArrayRef>,
    /// Null counts per column. For structs, this is a StructArray with nested Int64Arrays.
    null_counts: Vec<ArrayRef>,
}

impl StatisticsCollector {
    /// Create a new statistics collector.
    ///
    /// Only collects statistics for columns in `stats_columns`.
    pub(crate) fn new(
        data_schema: Arc<crate::arrow::datatypes::Schema>,
        stats_columns: &[String],
    ) -> Self {
        let num_columns = data_schema.fields().len();
        let stats_columns: HashSet<String> = stats_columns.iter().cloned().collect();

        // Initialize with null/zero values for each column
        let mut column_names = Vec::with_capacity(num_columns);
        let mut min_values = Vec::with_capacity(num_columns);
        let mut max_values = Vec::with_capacity(num_columns);
        let mut null_counts = Vec::with_capacity(num_columns);

        for field in data_schema.fields() {
            let col_name = field.name().to_string();
            column_names.push(col_name.clone());

            // Initialize arrays for all columns (placeholders for non-stats columns)
            let null_array = Self::create_null_array(field.data_type());
            min_values.push(null_array.clone());
            max_values.push(null_array);
            null_counts.push(Self::create_zero_null_count(field.data_type()));
        }

        Self {
            num_records: 0,
            column_names,
            stats_columns,
            min_values,
            max_values,
            null_counts,
        }
    }

    /// Check if a column should have stats collected.
    fn should_collect_stats(&self, column_name: &str) -> bool {
        self.stats_columns.contains(column_name)
    }

    /// Create a zero-initialized null count structure for the given data type.
    /// For leaf types, returns Int64Array([0]). For structs, returns nested structure.
    fn create_zero_null_count(data_type: &DataType) -> ArrayRef {
        match data_type {
            DataType::Struct(fields) => {
                let children: Vec<ArrayRef> = fields
                    .iter()
                    .map(|f| Self::create_zero_null_count(f.data_type()))
                    .collect();
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| {
                        let child_type = Self::null_count_data_type(f.data_type());
                        Field::new(f.name(), child_type, true)
                    })
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .unwrap_or_else(|_| StructArray::new_empty_fields(1, None)),
                ) as ArrayRef
            }
            _ => Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
        }
    }

    /// Get the data type for null count of a given data type.
    /// Leaf types -> Int64, Structs -> nested struct with Int64 leaves.
    fn null_count_data_type(data_type: &DataType) -> DataType {
        match data_type {
            DataType::Struct(fields) => {
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                DataType::Struct(null_count_fields)
            }
            _ => DataType::Int64,
        }
    }

    /// Combine an external mask with a column's own null buffer.
    /// Returns a mask where both must be valid for a row to be considered valid.
    fn combine_masks(external_mask: Option<&NullBuffer>, column: &ArrayRef) -> Option<NullBuffer> {
        match (external_mask, column.nulls()) {
            (Some(ext), Some(own)) => {
                let combined = (0..column.len())
                    .map(|i| ext.is_valid(i) && own.is_valid(i))
                    .collect::<Vec<_>>();
                Some(NullBuffer::from(combined))
            }
            (Some(ext), None) => Some(ext.clone()),
            (None, Some(own)) => Some(own.clone()),
            (None, None) => None,
        }
    }

    /// Compute null counts for a column, recursively for nested structs.
    /// If mask is provided, only count nulls in rows where mask is valid.
    fn compute_null_counts(column: &ArrayRef, mask: Option<&NullBuffer>) -> ArrayRef {
        match column.data_type() {
            DataType::Struct(fields) => {
                let Some(struct_arr) = column.as_any().downcast_ref::<StructArray>() else {
                    return Arc::new(StructArray::new_empty_fields(1, None)) as ArrayRef;
                };

                // Combine external mask with struct's own null buffer
                let combined_mask = Self::combine_masks(mask, column);

                // Count struct-level nulls (considering mask)
                let struct_nulls = if let Some(ref mask) = combined_mask {
                    (0..column.len())
                        .filter(|i| mask.is_valid(*i) && column.is_null(*i))
                        .count() as i64
                } else {
                    column.null_count() as i64
                };

                let children: Vec<ArrayRef> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let child_col = struct_arr.column(i);
                        // Pass combined mask to children
                        let child_counts =
                            Self::compute_null_counts(child_col, combined_mask.as_ref());
                        // Add struct-level nulls to each child's count
                        Self::add_to_null_count(&child_counts, struct_nulls, field.data_type())
                    })
                    .collect();

                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .unwrap_or_else(|_| StructArray::new_empty_fields(1, None)),
                ) as ArrayRef
            }
            _ => {
                // Leaf field - count nulls (considering mask)
                let null_count = if let Some(mask) = mask {
                    (0..column.len())
                        .filter(|i| mask.is_valid(*i) && column.is_null(*i))
                        .count() as i64
                } else {
                    column.null_count() as i64
                };
                Arc::new(Int64Array::from(vec![null_count])) as ArrayRef
            }
        }
    }

    /// Add a value to all leaf null counts in a null count structure.
    fn add_to_null_count(null_count: &ArrayRef, add: i64, data_type: &DataType) -> ArrayRef {
        if add == 0 {
            return null_count.clone();
        }
        match data_type {
            DataType::Struct(fields) => {
                let Some(struct_arr) = null_count.as_any().downcast_ref::<StructArray>() else {
                    return null_count.clone();
                };
                let children: Vec<ArrayRef> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        Self::add_to_null_count(struct_arr.column(i), add, field.data_type())
                    })
                    .collect();
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .unwrap_or_else(|_| StructArray::new_empty_fields(1, None)),
                ) as ArrayRef
            }
            _ => {
                let Some(arr) = null_count.as_any().downcast_ref::<Int64Array>() else {
                    return null_count.clone();
                };
                Arc::new(Int64Array::from(vec![arr.value(0) + add])) as ArrayRef
            }
        }
    }

    /// Merge two null count structures by adding corresponding values.
    fn merge_null_counts(current: &ArrayRef, new_val: &ArrayRef, data_type: &DataType) -> ArrayRef {
        match data_type {
            DataType::Struct(fields) => {
                let (Some(struct_a), Some(struct_b)) = (
                    current.as_any().downcast_ref::<StructArray>(),
                    new_val.as_any().downcast_ref::<StructArray>(),
                ) else {
                    return current.clone();
                };
                let children: Vec<ArrayRef> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        Self::merge_null_counts(
                            struct_a.column(i),
                            struct_b.column(i),
                            field.data_type(),
                        )
                    })
                    .collect();
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .unwrap_or_else(|_| StructArray::new_empty_fields(1, None)),
                ) as ArrayRef
            }
            _ => {
                let (Some(arr_a), Some(arr_b)) = (
                    current.as_any().downcast_ref::<Int64Array>(),
                    new_val.as_any().downcast_ref::<Int64Array>(),
                ) else {
                    return current.clone();
                };
                Arc::new(Int64Array::from(vec![arr_a.value(0) + arr_b.value(0)])) as ArrayRef
            }
        }
    }

    /// Update statistics with a new batch. Call this for each batch during write.
    ///
    /// If `mask` is provided, only rows where mask[i] is valid (true) are included in stats.
    /// This is used for deletion vector support where masked-out rows should be excluded.
    pub(crate) fn update(
        &mut self,
        batch: &RecordBatch,
        mask: Option<&NullBuffer>,
    ) -> DeltaResult<()> {
        // Count valid rows (considering mask)
        let valid_rows = if let Some(mask) = mask {
            (0..batch.num_rows()).filter(|i| mask.is_valid(*i)).count()
        } else {
            batch.num_rows()
        };
        self.num_records += valid_rows;

        for i in 0..batch.num_columns() {
            let column = batch.column(i);
            let column_name = &self.column_names[i];

            // Only collect stats for columns in stats_columns
            if !self.should_collect_stats(column_name) {
                continue;
            }

            // Accumulate null counts (with mask)
            let batch_null_counts = Self::compute_null_counts(column, mask);
            self.null_counts[i] = Self::merge_null_counts(
                &self.null_counts[i],
                &batch_null_counts,
                column.data_type(),
            );

            // Check if column has any non-null values (considering mask)
            let has_non_null = if let Some(mask) = mask {
                (0..column.len()).any(|idx| mask.is_valid(idx) && !column.is_null(idx))
            } else {
                column.null_count() < column.len()
            };

            // Update min/max if column has non-null values and type supports it
            if has_non_null && !column.is_empty() {
                match Self::compute_min_max(column, mask) {
                    Ok((batch_min, batch_max)) => {
                        self.min_values[i] =
                            Self::merge_min(&self.min_values[i], &batch_min, column.data_type())?;
                        self.max_values[i] =
                            Self::merge_max(&self.max_values[i], &batch_max, column.data_type())?;
                    }
                    Err(_) => {
                        // Type not supported for min/max - keep existing (null) values
                    }
                }
            }
        }

        Ok(())
    }

    /// Merge two min values, returning the smaller one.
    fn merge_min(
        current: &ArrayRef,
        new_val: &ArrayRef,
        data_type: &DataType,
    ) -> DeltaResult<ArrayRef> {
        // If current is null, use new value
        if current.null_count() == current.len() {
            return Ok(new_val.clone());
        }
        // If new is null, keep current
        if new_val.null_count() == new_val.len() {
            return Ok(current.clone());
        }

        // Compare and return the smaller value
        Self::compare_and_select(current, new_val, data_type, true)
    }

    /// Merge two max values, returning the larger one.
    fn merge_max(
        current: &ArrayRef,
        new_val: &ArrayRef,
        data_type: &DataType,
    ) -> DeltaResult<ArrayRef> {
        // If current is null, use new value
        if current.null_count() == current.len() {
            return Ok(new_val.clone());
        }
        // If new is null, keep current
        if new_val.null_count() == new_val.len() {
            return Ok(current.clone());
        }

        // Compare and return the larger value
        Self::compare_and_select(current, new_val, data_type, false)
    }

    /// Compare two single-element arrays and return the min (if select_min=true) or max.
    /// Supports all types that compute_min_max supports.
    fn compare_and_select(
        a: &ArrayRef,
        b: &ArrayRef,
        data_type: &DataType,
        select_min: bool,
    ) -> DeltaResult<ArrayRef> {
        macro_rules! compare_primitive {
            ($array_type:ty, $a:expr, $b:expr, $select_min:expr) => {{
                let arr_a = $a.as_any().downcast_ref::<$array_type>().unwrap();
                let arr_b = $b.as_any().downcast_ref::<$array_type>().unwrap();
                let val_a = arr_a.value(0);
                let val_b = arr_b.value(0);
                if $select_min {
                    if val_a <= val_b {
                        Ok($a.clone())
                    } else {
                        Ok($b.clone())
                    }
                } else if val_a >= val_b {
                    Ok($a.clone())
                } else {
                    Ok($b.clone())
                }
            }};
        }

        /// For float comparisons we need special handling for ordering
        macro_rules! compare_float {
            ($array_type:ty, $a:expr, $b:expr, $select_min:expr) => {{
                let arr_a = $a.as_any().downcast_ref::<$array_type>().unwrap();
                let arr_b = $b.as_any().downcast_ref::<$array_type>().unwrap();
                let val_a = arr_a.value(0);
                let val_b = arr_b.value(0);
                if $select_min {
                    if val_a.min(val_b) == val_a {
                        Ok($a.clone())
                    } else {
                        Ok($b.clone())
                    }
                } else if val_a.max(val_b) == val_a {
                    Ok($a.clone())
                } else {
                    Ok($b.clone())
                }
            }};
        }

        match data_type {
            // Signed integers
            DataType::Int8 => compare_primitive!(Int8Array, a, b, select_min),
            DataType::Int16 => compare_primitive!(Int16Array, a, b, select_min),
            DataType::Int32 => compare_primitive!(Int32Array, a, b, select_min),
            DataType::Int64 => compare_primitive!(Int64Array, a, b, select_min),

            // Unsigned integers
            DataType::UInt8 => compare_primitive!(UInt8Array, a, b, select_min),
            DataType::UInt16 => compare_primitive!(UInt16Array, a, b, select_min),
            DataType::UInt32 => compare_primitive!(UInt32Array, a, b, select_min),
            DataType::UInt64 => compare_primitive!(UInt64Array, a, b, select_min),

            // Floating point - use special float comparison
            DataType::Float16 => compare_float!(Float16Array, a, b, select_min),
            DataType::Float32 => compare_float!(Float32Array, a, b, select_min),
            DataType::Float64 => compare_float!(Float64Array, a, b, select_min),

            // Boolean
            DataType::Boolean => compare_primitive!(BooleanArray, a, b, select_min),

            // Dates
            DataType::Date32 => compare_primitive!(Date32Array, a, b, select_min),
            DataType::Date64 => compare_primitive!(Date64Array, a, b, select_min),

            // Time32
            DataType::Time32(TimeUnit::Second) => {
                compare_primitive!(Time32SecondArray, a, b, select_min)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                compare_primitive!(Time32MillisecondArray, a, b, select_min)
            }
            DataType::Time32(_) => Ok(a.clone()),

            // Time64
            DataType::Time64(TimeUnit::Microsecond) => {
                compare_primitive!(Time64MicrosecondArray, a, b, select_min)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                compare_primitive!(Time64NanosecondArray, a, b, select_min)
            }
            DataType::Time64(_) => Ok(a.clone()),

            // Timestamps - all time units
            DataType::Timestamp(TimeUnit::Second, _) => {
                compare_primitive!(TimestampSecondArray, a, b, select_min)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                compare_primitive!(TimestampMillisecondArray, a, b, select_min)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                compare_primitive!(TimestampMicrosecondArray, a, b, select_min)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                compare_primitive!(TimestampNanosecondArray, a, b, select_min)
            }

            // Durations
            DataType::Duration(TimeUnit::Second) => {
                compare_primitive!(DurationSecondArray, a, b, select_min)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                compare_primitive!(DurationMillisecondArray, a, b, select_min)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                compare_primitive!(DurationMicrosecondArray, a, b, select_min)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                compare_primitive!(DurationNanosecondArray, a, b, select_min)
            }

            // Intervals
            DataType::Interval(IntervalUnit::YearMonth) => {
                compare_primitive!(IntervalYearMonthArray, a, b, select_min)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                compare_primitive!(IntervalDayTimeArray, a, b, select_min)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                compare_primitive!(IntervalMonthDayNanoArray, a, b, select_min)
            }

            // Decimals
            DataType::Decimal128(_, _) => compare_primitive!(Decimal128Array, a, b, select_min),
            DataType::Decimal256(_, _) => compare_primitive!(Decimal256Array, a, b, select_min),

            // Strings
            DataType::Utf8 => {
                let (Some(arr_a), Some(arr_b)) = (
                    a.as_any().downcast_ref::<StringArray>(),
                    b.as_any().downcast_ref::<StringArray>(),
                ) else {
                    return Ok(a.clone());
                };
                let val_a = arr_a.value(0);
                let val_b = arr_b.value(0);
                if select_min {
                    if val_a <= val_b {
                        Ok(a.clone())
                    } else {
                        Ok(b.clone())
                    }
                } else if val_a >= val_b {
                    Ok(a.clone())
                } else {
                    Ok(b.clone())
                }
            }
            DataType::LargeUtf8 => {
                let (Some(arr_a), Some(arr_b)) = (
                    a.as_any().downcast_ref::<LargeStringArray>(),
                    b.as_any().downcast_ref::<LargeStringArray>(),
                ) else {
                    return Ok(a.clone());
                };
                let val_a = arr_a.value(0);
                let val_b = arr_b.value(0);
                if select_min {
                    if val_a <= val_b {
                        Ok(a.clone())
                    } else {
                        Ok(b.clone())
                    }
                } else if val_a >= val_b {
                    Ok(a.clone())
                } else {
                    Ok(b.clone())
                }
            }
            DataType::Utf8View => {
                let (Some(arr_a), Some(arr_b)) = (
                    a.as_any().downcast_ref::<StringViewArray>(),
                    b.as_any().downcast_ref::<StringViewArray>(),
                ) else {
                    return Ok(a.clone());
                };
                let val_a = arr_a.value(0);
                let val_b = arr_b.value(0);
                if select_min {
                    if val_a <= val_b {
                        Ok(a.clone())
                    } else {
                        Ok(b.clone())
                    }
                } else if val_a >= val_b {
                    Ok(a.clone())
                } else {
                    Ok(b.clone())
                }
            }

            // Struct - recursive
            DataType::Struct(fields) => {
                // Recursively merge each child field
                let (Some(struct_a), Some(struct_b)) = (
                    a.as_any().downcast_ref::<StructArray>(),
                    b.as_any().downcast_ref::<StructArray>(),
                ) else {
                    return Ok(a.clone());
                };

                let merged_children: Vec<ArrayRef> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let child_a = struct_a.column(i);
                        let child_b = struct_b.column(i);
                        if select_min {
                            Self::merge_min(child_a, child_b, field.data_type())
                        } else {
                            Self::merge_max(child_a, child_b, field.data_type())
                        }
                    })
                    .collect::<DeltaResult<Vec<_>>>()?;

                Ok(
                    Arc::new(StructArray::try_new(fields.clone(), merged_children, None)?)
                        as ArrayRef,
                )
            }
            _ => Ok(a.clone()), // Unsupported type, keep current
        }
    }

    /// Create a null array with a single null value of the given data type.
    /// Uses arrow's `new_null_array` for automatic type handling, like Shinkansen.
    fn create_null_array(data_type: &DataType) -> ArrayRef {
        crate::arrow::array::new_null_array(data_type, 1)
    }

    /// Default prefix length for string truncation in statistics.
    const STRING_PREFIX_LENGTH: usize = 32;

    /// Truncate a string to the prefix length for statistics.
    fn truncate_string(s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else {
            // Truncate at char boundary
            s.char_indices()
                .take_while(|(i, _)| *i < max_len)
                .map(|(_, c)| c)
                .collect()
        }
    }

    /// Compute min and max values for a column.
    /// Supports all types that Shinkansen supports for parity.
    /// If mask is provided, only rows where mask is valid are considered.
    fn compute_min_max(
        column: &ArrayRef,
        mask: Option<&NullBuffer>,
    ) -> DeltaResult<(ArrayRef, ArrayRef)> {
        use crate::arrow::array::Array;
        use crate::arrow::compute::{max_boolean, max_string, min_boolean, min_string};

        /// Helper macro for primitive types with mask support (types that implement Ord)
        macro_rules! compute_minmax_primitive_masked {
            ($array_type:ty, $column:expr, $mask:expr) => {{
                let arr = $column
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .ok_or_else(|| {
                        Error::generic(concat!("Failed to downcast ", stringify!($array_type)))
                    })?;
                let (min_val, max_val) = if let Some(mask) = $mask {
                    // Filter by mask
                    let filtered = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, opt_val)| if mask.is_valid(i) { opt_val } else { None })
                        .collect::<Vec<_>>();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                (
                    Arc::new(<$array_type>::from(vec![min_val])) as ArrayRef,
                    Arc::new(<$array_type>::from(vec![max_val])) as ArrayRef,
                )
            }};
        }

        /// Helper macro for float types with mask support (use PartialOrd via reduce)
        macro_rules! compute_minmax_float_masked {
            ($array_type:ty, $column:expr, $mask:expr) => {{
                let arr = $column
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .ok_or_else(|| {
                        Error::generic(concat!("Failed to downcast ", stringify!($array_type)))
                    })?;
                let (min_val, max_val) = if let Some(mask) = $mask {
                    // Filter by mask
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, opt_val)| if mask.is_valid(i) { opt_val } else { None })
                        .collect();
                    (
                        filtered.iter().copied().reduce(|a, b| a.min(b)),
                        filtered.iter().copied().reduce(|a, b| a.max(b)),
                    )
                } else {
                    (min(arr), max(arr))
                };
                (
                    Arc::new(<$array_type>::from(vec![min_val])) as ArrayRef,
                    Arc::new(<$array_type>::from(vec![max_val])) as ArrayRef,
                )
            }};
        }

        // Use arrow compute functions based on the data type
        let (min_scalar, max_scalar) = match column.data_type() {
            // Signed integers
            DataType::Int8 => compute_minmax_primitive_masked!(Int8Array, column, mask),
            DataType::Int16 => compute_minmax_primitive_masked!(Int16Array, column, mask),
            DataType::Int32 => compute_minmax_primitive_masked!(Int32Array, column, mask),
            DataType::Int64 => compute_minmax_primitive_masked!(Int64Array, column, mask),

            // Unsigned integers
            DataType::UInt8 => compute_minmax_primitive_masked!(UInt8Array, column, mask),
            DataType::UInt16 => compute_minmax_primitive_masked!(UInt16Array, column, mask),
            DataType::UInt32 => compute_minmax_primitive_masked!(UInt32Array, column, mask),
            DataType::UInt64 => compute_minmax_primitive_masked!(UInt64Array, column, mask),

            // Floating point - use special handling for f16/f32/f64 (PartialOrd, not Ord)
            DataType::Float16 => compute_minmax_float_masked!(Float16Array, column, mask),
            DataType::Float32 => compute_minmax_float_masked!(Float32Array, column, mask),
            DataType::Float64 => compute_minmax_float_masked!(Float64Array, column, mask),

            // Dates
            DataType::Date32 => compute_minmax_primitive_masked!(Date32Array, column, mask),
            DataType::Date64 => compute_minmax_primitive_masked!(Date64Array, column, mask),

            // Time32
            DataType::Time32(TimeUnit::Second) => {
                compute_minmax_primitive_masked!(Time32SecondArray, column, mask)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                compute_minmax_primitive_masked!(Time32MillisecondArray, column, mask)
            }
            DataType::Time32(_) => {
                return Err(Error::generic("Unsupported Time32 unit"));
            }

            // Time64
            DataType::Time64(TimeUnit::Microsecond) => {
                compute_minmax_primitive_masked!(Time64MicrosecondArray, column, mask)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                compute_minmax_primitive_masked!(Time64NanosecondArray, column, mask)
            }
            DataType::Time64(_) => {
                return Err(Error::generic("Unsupported Time64 unit"));
            }

            // Timestamps - handle all time units with mask
            DataType::Timestamp(TimeUnit::Second, tz) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast TimestampSecondArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                let min_arr = TimestampSecondArray::from(vec![min_val]);
                let max_arr = TimestampSecondArray::from(vec![max_val]);
                if let Some(tz) = tz {
                    (
                        Arc::new(min_arr.with_timezone(tz.as_ref())) as ArrayRef,
                        Arc::new(max_arr.with_timezone(tz.as_ref())) as ArrayRef,
                    )
                } else {
                    (Arc::new(min_arr) as ArrayRef, Arc::new(max_arr) as ArrayRef)
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        Error::generic("Failed to downcast TimestampMillisecondArray")
                    })?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                let min_arr = TimestampMillisecondArray::from(vec![min_val]);
                let max_arr = TimestampMillisecondArray::from(vec![max_val]);
                if let Some(tz) = tz {
                    (
                        Arc::new(min_arr.with_timezone(tz.as_ref())) as ArrayRef,
                        Arc::new(max_arr.with_timezone(tz.as_ref())) as ArrayRef,
                    )
                } else {
                    (Arc::new(min_arr) as ArrayRef, Arc::new(max_arr) as ArrayRef)
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        Error::generic("Failed to downcast TimestampMicrosecondArray")
                    })?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                let min_arr = TimestampMicrosecondArray::from(vec![min_val]);
                let max_arr = TimestampMicrosecondArray::from(vec![max_val]);
                if let Some(tz) = tz {
                    (
                        Arc::new(min_arr.with_timezone(tz.as_ref())) as ArrayRef,
                        Arc::new(max_arr.with_timezone(tz.as_ref())) as ArrayRef,
                    )
                } else {
                    (Arc::new(min_arr) as ArrayRef, Arc::new(max_arr) as ArrayRef)
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast TimestampNanosecondArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                let min_arr = TimestampNanosecondArray::from(vec![min_val]);
                let max_arr = TimestampNanosecondArray::from(vec![max_val]);
                if let Some(tz) = tz {
                    (
                        Arc::new(min_arr.with_timezone(tz.as_ref())) as ArrayRef,
                        Arc::new(max_arr.with_timezone(tz.as_ref())) as ArrayRef,
                    )
                } else {
                    (Arc::new(min_arr) as ArrayRef, Arc::new(max_arr) as ArrayRef)
                }
            }

            // Durations
            DataType::Duration(TimeUnit::Second) => {
                compute_minmax_primitive_masked!(DurationSecondArray, column, mask)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                compute_minmax_primitive_masked!(DurationMillisecondArray, column, mask)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                compute_minmax_primitive_masked!(DurationMicrosecondArray, column, mask)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                compute_minmax_primitive_masked!(DurationNanosecondArray, column, mask)
            }

            // Intervals
            DataType::Interval(IntervalUnit::YearMonth) => {
                compute_minmax_primitive_masked!(IntervalYearMonthArray, column, mask)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                compute_minmax_primitive_masked!(IntervalDayTimeArray, column, mask)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                compute_minmax_primitive_masked!(IntervalMonthDayNanoArray, column, mask)
            }

            // Decimals with mask
            DataType::Decimal128(precision, scale) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| Error::generic("Failed to downcast Decimal128Array"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                (
                    Arc::new(
                        Decimal128Array::from(vec![min_val])
                            .with_precision_and_scale(*precision, *scale)
                            .unwrap_or_else(|_| Decimal128Array::from(vec![min_val])),
                    ) as ArrayRef,
                    Arc::new(
                        Decimal128Array::from(vec![max_val])
                            .with_precision_and_scale(*precision, *scale)
                            .unwrap_or_else(|_| Decimal128Array::from(vec![max_val])),
                    ) as ArrayRef,
                )
            }
            DataType::Decimal256(precision, scale) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| Error::generic("Failed to downcast Decimal256Array"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min(arr), max(arr))
                };
                (
                    Arc::new(
                        Decimal256Array::from(vec![min_val])
                            .with_precision_and_scale(*precision, *scale)
                            .unwrap_or_else(|_| Decimal256Array::from(vec![min_val])),
                    ) as ArrayRef,
                    Arc::new(
                        Decimal256Array::from(vec![max_val])
                            .with_precision_and_scale(*precision, *scale)
                            .unwrap_or_else(|_| Decimal256Array::from(vec![max_val])),
                    ) as ArrayRef,
                )
            }

            // Strings - multiple variants with mask
            DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast StringArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied().map(|s| s.to_string()),
                        filtered.iter().max().copied().map(|s| s.to_string()),
                    )
                } else {
                    (
                        min_string(arr).map(|s| s.to_string()),
                        max_string(arr).map(|s| s.to_string()),
                    )
                };
                // Truncate strings to prefix length for statistics
                let min_truncated =
                    min_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                let max_truncated =
                    max_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                (
                    Arc::new(StringArray::from(vec![min_truncated])) as ArrayRef,
                    Arc::new(StringArray::from(vec![max_truncated])) as ArrayRef,
                )
            }
            DataType::LargeUtf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast LargeStringArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied().map(|s| s.to_string()),
                        filtered.iter().max().copied().map(|s| s.to_string()),
                    )
                } else {
                    (
                        arr.iter().flatten().min().map(|s| s.to_string()),
                        arr.iter().flatten().max().map(|s| s.to_string()),
                    )
                };
                let min_truncated =
                    min_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                let max_truncated =
                    max_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                (
                    Arc::new(LargeStringArray::from(vec![min_truncated])) as ArrayRef,
                    Arc::new(LargeStringArray::from(vec![max_truncated])) as ArrayRef,
                )
            }
            DataType::Utf8View => {
                let arr = column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast StringViewArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied().map(|s| s.to_string()),
                        filtered.iter().max().copied().map(|s| s.to_string()),
                    )
                } else {
                    (
                        arr.iter().flatten().min().map(|s| s.to_string()),
                        arr.iter().flatten().max().map(|s| s.to_string()),
                    )
                };
                let min_truncated =
                    min_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                let max_truncated =
                    max_val.map(|s| Self::truncate_string(&s, Self::STRING_PREFIX_LENGTH));
                (
                    Arc::new(StringViewArray::from(vec![min_truncated])) as ArrayRef,
                    Arc::new(StringViewArray::from(vec![max_truncated])) as ArrayRef,
                )
            }

            // Boolean with mask
            DataType::Boolean => {
                let arr = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast BooleanArray"))?;
                let (min_val, max_val) = if let Some(mask) = mask {
                    let filtered: Vec<_> = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| if mask.is_valid(i) { v } else { None })
                        .collect();
                    (
                        filtered.iter().min().copied(),
                        filtered.iter().max().copied(),
                    )
                } else {
                    (min_boolean(arr), max_boolean(arr))
                };
                (
                    Arc::new(BooleanArray::from(vec![min_val])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![max_val])) as ArrayRef,
                )
            }

            // Struct - recursive with combined mask
            DataType::Struct(fields) => {
                // Recursively compute min/max for each child field
                let struct_arr = column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| Error::generic("Failed to downcast StructArray"))?;

                // Combine external mask with struct's own nulls
                let combined_mask = Self::combine_masks(mask, column);

                let mut min_children = Vec::with_capacity(fields.len());
                let mut max_children = Vec::with_capacity(fields.len());

                for (i, field) in fields.iter().enumerate() {
                    let child_col = struct_arr.column(i);
                    match Self::compute_min_max(child_col, combined_mask.as_ref()) {
                        Ok((child_min, child_max)) => {
                            min_children.push(child_min);
                            max_children.push(child_max);
                        }
                        Err(_) => {
                            // Child type not supported - use null
                            let null_array = Self::create_null_array(field.data_type());
                            min_children.push(null_array.clone());
                            max_children.push(null_array);
                        }
                    }
                }

                (
                    Arc::new(StructArray::try_new(fields.clone(), min_children, None)?) as ArrayRef,
                    Arc::new(StructArray::try_new(fields.clone(), max_children, None)?) as ArrayRef,
                )
            }

            // Types that don't support min/max (like Shinkansen)
            DataType::Null
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _) => {
                // These types don't support min/max - return nulls
                return Err(Error::generic(format!(
                    "Data type {:?} does not support min/max statistics",
                    column.data_type()
                )));
            }

            _ => {
                return Err(Error::generic(format!(
                    "Unsupported data type for min/max: {:?}",
                    column.data_type()
                )));
            }
        };

        Ok((min_scalar, max_scalar))
    }

    /// Get the number of records accumulated.
    pub(crate) fn num_records(&self) -> usize {
        self.num_records
    }

    /// Finalize and build the statistics struct matching the expected Delta Lake stats schema.
    ///
    /// Only includes columns that appear in stats_columns.
    pub(crate) fn finalize(&self) -> DeltaResult<StructArray> {
        // Collect stats data - only include columns in stats_columns
        let stats_data: Vec<_> = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(_, name)| self.should_collect_stats(name))
            .map(|(i, name)| {
                (
                    name.clone(),
                    self.null_counts[i].clone(),
                    self.min_values[i].clone(),
                    self.max_values[i].clone(),
                )
            })
            .collect();

        // Build the complete stats struct
        let mut stats_fields_vec = vec![Field::new("numRecords", DataType::Int64, true)];
        let mut stats_arrays: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from(vec![self.num_records as i64]))];

        if !stats_data.is_empty() {
            // Build nullCount, minValues, maxValues from stats_data
            let mut null_count_fields = Vec::new();
            let mut null_count_values = Vec::new();
            let mut min_fields = Vec::new();
            let mut min_values = Vec::new();
            let mut max_fields = Vec::new();
            let mut max_values = Vec::new();

            for (name, null_count, min_val, max_val) in stats_data {
                // Always add null count
                null_count_fields.push(Field::new(&name, null_count.data_type().clone(), true));
                null_count_values.push(null_count);

                // Only add min/max if not all nulls (type supported it)
                if min_val.null_count() < min_val.len() {
                    min_fields.push(Field::new(&name, min_val.data_type().clone(), true));
                    max_fields.push(Field::new(&name, max_val.data_type().clone(), true));
                    min_values.push(min_val);
                    max_values.push(max_val);
                }
            }

            // Add nullCount
            let null_count_struct =
                StructArray::try_new(Fields::from(null_count_fields), null_count_values, None)?;
            stats_fields_vec.push(Field::new(
                "nullCount",
                DataType::Struct(null_count_struct.fields().clone()),
                true,
            ));
            stats_arrays.push(Arc::new(null_count_struct));

            // Add minValues/maxValues if any columns support it
            if !min_fields.is_empty() {
                let min_struct = StructArray::try_new(Fields::from(min_fields), min_values, None)?;
                let max_struct = StructArray::try_new(Fields::from(max_fields), max_values, None)?;

                stats_fields_vec.push(Field::new(
                    "minValues",
                    DataType::Struct(min_struct.fields().clone()),
                    true,
                ));
                stats_fields_vec.push(Field::new(
                    "maxValues",
                    DataType::Struct(max_struct.fields().clone()),
                    true,
                ));
                stats_arrays.push(Arc::new(min_struct));
                stats_arrays.push(Arc::new(max_struct));
            }
        }

        // tightBounds indicates whether stats are accurate (true) or potentially stale due to DVs (false).
        // For newly written files, tightBounds is always true because:
        // 1. We're computing stats from the actual data being written
        // 2. New files have no deletion vectors applied yet
        // If reading stats from files with DVs, tightBounds would need to be false.
        stats_fields_vec.push(Field::new("tightBounds", DataType::Boolean, true));
        stats_arrays.push(Arc::new(BooleanArray::from(vec![true])));

        let stats_array = StructArray::try_new(Fields::from(stats_fields_vec), stats_arrays, None)?;

        Ok(stats_array)
    }
}

/// Verifies that computed statistics match the expected stats schema.
///
/// This is used to validate stats after `write_parquet_file` to ensure the engine
/// produced stats that match the kernel's expected schema from `Transaction::stats_schema()`.
pub(crate) struct StatsVerifier;

impl StatsVerifier {
    /// Verify that computed stats match the expected schema structure.
    ///
    /// Returns Ok(()) if stats are valid, or an error describing the mismatch.
    ///
    /// # Arguments
    /// * `stats` - The computed stats StructArray from StatisticsCollector
    /// * `expected_columns` - Column names that should have stats (from Transaction::stats_columns())
    pub(crate) fn verify(
        stats: &StructArray,
        expected_columns: &[String],
    ) -> DeltaResult<StatsVerificationResult> {
        use crate::arrow::array::Array;

        let mut result = StatsVerificationResult::default();

        // 1. Verify numRecords exists and is Int64
        let num_records = stats
            .column_by_name("numRecords")
            .ok_or_else(|| Error::generic("Stats missing required field 'numRecords'"))?;
        if num_records.data_type() != &DataType::Int64 {
            return Err(Error::generic(format!(
                "numRecords has wrong type: expected Int64, got {:?}",
                num_records.data_type()
            )));
        }
        let num_records_arr = num_records
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::generic("Failed to downcast numRecords"))?;
        result.num_records = num_records_arr.value(0);

        // 2. Verify tightBounds exists and is Boolean
        let tight_bounds = stats
            .column_by_name("tightBounds")
            .ok_or_else(|| Error::generic("Stats missing required field 'tightBounds'"))?;
        if tight_bounds.data_type() != &DataType::Boolean {
            return Err(Error::generic(format!(
                "tightBounds has wrong type: expected Boolean, got {:?}",
                tight_bounds.data_type()
            )));
        }
        let tight_bounds_arr = tight_bounds
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| Error::generic("Failed to downcast tightBounds"))?;
        result.tight_bounds = tight_bounds_arr.value(0);

        // 3. Verify nullCount struct exists and has expected columns
        if let Some(null_count) = stats.column_by_name("nullCount") {
            let null_count_struct = null_count
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::generic("nullCount is not a struct"))?;

            for col_name in expected_columns {
                if null_count_struct.column_by_name(col_name).is_none() {
                    result.missing_null_count_columns.push(col_name.clone());
                } else {
                    result.present_null_count_columns.push(col_name.clone());
                }
            }
        }

        // 4. Verify minValues/maxValues structs (optional - only for types that support it)
        if let Some(min_values) = stats.column_by_name("minValues") {
            if let Some(min_struct) = min_values.as_any().downcast_ref::<StructArray>() {
                for field in min_struct.fields() {
                    result.min_max_columns.push(field.name().clone());
                }
            }
        }

        if let Some(max_values) = stats.column_by_name("maxValues") {
            if let Some(max_struct) = max_values.as_any().downcast_ref::<StructArray>() {
                // Verify min and max have same columns
                for field in max_struct.fields() {
                    if !result.min_max_columns.contains(field.name()) {
                        return Err(Error::generic(format!(
                            "maxValues has column '{}' not in minValues",
                            field.name()
                        )));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Verify stats and return detailed validation info for debugging.
    pub(crate) fn verify_detailed(
        stats: &StructArray,
        expected_columns: &[String],
    ) -> DeltaResult<String> {
        let result = Self::verify(stats, expected_columns)?;
        Ok(format!(
            "Stats Verification:\n\
             - numRecords: {}\n\
             - tightBounds: {}\n\
             - nullCount columns: {:?}\n\
             - min/max columns: {:?}\n\
             - missing nullCount for: {:?}",
            result.num_records,
            result.tight_bounds,
            result.present_null_count_columns,
            result.min_max_columns,
            result.missing_null_count_columns
        ))
    }
}

/// Result of stats verification.
#[derive(Debug, Default)]
pub(crate) struct StatsVerificationResult {
    pub num_records: i64,
    pub tight_bounds: bool,
    pub present_null_count_columns: Vec<String>,
    pub missing_null_count_columns: Vec<String>,
    pub min_max_columns: Vec<String>,
}

impl StatsVerificationResult {
    /// Returns true if all expected columns have nullCount stats.
    pub fn has_all_null_counts(&self) -> bool {
        self.missing_null_count_columns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Array;
    use crate::arrow::datatypes::Schema;

    #[test]
    fn test_statistics_collector_single_batch() {
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
            StatisticsCollector::new(schema, &["id".to_string(), "value".to_string()]);
        collector.update(&batch, None).unwrap();

        assert_eq!(collector.num_records(), 3);

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.len(), 1); // Single row of stats
        assert_eq!(stats.num_columns(), 5); // numRecords, nullCount, minValues, maxValues, tightBounds
    }

    #[test]
    fn test_statistics_collector_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![4, 5]))])
                .unwrap();

        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]);
        collector.update(&batch1, None).unwrap();
        collector.update(&batch2, None).unwrap();

        assert_eq!(collector.num_records(), 5);
    }

    #[test]
    fn test_statistics_collector_respects_stats_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("included", DataType::Int64, false),
            Field::new("excluded", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Only collect stats for "included" column
        let mut collector = StatisticsCollector::new(schema, &["included".to_string()]);
        collector.update(&batch, None).unwrap();

        let stats = collector.finalize().unwrap();

        // Should have numRecords, nullCount (with "included"), minValues, maxValues, tightBounds
        // nullCount, minValues, maxValues should only have "included" field
        assert_eq!(stats.num_columns(), 5);
    }

    #[test]
    fn test_statistics_collector_with_mask() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));

        // Create batch: [1, 2, 3, 4, 5] with [10, null, 30, 40, 50]
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    None,
                    Some(30),
                    Some(40),
                    Some(50),
                ])),
            ],
        )
        .unwrap();

        // Mask: [true, false, true, false, true] - rows 0, 2, 4 are valid
        let mask = NullBuffer::from(vec![true, false, true, false, true]);

        let mut collector =
            StatisticsCollector::new(schema, &["id".to_string(), "value".to_string()]);
        collector.update(&batch, Some(&mask)).unwrap();

        // Only 3 valid rows (0, 2, 4)
        assert_eq!(collector.num_records(), 3);

        let stats = collector.finalize().unwrap();

        // Verify numRecords
        let num_records = stats.column_by_name("numRecords").unwrap();
        let num_records_arr = num_records.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(num_records_arr.value(0), 3);

        // Verify min/max for id: should be min=1, max=5 (from rows 0, 2, 4)
        let min_values = stats.column_by_name("minValues").unwrap();
        let min_struct = min_values.as_any().downcast_ref::<StructArray>().unwrap();
        let id_min = min_struct
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_min.value(0), 1);

        let max_values = stats.column_by_name("maxValues").unwrap();
        let max_struct = max_values.as_any().downcast_ref::<StructArray>().unwrap();
        let id_max = max_struct
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_max.value(0), 5);

        // Verify min/max for value: should be min=10, max=50 (from rows 0, 2, 4)
        // Row 1 (value=null) is masked out anyway
        let value_min = min_struct
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_min.value(0), 10);

        let value_max = max_struct
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_max.value(0), 50);

        // Verify null counts: value should have 0 nulls (row 1 is masked out)
        let null_counts = stats.column_by_name("nullCount").unwrap();
        let null_struct = null_counts.as_any().downcast_ref::<StructArray>().unwrap();
        let value_null_count = null_struct
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_null_count.value(0), 0); // Row with null is masked out
    }

    #[test]
    fn test_statistics_collector_mask_all_filtered() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Mask all rows out
        let mask = NullBuffer::from(vec![false, false, false]);

        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]);
        collector.update(&batch, Some(&mask)).unwrap();

        // No valid rows
        assert_eq!(collector.num_records(), 0);

        let stats = collector.finalize().unwrap();
        let num_records = stats.column_by_name("numRecords").unwrap();
        let num_records_arr = num_records.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(num_records_arr.value(0), 0);
    }

    #[test]
    fn test_stats_verifier_valid_stats() {
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
            StatisticsCollector::new(schema, &["id".to_string(), "value".to_string()]);
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

        // Verify the stats
        let result = StatsVerifier::verify(&stats, &["id".to_string(), "value".to_string()])
            .expect("Stats should be valid");

        assert_eq!(result.num_records, 3);
        assert!(result.tight_bounds);
        assert!(result.has_all_null_counts());
        assert!(result
            .present_null_count_columns
            .contains(&"id".to_string()));
        assert!(result
            .present_null_count_columns
            .contains(&"value".to_string()));
        // Both columns support min/max
        assert!(result.min_max_columns.contains(&"id".to_string()));
        assert!(result.min_max_columns.contains(&"value".to_string()));
    }

    #[test]
    fn test_stats_verifier_detailed_output() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]);
        collector.update(&batch, None).unwrap();
        let stats = collector.finalize().unwrap();

        // Get detailed output
        let detailed = StatsVerifier::verify_detailed(&stats, &["id".to_string()])
            .expect("Should produce detailed output");

        assert!(detailed.contains("numRecords: 3"));
        assert!(detailed.contains("tightBounds: true"));
        assert!(detailed.contains("id"));
    }
}
