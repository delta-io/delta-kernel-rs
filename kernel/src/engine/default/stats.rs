//! Statistics collection for Delta Lake file writes.
//!
//! This module provides `StatisticsCollector` which accumulates statistics
//! across multiple Arrow RecordBatches during file writes.

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    RecordBatch, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::compute::{filter, max, min};
use crate::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use crate::{DeltaResult, Error};

/// Maximum prefix length for string statistics (Delta protocol requirement).
const STRING_PREFIX_LENGTH: usize = 32;

/// Collects statistics from RecordBatches for Delta Lake file statistics.
/// Supports streaming accumulation across multiple batches.
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

impl StatisticsCollector {
    /// Create a new statistics collector.
    ///
    /// # Arguments
    /// * `data_schema` - The Arrow schema of the data being written
    /// * `stats_columns` - Column names that should have statistics collected
    pub(crate) fn new(
        data_schema: Arc<crate::arrow::datatypes::Schema>,
        stats_columns: &[String],
    ) -> Self {
        let stats_set: HashSet<String> = stats_columns.iter().cloned().collect();

        let mut column_names = Vec::with_capacity(data_schema.fields().len());
        let mut null_counts = Vec::with_capacity(data_schema.fields().len());
        let mut min_values = Vec::with_capacity(data_schema.fields().len());
        let mut max_values = Vec::with_capacity(data_schema.fields().len());

        for field in data_schema.fields() {
            column_names.push(field.name().clone());
            null_counts.push(Self::create_zero_null_count(field.data_type()));
            let null_array = Self::create_null_array(field.data_type());
            min_values.push(null_array.clone());
            max_values.push(null_array);
        }

        Self {
            num_records: 0,
            column_names,
            stats_columns: stats_set,
            null_counts,
            min_values,
            max_values,
        }
    }

    /// Check if a column should have stats collected.
    fn should_collect_stats(&self, column_name: &str) -> bool {
        self.stats_columns.contains(column_name)
    }

    /// Create a zero-initialized null count structure for the given data type.
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
                        .expect("Failed to create null count struct"),
                )
            }
            _ => Arc::new(Int64Array::from(vec![0i64])),
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
                let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
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
    fn merge_null_counts(existing: &ArrayRef, new: &ArrayRef) -> ArrayRef {
        match existing.data_type() {
            DataType::Struct(fields) => {
                let existing_struct = existing.as_any().downcast_ref::<StructArray>().unwrap();
                let new_struct = new.as_any().downcast_ref::<StructArray>().unwrap();

                let children: Vec<ArrayRef> = (0..fields.len())
                    .map(|i| {
                        Self::merge_null_counts(existing_struct.column(i), new_struct.column(i))
                    })
                    .collect();

                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .expect("Failed to merge null count struct"),
                )
            }
            _ => {
                let existing_val = existing
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                let new_val = new.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
                Arc::new(Int64Array::from(vec![existing_val + new_val]))
            }
        }
    }

    /// Create a null array of the given type with length 1.
    fn create_null_array(data_type: &DataType) -> ArrayRef {
        new_null_array(data_type, 1)
    }

    /// Truncate a string to the max prefix length for stats.
    fn truncate_string(s: &str) -> String {
        s.chars().take(STRING_PREFIX_LENGTH).collect()
    }

    /// Compute min/max for a column, returning (min, max) as single-element arrays.
    /// If mask is provided, only masked-in rows are considered.
    fn compute_min_max(
        column: &ArrayRef,
        mask: Option<&NullBuffer>,
    ) -> DeltaResult<(ArrayRef, ArrayRef)> {
        // Apply mask if provided by filtering the array
        let filtered_column = match mask {
            Some(m) => {
                let bool_array = BooleanArray::from(
                    (0..column.len())
                        .map(|i| Some(m.is_valid(i)))
                        .collect::<Vec<_>>(),
                );
                filter(column.as_ref(), &bool_array)
                    .map_err(|e| Error::generic(format!("Failed to filter column: {e}")))?
            }
            None => column.clone(),
        };

        macro_rules! compute_minmax_primitive {
            ($array_type:ty, $column:expr) => {{
                let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(<$array_type>::from(vec![min_val])) as ArrayRef,
                    Arc::new(<$array_type>::from(vec![max_val])) as ArrayRef,
                ))
            }};
        }

        macro_rules! compute_minmax_float {
            ($array_type:ty, $column:expr) => {{
                let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
                // Use arrow's min/max which handle NaN correctly
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(<$array_type>::from(vec![min_val])) as ArrayRef,
                    Arc::new(<$array_type>::from(vec![max_val])) as ArrayRef,
                ))
            }};
        }

        match filtered_column.data_type() {
            DataType::Int8 => compute_minmax_primitive!(Int8Array, filtered_column),
            DataType::Int16 => compute_minmax_primitive!(Int16Array, filtered_column),
            DataType::Int32 => compute_minmax_primitive!(Int32Array, filtered_column),
            DataType::Int64 => compute_minmax_primitive!(Int64Array, filtered_column),
            DataType::UInt8 => compute_minmax_primitive!(UInt8Array, filtered_column),
            DataType::UInt16 => compute_minmax_primitive!(UInt16Array, filtered_column),
            DataType::UInt32 => compute_minmax_primitive!(UInt32Array, filtered_column),
            DataType::UInt64 => compute_minmax_primitive!(UInt64Array, filtered_column),
            DataType::Float32 => compute_minmax_float!(Float32Array, filtered_column),
            DataType::Float64 => compute_minmax_float!(Float64Array, filtered_column),
            DataType::Date32 => compute_minmax_primitive!(Date32Array, filtered_column),
            DataType::Date64 => compute_minmax_primitive!(Date64Array, filtered_column),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(
                        TimestampSecondArray::from(vec![min_val]).with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                    Arc::new(
                        TimestampSecondArray::from(vec![max_val]).with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                ))
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(
                        TimestampMillisecondArray::from(vec![min_val])
                            .with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                    Arc::new(
                        TimestampMillisecondArray::from(vec![max_val])
                            .with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                ))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![min_val])
                            .with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![max_val])
                            .with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                ))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(
                        TimestampNanosecondArray::from(vec![min_val]).with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                    Arc::new(
                        TimestampNanosecondArray::from(vec![max_val]).with_timezone_opt(tz.clone()),
                    ) as ArrayRef,
                ))
            }
            DataType::Decimal128(p, s) => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let min_val = min(array);
                let max_val = max(array);
                Ok((
                    Arc::new(
                        Decimal128Array::from(vec![min_val])
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    ) as ArrayRef,
                    Arc::new(
                        Decimal128Array::from(vec![max_val])
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    ) as ArrayRef,
                ))
            }
            DataType::Utf8 => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let min_val =
                    crate::arrow::compute::min_string(array).map(|s| Self::truncate_string(s));
                let max_val =
                    crate::arrow::compute::max_string(array).map(|s| Self::truncate_string(s));
                Ok((
                    Arc::new(StringArray::from(vec![min_val])) as ArrayRef,
                    Arc::new(StringArray::from(vec![max_val])) as ArrayRef,
                ))
            }
            DataType::LargeUtf8 => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap();
                let min_val =
                    crate::arrow::compute::min_string(array).map(|s| Self::truncate_string(s));
                let max_val =
                    crate::arrow::compute::max_string(array).map(|s| Self::truncate_string(s));
                Ok((
                    Arc::new(LargeStringArray::from(vec![min_val])) as ArrayRef,
                    Arc::new(LargeStringArray::from(vec![max_val])) as ArrayRef,
                ))
            }
            DataType::Utf8View => {
                let array = filtered_column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                // For StringViewArray, iterate manually to find min/max
                let mut min_val: Option<String> = None;
                let mut max_val: Option<String> = None;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        let val = array.value(i);
                        min_val = Some(match min_val {
                            Some(m) if m.as_str() <= val => m,
                            _ => val.to_string(),
                        });
                        max_val = Some(match max_val {
                            Some(m) if m.as_str() >= val => m,
                            _ => val.to_string(),
                        });
                    }
                }
                Ok((
                    Arc::new(StringViewArray::from(vec![
                        min_val.map(|s| Self::truncate_string(&s))
                    ])) as ArrayRef,
                    Arc::new(StringViewArray::from(vec![
                        max_val.map(|s| Self::truncate_string(&s))
                    ])) as ArrayRef,
                ))
            }
            // Types without meaningful min/max stats
            _ => {
                let null = Self::create_null_array(filtered_column.data_type());
                Ok((null.clone(), null))
            }
        }
    }

    /// Merge min values, keeping the smaller one.
    fn merge_min(existing: &ArrayRef, new: &ArrayRef) -> ArrayRef {
        if existing.is_null(0) {
            return new.clone();
        }
        if new.is_null(0) {
            return existing.clone();
        }
        // Both are single-element arrays, compare and return the smaller
        Self::compare_and_select(existing, new, true)
    }

    /// Merge max values, keeping the larger one.
    fn merge_max(existing: &ArrayRef, new: &ArrayRef) -> ArrayRef {
        if existing.is_null(0) {
            return new.clone();
        }
        if new.is_null(0) {
            return existing.clone();
        }
        // Both are single-element arrays, compare and return the larger
        Self::compare_and_select(existing, new, false)
    }

    /// Compare two single-element arrays and return the min (if is_min) or max.
    fn compare_and_select(a: &ArrayRef, b: &ArrayRef, is_min: bool) -> ArrayRef {
        macro_rules! compare_primitive {
            ($array_type:ty, $a:expr, $b:expr, $is_min:expr) => {{
                let a_arr = $a.as_any().downcast_ref::<$array_type>().unwrap();
                let b_arr = $b.as_any().downcast_ref::<$array_type>().unwrap();
                let a_val = a_arr.value(0);
                let b_val = b_arr.value(0);
                let result = if $is_min {
                    if a_val <= b_val {
                        a_val
                    } else {
                        b_val
                    }
                } else {
                    if a_val >= b_val {
                        a_val
                    } else {
                        b_val
                    }
                };
                Arc::new(<$array_type>::from(vec![result])) as ArrayRef
            }};
        }

        match a.data_type() {
            DataType::Int8 => compare_primitive!(Int8Array, a, b, is_min),
            DataType::Int16 => compare_primitive!(Int16Array, a, b, is_min),
            DataType::Int32 => compare_primitive!(Int32Array, a, b, is_min),
            DataType::Int64 => compare_primitive!(Int64Array, a, b, is_min),
            DataType::UInt8 => compare_primitive!(UInt8Array, a, b, is_min),
            DataType::UInt16 => compare_primitive!(UInt16Array, a, b, is_min),
            DataType::UInt32 => compare_primitive!(UInt32Array, a, b, is_min),
            DataType::UInt64 => compare_primitive!(UInt64Array, a, b, is_min),
            DataType::Date32 => compare_primitive!(Date32Array, a, b, is_min),
            DataType::Date64 => compare_primitive!(Date64Array, a, b, is_min),
            DataType::Float32 => {
                let a_arr = a.as_any().downcast_ref::<Float32Array>().unwrap();
                let b_arr = b.as_any().downcast_ref::<Float32Array>().unwrap();
                let a_val = a_arr.value(0);
                let b_val = b_arr.value(0);
                let result = if is_min {
                    a_val.min(b_val)
                } else {
                    a_val.max(b_val)
                };
                Arc::new(Float32Array::from(vec![result])) as ArrayRef
            }
            DataType::Float64 => {
                let a_arr = a.as_any().downcast_ref::<Float64Array>().unwrap();
                let b_arr = b.as_any().downcast_ref::<Float64Array>().unwrap();
                let a_val = a_arr.value(0);
                let b_val = b_arr.value(0);
                let result = if is_min {
                    a_val.min(b_val)
                } else {
                    a_val.max(b_val)
                };
                Arc::new(Float64Array::from(vec![result])) as ArrayRef
            }
            DataType::Utf8 => {
                let a_arr = a.as_any().downcast_ref::<StringArray>().unwrap();
                let b_arr = b.as_any().downcast_ref::<StringArray>().unwrap();
                let a_val = a_arr.value(0);
                let b_val = b_arr.value(0);
                let result = if is_min {
                    if a_val <= b_val {
                        a_val
                    } else {
                        b_val
                    }
                } else {
                    if a_val >= b_val {
                        a_val
                    } else {
                        b_val
                    }
                };
                Arc::new(StringArray::from(vec![result])) as ArrayRef
            }
            _ => a.clone(), // Fallback
        }
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
                    Self::merge_null_counts(&self.null_counts[col_idx], &batch_null_counts);

                // Update min/max
                let (batch_min, batch_max) = Self::compute_min_max(column, mask)?;
                self.min_values[col_idx] = Self::merge_min(&self.min_values[col_idx], &batch_min);
                self.max_values[col_idx] = Self::merge_max(&self.max_values[col_idx], &batch_max);
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

        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]);
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
            StatisticsCollector::new(schema, &["id".to_string(), "value".to_string()]);
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

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]);
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
        let mut collector = StatisticsCollector::new(schema, &["id".to_string()]);
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
            StatisticsCollector::new(schema, &["number".to_string(), "name".to_string()]);
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

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]);
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

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]);
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

        let mut collector = StatisticsCollector::new(schema, &["value".to_string()]);
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
