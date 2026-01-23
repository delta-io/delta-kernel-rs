//! Statistics collection for Delta Lake file writes.
//!
//! This module provides `StatisticsCollector` which accumulates statistics
//! across multiple Arrow RecordBatches during file writes.

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType, Field, Fields};
use crate::{DeltaResult, Error};

/// Collects statistics from RecordBatches for Delta Lake file statistics.
/// Supports streaming accumulation across multiple batches.
pub(crate) struct StatisticsCollector {
    /// Total number of records across all batches.
    num_records: i64,
    /// Column names from the data schema.
    column_names: Vec<String>,
    /// Column names that should have stats collected.
    stats_columns: HashSet<String>,
    /// Null counts per column. For structs, this is a StructArray with nested Int64Arrays.
    null_counts: Vec<ArrayRef>,
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

        for field in data_schema.fields() {
            column_names.push(field.name().clone());
            null_counts.push(Self::create_zero_null_count(field.data_type()));
        }

        Self {
            num_records: 0,
            column_names,
            stats_columns: stats_set,
            null_counts,
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

    /// Compute null counts for a column.
    fn compute_null_counts(column: &ArrayRef) -> ArrayRef {
        match column.data_type() {
            DataType::Struct(fields) => {
                let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
                let children: Vec<ArrayRef> = (0..fields.len())
                    .map(|i| Self::compute_null_counts(struct_array.column(i)))
                    .collect();
                let null_count_fields: Fields = fields
                    .iter()
                    .map(|f| Field::new(f.name(), Self::null_count_data_type(f.data_type()), true))
                    .collect();
                Arc::new(
                    StructArray::try_new(null_count_fields, children, None)
                        .expect("Failed to create null count struct"),
                )
            }
            _ => {
                let null_count = column.null_count() as i64;
                Arc::new(Int64Array::from(vec![null_count]))
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

    /// Update statistics with data from a RecordBatch.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        self.num_records += batch.num_rows() as i64;

        // Update null counts
        for (col_idx, column) in batch.columns().iter().enumerate() {
            let col_name = &self.column_names[col_idx];
            if self.should_collect_stats(col_name) {
                let batch_null_counts = Self::compute_null_counts(column);
                self.null_counts[col_idx] =
                    Self::merge_null_counts(&self.null_counts[col_idx], &batch_null_counts);
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
        collector.update(&batch).unwrap();
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
        collector.update(&batch).unwrap();
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
        collector.update(&batch1).unwrap();
        collector.update(&batch2).unwrap();
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
        collector.update(&batch).unwrap();
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
}
