//! Statistics collection for Delta Lake file writes.
//!
//! This module provides `StatisticsCollector` which accumulates statistics
//! across multiple Arrow RecordBatches during file writes.

use std::sync::Arc;

use crate::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType, Field};
use crate::{DeltaResult, Error};

/// Collects statistics from RecordBatches for Delta Lake file statistics.
/// Supports streaming accumulation across multiple batches.
#[allow(dead_code)]
pub(crate) struct StatisticsCollector {
    /// Total number of records across all batches.
    num_records: i64,
    /// Column names that should have stats collected.
    #[allow(dead_code)]
    stats_columns: Vec<String>,
}

#[allow(dead_code)]
impl StatisticsCollector {
    /// Create a new statistics collector.
    ///
    /// # Arguments
    /// * `data_schema` - The Arrow schema of the data being written
    /// * `stats_columns` - Column names that should have statistics collected
    pub(crate) fn new(
        _data_schema: Arc<crate::arrow::datatypes::Schema>,
        stats_columns: &[String],
    ) -> Self {
        Self {
            num_records: 0,
            stats_columns: stats_columns.to_vec(),
        }
    }

    /// Update statistics with data from a RecordBatch.
    ///
    /// This method accumulates statistics across multiple batches.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        self.num_records += batch.num_rows() as i64;
        Ok(())
    }

    /// Finalize and return the collected statistics as a StructArray.
    ///
    /// Returns a single-row StructArray with the Delta Lake stats schema:
    /// - numRecords: total row count
    /// - tightBounds: true for new files (no deletion vectors applied)
    pub(crate) fn finalize(&self) -> DeltaResult<StructArray> {
        let mut fields = Vec::new();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        // numRecords
        fields.push(Field::new("numRecords", DataType::Int64, true));
        arrays.push(Arc::new(Int64Array::from(vec![self.num_records])));

        // tightBounds - always true for new file writes
        // (false only when deletion vectors are applied to existing files)
        fields.push(Field::new("tightBounds", DataType::Boolean, true));
        arrays.push(Arc::new(BooleanArray::from(vec![true])));

        StructArray::try_new(fields.into(), arrays, None)
            .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array};
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

        // Check numRecords
        assert_eq!(stats.len(), 1);
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 3);

        // Check tightBounds
        let tight_bounds = stats
            .column_by_name("tightBounds")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(tight_bounds.value(0));
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
        collector.update(&batch1).unwrap();
        collector.update(&batch2).unwrap();
        let stats = collector.finalize().unwrap();

        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 5);
    }

    #[test]
    fn test_statistics_collector_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let empty: Vec<i64> = vec![];
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(empty))]).unwrap();

        let mut collector = StatisticsCollector::new(schema, &[]);
        collector.update(&batch).unwrap();
        let stats = collector.finalize().unwrap();

        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 0);
    }
}
