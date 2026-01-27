//! Statistics collection for Delta Lake file writes.
//!
//! Provides `collect_stats` to compute statistics for a RecordBatch during file writes.

use std::sync::Arc;

use crate::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType, Field};
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

/// Collect statistics from a RecordBatch for Delta Lake file statistics.
///
/// Returns a StructArray with the following fields:
/// - `numRecords`: total row count
/// - `tightBounds`: always true for new file writes
///
/// # Arguments
/// * `batch` - The RecordBatch to collect statistics from
/// * `_stats_columns` - Column names that should have statistics collected (reserved for future use)
pub(crate) fn collect_stats(
    batch: &RecordBatch,
    _stats_columns: &[ColumnName],
) -> DeltaResult<StructArray> {
    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

    // numRecords
    fields.push(Field::new("numRecords", DataType::Int64, true));
    arrays.push(Arc::new(Int64Array::from(vec![batch.num_rows() as i64])));

    // tightBounds - always true for new file writes
    fields.push(Field::new("tightBounds", DataType::Boolean, true));
    arrays.push(Arc::new(BooleanArray::from(vec![true])));

    StructArray::try_new(fields.into(), arrays, None)
        .map_err(|e| Error::generic(format!("Failed to create stats struct: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Int64Array;
    use crate::arrow::datatypes::Schema;
    use crate::expressions::column_name;

    #[test]
    fn test_collect_stats_basic() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("id")]).unwrap();

        assert_eq!(stats.len(), 1);

        // Check numRecords
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
    fn test_collect_stats_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let empty: Vec<i64> = vec![];
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(empty))]).unwrap();

        let stats = collect_stats(&batch, &[]).unwrap();

        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), 0);
    }
}
