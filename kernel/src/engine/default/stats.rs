//! Statistics collection for Delta Lake file writes.
//!
//! Provides `collect_stats` to compute null count statistics for a single RecordBatch
//! during file writes.

use std::sync::Arc;

use crate::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType, Field};
use crate::column_trie::ColumnTrie;
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

/// Downcast helper with descriptive error message.
fn downcast<T: 'static>(column: &ArrayRef) -> DeltaResult<&T> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::generic(format!(
            "Failed to downcast column to {}",
            std::any::type_name::<T>(),
        ))
    })
}

/// Compute null count for a column, filtering by the stats column filter.
///
/// Returns `Some(ArrayRef)` if this column should be included, `None` otherwise.
/// For struct columns, returns a nested StructArray. For leaf columns, returns Int64Array.
fn compute_null_count(
    column: &ArrayRef,
    path: &mut Vec<String>,
    filter: &ColumnTrie,
) -> DeltaResult<Option<ArrayRef>> {
    match column.data_type() {
        DataType::Struct(fields) => {
            let struct_array = downcast::<StructArray>(column)?;

            let mut child_fields: Vec<Field> = Vec::new();
            let mut child_arrays: Vec<ArrayRef> = Vec::new();

            for (i, field) in fields.iter().enumerate() {
                path.push(field.name().to_string());

                if let Some(child_array) = compute_null_count(struct_array.column(i), path, filter)?
                {
                    child_fields.push(Field::new(
                        field.name(),
                        child_array.data_type().clone(),
                        true,
                    ));
                    child_arrays.push(child_array);
                }

                path.pop();
            }

            if child_fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Arc::new(
                    StructArray::try_new(child_fields.into(), child_arrays, None)
                        .map_err(|e| Error::generic(format!("null count struct: {e}")))?,
                ) as ArrayRef))
            }
        }
        _ => {
            // Leaf: check filter
            if !filter.contains_prefix_of(path) {
                return Ok(None);
            }

            Ok(Some(
                Arc::new(Int64Array::from(vec![column.null_count() as i64])) as ArrayRef,
            ))
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
/// - `tightBounds`: always true for new file writes
///
/// # Arguments
/// * `batch` - The RecordBatch to collect statistics from
/// * `stats_columns` - Column names that should have statistics collected (allowlist).
///   Only these columns will appear in nullCount.
pub(crate) fn collect_stats(
    batch: &RecordBatch,
    stats_columns: &[ColumnName],
) -> DeltaResult<StructArray> {
    let filter = ColumnTrie::from_columns(stats_columns);
    let schema = batch.schema();

    let mut null_counts = StatsAccumulator::new("nullCount");

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut path = vec![field.name().to_string()];
        let column = batch.column(col_idx);

        if let Some(arr) = compute_null_count(column, &mut path, &filter)? {
            null_counts.push(field.name(), arr);
        }
    }

    // Build output struct
    let mut fields = vec![Field::new("numRecords", DataType::Int64, true)];
    let mut arrays: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(vec![batch.num_rows() as i64]))];

    if let Some((field, array)) = null_counts.build()? {
        fields.push(field);
        arrays.push(array);
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

        // Should not have nullCount
        assert!(stats.column_by_name("nullCount").is_none());
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
    }
}
