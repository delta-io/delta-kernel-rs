//! Prefix-sum evaluation for the synchronous plan executor.

use std::sync::Arc;

use crate::arrow::array::{Array as _, ArrayRef, Int64Array, RecordBatch};
use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::engine::arrow_conversion::TryFromKernel as _;
use crate::plans::ir::nodes::PrefixSum;
use crate::{DeltaResult, Error};

/// Evaluates a [`PrefixSum`].
///
/// Computes an exclusive prefix sum over rows of `input` batches. The first output value is 0;
/// NULL inputs do not contribute and emit NULL; zero values contribute 0 to the running total.
pub(super) fn eval_prefix_sum(
    prefix_sum: &PrefixSum,
    input: &[RecordBatch],
) -> DeltaResult<Vec<RecordBatch>> {
    let output_schema = Arc::new(ArrowSchema::try_from_kernel(&prefix_sum.schema)?);
    let mut running_total = 0i64;
    let mut output = Vec::with_capacity(input.len());
    for batch in input {
        let values = super::extract_long_column(batch, &prefix_sum.input)?;
        let mut offsets = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            if values.is_valid(row_idx) {
                offsets.push(Some(running_total));
                running_total = i64::checked_add(running_total, values.value(row_idx))
                    .ok_or_else(|| Error::generic("SyncPlanExecutor PrefixSum overflowed i64"))?;
            } else {
                offsets.push(None);
            }
        }
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(offsets)));
        output.push(RecordBatch::try_new(output_schema.clone(), columns)?);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::StringArray;
    use crate::arrow::util::pretty::pretty_format_batches;
    use crate::expressions::column_name;
    use crate::schema::schema_ref;

    /// Asserts `batches` pretty-print equal to `expected` (row order must match).
    fn assert_batches_eq(batches: &[RecordBatch], expected: &str) {
        let formatted = pretty_format_batches(batches).unwrap().to_string();
        assert_eq!(formatted.trim(), expected.trim());
    }

    /// Prefix-sums `numRecords` of the fixture schema into an appended `offset` column.
    fn test_eval_prefix_sum(input: &[RecordBatch]) -> DeltaResult<Vec<RecordBatch>> {
        let schema = schema_ref! {
            not_null "path": STRING,
            nullable "numRecords": LONG,
        };
        let prefix_sum = PrefixSum::try_new(&schema, column_name!("numRecords"), "offset")?;
        eval_prefix_sum(&prefix_sum, input)
    }

    fn batch(paths: &[&str], sizes: Vec<Option<i64>>) -> RecordBatch {
        let paths = Arc::new(StringArray::from(paths.to_vec())) as ArrayRef;
        let sizes = Arc::new(Int64Array::from(sizes));
        RecordBatch::try_from_iter([("path", paths), ("numRecords", sizes)]).unwrap()
    }

    #[rstest::rstest]
    #[case::encounter_order(
        vec![batch(&["a", "b", "c"], vec![Some(10), Some(5), Some(20)])],
        "\
+------+------------+--------+
| path | numRecords | offset |
+------+------------+--------+
| a    | 10         | 0      |
| b    | 5          | 10     |
| c    | 20         | 15     |
+------+------------+--------+"
    )]
    #[case::nulls_and_zeros(
        vec![batch(
            &["a", "b", "c", "d"],
            vec![Some(10), None, Some(0), Some(5)],
        )],
        "\
+------+------------+--------+
| path | numRecords | offset |
+------+------------+--------+
| a    | 10         | 0      |
| b    |            |        |
| c    | 0          | 10     |
| d    | 5          | 10     |
+------+------------+--------+"
    )]
    // pretty_format_batches concatenates rows from every batch into one table.
    #[case::across_batches(
        vec![
            batch(&["a"], vec![Some(10)]),
            batch(&["b", "c"], vec![Some(5), Some(20)]),
        ],
        "\
+------+------------+--------+
| path | numRecords | offset |
+------+------------+--------+
| a    | 10         | 0      |
| b    | 5          | 10     |
| c    | 20         | 15     |
+------+------------+--------+"
    )]
    fn exclusive_prefix_sum(
        #[case] input: Vec<RecordBatch>,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        assert_batches_eq(&test_eval_prefix_sum(&input)?, expected);
        Ok(())
    }

    #[test]
    fn empty_input_yields_empty_output() -> DeltaResult<()> {
        assert!(test_eval_prefix_sum(&[])?.is_empty());
        Ok(())
    }

    #[test]
    fn overflow_is_an_error() {
        let input = batch(&["a", "b"], vec![Some(i64::MAX), Some(1)]);
        let err = test_eval_prefix_sum(&[input]).unwrap_err();
        assert!(err.to_string().contains("overflowed i64"), "{err}");
    }
}
