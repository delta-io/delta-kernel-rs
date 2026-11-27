//! Prefix-sum evaluation for the synchronous plan executor.

use std::sync::Arc;

use crate::arrow::array::{Array as _, Int64Array, RecordBatch};
use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::engine::arrow_conversion::TryFromKernel as _;
use crate::plans::ir::nodes::PrefixSum;
use crate::{KernelError, KernelResult};

/// Evaluates a [`PrefixSum`].
pub(super) fn eval_prefix_sum(
    prefix_sum: &PrefixSum,
    input: &[RecordBatch],
) -> KernelResult<Vec<RecordBatch>> {
    let output_schema = Arc::new(ArrowSchema::try_from_kernel(&prefix_sum.schema)?);
    let mut running_total = 0i64;
    let mut output = Vec::with_capacity(input.len());
    for batch in input {
        let values = super::extract_long_column(batch, &prefix_sum.input)?;

        let mut offsets = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            if values.is_valid(row_idx) {
                offsets.push(Some(running_total));
                running_total =
                    i64::checked_add(running_total, values.value(row_idx)).ok_or_else(|| {
                        KernelError::generic("SyncPlanExecutor PrefixSum overflowed i64")
                    })?;
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
    use crate::arrow::array::{ArrayRef, StringArray, StructArray};
    use crate::arrow::buffer::NullBuffer;
    use crate::arrow::util::pretty::pretty_format_batches;
    use crate::engine::arrow_data::EngineDataArrowExt;
    use crate::engine::arrow_expression::evaluate_expression::extract_column;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::expressions::column_name;
    use crate::plans::{Operation, PlanBuilder, PlanExecutor};
    use crate::schema::{DataType, StructField, schema, schema_ref};

    /// Asserts `batches` pretty-print equal to `expected` (row order must match).
    fn assert_batches_eq(batches: &[RecordBatch], expected: &str) {
        let formatted = pretty_format_batches(batches).unwrap().to_string();
        assert_eq!(formatted.trim(), expected.trim());
    }

    /// Prefix-sums `numRecords` of the fixture schema into an appended `offset` column.
    fn test_eval_prefix_sum(input: &[RecordBatch]) -> KernelResult<Vec<RecordBatch>> {
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
    ) -> KernelResult<()> {
        assert_batches_eq(&test_eval_prefix_sum(&input)?, expected);
        Ok(())
    }

    #[rstest::rstest]
    fn nested_prefix_sum_masks_null_ancestors(
        #[values(false, true)] leaf_nullable: bool,
        #[values(false, true)] null_hidden_child: bool,
        #[values(false, true)] split_batches: bool,
    ) -> KernelResult<()> {
        let stats_schema = schema! {
            (StructField::new("numRecords", DataType::LONG, leaf_nullable)),
        };
        let outer_schema = schema! {
            nullable "stats": (stats_schema.clone()),
        };
        let input_schema = schema_ref! {
            nullable "outer": (outer_schema.clone()),
        };
        let values = Int64Array::from(vec![
            Some(i64::MAX),
            Some(10),
            if null_hidden_child {
                None
            } else {
                Some(i64::MAX)
            },
            Some(i64::MAX),
            if leaf_nullable { None } else { Some(5) },
            Some(20),
        ]);
        let stats = StructArray::try_new(
            ArrowSchema::try_from_kernel(&stats_schema)?
                .fields()
                .clone(),
            vec![Arc::new(values)],
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        )?;
        let outer = StructArray::try_new(
            ArrowSchema::try_from_kernel(&outer_schema)?
                .fields()
                .clone(),
            vec![Arc::new(stats)],
            Some(NullBuffer::from(vec![true, true, true, false, true, true])),
        )?;
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::try_from_kernel(&input_schema)?),
            vec![Arc::new(outer)],
        )?;
        // Slice away the first row to exercise validity buffers with nonzero offsets.
        let input = if split_batches {
            vec![batch.slice(1, 3), batch.slice(4, 2)]
        } else {
            vec![batch.slice(1, 5)]
        };
        let scoped = SyncPlanExecutor::default().get_scoped();
        let sources = input
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                let source =
                    scoped.create_ref(&format!("batch-{index}"), input_schema.clone(), batch)?;
                Ok(PlanBuilder::relation_source(source))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let plan = PlanBuilder::union_all(sources)?
            .prefix_sum(column_name!("outer.stats.numRecords"), "offset")?
            .build()?;
        let output = scoped
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(output.len(), if split_batches { 2 } else { 1 });
        let offsets: Vec<_> = output
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
            })
            .collect();
        assert_eq!(
            offsets,
            vec![
                Some(0),
                None,
                None,
                if leaf_nullable { None } else { Some(10) },
                Some(if leaf_nullable { 10 } else { 15 }),
            ]
        );
        let effective_values: Vec<_> = output
            .iter()
            .flat_map(|batch| {
                let values = extract_column(batch, &["outer", "stats", "numRecords"]).unwrap();
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            effective_values,
            [
                Some(10),
                None,
                None,
                if leaf_nullable { None } else { Some(5) },
                Some(20),
            ]
        );
        Ok(())
    }

    #[test]
    fn empty_input_yields_empty_output() -> KernelResult<()> {
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
