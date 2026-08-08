//! Aggregate evaluation for the synchronous plan executor.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use super::plan::encode_keys_as_rows;
use crate::arrow::array::{new_null_array, Array, ArrayRef, Int64Array, RecordBatch};
use crate::arrow::compute::interleave;
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::arrow::row::OwnedRow;
use crate::engine::arrow_conversion::TryIntoArrow as _;
use crate::engine::arrow_expression::evaluate_expression::{extract_column, extract_column_ref};
use crate::expressions::ColumnName;
use crate::plans::ir::nodes::{Agg, Aggregate, NonNullByOperands};
use crate::schema::DataType;
use crate::{DeltaResult, Error};

/// A specific row of input: `(batch index, row index)`.
type InputRow = (usize, usize);

/// Batch-scoped row updater produced by [`BoundAggregate::prepare`].
trait AggUpdater {
    /// Applies `row` to the agg's `state`
    fn update(&self, state: &mut dyn Any, row: InputRow) -> DeltaResult<()>;
}

/// Bound aggregate operator: init/prepare/materialize with type-erased per-group state.
trait BoundAggregate {
    /// Creates a new agg state (i.e. NULL for MIN/MAX)
    fn init_state(&self) -> Box<dyn Any>;
    /// Creates an `AggUpdater` for processing the rows of `batch`
    fn prepare<'a>(&'a self, batch: &'a RecordBatch) -> DeltaResult<Box<dyn AggUpdater + 'a>>;
    /// Finalize agg states and write them to a single output column
    fn finalize(&self, states: &[&dyn Any], input: &[RecordBatch]) -> DeltaResult<ArrayRef>;
}

/// Evaluates an [`Aggregate`].
///
/// Currently supports Min, Max, MinNonNullBy, and MaxNonNullBy. Comparison operands are LONG-only.
pub(super) fn eval_aggregate(
    aggregate: &Aggregate,
    input: &[RecordBatch],
) -> DeltaResult<Vec<RecordBatch>> {
    let ops: Vec<Box<dyn BoundAggregate>> = aggregate
        .aggs
        .iter()
        .zip(aggregate.schema.fields().skip(aggregate.group_by.len()))
        .map(|(agg, field)| bind_aggregate(agg, field.data_type()))
        .try_collect()?;

    // Each group tracks a representative input row and per-agg states as we route rows to it.
    let mut groups = HashMap::<OwnedRow, (InputRow, Vec<Box<dyn Any>>)>::new();
    let initial_aggs = || ops.iter().map(|op| op.init_state()).collect();
    for (batch_idx, batch) in input.iter().enumerate() {
        let updaters: Vec<_> = ops.iter().map(|op| op.prepare(batch)).try_collect()?;
        let group_keys = encode_keys_as_rows(batch, &aggregate.group_by)?;
        for (row_idx, group_key) in group_keys.into_iter().enumerate() {
            let row = (batch_idx, row_idx);
            let (_, aggs) = groups
                .entry(group_key)
                .or_insert_with(|| (row, initial_aggs()));
            for (updater, agg_state) in updaters.iter().zip(aggs) {
                updater.update(agg_state.as_mut(), row)?;
            }
        }
    }

    // Handle empty input. Grouped: no rows. Ungrouped: one synthetic group of initial agg states.
    let output_schema = Arc::new(aggregate.schema.as_ref().try_into_arrow()?);
    let (reps, mut aggs_by_group): (Vec<_>, Vec<_>) = groups.into_values().unzip();
    if aggs_by_group.is_empty() {
        if !aggregate.group_by.is_empty() {
            return Ok(vec![]);
        }
        aggs_by_group.push(initial_aggs());
    }

    // Materialize the grouping key columns, followed by agg columns
    let mut output_arrays = Vec::with_capacity(aggregate.schema.fields().len());
    for group_by in &aggregate.group_by {
        let arrays = extract_column_values(input, group_by)?;
        output_arrays.push(interleave_column_values(&arrays, &reps)?);
    }
    for (agg_idx, op) in ops.iter().enumerate() {
        let states = Vec::from_iter(aggs_by_group.iter().map(|aggs| aggs[agg_idx].as_ref()));
        output_arrays.push(op.finalize(&states, input)?);
    }

    Ok(vec![RecordBatch::try_new(output_schema, output_arrays)?])
}

// Extracts the named column from each of the input batches
fn extract_column_values(input: &[RecordBatch], name: &ColumnName) -> DeltaResult<Vec<ArrayRef>> {
    input
        .iter()
        .map(|batch| extract_column(batch, name))
        .try_collect()
}

// Thin wrapper around arrow `interleave` that converts our `&[ArrayRef]` into `&[&dyn Array]`
fn interleave_column_values(arrays: &[ArrayRef], indices: &[InputRow]) -> DeltaResult<ArrayRef> {
    let refs = Vec::from_iter(arrays.iter().map(|c| c.as_ref()));
    Ok(interleave(&refs, indices)?)
}

fn bind_aggregate(agg: &Agg, output_type: &DataType) -> DeltaResult<Box<dyn BoundAggregate>> {
    match agg {
        Agg::Min(value) => LongAccumulatorAgg::try_new(value, output_type, LongAccumulator::Min),
        Agg::Max(value) => LongAccumulatorAgg::try_new(value, output_type, LongAccumulator::Max),
        Agg::MinNonNullBy(ops) => NonNullByAgg::try_new(ops, output_type, Comparison::Min),
        Agg::MaxNonNullBy(ops) => NonNullByAgg::try_new(ops, output_type, Comparison::Max),
    }
}

// Used by both Min/Max and Min/MaxNonNullBy
#[derive(Clone, Copy)]
enum Comparison {
    Min,
    Max,
}

impl Comparison {
    fn replaces(self, winner: Option<i64>, candidate: i64) -> bool {
        match self {
            Self::Min => winner.is_none_or(|best| candidate < best),
            Self::Max => winner.is_none_or(|best| candidate > best),
        }
    }
}

#[derive(Clone, Copy)]
enum LongAccumulator {
    Min,
    Max,
}

struct LongAccumulatorAgg {
    value: ColumnName,
    op: LongAccumulator,
}

impl LongAccumulatorAgg {
    fn try_new(
        value: &ColumnName,
        output_type: &DataType,
        op: LongAccumulator,
    ) -> DeltaResult<Box<dyn BoundAggregate>> {
        if output_type != &DataType::LONG {
            return Err(Error::unsupported(
                "SyncPlanExecutor min/max aggregate with non-LONG value",
            ));
        }
        Ok(Box::new(Self {
            value: value.clone(),
            op,
        }))
    }
}

struct LongAccumulatorState(Option<i64>);

impl BoundAggregate for LongAccumulatorAgg {
    fn init_state(&self) -> Box<dyn Any> {
        Box::new(LongAccumulatorState(None))
    }

    fn prepare<'a>(&'a self, batch: &'a RecordBatch) -> DeltaResult<Box<dyn AggUpdater + 'a>> {
        Ok(Box::new(LongAccumulatorUpdater {
            values: extract_long_column(batch, &self.value)?,
            op: self.op,
        }))
    }

    fn finalize(&self, states: &[&dyn Any], _input: &[RecordBatch]) -> DeltaResult<ArrayRef> {
        let values = states
            .iter()
            .map(|state| Ok(downcast_state::<LongAccumulatorState>(*state)?.0))
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Arc::new(Int64Array::from(values)))
    }
}

struct LongAccumulatorUpdater<'a> {
    values: &'a Int64Array,
    op: LongAccumulator,
}

impl AggUpdater for LongAccumulatorUpdater<'_> {
    fn update(&self, state: &mut dyn Any, (_, row_idx): InputRow) -> DeltaResult<()> {
        if self.values.is_valid(row_idx) {
            let state = downcast_state_mut::<LongAccumulatorState>(state)?;
            let candidate = self.values.value(row_idx);
            match self.op {
                LongAccumulator::Min => {
                    if Comparison::Min.replaces(state.0, candidate) {
                        state.0 = Some(candidate);
                    }
                }
                LongAccumulator::Max => {
                    if Comparison::Max.replaces(state.0, candidate) {
                        state.0 = Some(candidate);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Min/max-by selector: compare LONG keys among rows with a non-null sentinel, gather `value`.
struct NonNullByAgg {
    value: ColumnName,
    null_sentinel: ColumnName,
    key: ColumnName,
    comparison: Comparison,
    output_type: ArrowDataType,
}

impl NonNullByAgg {
    fn try_new(
        operands: &NonNullByOperands,
        output_type: &DataType,
        comparison: Comparison,
    ) -> DeltaResult<Box<dyn BoundAggregate>> {
        Ok(Box::new(Self {
            value: operands.value.clone(),
            null_sentinel: operands.null_sentinel.clone(),
            key: operands.key.clone(),
            comparison,
            output_type: output_type.try_into_arrow()?,
        }))
    }
}

struct NonNullByState(Option<(InputRow, i64)>);

impl BoundAggregate for NonNullByAgg {
    fn init_state(&self) -> Box<dyn Any> {
        Box::new(NonNullByState(None))
    }

    fn prepare<'a>(&'a self, batch: &'a RecordBatch) -> DeltaResult<Box<dyn AggUpdater + 'a>> {
        Ok(Box::new(NonNullByUpdater {
            null_sentinels: extract_column_ref(batch, &self.null_sentinel)?,
            keys: extract_long_column(batch, &self.key)?,
            comparison: self.comparison,
        }))
    }

    fn finalize(&self, states: &[&dyn Any], input: &[RecordBatch]) -> DeltaResult<ArrayRef> {
        let mut arrays = extract_column_values(input, &self.value)?;
        arrays.push(new_null_array(&self.output_type, 1));

        // Groups with no winner interleave from the one-row null array we appended above.
        let initial_row = (input.len(), 0);
        let rows: Vec<_> = states
            .iter()
            .map(|state| -> DeltaResult<_> {
                let winner = downcast_state::<NonNullByState>(*state)?.0;
                Ok(winner.map_or(initial_row, |(row, _)| row))
            })
            .try_collect()?;

        interleave_column_values(&arrays, &rows)
    }
}

struct NonNullByUpdater<'a> {
    null_sentinels: &'a ArrayRef,
    keys: &'a Int64Array,
    comparison: Comparison,
}

impl AggUpdater for NonNullByUpdater<'_> {
    fn update(&self, state: &mut dyn Any, (batch_idx, row_idx): InputRow) -> DeltaResult<()> {
        if self.null_sentinels.is_valid(row_idx) && self.keys.is_valid(row_idx) {
            let state = downcast_state_mut::<NonNullByState>(state)?;
            let best = state.0.map(|(_, best)| best);
            let candidate = self.keys.value(row_idx);
            if self.comparison.replaces(best, candidate) {
                state.0 = Some(((batch_idx, row_idx), candidate));
            }
        }
        Ok(())
    }
}

fn extract_long_column<'a>(
    batch: &'a RecordBatch,
    name: &ColumnName,
) -> DeltaResult<&'a Int64Array> {
    let array = extract_column_ref(batch, name)?;
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        Error::unsupported(format!(
            "SyncPlanExecutor aggregate operand `{name}` has non-LONG type"
        ))
    })
}

fn downcast_state<T: 'static>(state: &dyn Any) -> DeltaResult<&T> {
    state.downcast_ref().ok_or_else(|| {
        Error::generic(format!(
            "Aggregate state is not a {}",
            std::any::type_name::<T>()
        ))
    })
}

fn downcast_state_mut<T: 'static>(state: &mut dyn Any) -> DeltaResult<&mut T> {
    state.downcast_mut().ok_or_else(|| {
        Error::generic(format!(
            "Aggregate state is not a {}",
            std::any::type_name::<T>()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{BooleanArray, StringArray, StructArray};
    use crate::arrow::datatypes::Field;
    use crate::arrow::util::pretty::pretty_format_batches;
    use crate::expressions::column_name;
    use crate::schema::schema_ref;

    /// Asserts `batches` pretty-print equal to `expected`, sorting data rows so HashMap group order
    /// does not matter. `expected` is the full pretty table (header, body, footer).
    fn assert_batches_eq(batches: &[RecordBatch], expected: &str) {
        let formatted = pretty_format_batches(batches).unwrap().to_string();
        let sort_body = |s: &str| -> String {
            let mut lines: Vec<_> = s.trim().lines().map(str::to_string).collect();
            let len = lines.len();
            if len > 3 {
                lines[2..len - 1].sort_unstable();
            }
            lines.join("\n")
        };
        assert_eq!(sort_body(&formatted), sort_body(expected));
    }

    #[test]
    fn grouped_aggregate_routes_rows_to_every_agg() -> DeltaResult<()> {
        let input = RecordBatch::try_from_iter([
            (
                "group",
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["low", "high", "none"])),
            ),
            (
                "sentinel",
                Arc::new(BooleanArray::from(vec![Some(true), Some(true), None])),
            ),
            (
                "key",
                Arc::new(Int64Array::from(vec![Some(1), Some(3), None])),
            ),
        ])?;
        let aggregate = Aggregate {
            group_by: vec![column_name!("group")],
            aggs: vec![
                Agg::max_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
                Agg::min_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
                Agg::max(column_name!("key")),
                Agg::min(column_name!("key")),
            ],
            schema: schema_ref! {
                not_null "group": STRING,
                nullable "maximum": STRING,
                nullable "minimum": STRING,
                nullable "max_key": LONG,
                nullable "min_key": LONG,
            },
        };

        assert_batches_eq(
            &eval_aggregate(&aggregate, &[input])?,
            "\
+-------+---------+---------+---------+---------+
| group | maximum | minimum | max_key | min_key |
+-------+---------+---------+---------+---------+
| a     | high    | low     | 3       | 1       |
| b     |         |         |         |         |
+-------+---------+---------+---------+---------+",
        );
        Ok(())
    }

    #[test]
    fn non_null_by_selects_across_batches() -> DeltaResult<()> {
        let aggregate = Aggregate {
            group_by: vec![],
            aggs: vec![
                Agg::min_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
                Agg::max_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
            ],
            schema: schema_ref! {
                nullable "minimum": STRING,
                nullable "maximum": STRING,
            },
        };
        let first = RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(StringArray::from(vec!["low"])) as ArrayRef,
            ),
            ("sentinel", Arc::new(BooleanArray::from(vec![true]))),
            ("key", Arc::new(Int64Array::from(vec![1]))),
        ])?;
        let second = RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(StringArray::from(vec!["high"])) as ArrayRef,
            ),
            ("sentinel", Arc::new(BooleanArray::from(vec![true]))),
            ("key", Arc::new(Int64Array::from(vec![2]))),
        ])?;

        assert_batches_eq(
            &eval_aggregate(&aggregate, &[first, second])?,
            "\
+---------+---------+
| minimum | maximum |
+---------+---------+
| low     | high    |
+---------+---------+",
        );
        Ok(())
    }

    #[test]
    fn empty_input_distinguishes_grouped_and_ungrouped_aggregates() -> DeltaResult<()> {
        let ungrouped = Aggregate {
            group_by: vec![],
            aggs: vec![
                Agg::min(column_name!("key")),
                Agg::max(column_name!("key")),
                Agg::min_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
                Agg::max_non_null_by(
                    column_name!("value"),
                    column_name!("sentinel"),
                    column_name!("key"),
                ),
            ],
            schema: schema_ref! {
                nullable "minimum": LONG,
                nullable "maximum": LONG,
                nullable "first": STRING,
                nullable "last": STRING,
            },
        };

        assert_batches_eq(
            &eval_aggregate(&ungrouped, &[])?,
            "\
+---------+---------+-------+------+
| minimum | maximum | first | last |
+---------+---------+-------+------+
|         |         |       |      |
+---------+---------+-------+------+",
        );

        let grouped = Aggregate {
            group_by: vec![column_name!("group")],
            aggs: vec![Agg::max(column_name!("key"))],
            schema: schema_ref! {
                not_null "group": STRING,
                nullable "max_key": LONG,
            },
        };
        assert!(eval_aggregate(&grouped, &[])?.is_empty());
        Ok(())
    }

    #[test]
    fn aggregate_resolves_nested_columns_over_top_level_traps() -> DeltaResult<()> {
        let outer = StructArray::from(vec![
            (
                Arc::new(Field::new("group", ArrowDataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["nested_group"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["nested_value"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("sentinel", ArrowDataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("key", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
            ),
        ]);
        let input = RecordBatch::try_from_iter([
            (
                "group",
                Arc::new(StringArray::from(vec!["trap_group"])) as ArrayRef,
            ),
            ("value", Arc::new(StringArray::from(vec!["trap_value"]))),
            ("sentinel", Arc::new(BooleanArray::from(vec![None]))),
            ("key", Arc::new(Int64Array::from(vec![Some(99)]))),
            ("outer", Arc::new(outer)),
        ])?;
        let aggregate = Aggregate {
            group_by: vec![column_name!("outer.group")],
            aggs: vec![
                Agg::min(column_name!("outer.key")),
                Agg::max_non_null_by(
                    column_name!("outer.value"),
                    column_name!("outer.sentinel"),
                    column_name!("outer.key"),
                ),
            ],
            schema: schema_ref! {
                not_null "group": STRING,
                nullable "minimum": LONG,
                nullable "winner": STRING,
            },
        };

        assert_batches_eq(
            &eval_aggregate(&aggregate, &[input])?,
            "\
+--------------+---------+--------------+
| group        | minimum | winner       |
+--------------+---------+--------------+
| nested_group | 7       | nested_value |
+--------------+---------+--------------+",
        );
        Ok(())
    }

    #[test]
    fn non_null_by_can_select_a_null_value() -> DeltaResult<()> {
        let input = RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(StringArray::from(vec![Some("fallback"), None])) as ArrayRef,
            ),
            ("sentinel", Arc::new(BooleanArray::from(vec![true, true]))),
            ("key", Arc::new(Int64Array::from(vec![1, 2]))),
        ])?;
        let aggregate = Aggregate {
            group_by: vec![],
            aggs: vec![Agg::max_non_null_by(
                column_name!("value"),
                column_name!("sentinel"),
                column_name!("key"),
            )],
            schema: schema_ref! {
                nullable "winner": STRING,
            },
        };

        assert_batches_eq(
            &eval_aggregate(&aggregate, &[input])?,
            "\
+--------+
| winner |
+--------+
|        |
+--------+",
        );
        Ok(())
    }
}
