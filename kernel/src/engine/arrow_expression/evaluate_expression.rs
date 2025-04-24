//! Expression handling based on arrow-rs compute kernels.
use crate::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, RecordBatch, StructArray,
};
use crate::arrow::compute::kernels::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq};
use crate::arrow::compute::kernels::numeric::{add, div, mul, sub};
use crate::arrow::compute::{and_kleene, is_null, not, or_kleene};
use crate::arrow::datatypes::Field as ArrowField;
use crate::arrow::error::ArrowError;
use itertools::Itertools;
use std::sync::Arc;

use super::in_list;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryOperator, Expression, JunctionExpression, JunctionOperator,
    UnaryExpression, UnaryOperator,
};
use crate::schema::DataType;

fn downcast_to_bool(arr: &dyn Array) -> DeltaResult<&BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::generic("expected boolean array"))
}

fn wrap_comparison_result(arr: BooleanArray) -> ArrayRef {
    Arc::new(arr) as _
}

pub(super) trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

// Given a RecordBatch or StructArray, recursively probe for a nested column path and return the
// corresponding column, or Err if the path is invalid. For example, given the following schema:
// ```text
// root: {
//   a: int32,
//   b: struct {
//     c: int32,
//     d: struct {
//       e: int32,
//       f: int64,
//     },
//   },
// }
// ```
// The path ["b", "d", "f"] would retrieve the int64 column while ["a", "b"] would produce an error.
pub(crate) fn extract_column(
    mut parent: &dyn ProvidesColumnByName,
    col: &[String],
) -> DeltaResult<ArrayRef> {
    let mut field_names = col.iter();
    let Some(mut field_name) = field_names.next() else {
        return Err(ArrowError::SchemaError("Empty column path".to_string()))?;
    };
    loop {
        let child = parent
            .column_by_name(field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("No such field: {field_name}")))?;
        field_name = match field_names.next() {
            Some(name) => name,
            None => return Ok(child.clone()),
        };
        parent = child
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ArrowError::SchemaError(format!("Not a struct: {field_name}")))?;
    }
}

pub(crate) fn evaluate_expression(
    expression: &Expression,
    batch: &RecordBatch,
    result_type: Option<&DataType>,
) -> DeltaResult<ArrayRef> {
    use BinaryOperator::*;
    use Expression::*;
    match (expression, result_type) {
        (Literal(scalar), _) => Ok(scalar.to_array(batch.num_rows())?),
        (Column(name), _) => extract_column(batch, name),
        (Struct(fields), Some(DataType::Struct(output_schema))) => {
            let columns = fields
                .iter()
                .zip(output_schema.fields())
                .map(|(expr, field)| evaluate_expression(expr, batch, Some(field.data_type())));
            let output_cols: Vec<ArrayRef> = columns.try_collect()?;
            let output_fields: Vec<ArrowField> = output_cols
                .iter()
                .zip(output_schema.fields())
                .map(|(output_col, output_field)| -> DeltaResult<_> {
                    Ok(ArrowField::new(
                        output_field.name(),
                        output_col.data_type().clone(),
                        output_col.is_nullable(),
                    ))
                })
                .try_collect()?;
            let result = StructArray::try_new(output_fields.into(), output_cols, None)?;
            Ok(Arc::new(result))
        }
        (Struct(_), _) => Err(Error::generic(
            "Data type is required to evaluate struct expressions",
        )),
        (Unary(UnaryExpression { op, expr }), _) => {
            let arr = evaluate_expression(expr.as_ref(), batch, None)?;
            let result = match op {
                UnaryOperator::Not => not(downcast_to_bool(&arr)?)?,
                UnaryOperator::IsNull => is_null(&arr)?,
            };
            Ok(Arc::new(result))
        }
        (
            Binary(BinaryExpression {
                op: In,
                left,
                right,
            }),
            None | Some(&DataType::BOOLEAN),
        ) => in_list::eval_in_list(batch, left, right),
        (
            Binary(BinaryExpression {
                op: NotIn,
                left,
                right,
            }),
            None | Some(&DataType::BOOLEAN),
        ) => {
            let reverse_op = Expression::binary(In, *left.clone(), *right.clone());
            let reverse_expr = evaluate_expression(&reverse_op, batch, None)?;
            let result = not(reverse_expr.as_boolean())?;
            Ok(wrap_comparison_result(result))
        }
        (Binary(BinaryExpression { op, left, right }), _) => {
            let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
            let right_arr = evaluate_expression(right.as_ref(), batch, None)?;

            type Operation = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;
            let eval: Operation = match op {
                Plus => add,
                Minus => sub,
                Multiply => mul,
                Divide => div,
                LessThan => |l, r| lt(l, r).map(wrap_comparison_result),
                LessThanOrEqual => |l, r| lt_eq(l, r).map(wrap_comparison_result),
                GreaterThan => |l, r| gt(l, r).map(wrap_comparison_result),
                GreaterThanOrEqual => |l, r| gt_eq(l, r).map(wrap_comparison_result),
                Equal => |l, r| eq(l, r).map(wrap_comparison_result),
                NotEqual => |l, r| neq(l, r).map(wrap_comparison_result),
                Distinct => |l, r| distinct(l, r).map(wrap_comparison_result),
                // NOTE: [Not]In was already covered above
                In | NotIn => return Err(Error::generic("Invalid expression given")),
            };

            Ok(eval(&left_arr, &right_arr)?)
        }
        (Junction(JunctionExpression { op, exprs }), None | Some(&DataType::BOOLEAN)) => {
            type Operation = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>;
            let (reducer, default): (Operation, _) = match op {
                JunctionOperator::And => (and_kleene, true),
                JunctionOperator::Or => (or_kleene, false),
            };
            exprs
                .iter()
                .map(|expr| evaluate_expression(expr, batch, result_type))
                .reduce(|l, r| {
                    let result = reducer(downcast_to_bool(&l?)?, downcast_to_bool(&r?)?)?;
                    Ok(wrap_comparison_result(result))
                })
                .unwrap_or_else(|| {
                    evaluate_expression(&Expression::literal(default), batch, result_type)
                })
        }
        (Junction(_), _) => Err(Error::Generic(format!(
            "Junction {expression:?} is expected to return boolean results, got {result_type:?}"
        ))),
    }
}
