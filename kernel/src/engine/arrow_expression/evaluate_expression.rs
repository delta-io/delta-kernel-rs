//! Expression handling based on arrow-rs compute kernels.
use crate::arrow::array::types::*;
use crate::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, RecordBatch, StructArray,
};
use crate::arrow::compute::kernels::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq};
use crate::arrow::compute::kernels::comparison::in_list_utf8;
use crate::arrow::compute::kernels::numeric::{add, div, mul, sub};
use crate::arrow::compute::{and_kleene, is_null, not, or_kleene};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    JunctionPredicate, JunctionPredicateOp, Predicate, Scalar, UnaryPredicate, UnaryPredicateOp,
};
use crate::schema::DataType;
use itertools::Itertools;
use std::sync::Arc;

fn downcast_to_bool(arr: &dyn Array) -> DeltaResult<&BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::generic("expected boolean array"))
}

trait ProvidesColumnByName {
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
fn extract_column(mut parent: &dyn ProvidesColumnByName, col: &[String]) -> DeltaResult<ArrayRef> {
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
    use BinaryExpressionOp::*;
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
        (Predicate(pred), None | Some(&DataType::BOOLEAN)) => {
            let result = evaluate_predicate(pred, batch)?;
            Ok(Arc::new(result))
        }
        (Predicate(_), Some(data_type)) => Err(Error::generic(format!(
            "Unexpected data type: {data_type:?}"
        ))),
        (Binary(BinaryExpression { op, left, right }), _) => {
            let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
            let right_arr = evaluate_expression(right.as_ref(), batch, None)?;

            type Operation = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;
            let eval: Operation = match op {
                Plus => add,
                Minus => sub,
                Multiply => mul,
                Divide => div,
            };

            Ok(eval(&left_arr, &right_arr)?)
        }
    }
}

pub(crate) fn evaluate_predicate(
    predicate: &Predicate,
    batch: &RecordBatch,
) -> DeltaResult<BooleanArray> {
    use BinaryPredicateOp::*;
    use Predicate::*;
    match predicate {
        BooleanExpression(expr) => {
            // Grr -- there's no way to cast an `Arc<dyn Array>` back to its native type, so we
            // can't use `Arc::into_inner` here and must unconditionally clone instead.
            let arr = evaluate_expression(expr, batch, Some(&DataType::BOOLEAN))?;
            Ok(downcast_to_bool(&arr)?.clone())
        }
        Not(pred) => Ok(not(&evaluate_predicate(pred, batch)?)?),
        Unary(UnaryPredicate { op, expr }) => {
            let arr = evaluate_expression(expr.as_ref(), batch, None)?;
            let result = match op {
                UnaryPredicateOp::IsNull => is_null(&arr)?,
            };
            Ok(result)
        }
        Binary(BinaryPredicate {
            op: In,
            left,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (Expression::Literal(_), Expression::Column(_)) => {
                let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
                let right_arr = evaluate_expression(right.as_ref(), batch, None)?;
                if let Some(string_arr) = left_arr.as_string_opt::<i32>() {
                    if let Some(right_arr) = right_arr.as_list_opt::<i32>() {
                        let result = in_list_utf8(string_arr, right_arr)?;
                        return Ok(result);
                    }
                }
                prim_array_cmp! {
                    left_arr, right_arr,
                    (ArrowDataType::Int8, Int8Type),
                    (ArrowDataType::Int16, Int16Type),
                    (ArrowDataType::Int32, Int32Type),
                    (ArrowDataType::Int64, Int64Type),
                    (ArrowDataType::UInt8, UInt8Type),
                    (ArrowDataType::UInt16, UInt16Type),
                    (ArrowDataType::UInt32, UInt32Type),
                    (ArrowDataType::UInt64, UInt64Type),
                    (ArrowDataType::Float16, Float16Type),
                    (ArrowDataType::Float32, Float32Type),
                    (ArrowDataType::Float64, Float64Type),
                    (ArrowDataType::Timestamp(TimeUnit::Second, _), TimestampSecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Millisecond, _), TimestampMillisecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Microsecond, _), TimestampMicrosecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Nanosecond, _), TimestampNanosecondType),
                    (ArrowDataType::Date32, Date32Type),
                    (ArrowDataType::Date64, Date64Type),
                    (ArrowDataType::Time32(TimeUnit::Second), Time32SecondType),
                    (ArrowDataType::Time32(TimeUnit::Millisecond), Time32MillisecondType),
                    (ArrowDataType::Time64(TimeUnit::Microsecond), Time64MicrosecondType),
                    (ArrowDataType::Time64(TimeUnit::Nanosecond), Time64NanosecondType),
                    (ArrowDataType::Duration(TimeUnit::Second), DurationSecondType),
                    (ArrowDataType::Duration(TimeUnit::Millisecond), DurationMillisecondType),
                    (ArrowDataType::Duration(TimeUnit::Microsecond), DurationMicrosecondType),
                    (ArrowDataType::Duration(TimeUnit::Nanosecond), DurationNanosecondType),
                    (ArrowDataType::Interval(IntervalUnit::DayTime), IntervalDayTimeType),
                    (ArrowDataType::Interval(IntervalUnit::YearMonth), IntervalYearMonthType),
                    (ArrowDataType::Interval(IntervalUnit::MonthDayNano), IntervalMonthDayNanoType),
                    (ArrowDataType::Decimal128(_, _), Decimal128Type),
                    (ArrowDataType::Decimal256(_, _), Decimal256Type)
                }
            }
            (Expression::Literal(lit), Expression::Literal(Scalar::Array(ad))) => {
                #[allow(deprecated)]
                let exists = ad.array_elements().contains(lit);
                Ok(BooleanArray::from(vec![exists]))
            }
            (l, r) => Err(Error::invalid_expression(format!(
                "Invalid right value for (NOT) IN comparison, left is: {l} right is: {r}"
            ))),
        },
        Binary(BinaryPredicate {
            op: NotIn,
            left,
            right,
        }) => {
            let reverse_op = Predicate::binary(In, *left.clone(), *right.clone());
            let reverse_pred = evaluate_predicate(&reverse_op, batch)?;
            Ok(not(&reverse_pred)?)
        }
        Binary(BinaryPredicate { op, left, right }) => {
            let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
            let right_arr = evaluate_expression(right.as_ref(), batch, None)?;

            type Operation = fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>;
            let eval: Operation = match op {
                LessThan => |l, r| lt(l, r),
                LessThanOrEqual => |l, r| lt_eq(l, r),
                GreaterThan => |l, r| gt(l, r),
                GreaterThanOrEqual => |l, r| gt_eq(l, r),
                Equal => |l, r| eq(l, r),
                NotEqual => |l, r| neq(l, r),
                Distinct => |l, r| distinct(l, r),
                // NOTE: [Not]In was already covered above
                In | NotIn => return Err(Error::generic("Invalid expression given")),
            };

            Ok(eval(&left_arr, &right_arr)?)
        }
        Junction(JunctionPredicate { op, preds }) => {
            type Operation = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>;
            let (reducer, default): (Operation, _) = match op {
                JunctionPredicateOp::And => (and_kleene, true),
                JunctionPredicateOp::Or => (or_kleene, false),
            };
            preds
                .iter()
                .map(|pred| evaluate_predicate(pred, batch))
                .reduce(|l, r| Ok(reducer(&l?, &r?)?))
                .unwrap_or_else(|| Ok(BooleanArray::from(vec![default; batch.num_rows()])))
        }
    }
}
