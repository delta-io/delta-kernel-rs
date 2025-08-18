//! Expression handling based on arrow-rs compute kernels.
use crate::arrow::array::types::*;
use crate::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, RecordBatch, StructArray,
};
use crate::arrow::compute::kernels::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct};
use crate::arrow::compute::kernels::comparison::in_list_utf8;
use crate::arrow::compute::kernels::numeric::{add, div, mul, sub};
use crate::arrow::compute::{and_kleene, is_not_null, is_null, not, or_kleene};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpressionOpAdaptor, ArrowOpaquePredicateOpAdaptor,
};
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    JunctionPredicate, JunctionPredicateOp, OpaqueExpression, OpaquePredicate, Predicate, Scalar,
    UnaryPredicate, UnaryPredicateOp,
};
use crate::schema::DataType;
use itertools::Itertools;
use std::borrow::Cow;
use std::sync::Arc;

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

/// Evaluates a kernel expression over a record batch
pub fn evaluate_expression(
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
            let result = evaluate_predicate(pred, batch, false)?;
            Ok(Arc::new(result))
        }
        (Predicate(_), Some(data_type)) => Err(Error::generic(format!(
            "Predicate evaluation produces boolean output, but caller expects {data_type:?}"
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
        (Opaque(OpaqueExpression { op, exprs }), _) => {
            match op
                .any_ref()
                .downcast_ref::<ArrowOpaqueExpressionOpAdaptor>()
            {
                Some(op) => op.eval_expr(exprs, batch, result_type),
                None => Err(Error::unsupported(format!(
                    "Unsupported opaque expression: {op:?}"
                ))),
            }
        }
        (Unknown(name), _) => Err(Error::unsupported(format!("Unknown expression: {name:?}"))),
    }
}

/// Evaluates a (possibly inverted) kernel predicate over a record batch
pub fn evaluate_predicate(
    predicate: &Predicate,
    batch: &RecordBatch,
    inverted: bool,
) -> DeltaResult<BooleanArray> {
    use BinaryPredicateOp::*;
    use Predicate::*;

    // Helper to conditionally invert results of arrow operations if we couldn't push down the NOT.
    let maybe_inverted = |result: Cow<'_, BooleanArray>| match inverted {
        true => not(&result),
        false => Ok(result.into_owned()),
    };

    match predicate {
        BooleanExpression(expr) => {
            // Grr -- there's no way to cast an `Arc<dyn Array>` back to its native type, so we
            // can't use `Arc::into_inner` here and must clone instead. At least the inner `Buffer`
            // instances are still cheaply clonable.
            let arr = evaluate_expression(expr, batch, Some(&DataType::BOOLEAN))?;
            match arr.as_any().downcast_ref::<BooleanArray>() {
                Some(arr) => Ok(maybe_inverted(Cow::Borrowed(arr))?),
                None => Err(Error::generic("expected boolean array")),
            }
        }
        Not(pred) => evaluate_predicate(pred, batch, !inverted),
        Unary(UnaryPredicate { op, expr }) => {
            let arr = evaluate_expression(expr.as_ref(), batch, None)?;
            let eval_op_fn = match (op, inverted) {
                (UnaryPredicateOp::IsNull, false) => is_null,
                (UnaryPredicateOp::IsNull, true) => is_not_null,
            };
            Ok(eval_op_fn(&arr)?)
        }
        Binary(BinaryPredicate { op, left, right }) => {
            let (left, right) = (left.as_ref(), right.as_ref());

            // IN is different from all the others, and also quite complex, so factor it out.
            //
            // TODO: Factor out as a stand-alone function instead of a closure?
            let eval_in = || match (left, right) {
                (Expression::Literal(_), Expression::Column(_)) => {
                    let left = evaluate_expression(left, batch, None)?;
                    let right = evaluate_expression(right, batch, None)?;
                    if let Some(string_arr) = left.as_string_opt::<i32>() {
                        if let Some(list_arr) = right.as_list_opt::<i32>() {
                            let result = in_list_utf8(string_arr, list_arr)?;
                            return Ok(result);
                        }
                    }

                    use ArrowDataType::*;
                    prim_array_cmp! {
                        left, right,
                        (Int8, Int8Type),
                        (Int16, Int16Type),
                        (Int32, Int32Type),
                        (Int64, Int64Type),
                        (UInt8, UInt8Type),
                        (UInt16, UInt16Type),
                        (UInt32, UInt32Type),
                        (UInt64, UInt64Type),
                        (Float16, Float16Type),
                        (Float32, Float32Type),
                        (Float64, Float64Type),
                        (Timestamp(TimeUnit::Second, _), TimestampSecondType),
                        (Timestamp(TimeUnit::Millisecond, _), TimestampMillisecondType),
                        (Timestamp(TimeUnit::Microsecond, _), TimestampMicrosecondType),
                        (Timestamp(TimeUnit::Nanosecond, _), TimestampNanosecondType),
                        (Date32, Date32Type),
                        (Date64, Date64Type),
                        (Time32(TimeUnit::Second), Time32SecondType),
                        (Time32(TimeUnit::Millisecond), Time32MillisecondType),
                        (Time64(TimeUnit::Microsecond), Time64MicrosecondType),
                        (Time64(TimeUnit::Nanosecond), Time64NanosecondType),
                        (Duration(TimeUnit::Second), DurationSecondType),
                        (Duration(TimeUnit::Millisecond), DurationMillisecondType),
                        (Duration(TimeUnit::Microsecond), DurationMicrosecondType),
                        (Duration(TimeUnit::Nanosecond), DurationNanosecondType),
                        (Interval(IntervalUnit::DayTime), IntervalDayTimeType),
                        (Interval(IntervalUnit::YearMonth), IntervalYearMonthType),
                        (Interval(IntervalUnit::MonthDayNano), IntervalMonthDayNanoType),
                        (Decimal128(_, _), Decimal128Type),
                        (Decimal256(_, _), Decimal256Type)
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
            };

            let eval_fn = match (op, inverted) {
                (LessThan, false) => lt,
                (LessThan, true) => gt_eq,
                (GreaterThan, false) => gt,
                (GreaterThan, true) => lt_eq,
                (Equal, false) => eq,
                (Equal, true) => neq,
                (Distinct, false) => distinct,
                (Distinct, true) => not_distinct,
                (In, _) => return Ok(maybe_inverted(Cow::Owned(eval_in()?))?),
            };

            let left = evaluate_expression(left, batch, None)?;
            let right = evaluate_expression(right, batch, None)?;
            Ok(eval_fn(&left, &right)?)
        }
        Junction(JunctionPredicate { op, preds }) => {
            // Leverage de Morgan's laws (invert the children and swap the operator):
            // NOT(AND(A, B)) = OR(NOT(A), NOT(B))
            // NOT(OR(A, B)) = AND(NOT(A), NOT(B))
            //
            // In case of an empty junction, we return a default value of TRUE (FALSE) for AND (OR),
            // as a "hidden" extra child: AND(TRUE, ...) = AND(...) and OR(FALSE, ...) = OR(...).
            use JunctionPredicateOp::*;
            type Operation = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>;
            let (reducer, default): (Operation, _) = match (op, inverted) {
                (And, false) | (Or, true) => (and_kleene, true),
                (Or, false) | (And, true) => (or_kleene, false),
            };
            preds
                .iter()
                .map(|pred| evaluate_predicate(pred, batch, inverted))
                .reduce(|l, r| Ok(reducer(&l?, &r?)?))
                .unwrap_or_else(|| Ok(BooleanArray::from(vec![default; batch.num_rows()])))
        }
        Opaque(OpaquePredicate { op, exprs }) => {
            match op.any_ref().downcast_ref::<ArrowOpaquePredicateOpAdaptor>() {
                Some(op) => op.eval_pred(exprs, batch, inverted),
                None => Err(Error::unsupported(format!(
                    "Unsupported opaque predicate: {op:?}"
                ))),
            }
        }
        Unknown(name) => Err(Error::unsupported(format!("Unknown predicate: {name:?}"))),
    }
}
