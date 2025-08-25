//! Expression handling based on arrow-rs compute kernels.
use crate::arrow::array::types::*;
use crate::arrow::array::{
    make_array, Array, ArrayData, ArrayRef, AsArray, BooleanArray, Datum, MutableArrayData,
    RecordBatch, StringArray, StringBuilder, StructArray,
};
use crate::arrow::compute::kernels::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct};
use crate::arrow::compute::kernels::comparison::in_list_utf8;
use crate::arrow::compute::kernels::numeric::{add, div, mul, sub};
use crate::arrow::compute::{and_kleene, cast, is_not_null, is_null, not, or_kleene};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::arrow::json::writer::{make_encoder, EncoderOptions};

use crate::engine::arrow_conversion::TryIntoArrow;
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpressionOpAdaptor, ArrowOpaquePredicateOpAdaptor,
};
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    JunctionPredicate, JunctionPredicateOp, OpaqueExpression, OpaquePredicate, Predicate, Scalar,
    UnaryExpression, UnaryExpressionOp, UnaryPredicate, UnaryPredicateOp, VariadicExpression,
    VariadicExpressionOp,
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
    use UnaryExpressionOp::*;
    use VariadicExpressionOp::*;
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
        (Unary(UnaryExpression { op: ToJson, expr }), result_type) => match result_type {
            None | Some(&DataType::STRING) => {
                let input = evaluate_expression(expr, batch, None)?;
                Ok(to_json(&input)?)
            }
            Some(data_type) => Err(Error::generic(format!(
                "ToJson operator requires STRING output, but got {data_type:?}"
            ))),
        },
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
        (
            Variadic(VariadicExpression {
                op: Coalesce,
                exprs,
            }),
            result_type,
        ) => {
            let arrays: Vec<ArrayRef> = exprs
                .iter()
                .map(|expr| evaluate_expression(expr, batch, None))
                .try_collect()?;
            Ok(coalesce_arrays(&arrays, result_type)?)
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

/// Converts a StructArray to JSON-encoded strings
pub fn to_json(input: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    let (array_ref, _is_scalar) = input.get();
    match array_ref.data_type() {
        ArrowDataType::Struct(_) => {
            let struct_array = array_ref.as_struct_opt().ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Expected struct array but got different type".to_string(),
                )
            })?;

            let num_rows = struct_array.len();
            if num_rows == 0 {
                return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())));
            }

            // Create the encoder using make_encoder with "struct mode" (not "list mode")
            let field = Arc::new(ArrowField::new_struct(
                "root",
                struct_array.fields().iter().cloned().collect::<Vec<_>>(),
                true,
            ));
            let options = EncoderOptions::default()
                .with_struct_mode(crate::arrow::json::StructMode::ObjectOnly);
            let mut encoder = make_encoder(&field, struct_array, &options)?;

            // Pre-allocate buffer and create string builder
            const ROW_SIZE_ESTIMATE: usize = 64;
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * ROW_SIZE_ESTIMATE);
            let mut json_buffer = Vec::with_capacity(ROW_SIZE_ESTIMATE);

            for i in 0..num_rows {
                if struct_array.is_null(i) {
                    builder.append_null();
                } else {
                    // Clear and reuse buffer for this row
                    json_buffer.clear();

                    // Encode this row to JSON
                    encoder.encode(i, &mut json_buffer);

                    // Convert to string and append to builder
                    let json_str = std::str::from_utf8(&json_buffer).map_err(|e| {
                        ArrowError::InvalidArgumentError(format!("Invalid UTF-8: {e}"))
                    })?;

                    builder.append_value(json_str);
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "TO_JSON can only be applied to struct arrays, got {:?}",
            array_ref.data_type()
        ))),
    }
}

/// Coalesce multiple arrays into one.
///
/// For each row, picks the first non-null value across the arrays. If the arrays are
/// of different types, they must be castable to a common supertype.
/// If `result_type` is provided, it must match the common supertype derived by [`common_supertype`].
pub fn coalesce_arrays(
    arrays: &[ArrayRef],
    result_type: Option<&DataType>,
) -> Result<ArrayRef, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Need at least one array".into(),
        ));
    }

    // Early exit for single array case
    if arrays.len() == 1 {
        let array = &arrays[0];

        // Validate result type if provided
        if let Some(result_type) = result_type {
            let result_arrow_type: ArrowDataType = result_type.try_into_arrow()?;
            if array.data_type() != &result_arrow_type {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Single array type {:?} does not match requested result type {result_type:?}",
                    array.data_type()
                )));
            }
        }

        return Ok(array.clone());
    }

    // Validate all arrays have the same length and collect types
    let num_rows = arrays[0].len();
    let mut types = Vec::with_capacity(arrays.len());
    for (i, arr) in arrays.iter().enumerate() {
        if arr.len() != num_rows {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Array at index {i} has length {}, expected {num_rows}",
                arr.len()
            )));
        }
        types.push(arr.data_type());
    }

    // Find common type
    let supertype = common_supertype(&types).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Could not find common supertype for types: {types:?}"
        ))
    })?;

    // Validate result type if provided
    if let Some(result_type) = result_type {
        let result_arrow_type: ArrowDataType = result_type.try_into_arrow()?;
        if result_arrow_type != supertype {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Requested result type {result_type:?} does not match common supertype {supertype:?}"
            )));
        }
    }

    // Cast arrays to common supertype and collect ArrayData for MutableArrayData
    let (casted, array_data): (Vec<ArrayRef>, Vec<ArrayData>) = arrays
        .iter()
        .map(|arr| {
            let casted_arr = if arr.data_type() == &supertype {
                Ok(arr.clone())
            } else {
                cast(arr, &supertype)
            };
            casted_arr.map(|arr| {
                let data = arr.to_data();
                (arr, data)
            })
        })
        .collect::<Result<Vec<_>, ArrowError>>()?
        .into_iter()
        .unzip();

    // Build result
    let mut mutable = MutableArrayData::new(array_data.iter().collect(), false, num_rows);
    for row in 0..num_rows {
        // Find first non-null value for this row
        match casted.iter().enumerate().find(|(_, arr)| arr.is_valid(row)) {
            Some((array_idx, _)) => {
                mutable.extend(array_idx, row, row + 1);
            }
            None => {
                mutable.extend_nulls(1);
            }
        }
    }

    Ok(make_array(mutable.freeze()))
}

/// Find a common supertype for a list of DataTypes via simple heuristics.
///
/// We currently only handle a limited set of types and just pick the "widest" type by Arrow's cast rules.
fn common_supertype(types: &[&ArrowDataType]) -> Option<ArrowDataType> {
    if types.is_empty() {
        return None;
    }

    // Try pairwise promotion
    types.iter().skip(1).try_fold(types[0].clone(), |acc, &t| {
        if acc == *t {
            Some(acc)
        } else {
            promote_types(&acc, t)
        }
    })
}

/// Promote two types to their common supertype
fn promote_types(left: &ArrowDataType, right: &ArrowDataType) -> Option<ArrowDataType> {
    use ArrowDataType::*;

    match (left, right) {
        // Same types
        (a, b) if a == b => Some(a.clone()),

        // Numeric type promotion (more specific patterns first)
        // We always upcast to Float64 if floats and integers are mixed
        (Float64, _) | (_, Float64) => Some(Float64),
        (Float32, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32) => {
            Some(Float64)
        }
        (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Float32) => Some(Float64),
        (Float32, _) | (_, Float32) => Some(Float32),

        // Integer promotion - signed takes precedence and we promote to wider types
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32) => Some(Int32),
        (Int8 | Int16 | UInt8 | UInt16 | UInt32, Int32) => Some(Int32),
        (Int16, Int8 | Int16 | UInt8 | UInt16) => Some(Int16),
        (Int8 | UInt8 | UInt16, Int16) => Some(Int16),
        (Int8, Int8 | UInt8) => Some(Int8),
        (UInt8, Int8) => Some(Int8),

        // Unsigned integer promotion
        (UInt64, UInt8 | UInt16 | UInt32 | UInt64) => Some(UInt64),
        (UInt8 | UInt16 | UInt32, UInt64) => Some(UInt64),
        (UInt32, UInt8 | UInt16 | UInt32) => Some(UInt32),
        (UInt8 | UInt16, UInt32) => Some(UInt32),
        (UInt16, UInt8 | UInt16) => Some(UInt16),
        (UInt8, UInt16) => Some(UInt16),

        // String types
        (Utf8, _) | (_, Utf8) => Some(Utf8),

        // Boolean type (no promotion)
        (Boolean, Boolean) => Some(Boolean),

        // No common type found
        _ => None,
    }
}
