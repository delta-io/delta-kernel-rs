//! Expression handling based on arrow-rs compute kernels.
use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;

use crate::arrow::array::types::*;
use crate::arrow::array::{
    make_array, Array, ArrayData, ArrayRef, AsArray, BooleanArray, Datum, MutableArrayData,
    NullBufferBuilder, RecordBatch, StringArray, StructArray,
};
use crate::arrow::buffer::OffsetBuffer;
use crate::arrow::compute::kernels::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct};
use crate::arrow::compute::kernels::comparison::in_list_utf8;
use crate::arrow::compute::kernels::numeric::{add, div, mul, sub};
use crate::arrow::compute::{and_kleene, is_not_null, is_null, not, or_kleene};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, IntervalUnit, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::arrow::json::writer::{make_encoder, EncoderOptions};
use crate::arrow::json::StructMode;
use crate::engine::arrow_conversion::TryIntoArrow;
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpressionOpAdaptor, ArrowOpaquePredicateOpAdaptor,
};
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    ExpressionRef, JunctionPredicate, JunctionPredicateOp, OpaqueExpression, OpaquePredicate,
    Predicate, Scalar, Transform, UnaryExpression, UnaryExpressionOp, UnaryPredicate,
    UnaryPredicateOp, VariadicExpression, VariadicExpressionOp,
};
use crate::schema::{DataType, StructType};

pub(super) trait ProvidesColumnByName {
    fn schema_fields(&self) -> &ArrowFields;
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef>;
}

impl ProvidesColumnByName for RecordBatch {
    fn schema_fields(&self) -> &ArrowFields {
        self.schema_ref().fields()
    }
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn schema_fields(&self) -> &ArrowFields {
        self.fields()
    }
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
pub(super) fn extract_column(
    mut parent: &dyn ProvidesColumnByName,
    col: &[impl AsRef<str>],
) -> DeltaResult<ArrayRef> {
    let mut field_names = col.iter();
    let Some(field_name) = field_names.next() else {
        return Err(ArrowError::SchemaError("Empty column path".to_string()))?;
    };
    let mut field_name = field_name.as_ref();
    loop {
        let child = parent
            .column_by_name(field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("No such field: {field_name}")))?;
        field_name = match field_names.next() {
            Some(name) => name.as_ref(),
            None => return Ok(child.clone()),
        };
        parent = child
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ArrowError::SchemaError(format!("Not a struct: {field_name}")))?;
    }
}

/// Evaluates a struct expression with given field expressions and output schema
fn evaluate_struct_expression(
    fields: &[ExpressionRef],
    batch: &RecordBatch,
    output_schema: &StructType,
) -> DeltaResult<ArrayRef> {
    let output_cols: Vec<ArrayRef> = fields
        .iter()
        .zip(output_schema.fields())
        .map(|(expr, field)| evaluate_expression(expr, batch, Some(field.data_type())))
        .try_collect()?;
    let output_fields: Vec<ArrowField> = output_cols
        .iter()
        .zip(output_schema.fields())
        .map(|(output_col, output_field)| {
            ArrowField::new(
                output_field.name(),
                output_col.data_type().clone(),
                output_col.is_nullable(),
            )
        })
        .collect();
    let data = StructArray::try_new(output_fields.into(), output_cols, None)?;
    Ok(Arc::new(data))
}

/// Evaluates a transform expression by building expressions in input schema order
fn evaluate_transform_expression(
    transform: &Transform,
    batch: &RecordBatch,
    output_schema: &StructType,
) -> DeltaResult<ArrayRef> {
    let mut used_field_transforms = 0;

    // Collect output columns directly to avoid creating intermediate Expr::Column instances.
    let mut output_cols = Vec::new();

    // Helper lambda to get the next output field type
    let mut output_schema_iter = output_schema.fields();
    let mut next_output_type = || {
        output_schema_iter
            .next()
            .map(|field| field.data_type())
            .ok_or_else(|| Error::generic("Too few fields in output schema"))
    };

    // Handle prepends (insertions before any field)
    for expr in &transform.prepended_fields {
        output_cols.push(evaluate_expression(expr, batch, Some(next_output_type()?))?);
    }

    // Extract the input path, if any
    let source_data = transform
        .input_path()
        .map(|path| extract_column(batch, path))
        .transpose()?;

    let source_data: &dyn ProvidesColumnByName = match source_data {
        Some(ref array) => array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::generic("Input path must point to a struct"))?,
        None => batch,
    };

    // Process each input field in order
    for input_field in source_data.schema_fields() {
        let field_name: &str = input_field.name();

        // Any field that isn't replaced passes through unchanged
        let field_transform = transform.field_transforms.get(field_name);
        if !field_transform.is_some_and(|t| t.is_replace) {
            output_cols.push(extract_column(source_data, &[field_name])?);
            let _ = next_output_type()?; // consume and discard the output schema field
        }

        // Process any insertions that come after this field
        if let Some(field_transform) = field_transform {
            for expr in &field_transform.exprs {
                output_cols.push(evaluate_expression(expr, batch, Some(next_output_type()?))?);
            }
            used_field_transforms += 1;
        }
    }

    // Verify that all field transforms were used
    if used_field_transforms != transform.field_transforms.len() {
        return Err(Error::generic(
            "Some field transforms reference invalid input field names",
        ));
    }

    // Verify we consumed all output schema fields
    if output_schema_iter.next().is_some() {
        return Err(Error::generic("Too many fields in output schema"));
    }

    // Build the final struct
    let output_fields: Vec<ArrowField> = output_cols
        .iter()
        .zip(output_schema.fields())
        .map(|(output_col, output_field)| {
            ArrowField::new(
                output_field.name(),
                output_col.data_type().clone(),
                output_col.is_nullable(),
            )
        })
        .collect();
    let data = StructArray::try_new(output_fields.into(), output_cols, None)?;
    Ok(Arc::new(data))
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
            evaluate_struct_expression(fields, batch, output_schema)
        }
        (Struct(_), _) => Err(Error::generic(
            "Data type is required to evaluate struct expressions",
        )),
        (Transform(transform), Some(DataType::Struct(output_schema))) => {
            evaluate_transform_expression(transform, batch, output_schema)
        }
        (Transform(_), _) => Err(Error::generic(
            "Data type is required to evaluate transform expressions",
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
                ArrowError::InvalidArgumentError(format!(
                    "Failed to convert {} to StructArray",
                    array_ref.data_type(),
                ))
            })?;

            let num_rows = struct_array.len();
            if num_rows == 0 {
                return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())));
            }

            // Create the encoder using make_encoder with "struct mode" (not "list mode")
            let field = Arc::new(ArrowField::new_struct(
                "root",
                struct_array.fields().iter().cloned().collect_vec(),
                true,
            ));
            let options = EncoderOptions::default().with_struct_mode(StructMode::ObjectOnly);
            let mut encoder = make_encoder(&field, struct_array, &options)?;

            // Pre-allocate the various buffers
            const ROW_SIZE_ESTIMATE: usize = 64;
            let mut data = Vec::with_capacity(num_rows * ROW_SIZE_ESTIMATE);
            let mut offsets = Vec::with_capacity(num_rows + 1);
            offsets.push(0);
            let mut nulls = NullBufferBuilder::new(num_rows);

            for i in 0..num_rows {
                if struct_array.is_null(i) {
                    nulls.append_null();
                } else {
                    encoder.encode(i, &mut data);
                    nulls.append_non_null();
                }

                // We have to set a valid physical offset even if the entry was null.
                // But it will refer to a 0-byte slice, since we didn't encode any new data.
                let offset = i32::try_from(data.len()).map_err(|_| {
                    ArrowError::InvalidArgumentError("Failed to convert offset".to_string())
                })?;
                offsets.push(offset);
            }

            let array = StringArray::try_new(
                OffsetBuffer::new(offsets.into()),
                data.into(),
                nulls.finish(),
            )?;
            Ok(Arc::new(array))
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "TO_JSON can only be applied to struct arrays, got {:?}",
            array_ref.data_type()
        ))),
    }
}

/// Coalesce multiple arrays into one by selecting the first non-null value from each row.
///
/// This function implements SQL COALESCE semantics: for each row, it iterates through
/// the input arrays from left to right and returns the first non-null value found. If all values
/// are null for a given row, the result will be null for that row.
///
/// # Parameters
/// - `arrays`: Slice of Arrow arrays to coalesce. Must not be empty and all arrays must have the same data type.
/// - `result_type`: Optional expected result type. If provided, must match the arrays' data type.
///
/// # Returns
/// An `ArrayRef` containing the coalesced values with the same number of rows as the input arrays.
///
/// # Errors
/// This function returns an `ArrowError` in the following cases:
/// - **Empty input**: The default engine currently does not support empty COALESCE statements.
/// - **Mismatched row counts**: Not all arrays have the same number of rows.
/// - **Mismatched data types**: Not all arrays have exactly the same data type.
/// - **Invalid result type**: If `result_type` is provided but doesn't match the arrays' data type.
pub fn coalesce_arrays(
    arrays: &[ArrayRef],
    result_type: Option<&DataType>,
) -> Result<ArrayRef, ArrowError> {
    let Some((first, rest)) = arrays.split_first() else {
        return Err(ArrowError::InvalidArgumentError(
            "The default engine currently does not support empty COALESCE statements".into(),
        ));
    };

    // Validate against the expected output type, if provided
    if let Some(result_type) = result_type {
        let result_type = result_type.try_into_arrow()?;
        if first.data_type() != &result_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Requested result type {result_type:?} does not match arrays' data type {:?}",
                first.data_type()
            )));
        }
    }

    // Early exit for single array case
    if rest.is_empty() {
        return Ok(first.clone());
    }

    // Verify all arrays have the same length and data type
    for (i, arr) in rest.iter().enumerate() {
        if arr.len() != first.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Array at index {} has length {}, expected {}",
                i + 1,
                arr.len(),
                first.len()
            )));
        }
        if arr.data_type() != first.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Array at index {} has type {:?}, but expected {:?}",
                i + 1,
                arr.data_type(),
                first.data_type()
            )));
        }
    }

    // Collect ArrayData for MutableArrayData
    let array_data: Vec<ArrayData> = arrays.iter().map(|arr| arr.to_data()).collect();

    // Build result
    let mut mutable = MutableArrayData::new(array_data.iter().collect(), false, first.len());
    for row in 0..first.len() {
        // Find first non-null value for this row
        match arrays.iter().enumerate().find(|(_, arr)| arr.is_valid(row)) {
            Some((array_idx, _)) => mutable.extend(array_idx, row, row + 1),
            None => mutable.extend_nulls(1),
        }
    }

    Ok(make_array(mutable.freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::expressions::{column_expr_ref, Expression as Expr, Transform};
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("b", ArrowDataType::Int32, false),
            ArrowField::new("c", ArrowDataType::Int32, false),
        ]);
        let a_values = Int32Array::from(vec![1, 2, 3]);
        let b_values = Int32Array::from(vec![10, 20, 30]);
        let c_values = Int32Array::from(vec![100, 200, 300]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a_values), Arc::new(b_values), Arc::new(c_values)],
        )
        .unwrap()
    }

    /// Helper function to validate Int32Array columns in test results
    fn validate_i32_column(result: &StructArray, idx: usize, expected: &[i32]) {
        let col = result
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), expected);
    }

    fn create_nested_test_batch() -> RecordBatch {
        let inner_schema = ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, false),
            ArrowField::new("y", ArrowDataType::Int32, false),
        ]);
        let nested_type = ArrowDataType::Struct(inner_schema.fields().clone());
        let schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("nested", nested_type, false),
        ]);

        let x_values = Int32Array::from(vec![1, 2, 3]);
        let y_values = Int32Array::from(vec![10, 20, 30]);
        let nested_struct = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("x", ArrowDataType::Int32, false)),
                Arc::new(x_values) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("y", ArrowDataType::Int32, false)),
                Arc::new(y_values) as ArrayRef,
            ),
        ]);

        let a_values = Int32Array::from(vec![100, 200, 300]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a_values), Arc::new(nested_struct)],
        )
        .unwrap()
    }

    #[test]
    fn test_identity_transforms() {
        let batch = create_test_batch();

        // Test 1: Empty transform (identity) - should be exactly equal to input
        let transform = Transform::new_top_level();
        let output_schema = StructType::new_unchecked(vec![
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::INTEGER, false),
            StructField::new("c", DataType::INTEGER, false),
        ]);

        let expr = Expr::Transform(transform);
        let result = evaluate_expression(
            &expr,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema))),
        )
        .unwrap();

        // For identity transform, output should be identical to input
        let struct_result = result.as_any().downcast_ref::<StructArray>().unwrap();

        // Compare each column directly with original batch columns
        for i in 0..3 {
            assert_eq!(struct_result.column(i).as_ref(), batch.column(i).as_ref());
        }

        // Test 2: Nested path identity (struct relocation without modification)
        let nested_batch = create_nested_test_batch();
        let transform_nested = Transform::new_nested(["nested"]);

        let nested_output_schema = StructType::new_unchecked(vec![
            StructField::new("x", DataType::INTEGER, false),
            StructField::new("y", DataType::INTEGER, false),
        ]);

        let expr_nested = Expr::Transform(transform_nested);
        let result_nested = evaluate_expression(
            &expr_nested,
            &nested_batch,
            Some(&DataType::Struct(Box::new(nested_output_schema))),
        )
        .unwrap();

        // Extract the original nested struct for comparison
        let original_nested = nested_batch
            .column_by_name("nested")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let nested_result = result_nested
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Compare each column from nested struct directly
        for i in 0..2 {
            assert_eq!(
                nested_result.column(i).as_ref(),
                original_nested.column(i).as_ref()
            );
        }
    }

    #[test]
    fn test_field_operations_and_multiple_insertions() {
        let batch = create_test_batch();

        let transform = Transform::new_top_level()
            .with_replaced_field("a", column_expr_ref!("b"))
            .with_dropped_field("b")
            .with_inserted_field(None::<&str>, Expr::literal(1).into())
            .with_inserted_field(None::<&str>, Expr::literal(2).into())
            .with_inserted_field(None::<&str>, column_expr_ref!("c"))
            .with_inserted_field(Some("c"), Expr::literal(42).into())
            .with_inserted_field(Some("c"), column_expr_ref!("a"))
            .with_inserted_field(Some("c"), Expr::literal(99).into());

        let output_schema = StructType::new_unchecked(vec![
            StructField::new("pre1", DataType::INTEGER, false), // prepend 1
            StructField::new("pre2", DataType::INTEGER, false), // prepend 2
            StructField::new("pre3", DataType::INTEGER, false), // prepend 3 (column c)
            StructField::new("a", DataType::INTEGER, false),    // replaced with column b
            StructField::new("c", DataType::INTEGER, false),    // passed through
            StructField::new("after_c1", DataType::INTEGER, false), // first insertion after c
            StructField::new("after_c2", DataType::INTEGER, false), // second insertion after c
            StructField::new("after_c3", DataType::INTEGER, false), // third insertion after c
        ]);

        let expr = Expr::Transform(transform);
        let result = evaluate_expression(
            &expr,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema))),
        )
        .unwrap();

        let struct_result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_result.num_columns(), 8);
        assert_eq!(struct_result.len(), 3);

        // Verify multiple prepends (in order)
        validate_i32_column(struct_result, 0, &[1, 1, 1]);
        validate_i32_column(struct_result, 1, &[2, 2, 2]);
        validate_i32_column(struct_result, 2, &[100, 200, 300]); // column c

        // Verify replaced field 'a' (should be column b values: [10, 20, 30])
        validate_i32_column(struct_result, 3, &[10, 20, 30]);

        // Verify passthrough field 'c' (should be original c values: [100, 200, 300])
        validate_i32_column(struct_result, 4, &[100, 200, 300]);

        // Verify multiple insertions after c (in order)
        validate_i32_column(struct_result, 5, &[42, 42, 42]);
        validate_i32_column(struct_result, 6, &[1, 2, 3]); // original column a
        validate_i32_column(struct_result, 7, &[99, 99, 99]);
    }

    #[test]
    fn test_nested_path_transforms() {
        let nested_batch = create_nested_test_batch();

        // Test 1: Simple struct relocation (copy nested struct to top level unchanged)
        let transform_copy = Transform::new_nested(["nested"]);

        let copy_output_schema = StructType::new_unchecked(vec![
            StructField::new("x", DataType::INTEGER, false),
            StructField::new("y", DataType::INTEGER, false),
        ]);

        let expr_copy = Expr::Transform(transform_copy);
        let result_copy = evaluate_expression(
            &expr_copy,
            &nested_batch,
            Some(&DataType::Struct(Box::new(copy_output_schema))),
        )
        .unwrap();

        // Verify the copy is identical to original nested struct
        let copy_result = result_copy.as_any().downcast_ref::<StructArray>().unwrap();
        let original_nested = nested_batch
            .column_by_name("nested")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        for i in 0..2 {
            assert_eq!(
                copy_result.column(i).as_ref(),
                original_nested.column(i).as_ref()
            );
        }

        // Test 2: Modify nested struct and relocate it
        let transform_modify = Transform::new_nested(["nested"])
            .with_replaced_field("x".to_string(), Expr::literal(777).into())
            .with_inserted_field(Some("y"), Expr::literal(555).into());

        let modify_output_schema = StructType::new_unchecked(vec![
            StructField::new("x", DataType::INTEGER, false), // replaced with literal 777
            StructField::new("y", DataType::INTEGER, false), // passed through
            StructField::new("new_field", DataType::INTEGER, false), // inserted after y
        ]);

        let expr_modify = Expr::Transform(transform_modify);
        let result_modify = evaluate_expression(
            &expr_modify,
            &nested_batch,
            Some(&DataType::Struct(Box::new(modify_output_schema))),
        )
        .unwrap();

        let modify_result = result_modify
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(modify_result.num_columns(), 3);
        assert_eq!(modify_result.len(), 3);

        // Verify replaced 'x' field (literal 777)
        validate_i32_column(modify_result, 0, &[777, 777, 777]);

        // Verify passthrough 'y' field (original nested.y: [10, 20, 30])
        validate_i32_column(modify_result, 1, &[10, 20, 30]);

        // Verify inserted field (literal 555)
        validate_i32_column(modify_result, 2, &[555, 555, 555]);
    }

    #[test]
    fn test_transform_validation() {
        let batch = create_test_batch();

        // Test unused replacement keys
        let transform =
            Transform::new_top_level().with_replaced_field("missing", Expr::literal(1).into());
        let output_schema = StructType::new_unchecked(vec![
            StructField::not_null("a", DataType::INTEGER),
            StructField::not_null("b", DataType::INTEGER),
            StructField::not_null("c", DataType::INTEGER),
        ]);

        let expr = Expr::Transform(transform);
        let result = evaluate_expression(
            &expr,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema.clone()))),
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("reference invalid input field names"));

        // Test unused insertion keys
        let transform2 = Transform::new_top_level()
            .with_inserted_field(Some("nonexistent"), Expr::literal(1).into());

        let expr2 = Expr::Transform(transform2);
        let result2 = evaluate_expression(
            &expr2,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema.clone()))),
        );
        assert!(result2.is_err());
        assert!(result2
            .unwrap_err()
            .to_string()
            .contains("reference invalid input field names"));

        // Test column count mismatch -- too many output schema fields
        let transform3 = Transform::new_top_level().with_dropped_field("a");

        let wrong_output_schema = StructType::new_unchecked(vec![
            StructField::not_null("a", DataType::INTEGER), // expects a field that was dropped
            StructField::not_null("b", DataType::INTEGER),
            StructField::not_null("c", DataType::INTEGER),
        ]);

        let expr3 = Expr::Transform(transform3);
        let result3 = evaluate_expression(
            &expr3,
            &batch,
            Some(&DataType::Struct(Box::new(wrong_output_schema))),
        );
        assert!(result3.is_err());
        assert!(result3
            .unwrap_err()
            .to_string()
            .contains("Too many fields in output schema"));

        // Test column count mismatch -- too few output schema fields
        let transform3 = Transform::new_top_level().with_dropped_field("a");

        let wrong_output_schema =
            StructType::new_unchecked(vec![StructField::not_null("c", DataType::INTEGER)]);

        let expr3 = Expr::Transform(transform3);
        let result3 = evaluate_expression(
            &expr3,
            &batch,
            Some(&DataType::Struct(Box::new(wrong_output_schema))),
        );
        assert!(result3.is_err());
        assert!(result3
            .unwrap_err()
            .to_string()
            .contains("Too few fields in output schema"));

        // Test missing output schema
        let transform4 = Transform::new_top_level();
        let expr4 = Expr::Transform(transform4);
        let result4 = evaluate_expression(&expr4, &batch, None);
        assert!(result4.is_err());
        assert!(result4
            .unwrap_err()
            .to_string()
            .contains("Data type is required"));
    }

    #[test]
    fn test_coalesce_arrays_same_type() {
        // Test with Int32 arrays
        let arr1 = Int32Array::from(vec![Some(1), None, Some(3), None, None, Some(8), None]);
        let arr2 = Int32Array::from(vec![None, Some(2), Some(4), None, Some(6), None, None]);
        let arr3 = Int32Array::from(vec![None, None, None, Some(5), Some(7), Some(9), None]);

        let result =
            coalesce_arrays(&[Arc::new(arr1), Arc::new(arr2), Arc::new(arr3)], None).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result_array.len(), 7);
        assert_eq!(result_array.value(0), 1); // From arr1
        assert_eq!(result_array.value(1), 2); // From arr2
        assert_eq!(result_array.value(2), 3); // From arr1
        assert_eq!(result_array.value(3), 5); // From arr3
        assert_eq!(result_array.value(4), 6); // From arr2
        assert_eq!(result_array.value(5), 8); // From arr1
        assert!(result_array.is_null(6));

        // Test with String arrays
        let str_arr1 = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
        let str_arr2 = Arc::new(StringArray::from(vec![None, Some("b"), None]));

        let str_result = coalesce_arrays(&[str_arr1, str_arr2], None).unwrap();
        let str_result_array = str_result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(str_result_array.len(), 3);
        assert_eq!(str_result_array.value(0), "a"); // From str_arr1
        assert_eq!(str_result_array.value(1), "b"); // From str_arr2
        assert_eq!(str_result_array.value(2), "c"); // From str_arr1
    }

    #[test]
    fn test_coalesce_arrays_all_nulls() {
        let arr1 = Arc::new(Int32Array::from(vec![None, None, None]));
        let arr2 = Arc::new(Int32Array::from(vec![None, None, None]));

        let result = coalesce_arrays(&[arr1, arr2], None).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result_array.len(), 3);
        assert!(result_array.is_null(0));
        assert!(result_array.is_null(1));
        assert!(result_array.is_null(2));
    }

    #[test]
    fn test_coalesce_arrays_single_array() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let result = coalesce_arrays(std::slice::from_ref(&arr), None).unwrap();

        // Should return the same array
        assert_eq!(result.as_ref(), arr.as_ref());
    }

    #[test]
    fn test_coalesce_arrays_type_mismatch_error() {
        // Test Int32 vs Int64 - should fail
        let int32_arr = Arc::new(Int32Array::from(vec![Some(1), None]));
        let int64_arr = Arc::new(Int64Array::from(vec![None, Some(2)]));

        let result = coalesce_arrays(&[int32_arr, int64_arr], None);
        assert_result_error_with_message(
            result,
            "Array at index 1 has type Int64, but expected Int32",
        );

        // Test Int32 vs String - should fail
        let int_arr = Arc::new(Int32Array::from(vec![Some(1)]));
        let str_arr = Arc::new(StringArray::from(vec![Some("hello")]));

        let result2 = coalesce_arrays(&[int_arr, str_arr], None);
        assert_result_error_with_message(
            result2,
            "Array at index 1 has type Utf8, but expected Int32",
        );
    }

    #[test]
    fn test_coalesce_arrays_length_mismatch_error() {
        // Test arrays with different lengths - should fail
        let arr1 = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let arr2 = Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)]));

        let result = coalesce_arrays(&[arr1, arr2], None);
        assert_result_error_with_message(result, "Array at index 1 has length 3, expected 2");
    }

    #[test]
    fn test_coalesce_arrays_empty_input_error() {
        // Test with empty arrays slice - should fail
        let result = coalesce_arrays(&[], None);
        assert_result_error_with_message(result, "empty COALESCE statements");
    }

    #[test]
    fn test_coalesce_arrays_result_type_validation() {
        let arr1 = Arc::new(Int32Array::from(vec![Some(1), None]));
        let arr2 = Arc::new(Int32Array::from(vec![None, Some(2)]));

        // Test with matching result type - should succeed
        let result = coalesce_arrays(&[arr1.clone(), arr2.clone()], Some(&DataType::INTEGER));
        assert!(result.is_ok());

        // Test with mismatched result type - should fail
        let result2 = coalesce_arrays(&[arr1, arr2], Some(&DataType::STRING));
        assert_result_error_with_message(
            result2,
            "Requested result type Utf8 does not match arrays' data type Int32",
        );
    }

    #[test]
    fn test_nested_transforms() {
        let nested_batch = create_nested_test_batch();

        // Simple nested transform - replace a field in the nested struct
        let nested_transform =
            Transform::new_nested(["nested"]).with_replaced_field("x", Expr::literal(999).into());

        let outer_transform = Transform::new_top_level()
            .with_inserted_field(Some("a"), Expr::Transform(nested_transform).into());

        let nested_output_schema = StructType::new_unchecked(vec![
            StructField::not_null("x", DataType::INTEGER),
            StructField::not_null("y", DataType::INTEGER),
        ]);
        let output_schema = StructType::new_unchecked(vec![
            StructField::not_null("a", DataType::INTEGER),
            StructField::not_null("transformed", nested_output_schema.clone()),
            StructField::not_null("nested", nested_output_schema),
        ]);

        let expr = Expr::Transform(outer_transform);
        let result = evaluate_expression(
            &expr,
            &nested_batch,
            Some(&DataType::Struct(Box::new(output_schema))),
        )
        .unwrap();

        let struct_result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_result.num_columns(), 3);
        assert_eq!(struct_result.len(), 3);

        // Verify original field 'a' (should be [100, 200, 300])
        validate_i32_column(struct_result, 0, &[100, 200, 300]);

        // Verify nested transform replaced 'x' with literal 999 and passed through 'y' unchanged.
        let nested_struct_result = struct_result
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        validate_i32_column(nested_struct_result, 0, &[999, 999, 999]);
        validate_i32_column(nested_struct_result, 1, &[10, 20, 30]);

        // Verify nested transform passed both 'x' and 'y' unchanged.
        let nested_struct_result = struct_result
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        validate_i32_column(nested_struct_result, 0, &[1, 2, 3]);
        validate_i32_column(nested_struct_result, 1, &[10, 20, 30]);
    }
}
