//! Expression handling based on arrow-rs compute kernels.
use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;

use crate::arrow::array::types::*;
use crate::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, NullBufferBuilder, RecordBatch, StringArray,
    StructArray,
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
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpressionOpAdaptor, ArrowOpaquePredicateOpAdaptor,
};
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    ExpressionRef, JunctionPredicate, JunctionPredicateOp, OpaqueExpression, OpaquePredicate,
    Predicate, Scalar, Transform, UnaryExpression, UnaryExpressionOp, UnaryPredicate,
    UnaryPredicateOp,
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
    let mut used_insertion_keys = 0;
    let mut used_replacement_keys = 0;

    // Collect output columns directly to avoid creating intermediate Expr::Column instances.
    let mut output_cols = Vec::new();

    // Handle prepends (insertions before any field)
    if let Some(prepend_exprs) = transform.field_insertions.get(&None) {
        for expr in prepend_exprs {
            output_cols.push(evaluate_expression(expr, batch, None)?);
        }
        used_insertion_keys += 1;
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

    // Process each input field in order (unified logic for both cases)
    for input_field in source_data.schema_fields() {
        let field_name = input_field.name().as_ref();

        // Handle the field based on replacement rules
        if let Some(replacement) = transform.field_replacements.get(field_name) {
            if let Some(expr) = replacement {
                output_cols.push(evaluate_expression(expr, batch, None)?);
            } // else no replacement => dropped
            used_replacement_keys += 1;
        } else {
            // Field passes through unchanged - extract based on source type
            output_cols.push(extract_column(source_data, &[field_name])?);
        }

        // Handle insertions after this input field
        let field_name = Some(Cow::Borrowed(field_name));
        if let Some(insertion_exprs) = transform.field_insertions.get(&field_name) {
            for expr in insertion_exprs {
                output_cols.push(evaluate_expression(expr, batch, None)?);
            }
            used_insertion_keys += 1;
        }
    }

    // Validate all transforms were used
    if used_insertion_keys != transform.field_insertions.len() {
        return Err(Error::generic(
            "Some insertion keys don't reference valid input field names",
        ));
    }
    if used_replacement_keys != transform.field_replacements.len() {
        return Err(Error::generic(
            "Some replacement keys don't reference valid input field names",
        ));
    }

    if output_cols.len() != output_schema.fields_len() {
        return Err(Error::generic(format!(
            "Expression count ({}) doesn't match output schema field count ({})",
            output_cols.len(),
            output_schema.fields_len()
        )));
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{ArrayRef, Int32Array, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::expressions::{column_expr_ref, Expression as Expr, Transform};
    use crate::schema::{DataType, StructField, StructType};
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
        let transform = Transform::new();
        let output_schema = StructType::new(vec![
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
        let transform_nested = Transform::new().with_input_path(["nested"]);

        let nested_output_schema = StructType::new(vec![
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

        let mut transform = Transform::new();

        // Replace field 'a' with column reference to 'b'
        transform
            .field_replacements
            .insert("a".to_string(), Some(column_expr_ref!("b")));

        // Drop field 'b'
        transform.field_replacements.insert("b".to_string(), None);

        // Multiple prepends (multiple insertions at same position)
        transform.field_insertions.insert(
            None,
            vec![
                Expr::literal(1).into(),
                Expr::literal(2).into(),
                column_expr_ref!("c"),
            ],
        );

        // Multiple insertions after 'c' (key feature: multiple at same position)
        transform.field_insertions.insert(
            Some(Cow::Borrowed("c")),
            vec![
                Expr::literal(42).into(),
                column_expr_ref!("a"), // references original column a
                Expr::literal(99).into(),
            ],
        );

        let output_schema = StructType::new(vec![
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
        let transform_copy = Transform::new().with_input_path(["nested"]);

        let copy_output_schema = StructType::new(vec![
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
        let mut transform_modify = Transform::new().with_input_path(["nested"]);

        // Replace 'x' field with a literal value
        transform_modify
            .field_replacements
            .insert("x".to_string(), Some(Expr::literal(777).into()));

        // Insert a new field after 'y'
        transform_modify
            .field_insertions
            .insert(Some(Cow::Borrowed("y")), vec![Expr::literal(555).into()]);

        let modify_output_schema = StructType::new(vec![
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
        let transform = Transform::new().with_replaced_field("missing", Expr::literal(1).into());
        let output_schema = StructType::new(vec![StructField::new("a", DataType::INTEGER, false)]);

        let expr = Expr::Transform(transform);
        let result = evaluate_expression(
            &expr,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema.clone()))),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("replacement keys"));

        // Test unused insertion keys
        let mut transform2 = Transform::new();
        transform2.field_insertions.insert(
            Some(Cow::Borrowed("nonexistent")),
            vec![Expr::literal(1).into()],
        );

        let expr2 = Expr::Transform(transform2);
        let result2 = evaluate_expression(
            &expr2,
            &batch,
            Some(&DataType::Struct(Box::new(output_schema.clone()))),
        );
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("insertion keys"));

        // Test column count mismatch
        let transform3 = Transform::new().with_dropped_field("a");

        let wrong_output_schema = StructType::new(vec![
            StructField::new("a", DataType::INTEGER, false), // expects a field that was dropped
            StructField::new("b", DataType::INTEGER, false),
            StructField::new("c", DataType::INTEGER, false),
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
            .contains("Expression count"));

        // Test missing output schema
        let transform4 = Transform::new();
        let expr4 = Expr::Transform(transform4);
        let result4 = evaluate_expression(&expr4, &batch, None);
        assert!(result4.is_err());
        assert!(result4
            .unwrap_err()
            .to_string()
            .contains("Data type is required"));
    }
}
