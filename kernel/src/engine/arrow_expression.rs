//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, is_null, not, or_kleene};
use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_ord::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_ord::comparison::in_list_utf8;
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Fields, IntervalUnit,
    Schema as ArrowSchema, TimeUnit,
};
use arrow_select::concat::concat;
use itertools::Itertools;

use super::arrow_conversion::LIST_ARRAY_ROOT;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::ensure_data_types;
use crate::engine::arrow_utils::prim_array_cmp;
use crate::error::{DeltaResult, Error};
use crate::expressions::{BinaryOperator, Expression, Scalar, UnaryOperator, VariadicOperator};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::{EngineData, ExpressionEvaluator, ExpressionHandler};

// TODO leverage scalars / Datum

fn downcast_to_bool(arr: &dyn Array) -> DeltaResult<&BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or(Error::generic("expected boolean array"))
}

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> DeltaResult<ArrayRef> {
        use Scalar::*;
        let arr: ArrayRef = match self {
            Integer(val) => Arc::new(Int32Array::from_value(*val, num_rows)),
            Long(val) => Arc::new(Int64Array::from_value(*val, num_rows)),
            Short(val) => Arc::new(Int16Array::from_value(*val, num_rows)),
            Byte(val) => Arc::new(Int8Array::from_value(*val, num_rows)),
            Float(val) => Arc::new(Float32Array::from_value(*val, num_rows)),
            Double(val) => Arc::new(Float64Array::from_value(*val, num_rows)),
            String(val) => Arc::new(StringArray::from(vec![val.clone(); num_rows])),
            Boolean(val) => Arc::new(BooleanArray::from(vec![*val; num_rows])),
            Timestamp(val) => {
                Arc::new(TimestampMicrosecondArray::from_value(*val, num_rows).with_timezone("UTC"))
            }
            TimestampNtz(val) => Arc::new(TimestampMicrosecondArray::from_value(*val, num_rows)),
            Date(val) => Arc::new(Date32Array::from_value(*val, num_rows)),
            Binary(val) => Arc::new(BinaryArray::from(vec![val.as_slice(); num_rows])),
            Decimal(val, precision, scale) => Arc::new(
                Decimal128Array::from_value(*val, num_rows)
                    .with_precision_and_scale(*precision, *scale as i8)?,
            ),
            Struct(data) => {
                let arrays = data
                    .values()
                    .iter()
                    .map(|val| val.to_array(num_rows))
                    .try_collect()?;
                let fields: Fields = data
                    .fields()
                    .iter()
                    .map(ArrowField::try_from)
                    .try_collect()?;
                Arc::new(StructArray::try_new(fields, arrays, None)?)
            }
            Array(data) => {
                #[allow(deprecated)]
                let values = data.array_elements();
                let vecs: Vec<_> = values.iter().map(|v| v.to_array(num_rows)).try_collect()?;
                let values: Vec<_> = vecs.iter().map(|x| x.as_ref()).collect();
                let offsets: Vec<_> = vecs.iter().map(|v| v.len()).collect();
                let offset_buffer = OffsetBuffer::from_lengths(offsets);
                let field = ArrowField::try_from(data.array_type())?;
                Arc::new(ListArray::new(
                    Arc::new(field),
                    offset_buffer,
                    concat(values.as_slice())?,
                    None,
                ))
            }
            Null(data_type) => match data_type {
                DataType::Primitive(primitive) => match primitive {
                    PrimitiveType::Byte => Arc::new(Int8Array::new_null(num_rows)),
                    PrimitiveType::Short => Arc::new(Int16Array::new_null(num_rows)),
                    PrimitiveType::Integer => Arc::new(Int32Array::new_null(num_rows)),
                    PrimitiveType::Long => Arc::new(Int64Array::new_null(num_rows)),
                    PrimitiveType::Float => Arc::new(Float32Array::new_null(num_rows)),
                    PrimitiveType::Double => Arc::new(Float64Array::new_null(num_rows)),
                    PrimitiveType::String => Arc::new(StringArray::new_null(num_rows)),
                    PrimitiveType::Boolean => Arc::new(BooleanArray::new_null(num_rows)),
                    PrimitiveType::Timestamp => {
                        Arc::new(TimestampMicrosecondArray::new_null(num_rows).with_timezone("UTC"))
                    }
                    PrimitiveType::TimestampNtz => {
                        Arc::new(TimestampMicrosecondArray::new_null(num_rows))
                    }
                    PrimitiveType::Date => Arc::new(Date32Array::new_null(num_rows)),
                    PrimitiveType::Binary => Arc::new(BinaryArray::new_null(num_rows)),
                    PrimitiveType::Decimal(precision, scale) => Arc::new(
                        Decimal128Array::new_null(num_rows)
                            .with_precision_and_scale(*precision, *scale as i8)?,
                    ),
                },
                DataType::Struct(t) => {
                    let fields: Fields = t.fields().map(ArrowField::try_from).try_collect()?;
                    Arc::new(StructArray::new_null(fields, num_rows))
                }
                DataType::Array(t) => {
                    let field =
                        ArrowField::new(LIST_ARRAY_ROOT, t.element_type().try_into()?, true);
                    Arc::new(ListArray::new_null(Arc::new(field), num_rows))
                }
                DataType::Map { .. } => unimplemented!(),
            },
        };
        Ok(arr)
    }
}

fn wrap_comparison_result(arr: BooleanArray) -> ArrayRef {
    Arc::new(arr) as Arc<dyn Array>
}

trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

fn extract_column<'array, 'path>(
    array: &'array dyn ProvidesColumnByName,
    path_step: &str,
    remaining_path_steps: &mut impl Iterator<Item=&'path str>,
) -> Result<&'array Arc<dyn Array>, ArrowError> {
    let child = array
        .column_by_name(path_step)
        .ok_or(ArrowError::SchemaError(format!(
            "No such field: {}",
            path_step,
        )))?;
    if let Some(next_path_step) = remaining_path_steps.next() {
        // This is not the last path step. Drill deeper.
        extract_column(
            column_as_struct(path_step, &Some(child))?,
            next_path_step,
            remaining_path_steps,
        )
    } else {
        // Last path step. Return it.
        Ok(child)
    }
}

fn column_as_struct<'a>(
    name: &str,
    column: &Option<&'a Arc<dyn Array>>,
) -> Result<&'a StructArray, ArrowError> {
    column
        .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(ArrowError::SchemaError(format!("{} is not a struct", name)))
}

fn evaluate_expression(
    expression: &Expression,
    batch: &RecordBatch,
    result_type: Option<&DataType>,
) -> DeltaResult<ArrayRef> {
    use BinaryOperator::*;
    use Expression::*;
    match (expression, result_type) {
        (Literal(scalar), _) => Ok(scalar.to_array(batch.num_rows())?),
        (Column(name), _) => {
            // TODO properly handle nested columns
            // https://github.com/delta-incubator/delta-kernel-rs/issues/86
            if name.contains('.') {
                let mut path = name.split('.');
                // Safety: we know that the first path step exists, because we checked for '.'
                Ok(extract_column(batch, path.next().unwrap(), &mut path).cloned()?)
            } else {
                batch
                    .column_by_name(name)
                    .ok_or(Error::missing_column(name))
                    .cloned()
            }
        }
        (Struct(fields), Some(DataType::Struct(schema))) => {
            let columns = fields
                .iter()
                .zip(schema.fields())
                .map(|(expr, field)| evaluate_expression(expr, batch, Some(field.data_type())));
            let output_cols: Vec<Arc<dyn Array>> = columns.try_collect()?;
            let output_fields: Vec<ArrowField> = output_cols
                .iter()
                .zip(schema.fields())
                .map(|(array, input_field)| -> DeltaResult<_> {
                    ensure_data_types(input_field.data_type(), array.data_type())?;
                    Ok(ArrowField::new(
                        input_field.name(),
                        array.data_type().clone(),
                        array.is_nullable(),
                    ))
                })
                .try_collect()?;
            let result = StructArray::try_new(output_fields.into(), output_cols, None)?;
            Ok(Arc::new(result))
        }
        (Struct(_), _) => Err(Error::generic(
            "Data type is required to evaluate struct expressions",
        )),
        (UnaryOperation { op, expr }, _) => {
            let arr = evaluate_expression(expr.as_ref(), batch, None)?;
            Ok(match op {
                UnaryOperator::Not => Arc::new(not(downcast_to_bool(&arr)?)?),
                UnaryOperator::IsNull => Arc::new(is_null(&arr)?),
            })
        }
        (
            BinaryOperation {
                op: In,
                left,
                right,
            },
            _,
        ) => match (left.as_ref(), right.as_ref()) {
            (_, Column(c)) => {
                let list_type = batch.column_by_name(c).map(|c| c.data_type());
                if !matches!(
                    list_type,
                    Some(ArrowDataType::List(_)) | Some(ArrowDataType::FixedSizeList(_, _))
                ) {
                    return Err(Error::InvalidExpressionEvaluation(format!(
                        "Right side column: {} is not a list or a fixed size list",
                        c
                    )));
                }
                let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
                let right_arr = evaluate_expression(right.as_ref(), batch, None)?;
                if let Some(string_arr) = left_arr.as_string_opt::<i32>() {
                    return in_list_utf8(string_arr, right_arr.as_list::<i32>())
                        .map(wrap_comparison_result)
                        .map_err(Error::generic_err);
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
            (Literal(lit), Literal(Scalar::Array(ad))) => {
                #[allow(deprecated)]
                let exists = ad.array_elements().contains(lit);
                Ok(Arc::new(BooleanArray::from(vec![exists])))
            }
            (_, _) => Err(Error::invalid_expression(format!(
                "Invalid right value for (NOT) IN comparison, right is: {:?}",
                right
            ))),
        },
        (
            BinaryOperation {
                op: NotIn,
                left,
                right,
            },
            _,
        ) => {
            let reverse_op = Expression::binary(In, *left.clone(), *right.clone());
            let reverse_expr = evaluate_expression(&reverse_op, batch, None)?;
            not(reverse_expr.as_boolean())
                .map(wrap_comparison_result)
                .map_err(Error::generic_err)
        }
        (BinaryOperation { op, left, right }, _) => {
            let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
            let right_arr = evaluate_expression(right.as_ref(), batch, None)?;

            type Operation = fn(&dyn Datum, &dyn Datum) -> Result<Arc<dyn Array>, ArrowError>;
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
                _ => return Err(Error::generic("Invalid expression given")),
            };

            eval(&left_arr, &right_arr).map_err(Error::generic_err)
        }
        (VariadicOperation { op, exprs }, None | Some(&DataType::BOOLEAN)) => {
            type Operation = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>;
            let (reducer, default): (Operation, _) = match op {
                VariadicOperator::And => (and_kleene, true),
                VariadicOperator::Or => (or_kleene, false),
            };
            exprs
                .iter()
                .map(|expr| evaluate_expression(expr, batch, result_type))
                .reduce(|l, r| {
                    Ok(reducer(downcast_to_bool(&l?)?, downcast_to_bool(&r?)?)
                        .map(wrap_comparison_result)?)
                })
                .unwrap_or_else(|| {
                    evaluate_expression(&Expression::literal(default), batch, result_type)
                })
        }
        (VariadicOperation { .. }, _) => {
            // NOTE: Update this error message if we add support for variadic operations on other types
            Err(Error::Generic(format!(
                "Variadic {expression:?} is expected to return boolean results, got {result_type:?}"
            )))
        }
    }
}

#[derive(Debug)]
pub struct ArrowExpressionHandler;

impl ExpressionHandler for ArrowExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression: Box::new(expression),
            output_type,
        })
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Box<Expression>,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        let batch = batch
            .as_any()
            .downcast_ref::<ArrowEngineData>()
            .ok_or(Error::engine_data_type("ArrowEngineData"))?
            .record_batch();
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };
        let array_ref = evaluate_expression(&self.expression, batch, Some(&self.output_type))?;
        let arrow_type: ArrowDataType = ArrowDataType::try_from(&self.output_type)?;
        let batch: RecordBatch = if let DataType::Struct(_) = self.output_type {
            array_ref
                .as_struct_opt()
                .ok_or(Error::unexpected_column_type("Expected a struct array"))?
                .into()
        } else {
            let schema = ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)]);
            RecordBatch::try_new(Arc::new(schema), vec![array_ref])?
        };
        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, Div, Mul, Sub};

    use arrow_array::{GenericStringArray, Int32Array};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::*;
    use crate::expressions::*;
    use crate::schema::ArrayType;
    use crate::DataType as DeltaDataTypes;

    #[test]
    fn test_array_column() {
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

        let schema = Schema::new(vec![arr_field.clone()]);

        let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let not_op = Expression::binary(
            BinaryOperator::NotIn,
            Expression::literal(5),
            Expression::column("item"),
        );

        let in_op = Expression::binary(
            BinaryOperator::NotIn,
            Expression::literal(5),
            Expression::column("item"),
        );

        let result = evaluate_expression(&not_op, &batch, None).unwrap();
        let expected = BooleanArray::from(vec![true, false, true]);
        assert_eq!(result.as_ref(), &expected);

        let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![true, false, true]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_bad_right_type_array() {
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Schema::new(vec![field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

        let in_op = Expression::binary(
            BinaryOperator::NotIn,
            Expression::literal(5),
            Expression::column("item"),
        );

        let in_result = evaluate_expression(&in_op, &batch, None);

        assert!(in_result.is_err());
        assert_eq!(
            in_result.unwrap_err().to_string(),
            "Invalid expression evaluation: Right side column: item is not a list or a fixed size list".to_string()
        )
    }

    #[test]
    fn test_literal_type_array() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Schema::new(vec![field.clone()]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let in_op = Expression::binary(
            BinaryOperator::NotIn,
            Expression::literal(5),
            Expression::literal(Scalar::Array(ArrayData::new(
                ArrayType::new(DeltaDataTypes::Primitive(PrimitiveType::Integer), false),
                vec![Scalar::Integer(1), Scalar::Integer(2)],
            ))),
        );

        let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![true]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_str_arrays() {
        let values = GenericStringArray::<i32>::from(vec![
            "hi", "bye", "hi", "hi", "bye", "bye", "hi", "bye", "hi",
        ]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));
        let schema = Schema::new(vec![arr_field.clone()]);
        let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let str_not_op = Expression::binary(
            BinaryOperator::NotIn,
            Expression::literal("bye"),
            Expression::column("item"),
        );

        let str_in_op = Expression::binary(
            BinaryOperator::In,
            Expression::literal("hi"),
            Expression::column("item"),
        );

        let result = evaluate_expression(&str_in_op, &batch, None).unwrap();
        let expected = BooleanArray::from(vec![true, true, true]);
        assert_eq!(result.as_ref(), &expected);

        let in_result = evaluate_expression(&str_not_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![false, false, false]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_extract_column() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values.clone())]).unwrap();
        let column = Expression::column("a");

        let results = evaluate_expression(&column, &batch, None).unwrap();
        assert_eq!(results.as_ref(), &values);

        let schema = Schema::new(vec![Field::new(
            "b",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)])),
            false,
        )]);

        let struct_values: ArrayRef = Arc::new(values.clone());
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, false)),
            struct_values,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(struct_array.clone())],
        )
            .unwrap();
        let column = Expression::column("b.a");
        let results = evaluate_expression(&column, &batch, None).unwrap();
        assert_eq!(results.as_ref(), &values);
    }

    #[test]
    fn test_binary_op_scalar() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::column("a");

        let expression = Box::new(column.clone().add(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().sub(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().mul(Expression::Literal(Scalar::Integer(2))));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        // TODO handle type casting
        let expression = Box::new(column.div(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert_eq!(results.as_ref(), expected.as_ref())
    }

    #[test]
    fn test_binary_op() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(values.clone()), Arc::new(values)],
        )
            .unwrap();
        let column_a = Expression::column("a");
        let column_b = Expression::column("b");

        let expression = Box::new(column_a.clone().add(column_b.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().sub(column_b.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().mul(column_b));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_binary_cmp() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::column("a");
        let lit = Expression::Literal(Scalar::Integer(2));

        let expression = Box::new(column.clone().lt(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().lt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().ne(lit.clone()));
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_logical() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        )
            .unwrap();
        let column_a = Expression::column("a");
        let column_b = Expression::column("b");

        let expression = Box::new(column_a.clone().and(column_b.clone()));
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().and(Expression::literal(true)));
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().or(column_b));
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column_a
                .clone()
                .or(Expression::literal(Scalar::Boolean(false))),
        );
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }
}
