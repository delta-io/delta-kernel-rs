//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, is_null, not, or_kleene};
use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_ord::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
};
use itertools::Itertools;

use super::arrow_conversion::LIST_ARRAY_ROOT;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::ensure_data_types;
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
    remaining_path_steps: &mut impl Iterator<Item = &'path str>,
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

    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::ops::{Add, Div, Mul, Sub};

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
