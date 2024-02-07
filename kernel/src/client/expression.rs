//! Default Expression handler.
//!
//! Expression handling based on arrow-rs compute kernels.
#![allow(trivial_casts)]
use std::sync::Arc;

use arrow_arith::boolean::{and, is_null, not, or};
use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::ArrowError;

use crate::error::{DeltaResult, Error};
use crate::expressions::{scalars::Scalar, Expression};
use crate::expressions::{BinaryOperator, UnaryOperator, VariadicOperator};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::simple_client::data::SimpleData;
use crate::{EngineData, ExpressionEvaluator, ExpressionHandler};

// TODO leverage scalars / Datum

fn downcast_to_bool(arr: &dyn Array) -> DeltaResult<&BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or(Error::Generic("expected boolean array".to_string()))
}

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> DeltaResult<ArrayRef> {
        use Scalar::*;
        let arr: Arc<dyn Array> = match self {
            Integer(val) => Arc::new(Int32Array::from_value(*val, num_rows)),
            Long(val) => Arc::new(Int64Array::from_value(*val, num_rows)),
            Float(val) => Arc::new(Float32Array::from_value(*val, num_rows)),
            String(val) => Arc::new(StringArray::from(vec![val.clone(); num_rows])),
            Boolean(val) => Arc::new(BooleanArray::from(vec![*val; num_rows])),
            Timestamp(val) => Arc::new(TimestampMicrosecondArray::from_value(*val, num_rows)),
            Date(val) => Arc::new(Date32Array::from_value(*val, num_rows)),
            Binary(val) => Arc::new(BinaryArray::from(vec![val.as_slice(); num_rows])),
            Decimal(val, precision, scale) => Arc::new(
                Decimal128Array::from_value(*val, num_rows)
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            Null(data_type) => match data_type {
                DataType::Primitive(primitive) => {
                    match primitive {
                        PrimitiveType::Byte => Arc::new(Int8Array::new_null(num_rows)),
                        PrimitiveType::Short => Arc::new(Int16Array::new_null(num_rows)),
                        PrimitiveType::Integer => Arc::new(Int32Array::new_null(num_rows)),
                        PrimitiveType::Long => Arc::new(Int64Array::new_null(num_rows)),
                        PrimitiveType::Float => Arc::new(Float32Array::new_null(num_rows)),
                        PrimitiveType::Double => Arc::new(Float64Array::new_null(num_rows)),
                        PrimitiveType::String => Arc::new(StringArray::new_null(num_rows)),
                        PrimitiveType::Boolean => Arc::new(BooleanArray::new_null(num_rows)),
                        PrimitiveType::Timestamp => {
                            Arc::new(TimestampMicrosecondArray::new_null(num_rows))
                        }
                        PrimitiveType::Date => Arc::new(Date32Array::new_null(num_rows)),
                        PrimitiveType::Binary => Arc::new(BinaryArray::new_null(num_rows)),
                        PrimitiveType::Decimal(precision, scale) => Arc::new(
                            Decimal128Array::new_null(num_rows)
                                // TODO update datatype?
                                .with_precision_and_scale(*precision as u8, *scale as i8)?,
                        ),
                    }
                }
                DataType::Array(_) => unimplemented!(),
                DataType::Map { .. } => unimplemented!(),
                DataType::Struct { .. } => unimplemented!(),
            },
        };
        Ok(arr)
    }
}

fn wrap_comparison_result(arr: BooleanArray) -> ArrayRef {
    Arc::new(arr) as Arc<dyn Array>
}

fn evaluate_expression(expression: &Expression, batch: &RecordBatch) -> DeltaResult<ArrayRef> {
    use BinaryOperator::*;
    use Expression::*;

    match expression {
        Literal(scalar) => Ok(scalar.to_array(batch.num_rows())?),
        Column(name) => batch
            .column_by_name(name)
            .ok_or(Error::MissingColumn(name.clone()))
            .cloned(),
        UnaryOperation { op, expr } => {
            let arr = evaluate_expression(expr.as_ref(), batch)?;
            Ok(match op {
                UnaryOperator::Not => Arc::new(not(downcast_to_bool(&arr)?)?),
                UnaryOperator::IsNull => Arc::new(is_null(&arr)?),
            })
        }
        BinaryOperation { op, left, right } => {
            let left_arr = evaluate_expression(left.as_ref(), batch)?;
            let right_arr = evaluate_expression(right.as_ref(), batch)?;

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
            };

            eval(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                source: Box::new(err),
            })
        }
        VariadicOperation { op, exprs } => {
            let reducer = match op {
                VariadicOperator::And => and,
                VariadicOperator::Or => or,
            };
            exprs
                .iter()
                .map(|expr| evaluate_expression(expr, batch))
                .reduce(|l, r| {
                    Ok(reducer(downcast_to_bool(&l?)?, downcast_to_bool(&r?)?)
                        .map(wrap_comparison_result)?)
                })
                .transpose()?
                .ok_or(Error::Generic("empty expression".to_string()))
        }
    }
}

#[derive(Debug)]
pub struct DefaultExpressionHandler {}

impl ExpressionHandler for DefaultExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            _input_schema: schema,
            expression: Box::new(expression),
        })
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    _input_schema: SchemaRef,
    expression: Box<Expression>,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        let batch = batch
            .as_any()
            .downcast_ref::<SimpleData>()
            .ok_or(Error::EngineDataType("SimpleData".into()))?
            .record_batch();
        let _result = evaluate_expression(&self.expression, batch)?;
        // TODO handled in #83
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::ops::{Add, Div, Mul, Sub};

    #[test]
    fn test_binary_op_scalar() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::Column("a".to_string());

        let expression = Box::new(column.clone().add(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().sub(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().mul(Expression::Literal(Scalar::Integer(2))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        // TODO handle type casting
        let expression = Box::new(column.div(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
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
        let column_a = Expression::Column("a".to_string());
        let column_b = Expression::Column("b".to_string());

        let expression = Box::new(column_a.clone().add(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().sub(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().mul(column_b));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_binary_cmp() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::Column("a".to_string());
        let lit = Expression::Literal(Scalar::Integer(2));

        let expression = Box::new(column.clone().lt(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().lt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().ne(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
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
        let column_a = Expression::Column("a".to_string());
        let column_b = Expression::Column("b".to_string());

        let expression = Box::new(column_a.clone().and(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column_a
                .clone()
                .and(Expression::literal(Scalar::Boolean(true))),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().or(column_b));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column_a
                .clone()
                .or(Expression::literal(Scalar::Boolean(false))),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }
}
