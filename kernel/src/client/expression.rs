//! Default Expression handler.
//!
//! Expression handling based on arrow-rs compute kernels.

use std::sync::Arc;

use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::RecordBatch as ColumnarBatch;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Int32Array, RecordBatch, Scalar as ArrowScalar, StringArray, TimestampMicrosecondArray,
};

use crate::error::{DeltaResult, Error};
use crate::expressions::BinaryOperator;
use crate::expressions::{scalars::Scalar, Expression};
use crate::schema::SchemaRef;
use crate::{ExpressionEvaluator, ExpressionHandler};

// TODO leverage scalars / Datum

impl Scalar {
    pub fn to_array(&self, num_rows: usize) -> ArrayRef {
        use Scalar::*;
        match self {
            Integer(val) => Arc::new(Int32Array::from(vec![*val; num_rows])),
            Float(val) => Arc::new(Float32Array::from(vec![*val; num_rows])),
            String(val) => Arc::new(StringArray::from(vec![val.clone(); num_rows])),
            Boolean(val) => Arc::new(BooleanArray::from(vec![*val; num_rows])),
            Timestamp(val) => Arc::new(TimestampMicrosecondArray::from(vec![*val; num_rows])),
            Date(val) => Arc::new(Date32Array::from(vec![*val; num_rows])),
            Binary(val) => Arc::new(BinaryArray::from(vec![val.as_slice(); num_rows])),
            Decimal(val, precision, scale) => Arc::new(
                Decimal128Array::from(vec![*val; num_rows])
                    .with_precision_and_scale(*precision, *scale)
                    .unwrap(),
            ),
            Null(_) => todo!(),
        }
    }
}

fn evaluate_expression(expression: &Box<Expression>, batch: &RecordBatch) -> DeltaResult<ArrayRef> {
    match expression.as_ref() {
        Expression::Literal(scalar) => Ok(scalar.to_array(batch.num_rows())),
        Expression::Column { name, .. } => batch
            .column_by_name(&name)
            .ok_or(Error::MissingColumn(name.clone()))
            .cloned(),
        Expression::BinaryOperator { op, left, right } => {
            let left_arr = evaluate_expression(left, batch)?;
            let right_arr = evaluate_expression(right, batch)?;
            match op {
                BinaryOperator::Plus => {
                    add(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Minus => {
                    sub(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Multiply => {
                    mul(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Divide => {
                    div(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
            }
        }
        _ => todo!(),
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
            input_schema: schema,
            expression: Box::new(expression),
        })
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Box<Expression>,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &ColumnarBatch) -> DeltaResult<ColumnarBatch> {
        let result = evaluate_expression(&self.expression, batch)?;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::ops::Add;
    use std::ops::Div;
    use std::ops::Mul;
    use std::ops::Sub;

    #[test]
    fn test_binary_op_scalar() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::Column {
            name: "a".to_string(),
            data_type: crate::schema::DataType::Primitive(crate::schema::PrimitiveType::Integer),
        };

        let expression = Box::new(
            column
                .clone()
                .add(Expression::Literal(Scalar::Integer(1)))
                .unwrap(),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column
                .clone()
                .sub(Expression::Literal(Scalar::Integer(1)))
                .unwrap(),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column
                .clone()
                .mul(Expression::Literal(Scalar::Integer(2)))
                .unwrap(),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        // TODO handle type casting
        let expression = Box::new(column.div(Expression::Literal(Scalar::Integer(1))).unwrap());
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
        let column_a = Expression::Column {
            name: "a".to_string(),
            data_type: crate::schema::DataType::Primitive(crate::schema::PrimitiveType::Integer),
        };
        let column_b = Expression::Column {
            name: "a".to_string(),
            data_type: crate::schema::DataType::Primitive(crate::schema::PrimitiveType::Integer),
        };

        let expression = Box::new(column_a.clone().add(column_b.clone()).unwrap());
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().sub(column_b.clone()).unwrap());
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().mul(column_b).unwrap());
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }
}
