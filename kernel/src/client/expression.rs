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
