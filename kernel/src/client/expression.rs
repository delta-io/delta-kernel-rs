//! Default Expression handler.
//!
//! Expression handling based on arrow-rs compute kernels.

use std::sync::Arc;

use crate::error::DeltaResult;
use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::{ExpressionEvaluator, ExpressionHandler};

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
            expression,
        })
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Expression,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> DeltaResult<arrow_array::RecordBatch> {
        todo!()
    }
}
