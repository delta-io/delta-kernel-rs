use crate::arrow::record_batch::RecordBatch;

use crate::{DeltaResult, ExpressionEvaluator};

use super::super::arrow_data::ArrowEngineData;

pub trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ExpressionEvaluatorExt;

    use crate::arrow::array::Int32Array;
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::expressions::*;
    use crate::EvaluationHandler;

    #[test_log::test]
    fn test_evaluate_arrow() {
        let handler = ArrowEvaluationHandler;

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();

        let expression = column_expr!("a");
        let expr = handler.new_expression_evaluator(
            Arc::new((&schema).try_into().unwrap()),
            expression,
            crate::schema::DataType::INTEGER,
        );

        let result = expr.evaluate_arrow(batch);
        assert!(result.is_ok());
    }
}
