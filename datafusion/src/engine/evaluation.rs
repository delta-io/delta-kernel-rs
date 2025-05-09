use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::execution::SessionState;
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::expressions::col;
use datafusion_physical_plan::PhysicalExpr;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{
    DeltaResult, EngineData, Error as DeltaError, EvaluationHandler, Expression,
    ExpressionEvaluator,
};
use parking_lot::RwLock;

use crate::expressions::to_datafusion_expr;

static ERROR_EXPR: LazyLock<Arc<dyn PhysicalExpr>> = LazyLock::new(|| {
    let err_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "error",
        ArrowDataType::Utf8,
        true,
    )]));
    col("error", err_schema.as_ref()).unwrap()
});

pub struct DataFusionEvaluationHandler {
    pub(super) state: Arc<RwLock<SessionState>>,
}

impl EvaluationHandler for DataFusionEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        let df_schema = match ArrowSchema::try_from(schema.as_ref())
            .map_err(DataFusionError::from)
            .and_then(DFSchema::try_from)
        {
            Ok(v) => v,
            Err(e) => return DataFusionExpressionEvaluator::new_err(output_type, e),
        };
        let physical_expressions = match to_datafusion_expr(&expression, &output_type)
            .and_then(|df_expr| self.state.read().create_physical_expr(df_expr, &df_schema))
        {
            Ok(v) => v,
            Err(e) => return DataFusionExpressionEvaluator::new_err(output_type, e),
        };
        DataFusionExpressionEvaluator::new(physical_expressions, output_type)
    }

    fn null_row(&self, output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
        let schema =
            ArrowSchema::try_from(output_schema.as_ref()).map_err(DeltaError::generic_err)?;
        let value = ScalarStructBuilder::new_null(schema.fields())
            .to_array_of_size(1)
            .map_err(DeltaError::generic_err)?;
        Ok(Box::new(ArrowEngineData::new(value.as_struct().into())))
    }
}

pub struct DataFusionExpressionEvaluator {
    /// The physical expression to evaluate
    expr: Arc<dyn PhysicalExpr>,
    /// The type of the output of the expression
    output_type: DataType,
    /// Error that occurred during initialization
    ///
    /// Raising this error is deferred until `evaluate` is called since
    /// creating the evaluator is an infallible operation.
    init_error: Option<DataFusionError>,
}

impl DataFusionExpressionEvaluator {
    pub fn new(expr: Arc<dyn PhysicalExpr>, output_type: DataType) -> Arc<Self> {
        Arc::new(Self {
            expr,
            init_error: None,
            output_type,
        })
    }

    fn new_err(output_type: DataType, error: DataFusionError) -> Arc<Self> {
        Arc::new(Self {
            expr: ERROR_EXPR.clone(),
            init_error: Some(error),
            output_type,
        })
    }

    fn raise_errors(&self) -> DeltaResult<()> {
        match &self.init_error {
            Some(e) => Err(DeltaError::generic(e.to_string())),
            None => Ok(()),
        }
    }
}

impl ExpressionEvaluator for DataFusionExpressionEvaluator {
    fn evaluate(&self, data: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        self.raise_errors()?;

        let batch = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| DeltaError::engine_data_type("ArrowEngineData"))?
            .record_batch();

        let results = self
            .expr
            .evaluate(batch)
            .and_then(|value| match value {
                ColumnarValue::Array(array) => Ok(array),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(batch.num_rows()),
            })
            .map_err(DeltaError::generic_err)?;

        let batch = match &self.output_type {
            DataType::Struct(_data) => {
                let arr = results
                    .as_struct_opt()
                    .ok_or_else(|| DeltaError::generic_err("Expected struct output"))?;
                arr.into()
            }
            _ => {
                let arrow_type = ArrowDataType::try_from(&self.output_type)?;
                let output_schema =
                    ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)]);
                RecordBatch::try_new(Arc::new(output_schema), vec![results.clone()])?
            }
        };

        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion_physical_plan::PhysicalExpr;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{DataType, PrimitiveType};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int32, true),
            Field::new("b", ArrowDataType::Utf8, true),
        ]));
        let a = Int32Array::from(vec![Some(1), Some(2), None]);
        let b = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    #[test]
    fn test_evaluate_success() {
        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Create a simple physical expression that adds 1 to column "a"
        let physical_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Literal::new(datafusion_common::ScalarValue::Int32(Some(1)))),
        )) as Arc<dyn PhysicalExpr>;

        let evaluator = DataFusionExpressionEvaluator::new(
            physical_expr,
            DataType::Primitive(PrimitiveType::Integer),
        );

        let result = evaluator.evaluate(&data).unwrap();
        let result_batch = result
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .unwrap()
            .record_batch();

        let expected = Int32Array::from(vec![Some(2), Some(3), None]);
        assert_eq!(
            result_batch.column(0).as_ref(),
            &expected,
            "Expected a + 1 to be [2, 3, null]"
        );
    }

    // #[test]
    // fn test_evaluate_with_errors() {
    //     let batch = create_test_batch();
    //     let data = ArrowEngineData::new(batch);
    //
    //     // Create a physical expression for a non-existent column
    //     let physical_expr = Arc::new(Column::new("non_existent", 0)) as Arc<dyn PhysicalExpr>;
    //
    //     let evaluator = DataFusionExpressionEvaluator::new(
    //         vec![physical_expr],
    //         vec![],
    //         DataType::Primitive(PrimitiveType::Integer),
    //     );
    //
    //     let result = evaluator.evaluate(&data);
    //     assert!(result.is_err(), "Expected error for non-existent column");
    // }

    // #[test]
    // fn test_evaluate_struct_output() {
    //     let batch = create_test_batch();
    //     let data = ArrowEngineData::new(batch);
    //
    //     // Create physical expressions for columns "a" and "b"
    //     Expression::struct_from([Expression::column(["a"])]);
    //
    //     let evaluator = DataFusionExpressionEvaluator::new(
    //         physical_exprs,
    //         vec![],
    //         DataType::Struct(Box::new(StructType::new(vec![
    //             StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
    //             StructField::new("b", DataType::Primitive(PrimitiveType::String), true),
    //         ]))),
    //     );
    //
    //     let result = evaluator.evaluate(&data).unwrap();
    //     let result_batch = result
    //         .any_ref()
    //         .downcast_ref::<ArrowEngineData>()
    //         .unwrap()
    //         .record_batch();
    //
    //     assert_eq!(result_batch.num_columns(), 2);
    //     assert_eq!(result_batch.num_rows(), 3);
    // }

    // #[test]
    // fn test_evaluate_with_init_errors() {
    //     let evaluator = DataFusionExpressionEvaluator::new(
    //         vec![],
    //         vec![DataFusionError::Internal("Test error".to_string())],
    //         DataType::Primitive(PrimitiveType::Integer),
    //     );
    //
    //     let batch = create_test_batch();
    //     let data = ArrowEngineData::new(batch);
    //
    //     let result = evaluator.evaluate(&data);
    //     assert!(result.is_err(), "Expected error due to init errors");
    // }
}
