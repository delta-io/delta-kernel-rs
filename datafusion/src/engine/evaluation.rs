use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::{DFSchema, DataFusionError, Result as DFResult};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::expressions::col;
use datafusion_physical_plan::PhysicalExpr;
use datafusion_session::{Session, SessionStore};
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
    /// The session store that contains the current session.
    pub(super) state: Arc<SessionStore>,
}

impl DataFusionEvaluationHandler {
    pub fn new(state: impl Into<Arc<SessionStore>>) -> Self {
        Self {
            state: state.into(),
        }
    }

    fn session(&self) -> DFResult<Arc<RwLock<dyn Session>>> {
        self.state
            .get_session()
            .upgrade()
            .ok_or_else(|| DataFusionError::Execution("no active session".into()))
    }
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
        let physical_expressions =
            match to_datafusion_expr(&expression, &output_type).and_then(|df_expr| {
                self.session()
                    .and_then(|session| session.read().create_physical_expr(df_expr, &df_schema))
            }) {
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

    fn raise_error(&self) -> DeltaResult<()> {
        match &self.init_error {
            Some(e) => Err(DeltaError::generic(e.to_string())),
            None => Ok(()),
        }
    }
}

impl ExpressionEvaluator for DataFusionExpressionEvaluator {
    fn evaluate(&self, data: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        self.raise_error()?;

        let batch = data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| DeltaError::engine_data_type("ArrowEngineData"))?
            .record_batch();

        let results = self
            .expr
            .evaluate(batch)
            // TODO(roeap): we should consider implementing EngineData for ColumnarValue directly
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
    use std::ops::Add;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};
    use delta_kernel::Engine;
    use rstest::*;

    use super::*;
    use crate::tests::df_engine;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int32, true),
            Field::new("b", ArrowDataType::Utf8, true),
        ]));
        let a = Int32Array::from(vec![Some(1), Some(2), None]);
        let b = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn test_evaluate_success(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let (engine, _ctx) = df_engine;
        let handler = engine.evaluation_handler();

        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Create a simple physical expression that adds 1 to column "a"
        let expr = Expression::column(["a"]).add(Expression::literal(1));
        let input_schema = Arc::new(StructType::new(vec![StructField::new(
            "a",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )]));

        let evaluator = handler.new_expression_evaluator(
            input_schema,
            expr,
            DataType::Primitive(PrimitiveType::Integer),
        );

        let result = evaluator.evaluate(&data).unwrap();
        let engine_data = ArrowEngineData::try_from_engine_data(result).unwrap();

        let expected = Int32Array::from(vec![Some(2), Some(3), None]);
        assert_eq!(
            engine_data.record_batch().column(0).as_ref(),
            &expected,
            "Expected a + 1 to be [2, 3, null]"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_evaluate_struct_output(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let (engine, _ctx) = df_engine;
        let handler = engine.evaluation_handler();

        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Create expression that returns a struct with two fields
        let expr =
            Expression::struct_from(vec![Expression::column(["a"]), Expression::column(["b"])]);
        let input_schema = Arc::new(StructType::new(vec![
            StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("b", DataType::Primitive(PrimitiveType::String), true),
        ]));

        let output_type = DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("x", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("y", DataType::Primitive(PrimitiveType::String), true),
        ])));

        let evaluator = handler.new_expression_evaluator(input_schema, expr, output_type);
        let result = evaluator.evaluate(&data).unwrap();
        let engine_data = ArrowEngineData::try_from_engine_data(result).unwrap();

        assert_eq!(
            engine_data.record_batch().num_columns(),
            2,
            "Expected struct with 2 fields"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_evaluate_init_error(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let (engine, _ctx) = df_engine;
        let handler = engine.evaluation_handler();

        // Create an invalid expression that will fail during initialization
        let expr = Expression::column(["non_existent"]);
        let input_schema = Arc::new(StructType::new(vec![StructField::new(
            "a",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )]));

        let evaluator = handler.new_expression_evaluator(
            input_schema,
            expr,
            DataType::Primitive(PrimitiveType::Integer),
        );

        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Evaluation should fail with the initialization error
        assert!(evaluator.evaluate(&data).is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_evaluate_scalar(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let (engine, _ctx) = df_engine;
        let handler = engine.evaluation_handler();

        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Create expression that returns a constant scalar value
        let expr = Expression::literal(42);
        let input_schema = Arc::new(StructType::new(vec![StructField::new(
            "a",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )]));

        let evaluator = handler.new_expression_evaluator(
            input_schema,
            expr,
            DataType::Primitive(PrimitiveType::Integer),
        );

        let result = evaluator.evaluate(&data).unwrap();
        let engine_data = ArrowEngineData::try_from_engine_data(result).unwrap();

        let expected = Int32Array::from(vec![Some(42), Some(42), Some(42)]);
        assert_eq!(
            engine_data.record_batch().column(0).as_ref(),
            &expected,
            "Expected constant value 42 repeated for all rows"
        );
    }
}
