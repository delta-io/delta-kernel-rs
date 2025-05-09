use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::execution::SessionState;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_physical_plan::PhysicalExpr;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{
    DeltaResult, EngineData, Error as DeltaError, EvaluationHandler, Expression,
    ExpressionEvaluator,
};
use parking_lot::RwLock;

use crate::expressions::to_datafusion_expr;

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
        let mut errors = Vec::new();
        let with_error = |e: DataFusionError| {
            errors.push(e);
            Arc::new(DataFusionExpressionEvaluator::new(
                vec![],
                errors,
                output_type.clone(),
            ))
        };

        let arrow_schema: ArrowSchema = match schema.as_ref().try_into() {
            Ok(v) => v,
            Err(e) => return with_error(e.into()),
        };

        let df_expr = match to_datafusion_expr(&expression) {
            Ok(v) => v,
            Err(e) => return with_error(e),
        };

        let df_schema: DFSchema = match arrow_schema.try_into() {
            Ok(v) => v,
            Err(e) => return with_error(e),
        };

        let state = self.state.read();
        let physical_expressions = df_expr
            .into_iter()
            .map(|expr: Expr| state.create_physical_expr(expr, &df_schema))
            .collect::<Result<Vec<_>, _>>();
        let physical_expressions = match physical_expressions {
            Ok(v) => v,
            Err(e) => return with_error(e),
        };

        Arc::new(DataFusionExpressionEvaluator::new(
            physical_expressions,
            vec![],
            output_type,
        ))
    }

    fn null_row(&self, output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
        todo!()
    }
}

pub struct DataFusionExpressionEvaluator {
    physical_expressions: Vec<Arc<dyn PhysicalExpr>>,
    init_errors: Vec<DataFusionError>,
    output_type: DataType,
}

impl DataFusionExpressionEvaluator {
    pub fn new(
        physical_expressions: Vec<Arc<dyn PhysicalExpr>>,
        init_errors: Vec<DataFusionError>,
        output_type: DataType,
    ) -> Self {
        Self {
            physical_expressions,
            init_errors,
            output_type,
        }
    }

    fn raise_errors(&self) -> DeltaResult<()> {
        match self.init_errors.len() {
            0 => Ok(()),
            _ => Err(self
                .init_errors
                .first()
                .map(|e| DeltaError::generic(e.to_string()))
                .expect("length matched 1")),
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
            .physical_expressions
            .iter()
            .map(|expr| {
                expr.evaluate(batch).and_then(|value| match value {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(batch.num_rows()),
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(DeltaError::generic_err)?;

        let output_schema: ArrowSchema = match &self.output_type {
            DataType::Struct(data) => data.as_ref().try_into()?,
            _ => {
                let arrow_type: ArrowDataType = ArrowDataType::try_from(&self.output_type)?;
                ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)])
            }
        };
        let batch = RecordBatch::try_new(Arc::new(output_schema), results)?;
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
    use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

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
            vec![physical_expr],
            vec![],
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

    #[test]
    fn test_evaluate_struct_output() {
        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        // Create physical expressions for columns "a" and "b"
        let physical_exprs = vec![
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
        ];

        let evaluator = DataFusionExpressionEvaluator::new(
            physical_exprs,
            vec![],
            DataType::Struct(Box::new(StructType::new(vec![
                StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
                StructField::new("b", DataType::Primitive(PrimitiveType::String), true),
            ]))),
        );

        let result = evaluator.evaluate(&data).unwrap();
        let result_batch = result
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .unwrap()
            .record_batch();

        assert_eq!(result_batch.num_columns(), 2);
        assert_eq!(result_batch.num_rows(), 3);
    }

    #[test]
    fn test_evaluate_with_init_errors() {
        let evaluator = DataFusionExpressionEvaluator::new(
            vec![],
            vec![DataFusionError::Internal("Test error".to_string())],
            DataType::Primitive(PrimitiveType::Integer),
        );

        let batch = create_test_batch();
        let data = ArrowEngineData::new(batch);

        let result = evaluator.evaluate(&data);
        assert!(result.is_err(), "Expected error due to init errors");
    }
}
