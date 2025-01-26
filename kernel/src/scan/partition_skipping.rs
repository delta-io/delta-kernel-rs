use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use tracing::debug;

use crate::schema::column_name;
use crate::{
    engine_data::GetData,
    expressions::Scalar,
    predicates::{DefaultPredicateEvaluator, PredicateEvaluator},
    scan::get_log_add_schema,
    schema::{ColumnName, DataType, MapType, SchemaRef},
    DeltaResult, Engine, EngineData, Expression, ExpressionEvaluator, ExpressionRef, RowVisitor,
};
use crate::{expressions::column_expr, schema::StructType};

pub(crate) struct PartitionSkippingFilter {
    evaluator: Arc<dyn ExpressionEvaluator>,
    predicate: Arc<Expression>,
    schema: SchemaRef,
}

impl PartitionSkippingFilter {
    pub(crate) fn new(
        engine: &dyn Engine,
        physical_predicate: Option<(ExpressionRef, SchemaRef)>,
        partition_columns: &[String],
    ) -> Option<Self> {
        static PARITIONS_EXPR: LazyLock<Expression> =
            LazyLock::new(|| column_expr!("add.partitionValues"));

        let (predicate, schema) = physical_predicate?;
        debug!("Creating a partition skipping filter for {:#?}", predicate);

        // Limit the schema passed to the row visitor of only the fields that are included
        // in the predicate and are also partition columns. The data skipping columns will
        // be handled elsewhere.
        let partition_fields = schema
            .fields()
            .filter(|f| partition_columns.contains(f.name()))
            .cloned();
        let schema = Arc::new(StructType::new(partition_fields));

        let partitions_map_type = MapType::new(DataType::STRING, DataType::STRING, true);

        let evaluator = engine.get_expression_handler().get_evaluator(
            get_log_add_schema().clone(),
            PARITIONS_EXPR.clone(),
            partitions_map_type.into(),
        );

        Some(Self {
            evaluator,
            predicate,
            schema,
        })
    }

    pub(crate) fn apply(&self, actions: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        let partitions = self.evaluator.evaluate(actions)?;
        assert_eq!(partitions.len(), actions.len());

        let mut visitor = PartitionVisitor::new(&self.predicate, &self.schema);
        visitor.visit_rows_of(partitions.as_ref())?;
        Ok(visitor.selection_vector.clone())
    }
}

struct PartitionVisitor {
    pub(crate) selection_vector: Vec<bool>,
    predicate: Arc<Expression>,
    schema: SchemaRef,
}

impl PartitionVisitor {
    pub(crate) fn new(predicate: &Arc<Expression>, schema: &SchemaRef) -> Self {
        Self {
            selection_vector: Vec::default(),
            predicate: Arc::clone(predicate),
            schema: Arc::clone(schema),
        }
    }
}

impl RowVisitor for PartitionVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<crate::schema::ColumnNamesAndTypes> =
            LazyLock::new(|| {
                (
                    vec![column_name!("output")],
                    vec![DataType::Map(Box::new(MapType::new(
                        DataType::STRING,
                        DataType::STRING,
                        true,
                    )))],
                )
                    .into()
            });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let getter = getters[0];
        for i in 0..row_count {
            let val: Option<DeltaResult<bool>> = getter.get_map(i, "output")?.map(|m| {
                let partition_values = m.materialize();
                let resolver = self.schema.fields()
                    .map(|field| {
                        let data_type = field.data_type();

                        let DataType::Primitive(primitive_type) = data_type else {
                            return Err(crate::Error::unsupported(
                                format!("Partition filtering only supported for primitive types. Found type {} for field {}", data_type, field.name())
                            ));
                        };

                        let scalar = partition_values
                            .get(field.name())
                            .map(|v| primitive_type.parse_scalar(v))
                            .transpose()?
                            .unwrap_or(
                                match field.nullable {
                                    true => Ok(Scalar::Null(data_type.clone())),
                                    false => Err(crate::Error::missing_data(format!("Missing partition values on a non-nullable field is not supported. Field {}", field.name)))
                                }?);

                        Ok((ColumnName::new([field.name()]), scalar))
                    })
                    .collect::<DeltaResult<HashMap<ColumnName, Scalar>>>()?;

                let filter = DefaultPredicateEvaluator::from(resolver);
                Ok(filter.eval_expr(&self.predicate, false).unwrap_or(true))
            });

            self.selection_vector.push(val.transpose()?.unwrap_or(true));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    use crate::engine::{arrow_data::ArrowEngineData, sync::SyncEngine};
    use crate::expressions::UnaryOperator;
    use crate::scan::get_log_schema;
    use crate::schema::{DataType, Schema, StructField};
    use crate::{DeltaResult, Engine, EngineData, Expression};

    use super::PartitionSkippingFilter;

    // TODO(nick): Merge all copies of this into one "test utils" thing
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(ArrowField::new("a", ArrowDataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_partition_skipping() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let json_handler = engine.get_json_handler();
        let json_strings: StringArray = vec![
            // All these values should be filtered due to c1 value
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"1","c2":""},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"2","c2":null},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"3","c2":"a"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,

            // Test both null and "" produce valid nulls
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"4","c2":""},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5","c2":null},"size":452,"modificationTime":1670892998136,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"b"},"size":452,"modificationTime":1670892998137,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,

            // Gracefully handle missing partition values as null
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"1"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5"},"size":452,"modificationTime":1670892998136,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,

            ]
        .into();

        let log_schema = get_log_schema().clone();
        let batch = json_handler
            .parse_json(
                string_array_to_engine_data(json_strings),
                log_schema.clone(),
            )
            .unwrap();

        let expr = Arc::new(Expression::and(
            Expression::unary(UnaryOperator::IsNull, Expression::column(["c2"])),
            Expression::ge(Expression::column(["c1"]), Expression::literal(4)),
        ));

        let schema = Arc::new(Schema::new(vec![
            StructField::new("c1", DataType::INTEGER, true),
            StructField::new("c2", DataType::STRING, true),
            StructField::new("c3", DataType::INTEGER, true),
        ]));

        let physical_predicate = Some((expr, schema));
        let filter = PartitionSkippingFilter::new(
            &engine,
            physical_predicate,
            &["c1".to_string(), "c2".to_string()],
        )
        .expect("Unable to create Partition Skipping Filter");

        let actual = filter.apply(batch.as_ref())?;

        let expected = vec![false, false, false, true, true, false, false, true];

        assert_eq!(actual, expected);
        Ok(())
    }
}
