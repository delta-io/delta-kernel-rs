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
            let val = getter.get_map(i, "output")?.map(|m| {
                let partition_values = m.materialize();
                let resolver = self.schema.fields()
                    .map(|field| {
                        let data_type = field.data_type();

                        let DataType::Primitive(primitive_type) = data_type else {
                            return Err(crate::Error::unsupported(
                                format!("Partition filtering only supported for primitive types. Found type {} for field {}", data_type, field.name())
                            ));
                        };

                        let scalar = partition_values.get(field.name()).map(|v| primitive_type.parse_scalar(v)).transpose()?.unwrap_or(Scalar::Null(data_type.clone()));

                        Ok((ColumnName::new([field.name()]), scalar))
                    })
                    .collect::<DeltaResult<HashMap<ColumnName, Scalar>>>()?;

                let filter = DefaultPredicateEvaluator::from(resolver);
                Ok(filter.eval_expr(&self.predicate, false).unwrap_or(true))
            });

            let val = match val {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => true,
            };

            self.selection_vector.push(val);
        }
        Ok(())
    }
}
