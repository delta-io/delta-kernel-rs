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
use crate::expressions::column_expr;

pub(crate) struct PartitionSkippingFilter {
    evaluator: Arc<dyn ExpressionEvaluator>,
    predicate: Arc<Expression>,
    schema: SchemaRef,
}

impl PartitionSkippingFilter {
    pub(crate) fn new(
        engine: &dyn Engine,
        physical_predicate: Option<(ExpressionRef, SchemaRef)>,
    ) -> Option<Self> {
        static PARITIONS_EXPR: LazyLock<Expression> =
            LazyLock::new(|| column_expr!("add.partitionValues"));

        let (predicate, schema) = physical_predicate?;
        debug!("Creating a partition skipping filter for {:#?}", predicate);
        println!("reference schema\n{:#?}", schema);

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
            let val = getter.get_map(i, "output")?.and_then(|m| {
                let partition_values = m.materialize();
                // TODO(tsaucer) instead of casting to Scalar::String we need to use the appropriate schema
                let resolver = partition_values
                    .iter()
                    .map(|(k, v)| {
                        let data_type = self.schema.field(k).map(|f| f.data_type());
                        let primitve_type = if let Some(DataType::Primitive(primitive)) = data_type
                        {
                            primitive.to_owned()
                        } else {
                            return Err(crate::Error::Generic(
                                "partition filtering only supported for primitive types"
                                    .to_string(),
                            ));
                        };

                        let scalar = primitve_type.parse_scalar(v)?;
                        Ok((ColumnName::new([k]), scalar))
                    })
                    .collect::<DeltaResult<HashMap<ColumnName, Scalar>>>()
                    .ok()?;
                let filter = DefaultPredicateEvaluator::from(resolver);
                Some(filter.eval_expr(&self.predicate, false).unwrap_or(true))
            });

            self.selection_vector.push(val.unwrap_or(true));
        }
        Ok(())
    }
}
