use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use tracing::debug;

use crate::expressions::column_expr;
use crate::schema::column_name;
use crate::{
    engine_data::GetData,
    expressions::Scalar,
    predicates::{DefaultPredicateEvaluator, PredicateEvaluator},
    scan::get_log_add_schema,
    schema::{ColumnName, DataType, MapType, SchemaRef},
    DeltaResult, Engine, EngineData, Expression, ExpressionEvaluator, ExpressionRef, RowVisitor,
};

pub(crate) struct PartitionSkippingFilter {
    evaluator: Arc<dyn ExpressionEvaluator>,
    predicate: Arc<Expression>,
}

impl PartitionSkippingFilter {
    pub(crate) fn new(
        engine: &dyn Engine,
        physical_predicate: Option<(ExpressionRef, SchemaRef)>,
    ) -> Option<Self> {
        static PARITIONS_EXPR: LazyLock<Expression> =
            LazyLock::new(|| column_expr!("add.partitionValues"));

        let (predicate, _referenced_schema) = physical_predicate?;
        debug!("Creating a partition skipping filter for {:#?}", predicate);

        let partitions_map_type = MapType::new(DataType::STRING, DataType::STRING, true);

        let evaluator = engine.get_expression_handler().get_evaluator(
            get_log_add_schema().clone(),
            PARITIONS_EXPR.clone(),
            partitions_map_type.into(),
        );

        Some(Self {
            evaluator,
            predicate,
        })
    }

    pub(crate) fn apply(&self, actions: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        let partitions = self.evaluator.evaluate(actions)?;
        assert_eq!(partitions.len(), actions.len());

        let mut visitor = PartitionVisitor::new(&self.predicate);
        visitor.visit_rows_of(partitions.as_ref())?;
        Ok(visitor.selection_vector.clone())
    }
}

struct PartitionVisitor {
    pub(crate) selection_vector: Vec<bool>,
    predicate: Arc<Expression>,
}

impl PartitionVisitor {
    pub(crate) fn new(predicate: &Arc<Expression>) -> Self {
        Self {
            selection_vector: Vec::default(),
            predicate: Arc::clone(predicate),
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
                // TODO(tsaucer) instead of casting to Scalar::String we need to use the appropriate schema
                let resolver: HashMap<ColumnName, Scalar> = partition_values
                    .iter()
                    .map(|(k, v)| (ColumnName::new([k]), Scalar::String(v.to_owned())))
                    .collect();
                let filter = DefaultPredicateEvaluator::from(resolver);
                filter.eval_expr(&self.predicate, false).unwrap_or(true)
            });

            self.selection_vector.push(val.unwrap_or(true));
        }
        Ok(())
    }
}
