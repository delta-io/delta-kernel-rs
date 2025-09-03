use std::{collections::HashSet, sync::Arc};

use itertools::Itertools;
use parquet_56::schema;

use crate::{
    engine_data::{self, GetData},
    schema::{ColumnName, DataType, Schema},
    DeltaResult, Engine, EngineData, Expression, RowVisitor,
};

pub trait Filter {
    fn input_types(&self) -> &'static [DataType];
    fn execute<'b>(&self, row_count: usize, getters: &[&dyn GetData<'b>])
        -> DeltaResult<Vec<bool>>;
}

// Could even become a macro:

// impl AddRemoveDedupFilter {
//     fn apply(&self, add_path: String, remove_path_: String) -> bool {
//         self.ref_mut().hashmap.contains_key(add_path)
//     }
//
// make_filter! {
//     AddRemoveDedupFilter
//     apply,
//     (String, String)
// }

// -->
//
// impl Filter for AddRemoveDedupFilter {
//   fn input_types(&self) -> &'static[DataType] {
//     [DataType::String, DataType::String]
//   }
//
//   fn execute<'b>(&self, row_count: usize, getters: &[&dyn GetData<'b>]) {
//      let selection_vector = vec![];
//      for i in 0..row_count {
//        selection_vector.push(self.apply(getters.get_string(i), getters.get_string(i)));
//      }
//   }
// }
//

struct FilterVisitor {
    filter: Arc<dyn Filter>,
    column_names: &'static [ColumnName],
    result: Vec<bool>,
}

impl RowVisitor for FilterVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        (self.column_names, self.filter.input_types())
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        self.result = self.filter.execute(row_count, getters)?;
        Ok(())
    }
}

// This would replace enginedata
struct FilteredEngineData {
    engine_data: Arc<dyn EngineData>,
    selection_vector: Vec<bool>,
    root_schema: Arc<Schema>,
}

struct Dataframe {
    engine_data: FilteredEngineData,
    plan: LogicalPlanNode,
}

enum LogicalPlanNode {
    Root,
    Filter {
        child: Box<LogicalPlanNode>,
        filter: Arc<dyn Filter>,
        column_names: &'static [ColumnName],
    },
    Select {
        child: Box<LogicalPlanNode>,
        column_names: Vec<ColumnName>,
    },
}

impl Dataframe {
    fn filter(self, filter: Arc<dyn Filter>, column_names: &'static [ColumnName]) -> Self {
        Self {
            engine_data: self.engine_data,
            plan: LogicalPlanNode::Filter {
                child: self.plan.into(),
                filter,
                column_names,
            },
        }
    }
    fn select(self, column_names: Vec<ColumnName>) -> Self {
        Self {
            engine_data: self.engine_data,
            plan: LogicalPlanNode::Select {
                child: self.plan.into(),
                column_names,
            },
        }
    }
    fn schema(&self) -> Arc<Schema> {
        fn recurse<'a>(node: &'a LogicalPlanNode, schema: &Arc<Schema>) -> Arc<Schema> {
            match node {
                LogicalPlanNode::Root => return schema.clone(),
                LogicalPlanNode::Filter {
                    child,
                    filter: _,
                    column_names: _,
                } => recurse(child.as_ref(), schema),
                LogicalPlanNode::Select {
                    child: _,
                    column_names,
                } => schema
                    .project(&column_names.iter().map(|x| x.to_string()).collect_vec())
                    .unwrap(),
            }
        }
        recurse(&self.plan, &self.engine_data.root_schema)
    }

    fn execute(self, engine: Arc<dyn Engine>) -> FilteredEngineData {
        fn recurse(
            engine: &dyn Engine,
            node: LogicalPlanNode,
            engine_data: FilteredEngineData,
        ) -> FilteredEngineData {
            match node {
                LogicalPlanNode::Root => engine_data,
                LogicalPlanNode::Filter {
                    child,
                    filter,
                    column_names,
                } => {
                    let child_engine_data = recurse(engine, *child, engine_data);
                    let mut visitor = FilterVisitor {
                        filter,
                        column_names,
                        result: vec![],
                    };
                    child_engine_data
                        .engine_data
                        .visit_rows(column_names, &mut visitor);
                    FilteredEngineData {
                        engine_data: child_engine_data.engine_data,
                        selection_vector: visitor.result,
                        root_schema: child_engine_data.root_schema,
                    }
                }
                LogicalPlanNode::Select {
                    child,
                    column_names,
                } => {
                    let child_engine_data = recurse(engine, *child, engine_data);

                    // This only really works for root-level fields, but can easily be generalized
                    let new_schema = child_engine_data
                        .root_schema
                        .project(&column_names.iter().map(|x| x.to_string()).collect_vec())
                        .unwrap();

                    let expression: Expression = Expression::struct_from(
                        column_names
                            .into_iter()
                            .map(|name| Expression::Column(name)),
                    );
                    let eval = engine.evaluation_handler().new_expression_evaluator(
                        child_engine_data.root_schema,
                        expression,
                        new_schema.clone().into(),
                    );

                    let new_engine_data = eval
                        .evaluate(child_engine_data.engine_data.as_ref())
                        .unwrap();
                    FilteredEngineData {
                        engine_data: new_engine_data.into(),
                        selection_vector: child_engine_data.selection_vector,
                        root_schema: new_schema,
                    }
                }
            }
        }
        recurse(engine.as_ref(), self.plan, self.engine_data)
    }
}

#[cfg(test)]
mod tests {}
