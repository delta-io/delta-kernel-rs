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
}

struct Dataframe {
    engine_data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>,
    root_schema: Arc<Schema>,
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

///
///
///
///  Ideally, this should be used like so:
///
///
///  let df = read_parquet
///               .filter(state.AddRemoveDedupFilter(), vec!["add,path", "add.dv_info", "remove.path", "remove.dv_info"])
///               .filter(state.DataSkippingStatsFilter(), vec!["add.stats"])
///               .filter(state.PartitionPruningFilter(), vec!["add.parition_values"])
///               .project(vec!["add.path", "add.size", "add.dv_info"])
///
impl Dataframe {
    fn filter(self, filter: Arc<dyn Filter>, column_names: &'static [ColumnName]) -> Self {
        Self {
            engine_data: self.engine_data,
            plan: LogicalPlanNode::Filter {
                child: self.plan.into(),
                filter,
                column_names,
            },
            root_schema: self.root_schema,
        }
    }
    fn select(self, column_names: Vec<ColumnName>) -> Self {
        Self {
            engine_data: self.engine_data,
            plan: LogicalPlanNode::Select {
                child: self.plan.into(),
                column_names,
            },
            root_schema: self.root_schema,
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
        recurse(&self.plan, &self.root_schema)
    }

    fn execute(self, engine: Arc<dyn Engine>) -> Box<Dataframe> {
        fn recurse(
            engine: &dyn Engine,
            node: LogicalPlanNode,
            schema: Arc<Schema>,
            engine_data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>,
        ) -> Box<Dataframe> {
            match node {
                LogicalPlanNode::Root => Box::new(Dataframe {
                    engine_data,
                    root_schema: schema.into(),
                    plan: LogicalPlanNode::Root,
                }),
                LogicalPlanNode::Filter {
                    child,
                    filter,
                    column_names,
                } => {
                    let child_df = recurse(engine, *child, schema, engine_data);
                    let Dataframe {
                        engine_data,
                        root_schema,
                        plan,
                    } = *child_df;

                    let mut visitor = FilterVisitor {
                        filter,
                        column_names,
                        result: vec![],
                    };
                    let new_iter = Box::new(engine_data.map_ok(move |batch| {
                        batch.engine_data.visit_rows(column_names, &mut visitor);

                        let mut selection_vector = vec![];
                        std::mem::swap(&mut selection_vector, &mut visitor.result);

                        let new_selection_vector = batch
                            .selection_vector
                            .iter()
                            .zip(selection_vector)
                            .map(|(a, b)| *a && b)
                            .collect_vec();
                        FilteredEngineData {
                            engine_data: batch.engine_data,
                            selection_vector: new_selection_vector,
                        }
                    }));
                    Box::new(Dataframe {
                        engine_data: new_iter,
                        root_schema,
                        // The plan is fully resolved at this point. So it is == to Root
                        plan,
                    })
                }
                LogicalPlanNode::Select {
                    child,
                    column_names,
                } => {
                    let child_df = recurse(engine, *child, schema, engine_data);
                    let Dataframe {
                        engine_data,
                        root_schema,
                        plan,
                    } = *child_df;

                    // This only really works for root-level fields, but can easily be generalized
                    let new_schema = root_schema
                        .project(&column_names.iter().map(|x| x.to_string()).collect_vec())
                        .unwrap();
                    let expression: Expression = Expression::struct_from(
                        column_names
                            .into_iter()
                            .map(|name| Expression::Column(name)),
                    );
                    let eval = engine.evaluation_handler().new_expression_evaluator(
                        root_schema,
                        expression,
                        new_schema.clone().into(),
                    );

                    let new_iter = engine_data.map_ok(move |batch| {
                        let FilteredEngineData {
                            engine_data,
                            selection_vector,
                        } = batch;
                        let new_engine_data = eval.evaluate(engine_data.as_ref()).unwrap();

                        FilteredEngineData {
                            engine_data: new_engine_data.into(),
                            selection_vector,
                        }
                    });

                    Box::new(Dataframe {
                        engine_data: Box::new(new_iter),
                        root_schema: new_schema,
                        // The plan is fully resolved at this point. So it is == to Root
                        plan,
                    })
                }
            }
        }
        recurse(
            engine.as_ref(),
            self.plan,
            self.root_schema,
            self.engine_data,
        )
    }
}

#[cfg(test)]
mod tests {}
