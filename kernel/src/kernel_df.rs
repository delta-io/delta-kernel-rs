use std::{
    any::Any,
    cell::RefCell,
    collections::HashSet,
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use parquet_56::schema;

use crate::{
    checkpoint::log_replay::CheckpointVisitor,
    engine_data::{self, GetData},
    log_replay::FileActionDeduplicator,
    log_segment::LogSegment,
    scan::{log_replay::AddRemoveDedupVisitor, CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA},
    schema::{ColumnName, DataType, Schema, SchemaRef},
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileMeta, RowVisitor,
    Snapshot,
};

pub trait RowFilter: RowVisitor {
    fn filter_row<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool>;
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
impl RowFilter for AddRemoveDedupVisitor<'_> {
    fn filter_row<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // let is_log_batch = self.deduplicator.is_log_batch();
        // let expected_getters = if is_log_batch { 9 } else { 5 };
        // require!(
        //     getters.len() == expected_getters,
        //     Error::InternalError(format!(
        //         "Wrong number of AddRemoveDedupVisitor getters: {}",
        //         getters.len()
        //     ))
        // );
        self.is_valid_add(i, getters)
    }
}

struct FilterVisitor {
    filter: Arc<Mutex<dyn RowFilter>>,
    selection_vector: Vec<bool>,
}

impl RowVisitor for FilterVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.filter
            .lock()
            .unwrap()
            .selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // let is_log_batch = self.deduplicator.is_log_batch();
        // let expected_getters = if is_log_batch { 9 } else { 5 };
        // require!(
        //     getters.len() == expected_getters,
        //     Error::InternalError(format!(
        //         "Wrong number of AddRemoveDedupVisitor getters: {}",
        //         getters.len()
        //     ))
        // );

        for i in 0..row_count {
            if self.selection_vector[i] {
                // self.selection_vector[i] = self.is_valid_add(i, getters)?;
                self.selection_vector[i] = self.filter.lock().unwrap().filter_row(i, getters)?;
            }
        }
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

#[derive(Debug)]
enum FileType {
    Json,
    Parquet,
}
#[derive(Debug)]
struct ScanNode {
    file_type: FileType,
    files: Vec<FileMeta>,
    schema: SchemaRef,
}
impl std::fmt::Debug for FilterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterNode")
            .field("child", &self.child)
            .field(
                "filter",
                &self
                    .filter
                    .lock()
                    .unwrap()
                    .selected_column_names_and_types(),
            )
            .field("column_names", &self.column_names)
            .finish()
    }
}
struct FilterNode {
    child: Box<LogicalPlanNode>,
    filter: Arc<Mutex<dyn RowFilter>>,
    column_names: &'static [ColumnName],
}

#[derive(Debug)]
struct SelectNode {
    child: Box<LogicalPlanNode>,
    columns: Vec<Arc<Expression>>,
    input_schema: SchemaRef,
    output_type: SchemaRef,
}
#[derive(Debug)]
struct UnionNode {
    a: Box<LogicalPlanNode>,
    b: Box<LogicalPlanNode>,
}
#[derive(Debug)]
enum LogicalPlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Select(SelectNode),
    Union(UnionNode),
    Custom(CustomNode),
}
#[derive(Debug)]
struct CustomNode {
    node_type: CustomImpl,
    child: Box<LogicalPlanNode>,
}

#[derive(Debug)]
enum CustomImpl {
    AddRemoveDedup,
    StatsSkipping,
}

type FallibleFilteredDataIter = Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>;

trait PhysicalPlanExecutor {
    fn execute(&self, plan: LogicalPlanNode) -> DeltaResult<FallibleFilteredDataIter>;
}

struct DefaultPlanExecutor {
    engine: Arc<dyn Engine>,
}

impl DefaultPlanExecutor {
    fn execute_scan(&self, node: ScanNode) -> DeltaResult<FallibleFilteredDataIter> {
        let ScanNode {
            file_type,
            files,
            schema,
        } = node;

        match file_type {
            FileType::Json => {
                let json_handler = self.engine.as_ref().json_handler();
                let files = json_handler.read_json_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files.map_ok(|file| {
                    let engine_data: Arc<dyn EngineData> = file.into();
                    FilteredEngineData {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data,
                    }
                })))
            }
            FileType::Parquet => {
                let parquet_handler = self.engine.as_ref().parquet_handler();
                let files = parquet_handler.read_parquet_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files.map_ok(|file| {
                    let engine_data: Arc<dyn EngineData> = file.into();
                    FilteredEngineData {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data,
                    }
                })))
            }
        }
    }

    fn execute_filter(&self, node: FilterNode) -> DeltaResult<FallibleFilteredDataIter> {
        let FilterNode {
            child,
            filter,
            column_names,
        } = node;
        let child_iter = self.execute(*child)?;

        let filtered_iter = child_iter.map(move |x| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = x?;
            let filter_clone = filter.clone();
            let mut visitor = FilterVisitor {
                filter: filter_clone,
                selection_vector,
            };
            engine_data.visit_rows(column_names, &mut visitor)?;
            Ok(FilteredEngineData {
                engine_data,
                selection_vector: visitor.selection_vector,
            })
        });
        Ok(Box::new(filtered_iter))
    }

    fn execute_union(&self, node: UnionNode) -> DeltaResult<FallibleFilteredDataIter> {
        let UnionNode { a, b } = node;
        Ok(Box::new(self.execute(*a)?.chain(self.execute(*b)?)))
    }

    fn execute_select(&self, node: SelectNode) -> DeltaResult<FallibleFilteredDataIter> {
        let SelectNode {
            child,
            columns,
            input_schema,
            output_type,
        } = node;

        let eval_handler = self.engine.evaluation_handler();
        let evaluator = eval_handler.new_expression_evaluator(
            input_schema,
            Expression::Struct(columns).into(),
            output_type.into(),
        );

        let child_iter = self.execute(*child)?;
        let res = child_iter.map(move |x| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = x?;
            let new_data = evaluator.evaluate(engine_data.as_ref())?;

            Ok(FilteredEngineData {
                engine_data: new_data.into(),
                selection_vector,
            })
        });
        Ok(Box::new(res))
    }
    fn execute_custom(&self, _node: CustomNode) -> DeltaResult<FallibleFilteredDataIter> {
        return Err(Error::generic("No custom operators implemented"));
    }
}

impl PhysicalPlanExecutor for DefaultPlanExecutor {
    fn execute(
        &self,
        plan: LogicalPlanNode,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>> {
        match plan {
            LogicalPlanNode::Scan(scan_node) => self.execute_scan(scan_node),
            LogicalPlanNode::Filter(filter_node) => self.execute_filter(filter_node),
            LogicalPlanNode::Select(select_node) => self.execute_select(select_node),
            LogicalPlanNode::Custom(custom_node) => self.execute_custom(custom_node),
            LogicalPlanNode::Union(union_node) => self.execute_union(union_node),
        }
    }
}
impl LogicalPlanNode {
    fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlanNode::Scan(scan_node) => scan_node.schema.clone(),
            LogicalPlanNode::Filter(filter_node) => filter_node.child.schema(),
            LogicalPlanNode::Select(select_node) => select_node.output_type.clone(),
            LogicalPlanNode::Union(union_node) => union_node.a.schema().clone(),
            LogicalPlanNode::Custom(custom_node) => todo!(),
        }
    }
    fn select(self, columns: Vec<ExpressionRef>) -> DeltaResult<Self> {
        let input_schema = self.schema();
        let output_type =
            Expression::Struct(columns.clone()).data_type(Some(input_schema.clone()))?;
        let DataType::Struct(output_schema) = output_type else {
            panic!("should be struct");
        };

        Ok(Self::Select(SelectNode {
            child: Box::new(self),
            columns,
            input_schema,
            output_type: output_schema.into(),
        }))
    }

    fn filter(self, filter: impl RowFilter) -> DeltaResult<Self> {
        Ok(Self::Filter(FilterNode {
            child: Box::new(self),
            filter: Arc::new(Mutex::new(filter)),
            column_names: todo!(),
        }))
    }

    fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> DeltaResult<Self> {
        Ok(Self::Scan(ScanNode {
            file_type: FileType::Json,
            files,
            schema,
        }))
    }
    fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> DeltaResult<Self> {
        Ok(Self::Scan(ScanNode {
            file_type: FileType::Parquet,
            files,
            schema,
        }))
    }

    fn union(self, other: LogicalPlanNode) -> DeltaResult<Self> {
        if self.schema() != other.schema() {
            return Err(Error::generic("schema mismatch in union"));
        }
        Ok(Self::Union(UnionNode {
            a: self.into(),
            b: other.into(),
        }))
    }
}

impl Snapshot {
    fn get_scan_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let json_paths = self
            .log_segment()
            .ascending_commit_files
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        let json_scan = LogicalPlanNode::scan_json(json_paths, COMMIT_READ_SCHEMA.clone())?;

        let parquet_paths = self
            .log_segment()
            .checkpoint_parts
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        let parquet_scan =
            LogicalPlanNode::scan_parquet(parquet_paths, COMMIT_READ_SCHEMA.clone())?;

        json_scan.union(parquet_scan)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use std::process::ExitCode;

    use crate::arrow::compute::filter_record_batch;
    use crate::arrow::record_batch::RecordBatch;
    use crate::arrow::util::pretty::print_batches;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::{DeltaResult, Snapshot};

    use itertools::process_results;
    use itertools::Itertools;

    use super::PhysicalPlanExecutor;
    use crate::{engine::sync::SyncEngine, kernel_df::DefaultPlanExecutor};

    #[test]
    fn test_scan_plan() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder(url).build(&engine).unwrap();
        let plan = snapshot.get_scan_plan().unwrap();
        println!("plan: {plan:?}");
        let executor = DefaultPlanExecutor {
            engine: Arc::new(engine),
        };
        let res_iter = executor.execute(plan).unwrap();
        process_results(res_iter, |data| {
            let batches = data
                .map(|batch| {
                    let mask = batch.selection_vector;
                    let data = batch.engine_data;
                    let record_batch: RecordBatch = data
                        .as_any()
                        .downcast::<ArrowEngineData>()
                        .unwrap()
                        .record_batch()
                        .clone();
                    filter_record_batch(&record_batch, &mask.into()).unwrap()
                })
                .collect_vec();
            print_batches(&batches).unwrap();
        })
        .unwrap();
    }
}
