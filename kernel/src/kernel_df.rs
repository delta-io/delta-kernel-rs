use std::{
    any::Any,
    cell::RefCell,
    collections::HashSet,
    sync::{Arc, LazyLock, Mutex, RwLock},
};

use itertools::Itertools;

use crate::{
    actions::deletion_vector::DeletionVectorDescriptor,
    checkpoint::log_replay::CheckpointVisitor,
    engine_data::{self, GetData, TypedGetData as _},
    log_replay::{FileActionDeduplicator, FileActionKey},
    log_segment::LogSegment,
    scan::{log_replay::AddRemoveDedupVisitor, CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA},
    schema::{ColumnName, DataType, Schema, SchemaRef},
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileMeta, RowVisitor,
    Snapshot,
};
use crate::{
    expressions::column_name,
    schema::{ColumnNamesAndTypes, MapType},
};

pub trait RowFilter: RowVisitor + Send + Sync {
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

pub struct FilterVisitor {
    pub filter: Arc<Mutex<dyn RowFilter>>,
    pub selection_vector: Vec<bool>,
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
pub struct FilteredEngineDataArc {
    pub engine_data: Arc<dyn EngineData>,
    pub selection_vector: Vec<bool>,
}

pub struct Dataframe {
    pub engine_data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>,
    pub root_schema: Arc<Schema>,
    pub plan: LogicalPlanNode,
}

#[derive(Debug)]
pub enum FileType {
    Json,
    Parquet,
}
#[derive(Debug)]
pub struct ScanNode {
    pub file_type: FileType,
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
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
pub struct FilterNode {
    pub child: Box<LogicalPlanNode>,
    pub filter: Arc<Mutex<dyn RowFilter>>,
    pub column_names: &'static [ColumnName],
    pub ordered: bool,
}

#[derive(Debug)]
pub struct SelectNode {
    pub child: Box<LogicalPlanNode>,
    pub columns: Vec<Arc<Expression>>,
    pub input_schema: SchemaRef,
    pub output_type: SchemaRef,
}
#[derive(Debug)]
pub struct UnionNode {
    pub a: Box<LogicalPlanNode>,
    pub b: Box<LogicalPlanNode>,
}
#[derive(Debug)]
pub enum LogicalPlanNode {
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
pub enum CustomImpl {
    AddRemoveDedup,
    StatsSkipping,
}

type FallibleFilteredDataIter = Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>;

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
                    FilteredEngineDataArc {
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
                    FilteredEngineDataArc {
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
            ordered,
        } = node;
        let child_iter = self.execute(*child)?;

        let filtered_iter = child_iter.map(move |x| {
            let FilteredEngineDataArc {
                engine_data,
                selection_vector,
            } = x?;
            let filter_clone = filter.clone();
            let mut visitor = FilterVisitor {
                filter: filter_clone,
                selection_vector,
            };
            engine_data.visit_rows(column_names, &mut visitor)?;
            Ok(FilteredEngineDataArc {
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
            let FilteredEngineDataArc {
                engine_data,
                selection_vector,
            } = x?;
            let new_data = evaluator.evaluate(engine_data.as_ref())?;

            Ok(FilteredEngineDataArc {
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
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>> {
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

    fn filter(self, filter: impl RowFilter + 'static) -> DeltaResult<Self> {
        let column_names = filter.selected_column_names_and_types().0;
        Ok(Self::Filter(FilterNode {
            child: Box::new(self),
            filter: Arc::new(Mutex::new(filter)),
            column_names,
            ordered: false,
        }))
    }

    fn filter_ordered(self, filter: impl RowFilter + 'static) -> DeltaResult<Self> {
        let column_names = filter.selected_column_names_and_types().0;
        Ok(Self::Filter(FilterNode {
            child: Box::new(self),
            filter: Arc::new(Mutex::new(filter)),
            column_names,
            ordered: true,
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
    pub fn get_scan_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let mut json_paths = self
            .log_segment()
            .ascending_commit_files
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        json_paths.reverse();
        let json_scan = LogicalPlanNode::scan_json(json_paths, COMMIT_READ_SCHEMA.clone())?;

        let parquet_paths = self
            .log_segment()
            .checkpoint_parts
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        let parquet_scan =
            LogicalPlanNode::scan_parquet(parquet_paths, COMMIT_READ_SCHEMA.clone())?;

        let set = Arc::new(RwLock::new(HashSet::new()));
        let x = SharedAddRemoveDedupFilter::new(set.clone(), json_scan.schema(), true);
        let y = SharedAddRemoveDedupFilter::new(set, parquet_scan.schema(), false);
        json_scan.filter_ordered(x)?.union(parquet_scan.filter(y)?)
    }
}

#[derive(Clone)]
pub struct SharedFileActionDeduplicator {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log for deduplication. This is a mutable reference to the set
    /// of seen file keys that persists across multiple log batches.
    seen_file_keys: Arc<RwLock<HashSet<FileActionKey>>>,
    // TODO: Consider renaming to `is_commit_batch`, `deduplicate_batch`, or `save_batch`
    // to better reflect its role in deduplication logic.
    /// Whether we're processing a log batch (as opposed to a checkpoint)
    is_log_batch: bool,
    /// Index of the getter containing the add.path column
    add_path_index: usize,
    /// Index of the getter containing the remove.path column
    remove_path_index: usize,
    /// Starting index for add action deletion vector columns
    add_dv_start_index: usize,
    /// Starting index for remove action deletion vector columns
    remove_dv_start_index: usize,
}

impl SharedFileActionDeduplicator {
    pub(crate) fn new(
        seen_file_keys: Arc<RwLock<HashSet<FileActionKey>>>,
        is_log_batch: bool,
        add_path_index: usize,
        remove_path_index: usize,
        add_dv_start_index: usize,
        remove_dv_start_index: usize,
    ) -> Self {
        Self {
            seen_file_keys,
            is_log_batch,
            add_path_index,
            remove_path_index,
            add_dv_start_index,
            remove_dv_start_index,
        }
    }

    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    pub(crate) fn check_and_record_seen(&mut self, key: FileActionKey) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen_file_keys.read().unwrap().contains(&key) {
            // println!(
            //     "Ignoring duplicate ({}, {:?}) in scan, is log {}",
            //     key.path, key.dv_unique_id, self.is_log_batch
            // );
            true
        } else {
            // println!(
            //     "Including ({}, {:?}) in scan, is log {}",
            //     key.path, key.dv_unique_id, self.is_log_batch
            // );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen_file_keys.write().unwrap().insert(key);
            }
            false
        }
    }

    /// Extracts the deletion vector unique ID if it exists.
    ///
    /// This function retrieves the necessary fields for constructing a deletion vector unique ID
    /// by accessing `getters` at `dv_start_index` and the following two indices. Specifically:
    /// - `dv_start_index` retrieves the storage type (`deletionVector.storageType`).
    /// - `dv_start_index + 1` retrieves the path or inline deletion vector (`deletionVector.pathOrInlineDv`).
    /// - `dv_start_index + 2` retrieves the optional offset (`deletionVector.offset`).
    fn extract_dv_unique_id<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        dv_start_index: usize,
    ) -> DeltaResult<Option<String>> {
        match getters[dv_start_index].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => {
                let path_or_inline =
                    getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
                let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;

                Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
                    storage_type,
                    path_or_inline,
                    offset,
                )))
            }
            None => Ok(None),
        }
    }

    /// Extracts a file action key and determines if it's an add operation.
    /// This method examines the data at the given index using the provided getters
    /// to identify whether a file action exists and what type it is.
    ///
    /// # Parameters
    /// - `i`: Index position in the data structure to examine
    /// - `getters`: Collection of data getter implementations used to access the data
    /// - `skip_removes`: Whether to skip remove actions when extracting file actions
    ///
    /// # Returns
    /// - `Ok(Some((key, is_add)))`: When a file action is found, returns the key and whether it's an add operation
    /// - `Ok(None)`: When no file action is found
    /// - `Err(...)`: On any error during extraction
    pub(crate) fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(FileActionKey, bool)>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), true)));
        }

        // The AddRemoveDedupVisitor skips remove actions when extracting file actions from a checkpoint batch.
        if skip_removes {
            return Ok(None);
        }

        // Try to extract a remove action by the required path column
        if let Some(path) = getters[self.remove_path_index].get_str(i, "remove.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.remove_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), false)));
        }

        // No file action found
        Ok(None)
    }

    /// Returns whether we are currently processing a log batch.
    ///
    /// `true` indicates we are processing a batch from a commit file.
    /// `false` indicates we are processing a batch from a checkpoint.
    pub(crate) fn is_log_batch(&self) -> bool {
        self.is_log_batch
    }
}

impl RowFilter for SharedAddRemoveDedupFilter {
    fn filter_row<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        let x = self.is_valid_add(i, getters)?;
        // println!(" is valid is {x}");
        Ok(x)
    }
}
pub struct SharedAddRemoveDedupFilter {
    deduplicator: SharedFileActionDeduplicator,
    logical_schema: SchemaRef,
}

impl SharedAddRemoveDedupFilter {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_PARTITION_VALUES_INDEX: usize = 1; // Position of "add.partitionValues" in getters
    const ADD_DV_START_INDEX: usize = 2; // Start position of add deletion vector columns
    const REMOVE_PATH_INDEX: usize = 5; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 6; // Start position of remove deletion vector columns

    pub fn new(
        seen: Arc<RwLock<HashSet<FileActionKey>>>,
        logical_schema: SchemaRef,
        is_log_batch: bool,
    ) -> SharedAddRemoveDedupFilter {
        SharedAddRemoveDedupFilter {
            deduplicator: SharedFileActionDeduplicator::new(
                seen,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            logical_schema,
        }
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    pub fn is_valid_add<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // When processing file actions, we extract path and deletion vector information based on action type:
        // - For Add actions: path is at index 0, followed by DV fields at indexes 2-4
        // - For Remove actions (in log batches only): path is at index 5, followed by DV fields at indexes 6-8
        // The file extraction logic selects the appropriate indexes based on whether we found a valid path.
        // Remove getters are not included when visiting a non-log batch (checkpoint batch), so do
        // not try to extract remove actions in that case.
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(
            i,
            getters,
            !self.deduplicator.is_log_batch(), // skip_removes. true if this is a checkpoint batch
        )?
        else {
            return Ok(false);
        };

        // Check both adds and removes (skipping already-seen), but only transform and return adds
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        Ok(true)
    }
}

impl RowVisitor for SharedAddRemoveDedupFilter {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The visitor assumes a schema with adds first and removes optionally afterward.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        if self.deduplicator.is_log_batch() {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..5], &types[..5])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use object_store::local::LocalFileSystem;
    use std::process::ExitCode;
    use test_utils::{load_test_data, DefaultEngineExtension};
    use tokio::runtime::Runtime;

    use crate::arrow::compute::filter_record_batch;
    use crate::arrow::record_batch::RecordBatch;
    use crate::arrow::util::pretty::print_batches;
    use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use crate::engine::default::DefaultEngine;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::{DeltaResult, Snapshot};

    use itertools::process_results;
    use itertools::Itertools;

    use super::PhysicalPlanExecutor;
    use crate::{engine::sync::SyncEngine, kernel_df::DefaultPlanExecutor};

    #[test]
    fn test_scan_plan() {
        let path =
            std::fs::canonicalize(PathBuf::from("/Users/oussama/projects/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let task_executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::new(object_store, task_executor.into());

        let snapshot = Snapshot::builder(url).build(&engine).unwrap();
        let plan = snapshot.get_scan_plan().unwrap();
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
