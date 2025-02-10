use std::clone::Clone;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use tracing::debug;

use super::data_skipping::DataSkippingFilter;
use super::ScanData;
use crate::actions::{get_log_add_schema, get_log_schema, ADD_NAME, REMOVE_NAME};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_expr, column_name, ColumnName, Expression, ExpressionRef};
use crate::path::{self, LogPathFileType, ParsedLogPath};
use crate::scan::DeletionVectorDescriptor;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, ExpressionEvaluator, JsonHandler, ParquetHandler,
};

#[derive(Debug, Hash, Eq, PartialEq)]
struct FileActionKey {
    path: String,
    dv_unique_id: Option<String>,
}
impl FileActionKey {
    fn new(path: impl Into<String>, dv_unique_id: Option<String>) -> Self {
        let path = path.into();
        Self { path, dv_unique_id }
    }
}

struct LogReplayScanner {
    filter: Option<DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<FileActionKey>,
}

/// A visitor that deduplicates a stream of add and remove actions into a stream of valid adds. Log
/// replay visits actions newest-first, so once we've seen a file action for a given (path, dvId)
/// pair, we should ignore all subsequent (older) actions for that same (path, dvId) pair. If the
/// first action for a given file is a remove, then that file does not show up in the result at all.
struct AddRemoveDedupVisitor<'seen> {
    seen: &'seen mut HashSet<FileActionKey>,
    selection_vector: Vec<bool>,
    is_log_batch: bool,
}

impl AddRemoveDedupVisitor<'_> {
    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    fn check_and_record_seen(&mut self, key: FileActionKey) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen.contains(&key) {
            debug!(
                "Ignoring duplicate ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            true
        } else {
            debug!(
                "Including ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen.insert(key);
            }
            false
        }
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    fn is_valid_add<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // Add will have a path at index 0 if it is valid; otherwise, if it is a log batch, we may
        // have a remove with a path at index 4. In either case, extract the three dv getters at
        // indexes that immediately follow a valid path index.
        let (path, dv_getters, is_add) = if let Some(path) = getters[0].get_str(i, "add.path")? {
            (path, &getters[1..4], true)
        } else if !self.is_log_batch {
            return Ok(false);
        } else if let Some(path) = getters[4].get_opt(i, "remove.path")? {
            (path, &getters[5..8], false)
        } else {
            return Ok(false);
        };

        let dv_unique_id = match dv_getters[0].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => Some(DeletionVectorDescriptor::unique_id_from_parts(
                storage_type,
                dv_getters[1].get(i, "deletionVector.pathOrInlineDv")?,
                dv_getters[2].get_opt(i, "deletionVector.offset")?,
            )),
            None => None,
        };

        // Process both adds and removes, but only return not already-seen adds
        let file_key = FileActionKey::new(path, dv_unique_id);
        Ok(!self.check_and_record_seen(file_key) && is_add)
    }
}

impl RowVisitor for AddRemoveDedupVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The visitor assumes a schema with adds first and removes optionally afterward.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
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
        if self.is_log_batch {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..4], &types[..4])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let expected_getters = if self.is_log_batch { 8 } else { 4 };
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if self.selection_vector[i] {
                self.selection_vector[i] = self.is_valid_add(i, getters)?;
            }
        }
        Ok(())
    }
}

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in scan/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off, and [`get_add_transform_expr`] below to match it.
pub(crate) static SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
    let file_constant_values =
        StructType::new([StructField::new("partitionValues", partition_values, true)]);
    let deletion_vector = StructType::new([
        StructField::new("storageType", DataType::STRING, true),
        StructField::new("pathOrInlineDv", DataType::STRING, true),
        StructField::new("offset", DataType::INTEGER, true),
        StructField::new("sizeInBytes", DataType::INTEGER, true),
        StructField::new("cardinality", DataType::LONG, true),
    ]);
    Arc::new(StructType::new([
        StructField::new("path", DataType::STRING, true),
        StructField::new("size", DataType::LONG, true),
        StructField::new("modificationTime", DataType::LONG, true),
        StructField::new("stats", DataType::STRING, true),
        StructField::new("deletionVector", deletion_vector, true),
        StructField::new("fileConstantValues", file_constant_values, true),
    ]))
});

pub(crate) static SCAN_ROW_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| SCAN_ROW_SCHEMA.clone().into());

fn get_add_transform_expr() -> Expression {
    Expression::Struct(vec![
        column_expr!("add.path"),
        column_expr!("add.size"),
        column_expr!("add.modificationTime"),
        column_expr!("add.stats"),
        column_expr!("add.deletionVector"),
        Expression::Struct(vec![column_expr!("add.partitionValues")]),
    ])
}

impl LogReplayScanner {
    /// Create a new [`LogReplayScanner`] instance
    fn new(engine: &dyn Engine, physical_predicate: Option<(ExpressionRef, SchemaRef)>) -> Self {
        Self {
            filter: DataSkippingFilter::new(engine, physical_predicate),
            seen: Default::default(),
        }
    }

    fn process_scan_batch(
        &mut self,
        add_transform: &dyn ExpressionEvaluator,
        actions: &dyn EngineData,
        is_log_batch: bool,
    ) -> DeltaResult<ScanData> {
        // Apply data skipping to get back a selection vector for actions that passed skipping. We
        // will update the vector below as log replay identifies duplicates that should be ignored.
        let selection_vector = match &self.filter {
            Some(filter) => filter.apply(actions)?,
            None => vec![true; actions.len()],
        };
        assert_eq!(selection_vector.len(), actions.len());

        let mut visitor = AddRemoveDedupVisitor {
            seen: &mut self.seen,
            selection_vector,
            is_log_batch,
        };
        visitor.visit_rows_of(actions)?;

        // TODO: Teach expression eval to respect the selection vector we just computed so carefully!
        let selection_vector = visitor.selection_vector;
        let result = add_transform.evaluate(actions)?;
        Ok((result, selection_vector))
    }
}

pub struct DeltaScanIterator {
    // engine: &dyn Engine,
    files_list: VecDeque<ParsedLogPath>,
    log_scanner: LogReplayScanner,
    physical_predicate: Option<(ExpressionRef, SchemaRef)>,
    commit_read_schema: SchemaRef,
    checkpoint_read_schema: SchemaRef,
    add_transform: Arc<dyn ExpressionEvaluator>,
    json_handler: Arc<dyn JsonHandler>,
    parquet_handler: Arc<dyn ParquetHandler>,
}

impl DeltaScanIterator {
    pub(crate) fn new(
        engine: &dyn Engine,
        commit_files: Vec<ParsedLogPath>,
        checkpoint_files: Vec<ParsedLogPath>,
        physical_predicate: Option<(ExpressionRef, SchemaRef)>,
    ) -> Self {
        let mut files_list = VecDeque::new();

        // Add commit files in reverse order
        for path in commit_files.into_iter().rev() {
            files_list.push_back(path);
        }

        // Add checkpoint files
        for path in checkpoint_files {
            files_list.push_back(path);
        }

        let log_scanner = LogReplayScanner::new(engine, physical_predicate.clone());
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME]).unwrap();
        let checkpoint_read_schema = get_log_add_schema().clone();

        let add_transform: Arc<dyn ExpressionEvaluator> =
            engine.get_expression_handler().get_evaluator(
                get_log_add_schema().clone(),
                get_add_transform_expr(),
                SCAN_ROW_DATATYPE.clone(),
            );

        Self {
            files_list,
            log_scanner,
            physical_predicate,
            commit_read_schema,
            checkpoint_read_schema,
            add_transform,
            json_handler: engine.get_json_handler(),
            parquet_handler: engine.get_parquet_handler(),
        }
    }
}

impl Iterator for DeltaScanIterator {
    type Item = DeltaResult<ScanData>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(path) = self.files_list.pop_front() {
            // check if commit or checkpoint file or ...
            match path.file_type {
                LogPathFileType::Commit => {
                    let action_iter = self
                        .json_handler
                        .read_json_files(
                            &[path.location.clone()],
                            self.commit_read_schema.clone(),
                            None,
                        )
                        .unwrap();

                    result
                    //we are returning an iterator here, shit
                    //problem stems from the fact that our file reader returns batches, and we somehow need to turn batches of enginedata into a single ScanData
                    //idk if this is possible...
                }
                LogPathFileType::UuidCheckpoint(uuid) => {
                    let action_iter = self
                        .json_handler
                        .read_json_files(
                            &[path.location.clone()],
                            self.commit_read_schema.clone(),
                            None,
                        )
                        .unwrap();

                    let result = action_iter.map(move |actions| -> DeltaResult<_> {
                        let actions = actions?;

                        self.log_scanner.process_scan_batch(
                            self.add_transform.as_ref(),
                            actions.as_ref(),
                            false,
                        )
                    });
                    Some(Ok(result.flatten().next().unwrap()))
                }
                _ => None,
            }
        } else {
            None
        }
    }
}
