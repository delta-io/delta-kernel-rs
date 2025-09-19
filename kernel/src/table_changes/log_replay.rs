//! Defines [`LogReplayScanner`] used by [`TableChangesScan`] to process commit files and extract
//! the metadata needed to generate the Change Data Feed.

use std::collections::{HashMap, HashSet};
use std::slice;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::{visit_deletion_vector_at, visit_protocol_at};
use crate::actions::{
    get_log_add_schema, Add, Cdc, Metadata, Protocol, Remove, ADD_NAME, CDC_NAME, METADATA_NAME,
    PROTOCOL_NAME, REMOVE_NAME,
};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_name, ColumnName};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::state::DvInfo;
use crate::schema::{
    ArrayType, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType,
    ToSchema as _,
};
use crate::table_changes::scan_file::{cdf_scan_row_expression, cdf_scan_row_schema};
use crate::table_changes::{check_cdf_table_properties, ensure_cdf_read_supported};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, PredicateRef, RowVisitor};

use itertools::Itertools;

#[cfg(test)]
mod tests;

/// Scan metadata for a Change Data Feed query. This holds metadata that's needed to read data rows.
pub(crate) struct TableChangesScanMetadata {
    /// Engine data with the schema defined in [`scan_row_schema`]
    ///
    /// Note: The schema of the engine data will be updated in the future to include columns
    /// used by Change Data Feed.
    pub(crate) scan_metadata: Box<dyn EngineData>,
    /// The selection vector used to filter the `scan_metadata`.
    pub(crate) selection_vector: Vec<bool>,
    /// A map from a remove action's path to its deletion vector
    pub(crate) remove_dvs: Arc<HashMap<String, DvInfo>>,
}

/// Given an iterator of [`ParsedLogPath`] returns an iterator of [`TableChangesScanMetadata`].
/// Each row that is selected in the returned `TableChangesScanMetadata.scan_metadata` (according
/// to the `selection_vector` field) _must_ be processed to complete the scan. Non-selected
/// rows _must_ be ignored.
///
/// Note: The [`ParsedLogPath`]s in the `commit_files` iterator must be ordered, contiguous
/// (JSON) commit files.
pub(crate) fn table_changes_action_iter(
    engine: Arc<dyn Engine>,
    commit_files: impl IntoIterator<Item = ParsedLogPath>,
    table_schema: SchemaRef,
    physical_predicate: Option<(PredicateRef, SchemaRef)>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>> {
    let filter = DataSkippingFilter::new(engine.as_ref(), physical_predicate).map(Arc::new);
    let result = commit_files
        .into_iter()
        .map(move |commit_file| -> DeltaResult<_> {
            let scanner = LogReplayScanner::try_new(engine.as_ref(), commit_file, &table_schema)?;
            scanner.into_scan_batches(engine.clone(), filter.clone())
        }) //Iterator-Result-Iterator-Result
        .flatten_ok() // Iterator-Result-Result
        .map(|x| x?); // Iterator-Result
    Ok(result)
}

/// Processes a single commit file from the log to generate an iterator of
/// [`TableChangesScanMetadata`]. The scanner operates in two phases that _must_ be performed in the
/// following order:
/// 1. Prepare phase [`LogReplayScanner::try_new`]: This iterates over every action in the commit.
///    In this phase, we do the following:
///     - Determine if there exist any `cdc` actions. We determine this in the first phase because
///       the selection vectors for actions are lazily constructed in phase 2. We must know ahead
///       of time whether to filter out add/remove actions.
///     - Constructs the remove deletion vector map from paths belonging to `remove` actions to the
///       action's corresponding [`DvInfo`]. This map will be filtered to only contain paths that
///       exists in another `add` action _within the same commit_. We store the result in `remove_dvs`.
///       Deletion vector resolution affects whether a remove action is selected in the second
///       phase, so we must perform it ahead of time in phase 1.
///     - Ensure that reading is supported on any protocol updates.
///     - Ensure that Change Data Feed is enabled for any metadata update. See  [`TableProperties`]
///     - Ensure that any schema update is compatible with the provided `schema`. Currently, schema
///       compatibility is checked through schema equality. This will be expanded in the future to
///       allow limited schema evolution.
///
/// Note: We check the protocol, change data feed enablement, and schema compatibility in phase 1
/// in order to detect errors and fail early.
///
/// Note: The reader feature [`ReaderFeatures::DeletionVectors`] controls whether the table is
/// allowed to contain deletion vectors. [`TableProperties`].enable_deletion_vectors only
/// determines whether writers are allowed to create _new_ deletion vectors. Hence, we do not need
/// to check the table property for deletion vector enablement.
///
/// See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors
///
/// TODO: When the kernel supports in-commit timestamps, we will also have to inspect CommitInfo
/// actions to find the timestamp. These are generated when incommit timestamps is enabled.
/// This must be done in the first phase because the second phase lazily transforms engine data with
/// an extra timestamp column. Thus, the timestamp must be known ahead of time.
/// See https://github.com/delta-io/delta-kernel-rs/issues/559
///
/// 2. Scan file generation phase [`LogReplayScanner::into_scan_batches`]: This iterates over every
///    action in the commit, and generates [`TableChangesScanMetadata`]. It does so by transforming the
///    actions using [`add_transform_expr`], and generating selection vectors with the following rules:
///     - If a `cdc` action was found in the prepare phase, only `cdc` actions are selected
///     - Otherwise, select `add` and `remove` actions. Note that only `remove` actions that do not
///       share a path with an `add` action are selected.
///
/// Note: As a consequence of the two phases, LogReplayScanner will iterate over each action in the
/// commit twice. It also may use an unbounded amount of memory, proportional to the number of
/// `add` + `remove` actions in the _single_ commit.
struct LogReplayScanner {
    // True if a `cdc` action was found after running [`LogReplayScanner::try_new`]
    has_cdc_action: bool,
    // A map from path to the deletion vector from the remove action. It is guaranteed that there
    // is an add action with the same path in this commit
    remove_dvs: HashMap<String, DvInfo>,
    // The commit file that this replay scanner will operate on.
    commit_file: ParsedLogPath,
    // The timestamp associated with this commit. This is the file modification time
    // from the commit's [`FileMeta`].
    //
    //
    // TODO when incommit timestamps are supported: If there is a [`CommitInfo`] with a timestamp
    // generated by in-commit timestamps, that timestamp will be used instead.
    //
    // Note: This will be used once an expression is introduced to transform the engine data in
    // [`TableChangesScanMetadata`]
    timestamp: i64,
}

impl LogReplayScanner {
    /// Constructs a LogReplayScanner, performing the Prepare phase detailed in [`LogReplayScanner`].
    /// This iterates over each action in the commit. It performs the following:
    /// 1. Check the commits for the presence of a `cdc` action.
    /// 2. Construct a map from path to deletion vector of remove actions that share the same path
    ///    as an add action.
    /// 3. Perform validation on each protocol and metadata action in the commit.
    ///
    /// For more details, see the documentation for [`LogReplayScanner`].
    fn try_new(
        engine: &dyn Engine,
        commit_file: ParsedLogPath,
        table_schema: &SchemaRef,
    ) -> DeltaResult<Self> {
        let visitor_schema = PreparePhaseVisitor::schema();

        // Note: We do not perform data skipping yet because we need to visit all add and
        // remove actions for deletion vector resolution to be correct.
        //
        // Consider a scenario with a pair of add/remove actions with the same path. The add
        // action has file statistics, while the remove action does not (stats is optional for
        // remove). In this scenario we might skip the add action, while the remove action remains.
        // As a result, we would read the file path for the remove action, which is unnecessary because
        // all of the rows will be filtered by the predicate. Instead, we wait until deletion
        // vectors are resolved so that we can skip both actions in the pair.
        let action_iter = engine.json_handler().read_json_files(
            slice::from_ref(&commit_file.location),
            visitor_schema,
            None, // not safe to apply data skipping yet
        )?;

        let mut remove_dvs = HashMap::default();
        let mut add_paths = HashSet::default();
        let mut has_cdc_action = false;
        for actions in action_iter {
            let actions = actions?;

            let mut visitor = PreparePhaseVisitor {
                add_paths: &mut add_paths,
                remove_dvs: &mut remove_dvs,
                has_cdc_action: &mut has_cdc_action,
                protocol: None,
                metadata_info: None,
            };
            visitor.visit_rows_of(actions.as_ref())?;

            if let Some(protocol) = visitor.protocol {
                ensure_cdf_read_supported(&protocol)
                    .map_err(|_| Error::change_data_feed_unsupported(commit_file.version))?;
            }
            if let Some((schema, configuration)) = visitor.metadata_info {
                let schema: StructType = serde_json::from_str(&schema)?;
                // Currently, schema compatibility is defined as having equal schema types. In the
                // future, more permisive schema evolution will be supported.
                // See: https://github.com/delta-io/delta-kernel-rs/issues/523
                require!(
                    table_schema.as_ref() == &schema,
                    Error::change_data_feed_incompatible_schema(table_schema, &schema)
                );
                let table_properties = TableProperties::from(configuration);
                check_cdf_table_properties(&table_properties)
                    .map_err(|_| Error::change_data_feed_unsupported(commit_file.version))?;
            }
        }
        // We resolve the remove deletion vector map after visiting the entire commit.
        if has_cdc_action {
            remove_dvs.clear();
        } else {
            // The only (path, deletion_vector) pairs we must track are ones whose path is the
            // same as an `add` action.
            remove_dvs.retain(|rm_path, _| add_paths.contains(rm_path));
        }
        Ok(LogReplayScanner {
            timestamp: commit_file.location.last_modified,
            commit_file,
            has_cdc_action,
            remove_dvs,
        })
    }
    /// Generates an iterator of [`TableChangesScanMetadata`] by iterating over each action of the
    /// commit, generating a selection vector, and transforming the engine data. This performs
    /// phase 2 of [`LogReplayScanner`].
    fn into_scan_batches(
        self,
        engine: Arc<dyn Engine>,
        filter: Option<Arc<DataSkippingFilter>>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>> {
        let Self {
            has_cdc_action,
            remove_dvs,
            commit_file,
            // TODO: Add the timestamp as a column with an expression
            timestamp,
        } = self;
        let remove_dvs = Arc::new(remove_dvs);

        let schema = FileActionSelectionVisitor::schema();
        let action_iter = engine.json_handler().read_json_files(
            slice::from_ref(&commit_file.location),
            schema,
            None,
        )?;
        let commit_version = commit_file
            .version
            .try_into()
            .map_err(|_| Error::generic("Failed to convert commit version to i64"))?;
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            get_log_add_schema().clone(),
            Arc::new(cdf_scan_row_expression(timestamp, commit_version)),
            cdf_scan_row_schema().into(),
        );

        let result = action_iter.map(move |actions| -> DeltaResult<_> {
            let actions = actions?;

            // Apply data skipping to get back a selection vector for actions that passed skipping.
            // We start our selection vector based on what was filtered. We will add to this vector
            // below if a file has been removed. Note: None implies all files passed data skipping.
            let selection_vector = match &filter {
                Some(filter) => filter.apply(actions.as_ref())?,
                None => vec![true; actions.len()],
            };

            let mut visitor =
                FileActionSelectionVisitor::new(&remove_dvs, selection_vector, has_cdc_action);
            visitor.visit_rows_of(actions.as_ref())?;
            let scan_metadata = evaluator.evaluate(actions.as_ref())?;
            Ok(TableChangesScanMetadata {
                scan_metadata,
                selection_vector: visitor.selection_vector,
                remove_dvs: remove_dvs.clone(),
            })
        });
        Ok(result)
    }
}

// This is a visitor used in the prepare phase of [`LogReplayScanner`]. See
// [`LogReplayScanner::try_new`] for details usage.
struct PreparePhaseVisitor<'a> {
    protocol: Option<Protocol>,
    metadata_info: Option<(String, HashMap<String, String>)>,
    has_cdc_action: &'a mut bool,
    add_paths: &'a mut HashSet<String>,
    remove_dvs: &'a mut HashMap<String, DvInfo>,
}
impl PreparePhaseVisitor<'_> {
    fn schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(ADD_NAME, Add::to_schema()),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
            StructField::nullable(CDC_NAME, Cdc::to_schema()),
            StructField::nullable(METADATA_NAME, Metadata::to_schema()),
            StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        ]))
    }
}

impl RowVisitor for PreparePhaseVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The order of the names and types is based on [`PreparePhaseVisitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            const BOOLEAN: DataType = DataType::BOOLEAN;
            let string_list: DataType = ArrayType::new(STRING, false).into();
            let string_string_map = MapType::new(STRING, STRING, false).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (BOOLEAN, column_name!("add.dataChange")),
                (STRING, column_name!("remove.path")),
                (BOOLEAN, column_name!("remove.dataChange")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                (INTEGER, column_name!("remove.deletionVector.sizeInBytes")),
                (LONG, column_name!("remove.deletionVector.cardinality")),
                (STRING, column_name!("cdc.path")),
                (STRING, column_name!("metaData.schemaString")),
                (string_string_map, column_name!("metaData.configuration")),
                (INTEGER, column_name!("protocol.minReaderVersion")),
                (INTEGER, column_name!("protocol.minWriterVersion")),
                (string_list.clone(), column_name!("protocol.readerFeatures")),
                (string_list, column_name!("protocol.writerFeatures")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        require!(
            getters.len() == 16,
            Error::InternalError(format!(
                "Wrong number of PreparePhaseVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            if let Some(path) = getters[0].get_str(i, "add.path")? {
                // If no data was changed, we must ignore that action
                if !*self.has_cdc_action && getters[1].get(i, "add.dataChange")? {
                    self.add_paths.insert(path.to_string());
                }
            } else if let Some(path) = getters[2].get_str(i, "remove.path")? {
                // If no data was changed, we must ignore that action
                if !*self.has_cdc_action && getters[3].get(i, "remove.dataChange")? {
                    let deletion_vector = visit_deletion_vector_at(i, &getters[4..=8])?;
                    self.remove_dvs
                        .insert(path.to_string(), DvInfo { deletion_vector });
                }
            } else if getters[9].get_str(i, "cdc.path")?.is_some() {
                *self.has_cdc_action = true;
            } else if let Some(schema) = getters[10].get_str(i, "metaData.schemaString")? {
                let configuration_map_opt = getters[11].get_opt(i, "metadata.configuration")?;
                let configuration = configuration_map_opt.unwrap_or_else(HashMap::new);
                self.metadata_info = Some((schema.to_string(), configuration));
            } else if let Some(protocol) = visit_protocol_at(i, &getters[12..])? {
                self.protocol = Some(protocol);
            }
        }
        Ok(())
    }
}

// This visitor generates selection vectors based on the rules specified in [`LogReplayScanner`].
// See [`LogReplayScanner::into_scan_batches`] for usage.
struct FileActionSelectionVisitor<'a> {
    selection_vector: Vec<bool>,
    has_cdc_action: bool,
    remove_dvs: &'a HashMap<String, DvInfo>,
}

impl<'a> FileActionSelectionVisitor<'a> {
    fn new(
        remove_dvs: &'a HashMap<String, DvInfo>,
        selection_vector: Vec<bool>,
        has_cdc_action: bool,
    ) -> Self {
        FileActionSelectionVisitor {
            selection_vector,
            has_cdc_action,
            remove_dvs,
        }
    }
    fn schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(CDC_NAME, Cdc::to_schema()),
            StructField::nullable(ADD_NAME, Add::to_schema()),
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        ]))
    }
}

impl RowVisitor for FileActionSelectionVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // Note: The order of the names and types is based on [`FileActionSelectionVisitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const BOOLEAN: DataType = DataType::BOOLEAN;
            let types_and_names = vec![
                (STRING, column_name!("cdc.path")),
                (STRING, column_name!("add.path")),
                (BOOLEAN, column_name!("add.dataChange")),
                (STRING, column_name!("remove.path")),
                (BOOLEAN, column_name!("remove.dataChange")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        require!(
            getters.len() == 5,
            Error::InternalError(format!(
                "Wrong number of FileActionSelectionVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }

            if self.has_cdc_action {
                self.selection_vector[i] = getters[0].get_str(i, "cdc.path")?.is_some()
            } else if getters[1].get_str(i, "add.path")?.is_some() {
                self.selection_vector[i] = getters[2].get(i, "add.dataChange")?;
            } else if let Some(path) = getters[3].get_str(i, "remove.path")? {
                let data_change: bool = getters[4].get(i, "remove.dataChange")?;
                self.selection_vector[i] = data_change && !self.remove_dvs.contains_key(path)
            } else {
                self.selection_vector[i] = false
            }
        }
        Ok(())
    }
}
