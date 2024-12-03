//! Defines [`LogReplayScanner`] used by [`TableChangesScan`] to process commit files and extract
//! the metadata needed to generate the Change Data Feed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::{visit_deletion_vector_at, ProtocolVisitor};
use crate::actions::{
    get_log_add_schema, get_log_schema, Protocol, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::engine_data::TypedGetData;
use crate::expressions::{column_expr, column_name, ColumnName, Expression};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::scan_row_schema;
use crate::scan::state::DvInfo;
use crate::schema::{ArrayType, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructType};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, ExpressionRef, RowVisitor};
use itertools::Itertools;

#[cfg(test)]
mod tests;

pub struct TableChangesScanData {
    pub data: Box<dyn EngineData>,
    // The selection vector used to filter log actions.
    pub selection_vector: Vec<bool>,
    // An optional map from a remove action's path to its deletion vector
    pub remove_dv: Option<Arc<HashMap<String, DvInfo>>>,
}

/// Given an iterator of ParsedLogPath and a predicate, returns an iterator of
/// [`TableChangesScanData`]  Each row that is selected in the returned
/// `engine_data` _must_ be processed to complete the scan. Non-selected rows
/// _must_ be ignored.
pub(crate) fn table_changes_action_iter(
    engine: Arc<dyn Engine>,
    commit_files: impl IntoIterator<Item = ParsedLogPath>,
    table_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
    let filter = DataSkippingFilter::new(engine.as_ref(), &table_schema, predicate);

    let result = commit_files
        .into_iter()
        .map(move |commit_file| -> DeltaResult<_> {
            let scanner = prepare_table_changes(
                commit_file,
                engine.as_ref(),
                filter.clone(),
                table_schema.clone(),
            )?;
            scanner.into_scan_batches(engine.clone())
        })
        .flatten_ok()
        .map(|x| x?);
    Ok(result)
}

// Gets the expression for generating the engine data in [`TableChangesScanData`].
//
// TODO: This expression is temporary. In the future it will also select `cdc` and `remove` actions
// fields.
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

/// Processes a single commit file from the log to generate an iterator of [`TableChangesScanData`].
/// The scanner operates in two phases that _must_ be performed in the following order:
/// 1. Prepare phase [`LogReplayScanner::prepare_table_changes`]: This performs one iteration over every
///    action in the commit. In this phase, we do the following:
///     - Find the timestamp from a `CommitInfo` action if it exists. These are generated when
///       In-commit timestamps is enabled.
///     - Ensure that reading is supported on any protocol updates
///     - Ensure that Change Data Feed is enabled for any metadata update. See  [`TableProperties`]
///     - Ensure that any schema update is compatible with the provided `schema`. Currently, schema
///       compatibility is checked through schema equality. This will be expanded in the future to
///       allow limited schema evolution.
///     - Determine if there exist any `cdc` actions.
///     - Constructs the remove deletion vector map from paths belonging to `remove` actions to the
///       action's corresponding [`DvInfo`]. This map will be filtered to only contain paths that
///       exists in another `add` action _within the same commit_. This corresponds to `remove_dv`
///       in [`TableChangesScanData`].
/// 2. Scan file generation phase [`LogReplayScanner::into_scan_batches`]: This performs another
///    iteration over every action in the commit, and generates [`TableChangesScanData`]. It does
///    so by transforming the actions using [`get_add_transform_expr`], and generating selection
///    vectors with the following rules:
///     - If a `cdc` action was found in the prepare phase, only `cdc` actions are selected
///     - Otherwise, select `add` and `remove` actions. Note that only `remove` actions that do not
///       share a path with an `add` action are selected.
///
/// Note: As a consequence of the two phases, LogReplayScanner will iterate over each action in the
/// commit twice. It also may use an unbounded amount of memory, proportional to the number of
/// `add` + `remove` actions in the commit.
struct LogReplayScanner {
    // True if a `cdc` action was found after running [`LogReplayScanner::prepare_table_changes`]
    has_cdc_action: bool,
    // A map from path to the deletion vector from the remove action. It is guaranteed that there
    // is an add action with the same path in this commit
    remove_dvs: HashMap<String, DvInfo>,
    // The commit file that this replay scanner will operate on.
    commit_file: ParsedLogPath,
    // The timestamp associated with this commit. By default this is the file modification time
    // from the commit's [`FileMeta`]. If there is a [`CommitInfo`] with a timestamp generated by
    // in-commit timestamps, that timestamp will be used instead.
    timestamp: i64,
    // The data skipping filter for filtering log actions
    filter: Option<DataSkippingFilter>,
}

fn prepare_table_changes(
    commit_file: ParsedLogPath,
    engine: &dyn Engine,
    filter: Option<DataSkippingFilter>,
    table_schema: SchemaRef,
) -> DeltaResult<LogReplayScanner> {
    let visitor_schema = PreparePhaseVisitor::schema()?;
    let action_iter = engine.get_json_handler().read_json_files(
        &[commit_file.location.clone()],
        visitor_schema,
        None,
    )?;

    let mut remove_dvs = HashMap::default();
    let mut add_paths = HashSet::new();
    let mut has_cdc_action = false;
    let mut timestamp = commit_file.location.last_modified;
    for actions in action_iter {
        let actions = actions?;

        let selection_vector = match &filter {
            Some(filter) => filter.apply(actions.as_ref())?,
            None => vec![true; actions.len()],
        };

        let mut visitor = PreparePhaseVisitor::new(
            selection_vector,
            &mut add_paths,
            &mut remove_dvs,
            &mut has_cdc_action,
            &mut timestamp,
        );
        visitor.visit_rows_of(actions.as_ref())?;

        if let Some(protocol) = visitor.protocol {
            protocol.ensure_read_supported()?;
        }
        if let Some((schema, configuration)) = visitor.metadata_info {
            let schema: StructType = serde_json::from_str(&schema)?;
            require!(
                table_schema.as_ref() == &schema,
                Error::change_data_feed_incompatible_schema(&table_schema, &schema)
            );

            let table_properties = TableProperties::from(configuration);
            require!(
                table_properties.enable_change_data_feed.unwrap_or(false),
                Error::change_data_feed_unsupported(commit_file.version)
            )
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
        timestamp,
        commit_file,
        has_cdc_action,
        remove_dvs,
        filter,
    })
}
impl LogReplayScanner {
    fn into_scan_batches(
        self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let Self {
            has_cdc_action,
            remove_dvs,
            commit_file,
            timestamp: _,
            filter,
        } = self;
        let remove_dvs = Arc::new(remove_dvs);

        let schema = FileActionSelectionVisitor::schema()?;
        let action_iter = engine.get_json_handler().read_json_files(
            &[commit_file.location.clone()],
            schema,
            None,
        )?;

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

            let data = engine
                .get_expression_handler()
                .get_evaluator(
                    get_log_add_schema().clone(),
                    get_add_transform_expr(),
                    scan_row_schema().into(),
                )
                .evaluate(actions.as_ref())?;
            Ok(TableChangesScanData {
                data,
                selection_vector: visitor.selection_vector,
                remove_dv: Some(remove_dvs.clone()),
            })
        });
        Ok(result)
    }
}

// This is a visitor used in the prepare phase of [`LogReplayScanner`]. See
// [`LogReplayScanner::prepare_table_changes`] for details usage.
struct PreparePhaseVisitor<'a> {
    selection_vector: Vec<bool>,
    protocol: Option<Protocol>,
    metadata_info: Option<(String, HashMap<String, String>)>,
    timestamp: &'a mut i64,
    has_cdc_action: &'a mut bool,
    add_paths: &'a mut HashSet<String>,
    remove_dvs: &'a mut HashMap<String, DvInfo>,
}
impl<'a> PreparePhaseVisitor<'a> {
    fn new(
        selection_vector: Vec<bool>,
        add_paths: &'a mut HashSet<String>,
        remove_dvs: &'a mut HashMap<String, DvInfo>,
        has_cdc_action: &'a mut bool,
        timestamp: &'a mut i64,
    ) -> Self {
        PreparePhaseVisitor {
            selection_vector,
            protocol: None,
            metadata_info: None,
            timestamp,
            add_paths,
            has_cdc_action,
            remove_dvs,
        }
    }
    fn schema() -> DeltaResult<Arc<StructType>> {
        get_log_schema().project(&[
            ADD_NAME,
            REMOVE_NAME,
            CDC_NAME,
            COMMIT_INFO_NAME,
            METADATA_NAME,
            PROTOCOL_NAME,
        ])
    }
}

impl<'a> RowVisitor for PreparePhaseVisitor<'a> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The order of the names and types is based on [`PreparePhaseVisitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let string_list: DataType = ArrayType::new(STRING, false).into();
            let string_string_map = MapType::new(STRING, STRING, false).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                (INTEGER, column_name!("remove.deletionVector.sizeInBytes")),
                (LONG, column_name!("remove.deletionVector.cardinality")),
                (STRING, column_name!("cdc.path")),
                (LONG, column_name!("commitInfo.timestamp")),
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

    fn visit<'b>(
        &mut self,
        row_count: usize,
        getters: &[&'b dyn crate::engine_data::GetData<'b>],
    ) -> DeltaResult<()> {
        require!(
            getters.len() == 15,
            Error::InternalError(format!(
                "Wrong number of PreparePhaseVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }
            if let Some(path) = getters[0].get_str(i, "add.path")? {
                self.add_paths.insert(path.to_string());
            } else if let Some(path) = getters[1].get_str(i, "remove.path")? {
                let deletion_vector = visit_deletion_vector_at(i, &getters[2..=6])?;
                self.remove_dvs
                    .insert(path.to_string(), DvInfo { deletion_vector });
            } else if getters[7].get_str(i, "cdc.path")?.is_some() {
                *self.has_cdc_action = true;
            } else if let Some(timestamp) = getters[8].get_long(i, "commitInfo.timestamp")? {
                *self.timestamp = timestamp;
            } else if let Some(schema) = getters[9].get_str(i, "metaData.schemaString")? {
                let configuration_map_opt: Option<HashMap<_, _>> =
                    getters[10].get_opt(i, "metadata.configuration")?;

                // A null configuration here means that we found an empty configuration. This is
                // different from not finding any metadata action, where self.metadata = None
                let configuration = configuration_map_opt.unwrap_or_else(HashMap::new);

                self.metadata_info = Some((schema.to_string(), configuration));
            } else if let Some(min_reader_version) =
                getters[11].get_int(i, "protocol.min_reader_version")?
            {
                let protocol =
                    ProtocolVisitor::visit_protocol(i, min_reader_version, &getters[11..=14])?;
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
    fn schema() -> DeltaResult<Arc<StructType>> {
        get_log_schema().project(&[ADD_NAME, REMOVE_NAME, CDC_NAME])
    }
}

impl<'a> RowVisitor for FileActionSelectionVisitor<'a> {
    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [crate::expressions::ColumnName],
        &'static [crate::schema::DataType],
    ) {
        // NOTE: The order of the names and types is based on [`FileActionSelectionVisitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("cdc.path")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'b>(
        &mut self,
        row_count: usize,
        getters: &[&'b dyn crate::engine_data::GetData<'b>],
    ) -> DeltaResult<()> {
        let expected_getters = 3;
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }
            if getters[0].get_str(i, "add.path")?.is_some() {
                self.selection_vector[i] = !self.has_cdc_action;
            } else if let Some(path) = getters[1].get_str(i, "remove.path")? {
                self.selection_vector[i] =
                    !self.has_cdc_action && !self.remove_dvs.contains_key(path)
            } else {
                self.selection_vector[i] = getters[2].get_str(i, "cdc.path")?.is_some()
            };
        }
        Ok(())
    }
}
