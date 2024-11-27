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
use crate::expressions::column_name;
use crate::expressions::{column_expr, Expression};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::log_replay::SCAN_ROW_DATATYPE;
use crate::scan::state::DvInfo;
use crate::schema::{ArrayType, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructType};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, ExpressionEvaluator, ExpressionRef, JsonHandler,
    RowVisitor,
};
use itertools::Itertools;

#[allow(unused)]
pub struct TableChangesScanData {
    data: Box<dyn EngineData>,
    selection_vector: Vec<bool>,
    remove_dv: Option<Arc<HashMap<String, DvInfo>>>,
}

/// Given an iterator of ParsedLogPath and a predicate, returns an iterator of
/// [`TableChangesScanData`]  Each row that is selected in the returned
/// `engine_data` _must_ be processed to complete the scan. Non-selected rows
/// _must_ be ignored.
pub(crate) fn table_changes_action_iter(
    engine: &dyn Engine,
    commit_files: impl IntoIterator<Item = ParsedLogPath>,
    table_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
    let json_handler = engine.get_json_handler();
    let filter = DataSkippingFilter::new(engine, &table_schema, predicate);

    let expression_evaluator = engine.get_expression_handler().get_evaluator(
        get_log_add_schema().clone(),
        get_add_transform_expr(),
        SCAN_ROW_DATATYPE.clone(),
    );

    let result = commit_files
        .into_iter()
        .map(move |commit_file| -> DeltaResult<_> {
            let mut scanner = LogReplayScanner::new(
                commit_file,
                json_handler.clone(),
                filter.clone(),
                expression_evaluator.clone(),
                table_schema.clone(),
            );
            scanner.prepare_phase()?;
            scanner.into_scan_batches()
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
/// 1. Prepare phase [`LogReplayScanner::prepare_phase`]: This performs one iteration over every
///    action in the commit. In this phase, we do the following:
///     - Find the timestamp from a `CommitInfo` action if it exists. These are generated when
///       In-commit timestamps is enabled.
///     - Ensure that reading is supported on any protocol updates
///     - Ensure that Change Data Feed is enabled for any metadata update. See  [`TableProperties`]
///     - Ensure that any schema update is compatible with the provided `schema`. Currently, schema
///       compatibility is checked through schema equality. This will be expanded in the future to
///       allow limited schema evolution.
///     - Determine if there exists any `cdc` actions.
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
/// `add` and `remove` actions in the commit.
struct LogReplayScanner {
    // True if a `cdc` action was found after running [`LogReplayScanner::prepare_phase`]
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
    // The schema of the table
    schema: SchemaRef,
    json_handler: Arc<dyn JsonHandler>,
    filter: Option<DataSkippingFilter>,
    expression_evaluator: Arc<dyn ExpressionEvaluator>,
}

impl LogReplayScanner {
    fn new(
        commit_file: ParsedLogPath,
        json_handler: Arc<dyn JsonHandler>,
        filter: Option<DataSkippingFilter>,
        expression_evaluator: Arc<dyn ExpressionEvaluator>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            timestamp: commit_file.location.last_modified,
            commit_file,
            json_handler,
            filter,
            has_cdc_action: Default::default(),
            remove_dvs: Default::default(),
            expression_evaluator,
            schema,
        }
    }
    fn prepare_phase(&mut self) -> DeltaResult<()> {
        let schema = PreparePhaseVisitor::schema()?;
        let action_iter = self.json_handler.read_json_files(
            &[self.commit_file.location.clone()],
            schema,
            None,
        )?;

        let mut add_paths = HashSet::new();
        for actions in action_iter {
            let actions = actions?;

            // Apply data skipping to get back a selection vector for actions that passed skipping.
            // We start our selection vector based on what was filtered. We will add to this vector
            // below if a file has been removed. Note: None implies all files passed data skipping.
            let selection_vector = match &self.filter {
                Some(filter) => filter.apply(actions.as_ref())?,
                None => vec![true; actions.len()],
            };

            let mut visitor = PreparePhaseVisitor::new(self, selection_vector, &mut add_paths);
            visitor.visit_rows_of(actions.as_ref())?;

            if let Some(protocol) = visitor.protocol {
                protocol.ensure_read_supported()?;
            }
            if let Some((schema, configuration)) = visitor.metadata_info {
                let schema: StructType = serde_json::from_str(&schema)?;
                require!(
                    self.schema.as_ref() == &schema,
                    Error::change_data_feed_incompatible_schema(&self.schema, &schema)
                );

                let table_properties = TableProperties::from(configuration);
                require!(
                    table_properties.enable_change_data_feed.unwrap_or(false),
                    Error::change_data_feed_unsupported(self.commit_file.version)
                )
            }
        }

        // We resolve the remove deletion vector map after visiting the entire commit.
        if self.has_cdc_action {
            self.remove_dvs.clear();
        } else {
            // The only (path, deletion_vector) pairs we must track are ones whose path is the
            // same as an `add` action.
            self.remove_dvs
                .retain(|rm_path, _| add_paths.contains(rm_path));
        }
        Ok(())
    }

    fn into_scan_batches(
        self,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let Self {
            has_cdc_action,
            remove_dvs,
            commit_file,
            json_handler,
            timestamp: _,
            filter,
            expression_evaluator,
            schema: _,
        } = self;
        let remove_dvs = Arc::new(remove_dvs);

        let schema = FileActionSelectionVisitor::schema()?;
        let action_iter =
            json_handler.read_json_files(&[commit_file.location.clone()], schema, None)?;

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

            let data = expression_evaluator.evaluate(actions.as_ref())?;
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
// [`LogReplayScanner::prepare_phase`] for details usage.
struct PreparePhaseVisitor<'a> {
    scanner: &'a mut LogReplayScanner,
    selection_vector: Vec<bool>,
    protocol: Option<Protocol>,
    metadata_info: Option<(String, HashMap<String, String>)>,
    add_paths: &'a mut HashSet<String>,
}
impl<'a> PreparePhaseVisitor<'a> {
    fn new(
        scanner: &'a mut LogReplayScanner,
        selection_vector: Vec<bool>,
        add_paths: &'a mut HashSet<String>,
    ) -> Self {
        PreparePhaseVisitor {
            scanner,
            selection_vector,
            protocol: None,
            metadata_info: None,
            add_paths,
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
    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [crate::expressions::ColumnName],
        &'static [crate::schema::DataType],
    ) {
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
        let expected_getters = 15;
        require!(
            getters.len() == expected_getters,
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
                self.scanner
                    .remove_dvs
                    .insert(path.to_string(), DvInfo { deletion_vector });
            } else if getters[7].get_str(i, "cdc.path")?.is_some() {
                self.scanner.has_cdc_action = true;
            } else if let Some(timestamp) = getters[8].get_long(i, "commitInfo.timestamp")? {
                self.scanner.timestamp = timestamp;
            } else if let Some(schema) = getters[9].get_str(i, "metaData.schemaString")? {
                let configuration_map_opt: Option<HashMap<_, _>> =
                    getters[10].get_opt(i, "metadata.configuration")?;

                // A null configuration here means that we found an empty configuration. This is
                // different from not finding any metadata action, where self.configuration = None
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

#[cfg(test)]
mod tests {

    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::scan::state::DvInfo;
    use crate::schema::{DataType, StructField, StructType};
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::{get_add_transform_expr, LogReplayScanner, TableChangesScanData};
    use crate::actions::{get_log_add_schema, Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::path::ParsedLogPath;
    use crate::scan::log_replay::SCAN_ROW_DATATYPE;
    use crate::{DeltaResult, Engine, Error, Version};
    use test_utils::delta_path_for_version;

    #[derive(Serialize)]
    enum Action {
        #[serde(rename = "add")]
        Add(Add),
        #[serde(rename = "remove")]
        Remove(Remove),
        #[serde(rename = "cdc")]
        Cdc(Cdc),
        #[serde(rename = "metaData")]
        Metadata(Metadata),
        #[serde(rename = "protocol")]
        Protocol(Protocol),
        #[serde(rename = "commitInfo")]
        CommitInfo(CommitInfo),
    }

    impl From<Add> for Action {
        fn from(value: Add) -> Self {
            Self::Add(value)
        }
    }
    impl From<Remove> for Action {
        fn from(value: Remove) -> Self {
            Self::Remove(value)
        }
    }
    impl From<Cdc> for Action {
        fn from(value: Cdc) -> Self {
            Self::Cdc(value)
        }
    }
    impl From<Metadata> for Action {
        fn from(value: Metadata) -> Self {
            Self::Metadata(value)
        }
    }
    impl From<Protocol> for Action {
        fn from(value: Protocol) -> Self {
            Self::Protocol(value)
        }
    }
    impl From<CommitInfo> for Action {
        fn from(value: CommitInfo) -> Self {
            Self::CommitInfo(value)
        }
    }

    struct MockTable {
        commit_num: u64,
        store: Arc<LocalFileSystem>,
        dir: TempDir,
    }

    impl MockTable {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            Self {
                commit_num: 0,
                store,
                dir,
            }
        }
        pub(crate) async fn commit(&mut self, actions: &[Action]) {
            let data = actions
                .iter()
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");

            let path = delta_path_for_version(self.commit_num, "json");
            self.commit_num += 1;
            // add log files to store

            self.store
                .put(&path, data.into())
                .await
                .expect("put log file in store");
        }
        pub(crate) fn table_root(&self) -> &Path {
            self.dir.path()
        }
    }
    fn get_schema() -> StructType {
        StructType::new([
            StructField::new("id", DataType::LONG, true),
            StructField::new("value", DataType::STRING, true),
        ])
    }

    fn get_segment(
        engine: &dyn Engine,
        path: &Path,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Vec<ParsedLogPath>> {
        let table_root = url::Url::from_directory_path(path).unwrap();
        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.get_file_system_client().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;
        Ok(log_segment.ascending_commit_files)
    }

    fn get_commit_log_scanner(engine: &dyn Engine, commit: ParsedLogPath) -> LogReplayScanner {
        let expression_evaluator = engine.get_expression_handler().get_evaluator(
            get_log_add_schema().clone(),
            get_add_transform_expr(),
            SCAN_ROW_DATATYPE.clone(),
        );
        LogReplayScanner::new(
            commit,
            engine.get_json_handler(),
            None,
            expression_evaluator.clone(),
            get_schema().into(),
        )
    }
    fn result_to_sv(iter: impl Iterator<Item = DeltaResult<TableChangesScanData>>) -> Vec<bool> {
        iter.map_ok(|scan_data| scan_data.selection_vector.into_iter())
            .flatten_ok()
            .try_collect()
            .unwrap()
    }

    #[tokio::test]
    async fn metadata_protocol() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        let schema_string = serde_json::to_string(&get_schema()).unwrap();
        mock_table
            .commit(&[
                Metadata {
                    schema_string,
                    configuration: HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                    ]),
                    ..Default::default()
                }
                .into(),
                Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"]))
                    .unwrap()
                    .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let mut scanner = get_commit_log_scanner(&engine, commits.next().unwrap());

        scanner.prepare_phase().unwrap();
        assert!(!scanner.has_cdc_action);
        assert!(scanner.remove_dvs.is_empty());

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[false, false]
        );
    }
    #[tokio::test]
    async fn configuration_fails() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        let schema_string = serde_json::to_string(&get_schema()).unwrap();
        mock_table
            .commit(&[Metadata {
                schema_string,
                configuration: HashMap::from([(
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                )]),
                ..Default::default()
            }
            .into()])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let mut scanner = get_commit_log_scanner(&engine, commits.next().unwrap());

        assert!(matches!(
            scanner.prepare_phase(),
            Err(Error::ChangeDataFeedUnsupported(_))
        ));
    }

    #[tokio::test]
    async fn incompatible_schema() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        // The original schema has two fields: `id` and value.
        let schema = get_schema().project(&["id"]).unwrap();
        let schema_string = serde_json::to_string(&schema).unwrap();
        mock_table
            .commit(&[Metadata {
                schema_string,
                configuration: HashMap::from([
                    ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                    (
                        "delta.enableDeletionVectors".to_string(),
                        "true".to_string(),
                    ),
                ]),
                ..Default::default()
            }
            .into()])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let mut scanner = get_commit_log_scanner(&engine, commits.next().unwrap());

        assert!(matches!(
            scanner.prepare_phase(),
            Err(Error::ChangeDataFeedIncompatibleSchema(_, _))
        ));
    }

    #[tokio::test]
    async fn add_remove() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.prepare_phase().unwrap();
        assert!(!scanner.has_cdc_action);
        assert_eq!(scanner.remove_dvs, HashMap::new());

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[true, true]
        );
    }

    #[tokio::test]
    async fn cdc_selection() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
                Cdc {
                    path: "fake_path_3".into(),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.prepare_phase().unwrap();
        assert!(scanner.has_cdc_action);
        assert_eq!(scanner.remove_dvs, HashMap::new());

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[false, false, true]
        );
    }

    #[tokio::test]
    async fn dv() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let deletion_vector1 = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let deletion_vector2 = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
            offset: Some(1),
            size_in_bytes: 38,
            cardinality: 3,
        };
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_1".into(),
                    deletion_vector: Some(deletion_vector1.clone()),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    deletion_vector: Some(deletion_vector2.clone()),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.prepare_phase().unwrap();
        assert!(!scanner.has_cdc_action);
        assert_eq!(
            scanner.remove_dvs,
            HashMap::from([(
                "fake_path_1".to_string(),
                DvInfo {
                    deletion_vector: Some(deletion_vector1.clone())
                }
            )])
        );

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[true, false, true]
        );
    }
    #[tokio::test]
    async fn failing_protocol() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let protocol = Protocol::try_new(
            3,
            1,
            ["fake_feature".to_string()].into(),
            ["fake_feature".to_string()].into(),
        )
        .unwrap();

        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
                protocol.into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        assert!(scanner.prepare_phase().is_err());
    }
    #[tokio::test]
    async fn in_commit_timestamp() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let timestamp = 123456;
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                CommitInfo {
                    timestamp: Some(timestamp),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.prepare_phase().unwrap();
        assert_eq!(scanner.timestamp, timestamp);
    }

    #[tokio::test]
    async fn file_meta_timestamp() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        mock_table
            .commit(&[Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into()])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let file_meta_ts = commit.location.last_modified;
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.prepare_phase().unwrap();
        assert_eq!(scanner.timestamp, file_meta_ts);
    }
}
