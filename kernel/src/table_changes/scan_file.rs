//! This module handles [`CdfScanFile`]s for [`TableChangesScan`]. A [`CdfScanFile`] consists of all
//! the metadata required to generate a change data feed. [`CdfScanFile`] can be constructed using
//! [`CdfScanFileVisitor`]. The visitor reads from engine data with the schema
//! [`cdf_scan_row_schema`]. You can convert engine data to this schema using the
//! [`cdf_scan_row_expression`].
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;
use itertools::Itertools;

use super::log_replay::TableChangesScanMetadata;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::visitors::visit_deletion_vector_at;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::scan::state::DvInfo;
use crate::schema::{
    ColumnName, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType,
};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};

// The type of action associated with a [`CdfScanFile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdfScanFileType {
    Add,
    Remove,
    Cdc,
}

impl CdfScanFileType {
    pub(crate) fn get_cdf_string_value(&self) -> &str {
        match self {
            CdfScanFileType::Add => super::ADD_CHANGE_TYPE,
            CdfScanFileType::Remove => super::REMOVE_CHANGE_TYPE,
            CdfScanFileType::Cdc => "not-expected",
        }
    }
}

/// Represents all the metadata needed to read a Change Data Feed.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CdfScanFile {
    /// The type of action this file belongs to. This may be one of add, remove, or cdc
    pub scan_type: CdfScanFileType,
    /// A `&str` which is the path to the file
    pub path: String,
    /// A [`DvInfo`] struct with the path to the action's deletion vector
    pub dv_info: DvInfo,
    /// An optional [`DvInfo`] struct. If present, this is deletion vector of a remove action with
    /// the same path as this [`CdfScanFile`]
    pub remove_dv: Option<DvInfo>,
    /// A `HashMap<String, String>` which are partition values
    pub partition_values: HashMap<String, String>,
    /// The commit version that this action was performed in
    pub commit_version: i64,
    /// The timestamp of the commit that this action was performed in
    pub commit_timestamp: i64,
    /// The size of the file in bytes
    pub size: Option<i64>,
    /// The `baseRowId` from the file action.
    pub base_row_id: Option<i64>,
    /// The `defaultRowCommitVersion` from the file action.
    pub default_row_commit_version: Option<i64>,
}

pub(crate) type CdfScanCallback<T> = fn(context: &mut T, scan_file: CdfScanFile);

/// Describes one data file and the metadata required to reconstruct row-level changes.
///
/// Its deletion vector is the physical vector recorded in the file action.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
#[internal_api]
pub(crate) struct TableChangesScanFile {
    /// Path to the file, relative to the table root.
    pub path: String,

    /// The file's physical deletion vector, if any.
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Partition values from the file action.
    ///
    /// The keys are physical column names when column mapping is enabled.
    pub partition_values: HashMap<String, String>,

    /// The size of the file in bytes, if known.
    pub size: Option<i64>,

    /// The commit version this action was performed in.
    pub commit_version: i64,

    /// The commit timestamp (milliseconds since the epoch). In-commit-timestamp aware when the
    /// table has in-commit timestamps enabled; otherwise the commit file's modification time.
    pub commit_timestamp: i64,

    /// The base row ID assigned to the file's first physical row.
    ///
    /// The effective row ID is
    /// `coalesce(materialized_row_id, base_row_id + physical_row_index)`, where the physical index
    /// is assigned before applying the deletion vector. The physical Parquet column is returned by
    /// `TableChanges::materialized_row_id_column_name`.
    pub base_row_id: Option<i64>,

    /// The first commit version containing an `add` action for this path.
    ///
    /// This value is not necessarily the last version that updated a row. A row's commit version
    /// is derived as `coalesce(materialized_row_commit_version, default_row_commit_version)`.
    /// The physical Parquet column is returned by
    /// `TableChanges::materialized_row_commit_version_column_name`.
    pub default_row_commit_version: Option<i64>,
}

/// Describes the add and remove file images used to reconstruct row-level changes.
///
/// The remove side is the pre-image and the add side is the post-image. Matching rows by stable row
/// ID and comparing row commit versions distinguishes updates from rows carried forward by a
/// copy-on-write rewrite. Rows present only on the remove side are deletes; rows present only on
/// the add side are inserts.
///
/// At least one side is present. Copy-on-write actions with different paths remain separate and
/// require reconciliation across the full listing. `AddCDCFile` actions are ignored.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
#[internal_api]
pub(crate) struct TableChangesFileAction {
    /// The add side, present for an insert or the post-image of an update.
    pub add: Option<TableChangesScanFile>,
    /// The remove side, present for a delete or the pre-image of an update.
    pub remove: Option<TableChangesScanFile>,
}

impl TableChangesFileAction {
    /// Converts a scan file, preserving both sides of a same-commit deletion-vector update.
    pub(crate) fn try_from_scan_file(scan_file: CdfScanFile) -> DeltaResult<Self> {
        match scan_file.scan_type {
            CdfScanFileType::Add => {
                require!(
                    scan_file.base_row_id.is_some(),
                    Error::missing_data(format!(
                        "baseRowId for row-tracking add action at path {} in version {}",
                        scan_file.path, scan_file.commit_version
                    ))
                );
                require!(
                    scan_file.default_row_commit_version.is_some(),
                    Error::missing_data(format!(
                        "defaultRowCommitVersion for row-tracking add action at path {} in \
                         version {}",
                        scan_file.path, scan_file.commit_version
                    ))
                );
                let remove = scan_file.remove_dv.as_ref().map(|dv| {
                    TableChangesScanFile::from_scan_file(&scan_file, dv.deletion_vector.clone())
                });
                Ok(TableChangesFileAction {
                    add: Some(TableChangesScanFile::from_scan_file(
                        &scan_file,
                        scan_file.dv_info.deletion_vector.clone(),
                    )),
                    remove,
                })
            }
            CdfScanFileType::Remove => Ok(TableChangesFileAction {
                add: None,
                remove: Some(TableChangesScanFile::from_scan_file(
                    &scan_file,
                    scan_file.dv_info.deletion_vector.clone(),
                )),
            }),
            CdfScanFileType::Cdc => Err(Error::internal_error(format!(
                "Row-tracking change feed listing unexpectedly produced a cdc scan file: \
                 path={}, version={}",
                scan_file.path, scan_file.commit_version
            ))),
        }
    }
}

impl TableChangesScanFile {
    fn from_scan_file(
        scan_file: &CdfScanFile,
        deletion_vector: Option<DeletionVectorDescriptor>,
    ) -> Self {
        TableChangesScanFile {
            path: scan_file.path.clone(),
            deletion_vector,
            partition_values: scan_file.partition_values.clone(),
            size: scan_file.size,
            commit_version: scan_file.commit_version,
            commit_timestamp: scan_file.commit_timestamp,
            base_row_id: scan_file.base_row_id,
            default_row_commit_version: scan_file.default_row_commit_version,
        }
    }
}

/// Transforms an iterator of [`TableChangesScanMetadata`] into an iterator of
/// [`CdfScanFile`] by visiting the engine data.
pub(crate) fn scan_metadata_to_scan_file(
    scan_metadata: impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>,
) -> impl Iterator<Item = DeltaResult<CdfScanFile>> {
    scan_metadata
        .map(|scan_metadata| -> DeltaResult<_> {
            let scan_metadata = scan_metadata?;
            let callback: CdfScanCallback<Vec<CdfScanFile>> =
                |context, scan_file| context.push(scan_file);
            Ok(visit_cdf_scan_files(&scan_metadata, vec![], callback)?.into_iter())
        }) // Iterator-Result-Iterator
        .flatten_ok() // Iterator-Result
}

/// Request that the kernel call a callback on each valid file that needs to be read for the
/// scan.
///
/// The arguments to the callback are:
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through
///   to each call
/// * `CdfScanFile`: a [`CdfScanFile`] struct that holds all the metadata required to perform Change
///   Data Feed
///
/// ## Context
/// A note on the `context`. This can be any value the engine wants. This function takes ownership
/// of the passed arg, but then returns it, so the engine can repeatedly call `visit_cdf_scan_files`
/// with the same context.
///
/// ## Example
/// ```ignore
/// let mut context = [my context];
/// for res in scan_metadata { // scan metadata table_changes_scan.scan_metadata()
///     let (data, vector, remove_dv) = res?;
///     context = delta_kernel::table_changes::scan_file::visit_cdf_scan_files(
///        data.as_ref(),
///        selection_vector,
///        context,
///        my_callback,
///     )?;
/// }
/// ```
pub(crate) fn visit_cdf_scan_files<T>(
    scan_metadata: &TableChangesScanMetadata,
    context: T,
    callback: CdfScanCallback<T>,
) -> DeltaResult<T> {
    let mut visitor = CdfScanFileVisitor {
        callback,
        context,
        selection_vector: &scan_metadata.selection_vector,
        remove_dvs: scan_metadata.remove_dvs.as_ref(),
    };

    visitor.visit_rows_of(scan_metadata.scan_metadata.as_ref())?;
    Ok(visitor.context)
}

/// A visitor that extracts [`CdfScanFile`]s from engine data. Expects data to have the schema
/// [`cdf_scan_row_schema`].
struct CdfScanFileVisitor<'a, T> {
    callback: CdfScanCallback<T>,
    selection_vector: &'a [bool],
    remove_dvs: &'a HashMap<String, DvInfo>,
    context: T,
}

struct FileSideSpec {
    scan_type: CdfScanFileType,
    start_index: usize,
    path_field: &'static str,
    partition_values_field: &'static str,
    size_field: &'static str,
    base_row_id_field: &'static str,
    default_row_commit_version_field: &'static str,
}

const ADD_FILE_SIDE: FileSideSpec = FileSideSpec {
    scan_type: CdfScanFileType::Add,
    start_index: 0,
    path_field: "scanFile.add.path",
    partition_values_field: "scanFile.add.fileConstantValues.partitionValues",
    size_field: "scanFile.add.size",
    base_row_id_field: "scanFile.add.baseRowId",
    default_row_commit_version_field: "scanFile.add.defaultRowCommitVersion",
};
const REMOVE_FILE_SIDE: FileSideSpec = FileSideSpec {
    scan_type: CdfScanFileType::Remove,
    start_index: 10,
    path_field: "scanFile.remove.path",
    partition_values_field: "scanFile.remove.fileConstantValues.partitionValues",
    size_field: "scanFile.remove.size",
    base_row_id_field: "scanFile.remove.baseRowId",
    default_row_commit_version_field: "scanFile.remove.defaultRowCommitVersion",
};
const CDC_PATH_INDEX: usize = 20;
const CDC_PARTITION_VALUES_INDEX: usize = 21;
const CDC_SIZE_INDEX: usize = 22;
const COMMIT_TIMESTAMP_INDEX: usize = 23;
const COMMIT_VERSION_INDEX: usize = 24;
const CDF_SCAN_FILE_GETTER_COUNT: usize = 25;

struct FileSide {
    scan_type: CdfScanFileType,
    path: String,
    deletion_vector: Option<DeletionVectorDescriptor>,
    partition_values: Option<HashMap<String, String>>,
    size: Option<i64>,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
}

fn read_file_side<'a>(
    row_index: usize,
    getters: &[&'a dyn GetData<'a>],
    spec: &FileSideSpec,
) -> DeltaResult<Option<FileSide>> {
    let Some(path) = getters[spec.start_index].get_opt(row_index, spec.path_field)? else {
        return Ok(None);
    };
    let deletion_vector = visit_deletion_vector_at(
        row_index,
        &getters[spec.start_index + 1..=spec.start_index + 5],
    )?;
    let partition_values =
        getters[spec.start_index + 6].get_opt(row_index, spec.partition_values_field)?;
    let size = getters[spec.start_index + 7].get_opt(row_index, spec.size_field)?;
    let base_row_id = getters[spec.start_index + 8].get_opt(row_index, spec.base_row_id_field)?;
    let default_row_commit_version =
        getters[spec.start_index + 9].get_opt(row_index, spec.default_row_commit_version_field)?;
    Ok(Some(FileSide {
        scan_type: spec.scan_type,
        path,
        deletion_vector,
        partition_values,
        size,
        base_row_id,
        default_row_commit_version,
    }))
}

impl<T> RowVisitor for CdfScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == CDF_SCAN_FILE_GETTER_COUNT,
            Error::InternalError(format!(
                "Wrong number of CdfScanFileVisitor getters: {}",
                getters.len()
            ))
        );
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                continue;
            }

            let file_side = if let Some(side) = read_file_side(row_index, getters, &ADD_FILE_SIDE)?
            {
                side
            } else if let Some(side) = read_file_side(row_index, getters, &REMOVE_FILE_SIDE)? {
                side
            } else if let Some(path) =
                getters[CDC_PATH_INDEX].get_opt(row_index, "scanFile.cdc.path")?
            {
                let partition_values = getters[CDC_PARTITION_VALUES_INDEX]
                    .get_opt(row_index, "scanFile.cdc.fileConstantValues.partitionValues")?;
                let size = getters[CDC_SIZE_INDEX].get_opt(row_index, "scanFile.cdc.size")?;
                FileSide {
                    scan_type: CdfScanFileType::Cdc,
                    path,
                    deletion_vector: None,
                    partition_values,
                    size,
                    base_row_id: None,
                    default_row_commit_version: None,
                }
            } else {
                continue;
            };
            let scan_file = CdfScanFile {
                remove_dv: self.remove_dvs.get(&file_side.path).cloned(),
                scan_type: file_side.scan_type,
                path: file_side.path,
                dv_info: DvInfo {
                    deletion_vector: file_side.deletion_vector,
                },
                partition_values: file_side.partition_values.unwrap_or_default(),
                commit_timestamp: getters[COMMIT_TIMESTAMP_INDEX]
                    .get(row_index, "scanFile.timestamp")?,
                commit_version: getters[COMMIT_VERSION_INDEX]
                    .get(row_index, "scanFile.commit_version")?,
                size: file_side.size,
                base_row_id: file_side.base_row_id,
                default_row_commit_version: file_side.default_row_commit_version,
            };
            (self.callback)(&mut self.context, scan_file)
        }
        Ok(())
    }

    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| cdf_scan_row_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }
}

/// Get the schema that scan rows (from [`TableChanges::scan_metadata`]) will be returned with.
pub(crate) fn cdf_scan_row_schema() -> SchemaRef {
    static CDF_SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
        let deletion_vector = StructType::new_unchecked([
            StructField::nullable("storageType", DataType::STRING),
            StructField::nullable("pathOrInlineDv", DataType::STRING),
            StructField::nullable("offset", DataType::INTEGER),
            StructField::nullable("sizeInBytes", DataType::INTEGER),
            StructField::nullable("cardinality", DataType::LONG),
        ]);
        let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
        let file_constant_values =
            StructType::new_unchecked([StructField::nullable("partitionValues", partition_values)]);

        let add = StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("deletionVector", deletion_vector.clone()),
            StructField::nullable("fileConstantValues", file_constant_values.clone()),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("baseRowId", DataType::LONG),
            StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        ]);
        let remove = StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("deletionVector", deletion_vector),
            StructField::nullable("fileConstantValues", file_constant_values.clone()),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("baseRowId", DataType::LONG),
            StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        ]);
        let cdc = StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("fileConstantValues", file_constant_values),
            StructField::nullable("size", DataType::LONG),
        ]);

        Arc::new(StructType::new_unchecked([
            StructField::nullable("add", add),
            StructField::nullable("remove", remove),
            StructField::nullable("cdc", cdc),
            StructField::not_null("timestamp", DataType::LONG),
            StructField::not_null("commit_version", DataType::LONG),
        ]))
    });
    CDF_SCAN_ROW_SCHEMA.clone()
}

/// Expression to convert an action with `commit_schema` into one with
/// [`cdf_scan_row_schema`]. This is the expression used to create [`TableChangesScanMetadata`].
pub(crate) fn cdf_scan_row_expression(commit_timestamp: i64, commit_number: i64) -> Expression {
    Expression::struct_from([
        Expression::struct_from([
            column_expr!("add.path"),
            column_expr!("add.deletionVector"),
            Expression::struct_from([column_expr!("add.partitionValues")]),
            column_expr!("add.size"),
            column_expr!("add.baseRowId"),
            column_expr!("add.defaultRowCommitVersion"),
        ]),
        Expression::struct_from([
            column_expr!("remove.path"),
            column_expr!("remove.deletionVector"),
            Expression::struct_from([column_expr!("remove.partitionValues")]),
            column_expr!("remove.size"),
            column_expr!("remove.baseRowId"),
            column_expr!("remove.defaultRowCommitVersion"),
        ]),
        Expression::struct_from([
            column_expr!("cdc.path"),
            Expression::struct_from([column_expr!("cdc.partitionValues")]),
            column_expr!("cdc.size"),
        ]),
        Expression::literal(commit_timestamp),
        Expression::literal(commit_number),
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use itertools::Itertools;
    use rstest::rstest;

    use super::{scan_metadata_to_scan_file, CdfScanFile, CdfScanFileType, TableChangesFileAction};
    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use crate::actions::{Add, Cdc, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::scan::state::DvInfo;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::log_replay::{
        table_changes_action_iter, table_changes_action_iter_with_mode,
    };
    use crate::table_changes::test_utils::{row_tracking_table_config, test_deletion_vector};
    use crate::table_changes::CdfMode;
    use crate::table_configuration::TableConfiguration;
    use crate::table_properties::{COLUMN_MAPPING_MODE, ENABLE_CHANGE_DATA_FEED};
    use crate::utils::test_utils::{assert_result_error_with_message, Action, LocalMockTable};
    use crate::Engine as _;

    fn row_tracking_add(
        path: &str,
        deletion_vector: Option<DeletionVectorDescriptor>,
        data_change: bool,
        base_row_id: i64,
        default_row_commit_version: i64,
    ) -> Add {
        Add {
            path: path.into(),
            deletion_vector,
            partition_values: HashMap::new(),
            data_change,
            size: 100,
            base_row_id: Some(base_row_id),
            default_row_commit_version: Some(default_row_commit_version),
            ..Default::default()
        }
    }

    fn row_tracking_remove(
        path: &str,
        deletion_vector: Option<DeletionVectorDescriptor>,
        base_row_id: Option<i64>,
        default_row_commit_version: Option<i64>,
    ) -> Remove {
        Remove {
            path: path.into(),
            deletion_vector,
            data_change: true,
            base_row_id,
            default_row_commit_version,
            ..Default::default()
        }
    }

    fn row_tracking_scan_files(
        engine: Arc<SyncEngine>,
        mock_table: &LocalMockTable,
    ) -> Vec<CdfScanFile> {
        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let log_segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            table_root.join("_delta_log/").unwrap(),
            0,
            None,
        )
        .unwrap();
        let table_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]));
        let table_config = row_tracking_table_config(table_root, table_schema.clone());
        let scan_metadata = table_changes_action_iter_with_mode(
            engine,
            &table_config,
            log_segment.listed.ascending_commit_files,
            table_schema,
            None,
            CdfMode::RowTracking,
        )
        .unwrap();
        scan_metadata_to_scan_file(scan_metadata)
            .try_collect()
            .unwrap()
    }

    #[tokio::test]
    async fn test_scan_file_visiting() {
        let engine = SyncEngine::new();
        let mut mock_table = LocalMockTable::new();

        let dv_info = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let add_partition_values = HashMap::from([("a".to_string(), "b".to_string())]);
        let add_paired = Add {
            path: "fake_path_1".into(),
            deletion_vector: Some(dv_info.clone()),
            partition_values: add_partition_values,
            data_change: true,
            size: 100i64,
            ..Default::default()
        };
        let paired_remove = Remove {
            path: "fake_path_1".into(),
            deletion_vector: None,
            partition_values: None,
            data_change: true,
            size: Some(200i64),
            ..Default::default()
        };

        let rm_dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
            offset: Some(1),
            size_in_bytes: 38,
            cardinality: 3,
        };
        let rm_partition_values = Some(HashMap::from([("c".to_string(), "d".to_string())]));
        let remove = Remove {
            path: "fake_path_2".into(),
            deletion_vector: Some(rm_dv),
            partition_values: rm_partition_values,
            data_change: true,
            size: None,
            ..Default::default()
        };

        let cdc_partition_values = HashMap::from([("x".to_string(), "y".to_string())]);
        let cdc = Cdc {
            path: "fake_path_3".into(),
            partition_values: cdc_partition_values,
            ..Default::default()
        };

        let remove_no_partition = Remove {
            path: "fake_path_2".into(),
            deletion_vector: None,
            partition_values: None,
            data_change: true,
            size: None,
            ..Default::default()
        };

        mock_table
            .commit([
                Action::Remove(paired_remove.clone()),
                Action::Add(add_paired.clone()),
                Action::Remove(remove.clone()),
            ])
            .await;
        mock_table.commit([Action::Cdc(cdc.clone())]).await;
        mock_table
            .commit([Action::Remove(remove_no_partition.clone())])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let log_root = table_root.join("_delta_log/").unwrap();
        let log_segment =
            LogSegment::for_table_changes(engine.storage_handler().as_ref(), log_root, 0, None)
                .unwrap();
        let table_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]));

        // Create a TableConfiguration for testing
        let metadata = Metadata::try_new(
            None,
            None,
            table_schema.clone(),
            vec![],
            0,
            HashMap::from([
                (ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string()),
                (COLUMN_MAPPING_MODE.to_string(), "none".to_string()),
            ]),
        )
        .unwrap();
        // CDF (enableChangeDataFeed) requires min_writer_version = 4
        let protocol = Protocol::try_new_legacy(1, 4).unwrap();
        let table_config =
            TableConfiguration::try_new(metadata, protocol, table_root.clone(), 0).unwrap();

        let scan_metadata = table_changes_action_iter(
            Arc::new(engine),
            &table_config,
            log_segment.listed.ascending_commit_files.clone(),
            table_schema,
            None,
        )
        .unwrap();
        let scan_files: Vec<_> = scan_metadata_to_scan_file(scan_metadata)
            .try_collect()
            .unwrap();

        // Generate the expected [`CdfScanFile`]
        let timestamps = log_segment
            .listed
            .ascending_commit_files
            .iter()
            .map(|commit| commit.location.last_modified)
            .collect_vec();
        let expected_remove_dv = DvInfo {
            deletion_vector: None,
        };
        let expected_scan_files = vec![
            CdfScanFile {
                scan_type: CdfScanFileType::Add,
                path: add_paired.path,
                dv_info: DvInfo {
                    deletion_vector: add_paired.deletion_vector,
                },
                partition_values: add_paired.partition_values,
                commit_version: 0,
                commit_timestamp: timestamps[0],
                remove_dv: Some(expected_remove_dv),
                size: Some(add_paired.size),
                base_row_id: None,
                default_row_commit_version: None,
            },
            CdfScanFile {
                scan_type: CdfScanFileType::Remove,
                path: remove.path,
                dv_info: DvInfo {
                    deletion_vector: remove.deletion_vector,
                },
                partition_values: remove.partition_values.unwrap(),
                commit_version: 0,
                commit_timestamp: timestamps[0],
                remove_dv: None,
                size: remove.size,
                base_row_id: None,
                default_row_commit_version: None,
            },
            CdfScanFile {
                scan_type: CdfScanFileType::Cdc,
                path: cdc.path,
                dv_info: DvInfo {
                    deletion_vector: None,
                },
                partition_values: cdc.partition_values,
                commit_version: 1,
                commit_timestamp: timestamps[1],
                remove_dv: None,
                size: Some(cdc.size),
                base_row_id: None,
                default_row_commit_version: None,
            },
            CdfScanFile {
                scan_type: CdfScanFileType::Remove,
                path: remove_no_partition.path,
                dv_info: DvInfo {
                    deletion_vector: None,
                },
                partition_values: HashMap::new(),
                commit_version: 2,
                commit_timestamp: timestamps[2],
                remove_dv: None,
                size: remove_no_partition.size,
                base_row_id: None,
                default_row_commit_version: None,
            },
        ];

        assert_eq!(scan_files, expected_scan_files);
    }

    #[tokio::test]
    async fn test_row_tracking_listing_ignores_cdc_and_surfaces_row_tracking_fields() {
        let engine = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();

        let post_dv = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
        let baseline_dv = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 1);
        let add_paired = row_tracking_add("path_1", Some(post_dv.clone()), true, 10, 0);
        let paired_remove =
            row_tracking_remove("path_1", Some(baseline_dv.clone()), Some(10), Some(0));
        let remove_unpaired = row_tracking_remove("path_2", None, None, None);
        let cdc = Cdc {
            path: "cdc_path".into(),
            partition_values: HashMap::new(),
            ..Default::default()
        };
        let add_insert = row_tracking_add("path_4", None, true, 20, 2);
        let add_no_data_change = row_tracking_add("path_6", None, false, 40, 3);

        mock_table
            .commit([
                Action::Remove(paired_remove),
                Action::Add(add_paired),
                Action::Remove(remove_unpaired),
            ])
            .await;
        mock_table.commit([Action::Cdc(cdc)]).await;
        mock_table.commit([Action::Add(add_insert)]).await;
        mock_table.commit([Action::Add(add_no_data_change)]).await;

        let scan_files = row_tracking_scan_files(engine, &mock_table);

        // The cdc action in commit 1 is ignored: only add/remove-derived files appear. The
        // metadata-only add (`data_change == false`) in commit 3 is also excluded.
        assert!(scan_files
            .iter()
            .all(|f| f.scan_type != CdfScanFileType::Cdc));
        assert!(
            !scan_files.iter().any(|f| f.path == "path_6"),
            "metadata-only add (data_change=false) must be excluded"
        );
        assert_eq!(scan_files.len(), 3);

        // The paired add (a DV update) carries its own (post-image) DV, the paired remove's
        // (baseline) DV, and the row-tracking fields.
        let paired = scan_files
            .iter()
            .find(|f| f.path == "path_1")
            .expect("paired add present");
        assert_eq!(paired.scan_type, CdfScanFileType::Add);
        assert_eq!(paired.base_row_id, Some(10));
        assert_eq!(paired.default_row_commit_version, Some(0));
        assert_eq!(paired.dv_info.deletion_vector, Some(post_dv));
        assert_eq!(
            paired
                .remove_dv
                .as_ref()
                .and_then(|dv| dv.deletion_vector.clone()),
            Some(baseline_dv)
        );

        // The unpaired remove is a delete and has no paired remove DV.
        let unpaired = scan_files
            .iter()
            .find(|f| f.path == "path_2")
            .expect("unpaired remove present");
        assert_eq!(unpaired.scan_type, CdfScanFileType::Remove);
        assert_eq!(unpaired.remove_dv, None);
        assert_eq!(unpaired.base_row_id, None);
        assert_eq!(unpaired.default_row_commit_version, None);

        // The standalone add is an insert and surfaces its base row id.
        let insert = scan_files
            .iter()
            .find(|f| f.path == "path_4")
            .expect("insert add present");
        assert_eq!(insert.scan_type, CdfScanFileType::Add);
        assert_eq!(insert.base_row_id, Some(20));

        // Converting to the public grouped type never errors (no cdc) and preserves the metadata.
        // Group by path via the add/remove side that carries it.
        let listing: Vec<TableChangesFileAction> = scan_files
            .into_iter()
            .map(TableChangesFileAction::try_from_scan_file)
            .try_collect()
            .unwrap();
        let path_of = |fa: &TableChangesFileAction| {
            fa.add
                .as_ref()
                .or(fa.remove.as_ref())
                .map(|s| s.path.clone())
        };

        // The paired DV update (path_1) groups into both sides for the one file: the add side
        // carries the post-image DV, the remove side the baseline DV, each with the row-tracking
        // fields.
        let paired = listing
            .iter()
            .find(|fa| path_of(fa).as_deref() == Some("path_1"))
            .expect("paired listing present");
        let paired_add = paired.add.as_ref().expect("paired add side");
        let paired_remove = paired.remove.as_ref().expect("paired remove side");
        assert_eq!(paired_add.base_row_id, Some(10));
        assert!(paired_add.deletion_vector.is_some());
        assert!(paired_remove.deletion_vector.is_some());
        assert_eq!(paired_remove.base_row_id, Some(10));

        // The unpaired remove (path_2) is a single-sided delete.
        let unpaired_delete = listing
            .iter()
            .find(|fa| path_of(fa).as_deref() == Some("path_2"))
            .expect("unpaired delete present");
        assert!(unpaired_delete.add.is_none());
        assert!(unpaired_delete.remove.is_some());

        // The standalone add (path_4) is a single-sided insert.
        let insert = listing
            .iter()
            .find(|fa| path_of(fa).as_deref() == Some("path_4"))
            .expect("insert present");
        assert!(insert.remove.is_none());
        assert_eq!(
            insert.add.as_ref().expect("insert add side").base_row_id,
            Some(20)
        );
    }

    #[rstest]
    #[case::with_deletion_vector(Some(test_deletion_vector("baseline", 1)))]
    #[case::without_deletion_vector(None)]
    fn cdf_file_action_preserves_paired_remove_deletion_vector(
        #[case] deletion_vector: Option<DeletionVectorDescriptor>,
    ) {
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: "path".into(),
            dv_info: DvInfo {
                deletion_vector: Some(test_deletion_vector("current", 2)),
            },
            remove_dv: Some(DvInfo {
                deletion_vector: deletion_vector.clone(),
            }),
            partition_values: HashMap::new(),
            commit_version: 1,
            commit_timestamp: 2,
            size: Some(3),
            base_row_id: Some(4),
            default_row_commit_version: Some(5),
        };

        let action = TableChangesFileAction::try_from_scan_file(scan_file).unwrap();
        assert_eq!(action.remove.unwrap().deletion_vector, deletion_vector);
        assert!(action.add.is_some());
    }

    #[rstest]
    #[case::missing_base_row_id(None, Some(5), "baseRowId")]
    #[case::missing_default_row_commit_version(Some(4), None, "defaultRowCommitVersion")]
    fn cdf_file_action_rejects_add_missing_row_tracking_fields(
        #[case] base_row_id: Option<i64>,
        #[case] default_row_commit_version: Option<i64>,
        #[case] expected: &str,
    ) {
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: "path".into(),
            dv_info: DvInfo {
                deletion_vector: None,
            },
            remove_dv: None,
            partition_values: HashMap::new(),
            commit_version: 1,
            commit_timestamp: 2,
            size: Some(3),
            base_row_id,
            default_row_commit_version,
        };

        assert_result_error_with_message(
            TableChangesFileAction::try_from_scan_file(scan_file),
            expected,
        );
    }

    #[test]
    fn cdf_file_action_rejects_cdc_scan_file() {
        // The row-tracking listing path never selects cdc files, so converting one signals an
        // internal inconsistency and must error rather than silently producing a bad listing.
        let cdc_scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Cdc,
            path: "cdc".into(),
            dv_info: DvInfo {
                deletion_vector: None,
            },
            remove_dv: None,
            partition_values: HashMap::new(),
            commit_version: 0,
            commit_timestamp: 0,
            size: None,
            base_row_id: None,
            default_row_commit_version: None,
        };
        assert!(TableChangesFileAction::try_from_scan_file(cdc_scan_file).is_err());
    }
}
