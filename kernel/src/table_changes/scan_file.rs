//! This module handles [`CdfScanFile`]s for [`TableChangesScan`]. A [`CdfScanFile`] consists of all
//! the metadata required to generate a change data feed. [`CdfScanFile`] can be constructed using
//! [`CdfScanFileVisitor`]. The visitor reads from engine data with the schema
//! [`cdf_scan_row_schema`]. You can convert engine data to this schema using the
//! [`cdf_scan_row_expression`].
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

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
#[derive(Debug, Clone, PartialEq)]
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
    /// The base row ID of the file's first row, from the action's `baseRowId` field. See
    /// [`TableChangesScanFile::base_row_id`] for the full row-id contract (effective row id,
    /// materialized override column). `None` for files with no base row ID (e.g. predating row
    /// tracking).
    pub base_row_id: Option<i64>,
    /// The default row commit version of the file, from the action's `defaultRowCommitVersion`
    /// field. See [`TableChangesScanFile::default_row_commit_version`]. Only populated for tables
    /// with row tracking enabled.
    pub default_row_commit_version: Option<i64>,
}

pub(crate) type CdfScanCallback<T> = fn(context: &mut T, scan_file: CdfScanFile);

/// One side of a [`TableChangesFileAction`]: a single file that must be read, together with the
/// row-tracking and deletion-vector metadata needed to reconstruct change events, but without any
/// data read.
///
/// `deletion_vector` is always the file's *physical* on-disk deletion vector, exactly as recorded
/// in the underlying `add`/`remove` action -- the same meaning whether this is the `add` or the
/// `remove` side. The kernel never rewrites it into a computed change-set.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TableChangesScanFile {
    /// Path to the file, relative to the table root.
    pub path: String,

    /// The file's physical deletion vector, if any.
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Partition values for the file.
    pub partition_values: HashMap<String, String>,

    /// The size of the file in bytes, if known.
    pub size: Option<i64>,

    /// The commit version this action was performed in.
    pub commit_version: i64,

    /// The commit timestamp (milliseconds since the epoch). In-commit-timestamp aware when the
    /// table has in-commit timestamps enabled; otherwise the commit file's modification time.
    pub commit_timestamp: i64,

    /// The base row ID of the file's first row. A row's *default* row ID is `base_row_id +
    /// physical_row_index` (its position in the file before applying the deletion vector). This
    /// default can be overridden per-row: a file may carry a materialized row-id column whose
    /// non-null values take precedence, so the effective row ID is `coalesce(materialized_row_id,
    /// base_row_id + physical_row_index)`. To match pre-image and post-image rows you must read
    /// that column rather than rely on `base_row_id` alone. The column's name is not carried
    /// on this listing entry; resolve it from the table's
    /// `delta.rowTracking.materializedRowIdColumnName` property, read from the
    /// [`crate::table_properties::TableProperties`] of a [`crate::Snapshot`] built at the feed's
    /// end version. `None` for files with no base row ID (e.g. predating row tracking).
    pub base_row_id: Option<i64>,

    /// The default row commit version of the file. Analogous to `base_row_id`: the effective
    /// per-row commit version is `coalesce(materialized_row_commit_version,
    /// default_row_commit_version)`, where the materialized column is named by the
    /// `delta.rowTracking.materializedRowCommitVersionColumnName` table property.
    pub default_row_commit_version: Option<i64>,
}

/// A file's change over the listed range, grouped into its `add` and `remove` sides. This is the
/// public, listing-only output of [`TableChanges::scan_file_listing`]: it identifies *which* files
/// to read and carries their row-tracking and deletion-vector metadata, but reads no data. The
/// connector performs the data scan and the row-id reconciliation itself.
///
/// The change type is encoded by which sides are present -- there is no separate kind or flag:
/// - `{ add: Some, remove: None }` -- an insert.
/// - `{ add: None, remove: Some }` -- a delete.
/// - `{ add: Some, remove: Some }` -- an update of the same file: read the pre-image (the `remove`
///   side under its DV) and the post-image (the `add` side under its DV) and reconcile rows by row
///   id (see [`TableChangesScanFile::base_row_id`]) -- a row present only post-image is an insert,
///   only pre-image a delete, in both unchanged.
///
/// At least one side is always present. A copy-on-write update that rewrites one file into a file
/// at a *different* path is not grouped here (the two sides have different paths); it surfaces as a
/// delete of the old path and an insert of the new, reconciled by row id across the whole listing.
///
/// Unlike the (`enableChangeDataFeed`) reader, this never references `_change_data` (cdc) files:
/// change events are reconstructed from `add`/`remove` actions and their row-tracking fields.
///
/// [`TableChanges::scan_file_listing`]: crate::table_changes::TableChanges::scan_file_listing
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TableChangesFileAction {
    /// The add side, present for an insert or the post-image of an update.
    pub add: Option<TableChangesScanFile>,
    /// The remove side, present for a delete or the pre-image of an update.
    pub remove: Option<TableChangesScanFile>,
}

impl TableChangesFileAction {
    /// Converts an internal [`CdfScanFile`] into a public [`TableChangesFileAction`]. A same-commit
    /// deletion-vector update (an `add` whose `remove_dv` is set) is grouped into both sides for
    /// the one file; an unpaired add or remove yields a single-sided action. Returns an error
    /// for `cdc`-typed scan files: the row-tracking listing path never selects `_change_data`
    /// files, so encountering one indicates an internal inconsistency.
    pub(crate) fn try_from_scan_file(scan_file: CdfScanFile) -> DeltaResult<Self> {
        // The remove side of a same-commit paired update is the same physical file in the same
        // commit as the add, differing only in its deletion vector.
        let paired_remove = |scan_file: &CdfScanFile| TableChangesScanFile {
            path: scan_file.path.clone(),
            deletion_vector: scan_file
                .remove_dv
                .as_ref()
                .and_then(|dv| dv.deletion_vector.clone()),
            partition_values: scan_file.partition_values.clone(),
            size: scan_file.size,
            commit_version: scan_file.commit_version,
            commit_timestamp: scan_file.commit_timestamp,
            base_row_id: scan_file.base_row_id,
            default_row_commit_version: scan_file.default_row_commit_version,
        };
        match scan_file.scan_type {
            CdfScanFileType::Add => {
                let remove = scan_file
                    .remove_dv
                    .is_some()
                    .then(|| paired_remove(&scan_file));
                Ok(TableChangesFileAction {
                    add: Some(TableChangesScanFile::from_scan_file(scan_file)),
                    remove,
                })
            }
            CdfScanFileType::Remove => Ok(TableChangesFileAction {
                add: None,
                remove: Some(TableChangesScanFile::from_scan_file(scan_file)),
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
    /// Builds a [`TableChangesScanFile`] from a scan file, taking the scan file's own deletion
    /// vector (`dv_info`). Used for a plain add side or an unpaired remove side; the paired
    /// remove side of a same-commit update is built separately since its DV comes from
    /// `remove_dv`.
    fn from_scan_file(scan_file: CdfScanFile) -> Self {
        TableChangesScanFile {
            path: scan_file.path,
            deletion_vector: scan_file.dv_info.deletion_vector,
            partition_values: scan_file.partition_values,
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

impl<T> RowVisitor for CdfScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 25,
            Error::InternalError(format!(
                "Wrong number of CdfScanFileVisitor getters: {}",
                getters.len()
            ))
        );
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                continue;
            }

            let (
                scan_type,
                path,
                deletion_vector,
                partition_values,
                size,
                base_row_id,
                default_row_commit_version,
            ) = if let Some(path) = getters[0].get_opt(row_index, "scanFile.add.path")? {
                let scan_type = CdfScanFileType::Add;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[1..=5])?;
                let partition_values = getters[6]
                    .get_opt(row_index, "scanFile.add.fileConstantValues.partitionValues")?;
                let size = getters[7].get_opt(row_index, "scanFile.add.size")?;
                let base_row_id = getters[8].get_opt(row_index, "scanFile.add.baseRowId")?;
                let default_row_commit_version =
                    getters[9].get_opt(row_index, "scanFile.add.defaultRowCommitVersion")?;
                (
                    scan_type,
                    path,
                    deletion_vector,
                    partition_values,
                    size,
                    base_row_id,
                    default_row_commit_version,
                )
            } else if let Some(path) = getters[10].get_opt(row_index, "scanFile.remove.path")? {
                let scan_type = CdfScanFileType::Remove;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[11..=15])?;
                let partition_values = getters[16].get_opt(
                    row_index,
                    "scanFile.remove.fileConstantValues.partitionValues",
                )?;
                let size = getters[17].get_opt(row_index, "scanFile.remove.size")?;
                let base_row_id = getters[18].get_opt(row_index, "scanFile.remove.baseRowId")?;
                let default_row_commit_version =
                    getters[19].get_opt(row_index, "scanFile.remove.defaultRowCommitVersion")?;
                (
                    scan_type,
                    path,
                    deletion_vector,
                    partition_values,
                    size,
                    base_row_id,
                    default_row_commit_version,
                )
            } else if let Some(path) = getters[20].get_opt(row_index, "scanFile.cdc.path")? {
                let scan_type = CdfScanFileType::Cdc;
                let partition_values = getters[21]
                    .get_opt(row_index, "scanFile.cdc.fileConstantValues.partitionValues")?;
                let size = getters[22].get_opt(row_index, "scanFile.cdc.size")?;
                (scan_type, path, None, partition_values, size, None, None)
            } else {
                continue;
            };
            let partition_values = partition_values.unwrap_or_else(Default::default);
            let scan_file = CdfScanFile {
                remove_dv: self.remove_dvs.get(&path).cloned(),
                scan_type,
                path,
                dv_info: DvInfo { deletion_vector },
                partition_values,
                commit_timestamp: getters[23].get(row_index, "scanFile.timestamp")?,
                commit_version: getters[24].get(row_index, "scanFile.commit_version")?,
                size,
                base_row_id,
                default_row_commit_version,
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
    use crate::table_changes::CdfMode;
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::TableFeature;
    use crate::table_properties::{
        COLUMN_MAPPING_MODE, ENABLE_CHANGE_DATA_FEED, ENABLE_ROW_TRACKING,
    };
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Engine as _;

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

        // commit 0: a deletion-vector update (remove+add on the same path) plus an unpaired remove.
        let post_dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let add_paired = Add {
            path: "path_1".into(),
            deletion_vector: Some(post_dv.clone()),
            partition_values: HashMap::new(),
            data_change: true,
            size: 100,
            base_row_id: Some(10),
            default_row_commit_version: Some(0),
            ..Default::default()
        };
        let baseline_dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
            offset: Some(1),
            size_in_bytes: 38,
            cardinality: 1,
        };
        let paired_remove = Remove {
            path: "path_1".into(),
            deletion_vector: Some(baseline_dv.clone()),
            data_change: true,
            base_row_id: Some(10),
            default_row_commit_version: Some(0),
            ..Default::default()
        };
        let remove_unpaired = Remove {
            path: "path_2".into(),
            deletion_vector: None,
            data_change: true,
            ..Default::default()
        };

        // commit 1: a cdc action that the row-tracking path must ignore.
        let cdc = Cdc {
            path: "cdc_path".into(),
            partition_values: HashMap::new(),
            ..Default::default()
        };

        // commit 2: a standalone add (an insert).
        let add_insert = Add {
            path: "path_4".into(),
            deletion_vector: None,
            partition_values: HashMap::new(),
            data_change: true,
            size: 50,
            base_row_id: Some(20),
            default_row_commit_version: Some(2),
            ..Default::default()
        };

        // commit 3: a deletion-vector update whose removed version carried no deletion vector (so
        // the update is detectable only by the paired remove side, not its DV), plus a
        // metadata-only add (`data_change == false`) that must be excluded from the feed.
        let add_paired_no_dv = Add {
            path: "path_5".into(),
            deletion_vector: None,
            partition_values: HashMap::new(),
            data_change: true,
            size: 70,
            base_row_id: Some(30),
            default_row_commit_version: Some(3),
            ..Default::default()
        };
        let paired_remove_no_dv = Remove {
            path: "path_5".into(),
            deletion_vector: None,
            data_change: true,
            base_row_id: Some(30),
            default_row_commit_version: Some(3),
            ..Default::default()
        };
        let add_no_data_change = Add {
            path: "path_6".into(),
            deletion_vector: None,
            partition_values: HashMap::new(),
            data_change: false,
            size: 80,
            base_row_id: Some(40),
            default_row_commit_version: Some(3),
            ..Default::default()
        };

        mock_table
            .commit([
                Action::Remove(paired_remove.clone()),
                Action::Add(add_paired.clone()),
                Action::Remove(remove_unpaired.clone()),
            ])
            .await;
        mock_table.commit([Action::Cdc(cdc.clone())]).await;
        mock_table.commit([Action::Add(add_insert.clone())]).await;
        mock_table
            .commit([
                Action::Remove(paired_remove_no_dv.clone()),
                Action::Add(add_paired_no_dv.clone()),
                Action::Add(add_no_data_change.clone()),
            ])
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

        // Build a row-tracking-enabled start configuration, matching `CdfMode::ReadTime`. The
        // iterator's row-tracking behavior is driven by the mode argument; the feature enablement
        // on this config is not re-checked here unless a commit in the range carries a metadata
        // update (none do). `RowTracking` requires `DomainMetadata` to be supported.
        let metadata = Metadata::try_new(
            None,
            None,
            table_schema.clone(),
            vec![],
            0,
            HashMap::from([(ENABLE_ROW_TRACKING.to_string(), "true".to_string())]),
        )
        .unwrap();
        let protocol = Protocol::try_new_modern(
            TableFeature::EMPTY_LIST,
            [TableFeature::RowTracking, TableFeature::DomainMetadata],
        )
        .unwrap();
        let table_config =
            TableConfiguration::try_new(metadata, protocol, table_root.clone(), 0).unwrap();

        let scan_metadata = table_changes_action_iter_with_mode(
            engine,
            &table_config,
            log_segment.listed.ascending_commit_files.clone(),
            table_schema,
            None,
            CdfMode::ReadTime,
        )
        .unwrap();
        let scan_files: Vec<_> = scan_metadata_to_scan_file(scan_metadata)
            .try_collect()
            .unwrap();

        // The cdc action in commit 1 is ignored: only add/remove-derived files appear. The
        // metadata-only add (`data_change == false`) in commit 3 is also excluded, leaving path_1,
        // path_2, path_4, and path_5.
        assert!(scan_files
            .iter()
            .all(|f| f.scan_type != CdfScanFileType::Cdc));
        assert!(
            !scan_files.iter().any(|f| f.path == "path_6"),
            "metadata-only add (data_change=false) must be excluded"
        );
        assert_eq!(scan_files.len(), 4);

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

        // A DV update whose removed version had no deletion vector still groups into both sides;
        // the remove side simply carries no DV. This is the case that would look like a bare insert
        // if the two sides were not grouped.
        let paired_no_dv = listing
            .iter()
            .find(|fa| path_of(fa).as_deref() == Some("path_5"))
            .expect("paired-no-dv listing present");
        let paired_no_dv_add = paired_no_dv.add.as_ref().expect("paired-no-dv add side");
        let paired_no_dv_remove = paired_no_dv
            .remove
            .as_ref()
            .expect("paired-no-dv remove side");
        assert_eq!(paired_no_dv_add.base_row_id, Some(30));
        assert!(paired_no_dv_remove.deletion_vector.is_none());

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
