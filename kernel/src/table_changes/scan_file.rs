//! This module handles [`CdfScanFile`]s for [`TableChangesScan`]. A [`CdfScanFile`] consists of all the
//! metadata required to generate a change data feed. [`CdfScanFile`] can be constructed using
//! [`CdfScanFileVisitor`]. The visitor reads from engine data with the schema [`cdf_scan_row_schema`].
//! You can convert engine data to this schema using the [`cdf_scan_row_expression`].
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use super::log_replay::TableChangesScanMetadata;
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
}

pub(crate) type CdfScanCallback<T> = fn(context: &mut T, scan_file: CdfScanFile);

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
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through to each call
/// * `CdfScanFile`: a [`CdfScanFile`] struct that holds all the metadata required to perform Change Data
///   Feed
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
            getters.len() == 18,
            Error::InternalError(format!(
                "Wrong number of CdfScanFileVisitor getters: {}",
                getters.len()
            ))
        );
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                continue;
            }

            let (scan_type, path, deletion_vector, partition_values) =
                if let Some(path) = getters[0].get_opt(row_index, "scanFile.add.path")? {
                    let scan_type = CdfScanFileType::Add;
                    let deletion_vector = visit_deletion_vector_at(row_index, &getters[1..=5])?;
                    let partition_values = getters[6]
                        .get_opt(row_index, "scanFile.add.fileConstantValues.partitionValues")?;
                    (scan_type, path, deletion_vector, partition_values)
                } else if let Some(path) = getters[7].get_opt(row_index, "scanFile.remove.path")? {
                    let scan_type = CdfScanFileType::Remove;
                    let deletion_vector = visit_deletion_vector_at(row_index, &getters[8..=12])?;
                    let partition_values = getters[13].get_opt(
                        row_index,
                        "scanFile.remove.fileConstantValues.partitionValues",
                    )?;
                    (scan_type, path, deletion_vector, partition_values)
                } else if let Some(path) = getters[14].get_opt(row_index, "scanFile.cdc.path")? {
                    let scan_type = CdfScanFileType::Cdc;
                    let partition_values = getters[15]
                        .get_opt(row_index, "scanFile.cdc.fileConstantValues.partitionValues")?;
                    (scan_type, path, None, partition_values)
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
                commit_timestamp: getters[16].get(row_index, "scanFile.timestamp")?,
                commit_version: getters[17].get(row_index, "scanFile.commit_version")?,
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
        ]);
        let remove = StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("deletionVector", deletion_vector),
            StructField::nullable("fileConstantValues", file_constant_values.clone()),
        ]);
        let cdc = StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("fileConstantValues", file_constant_values),
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
        ]),
        Expression::struct_from([
            column_expr!("remove.path"),
            column_expr!("remove.deletionVector"),
            Expression::struct_from([column_expr!("remove.partitionValues")]),
        ]),
        Expression::struct_from([
            column_expr!("cdc.path"),
            Expression::struct_from([column_expr!("cdc.partitionValues")]),
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

    use super::{scan_metadata_to_scan_file, CdfScanFile, CdfScanFileType};
    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use crate::actions::{Add, Cdc, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::scan::state::DvInfo;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::log_replay::table_changes_action_iter;
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
            ..Default::default()
        };
        let remove_paired = Remove {
            path: "fake_path_1".into(),
            deletion_vector: None,
            partition_values: None,
            data_change: true,
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
            ..Default::default()
        };

        mock_table
            .commit([
                Action::Remove(remove_paired.clone()),
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
        let table_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]);
        let scan_metadata = table_changes_action_iter(
            Arc::new(engine),
            log_segment.ascending_commit_files.clone(),
            table_schema.into(),
            None,
        )
        .unwrap();
        let scan_files: Vec<_> = scan_metadata_to_scan_file(scan_metadata)
            .try_collect()
            .unwrap();

        // Generate the expected [`CdfScanFile`]
        let timestamps = log_segment
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
            },
        ];

        assert_eq!(scan_files, expected_scan_files);
    }
}
