//! This module handles [`ScanFile`]s for [`TableChangeScan`]. A [`ScanFile`] consists of all the
//! metadata required to generate Change Data Feed. [`ScanFile`] can be read using
//! [`ScanFileVisitor`]. The visitor may read from a log with the schema [`scan_row_schema`].
//! You can convert a log to this schema using the [`transform_to_scan_row_expression`].

use itertools::Itertools;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::visit_deletion_vector_at;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::scan::state::DvInfo;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

use super::log_replay::TableChangesScanData;

#[allow(unused)]
pub(crate) fn scan_data_to_scan_file(
    scan_data: impl Iterator<Item = DeltaResult<TableChangesScanData>>,
) -> impl Iterator<Item = DeltaResult<(ScanFile, Option<Arc<HashMap<String, DvInfo>>>)>> {
    scan_data
        .map(|scan_data| -> DeltaResult<_> {
            let scan_data = scan_data?;
            let callback: ScanCallback<Vec<ScanFile>> =
                |context, scan_file| context.push(scan_file);
            let result = visit_scan_files(
                scan_data.data.as_ref(),
                &scan_data.selection_vector,
                vec![],
                callback,
            )?
            .into_iter()
            .map(move |scan_file| (scan_file, scan_data.remove_dv.clone()));
            Ok(result)
        })
        .flatten_ok()
}

// The type of action associated with a [`ScanFile`]
#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScanFileType {
    Add,
    Remove,
    Cdc,
}

/// Represents all the metadata needed to read a Change Data Feed. It has the following fields:
#[allow(unused)]
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct ScanFile {
    /// The type of action this file belongs to. This may be one of add, remove, or cdc.
    pub tpe: ScanFileType,
    /// a `&str` which is the path to the file
    pub path: String,
    /// a [`DvInfo`] struct, which allows getting the selection vector for this file
    pub dv_info: DvInfo,
    /// a `HashMap<String, String>` which are partition values
    pub partition_values: HashMap<String, String>,
    /// the commit version that this action was performed in
    pub commit_version: u64,
    /// the timestamp of the commit that this action was performed in
    pub timestamp: i64,
}

pub(crate) type ScanCallback<T> = fn(context: &mut T, scan_file: ScanFile);

/// Request that the kernel call a callback on each valid file that needs to be read for the
/// scan.
///
/// The arguments to the callback are:
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through to each call
/// * `ScanFile`: a [`ScanFile`] struct that holds all the metadata required to perform Change Data
///   Feed
///
/// ## Context
/// A note on the `context`. This can be any value the engine wants. This function takes ownership
/// of the passed arg, but then returns it, so the engine can repeatedly call `visit_scan_files`
/// with the same context.
///
/// ## Example
/// ```ignore
/// let mut context = [my context];
/// for res in scan_data { // scan data table_changes_scan.scan_data()
///     let (data, vector, remove_dv) = res?;
///     context = delta_kernel::table_changes::scan_file::visit_scan_files(
///        data.as_ref(),
///        selection_vector,
///        context,
///        my_callback,
///     )?;
/// }
/// ```
#[allow(unused)]
pub(crate) fn visit_scan_files<T>(
    data: &dyn EngineData,
    selection_vector: &[bool],
    context: T,
    callback: ScanCallback<T>,
) -> DeltaResult<T> {
    let mut visitor = ScanFileVisitor {
        callback,
        selection_vector,
        context,
    };

    visitor.visit_rows_of(data)?;
    Ok(visitor.context)
}

// add some visitor magic for engines
#[allow(unused)]
struct ScanFileVisitor<'a, T> {
    callback: ScanCallback<T>,
    selection_vector: &'a [bool],
    context: T,
}

impl<T> RowVisitor for ScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let expected_getters = 18;
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of ScanFileVisitor getters: {}",
                getters.len()
            ))
        );
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                // skip skipped rows
                continue;
            }
            let timestamp = getters[16].get(row_index, "scanFile.timestamp")?;
            let commit_version: i64 = getters[17].get(row_index, "scanFile.commit_version")?;
            let commit_version: u64 = commit_version.try_into().map_err(Error::generic)?;

            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.add.path")? {
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[1..=5])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values =
                    getters[6].get(row_index, "scanFile.add.fileConstantValues.partitionValues")?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Add,
                    path,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[7].get_opt(row_index, "scanFile.remove.path")? {
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[8..=12])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values = getters[13].get(
                    row_index,
                    "scanFile.remove.fileConstantValues.partitionValues",
                )?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Remove,
                    path,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[14].get_opt(row_index, "scanFile.cdc.path")? {
                let partition_values = getters[15]
                    .get(row_index, "scanFile.cdc.fileConstantValues.partitionValues")?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Cdc,
                    path,
                    dv_info: DvInfo {
                        deletion_vector: None,
                    },
                    partition_values,
                    commit_version,
                    timestamp,
                };

                (self.callback)(&mut self.context, scan_file)
            }
        }
        Ok(())
    }

    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [crate::expressions::ColumnName],
        &'static [DataType],
    ) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| scan_row_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }
}

/// Get the schema that scan rows (from [`TableChanges::scan_data`]) will be returned with.
///
/// It is:
/// ```ignored
/// {
///   add: {
///     path: string,
///     deletionVector: {
///       storageType: string,
///       pathOrInlineDv: string,
///       offset: int,
///       sizeInBytes: int,
///       cardinality: long,
///     },
///     fileConstantValues: {
///       partitionValues: map<string, string>
///     }
///   }
///   remove: {
///     path: string,
///     deletionVector: {
///       storageType: string,
///       pathOrInlineDv: string,
///       offset: int,
///       sizeInBytes: int,
///       cardinality: long,
///     },
///     fileConstantValues: {
///       partitionValues: map<string, string>
///     },
///   }
///   cdc: {
///     path: string,
///     fileConstantValues: {
///       partitionValues: map<string, string>
///     }
///   }
///   commit_timestamp: long,
///   commit_version: long
///
/// }
/// ```
pub(crate) fn scan_row_schema() -> SchemaRef {
    static TABLE_CHANGES_SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
        let deletion_vector = StructType::new([
            StructField::new("storageType", DataType::STRING, true),
            StructField::new("pathOrInlineDv", DataType::STRING, true),
            StructField::new("offset", DataType::INTEGER, true),
            StructField::new("sizeInBytes", DataType::INTEGER, true),
            StructField::new("cardinality", DataType::LONG, true),
        ]);
        let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
        let file_constant_values =
            StructType::new([StructField::new("partitionValues", partition_values, true)]);

        let add = StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("deletionVector", deletion_vector.clone(), true),
            StructField::new("fileConstantValues", file_constant_values.clone(), true),
        ]);
        let remove = StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("deletionVector", deletion_vector, true),
            StructField::new("fileConstantValues", file_constant_values.clone(), true),
        ]);
        let cdc = StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("fileConstantValues", file_constant_values, true),
        ]);

        Arc::new(StructType::new([
            StructField::new("add", add, true),
            StructField::new("remove", remove, true),
            StructField::new("cdc", cdc, true),
            StructField::new("timestamp", DataType::LONG, true),
            StructField::new("commit_version", DataType::LONG, true),
        ]))
    });
    TABLE_CHANGES_SCAN_ROW_SCHEMA.clone()
}

/// Expression to convert an action with `log_schema` into one with
/// `TABLE_CHANGES_SCAN_ROW_SCHEMA`. This is the expression used to create `TableChangesScanData`.
#[allow(unused)]
pub(crate) fn transform_to_scan_row_expression(timestamp: i64, commit_number: i64) -> Expression {
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
        timestamp.into(),
        commit_number.into(),
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;

    use super::ScanFileType;
    use super::{
        scan_row_schema, transform_to_scan_row_expression, visit_scan_files, ScanCallback, ScanFile,
    };
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::actions::{get_log_schema, Add, Cdc, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::scan::state::DvInfo;
    use crate::utils::test_utils::MockTable;
    use crate::{DeltaResult, Engine};

    #[tokio::test]
    async fn schema_transform_correct() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let add_dv = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let add_partition_values = HashMap::from([("a".to_string(), "b".to_string())]);
        let add = Add {
            path: "fake_path_1".into(),
            deletion_vector: Some(add_dv.clone()),
            partition_values: add_partition_values,
            ..Default::default()
        };

        let rm_dv = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
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
            ..Default::default()
        };

        let cdc_partition_values = HashMap::from([("x".to_string(), "y".to_string())]);
        let cdc = Cdc {
            path: "fake_path_3".into(),
            partition_values: cdc_partition_values,
            ..Default::default()
        };

        mock_table
            .commit(&[
                add.clone().into(),
                remove.clone().into(),
                cdc.clone().into(),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let log_root = table_root.join("_delta_log/").unwrap();
        let log_segment =
            LogSegment::for_table_changes(engine.get_file_system_client().as_ref(), log_root, 0, 0)
                .unwrap();
        let commit = log_segment.ascending_commit_files[0].clone();

        let actions = engine
            .get_json_handler()
            .read_json_files(&[commit.location.clone()], get_log_schema().clone(), None)
            .unwrap();

        let timestamp = 1234_i64;
        let commit_version = 42_u64;

        let scan_files: Vec<_> = actions
            .map_ok(|actions| {
                engine
                    .get_expression_handler()
                    .get_evaluator(
                        get_log_schema().clone(),
                        transform_to_scan_row_expression(1234, 42),
                        scan_row_schema().into(),
                    )
                    .evaluate(actions.as_ref())
                    .unwrap()
            })
            .map(|data| -> DeltaResult<_> {
                let data = data?;
                let selection_vector = vec![true; data.len()];
                let callback: ScanCallback<Vec<ScanFile>> =
                    |context, scan_file| context.push(scan_file);
                visit_scan_files(data.as_ref(), &selection_vector, vec![], callback)
            })
            .flatten_ok()
            .try_collect()
            .unwrap();

        let expected_scan_files = vec![
            ScanFile {
                tpe: ScanFileType::Add,
                path: add.path,
                dv_info: DvInfo {
                    deletion_vector: add.deletion_vector,
                },
                partition_values: add.partition_values,
                commit_version,
                timestamp,
            },
            ScanFile {
                tpe: ScanFileType::Remove,
                path: remove.path,
                dv_info: DvInfo {
                    deletion_vector: remove.deletion_vector,
                },
                partition_values: remove.partition_values.unwrap(),
                commit_version,
                timestamp,
            },
            ScanFile {
                tpe: ScanFileType::Cdc,
                path: cdc.path,
                dv_info: DvInfo {
                    deletion_vector: None,
                },
                partition_values: cdc.partition_values,
                commit_version,
                timestamp,
            },
        ];
        assert_eq!(expected_scan_files, scan_files);
    }
}
