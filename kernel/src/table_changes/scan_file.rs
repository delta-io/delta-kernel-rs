//! This module handles [`ScanFile`]s for [`TableChangeScan`]. A [`ScanFile`] consists of all the
//! metadata required to generate Change Data Feed. [`ScanFile`] can be read using
//! [`ScanFileVisitor`]. The visitor may read from a log with the schema [`scan_row_schema`].
//! You can convert a log to this schema using the [`transform_to_scan_row_expression`].

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::visit_deletion_vector_at;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::scan::state::DvInfo;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

// The type of action associated with a [`ScanFile`]
#[derive(Debug, Clone)]
pub(crate) enum ScanFileType {
    Add,
    Remove,
    Cdc,
}

/// Represents all the metadata needed to read a Change Data Feed. It has the following fields:
/// * `tpe`: The type of action this file belongs to. This may be one of add, remove, or cdc.
/// * `path`: a `&str` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `dv_info`: a [`DvInfo`] struct, which allows getting the selection vector for this file
/// * `partition_values`: a `HashMap<String, String>` which are partition values
/// * `commit_version`: the commit version that this action was performed in
/// * `timestamp`: the timestamp of the commit that this action was performed in
#[derive(Debug)]
pub(crate) struct ScanFile {
    pub tpe: ScanFileType,
    pub path: String,
    pub size: i64,
    pub dv_info: DvInfo,
    pub partition_values: HashMap<String, String>,
    pub commit_version: u64,
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
struct ScanFileVisitor<'a, T> {
    callback: ScanCallback<T>,
    selection_vector: &'a [bool],
    context: T,
}

impl<T> RowVisitor for ScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let expected_getters = 21;
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
            let timestamp = getters[19].get(row_index, "scanFile.timestamp")?;
            let commit_version: i64 = getters[20].get(row_index, "scanFile.commit_version")?;
            let commit_version: u64 = commit_version.try_into().map_err(Error::generic)?;

            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.add.path")? {
                let size = getters[1].get(row_index, "scanFile.add.size")?;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[2..=6])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values =
                    getters[7].get(row_index, "scanFile.add.fileConstantValues.partitionValues")?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Add,
                    path,
                    size,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[8].get_opt(row_index, "scanFile.remove.path")? {
                let size = getters[9].get(row_index, "scanFile.remove.size")?;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[10..=14])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values = getters[15].get(
                    row_index,
                    "scanFile.remove.fileConstantValues.partitionValues",
                )?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Remove,
                    path,
                    size,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[16].get_opt(row_index, "scanFile.cdc.path")? {
                let size = getters[17].get(row_index, "scanFile.cdc.size")?;
                let partition_values = getters[18]
                    .get(row_index, "scanFile.cdc.fileConstantValues.partitionValues")?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Cdc,
                    path,
                    size,
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
///     size: long,
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
///     size: long,
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
///     size: long,
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
            column_expr!("cdc.size"),
            Expression::struct_from([column_expr!("cdc.partitionValues")]),
        ]),
        timestamp.into(),
        commit_number.into(),
    ])
}

#[cfg(test)]
mod tests {}
