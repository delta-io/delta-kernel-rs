use std::collections::HashMap;
use std::iter::{self, empty, once};
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::scan::state::DvInfo;
use crate::scan::{ColumnType, ScanResult};
use crate::schema::{SchemaRef, StructType};
use crate::table_changes::scan_file::ScanFileType;
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Engine, Error, ExpressionRef};

use super::data_read::{resolve_scan_file_dv, DataReader};
use super::log_replay::{table_changes_action_iter, TableChangesScanData};
use super::scan_file::{scan_data_to_scan_file, ScanFile};
use super::{TableChanges, CDF_FIELDS};

/// The result of building a [`TableChanges`] scan over a table. This can be used to get a change
/// data feed from the table
#[allow(unused)]
#[derive(Debug)]
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
    physical_schema: StructType,
    table_schema: SchemaRef,
}

/// This builder constructs a [`TableChangesScan`] that can be used to read the [`TableChanges`]
/// of a table. [`TableChangesScanBuilder`] allows you to specify a schema to project the columns
/// or specify a predicate to filter rows in the Change Data Feed. Note that predicates over Change
/// Data Feed columns `_change_type`, `_commit_version`, and `_commit_timestamp` are not currently
/// allowed. See issue [#525](https://github.com/delta-io/delta-kernel-rs/issues/525).
///
/// Note: There is a lot of shared functionality between [`TableChangesScanBuilder`] and
/// [`ScanBuilder`].
///
/// [`ScanBuilder`]: crate::scan::ScanBuilder
/// #Examples
/// Construct a [`TableChangesScan`] from `table_changes` with a given schema and predicate
/// ```rust
/// # use std::sync::Arc;
/// # use delta_kernel::engine::sync::SyncEngine;
/// # use delta_kernel::expressions::{column_expr, Scalar};
/// # use delta_kernel::{Expression, Table};
/// # let path = "./tests/data/table-with-cdf";
/// # let engine = Box::new(SyncEngine::new());
/// # let table = Table::try_from_uri(path).unwrap();
/// # let table_changes = table.table_changes(engine.as_ref(), 0, 1).unwrap();
/// let schema = table_changes
///     .schema()
///     .project(&["id", "_commit_version"])
///     .unwrap();
/// let predicate = Arc::new(Expression::gt(column_expr!("id"), Scalar::from(10)));
/// let scan = table_changes
///     .into_scan_builder()
///     .with_schema(schema)
///     .with_predicate(predicate.clone())
///     .build();
/// ```
#[derive(Debug)]
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}

impl TableChangesScanBuilder {
    /// Create a new [`TableChangesScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`TableChanges`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn with_schema(mut self, schema: impl Into<Option<SchemaRef>>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use `TableChanges`'s entire (logical) schema (e.g. SELECT *)
        let logical_schema = self
            .schema
            .unwrap_or_else(|| self.table_changes.schema.clone().into());
        let mut have_partition_cols = false;
        let mut read_fields = Vec::with_capacity(logical_schema.fields.len());

        // Loop over all selected fields. We produce the following:
        // - If the field is read from the parquet file then it is ([`ColumnType::Selected`]).
        // - If the field is a column generated by CDF, it is also  ([`ColumnType::Selected`]).
        //   These fields will be handled separately from the other ([`ColumnType::Selected`]).
        // - If the field is a partition column, it is ([`ColumnType::Partition`]).
        //
        //   Both the partition columns and CDF generated columns will be filled in by evaluating an
        //   expression when transforming physical data to the logical representation.
        let all_fields = logical_schema
            .fields()
            .enumerate()
            .map(|(index, logical_field)| -> DeltaResult<_> {
                if self
                    .table_changes
                    .partition_columns()
                    .contains(logical_field.name())
                {
                    // Store the index into the schema for this field. When we turn it into an
                    // expression in the inner loop, we will index into the schema and get the name and
                    // data type, which we need to properly materialize the column.
                    have_partition_cols = true;
                    Ok(ColumnType::Partition(index))
                } else if CDF_FIELDS
                    .iter()
                    .any(|field| field.name() == logical_field.name())
                {
                    // CDF Columns are generated, so they do not have a column mapping. These will
                    // be processed separately and used to build an expression when transforming physical
                    // data to logical.
                    Ok(ColumnType::Selected(logical_field.name().to_string()))
                } else {
                    // Add to read schema, store field so we can build a `Column` expression later
                    // if needed (i.e. if we have partition columns)
                    let physical_field =
                        logical_field.make_physical(*self.table_changes.column_mapping_mode())?;
                    debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                    let physical_name = physical_field.name.clone();
                    read_fields.push(physical_field);
                    Ok(ColumnType::Selected(physical_name))
                }
            })
            .try_collect()?;
        let table_schema = self.table_changes.end_snapshot.schema().clone().into();
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            predicate: self.predicate,
            all_fields,
            have_partition_cols,
            physical_schema: StructType::new(read_fields),
            table_schema,
        })
    }
}

/// State that doesn't change between scans
/// TODO: make serializable/deserializable again
#[derive(Clone, Debug)]
pub struct GlobalScanState {
    pub table_root: String,
    pub partition_columns: Vec<String>,
    pub logical_schema: SchemaRef,
    pub read_schema: SchemaRef,
    pub column_mapping_mode: ColumnMappingMode,
    pub have_partition_cols: bool,
    pub all_fields: Vec<ColumnType>,
}
impl TableChangesScan {
    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub(crate) fn global_scan_state(&self) -> GlobalScanState {
        GlobalScanState {
            table_root: self.table_changes.table_root.to_string(),
            partition_columns: self.table_changes.partition_columns().clone(),
            logical_schema: self.logical_schema.clone(),
            read_schema: self.physical_schema.clone().into(),
            column_mapping_mode: *self.table_changes.column_mapping_mode(),
            have_partition_cols: self.have_partition_cols,
            all_fields: self.all_fields.clone(),
        }
    }

    pub fn scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let commits = self
            .table_changes
            .log_segment
            .ascending_commit_files
            .clone();
        table_changes_action_iter(
            engine,
            commits,
            self.table_schema.clone(),
            self.predicate.clone(),
        )
    }
    pub fn execute(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>> + '_> {
        let scan_data = self.scan_data(engine.as_ref())?;
        let scan_files = scan_data_to_scan_file(scan_data);
        let global_scan_state = self.global_scan_state();

        let dv_engine_ref = engine.clone();
        let result = scan_files
            .map(move |scan_file| {
                let (scan_file, rm_dv) = scan_file?;
                resolve_scan_file_dv(
                    dv_engine_ref.as_ref(),
                    self.table_changes.table_root(),
                    scan_file,
                    rm_dv,
                )
            })
            .flatten_ok()
            .map(move |x| -> DeltaResult<_> {
                let (scan_file, selection_vector) = x?;
                let engine = engine.clone();
                let reader =
                    DataReader::new(global_scan_state.clone(), scan_file, selection_vector);
                reader.into_data(engine.as_ref())
            })
            .flatten_ok()
            .map(|x| x?);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr, Scalar};
    use crate::scan::ColumnType;
    use crate::schema::{DataType, StructField, StructType};
    use crate::{Expression, Table};

    #[test]
    fn simple_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = table.table_changes(engine.as_ref(), 0, 1).unwrap();

        let scan = table_changes.into_scan_builder().build().unwrap();
        // Note that this table is not partitioned. `part` is a regular field
        assert_eq!(
            scan.all_fields,
            vec![
                ColumnType::Selected("part".to_string()),
                ColumnType::Selected("id".to_string()),
                ColumnType::Selected("_change_type".to_string()),
                ColumnType::Selected("_commit_version".to_string()),
                ColumnType::Selected("_commit_timestamp".to_string()),
            ]
        );
        assert_eq!(scan.predicate, None);
        assert!(!scan.have_partition_cols);
    }

    #[test]
    fn projected_and_filtered_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = table.table_changes(engine.as_ref(), 0, 1).unwrap();

        let schema = table_changes
            .schema()
            .project(&["id", "_commit_version"])
            .unwrap();
        let predicate = Arc::new(Expression::gt(column_expr!("id"), Scalar::from(10)));
        let scan = table_changes
            .into_scan_builder()
            .with_schema(schema)
            .with_predicate(predicate.clone())
            .build()
            .unwrap();
        assert_eq!(
            scan.all_fields,
            vec![
                ColumnType::Selected("id".to_string()),
                ColumnType::Selected("_commit_version".to_string()),
            ]
        );
        assert_eq!(
            scan.logical_schema,
            StructType::new([
                StructField::new("id", DataType::INTEGER, true),
                StructField::new("_commit_version", DataType::LONG, false),
            ])
            .into()
        );
        assert!(!scan.have_partition_cols);
        assert_eq!(scan.predicate, Some(predicate));
    }
}
