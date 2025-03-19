//! Functionality to create and execute table changes scans over the data in the delta table

use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;
use url::Url;

use crate::actions::deletion_vector::split_vector;
use crate::scan::state::GlobalScanState;
use crate::scan::{ColumnType, PhysicalPredicate, ScanResult};
use crate::schema::{SchemaRef, StructType};
use crate::{DeltaResult, Engine, ExpressionRef, FileMeta};

use super::log_replay::{table_changes_action_iter, TableChangesScanData};
use super::physical_to_logical::{physical_to_logical_expr, scan_file_physical_schema};
use super::resolve_dvs::{resolve_scan_file_dv, ResolvedCdfScanFile};
use super::scan_file::scan_data_to_scan_file;
use super::{TableChanges, CDF_FIELDS};

/// The result of building a [`TableChanges`] scan over a table. This can be used to get the change
/// data feed from the table.
#[derive(Debug)]
pub struct TableChangesScan {
    // The [`TableChanges`] that specifies this scan's start and end versions
    table_changes: Arc<TableChanges>,
    // The logical schema of the Change Data Feed. By default, this is the schema from
    // [`TableChanges::schema`]. The schema may be projected to a subset of those columns. See
    // [`TableChangesScanBuilder::with_schema`]
    logical_schema: SchemaRef,
    // The physical schema. This schema omits partition columns and columns generated for Change
    // Data Feed
    physical_schema: SchemaRef,
    // The predicate to filter the data
    physical_predicate: PhysicalPredicate,
    // The [`ColumnType`] of all the fields in the `logical_schema`
    all_fields: Arc<Vec<ColumnType>>,
}

/// This builder constructs a [`TableChangesScan`] that can be used to read the [`TableChanges`]
/// of a table. [`TableChangesScanBuilder`] allows you to specify a schema to project the columns
/// or specify a predicate to filter rows in the Change Data Feed. Note that predicates containing Change
/// Data Feed columns `_change_type`, `_commit_version`, and `_commit_timestamp` are not currently
/// allowed. See issue [#525](https://github.com/delta-io/delta-kernel-rs/issues/525).
///
/// Note: There is a lot of shared functionality between [`TableChangesScanBuilder`] and
/// [`ScanBuilder`].
///
/// [`ScanBuilder`]: crate::scan::ScanBuilder
/// # Example
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
                    let physical_field = logical_field.make_physical();
                    debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                    let physical_name = physical_field.name.clone();
                    read_fields.push(physical_field);
                    Ok(ColumnType::Selected(physical_name))
                }
            })
            .try_collect()?;
        let physical_predicate = match self.predicate {
            Some(predicate) => PhysicalPredicate::try_new(&predicate, &logical_schema)?,
            None => PhysicalPredicate::None,
        };

        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            physical_predicate,
            all_fields: Arc::new(all_fields),
            physical_schema: StructType::new(read_fields).into(),
        })
    }
}

impl TableChangesScan {
    /// Returns an iterator of [`TableChangesScanData`] necessary to read CDF. Each row
    /// represents an action in the delta log. These rows are filtered to yield only the actions
    /// necessary to read CDF. Additionally, [`TableChangesScanData`] holds metadata on the
    /// deletion vectors present in the commit. The engine data in each scan data is guaranteed
    /// to belong to the same commit. Several [`TableChangesScanData`] may belong to the same commit.
    fn scan_data(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let commits = self
            .table_changes
            .log_segment
            .ascending_commit_files
            .clone();
        // NOTE: This is a cheap arc clone
        let physical_predicate = match self.physical_predicate.clone() {
            PhysicalPredicate::StaticSkipAll => return Ok(None.into_iter().flatten()),
            PhysicalPredicate::Some(predicate, schema) => Some((predicate, schema)),
            PhysicalPredicate::None => None,
        };
        let schema = self.table_changes.end_snapshot.schema().clone().into();
        let it = table_changes_action_iter(engine, commits, schema, physical_predicate)?;
        Ok(Some(it).into_iter().flatten())
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    fn global_scan_state(&self) -> GlobalScanState {
        let end_snapshot = &self.table_changes.end_snapshot;
        GlobalScanState {
            table_root: self.table_changes.table_root.to_string(),
            partition_columns: end_snapshot.metadata().partition_columns.clone(),
            logical_schema: self.logical_schema.clone(),
            physical_schema: self.physical_schema.clone(),
        }
    }

    /// Get a shared reference to the [`Schema`] of the table changes scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    /// Get the predicate [`ExpressionRef`] of the scan.
    fn physical_predicate(&self) -> Option<ExpressionRef> {
        if let PhysicalPredicate::Some(ref predicate, _) = self.physical_predicate {
            Some(predicate.clone())
        } else {
            None
        }
    }

    /// Perform an "all in one" scan to get the change data feed. This will use the provided `engine`
    /// to read and process all the data for the query. Each [`ScanResult`] in the resultant iterator
    /// encapsulates the raw data and an optional boolean vector built from the deletion vector if it
    /// was present. See the documentation for [`ScanResult`] for more details.
    pub fn execute(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
        let scan_data = self.scan_data(engine.clone())?;
        let scan_files = scan_data_to_scan_file(scan_data);

        let global_scan_state = self.global_scan_state();
        let table_root = self.table_changes.table_root().clone();
        let all_fields = self.all_fields.clone();
        let physical_predicate = self.physical_predicate();
        let dv_engine_ref = engine.clone();

        let result = scan_files
            .map(move |scan_file| {
                resolve_scan_file_dv(dv_engine_ref.as_ref(), &table_root, scan_file?)
            }) // Iterator-Result-Iterator
            .flatten_ok() // Iterator-Result
            .map(move |resolved_scan_file| -> DeltaResult<_> {
                read_scan_file(
                    engine.as_ref(),
                    resolved_scan_file?,
                    &global_scan_state,
                    &all_fields,
                    physical_predicate.clone(),
                )
            }) // Iterator-Result-Iterator-Result
            .flatten_ok() // Iterator-Result-Result
            .map(|x| x?); // Iterator-Result

        Ok(result)
    }
}

/// Reads the data at the `resolved_scan_file` and transforms the data from physical to logical.
/// The result is a fallible iterator of [`ScanResult`] containing the logical data.
fn read_scan_file(
    engine: &dyn Engine,
    resolved_scan_file: ResolvedCdfScanFile,
    global_state: &GlobalScanState,
    all_fields: &[ColumnType],
    physical_predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
    let ResolvedCdfScanFile {
        scan_file,
        mut selection_vector,
    } = resolved_scan_file;

    let physical_to_logical_expr =
        physical_to_logical_expr(&scan_file, global_state.logical_schema.as_ref(), all_fields)?;
    let physical_schema =
        scan_file_physical_schema(&scan_file, global_state.physical_schema.as_ref());
    let phys_to_logical_eval = engine.get_expression_handler().get_evaluator(
        physical_schema.clone(),
        physical_to_logical_expr,
        global_state.logical_schema.clone().into(),
    );
    // Determine if the scan file was derived from a deletion vector pair
    let is_dv_resolved_pair = scan_file.remove_dv.is_some();

    let table_root = Url::parse(&global_state.table_root)?;
    let location = table_root.join(&scan_file.path)?;
    let file = FileMeta {
        last_modified: 0,
        size: 0,
        location,
    };
    let read_result_iter = engine.get_parquet_handler().read_parquet_files(
        &[file],
        physical_schema,
        physical_predicate,
    )?;

    let result = read_result_iter.map(move |batch| -> DeltaResult<_> {
        let batch = batch?;
        // to transform the physical data into the correct logical form
        let logical = phys_to_logical_eval.evaluate(batch.as_ref());
        let len = logical.as_ref().map_or(0, |res| res.len());
        // need to split the dv_mask. what's left in dv_mask covers this result, and rest
        // will cover the following results. we `take()` out of `selection_vector` to avoid
        // trying to return a captured variable. We're going to reassign `selection_vector`
        // to `rest` in a moment anyway
        let mut sv = selection_vector.take();

        // Gets the selection vector for a data batch with length `len`. There are three cases to
        // consider:
        // 1. A scan file derived from a deletion vector pair getting resolved.
        // 2. A scan file that was not the result of a resolved pair, and has a deletion vector.
        // 3. A scan file that was not the result of a resolved pair, and has no deletion vector.
        //
        // # Case 1
        // If the scan file is derived from a deletion vector pair, its selection vector should be
        // extended with `false`. Consider a resolved selection vector `[0, 1]`. Only row 1 has
        // changed. If there were more rows (for example 4 total), then none of them have changed.
        // Hence, the selection vector is extended to become `[0, 1, 0, 0]`.
        //
        // # Case 2
        // If the scan file has a deletion vector but is unpaired, its selection vector should be
        // extended with `true`. Consider a deletion vector with row 1 deleted. This generates a
        // selection vector `[1, 0, 1]`. Only row 1 is deleted. Rows 0 and 2 are selected. If there
        // are more rows (for example 4), then all the extra rows should be selected. The selection
        // vector becomes `[1, 0, 1, 1]`.
        //
        // # Case 3
        // These scan files are either simple adds, removes, or cdc files. This case is a noop because
        // the selection vector is `None`.
        let extend = Some(!is_dv_resolved_pair);
        let rest = split_vector(sv.as_mut(), len, extend);
        let result = ScanResult {
            raw_data: logical,
            raw_mask: sv,
        };
        selection_vector = rest;
        Ok(result)
    });
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr, Scalar};
    use crate::scan::{ColumnType, PhysicalPredicate};
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::COMMIT_VERSION_COL_NAME;
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
            .into()
        );
        assert_eq!(scan.physical_predicate, PhysicalPredicate::None);
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
            .project(&["id", COMMIT_VERSION_COL_NAME])
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
            .into()
        );
        assert_eq!(
            scan.logical_schema,
            StructType::new([
                StructField::nullable("id", DataType::INTEGER),
                StructField::not_null("_commit_version", DataType::LONG),
            ])
            .into()
        );
        assert_eq!(
            scan.physical_predicate,
            PhysicalPredicate::Some(
                predicate,
                StructType::new([StructField::nullable("id", DataType::INTEGER),]).into()
            )
        );
    }
}
