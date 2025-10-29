//! Functionality to create and execute table changes scans over the data in the delta table

use std::sync::Arc;

use itertools::Itertools;
use url::Url;

use crate::actions::deletion_vector::split_vector;
use crate::scan::field_classifiers::CdfTransformFieldClassifier;
use crate::scan::state_info::StateInfo;
use crate::scan::{PhysicalPredicate, ScanResult};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, FileMeta, PredicateRef};

use super::log_replay::{table_changes_action_iter, TableChangesScanMetadata};
use super::physical_to_logical::{get_cdf_transform_expr, scan_file_physical_schema};
use super::resolve_dvs::{resolve_scan_file_dv, ResolvedCdfScanFile};
use super::scan_file::scan_metadata_to_scan_file;
use super::TableChanges;

/// The result of building a [`TableChanges`] scan over a table. This can be used to get the change
/// data feed from the table.
#[derive(Debug)]
pub struct TableChangesScan {
    // The [`TableChanges`] that specifies this scan's start and end versions
    table_changes: Arc<TableChanges>,
    // All scan state including schemas, predicate, and transform spec
    state_info: Arc<StateInfo>,
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
/// # use test_utils::DefaultEngineExtension;
/// # use delta_kernel::engine::default::DefaultEngine;
/// # use delta_kernel::expressions::{column_expr, Scalar};
/// # use delta_kernel::Predicate;
/// # use delta_kernel::table_changes::TableChanges;
/// # let path = "./tests/data/table-with-cdf";
/// # let engine = DefaultEngine::new_local();
/// # let url = delta_kernel::try_parse_uri(path).unwrap();
/// # let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).unwrap();
/// let schema = table_changes
///     .schema()
///     .project(&["id", "_commit_version"])
///     .unwrap();
/// let predicate = Arc::new(Predicate::gt(column_expr!("id"), Scalar::from(10)));
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
    predicate: Option<PredicateRef>,
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
    pub fn with_predicate(mut self, predicate: impl Into<Option<PredicateRef>>) -> Self {
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

        // Create StateInfo using CDF field classifier
        let state_info = StateInfo::try_new(
            logical_schema,
            self.table_changes.end_snapshot.table_configuration(),
            self.predicate,
            CdfTransformFieldClassifier,
        )?;

        Ok(TableChangesScan {
            table_changes: self.table_changes,
            state_info: Arc::new(state_info),
        })
    }
}

impl TableChangesScan {
    /// Returns an iterator of [`TableChangesScanMetadata`] necessary to read CDF. Each row
    /// represents an action in the delta log. These rows are filtered to yield only the actions
    /// necessary to read CDF. Additionally, [`TableChangesScanMetadata`] holds metadata on the
    /// deletion vectors present in the commit. The engine data in each scan metadata is guaranteed
    /// to belong to the same commit. Several [`TableChangesScanMetadata`] may belong to the same
    /// commit.
    fn scan_metadata(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>> {
        let commits = self
            .table_changes
            .log_segment
            .ascending_commit_files
            .clone();
        // NOTE: This is a cheap arc clone
        let physical_predicate = match self.state_info.physical_predicate.clone() {
            PhysicalPredicate::StaticSkipAll => return Ok(None.into_iter().flatten()),
            PhysicalPredicate::Some(predicate, schema) => Some((predicate, schema)),
            PhysicalPredicate::None => None,
        };
        let schema = self.table_changes.end_snapshot.schema();
        let it = table_changes_action_iter(engine, commits, schema, physical_predicate)?;
        Ok(Some(it).into_iter().flatten())
    }

    /// Get a shared reference to the logical [`Schema`] of the table changes scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn logical_schema(&self) -> &SchemaRef {
        &self.state_info.logical_schema
    }

    /// Get a shared reference to the physical [`Schema`] of the table changes scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn physical_schema(&self) -> &SchemaRef {
        &self.state_info.physical_schema
    }

    pub fn table_root(&self) -> &Url {
        self.table_changes.table_root()
    }

    /// Get the predicate [`PredicateRef`] of the scan.
    fn physical_predicate(&self) -> Option<PredicateRef> {
        if let PhysicalPredicate::Some(ref predicate, _) = self.state_info.physical_predicate {
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
        let scan_metadata = self.scan_metadata(engine.clone())?;
        let scan_files = scan_metadata_to_scan_file(scan_metadata);

        let table_root = self.table_changes.table_root().clone();
        let state_info = self.state_info.clone();
        let dv_engine_ref = engine.clone();

        let table_root_copy = self.table_changes.table_root().clone();
        let physical_predicate = self.physical_predicate().clone();

        let result = scan_files
            .map(move |scan_file| {
                resolve_scan_file_dv(dv_engine_ref.as_ref(), &table_root, scan_file?)
            }) // Iterator-Result-Iterator
            .flatten_ok() // Iterator-Result
            .map(move |resolved_scan_file| -> DeltaResult<_> {
                read_scan_file(
                    engine.as_ref(),
                    resolved_scan_file?,
                    &table_root_copy,
                    state_info.as_ref(),
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
    table_root: &Url,
    state_info: &StateInfo,
    _physical_predicate: Option<PredicateRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
    let ResolvedCdfScanFile {
        scan_file,
        mut selection_vector,
    } = resolved_scan_file;

    let physical_schema =
        scan_file_physical_schema(&scan_file, state_info.physical_schema.as_ref());
    let transform_expr = get_cdf_transform_expr(&scan_file, state_info, physical_schema.as_ref())?;

    // Only create an evaluator if transformation is needed
    let phys_to_logical_eval = transform_expr.map(|expr| {
        engine.evaluation_handler().new_expression_evaluator(
            physical_schema.clone(),
            expr,
            state_info.logical_schema.clone().into(),
        )
    });
    // Determine if the scan file was derived from a deletion vector pair
    let is_dv_resolved_pair = scan_file.remove_dv.is_some();

    let location = table_root.join(&scan_file.path)?;
    let file = FileMeta {
        last_modified: 0,
        size: 0,
        location,
    };
    // TODO(#860): we disable predicate pushdown until we support row indexes.
    let read_result_iter =
        engine
            .parquet_handler()
            .read_parquet_files(&[file], physical_schema, None)?;

    let result = read_result_iter.map(move |batch| -> DeltaResult<_> {
        let batch = batch?;
        // Transform the physical data into the correct logical form, or pass through unchanged
        let logical = if let Some(ref eval) = phys_to_logical_eval {
            eval.evaluate(batch.as_ref())
        } else {
            // No transformation needed - pass through the batch as-is
            Ok(batch)
        };
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
    use crate::scan::PhysicalPredicate;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::TableChanges;
    use crate::table_changes::COMMIT_VERSION_COL_NAME;
    use crate::transforms::FieldTransformSpec;
    use crate::Predicate;

    #[test]
    fn simple_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).unwrap();

        let scan = table_changes.into_scan_builder().build().unwrap();

        // Check that StateInfo has been properly created with correct transforms
        // Note that this table is not partitioned. `part` is a regular field
        // Schema: part (idx 0), id (idx 1), _change_type (idx 2), _commit_version (idx 3), _commit_timestamp (idx 4)
        assert!(scan.state_info.transform_spec.is_some());
        let transform_spec = scan.state_info.transform_spec.as_ref().unwrap();

        // Should have transforms for _change_type (Dynamic), _commit_version, and _commit_timestamp
        assert_eq!(transform_spec.len(), 3);

        // Verify _change_type is handled as Dynamic at field_index 2, inserted after "id"
        assert!(transform_spec.iter().any(|t| matches!(t,
            FieldTransformSpec::DynamicColumn {
                physical_name,
                insert_after: Some(insert_after),
                field_index
            }
            if physical_name == "_change_type"
                && insert_after == "id"
                && *field_index == 2
        )));

        // Verify _commit_version at field_index 3, inserted after "id"
        assert!(transform_spec.iter().any(|t| matches!(t,
            FieldTransformSpec::MetadataDerivedColumn {
                insert_after: Some(insert_after),
                field_index
            }
            if insert_after == "id" && *field_index == 3
        )));

        // Verify _commit_timestamp at field_index 4, inserted after "id"
        assert!(transform_spec.iter().any(|t| matches!(t,
            FieldTransformSpec::MetadataDerivedColumn {
                insert_after: Some(insert_after),
                field_index
            }
            if insert_after == "id" && *field_index == 4
        )));

        // Verify predicate
        assert!(matches!(
            scan.state_info.physical_predicate,
            PhysicalPredicate::None
        ));
    }

    #[test]
    fn projected_and_filtered_table_changes_scan_builder() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).unwrap();

        let schema = table_changes
            .schema()
            .project(&["id", COMMIT_VERSION_COL_NAME])
            .unwrap();
        let predicate = Arc::new(Predicate::gt(column_expr!("id"), Scalar::from(10)));
        let scan = table_changes
            .into_scan_builder()
            .with_schema(schema.clone())
            .with_predicate(predicate.clone())
            .build()
            .unwrap();

        // Check logical schema matches projection
        assert_eq!(
            *scan.logical_schema(),
            StructType::new_unchecked([
                StructField::nullable("id", DataType::INTEGER),
                StructField::not_null("_commit_version", DataType::LONG),
            ])
            .into()
        );

        // Check physical schema only has the regular field 'id' (no CDF metadata columns)
        assert_eq!(scan.state_info.physical_schema.fields().len(), 1);
        assert_eq!(
            scan.state_info
                .physical_schema
                .field_at_index(0)
                .unwrap()
                .name(),
            "id"
        );

        // Check transform spec has the metadata column
        // Projected schema: id (idx 0), _commit_version (idx 1)
        assert!(scan.state_info.transform_spec.is_some());
        let transform_spec = scan.state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1); // Only _commit_version
        assert!(matches!(&transform_spec[0],
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after: Some(insert_after)
            }
            if *field_index == 1 && insert_after == "id"
        ));

        // Check predicate is properly set
        assert!(matches!(&scan.state_info.physical_predicate,
            PhysicalPredicate::Some(pred, pred_schema)
            if pred == &predicate && pred_schema.fields().len() == 1
        ));
    }
}
