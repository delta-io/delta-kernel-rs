//! Functionality to create and execute scans (reads) over data stored in a delta table

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;
use itertools::Itertools;
use tracing::debug;
use url::Url;

use self::log_replay::get_scan_metadata_transform_expr;
use crate::actions::deletion_vector::{
    deletion_treemap_to_bools, split_vector, DeletionVectorDescriptor,
};
use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
use crate::engine_data::FilteredEngineData;
use crate::expressions::transforms::ExpressionTransform;
use crate::expressions::{ColumnName, ExpressionRef, Predicate, PredicateRef, Scalar};
use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, EmptyColumnResolver};
use crate::listed_log_files::ListedLogFiles;
use crate::log_replay::{ActionsBatch, HasSelectionVector};
use crate::log_segment::LogSegment;
use crate::scan::log_replay::BASE_ROW_ID_NAME;
use crate::scan::state::{DvInfo, Stats};
use crate::scan::state_info::StateInfo;
use crate::schema::{
    ArrayType, DataType, MapType, PrimitiveType, Schema, SchemaRef, SchemaTransform, StructField,
    ToSchema as _,
};
use crate::table_features::{ColumnMappingMode, Operation};
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta, SnapshotRef, Version};

use self::log_replay::scan_action_iter;

pub(crate) mod data_skipping;
pub(crate) mod field_classifiers;
pub mod log_replay;
pub mod state;
pub(crate) mod state_info;

#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
mod tests;

// safety: we define get_commit_schema() and _know_ it contains ADD_NAME and REMOVE_NAME
#[allow(clippy::unwrap_used)]
pub(crate) static COMMIT_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    get_commit_schema()
        .project(&[ADD_NAME, REMOVE_NAME])
        .unwrap()
});
// safety: we define get_commit_schema() and _know_ it contains ADD_NAME and SIDECAR_NAME
#[allow(clippy::unwrap_used)]
static CHECKPOINT_READ_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| get_commit_schema().project(&[ADD_NAME]).unwrap());

/// Builder to scan a snapshot of a table.
pub struct ScanBuilder {
    snapshot: SnapshotRef,
    schema: Option<SchemaRef>,
    predicate: Option<PredicateRef>,
}

impl std::fmt::Debug for ScanBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("ScanBuilder")
            .field("schema", &self.schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl ScanBuilder {
    /// Create a new [`ScanBuilder`] instance.
    pub fn new(snapshot: impl Into<SnapshotRef>) -> Self {
        Self {
            snapshot: snapshot.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`Snapshot`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`Snapshot`]: crate::snapshot::Snapshot
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`Snapshot`]. See
    /// [`ScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn with_schema_opt(self, schema_opt: Option<SchemaRef>) -> Self {
        match schema_opt {
            Some(schema) => self.with_schema(schema),
            None => self,
        }
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

    /// Build the [`Scan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`Scan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<Scan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self.schema.unwrap_or_else(|| self.snapshot.schema());

        self.snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Scan)?;

        let state_info = StateInfo::try_new(
            logical_schema,
            self.snapshot.table_configuration(),
            self.predicate,
            (), // No classifer, default is for scans
        )?;

        Ok(Scan {
            snapshot: self.snapshot,
            state_info: Arc::new(state_info),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PhysicalPredicate {
    Some(PredicateRef, SchemaRef),
    StaticSkipAll,
    None,
}

impl PhysicalPredicate {
    /// If we have a predicate, verify the columns it references and apply column mapping. First, get
    /// the set of references; use that to filter the schema to only the columns of interest (and
    /// verify that all referenced columns exist); then use the resulting logical/physical mappings
    /// to rewrite the expression with physical column names.
    ///
    /// NOTE: It is possible the predicate resolves to FALSE even ignoring column references,
    /// e.g. `col > 10 AND FALSE`. Such predicates can statically skip the whole query.
    pub(crate) fn try_new(
        predicate: &Predicate,
        logical_schema: &Schema,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<PhysicalPredicate> {
        if can_statically_skip_all_files(predicate) {
            return Ok(PhysicalPredicate::StaticSkipAll);
        }
        let mut get_referenced_fields = GetReferencedFields {
            unresolved_references: predicate.references(),
            column_mappings: HashMap::new(),
            logical_path: vec![],
            physical_path: vec![],
            column_mapping_mode,
        };
        let schema_opt = get_referenced_fields.transform_struct(logical_schema);
        let mut unresolved = get_referenced_fields.unresolved_references.into_iter();
        if let Some(unresolved) = unresolved.next() {
            // Schema traversal failed to resolve at least one column referenced by the predicate.
            //
            // NOTE: It's a pretty serious engine bug if we got this far with a query whose WHERE
            // clause has invalid column references. Data skipping is best-effort and the predicate
            // anyway needs to be evaluated against every row of data -- which is impossible if the
            // columns are missing/invalid. Just blow up instead of trying to handle it gracefully.
            return Err(Error::missing_column(format!(
                "Predicate references unknown column: {unresolved}"
            )));
        }
        let Some(schema) = schema_opt else {
            // The predicate doesn't statically skip all files, and it doesn't reference any columns
            // that could dynamically change its behavior, so it's useless for data skipping.
            return Ok(PhysicalPredicate::None);
        };
        let mut apply_mappings = ApplyColumnMappings {
            column_mappings: get_referenced_fields.column_mappings,
        };
        if let Some(predicate) = apply_mappings.transform_pred(predicate) {
            Ok(PhysicalPredicate::Some(
                Arc::new(predicate.into_owned()),
                Arc::new(schema.into_owned()),
            ))
        } else {
            Ok(PhysicalPredicate::None)
        }
    }
}

// Evaluates a static data skipping predicate, ignoring any column references, and returns true if
// the predicate allows to statically skip all files. Since this is direct evaluation (not an
// expression rewrite), we use a `DefaultKernelPredicateEvaluator` with an empty column resolver.
fn can_statically_skip_all_files(predicate: &Predicate) -> bool {
    use crate::kernel_predicates::KernelPredicateEvaluator as _;
    let evaluator = DefaultKernelPredicateEvaluator::from(EmptyColumnResolver);
    evaluator.eval_sql_where(predicate) == Some(false)
}

// Build the stats read schema filtering the table schema to keep only skipping-eligible
// leaf fields that the skipping expression actually references. Also extract physical name
// mappings so we can access the correct physical stats column for each logical column.
struct GetReferencedFields<'a> {
    unresolved_references: HashSet<&'a ColumnName>,
    column_mappings: HashMap<ColumnName, ColumnName>,
    logical_path: Vec<String>,
    physical_path: Vec<String>,
    column_mapping_mode: ColumnMappingMode,
}
impl<'a> SchemaTransform<'a> for GetReferencedFields<'a> {
    // Capture the path mapping for this leaf field
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        // Record the physical name mappings for all referenced leaf columns
        self.unresolved_references
            .remove(self.logical_path.as_slice())
            .then(|| {
                self.column_mappings.insert(
                    ColumnName::new(&self.logical_path),
                    ColumnName::new(&self.physical_path),
                );
                Cow::Borrowed(ptype)
            })
    }

    // array and map fields are not eligible for data skipping, so filter them out.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        let physical_name = field.physical_name(self.column_mapping_mode);
        self.logical_path.push(field.name.clone());
        self.physical_path.push(physical_name.to_string());
        let field = self.recurse_into_struct_field(field);
        self.logical_path.pop();
        self.physical_path.pop();
        Some(Cow::Owned(field?.with_name(physical_name)))
    }
}

struct ApplyColumnMappings {
    column_mappings: HashMap<ColumnName, ColumnName>,
}
impl<'a> ExpressionTransform<'a> for ApplyColumnMappings {
    // NOTE: We already verified all column references. But if the map probe ever did fail, the
    // transform would just delete any expression(s) that reference the invalid column.
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        self.column_mappings
            .get(name)
            .map(|physical_name| Cow::Owned(physical_name.clone()))
    }
}

/// utility method making it easy to get a transform for a particular row. If the requested row is
/// outside the range of the passed slice returns `None`, otherwise returns the element at the index
/// of the specified row
pub fn get_transform_for_row(
    row: usize,
    transforms: &[Option<ExpressionRef>],
) -> Option<ExpressionRef> {
    transforms.get(row).cloned().flatten()
}

/// [`ScanMetadata`] contains (1) a batch of [`FilteredEngineData`] specifying data files to be scanned
/// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
/// from those files.
pub struct ScanMetadata {
    /// Filtered engine data with one row per file to scan (and only selected rows should be scanned)
    pub scan_files: FilteredEngineData,

    /// Row-level transformations to apply to data read from files.
    ///
    /// Each entry in this vector corresponds to a row in the `scan_files` data. The entry is an
    /// optional expression that must be applied to convert the file's data into the logical schema
    /// expected by the scan:
    ///
    /// - `Some(expr)`: Apply this expression to transform the data to match
    ///   [`Scan::logical_schema()`].
    /// - `None`: No transformation is needed; the data is already in the correct logical form.
    ///
    /// Note: This vector can be indexed by row number, as rows masked by the selection vector will
    /// have corresponding entries that will be `None`.
    pub scan_file_transforms: Vec<Option<ExpressionRef>>,
}

impl ScanMetadata {
    fn try_new(
        data: Box<dyn EngineData>,
        selection_vector: Vec<bool>,
        scan_file_transforms: Vec<Option<ExpressionRef>>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            scan_files: FilteredEngineData::try_new(data, selection_vector)?,
            scan_file_transforms,
        })
    }
}

impl HasSelectionVector for ScanMetadata {
    fn has_selected_rows(&self) -> bool {
        self.scan_files.selection_vector().contains(&true)
    }
}

/// The result of building a scan over a table. This can be used to get the actual data from
/// scanning the table.
pub struct Scan {
    snapshot: SnapshotRef,
    state_info: Arc<StateInfo>,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("schema", &self.state_info.logical_schema)
            .field("predicate", &self.state_info.physical_predicate)
            .finish()
    }
}

impl Scan {
    /// The table's root URL. Any relative paths returned from `scan_data` (or in a callback from
    /// [`ScanMetadata::visit_scan_files`]) must be resolved against this root to get the actual path to
    /// the file.
    ///
    /// [`ScanMetadata::visit_scan_files`]: crate::scan::ScanMetadata::visit_scan_files
    // NOTE: this is obviously included in the snapshot, just re-exposed here for convenience.
    pub fn table_root(&self) -> &Url {
        self.snapshot.table_root()
    }

    /// Get a shared reference to the [`Snapshot`] of this scan.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn snapshot(&self) -> &SnapshotRef {
        &self.snapshot
    }

    /// Get a shared reference to the logical [`Schema`] of the scan (i.e. the output schema of the
    /// scan). Note that the logical schema can differ from the physical schema due to e.g.
    /// partition columns which are present in the logical schema but not in the physical schema.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn logical_schema(&self) -> &SchemaRef {
        &self.state_info.logical_schema
    }

    /// Get a shared reference to the physical [`Schema`] of the scan. This represents the schema
    /// of the underlying data files which must be read from storage.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn physical_schema(&self) -> &SchemaRef {
        &self.state_info.physical_schema
    }

    /// Get the predicate [`PredicateRef`] of the scan.
    pub fn physical_predicate(&self) -> Option<PredicateRef> {
        if let PhysicalPredicate::Some(ref predicate, _) = self.state_info.physical_predicate {
            Some(predicate.clone())
        } else {
            None
        }
    }

    /// Get an iterator of [`ScanMetadata`]s that should be used to facilitate a scan. This handles
    /// log-replay, reconciling Add and Remove actions, and applying data skipping (if possible).
    /// Each item in the returned iterator is a struct of:
    /// - `Box<dyn EngineData>`: Data in engine format, where each row represents a file to be
    ///   scanned. The schema for each row can be obtained by calling [`scan_row_schema`].
    /// - `Vec<bool>`: A selection vector. If a row is at index `i` and this vector is `false` at
    ///   index `i`, then that row should *not* be processed (i.e. it is filtered out). If the vector
    ///   is `true` at index `i` the row *should* be processed. If the selection vector is *shorter*
    ///   than the number of rows returned, missing elements are considered `true`, i.e. included in
    ///   the query. NB: If you are using the default engine and plan to call arrow's
    ///   `filter_record_batch`, you _need_ to extend this vector to the full length of the batch or
    ///   arrow will drop the extra rows.
    /// - `Vec<Option<Expression>>`: Transformation expressions that need to be applied. For each
    ///   row at index `i` in the above data, if an expression exists at index `i` in the `Vec`,
    ///   the associated expression _must_ be applied to the data read from the file specified by
    ///   the row. The resultant schema for this expression is guaranteed to be
    ///   [`Self::logical_schema()`]. If the item at index `i` in this `Vec` is `None`, or if the
    ///   `Vec` contains fewer than `i` elements, no expression need be applied and the data read
    ///   from disk is already in the correct logical state.
    pub fn scan_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadata>>> {
        self.scan_metadata_inner(engine, self.replay_for_scan_metadata(engine)?)
    }

    /// Get an updated iterator of [`ScanMetadata`]s based on an existing iterator of [`EngineData`]s.
    ///
    /// The existing iterator is assumed to contain data from a previous call to `scan_metadata`.
    /// Engines may decide to cache the results of `scan_metadata` to avoid additional IO operations
    /// required to replay the log.
    ///
    /// As such the new scan's predicate must "contain" the previous scan's predicate. That is, the new
    /// scan's predicate MUST skip all files the previous scan's predicate skipped. The new scan's
    /// predicate is also allowed to skip files the previous predicate kept. For example, if the previous
    /// scan predicate was
    /// ```sql
    /// WHERE a < 42 AND b = 10
    /// ```
    /// then it is legal for the new scan to use predicates such as the following:
    /// ```sql
    /// WHERE a = 30 AND b = 10
    /// WHERE a < 10 AND b = 10
    /// WHERE a < 42 AND b = 10 AND c = 20
    /// ```
    /// but it is NOT legal for the new scan to use predicates like these:
    /// ```sql
    /// WHERE a < 42
    /// WHERE a = 50 AND b = 10
    /// WHERE a < 42 AND b <= 10
    /// WHERE a < 42 OR b = 10
    /// ```
    ///
    /// <div class="warning">
    ///
    /// The current implementation does not yet validate the existing
    /// predicate against the current predicate. Until this is implemented,
    /// the caller must ensure that the existing predicate is compatible with
    /// the current predicate.
    ///
    /// </div>
    ///
    /// # Parameters
    ///
    /// * `existing_version` - Table version the provided data was read from.
    /// * `existing_data` - Existing processed scan metadata with all selection vectors applied.
    /// * `existing_predicate` - The predicate used by the previous scan.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn scan_metadata_from(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: impl IntoIterator<Item = Box<dyn EngineData>> + 'static,
        _existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ScanMetadata>>>> {
        static RESTORED_ADD_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
            use crate::scan::log_replay::DEFAULT_ROW_COMMIT_VERSION_NAME;

            let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
            DataType::struct_type_unchecked(vec![StructField::nullable(
                "add",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                    StructField::not_null("partitionValues", partition_values),
                    StructField::not_null("size", DataType::LONG),
                    StructField::nullable("modificationTime", DataType::LONG),
                    StructField::nullable("stats", DataType::STRING),
                    StructField::nullable(
                        "tags",
                        MapType::new(DataType::STRING, DataType::STRING, true),
                    ),
                    StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
                    StructField::nullable(BASE_ROW_ID_NAME, DataType::LONG),
                    StructField::nullable(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
                ]),
            )])
        });

        // TODO(#966): validate that the current predicate is compatible with the hint predicate.

        if existing_version > self.snapshot.version() {
            return Err(Error::Generic(format!(
                "existing_version {} is greater than current version {}",
                existing_version,
                self.snapshot.version()
            )));
        }

        // in order to be processed by our log replay, we must re-shape the existing scan metadata
        // back into shape as we read it from the log. Since it is already reconciled data,
        // we treat it as if it originated from a checkpoint.
        let transform = engine.evaluation_handler().new_expression_evaluator(
            scan_row_schema(),
            get_scan_metadata_transform_expr(),
            RESTORED_ADD_SCHEMA.clone(),
        )?;
        let apply_transform = move |data: Box<dyn EngineData>| {
            Ok(ActionsBatch::new(transform.evaluate(data.as_ref())?, false))
        };

        // If the snapshot version corresponds to the hint version, we process the existing data
        // to apply file skipping and provide the required transformations.
        if existing_version == self.snapshot.version() {
            let scan = existing_data.into_iter().map(apply_transform);
            return Ok(Box::new(self.scan_metadata_inner(engine, scan)?));
        }

        let log_segment = self.snapshot.log_segment();

        // If the current log segment contains a checkpoint newer than the hint version
        // we disregard the existing data hint, and perform a full scan. The current log segment
        // only has deltas after the checkpoint, so we cannot update from prior versions.
        // TODO: we may be able to apply heuristics or other logic to try and fetch missing deltas
        // from the log.
        if matches!(log_segment.checkpoint_version, Some(v) if v > existing_version) {
            return Ok(Box::new(self.scan_metadata(engine)?));
        }

        // create a new log segment containing only the commits added after the version hint.
        let mut ascending_commit_files = log_segment.ascending_commit_files.clone();
        ascending_commit_files.retain(|f| f.version > existing_version);
        let listed_log_files = ListedLogFiles {
            ascending_commit_files,
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: log_segment.latest_commit_file.clone(),
        };
        let new_log_segment = LogSegment::try_new(
            listed_log_files,
            log_segment.log_root.clone(),
            Some(log_segment.end_version),
        )?;

        let it = new_log_segment
            .read_actions_with_projected_checkpoint_actions(
                engine,
                COMMIT_READ_SCHEMA.clone(),
                CHECKPOINT_READ_SCHEMA.clone(),
                None,
            )?
            .chain(existing_data.into_iter().map(apply_transform));

        Ok(Box::new(self.scan_metadata_inner(engine, it)?))
    }

    fn scan_metadata_inner(
        &self,
        engine: &dyn Engine,
        action_batch_iter: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadata>>> {
        if let PhysicalPredicate::StaticSkipAll = self.state_info.physical_predicate {
            return Ok(None.into_iter().flatten());
        }
        let it = scan_action_iter(engine, action_batch_iter, self.state_info.clone())?;
        Ok(Some(it).into_iter().flatten())
    }

    // Factored out to facilitate testing
    fn replay_for_scan_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        // NOTE: We don't pass any meta-predicate because we expect no meaningful row group skipping
        // when ~every checkpoint file will contain the adds and removes we are looking for.
        self.snapshot
            .log_segment()
            .read_actions_with_projected_checkpoint_actions(
                engine,
                COMMIT_READ_SCHEMA.clone(),
                CHECKPOINT_READ_SCHEMA.clone(),
                None,
            )
    }

    /// Perform an "all in one" scan. This will use the provided `engine` to read and process all
    /// the data for the query. Each [`EngineData`] in the resultant iterator is a portion of the
    /// final table data. Generally connectors/engines will want to use [`Scan::scan_metadata`] so
    /// they can have more control over the execution of the scan.
    // This calls [`Scan::scan_metadata`] to get an iterator of `ScanMetadata` actions for the scan,
    // and then uses the `engine`'s [`crate::ParquetHandler`] to read the actual table data.
    pub fn execute(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>>> {
        struct ScanFile {
            path: String,
            size: i64,
            dv_info: DvInfo,
            transform: Option<ExpressionRef>,
        }
        fn scan_metadata_callback(
            batches: &mut Vec<ScanFile>,
            path: &str,
            size: i64,
            _: Option<Stats>,
            dv_info: DvInfo,
            transform: Option<ExpressionRef>,
            _: HashMap<String, String>,
        ) {
            batches.push(ScanFile {
                path: path.to_string(),
                size,
                dv_info,
                transform,
            });
        }

        debug!(
            "Executing scan with logical schema {:#?} and physical schema {:#?}",
            self.state_info.logical_schema, self.state_info.physical_schema
        );

        let table_root = self.snapshot.table_root().clone();

        let scan_metadata_iter = self.scan_metadata(engine.as_ref())?;
        let scan_files_iter = scan_metadata_iter
            .map(|res| {
                let scan_metadata = res?;
                let scan_files = vec![];
                scan_metadata.visit_scan_files(scan_files, scan_metadata_callback)
            })
            // Iterator<DeltaResult<Vec<ScanFile>>> to Iterator<DeltaResult<ScanFile>>
            .flatten_ok();

        let physical_schema = self.physical_schema().clone();
        let logical_schema = self.logical_schema().clone();
        let result = scan_files_iter
            .map(move |scan_file| -> DeltaResult<_> {
                let scan_file = scan_file?;
                let file_path = table_root.join(&scan_file.path)?;
                let mut selection_vector = scan_file
                    .dv_info
                    .get_selection_vector(engine.as_ref(), &table_root)?;
                let meta = FileMeta {
                    last_modified: 0,
                    size: scan_file.size.try_into().map_err(|_| {
                        Error::generic("Unable to convert scan file size into FileSize")
                    })?,
                    location: file_path,
                };

                // WARNING: We validated the physical predicate against a schema that includes
                // partition columns, but the read schema we use here does _NOT_ include partition
                // columns. So we cannot safely assume that all column references are valid. See
                // https://github.com/delta-io/delta-kernel-rs/issues/434 for more details.
                //
                // TODO(#860): we disable predicate pushdown until we support row indexes.
                let read_result_iter = engine.parquet_handler().read_parquet_files(
                    &[meta],
                    physical_schema.clone(),
                    None,
                )?;

                let engine = engine.clone(); // Arc clone
                let physical_schema_inner = physical_schema.clone();
                let logical_schema_inner = logical_schema.clone();
                Ok(read_result_iter.map(move |read_result| -> DeltaResult<_> {
                    let read_result = read_result?;
                    // transform the physical data into the correct logical form
                    let logical = state::transform_to_logical(
                        engine.as_ref(),
                        read_result,
                        &physical_schema_inner,
                        &logical_schema_inner,
                        scan_file.transform.clone(), // Arc clone
                    );
                    let len = logical.as_ref().map_or(0, |res| res.len());
                    // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                    // will cover the following results. we `take()` out of `selection_vector` to avoid
                    // trying to return a captured variable. We're going to reassign `selection_vector`
                    // to `rest` in a moment anyway
                    let mut sv = selection_vector.take();
                    let rest = split_vector(sv.as_mut(), len, None);
                    let result = match sv {
                        Some(sv) => logical.and_then(|data| data.apply_selection_vector(sv)),
                        None => logical,
                    };
                    selection_vector = rest;
                    result
                }))
            })
            // Iterator<DeltaResult<Iterator<DeltaResult<Box<dyn EngineData>>>>> to Iterator<DeltaResult<DeltaResult<Box<dyn EngineData>>>>
            .flatten_ok()
            // Iterator<DeltaResult<DeltaResult<Box<dyn EngineData>>>> to Iterator<DeltaResult<Box<dyn EngineData>>>
            .map(|x| x?);
        Ok(result)
    }
}

/// Get the schema that scan rows (from [`Scan::scan_metadata`]) will be returned with.
///
/// It is:
/// ```ignored
/// {
///    path: string,
///    size: long,
///    modificationTime: long,
///    stats: string,
///    deletionVector: {
///      storageType: string,
///      pathOrInlineDv: string,
///      offset: int,
///      sizeInBytes: int,
///      cardinality: long,
///    },
///    fileConstantValues: {
///      partitionValues: map<string, string>,
///      tags: map<string, string>,
///      baseRowId: long,
///      defaultRowCommitVersion: long,
///    }
/// }
/// ```
pub fn scan_row_schema() -> SchemaRef {
    log_replay::SCAN_ROW_SCHEMA.clone()
}

pub fn selection_vector(
    engine: &dyn Engine,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Url,
) -> DeltaResult<Vec<bool>> {
    let storage = engine.storage_handler();
    let dv_treemap = descriptor.read(storage, table_root)?;
    Ok(deletion_treemap_to_bools(dv_treemap))
}
