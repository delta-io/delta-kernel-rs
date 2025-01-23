//! Functionality to create and execute scans (reads) over data stored in a delta table

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;
use url::Url;

use crate::actions::deletion_vector::{
    deletion_treemap_to_bools, split_vector, DeletionVectorDescriptor,
};
use crate::actions::{get_log_add_schema, get_log_schema, ADD_NAME, REMOVE_NAME};
use crate::expressions::{ColumnName, Expression, ExpressionRef, ExpressionTransform, Scalar};
use crate::predicates::{DefaultPredicateEvaluator, EmptyColumnResolver};
use crate::scan::state::{DvInfo, Stats};
use crate::schema::{
    ArrayType, DataType, MapType, PrimitiveType, Schema, SchemaRef, SchemaTransform, StructField,
    StructType,
};
use crate::snapshot::Snapshot;
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta};

use self::log_replay::scan_action_iter;
use self::state::GlobalScanState;

pub(crate) mod data_skipping;
pub mod log_replay;
pub mod state;

/// Builder to scan a snapshot of a table.
pub struct ScanBuilder {
    snapshot: Arc<Snapshot>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
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
    pub fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
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
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
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
        let logical_schema = self
            .schema
            .unwrap_or_else(|| self.snapshot.schema().clone().into());
        let state_info = get_state_info(
            logical_schema.as_ref(),
            &self.snapshot.metadata().partition_columns,
        )?;

        let physical_predicate = match self.predicate {
            Some(predicate) => PhysicalPredicate::try_new(&predicate, &logical_schema)?,
            None => PhysicalPredicate::None,
        };

        Ok(Scan {
            snapshot: self.snapshot,
            logical_schema,
            physical_schema: Arc::new(StructType::new(state_info.read_fields)),
            physical_predicate,
            all_fields: Arc::new(state_info.all_fields),
            have_partition_cols: state_info.have_partition_cols,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PhysicalPredicate {
    Some(ExpressionRef, SchemaRef),
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
        predicate: &Expression,
        logical_schema: &Schema,
    ) -> DeltaResult<PhysicalPredicate> {
        if can_statically_skip_all_files(predicate) {
            return Ok(PhysicalPredicate::StaticSkipAll);
        }
        let mut get_referenced_fields = GetReferencedFields {
            unresolved_references: predicate.references(),
            column_mappings: HashMap::new(),
            logical_path: vec![],
            physical_path: vec![],
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
        if let Some(predicate) = apply_mappings.transform(predicate) {
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
// expression rewrite), we use a `DefaultPredicateEvaluator` with an empty column resolver.
fn can_statically_skip_all_files(predicate: &Expression) -> bool {
    use crate::predicates::PredicateEvaluator as _;
    DefaultPredicateEvaluator::from(EmptyColumnResolver).eval_sql_where(predicate) == Some(false)
}

// Build the stats read schema filtering the table schema to keep only skipping-eligible
// leaf fields that the skipping expression actually references. Also extract physical name
// mappings so we can access the correct physical stats column for each logical column.
struct GetReferencedFields<'a> {
    unresolved_references: HashSet<&'a ColumnName>,
    column_mappings: HashMap<ColumnName, ColumnName>,
    logical_path: Vec<String>,
    physical_path: Vec<String>,
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
        let physical_name = field.physical_name();
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
    fn transform_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        self.column_mappings
            .get(name)
            .map(|physical_name| Cow::Owned(physical_name.clone()))
    }
}

/// A vector of this type is returned from calling [`Scan::execute`]. Each [`ScanResult`] contains
/// the raw [`EngineData`] as read by the engines [`crate::ParquetHandler`], and a boolean
/// mask. Rows can be dropped from a scan due to deletion vectors, so we communicate back both
/// EngineData and information regarding whether a row should be included or not (via an internal
/// mask). See the docs below for [`ScanResult::full_mask`] for details on the mask.
pub struct ScanResult {
    /// Raw engine data as read from the disk for a particular file included in the query. Note
    /// that this data may include data that should be filtered out based on the mask given by
    /// [`full_mask`].
    ///
    /// [`full_mask`]: #method.full_mask
    pub raw_data: DeltaResult<Box<dyn EngineData>>,
    /// Raw row mask.
    // TODO(nick) this should be allocated by the engine
    pub(crate) raw_mask: Option<Vec<bool>>,
}

impl ScanResult {
    /// Returns the raw row mask. If an item at `raw_mask()[i]` is true, row `i` is
    /// valid. Otherwise, row `i` is invalid and should be ignored.
    ///
    /// The raw mask is dangerous to use because it may be shorter than expected. In particular, if
    /// you are using the default engine and plan to call arrow's `filter_record_batch`, you _need_
    /// to extend the mask to the full length of the batch or arrow will drop the extra
    /// rows. Calling [`full_mask`] instead avoids this risk entirely, at the cost of a copy.
    ///
    /// [`full_mask`]: #method.full_mask
    pub fn raw_mask(&self) -> Option<&Vec<bool>> {
        self.raw_mask.as_ref()
    }

    /// Extends the underlying (raw) mask to match the row count of the accompanying data.
    ///
    /// If the raw mask is *shorter* than the number of rows returned, missing elements are
    /// considered `true`, i.e. included in the query. If the mask is `None`, all rows are valid.
    ///
    /// NB: If you are using the default engine and plan to call arrow's `filter_record_batch`, you
    /// _need_ to extend the mask to the full length of the batch or arrow will drop the extra rows.
    pub fn full_mask(&self) -> Option<Vec<bool>> {
        let mut mask = self.raw_mask.clone()?;
        mask.resize(self.raw_data.as_ref().ok()?.len(), true);
        Some(mask)
    }
}

/// Scan uses this to set up what kinds of top-level columns it is scanning. For `Selected` we just
/// store the name of the column, as that's all that's needed during the actual query. For
/// `Partition` we store an index into the logical schema for this query since later we need the
/// data type as well to materialize the partition column.
#[derive(PartialEq, Debug)]
pub enum ColumnType {
    // A column, selected from the data, as is
    Selected(String),
    // A partition column that needs to be added back in
    Partition(usize),
}

/// A transform is ultimately a `Struct` expr. This holds the set of expressions that make that struct expr up
type Transform = Vec<TransformExpr>;

/// Transforms aren't computed all at once. So static ones can just go straight to `Expression`, but
/// things like partition columns need to filled in. This enum holds an expression that's part of a
/// `Transform`.
pub(crate) enum TransformExpr {
    Static(Expression),
    Partition(usize),
}

// TODO(nick): Make this a struct in a follow-on PR
// (data, deletion_vec, transforms)
pub type ScanData = (Box<dyn EngineData>, Vec<bool>, Vec<Option<ExpressionRef>>);

/// The result of building a scan over a table. This can be used to get the actual data from
/// scanning the table.
pub struct Scan {
    snapshot: Arc<Snapshot>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    physical_predicate: PhysicalPredicate,
    all_fields: Arc<Vec<ColumnType>>,
    have_partition_cols: bool,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("schema", &self.logical_schema)
            .field("predicate", &self.physical_predicate)
            .finish()
    }
}

impl Scan {
    /// Get a shared reference to the [`Schema`] of the scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn physical_predicate(&self) -> Option<ExpressionRef> {
        if let PhysicalPredicate::Some(ref predicate, _) = self.physical_predicate {
            Some(predicate.clone())
        } else {
            None
        }
    }

    /// Convert the parts of the transform that can be computed statically into `Expression`s. For
    /// parts that cannot be computed statically, include enough metadata so lower levels of
    /// processing can create and fill in an expression.
    fn get_static_transform(all_fields: &[ColumnType]) -> Transform {
        all_fields
            .iter()
            .map(|field| match field {
                ColumnType::Selected(col_name) => {
                    TransformExpr::Static(ColumnName::new([col_name]).into())
                }
                ColumnType::Partition(idx) => TransformExpr::Partition(*idx),
            })
            .collect()
    }

    /// Get an iterator of [`EngineData`]s that should be included in scan for a query. This handles
    /// log-replay, reconciling Add and Remove actions, and applying data skipping (if
    /// possible). Each item in the returned iterator is a tuple of:
    /// - `Box<dyn EngineData>`: Data in engine format, where each row represents a file to be
    ///   scanned. The schema for each row can be obtained by calling [`scan_row_schema`].
    /// - `Vec<bool>`: A selection vector. If a row is at index `i` and this vector is `false` at
    ///   index `i`, then that row should *not* be processed (i.e. it is filtered out). If the vector
    ///   is `true` at index `i` the row *should* be processed. If the selector vector is *shorter*
    ///   than the number of rows returned, missing elements are considered `true`, i.e. included in
    ///   the query. NB: If you are using the default engine and plan to call arrow's
    ///   `filter_record_batch`, you _need_ to extend this vector to the full length of the batch or
    ///   arrow will drop the extra rows.
    /// - `Vec<Option<Expression>>`: Transformation expressions that need to be applied. For each
    ///    row at index `i` in the above data, if an expression exists at index `i` in the `Vec`,
    ///    the associated expression _must_ be applied to the data read from the file specified by
    ///    the row. The resultant schema for this expression is guaranteed to be `Scan.schema()`. If
    ///    the item at index `i` in this `Vec` is `None`, or if the `Vec` contains fewer than `i`
    ///    elements, no expression need be applied and the data read from disk is already in the
    ///    correct logical state.
    pub fn scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanData>>> {
        // Compute the static part of the transformation. This is `None` if no transformation is
        // needed (currently just means no partition cols AND no column mapping but will be extended
        // for other transforms as we support them)
        let static_transform = (self.have_partition_cols
            || self.snapshot.column_mapping_mode != ColumnMappingMode::None)
            .then_some(Arc::new(Scan::get_static_transform(&self.all_fields)));
        let physical_predicate = match self.physical_predicate.clone() {
            PhysicalPredicate::StaticSkipAll => return Ok(None.into_iter().flatten()),
            PhysicalPredicate::Some(predicate, schema) => Some((predicate, schema)),
            PhysicalPredicate::None => None,
        };
        let it = scan_action_iter(
            engine,
            self.replay_for_scan_data(engine)?,
            self.logical_schema.clone(),
            static_transform,
            physical_predicate,
        );
        Ok(Some(it).into_iter().flatten())
    }

    // Factored out to facilitate testing
    fn replay_for_scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_add_schema().clone();

        // NOTE: We don't pass any meta-predicate because we expect no meaningful row group skipping
        // when ~every checkpoint file will contain the adds and removes we are looking for.
        self.snapshot
            .log_segment
            .replay(engine, commit_read_schema, checkpoint_read_schema, None)
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub fn global_scan_state(&self) -> GlobalScanState {
        GlobalScanState {
            table_root: self.snapshot.table_root.to_string(),
            partition_columns: self.snapshot.metadata().partition_columns.clone(),
            logical_schema: self.logical_schema.clone(),
            physical_schema: self.physical_schema.clone(),
            column_mapping_mode: self.snapshot.column_mapping_mode,
        }
    }

    /// Perform an "all in one" scan. This will use the provided `engine` to read and
    /// process all the data for the query. Each [`ScanResult`] in the resultant iterator encapsulates
    /// the raw data and an optional boolean vector built from the deletion vector if it was
    /// present. See the documentation for [`ScanResult`] for more details. Generally
    /// connectors/engines will want to use [`Scan::scan_data`] so they can have more control over
    /// the execution of the scan.
    // This calls [`Scan::scan_data`] to get an iterator of `ScanData` actions for the scan, and then uses the
    // `engine`'s [`crate::ParquetHandler`] to read the actual table data.
    pub fn execute(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
        struct ScanFile {
            path: String,
            size: i64,
            dv_info: DvInfo,
            partition_values: HashMap<String, String>,
        }
        fn scan_data_callback(
            batches: &mut Vec<ScanFile>,
            path: &str,
            size: i64,
            _: Option<Stats>,
            dv_info: DvInfo,
            partition_values: HashMap<String, String>,
        ) {
            batches.push(ScanFile {
                path: path.to_string(),
                size,
                dv_info,
                partition_values,
            });
        }

        debug!(
            "Executing scan with logical schema {:#?} and physical schema {:#?}",
            self.logical_schema, self.physical_schema
        );

        let global_state = Arc::new(self.global_scan_state());
        let table_root = self.snapshot.table_root.clone();
        let physical_predicate = self.physical_predicate();
        let all_fields = self.all_fields.clone();
        let have_partition_cols = self.have_partition_cols;

        let scan_data = self.scan_data(engine.as_ref())?;
        let scan_files_iter = scan_data
            .map(|res| {
                let (data, vec, _transforms) = res?;
                let scan_files = vec![];
                state::visit_scan_files(data.as_ref(), &vec, scan_files, scan_data_callback)
            })
            // Iterator<DeltaResult<Vec<ScanFile>>> to Iterator<DeltaResult<ScanFile>>
            .flatten_ok();

        let result = scan_files_iter
            .map(move |scan_file| -> DeltaResult<_> {
                let scan_file = scan_file?;
                let file_path = table_root.join(&scan_file.path)?;
                let mut selection_vector = scan_file
                    .dv_info
                    .get_selection_vector(engine.as_ref(), &table_root)?;
                let meta = FileMeta {
                    last_modified: 0,
                    size: scan_file.size as usize,
                    location: file_path,
                };

                // WARNING: We validated the physical predicate against a schema that includes
                // partition columns, but the read schema we use here does _NOT_ include partition
                // columns. So we cannot safely assume that all column references are valid. See
                // https://github.com/delta-io/delta-kernel-rs/issues/434 for more details.
                let read_result_iter = engine.get_parquet_handler().read_parquet_files(
                    &[meta],
                    global_state.physical_schema.clone(),
                    physical_predicate.clone(),
                )?;

                // Arc clones
                let engine = engine.clone();
                let global_state = global_state.clone();
                let all_fields = all_fields.clone();
                Ok(read_result_iter.map(move |read_result| -> DeltaResult<_> {
                    let read_result = read_result?;
                    // to transform the physical data into the correct logical form
                    let logical = transform_to_logical_internal(
                        engine.as_ref(),
                        read_result,
                        &global_state,
                        &scan_file.partition_values,
                        &all_fields,
                        have_partition_cols,
                    );
                    let len = logical.as_ref().map_or(0, |res| res.len());
                    // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                    // will cover the following results. we `take()` out of `selection_vector` to avoid
                    // trying to return a captured variable. We're going to reassign `selection_vector`
                    // to `rest` in a moment anyway
                    let mut sv = selection_vector.take();
                    let rest = split_vector(sv.as_mut(), len, None);
                    let result = ScanResult {
                        raw_data: logical,
                        raw_mask: sv,
                    };
                    selection_vector = rest;
                    Ok(result)
                }))
            })
            // Iterator<DeltaResult<Iterator<DeltaResult<ScanResult>>>> to Iterator<DeltaResult<DeltaResult<ScanResult>>>
            .flatten_ok()
            // Iterator<DeltaResult<DeltaResult<ScanResult>>> to Iterator<DeltaResult<ScanResult>>
            .map(|x| x?);
        Ok(result)
    }
}

/// Get the schema that scan rows (from [`Scan::scan_data`]) will be returned with.
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
///      partitionValues: map<string, string>
///    }
/// }
/// ```
pub fn scan_row_schema() -> Schema {
    log_replay::SCAN_ROW_SCHEMA.as_ref().clone()
}

pub(crate) fn parse_partition_value(
    raw: Option<&String>,
    data_type: &DataType,
) -> DeltaResult<Scalar> {
    match (raw, data_type.as_primitive_opt()) {
        (Some(v), Some(primitive)) => primitive.parse_scalar(v),
        (Some(_), None) => Err(Error::generic(format!(
            "Unexpected partition column type: {data_type:?}"
        ))),
        _ => Ok(Scalar::Null(data_type.clone())),
    }
}

/// All the state needed to process a scan.
struct StateInfo {
    /// All fields referenced by the query.
    all_fields: Vec<ColumnType>,
    /// The physical (parquet) read schema to use.
    read_fields: Vec<StructField>,
    /// True if this query references any partition columns.
    have_partition_cols: bool,
}

/// Get the state needed to process a scan, see [`StateInfo`] for details.
fn get_state_info(logical_schema: &Schema, partition_columns: &[String]) -> DeltaResult<StateInfo> {
    let mut have_partition_cols = false;
    let mut read_fields = Vec::with_capacity(logical_schema.fields.len());
    // Loop over all selected fields and note if they are columns that will be read from the
    // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
    // be filled in by evaluating an expression ([`ColumnType::Partition`])
    let all_fields = logical_schema
        .fields()
        .enumerate()
        .map(|(index, logical_field)| -> DeltaResult<_> {
            if partition_columns.contains(logical_field.name()) {
                // Store the index into the schema for this field. When we turn it into an
                // expression in the inner loop, we will index into the schema and get the name and
                // data type, which we need to properly materialize the column.
                have_partition_cols = true;
                Ok(ColumnType::Partition(index))
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
    Ok(StateInfo {
        all_fields,
        read_fields,
        have_partition_cols,
    })
}

pub fn selection_vector(
    engine: &dyn Engine,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Url,
) -> DeltaResult<Vec<bool>> {
    let fs_client = engine.get_file_system_client();
    let dv_treemap = descriptor.read(fs_client, table_root)?;
    Ok(deletion_treemap_to_bools(dv_treemap))
}

/// Transform the raw data read from parquet into the correct logical form, based on the provided
/// global scan state and partition values
pub fn transform_to_logical(
    engine: &dyn Engine,
    data: Box<dyn EngineData>,
    global_state: &GlobalScanState,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<Box<dyn EngineData>> {
    let state_info = get_state_info(
        &global_state.logical_schema,
        &global_state.partition_columns,
    )?;
    transform_to_logical_internal(
        engine,
        data,
        global_state,
        partition_values,
        &state_info.all_fields,
        state_info.have_partition_cols,
    )
}

// We have this function because `execute` can save `all_fields` and `have_partition_cols` in the
// scan, and then reuse them for each batch transform
fn transform_to_logical_internal(
    engine: &dyn Engine,
    data: Box<dyn EngineData>,
    global_state: &GlobalScanState,
    partition_values: &std::collections::HashMap<String, String>,
    all_fields: &[ColumnType],
    have_partition_cols: bool,
) -> DeltaResult<Box<dyn EngineData>> {
    let physical_schema = global_state.physical_schema.clone();
    if !have_partition_cols && global_state.column_mapping_mode == ColumnMappingMode::None {
        return Ok(data);
    }
    // need to add back partition cols and/or fix-up mapped columns
    let all_fields = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = global_state.logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name();
                let value_expression =
                    parse_partition_value(partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) => Ok(ColumnName::new([field_name]).into()),
        })
        .try_collect()?;
    let read_expression = Expression::Struct(all_fields);
    let result = engine
        .get_expression_handler()
        .get_evaluator(
            physical_schema,
            read_expression,
            global_state.logical_schema.clone().into(),
        )
        .evaluate(data.as_ref())?;
    Ok(result)
}

// some utils that are used in file_stream.rs and state.rs tests
#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::{
        actions::get_log_schema,
        engine::{
            arrow_data::ArrowEngineData,
            sync::{json::SyncJsonHandler, SyncEngine},
        },
        scan::log_replay::scan_action_iter,
        schema::SchemaRef,
        EngineData, JsonHandler,
    };

    use super::{state::ScanCallback, Transform};

    // TODO(nick): Merge all copies of this into one "test utils" thing
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    // simple add
    pub(crate) fn add_batch_simple() -> Box<ArrowEngineData> {
        let handler = SyncJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = get_log_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    // add batch with a removed file
    pub(crate) fn add_batch_with_remove() -> Box<ArrowEngineData> {
        let handler = SyncJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"remove":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","deletionTimestamp":1677811194426,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":635,"tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = get_log_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    // add batch with a `date` partition col
    pub(crate) fn add_batch_with_partition_col() -> Box<ArrowEngineData> {
        let handler = SyncJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c001.snappy.parquet","partitionValues": {"date": "2017-12-11"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        ]
        .into();
        let output_schema = get_log_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    /// Create a scan action iter and validate what's called back. If you pass `None` as
    /// `logical_schema`, `transform` should also be `None`
    #[allow(clippy::vec_box)]
    pub(crate) fn run_with_validate_callback<T: Clone>(
        batch: Vec<Box<ArrowEngineData>>,
        logical_schema: Option<SchemaRef>,
        transform: Option<Arc<Transform>>,
        expected_sel_vec: &[bool],
        context: T,
        validate_callback: ScanCallback<T>,
    ) {
        let logical_schema =
            logical_schema.unwrap_or_else(|| Arc::new(crate::schema::StructType::new(vec![])));
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch.into_iter().map(|batch| Ok((batch as _, true))),
            logical_schema,
            transform,
            None,
        );
        let mut batch_count = 0;
        for res in iter {
            let (batch, sel, _transforms) = res.unwrap();
            assert_eq!(sel, expected_sel_vec);
            crate::scan::state::visit_scan_files(
                batch.as_ref(),
                &sel,
                context.clone(),
                validate_callback,
            )
            .unwrap();
            batch_count += 1;
        }
        assert_eq!(batch_count, 1);
    }
}

#[cfg(all(test, feature = "sync-engine"))]
mod tests {
    use std::path::PathBuf;

    use crate::engine::sync::SyncEngine;
    use crate::expressions::column_expr;
    use crate::schema::{ColumnMetadataKey, PrimitiveType};
    use crate::Table;

    use super::*;

    #[test]
    fn test_static_skipping() {
        const NULL: Expression = Expression::null_literal(DataType::BOOLEAN);
        let test_cases = [
            (false, column_expr!("a")),
            (true, Expression::literal(false)),
            (false, Expression::literal(true)),
            (true, NULL),
            (true, Expression::and(column_expr!("a"), false)),
            (false, Expression::or(column_expr!("a"), true)),
            (false, Expression::or(column_expr!("a"), false)),
            (false, Expression::lt(column_expr!("a"), 10)),
            (false, Expression::lt(Expression::literal(10), 100)),
            (true, Expression::gt(Expression::literal(10), 100)),
            (true, Expression::and(NULL, column_expr!("a"))),
        ];
        for (should_skip, predicate) in test_cases {
            assert_eq!(
                can_statically_skip_all_files(&predicate),
                should_skip,
                "Failed for predicate: {:#?}",
                predicate
            );
        }
    }

    #[test]
    fn test_physical_predicate() {
        let logical_schema = StructType::new(vec![
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG).with_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                "phys_b",
            )]),
            StructField::nullable("phys_b", DataType::LONG).with_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                "phys_c",
            )]),
            StructField::nullable(
                "nested",
                StructType::new(vec![
                    StructField::nullable("x", DataType::LONG),
                    StructField::nullable("y", DataType::LONG).with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_y",
                    )]),
                ]),
            ),
            StructField::nullable(
                "mapped",
                StructType::new(vec![StructField::nullable("n", DataType::LONG)
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_n",
                    )])]),
            )
            .with_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                "phys_mapped",
            )]),
        ]);

        // NOTE: We break several column mapping rules here because they don't matter for this
        // test. For example, we do not provide field ids, and not all columns have physical names.
        let test_cases = [
            (Expression::literal(true), Some(PhysicalPredicate::None)),
            (
                Expression::literal(false),
                Some(PhysicalPredicate::StaticSkipAll),
            ),
            (column_expr!("x"), None), // no such column
            (
                column_expr!("a"),
                Some(PhysicalPredicate::Some(
                    column_expr!("a").into(),
                    StructType::new(vec![StructField::nullable("a", DataType::LONG)]).into(),
                )),
            ),
            (
                column_expr!("b"),
                Some(PhysicalPredicate::Some(
                    column_expr!("phys_b").into(),
                    StructType::new(vec![StructField::nullable("phys_b", DataType::LONG)
                        .with_metadata([(
                            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                            "phys_b",
                        )])])
                    .into(),
                )),
            ),
            (
                column_expr!("nested.x"),
                Some(PhysicalPredicate::Some(
                    column_expr!("nested.x").into(),
                    StructType::new(vec![StructField::nullable(
                        "nested",
                        StructType::new(vec![StructField::nullable("x", DataType::LONG)]),
                    )])
                    .into(),
                )),
            ),
            (
                column_expr!("nested.y"),
                Some(PhysicalPredicate::Some(
                    column_expr!("nested.phys_y").into(),
                    StructType::new(vec![StructField::nullable(
                        "nested",
                        StructType::new(vec![StructField::nullable("phys_y", DataType::LONG)
                            .with_metadata([(
                                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                                "phys_y",
                            )])]),
                    )])
                    .into(),
                )),
            ),
            (
                column_expr!("mapped.n"),
                Some(PhysicalPredicate::Some(
                    column_expr!("phys_mapped.phys_n").into(),
                    StructType::new(vec![StructField::nullable(
                        "phys_mapped",
                        StructType::new(vec![StructField::nullable("phys_n", DataType::LONG)
                            .with_metadata([(
                                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                                "phys_n",
                            )])]),
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_mapped",
                    )])])
                    .into(),
                )),
            ),
            (
                Expression::and(column_expr!("mapped.n"), true),
                Some(PhysicalPredicate::Some(
                    Expression::and(column_expr!("phys_mapped.phys_n"), true).into(),
                    StructType::new(vec![StructField::nullable(
                        "phys_mapped",
                        StructType::new(vec![StructField::nullable("phys_n", DataType::LONG)
                            .with_metadata([(
                                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                                "phys_n",
                            )])]),
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_mapped",
                    )])])
                    .into(),
                )),
            ),
            (
                Expression::and(column_expr!("mapped.n"), false),
                Some(PhysicalPredicate::StaticSkipAll),
            ),
        ];

        for (predicate, expected) in test_cases {
            let result = PhysicalPredicate::try_new(&predicate, &logical_schema).ok();
            assert_eq!(
                result, expected,
                "Failed for predicate: {:#?}, expected {:#?}, got {:#?}",
                predicate, expected, result
            );
        }
    }

    fn get_files_for_scan(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
        let scan_data = scan.scan_data(engine)?;
        fn scan_data_callback(
            paths: &mut Vec<String>,
            path: &str,
            _size: i64,
            _: Option<Stats>,
            dv_info: DvInfo,
            _partition_values: HashMap<String, String>,
        ) {
            paths.push(path.to_string());
            assert!(dv_info.deletion_vector.is_none());
        }
        let mut files = vec![];
        for data in scan_data {
            let (data, vec, _transforms) = data?;
            files = state::visit_scan_files(data.as_ref(), &vec, files, scan_data_callback)?;
        }
        Ok(files)
    }

    #[test]
    fn test_scan_data_paths() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let scan = snapshot.into_scan_builder().build().unwrap();
        let files = get_files_for_scan(scan, &engine).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0],
            "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"
        );
    }

    #[test_log::test]
    fn test_scan_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = Arc::new(SyncEngine::new());

        let table = Table::new(url);
        let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
        let scan = snapshot.into_scan_builder().build().unwrap();
        let files: Vec<ScanResult> = scan.execute(engine).unwrap().try_collect().unwrap();

        assert_eq!(files.len(), 1);
        let num_rows = files[0].raw_data.as_ref().unwrap().len();
        assert_eq!(num_rows, 10)
    }

    #[test]
    fn test_get_partition_value() {
        let cases = [
            (
                "string",
                PrimitiveType::String,
                Scalar::String("string".to_string()),
            ),
            ("123", PrimitiveType::Integer, Scalar::Integer(123)),
            ("1234", PrimitiveType::Long, Scalar::Long(1234)),
            ("12", PrimitiveType::Short, Scalar::Short(12)),
            ("1", PrimitiveType::Byte, Scalar::Byte(1)),
            ("1.1", PrimitiveType::Float, Scalar::Float(1.1)),
            ("10.10", PrimitiveType::Double, Scalar::Double(10.1)),
            ("true", PrimitiveType::Boolean, Scalar::Boolean(true)),
            ("2024-01-01", PrimitiveType::Date, Scalar::Date(19723)),
            ("1970-01-01", PrimitiveType::Date, Scalar::Date(0)),
            (
                "1970-01-01 00:00:00",
                PrimitiveType::Timestamp,
                Scalar::Timestamp(0),
            ),
            (
                "1970-01-01 00:00:00.123456",
                PrimitiveType::Timestamp,
                Scalar::Timestamp(123456),
            ),
            (
                "1970-01-01 00:00:00.123456789",
                PrimitiveType::Timestamp,
                Scalar::Timestamp(123456),
            ),
        ];

        for (raw, data_type, expected) in &cases {
            let value = parse_partition_value(
                Some(&raw.to_string()),
                &DataType::Primitive(data_type.clone()),
            )
            .unwrap();
            assert_eq!(value, *expected);
        }
    }

    #[test]
    fn test_replay_for_scan_data() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let scan = snapshot.into_scan_builder().build().unwrap();
        let data: Vec<_> = scan
            .replay_for_scan_data(&engine)
            .unwrap()
            .try_collect()
            .unwrap();
        // No predicate pushdown attempted, because at most one part of a multi-part checkpoint
        // could be skipped when looking for adds/removes.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        assert_eq!(data.len(), 5);
    }

    #[test]
    fn test_data_row_group_skipping() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = Arc::new(SyncEngine::new());

        let table = Table::new(url);
        let snapshot = Arc::new(table.snapshot(engine.as_ref(), None).unwrap());

        // No predicate pushdown attempted, so the one data file should be returned.
        //
        // NOTE: The data file contains only five rows -- near guaranteed to produce one row group.
        let scan = snapshot.clone().scan_builder().build().unwrap();
        let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
        assert_eq!(data.len(), 1);

        // Ineffective predicate pushdown attempted, so the one data file should be returned.
        let int_col = column_expr!("numeric.ints.int32");
        let value = Expression::literal(1000i32);
        let predicate = Arc::new(int_col.clone().gt(value.clone()));
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .unwrap();
        let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
        assert_eq!(data.len(), 1);

        // Effective predicate pushdown, so no data files should be returned.
        let predicate = Arc::new(int_col.lt(value));
        let scan = snapshot
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .unwrap();
        let data: Vec<_> = scan.execute(engine).unwrap().try_collect().unwrap();
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn test_missing_column_row_group_skipping() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = Arc::new(SyncEngine::new());

        let table = Table::new(url);
        let snapshot = Arc::new(table.snapshot(engine.as_ref(), None).unwrap());

        // Predicate over a logically valid but physically missing column. No data files should be
        // returned because the column is inferred to be all-null.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 - This
        // optimization is currently disabled, so the one data file is still returned.
        let predicate = Arc::new(column_expr!("missing").lt(1000i64));
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .unwrap();
        let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
        assert_eq!(data.len(), 1);

        // Predicate over a logically missing column fails the scan
        let predicate = Arc::new(column_expr!("numeric.ints.invalid").lt(1000));
        snapshot
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .expect_err("unknown column");
    }

    #[test_log::test]
    fn test_scan_with_checkpoint() -> DeltaResult<()> {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))?;

        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None)?;
        let scan = snapshot.into_scan_builder().build()?;
        let files = get_files_for_scan(scan, &engine)?;
        // test case:
        //
        // commit0:     P and M, no add/remove
        // commit1:     add file-ad1
        // commit2:     remove file-ad1, add file-a19
        // checkpoint2: remove file-ad1, add file-a19
        // commit3:     remove file-a19, add file-70b
        //
        // thus replay should produce only file-70b
        assert_eq!(
            files,
            vec!["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"]
        );
        Ok(())
    }
}
