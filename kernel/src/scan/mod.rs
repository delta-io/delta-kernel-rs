//! Functionality to create and execute scans (reads) over data stored in a delta table

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;
use url::Url;

use crate::actions::deletion_vector::{split_vector, treemap_to_bools, DeletionVectorDescriptor};
use crate::actions::{get_log_schema, ADD_NAME, REMOVE_NAME};
use crate::expressions::{Expression, Scalar};
use crate::features::ColumnMappingMode;
use crate::scan::state::{DvInfo, Stats};
use crate::schema::{DataType, Schema, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta};

use self::log_replay::scan_action_iter;
use self::state::GlobalScanState;

mod data_skipping;
pub mod log_replay;
pub mod state;

/// Builder to scan a snapshot of a table.
pub struct ScanBuilder {
    snapshot: Arc<Snapshot>,
    schema: Option<SchemaRef>,
    predicate: Option<Expression>,
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

    /// Predicates specified in this crate's [`Expression`] type.
    ///
    /// Can be used to filter the rows in a scan. For example, using the predicate
    /// `x < 4` to return a subset of the rows in the scan which satisfy the filter.
    pub fn with_predicate(mut self, predicate: Expression) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Optionally provide an [`Expression`] to filter rows. See [`ScanBuilder::with_predicate`] for
    /// details. If `predicate_opt` is `None`, this is a no-op.
    pub fn with_predicate_opt(self, predicate_opt: Option<Expression>) -> Self {
        match predicate_opt {
            Some(predicate) => self.with_predicate(predicate),
            None => self,
        }
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
        let (all_fields, read_fields, have_partition_cols) = get_state_info(
            logical_schema.as_ref(),
            &self.snapshot.metadata().partition_columns,
            self.snapshot.column_mapping_mode,
        )?;
        let physical_schema = Arc::new(StructType::new(read_fields));
        Ok(Scan {
            snapshot: self.snapshot,
            logical_schema,
            physical_schema,
            predicate: self.predicate,
            all_fields,
            have_partition_cols,
        })
    }
}

/// A vector of this type is returned from calling [`Scan::execute`]. Each [`ScanResult`] contains
/// the raw [`EngineData`] as read by the engines [`crate::ParquetHandler`], and a boolean
/// mask. Rows can be dropped from a scan due to deletion vectors, so we communicate back both
/// EngineData and information regarding whether a row should be included or not See the docs below
/// for [`ScanResult::mask`] for details on the mask.
pub struct ScanResult {
    /// Raw engine data as read from the disk for a particular file included in the query
    pub raw_data: DeltaResult<Box<dyn EngineData>>,
    /// If an item at mask\[i\] is true, the row at that row index is valid, otherwise if it is
    /// false, the row at that row index is invalid and should be ignored. If the mask is *shorter*
    /// than the number of rows returned, missing elements are considered `true`, i.e. included in
    /// the query. If this is None, all rows are valid. NB: If you are using the default engine and
    /// plan to call arrow's `filter_record_batch`, you _need_ to extend this vector to the full
    /// length of the batch or arrow will drop the extra rows
    // TODO(nick) this should be allocated by the engine
    pub mask: Option<Vec<bool>>,
}

/// Scan uses this to set up what kinds of columns it is scanning. For `Selected` we just store the
/// name of the column, as that's all that's needed during the actual query. For `Partition` we
/// store an index into the logical schema for this query since later we need the data type as well
/// to materialize the partition column.
pub enum ColumnType {
    // A column, selected from the data, as is
    Selected(String),
    // A partition column that needs to be added back in
    Partition(usize),
}

pub type ScanData = (Box<dyn EngineData>, Vec<bool>);

/// The result of building a scan over a table. This can be used to get the actual data from
/// scanning the table.
pub struct Scan {
    snapshot: Arc<Snapshot>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Option<Expression>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("schema", &self.logical_schema)
            .field("predicate", &self.predicate)
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
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
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
    pub fn scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanData>>> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_schema().project(&[ADD_NAME])?;

        let log_iter = self.snapshot.log_segment.replay(
            engine,
            commit_read_schema,
            checkpoint_read_schema,
            self.predicate.clone(),
        )?;

        Ok(scan_action_iter(
            engine,
            log_iter,
            &self.logical_schema,
            &self.predicate,
        ))
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub fn global_scan_state(&self) -> GlobalScanState {
        GlobalScanState {
            table_root: self.snapshot.table_root.to_string(),
            partition_columns: self.snapshot.metadata().partition_columns.clone(),
            logical_schema: self.logical_schema.clone(),
            read_schema: self.physical_schema.clone(),
            column_mapping_mode: self.snapshot.column_mapping_mode,
        }
    }

    /// Perform an "all in one" scan. This will use the provided `engine` to read and
    /// process all the data for the query. Each [`ScanResult`] in the resultant vector encapsulates
    /// the raw data and an optional boolean vector built from the deletion vector if it was
    /// present. See the documentation for [`ScanResult`] for more details. Generally
    /// connectors/engines will want to use [`Scan::scan_data`] so they can have more control over
    /// the execution of the scan.
    // This calls [`Scan::files`] to get a set of `Add` actions for the scan, and then uses the
    // `engine`'s [`crate::ParquetHandler`] to read the actual table data.
    pub fn execute(&self, engine: &dyn Engine) -> DeltaResult<Vec<ScanResult>> {
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
        let scan_data = self.scan_data(engine)?;
        let mut scan_files = vec![];
        for data in scan_data {
            let (data, vec) = data?;
            scan_files =
                state::visit_scan_files(data.as_ref(), &vec, scan_files, scan_data_callback)?;
        }
        scan_files
            .into_iter()
            .map(|scan_file| -> DeltaResult<_> {
                let file_path = self.snapshot.table_root.join(&scan_file.path)?;
                let mut selection_vector = scan_file
                    .dv_info
                    .get_selection_vector(engine, &self.snapshot.table_root)?;
                let meta = FileMeta {
                    last_modified: 0,
                    size: scan_file.size as usize,
                    location: file_path,
                };
                let read_result_iter = engine.get_parquet_handler().read_parquet_files(
                    &[meta],
                    global_state.read_schema.clone(),
                    None,
                )?;
                let gs = global_state.clone(); // Arc clone
                Ok(read_result_iter.into_iter().map(move |read_result| {
                    let read_result = read_result?;
                    // to transform the physical data into the correct logical form
                    let logical = transform_to_logical_internal(
                        engine,
                        read_result,
                        &gs,
                        &scan_file.partition_values,
                        &self.all_fields,
                        self.have_partition_cols,
                    );
                    let len = logical.as_ref().map_or(0, |res| res.length());
                    // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                    // will cover the following results. we `take()` out of `selection_vector` to avoid
                    // trying to return a captured variable. We're going to reassign `selection_vector`
                    // to `rest` in a moment anyway
                    let mut sv = selection_vector.take();
                    let rest = split_vector(sv.as_mut(), len, None);
                    let result = ScanResult {
                        raw_data: logical,
                        mask: sv,
                    };
                    selection_vector = rest;
                    Ok(result)
                }))
            })
            .flatten_ok()
            .try_collect()?
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

fn parse_partition_value(raw: Option<&String>, data_type: &DataType) -> DeltaResult<Scalar> {
    match raw {
        Some(v) => match data_type {
            DataType::Primitive(primitive) => primitive.parse_scalar(v),
            _ => Err(Error::generic(format!(
                "Unexpected partition column type: {data_type:?}"
            ))),
        },
        _ => Ok(Scalar::Null(data_type.clone())),
    }
}

/// Get the state needed to process a scan. In particular this returns a triple of
/// (all_fields_in_query, fields_to_read_from_parquet, have_partition_cols) where:
/// - all_fields_in_query - all fields in the query as [`ColumnType`] enums
/// - fields_to_read_from_parquet - Which fields should be read from the raw parquet files. This takes
///   into account column mapping
/// - have_partition_cols - boolean indicating if we have partition columns in this query
fn get_state_info(
    logical_schema: &Schema,
    partition_columns: &[String],
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<(Vec<ColumnType>, Vec<StructField>, bool)> {
    let mut have_partition_cols = false;
    let mut read_fields = Vec::with_capacity(logical_schema.fields.len());
    // Loop over all selected fields and note if they are columns that will be read from the
    // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
    // be filled in by evaluating an expression ([`ColumnType::Partition`])
    let column_types = logical_schema
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
                let physical_name = logical_field.physical_name(column_mapping_mode)?;
                let physical_field = logical_field.with_name(physical_name);
                read_fields.push(physical_field);
                Ok(ColumnType::Selected(physical_name.to_string()))
            }
        })
        .try_collect()?;
    Ok((column_types, read_fields, have_partition_cols))
}

pub fn selection_vector(
    engine: &dyn Engine,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Url,
) -> DeltaResult<Vec<bool>> {
    let fs_client = engine.get_file_system_client();
    let dv_treemap = descriptor.read(fs_client, table_root)?;
    Ok(treemap_to_bools(dv_treemap))
}

/// Transform the raw data read from parquet into the correct logical form, based on the provided
/// global scan state and partition values
pub fn transform_to_logical(
    engine: &dyn Engine,
    data: Box<dyn EngineData>,
    global_state: &GlobalScanState,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<Box<dyn EngineData>> {
    let (all_fields, _read_fields, have_partition_cols) = get_state_info(
        &global_state.logical_schema,
        &global_state.partition_columns,
        global_state.column_mapping_mode,
    )?;
    transform_to_logical_internal(
        engine,
        data,
        global_state,
        partition_values,
        &all_fields,
        have_partition_cols,
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
    let read_schema = global_state.read_schema.clone();
    if have_partition_cols || global_state.column_mapping_mode != ColumnMappingMode::None {
        // need to add back partition cols and/or fix-up mapped columns
        let all_fields = all_fields
            .iter()
            .map(|field| match field {
                ColumnType::Partition(field_idx) => {
                    let field = global_state
                        .logical_schema
                        .fields
                        .get_index(*field_idx)
                        .ok_or_else(|| {
                            Error::generic("logical schema did not contain expected field, can't transform data")
                        })?.1;
                    let name = field.physical_name(global_state.column_mapping_mode)?;
                    let value_expression = parse_partition_value(
                        partition_values.get(name),
                        field.data_type(),
                    )?;
                    Ok::<Expression, Error>(Expression::Literal(value_expression))
                }
                ColumnType::Selected(field_name) => Ok(Expression::column(field_name)),
            })
            .try_collect()?;
        let read_expression = Expression::Struct(all_fields);
        let result = engine
            .get_expression_handler()
            .get_evaluator(
                read_schema,
                read_expression.clone(),
                global_state.logical_schema.clone().into(),
            )
            .evaluate(data.as_ref())?;
        Ok(result)
    } else {
        Ok(data)
    }
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
        schema::{StructField, StructType},
        EngineData, JsonHandler,
    };

    use super::state::ScanCallback;

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
        let output_schema = Arc::new(get_log_schema().clone());
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
        let output_schema = Arc::new(get_log_schema().clone());
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    #[allow(clippy::vec_box)]
    pub(crate) fn run_with_validate_callback<T: Clone>(
        batch: Vec<Box<ArrowEngineData>>,
        expected_sel_vec: &[bool],
        context: T,
        validate_callback: ScanCallback<T>,
    ) {
        let engine = SyncEngine::new();
        // doesn't matter here
        let table_schema = Arc::new(StructType::new(vec![StructField::new(
            "foo",
            crate::schema::DataType::STRING,
            false,
        )]));
        let iter = scan_action_iter(
            &engine,
            batch.into_iter().map(|batch| Ok((batch as _, true))),
            &table_schema,
            &None,
        );
        let mut batch_count = 0;
        for res in iter {
            let (batch, sel) = res.unwrap();
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
    use crate::schema::PrimitiveType;
    use crate::Table;

    use super::*;

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
            let (data, vec) = data?;
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

    #[test]
    fn test_scan_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let scan = snapshot.into_scan_builder().build().unwrap();
        let files = scan.execute(&engine).unwrap();

        assert_eq!(files.len(), 1);
        let num_rows = files[0].raw_data.as_ref().unwrap().length();
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
