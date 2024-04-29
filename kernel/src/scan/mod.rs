//! Functionality to create and execute scans (reads) over data stored in a delta table

use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;
use url::Url;

use self::file_stream::{log_replay_iter, scan_action_iter};
use self::state::GlobalScanState;
use crate::actions::deletion_vector::{treemap_to_bools, DeletionVectorDescriptor};
use crate::actions::{get_log_schema, Add, ADD_NAME, REMOVE_NAME};
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, Schema, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, EngineData, EngineInterface, Error, FileMeta};

mod data_skipping;
pub mod file_stream;
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
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
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
    /// [`ScanBuilder::with_schema`] for details. If schema_opt is `None` this is a no-op.
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

    /// Build the [`Scan`].
    ///
    /// This is lazy and performs no 'work' at this point. The [`Scan`] type itself can be used
    /// to fetch the files and associated metadata required to perform actual data reads.
    pub fn build(self) -> Scan {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let read_schema = self
            .schema
            .unwrap_or_else(|| self.snapshot.schema().clone().into());
        Scan {
            snapshot: self.snapshot,
            read_schema,
            predicate: self.predicate,
        }
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
    /// false, the row at that row index is invalid and should be ignored. If this is None, all rows
    /// are valid.
    // TODO(nick) this should be allocated by the engine
    pub mask: Option<Vec<bool>>,
}

/// Scan uses this to set up what kinds of columns it is scanning. A reference to the field is
/// stored so we can get the name and data_type out when building the read expression
pub enum ColumnType<'a> {
    Selected(&'a StructField),
    Partition(&'a StructField),
}

pub type ScanData = (Box<dyn EngineData>, Vec<bool>);

/// The result of building a scan over a table. This can be used to get the actual data from
/// scanning the table.
pub struct Scan {
    snapshot: Arc<Snapshot>,
    read_schema: SchemaRef,
    predicate: Option<Expression>,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("schema", &self.read_schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl Scan {
    /// Get a shared reference to the [`Schema`] of the scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.read_schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
    }

    /// Get an iterator of Add actions that should be included in scan for a query. This handles
    /// log-replay, reconciling Add and Remove actions, and applying data skipping (if possible)
    pub(crate) fn files(
        &self,
        engine_interface: &dyn EngineInterface,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>>> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_schema().project(&[ADD_NAME])?;

        let log_iter = self.snapshot.log_segment.replay(
            engine_interface,
            commit_read_schema,
            checkpoint_read_schema,
            self.predicate.clone(),
        )?;

        Ok(log_replay_iter(
            engine_interface,
            log_iter,
            &self.read_schema,
            &self.predicate,
        ))
    }

    /// Get an iterator of [`EngineData`]s that should be included in scan for a query. This handles
    /// log-replay, reconciling Add and Remove actions, and applying data skipping (if
    /// possible). Each item in the returned iterator is a tuple of:
    /// - `Box<dyn EngineData>`: Data in engine format, where each row represents a file to be
    /// scanned. The schema for each row can be obtained by calling [`scan_row_schema`].
    /// - `Vec<bool>`: A selection vector. If a row is at index `i` and this vector is `false` at
    /// index `i`, then that row should *not* be processed (i.e. it is filtered out). If the vector
    /// is `true` at index `i` the row *should* be processed.
    pub fn scan_data(
        &self,
        engine_interface: &dyn EngineInterface,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanData>>> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_schema().project(&[ADD_NAME])?;

        let log_iter = self.snapshot.log_segment.replay(
            engine_interface,
            commit_read_schema,
            checkpoint_read_schema,
            self.predicate.clone(),
        )?;

        Ok(scan_action_iter(
            engine_interface,
            log_iter,
            &self.read_schema,
            &self.predicate,
        ))
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub fn global_scan_state(&self) -> GlobalScanState {
        let partition_columns = &self.snapshot.metadata().partition_columns;
        let (_all_fields, read_fields, _have_partition_cols) =
            get_state_info(self.schema().as_ref(), partition_columns);
        let read_schema = StructType::new(read_fields);
        let logical_schema = self.schema().as_ref().clone();
        GlobalScanState {
            table_root: self.snapshot.table_root.to_string(),
            partition_columns: partition_columns.clone(),
            logical_schema,
            read_schema,
        }
    }

    /// Perform an "all in one" scan. This will use the provided `engine_interface` to read and
    /// process all the data for the query. Each [`ScanResult`] in the resultant vector encapsulates
    /// the raw data and an optional boolean vector built from the deletion vector if it was
    /// present. See the documentation for [`ScanResult`] for more details. Generally
    /// connectors/engines will want to use [`Scan::scan_data`] so they can have more control over
    /// the execution of the scan.
    // This calls [`Scan::files`] to get a set of `Add` actions for the scan, and then uses the
    // `engine_interface`'s [`crate::ParquetHandler`] to read the actual table data.
    pub fn execute(&self, engine_interface: &dyn EngineInterface) -> DeltaResult<Vec<ScanResult>> {
        let partition_columns = &self.snapshot.metadata().partition_columns;
        let (all_fields, read_fields, have_partition_cols) =
            get_state_info(self.schema().as_ref(), partition_columns);
        let read_schema = Arc::new(StructType::new(read_fields));
        debug!("Executing scan with read schema {read_schema:#?}");
        let output_schema = DataType::Struct(Box::new(self.schema().as_ref().clone()));
        let parquet_handler = engine_interface.get_parquet_handler();

        let mut results: Vec<ScanResult> = vec![];
        let files = self.files(engine_interface)?;
        for add_result in files {
            let add = add_result?;
            let meta = FileMeta {
                last_modified: add.modification_time,
                size: add.size as usize,
                location: self.snapshot.table_root.join(&add.path)?,
            };

            let read_results =
                parquet_handler.read_parquet_files(&[meta], read_schema.clone(), None)?;

            let read_expression = if have_partition_cols {
                // Loop over all fields and create the correct expressions for them
                let all_fields: Vec<Expression> = all_fields
                    .iter()
                    .map(|field| match field {
                        ColumnType::Partition(field) => {
                            let value_expression = parse_partition_value(
                                add.partition_values.get(field.name()),
                                field.data_type(),
                            )?;
                            Ok::<Expression, Error>(Expression::Literal(value_expression))
                        }
                        ColumnType::Selected(field) => Ok(Expression::column(field.name())),
                    })
                    .try_collect()?;
                Some(Expression::Struct(all_fields))
            } else {
                None
            };
            debug!("Final expression for read: {read_expression:?}");

            let dv_treemap = add
                .deletion_vector
                .as_ref()
                .map(|dv_descriptor| {
                    let fs_client = engine_interface.get_file_system_client();
                    dv_descriptor.read(fs_client, &self.snapshot.table_root)
                })
                .transpose()?;

            let mut dv_mask = dv_treemap.map(treemap_to_bools);

            for read_result in read_results {
                let len = if let Ok(ref res) = read_result {
                    res.length()
                } else {
                    0
                };

                let read_result = match read_expression {
                    Some(ref read_expression) => engine_interface
                        .get_expression_handler()
                        .get_evaluator(
                            read_schema.clone(),
                            read_expression.clone(),
                            output_schema.clone(),
                        )
                        .evaluate(read_result?.as_ref()),
                    None => {
                        // if we don't have partition columns, the result is just what we read
                        read_result
                    }
                };

                // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                // will cover the following results
                let rest = dv_mask.as_mut().map(|mask| mask.split_off(len));

                let scan_result = ScanResult {
                    raw_data: read_result,
                    mask: dv_mask,
                };
                dv_mask = rest;
                results.push(scan_result);
            }
        }
        Ok(results)
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
    file_stream::SCAN_ROW_SCHEMA.as_ref().clone()
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
/// - fields_to_read_from_parquet - Which fields should be read from the raw parquet files
/// - have_partition_cols - boolean indicating if we have partition columns in this query
fn get_state_info<'a>(
    schema: &'a Schema,
    partition_columns: &[String],
) -> (Vec<ColumnType<'a>>, Vec<StructField>, bool) {
    let mut have_partition_cols = false;
    let mut read_fields = Vec::with_capacity(schema.fields.len());
    // Loop over all selected fields and note if they are columns that will be read from the
    // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
    // be filled in by evaluating an expression ([`ColumnType::Partition`])
    let column_types = schema
        .fields()
        .map(|field| {
            if partition_columns.contains(field.name()) {
                // todo: this is slow(ish)
                // Store the raw field, we will turn it into an expression in the inner loop
                // since the expression could be different for each add file
                have_partition_cols = true;
                ColumnType::Partition(field)
            } else {
                // Add to read schema, store field so we can build a `Column` expression later
                // if needed (i.e. if we have partition columns)
                read_fields.push(field.clone());
                ColumnType::Selected(field)
            }
        })
        .collect();
    (column_types, read_fields, have_partition_cols)
}

pub fn selection_vector(
    engine_interface: &dyn EngineInterface,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Url,
) -> DeltaResult<Vec<bool>> {
    let fs_client = engine_interface.get_file_system_client();
    let dv_treemap = descriptor.read(fs_client, table_root)?;
    Ok(treemap_to_bools(dv_treemap))
}

pub fn transform_to_logical(
    engine_interface: &dyn EngineInterface,
    data: Box<dyn EngineData>,
    global_state: &GlobalScanState,
    partition_values: &std::collections::HashMap<String, String>,
) -> DeltaResult<Box<dyn EngineData>> {
    let (all_fields, _read_fields, have_partition_cols) = get_state_info(
        &global_state.logical_schema,
        &global_state.partition_columns,
    );
    let read_schema = Arc::new(global_state.read_schema.clone());
    if have_partition_cols {
        // need to add back partition cols
        let all_fields: Vec<Expression> = all_fields
            .iter()
            .map(|field| match field {
                ColumnType::Partition(field) => {
                    let value_expression = parse_partition_value(
                        partition_values.get(field.name()),
                        field.data_type(),
                    )?;
                    Ok::<Expression, Error>(Expression::Literal(value_expression))
                }
                ColumnType::Selected(field) => Ok(Expression::column(field.name())),
            })
            .try_collect()?;
        let read_expression = Expression::Struct(all_fields);
        let result = engine_interface
            .get_expression_handler()
            .get_evaluator(
                read_schema.clone(),
                read_expression.clone(),
                DataType::Struct(Box::new(global_state.logical_schema.clone())),
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
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::{
        actions::get_log_schema,
        client::{
            arrow_data::ArrowEngineData,
            sync::{json::SyncJsonHandler, SyncEngineInterface},
        },
        scan::file_stream::scan_action_iter,
        schema::{StructField, StructType},
        EngineData, JsonHandler,
    };

    use super::state::DvInfo;

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
        expected_sel_vec: Vec<bool>,
        context: T,
        validate_callback: fn(
            context: &mut T,
            path: &str,
            size: i64,
            dv_info: DvInfo,
            partition_values: HashMap<String, String>,
        ),
    ) {
        let interface = SyncEngineInterface::new();
        // doesn't matter here
        let table_schema = Arc::new(StructType::new(vec![StructField::new(
            "foo",
            crate::schema::DataType::STRING,
            false,
        )]));
        let iter = scan_action_iter(
            &interface,
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
                sel,
                context.clone(),
                validate_callback,
            )
            .unwrap();
            batch_count += 1;
        }
        assert_eq!(batch_count, 1);
    }
}

#[cfg(all(test, feature = "sync-client"))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::client::sync::SyncEngineInterface;
    use crate::schema::PrimitiveType;
    use crate::Table;

    #[test]
    fn test_scan_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_interface = SyncEngineInterface::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_interface, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files: Vec<Add> = scan
            .files(&engine_interface)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(
            &files[0].path,
            "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"
        );
        assert!(&files[0].deletion_vector.is_none());
    }

    #[test]
    fn test_scan_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_interface = SyncEngineInterface::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_interface, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files = scan.execute(&engine_interface).unwrap();

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
        let engine_interface = SyncEngineInterface::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_interface, None)?;
        let scan = ScanBuilder::new(snapshot).build();
        let files: Vec<DeltaResult<Add>> = scan.files(&engine_interface)?.collect();

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
            files
                .into_iter()
                .map(|file| file.unwrap().path)
                .collect::<Vec<_>>(),
            vec!["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"]
        );
        Ok(())
    }
}
