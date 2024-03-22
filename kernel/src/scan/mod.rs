use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;

use self::file_stream::log_replay_iter;
use crate::actions::Add;
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, EngineData, EngineInterface, Error, FileMeta};

mod data_skipping;
pub mod file_stream;

// TODO projection: something like fn select(self, columns: &[&str])
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
enum ColumnType<'a> {
    Selected(&'a StructField),
    Partition(&'a StructField),
}

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
    /// Get a shred reference to the [`Schema`] of the scan.
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
    pub fn files(
        &self,
        engine_interface: &dyn EngineInterface,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>>> {
        let commit_read_schema = Arc::new(StructType::new(vec![
            crate::actions::schemas::ADD_FIELD.clone(),
            crate::actions::schemas::REMOVE_FIELD.clone(),
        ]));
        let checkpoint_read_schema = Arc::new(StructType::new(vec![
            crate::actions::schemas::ADD_FIELD.clone(),
        ]));

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

    /// This is the main method to 'materialize' the scan. It returns a [`Result`] of
    /// `Vec<`[`ScanResult`]`>`. This calls [`Scan::files`] to get a set of `Add` actions for the scan,
    /// and then uses the `engine_interface`'s [`crate::ParquetHandler`] to read the actual table
    /// data. Each [`ScanResult`] encapsulates the raw data and an optional boolean vector built
    /// from the deletion vector if it was present. See the documentation for [`ScanResult`] for
    /// more details.
    pub fn execute(&self, engine_interface: &dyn EngineInterface) -> DeltaResult<Vec<ScanResult>> {
        let partition_columns = &self.snapshot.metadata().partition_columns;
        let mut read_fields = Vec::with_capacity(self.schema().fields.len());
        let mut have_partition_cols = false;
        // Loop over all selected fields and note if they are columns that will be read from the
        // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
        // be filled in by evaluating an expression ([`ColumnType::Partition`])
        let all_fields: Vec<_> = self
            .schema()
            .fields()
            .map(|field| {
                if partition_columns.contains(field.name()) {
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
        let read_schema = Arc::new(StructType::new(read_fields));
        debug!("Executing scan with read schema {read_schema:#?}");
        let output_schema = DataType::Struct(Box::new(self.schema().as_ref().clone()));

        // // start col_mapping hack
        // let mut p = 0;
        // let fields = self
        //     .schema()
        //     .fields()
        //     .map(|field| {
        //         let f =
        //             StructField::new(format!("f{}", p), field.data_type().clone(), field.nullable);
        //         p += 1;
        //         f
        //     })
        //     .collect();
        // // end hack
        //let output_schema = DataType::Struct(Box::new(StructType::new(fields)));
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
                    dv_descriptor.read(fs_client, self.snapshot.table_root.clone())
                })
                .transpose()?;

            let mut dv_mask = dv_treemap.map(super::actions::deletion_vector::treemap_to_bools);

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

fn parse_partition_value(
    raw: Option<&Option<String>>,
    data_type: &DataType,
) -> DeltaResult<Scalar> {
    match raw {
        Some(Some(v)) => match data_type {
            DataType::Primitive(primitive) => primitive.parse_scalar(v),
            _ => Err(Error::generic(format!(
                "Unexpected partition column type: {data_type:?}"
            ))),
        },
        _ => Ok(Scalar::Null(data_type.clone())),
    }
}

#[cfg(all(test, feature = "default-client"))]
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
                Some(&Some(raw.to_string())),
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
