use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, StructArray};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;

use self::file_stream::log_replay_iter;
use crate::actions::ActionType;
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::{Add, DeltaResult, EngineInterface, Error, FileMeta};

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
    /// This is the schema of the columns that will be returned by the scan,
    /// and not the schema of the entire table.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.read_schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
    }

    /// This is the main method to 'materialize' the scan. It returns a `ScanFileBatchIterator`
    /// which yields record batches of scan files and their associated metadata. Rows of the scan
    /// files batches correspond to data reads, and the DeltaReader is used to materialize the scan
    /// files into actual table data.
    pub fn files(
        &self,
        engine_interface: &dyn EngineInterface,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>>> {
        lazy_static::lazy_static! {
            static ref ACTION_SCHEMA: SchemaRef = Arc::new(StructType::new(vec![
                ActionType::Add.schema_field().clone(),
                ActionType::Remove.schema_field().clone(),
            ]));
        }

        let log_iter = self.snapshot.log_segment.replay(
            engine_interface,
            ACTION_SCHEMA.clone(),
            self.predicate.clone(),
        )?;

        Ok(log_replay_iter(
            engine_interface,
            log_iter,
            &self.read_schema,
            &self.predicate,
        ))
    }

    pub fn execute(&self, engine_interface: &dyn EngineInterface) -> DeltaResult<Vec<RecordBatch>> {
        let parquet_handler = engine_interface.get_parquet_handler();

        let read_schema = Arc::new(StructType::new(
            self.schema()
                .fields()
                .into_iter()
                .filter(|f| {
                    !self
                        .snapshot
                        .metadata()
                        .partition_columns
                        .contains(f.name())
                })
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let mut partition_fields = self
            .snapshot
            .metadata()
            .partition_columns
            .iter()
            .map(|column| {
                self.schema()
                    .field(column)
                    .ok_or(Error::Generic("Unexpected partition column".to_string()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        partition_fields.reverse();

        let select_fields = read_schema
            .fields()
            .iter()
            .map(|f| Expression::Column(f.name().to_string()))
            .collect_vec();

        self.files(engine_interface)?
            .map(|res| {
                let add = res?;
                let meta = FileMeta {
                    last_modified: add.modification_time,
                    size: add.size as usize,
                    location: self.snapshot.table_root.join(&add.path)?,
                };
                let batches = parquet_handler
                    .read_parquet_files(&[meta], read_schema.clone(), None)?
                    .collect::<DeltaResult<Vec<_>>>()?;

                if batches.is_empty() {
                    return Ok(None);
                }

                let schema = batches[0].schema();
                let batch = concat_batches(&schema, &batches)?;

                let batch = if partition_fields.is_empty() {
                    batch
                } else {
                    let mut fields =
                        Vec::with_capacity(partition_fields.len() + batch.num_columns());
                    for field in &partition_fields {
                        let value_expression =
                            if let Some(Some(value)) = add.partition_values.get(field.name()) {
                                Expression::Literal(get_partition_value(value, field.data_type())?)
                            } else {
                                // TODO: is it allowed to assume null for missing partition values?
                                Expression::Literal(Scalar::Null(field.data_type().clone()))
                            };
                        fields.push(value_expression);
                    }
                    fields.extend(select_fields.clone());

                    let evaluator = engine_interface.get_expression_handler().get_evaluator(
                        read_schema.clone(),
                        Expression::Struct(fields),
                        DataType::Struct(Box::new(self.schema().as_ref().clone())),
                    );

                    evaluator
                        .evaluate(&batch)?
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or(Error::UnexpectedColumnType("Unexpected array type".into()))?
                        .into()
                };

                if let Some(dv_descriptor) = add.deletion_vector {
                    let fs_client = engine_interface.get_file_system_client();
                    let dv = dv_descriptor.read(fs_client, self.snapshot.table_root.clone())?;
                    let mask: BooleanArray = (0..batch.num_rows())
                        .map(|i| Some(!dv.contains(i.try_into().expect("fit into u32"))))
                        .collect();
                    Ok(Some(filter_record_batch(&batch, &mask)?))
                } else {
                    Ok(Some(batch))
                }
            })
            .filter_map_ok(|batch| batch)
            .collect()
    }
}

fn get_partition_value(raw: &str, data_type: &DataType) -> DeltaResult<Scalar> {
    match data_type {
        DataType::Primitive(primitive) => primitive.parse_scalar(raw),
        _ => todo!(),
    }
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::client::DefaultTableClient;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::schema::PrimitiveType;
    use crate::Table;

    #[test]
    fn test_scan_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_interface = DefaultTableClient::try_new(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Arc::new(TokioBackgroundExecutor::new()),
        )
        .unwrap();

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
        let engine_interface = DefaultTableClient::try_new(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Arc::new(TokioBackgroundExecutor::new()),
        )
        .unwrap();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_interface, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files = scan.execute(&engine_interface).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].num_rows(), 10)
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
            let value = get_partition_value(raw, &DataType::Primitive(data_type.clone())).unwrap();
            assert_eq!(value, *expected);
        }
    }
}
