use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;

use self::file_stream::log_replay_iter;
use crate::actions::ActionType;
use crate::expressions::Expression;
use crate::schema::{SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::{Add, DeltaResult, EngineClient, FileMeta};

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
        engine_client: &dyn EngineClient,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>>> {
        lazy_static::lazy_static! {
            static ref ACTION_SCHEMA: SchemaRef = Arc::new(StructType::new(vec![
                ActionType::Add.schema_field().clone(),
                ActionType::Remove.schema_field().clone(),
            ]));
        }

        let log_iter = self.snapshot.log_segment.replay(
            engine_client,
            ACTION_SCHEMA.clone(),
            self.predicate.clone(),
        )?;

        Ok(log_replay_iter(
            table_client,
            log_iter,
            &self.read_schema,
            &self.predicate,
        ))
    }

    pub fn execute(&self, engine_client: &dyn EngineClient) -> DeltaResult<Vec<RecordBatch>> {
        let parquet_handler = engine_client.get_parquet_handler();

        self.files(engine_client)?
            .map(|res| {
                let add = res?;
                let meta = FileMeta {
                    last_modified: add.modification_time,
                    size: add.size as usize,
                    location: self.snapshot.table_root.join(&add.path)?,
                };
                let batches = parquet_handler
                    .read_parquet_files(&[meta], self.read_schema.clone(), None)?
                    .collect::<DeltaResult<Vec<_>>>()?;

                if batches.is_empty() {
                    return Ok(None);
                }

                let schema = batches[0].schema();
                let batch = concat_batches(&schema, &batches)?;

                if let Some(dv_descriptor) = add.deletion_vector {
                    let fs_client = engine_client.get_file_system_client();
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

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::client::DefaultTableClient;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::Table;

    #[test]
    fn test_scan_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_client = DefaultTableClient::try_new(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Arc::new(TokioBackgroundExecutor::new()),
        )
        .unwrap();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_client, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files: Vec<Add> = scan.files(&engine_client).unwrap().try_collect().unwrap();

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
        let engine_client = DefaultTableClient::try_new(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Arc::new(TokioBackgroundExecutor::new()),
        )
        .unwrap();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_client, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files = scan.execute(&engine_client).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].num_rows(), 10)
    }
}
