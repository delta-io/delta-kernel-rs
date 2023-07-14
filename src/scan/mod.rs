use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Fields, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use futures::stream::{StreamExt, TryStreamExt};
use url::Url;

use self::file_stream::LogReplayStream;
use crate::actions::ActionType;
use crate::expressions::Expression;
use crate::schema::{Schema, SchemaRef};
use crate::snapshot::LogSegmentNew;
use crate::{DeltaResult, FileMeta, TableClient};

mod data_skipping;
pub mod file_stream;

// TODO projection: something like fn select(self, columns: &[&str])
/// Builder to scan a snapshot of a table.
pub struct ScanBuilder<JRC: Send, PRC: Send> {
    table_root: Url,
    log_segment: LogSegmentNew,
    snapshot_schema: SchemaRef,
    schema: Option<SchemaRef>,
    predicate: Option<Expression>,
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
}

impl<JRC: Send, PRC: Send> std::fmt::Debug for ScanBuilder<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("ScanBuilder")
            .field("schema", &self.schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl<JRC: Send, PRC: Send + Sync> ScanBuilder<JRC, PRC> {
    /// Create a new [`ScanBuilder`] instance.
    pub(crate) fn new(
        table_root: Url,
        snapshot_schema: SchemaRef,
        log_segment: LogSegmentNew,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    ) -> Self {
        Self {
            table_root,
            snapshot_schema,
            log_segment,
            schema: None,
            predicate: None,
            table_client,
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
    pub fn build(self) -> Scan<JRC, PRC> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let schema = self.schema.unwrap_or_else(|| self.snapshot_schema.clone());
        Scan {
            table_root: self.table_root,
            log_segment: self.log_segment,
            schema,
            predicate: self.predicate,
            table_client: self.table_client,
        }
    }
}

pub struct Scan<JRC: Send, PRC: Send + Sync> {
    table_root: Url,
    log_segment: LogSegmentNew,
    schema: SchemaRef,
    predicate: Option<Expression>,
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
}

impl<JRC: Send, PRC: Send + Sync> std::fmt::Debug for Scan<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl<JRC: Send, PRC: Send + Sync + 'static> Scan<JRC, PRC> {
    /// Get a shred refernce to the [`Schema`] of the scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
    }

    /// This is the main method to 'materialize' the scan. It returns a `ScanFileBatchIterator`
    /// which yields record batches of scan files and their associated metadata. Rows of the scan
    /// files batches correspond to data reads, and the DeltaReader is used to materialize the scan
    /// files into actual table data.
    pub fn files(&self) -> DeltaResult<LogReplayStream> {
        // TODO use LogSegmentNEw replay ...
        // TODO create function to generate native schema
        let schema = ArrowSchema {
            fields: Fields::from_iter([ActionType::Add.field(), ActionType::Remove.field()]),
            metadata: Default::default(),
        };
        let schema = Arc::new(Schema::try_from(&schema).unwrap());

        let mut commit_files: Vec<_> = self.log_segment.commit_files().cloned().collect();
        // NOTE this will already sort in reverse order
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        let json_handler = self.table_client.get_json_handler();
        let commit_reads =
            json_handler.contextualize_file_reads(commit_files, self.predicate.clone())?;

        let parquet_handler = self.table_client.get_parquet_handler();
        let checkpoint_reads = parquet_handler.contextualize_file_reads(
            self.log_segment
                .checkpoint_files()
                .cloned()
                .collect::<Vec<_>>(),
            self.predicate.clone(),
        )?;

        let stream = json_handler
            .read_json_files(commit_reads, schema.clone())?
            .chain(parquet_handler.read_parquet_files(checkpoint_reads, schema.clone())?)
            .boxed();

        LogReplayStream::new(
            stream,
            self.predicate.clone(),
            self.table_client.get_file_system_client(),
            self.table_root.clone(),
        )
    }

    pub async fn execute(&self) -> DeltaResult<Vec<RecordBatch>> {
        let parquet_handler = self.table_client.get_parquet_handler();
        let mut stream = self.files()?.boxed();

        let mut results = Vec::new();
        while let Some(Ok(data)) = stream.next().await {
            for file in data {
                let meta = FileMeta {
                    last_modified: file.add.modification_time,
                    size: file.add.size as usize,
                    location: self.table_root.join(&file.add.path)?,
                };
                let context = parquet_handler.contextualize_file_reads(vec![meta], None)?;
                let batches = parquet_handler
                    .read_parquet_files(context, self.schema.clone())?
                    .try_collect::<Vec<_>>()
                    .await?;
                if batches.len() < 1 {
                    continue;
                }
                let schema = batches[0].schema();
                let batch = concat_batches(&schema, &batches)?;
                if let Some(fut_dv) = file.dv {
                    let dv = fut_dv.await?;
                    let vec: Vec<_> = (0..batch.num_rows())
                        .map(|i| Some(!dv.contains(i.try_into().expect("fit into u32"))))
                        .collect();
                    let dv = BooleanArray::from(vec);
                    results.push(filter_record_batch(&batch, &dv)?);
                } else {
                    results.push(batch);
                }
            }
        }

        Ok(results)
    }
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::path::PathBuf;

    use futures::TryStreamExt;

    use super::*;
    use crate::client::DefaultTableClient;
    use crate::Table;

    #[tokio::test]
    async fn test_scan_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let table_client = Arc::new(
            DefaultTableClient::try_new(&url, std::iter::empty::<(&str, &str)>()).unwrap(),
        );

        let table = Table::new(url, table_client);
        let snapshot = table.snapshot(None).await.unwrap();
        let scan = snapshot.scan().await.unwrap().build();
        let files = scan.files().unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].len(), 1)
    }

    #[tokio::test]
    async fn test_scan_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let table_client = Arc::new(
            DefaultTableClient::try_new(&url, std::iter::empty::<(&str, &str)>()).unwrap(),
        );

        let table = Table::new(url, table_client);
        let snapshot = table.snapshot(None).await.unwrap();
        let scan = snapshot.scan().await.unwrap().build();
        let files = scan.execute().await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].num_rows(), 10)
    }
}
