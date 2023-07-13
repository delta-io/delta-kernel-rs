use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema as ArrowSchema};
use futures::stream::{BoxStream, Stream, StreamExt, TryStreamExt};
use futures::task::Poll;
use url::Url;

use self::file_stream::LogReplayStream;
use crate::actions::{Action, ActionType};
use crate::expressions::Expression;
use crate::schema::{Schema, SchemaRef};
use crate::snapshot::LogSegmentNew;
use crate::{DeltaResult, Error, FileMeta, ParquetHandler, TableClient};

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

        LogReplayStream::new(stream, self.predicate.clone())
    }

    pub fn execute(&self) -> DeltaResult<Pin<Box<dyn Stream<Item = DeltaResult<RecordBatch>>>>> {
        let parquet_handler = self.table_client.get_parquet_handler();
        let stream = self.files()?.boxed();
        Ok(Box::pin(
            DataStream::new(
                self.table_root.clone(),
                self.schema.clone(),
                parquet_handler,
                stream,
            )
            .try_flatten(),
        ))
    }
}

#[allow(missing_debug_implementations)]
pub struct DataStream<PRC: Sync> {
    table_root: Url,
    parquet_client: Arc<dyn ParquetHandler<FileReadContext = PRC>>,
    schema: SchemaRef,
    stream: BoxStream<'static, DeltaResult<Vec<Action>>>,
}

impl<PRC: Send + Sync> DataStream<PRC> {
    pub fn new(
        table_root: Url,
        schema: SchemaRef,
        parquet_client: Arc<dyn ParquetHandler<FileReadContext = PRC>>,
        stream: BoxStream<'static, DeltaResult<Vec<Action>>>,
    ) -> Self {
        Self {
            table_root,
            schema,
            parquet_client,
            stream,
        }
    }
}

impl<PRC: Send + Sync> Stream for DataStream<PRC> {
    type Item = DeltaResult<Pin<Box<dyn Stream<Item = DeltaResult<RecordBatch>>>>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let stream = Pin::new(&mut self.stream);
        match stream.poll_next(ctx) {
            Poll::Ready(Some(Ok(actions))) => {
                let files = actions
                    .into_iter()
                    .map(|action| match action {
                        Action::Add(add) => FileMeta {
                            last_modified: add.modification_time,
                            size: add.size as usize,
                            location: self.table_root.join(&add.path).unwrap(),
                        },
                        _ => panic!("unexpected action type"),
                    })
                    .collect::<Vec<_>>();
                let contexts = self.parquet_client.contextualize_file_reads(files, None)?;
                let stream = self
                    .parquet_client
                    .read_parquet_files(contexts, self.schema.clone())?;
                Poll::Ready(Some(Ok(stream)))
            }
            Poll::Ready(Some(Err(err))) => {
                println!("error ---> {:?}", err);
                Poll::Ready(Some(Err(Error::MissingVersion)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
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
        let files = scan
            .execute()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].num_rows(), 10)
    }
}
