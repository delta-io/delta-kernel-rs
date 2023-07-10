use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field as ArrowField, Fields, Schema as ArrowSchema};
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::ObjectStore;

use self::file_stream::{LogStream, ScanFileStream};
use self::reader::DeltaReader;
use crate::actions::ActionType;
use crate::expressions::Expression;
use crate::schema::{Schema, SchemaRef};
use crate::snapshot::replay::LogSegment;
use crate::snapshot::LogSegmentNew;
use crate::{DeltaResult, TableClient};

mod data_skipping;
pub mod file_stream;
mod log_replay;
mod reader;

// TODO projection: something like fn select(self, columns: &[&str])
/// Builder to scan a snapshot of a table.
pub struct ScanBuilder<JRC: Send, PRC: Send> {
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

impl<JRC: Send, PRC: Send> ScanBuilder<JRC, PRC> {
    /// Create a new [`ScanBuilder`] instance.
    pub(crate) fn new(
        snapshot_schema: SchemaRef,
        log_segment: LogSegmentNew,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    ) -> Self {
        Self {
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
            log_segment: self.log_segment,
            schema,
            predicate: self.predicate,
            table_client: self.table_client,
        }
    }
}

pub struct Scan<JRC: Send, PRC: Send> {
    log_segment: LogSegmentNew,
    schema: SchemaRef,
    predicate: Option<Expression>,
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
}

impl<JRC: Send, PRC: Send> std::fmt::Debug for Scan<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl<JRC: Send, PRC: Send> Scan<JRC, PRC> {
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
    pub fn files(&self) -> DeltaResult<LogStream> {
        // TODO create function to generate native schema
        let schema = ArrowSchema {
            fields: Fields::from_iter([ActionType::Add.field(), ActionType::Remove.field()]),
            metadata: Default::default(),
        };
        let schema = Arc::new(Schema::try_from(&schema).unwrap());

        let json_handler = self.table_client.get_json_handler();
        let commit_reads = json_handler.contextualize_file_reads(
            self.log_segment.commit_files().cloned().collect::<Vec<_>>(),
            self.predicate.clone(),
        )?;

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

        LogStream::new(stream, self.predicate.clone())
    }

    pub async fn execute(&self) -> DeltaResult<BoxStream<'_, DeltaResult<RecordBatch>>> {
        todo!()
    }
}

/// Scan/read a table. The schema provided implements a projection (column selection) and the
/// predicate implements filtering (row selection).
#[derive(Debug)]
pub struct ScanCurr {
    location: Path,
    log_segment: LogSegment,
    schema: ArrowSchema,
    predicate: Option<Expression>,
}

impl<'a> ScanCurr {
    /// This is the main method to 'materialize' the scan. It returns a `ScanFileBatchIterator`
    /// which yields record batches of scan files and their associated metadata. Rows of the scan
    /// files batches correspond to data reads, and the DeltaReader is used to materialize the scan
    /// files into actual table data.
    pub fn files(&'a self, storage_client: Arc<dyn ObjectStore>) -> ScanFileStream<'a> {
        ScanFileStream::new(&self.log_segment, &self.predicate, storage_client)
    }

    /// Creates a `DeltaReader` for the scan. This reader implements the data reading portion of
    /// scans. Use the `files` method on the scan to perform metadata processing to produce record
    /// batches of scan files; then each row in the batch which corresponds to a table data file is
    /// materialized into an iterator over table data via the DeltaReader.
    pub fn create_reader(&'a self) -> DeltaReader {
        DeltaReader::new(self.location.clone())
    }

    /// Get the schema of the scan.
    pub fn schema(&self) -> &ArrowSchema {
        &self.schema
    }

    /// Get the predicate of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
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
        assert_eq!(files[0].num_rows(), 1)
    }
}
