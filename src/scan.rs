use crate::delta_log::*;
use crate::expressions::Expression;

use arrow::record_batch::RecordBatch;
use futures::prelude::*;
use futures::stream::{BoxStream, StreamExt};
use futures::task::{Context, Poll};
use object_store::path::Path;
use object_store::ObjectStore;

use self::reader::DeltaReader;

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

mod data_skipping;
use data_skipping::data_skipping_filter;

// the Scan type is only concerned with metadata. it produces DataFiles which must then be read
// TODO move this up/out of scan?
mod reader;

// TODO ownership: should this own location/log_segment/schema/expression?
/// Builder to scan a snapshot of a table.
#[derive(Debug)]
pub struct ScanBuilder {
    location: Path,
    log_segment: LogSegment,
    snapshot_schema: arrow::datatypes::Schema,
    schema: Option<arrow::datatypes::Schema>,
    predicate: Option<Expression>,
}

/// Scan/read a table. The schema provided implements a projection (column selection) and the
/// predicate implements filtering (row selection).
#[derive(Debug)]
pub struct Scan {
    location: Path,
    log_segment: LogSegment,
    schema: arrow::datatypes::Schema,
    predicate: Option<Expression>,
}

impl ScanBuilder {
    pub(crate) fn new(
        location: Path,
        snapshot_schema: arrow::datatypes::Schema,
        log_segment: LogSegment,
    ) -> Self {
        ScanBuilder {
            location,
            snapshot_schema,
            log_segment,
            schema: None,
            predicate: None,
        }
    }

    /// Provide an arrow schema for columns to select from the snapshot. For example, a table with
    /// columns `[a, b, c]` could have a scan which reads only the first two columns by using the
    /// schema `[a, b]`
    // TODO projection: something like fn select(self, columns: &[&str])
    pub fn with_schema(mut self, schema: arrow::datatypes::Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Predicates specified in this crate's `Expression` type can be used to filter the rows in a
    /// scan. For example, using the predicate `x < 4` to return a subset of the rows in the scan
    /// which satisfy the filter.
    pub fn with_predicate(mut self, predicate: Expression) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Construct the `Scan`. Note this is lazy and performs no 'work' at this point. The `Scan`
    /// type itself can be used to fetch the files and associated metadata required to perform
    /// actual data reads.
    pub fn build(self) -> Scan {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let schema = self.schema.unwrap_or_else(|| self.snapshot_schema.clone());
        Scan {
            location: self.location,
            log_segment: self.log_segment,
            schema,
            predicate: self.predicate,
        }
    }
}

impl<'a> Scan {
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
    pub fn schema(&self) -> &arrow::datatypes::Schema {
        &self.schema
    }

    /// Get the predicate of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
    }
}

pub struct ScanFileStream<'a> {
    stream: BoxStream<'a, RecordBatch>,
    log_replay: LogReplay,
    log_segment: &'a LogSegment,
    predicate: &'a Option<Expression>,
}

impl std::fmt::Debug for ScanFileStream<'_> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        todo!()
    }
}

impl Stream for ScanFileStream<'_> {
    type Item = RecordBatch;
    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<<Self as futures::Stream>::Item>> {
        let stream = Pin::new(&mut self.stream);

        match stream.poll_next(ctx) {
            futures::task::Poll::Ready(value) => {
                if let Some(actions) = value {
                    let skipped = data_skipping_filter(actions, self.predicate);
                    futures::task::Poll::Ready(Some(self.log_replay.replay(skipped)))
                } else {
                    futures::task::Poll::Ready(value)
                }
            }
            other => other,
        }
    }
}

impl<'a> ScanFileStream<'a> {
    fn new(
        log_segment: &'a LogSegment,
        predicate: &'a Option<Expression>,
        storage_client: Arc<dyn ObjectStore>,
    ) -> Self {
        let storage_client = storage_client.clone();

        let stream = Box::pin(
            stream::iter(log_segment.iter().rev())
                .map(move |log| log.read(storage_client.clone()))
                .then(|f| f)
                .flat_map(stream::iter),
        );
        Self {
            log_segment,
            predicate,
            stream,
            log_replay: LogReplay::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    /*
     * This test has a bit more scaffolding to make it easier to simulate a real use case of the
     * ScanFileStream
     */
    #[tokio::test]
    async fn test_scanfilestream() {
        let storage_client = LocalFileSystem::new();
        let path1 = Path::from(format!(
            "{}/tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let path2 = Path::from(format!(
            "{}/tests/data/table-with-dv-small/_delta_log/00000000000000000001.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let log_segment = LogSegment {
            log_files: vec![path1.try_into().unwrap(), path2.try_into().unwrap()],
        };

        let mut scan_stream = ScanFileStream::new(&log_segment, &None, Arc::new(storage_client));
        let mut counted = 0;

        while let Some(batch) = scan_stream.next().await {
            let _batch: RecordBatch = batch;
            //println!("batch: {batch:?}");
            counted += 1;
        }
        assert_eq!(
            2, counted,
            "Expected to have iterated over some RecordBatches"
        );
    }

    #[tokio::test]
    async fn test_stream_for_scanning() {
        let storage_client = LocalFileSystem::new();
        let path = Path::from(format!(
            "{}/tests/data/table-without-dv-small/_delta_log/00000000000000000000.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let log_file: LogFile = path.try_into().unwrap();
        let segment = LogSegment {
            log_files: vec![log_file.clone(), log_file],
        };

        let storage_client = Arc::new(storage_client);
        let iterator = stream::iter(
            segment
                .iter()
                .rev()
                .map(|log| log.read(storage_client.clone())),
        )
        .then(|f| f)
        .map(stream::iter)
        .flatten();

        let mut counted = 0;
        for _batch in iterator.collect::<Vec<RecordBatch>>().await {
            counted += 1;
        }
        assert_eq!(
            2, counted,
            "Expected to have iterated over some RecordBatches"
        );
    }
}
