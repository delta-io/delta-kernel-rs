use crate::delta_log::*;
use crate::expressions::Expression;
use crate::storage::StorageClient;
use std::fmt::Debug;
use std::path::PathBuf;

mod data_skipping;
use arrow::record_batch::RecordBatch;
use data_skipping::data_skipping_filter;

use self::reader::DeltaReader;

// the Scan type is only concerned with metadata. it produces DataFiles which must then be read
// TODO move this up/out of scan?
mod reader;
// mod deletion_vectors;

// TODO ownership: should this own location/log_segment/schema/expression?
/// Builder to scan a snapshot of a table.
#[derive(Debug)]
pub struct ScanBuilder {
    location: PathBuf,
    log_segment: LogSegment,
    snapshot_schema: arrow::datatypes::Schema,
    schema: Option<arrow::datatypes::Schema>,
    predicate: Option<Expression>,
}

/// Scan/read a table. The schema provided implements a projection (column selection) and the
/// predicate implements filtering (row selection).
#[derive(Debug)]
pub struct Scan {
    location: PathBuf,
    log_segment: LogSegment,
    schema: arrow::datatypes::Schema,
    predicate: Option<Expression>,
}

impl ScanBuilder {
    pub(crate) fn new(
        location: PathBuf,
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
    pub fn files<S: StorageClient>(&'a self, storage_client: &'a S) -> ScanFileBatchIterator<'a> {
        ScanFileBatchIterator::new(storage_client, &self.log_segment, &self.predicate)
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

/// An iterator over scan files and their associated metadata to read valid data for a table scan.
pub struct ScanFileBatchIterator<'a> {
    actions_batch_iter: Box<dyn Iterator<Item = RecordBatch> + 'a>,
    log_replay: LogReplay,
    predicate: &'a Option<Expression>,
}

// FIXME need to actually implement useful debug
impl Debug for ScanFileBatchIterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "someone help make a better debug impl")
    }
}

impl Iterator for ScanFileBatchIterator<'_> {
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        // NOTE duplicate work in the data skipping: we map(data_skipping_filter) in order to
        // compute and apply the boolean vector of data skipping to all add file actions, including
        // ones which are invalid from prior remove_file actions
        self.actions_batch_iter.next().map(|actions| {
            // filter actions based on predicate. it's a 'map' over record batches since we have
            // [RecordBatch(action1, action2, action3), ...] and produce something like
            // [RecordBatch(action2, action3), ...]
            let data_skipped_actions = data_skipping_filter(actions, self.predicate);
            self.log_replay.replay(data_skipped_actions)
        })
    }
}

impl<'a> ScanFileBatchIterator<'a> {
    pub(crate) fn new<S: StorageClient + 'a>(
        storage_client: &'a S,
        log_segment: &'a LogSegment,
        predicate: &'a Option<Expression>,
    ) -> Self {
        // iter over record batches of actions
        let actions_batch_iter = log_segment
            .iter() // iter over commit/checkpoint files
            .rev() // ...in reverse (commit12, commit11, checkpoint10.p1, checkpoint10.p2)
            .flat_map(|file| file.read(storage_client));

        ScanFileBatchIterator {
            actions_batch_iter: Box::new(actions_batch_iter),
            log_replay: LogReplay::new(),
            predicate,
        }
    }
}
