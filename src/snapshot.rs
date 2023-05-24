use crate::delta_log::LogSegment;
use crate::scan::ScanBuilder;
use crate::storage::StorageClient;
use crate::Version;
use std::path::PathBuf;

/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
// TODO expose methods for accessing the files of a table (with file pruning).
#[derive(Debug)]
pub struct Snapshot {
    /// root location of the delta table
    // TODO many of these 'sub-types' of the top-level DeltaTable type own their paths. Should we
    // have refs?
    location: PathBuf,
    /// snapshot version
    version: Version,
    /// arrow schema of the snapshot
    schema: arrow::datatypes::Schema,
    /// log segment (essentially commit/checkpoint file names) valid for this snapshot
    pub(crate) log_segment: LogSegment,
}

impl Snapshot {
    /// Create a snapshot for a given table version. Main work done is listing files in storage and
    /// performing lightweight log replay to resolve protocol and metadata (schema) for the
    /// table/snapshot.
    pub(crate) fn new<S: StorageClient>(
        location: PathBuf,
        version: Version,
        storage_client: &S,
    ) -> Self {
        let log_segment = LogSegment::new::<S>(location.clone(), version, storage_client);
        // FIXME need to resolve the table schema during snapshot creation using some minimal
        // log replay
        let schema = arrow::datatypes::Schema::empty();
        Snapshot {
            location,
            schema,
            version,
            log_segment,
        }
    }

    /// version of the snapshot
    pub fn version(&self) -> Version {
        self.version
    }

    /// schema of the snapshot
    pub fn schema(&self) -> &arrow::datatypes::Schema {
        &self.schema
    }

    /// Create a builder to perform a scan of the snapshot.
    pub fn scan(&self) -> ScanBuilder {
        ScanBuilder::new(
            self.location.clone(),
            self.schema.clone(),
            self.log_segment.clone(),
        )
    }
}
