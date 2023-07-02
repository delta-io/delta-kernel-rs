//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::sync::Arc;

use arrow_schema::Schema;
use object_store::path::Path;
use object_store::ObjectStore;

use self::replay::LogSegment;
use crate::scan::ScanBuilder;
use crate::{DeltaResult, Version};

pub(crate) mod replay;

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
    location: Path,
    /// snapshot version
    version: Version,
    /// arrow schema of the snapshot
    schema: Schema,
    /// log segment (essentially commit/checkpoint file names) valid for this snapshot
    pub(crate) log_segment: LogSegment,
}

impl Snapshot {
    /// Create a snapshot for a given table version. Main work done is listing files in storage and
    /// performing lightweight log replay to resolve protocol and metadata (schema) for the
    /// table/snapshot.
    pub(crate) async fn try_new(
        location: Path,
        version: Version,
        storage: Arc<dyn ObjectStore>,
    ) -> DeltaResult<Self> {
        let log_segment = LogSegment::try_new(location.clone(), version, storage).await?;
        // FIXME need to resolve the table schema during snapshot creation using some minimal
        // log replay
        let schema = Schema::empty();
        Ok(Self {
            location,
            schema,
            version,
            log_segment,
        })
    }

    /// version of the snapshot
    pub fn version(&self) -> Version {
        self.version
    }

    /// schema of the snapshot
    pub fn schema(&self) -> &Schema {
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
