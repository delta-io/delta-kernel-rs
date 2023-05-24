use crate::snapshot::Snapshot;
use crate::storage::StorageClient;
use crate::Version;
use std::path::PathBuf;

/// In-memory representation of a Delta table. Location specifies the root location of the table.
// We should verify the given path exists and is accessible before allowing any
// read operation. Ideally, it should also cache known tables, snapshots, and files, so repeated
// reads can reuse them instead of going back to cloud storage each time.
// TODO remove Paths and instead use something like object_store crate's Path type
#[derive(Debug)]
pub struct DeltaTable {
    pub(crate) location: PathBuf,
}

impl DeltaTable {
    /// Create a new Delta table from the root table path.
    pub fn new(table_path: &str) -> Self {
        // TODO normalize remove trailing slash if present
        let table_path = PathBuf::from(table_path);
        DeltaTable {
            location: table_path,
        }
    }

    /// Get the latest snapshot of the table. Requires a storage client to read table metadata.
    // TODO can we have a DeltaTable with no snapshots? (likely just the table-doesnt-exist case).
    // should this return Option<Snapshot>
    pub fn get_latest_snapshot<S: StorageClient>(&self, storage_client: &S) -> Snapshot {
        Snapshot::new(
            self.location.clone(),
            self.get_latest_version(storage_client).unwrap(),
            storage_client,
        )
    }

    /// get a snapshot corresponding to `version` for the table. Requires a storage client to read
    /// table metadata.
    pub fn get_snapshot<S: StorageClient>(&self, version: Version, storage_client: &S) -> Snapshot {
        Snapshot::new(self.location.clone(), version, storage_client)
    }

    // FIXME listing _delta_log should likely be moved to delta_log module?
    fn get_latest_version<S: StorageClient>(&self, storage_client: &S) -> Option<Version> {
        // NOTE duplicate work: we list files here but we could keep them for use in the snapshot.
        let mut path = self.location.clone();
        path.push("_delta_log");
        let mut paths = storage_client.list(path.to_str().unwrap());
        paths.sort();
        let last_commit = paths.iter().rev().next();
        last_commit.map(|p| {
            p.file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<Version>()
                .unwrap()
        })
    }
}
