use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::*;

use crate::snapshot::Snapshot;
use crate::{version_from_path, Version};

/// In-memory representation of a Delta table. Location specifies the root location of the table.
// We should verify the given path exists and is accessible before allowing any
// read operation. Ideally, it should also cache known tables, snapshots, and files, so repeated
// reads can reuse them instead of going back to cloud storage each time.
// TODO remove Paths and instead use something like object_store crate's Path type
#[derive(Debug)]
pub struct DeltaTable {
    pub(crate) location: Path,
}

impl DeltaTable {
    /// Create a new Delta table from the root table path.
    pub fn new(table_path: &str) -> Self {
        // TODO normalize remove trailing slash if present
        DeltaTable {
            location: Path::from(table_path),
        }
    }

    /// Get the latest snapshot of the table. Requires a storage client to read table metadata.
    // TODO can we have a DeltaTable with no snapshots? (likely just the table-doesnt-exist case).
    // should this return Option<Snapshot>
    pub async fn get_latest_snapshot(&self, storage: Arc<dyn ObjectStore>) -> Snapshot {
        Snapshot::new(
            self.location.clone(),
            self.get_latest_version(storage.clone()).await.unwrap(),
            storage,
        )
        .await
    }

    /// get a snapshot corresponding to `version` for the table. Requires a storage client to read
    /// table metadata.
    pub async fn get_snapshot(&self, version: Version, storage: Arc<dyn ObjectStore>) -> Snapshot {
        Snapshot::new(self.location.clone(), version, storage).await
    }

    // FIXME listing _delta_log should likely be moved to delta_log module?
    async fn get_latest_version(&self, storage: Arc<dyn ObjectStore>) -> Option<Version> {
        // NOTE duplicate work: we list files here but we could keep them for use in the snapshot.
        let path = self.location.child("_delta_log");
        // XXX: Error handling on list
        let list_stream = storage.list(Some(&path)).await.unwrap();
        let mut paths: Vec<Path> = list_stream.map(|res| res.unwrap().location).collect().await;
        paths.sort();

        // XXX: This is not handling checkpoint files efficiently
        if let Some(last_commit) = paths.iter().rev().find(|p| p.extension() == Some("json")) {
            match version_from_path(last_commit) {
                Ok(version) => Some(version),
                Err(e) => {
                    println!("AHCK! {e:?}");
                    error!("Failed to get a version from the last commit: {e:?} {last_commit:?}");
                    None
                }
            }
        } else {
            None
        }
    }
}
