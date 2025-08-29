//! Builder for creating [`Snapshot`] instances.

use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, Snapshot, Version};

use url::Url;

/// Builder for creating [`Snapshot`] instances.
///
/// # Example
///
/// ```no_run
/// # use delta_kernel::{Snapshot, Engine};
/// # use url::Url;
/// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
/// let table_root = Url::parse("file:///path/to/table")?;
///
/// // Build a snapshot
/// let snapshot = Snapshot::builder(table_root.clone())
///     .at_version(5) // Optional: specify a time-travel version (default is latest version)
///     .build(engine)?;
///
/// # Ok(())
/// # }
/// ```
pub struct SnapshotBuilder {
    table_root: Url,
    version: Option<Version>,
}

impl SnapshotBuilder {
    pub(crate) fn new(table_root: Url) -> Self {
        Self {
            table_root,
            version: None,
        }
    }

    /// Set the target version of the [`Snapshot`]. When omitted, the Snapshot is created at the
    /// latest version of the table.
    pub fn at_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Create a new [`Snapshot`] instance.
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        let log_segment = LogSegment::for_snapshot(
            engine.storage_handler().as_ref(),
            self.table_root.join("_delta_log/")?,
            self.version,
        )?;
        Snapshot::try_new_from_log_segment(self.table_root, log_segment, engine)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};

    use itertools::Itertools;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use serde_json::json;

    use super::*;

    fn setup_test() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<dyn ObjectStore>,
        Url,
    ) {
        let table_root = Url::parse("memory:///test_table").unwrap();
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngine::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));
        (engine, store, table_root)
    }

    fn create_table(store: &Arc<dyn ObjectStore>, _table_root: &Url) -> DeltaResult<()> {
        let protocol = json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged"],
        });

        let metadata = json!({
            "id": "test-table-id",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });

        // Create commit 0 with protocol and metadata
        let commit0 = [
            json!({
                "protocol": protocol
            }),
            json!({
                "metaData": metadata
            }),
        ];

        // Write commit 0
        let commit0_data = commit0
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = object_store::path::Path::from(format!("_delta_log/{:020}.json", 0).as_str());
        futures::executor::block_on(async { store.put(&path, commit0_data.into()).await })?;

        // Create commit 1 with a single addFile action
        let commit1 = [json!({
            "add": {
                "path": "part-00000-test.parquet",
                "partitionValues": {},
                "size": 1024,
                "modificationTime": 1587968586000i64,
                "dataChange": true,
                "stats": null,
                "tags": null
            }
        })];

        // Write commit 1
        let commit1_data = commit1
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = object_store::path::Path::from(format!("_delta_log/{:020}.json", 1).as_str());
        futures::executor::block_on(async { store.put(&path, commit1_data.into()).await })?;

        Ok(())
    }

    #[test]
    fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_table(&store, &table_root)?;

        let snapshot = SnapshotBuilder::new(table_root.clone()).build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::new(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }
}
