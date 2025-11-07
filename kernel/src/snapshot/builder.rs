//! Builder for creating [`Snapshot`] instances.
use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

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
/// let snapshot = Snapshot::builder_for(table_root.clone())
///     .at_version(5) // Optional: specify a time-travel version (default is latest version)
///     .build(engine)?;
///
/// # Ok(())
/// # }
/// ```
//
// Note the SnapshotBuilder must have either a table_root or an existing_snapshot (but not both).
// We enforce this in the constructors. We could improve this in the future with different
// types/add type state.
#[derive(Debug)]
pub struct SnapshotBuilder {
    table_root: Option<Url>,
    existing_snapshot: Option<SnapshotRef>,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
}

impl SnapshotBuilder {
    pub(crate) fn new_for(table_root: Url) -> Self {
        Self {
            table_root: Some(table_root),
            existing_snapshot: None,
            version: None,
            log_tail: Vec::new(),
        }
    }

    pub(crate) fn new_from(existing_snapshot: SnapshotRef) -> Self {
        Self {
            table_root: None,
            existing_snapshot: Some(existing_snapshot),
            version: None,
            log_tail: Vec::new(),
        }
    }

    /// Set the target version of the [`Snapshot`]. When omitted, the Snapshot is created at the
    /// latest version of the table.
    pub fn at_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the log tail to use when building the snapshot. This allows catalogs or external
    /// systems to provide an up-to-date log tail when used to build a snapshot.
    ///
    /// Note that the log tail must be a contiguous sequence of commits from M..=N where N is the
    /// latest version of the table and 0 <= M <= N.
    #[cfg(feature = "catalog-managed")]
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    /// Create a new [`Snapshot`]. This returns a [`SnapshotRef`] (`Arc<Snapshot>`), perhaps
    /// returning a reference to an existing snapshot if the request to build a new snapshot
    /// matches the version of an existing snapshot.
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
        let log_tail = self.log_tail.into_iter().map(Into::into).collect();
        if let Some(table_root) = self.table_root {
            let log_segment = LogSegment::for_snapshot(
                engine.storage_handler().as_ref(),
                table_root.join("_delta_log/")?,
                log_tail,
                self.version,
            )?;
            Ok(Snapshot::try_new_from_log_segment(table_root, log_segment, engine)?.into())
        } else {
            let existing_snapshot = self.existing_snapshot.ok_or_else(|| {
                Error::internal_error(
                    "SnapshotBuilder should have either table_root or existing_snapshot",
                )
            })?;
            Snapshot::try_new_from(existing_snapshot, log_tail, engine, self.version)
        }
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

        let snapshot = SnapshotBuilder::new_for(table_root.clone()).build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::new_for(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }
}
