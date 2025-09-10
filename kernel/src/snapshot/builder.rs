//! Builder for creating [`Snapshot`] instances.

use std::sync::Arc;

use crate::listed_log_files::ListedLogFiles;
use crate::log_segment::LogSegment;
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

use url::Url;

/// Builder for creating [`Snapshot`] instances.
///
/// # Example
///
/// ```no_run
/// # use delta_kernel::{Snapshot, Engine};
/// # use std::sync::Arc;
/// # use url::Url;
/// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
/// let table_root = Url::parse("file:///path/to/table")?;
///
/// // Build a snapshot from scratch
/// let snapshot = Snapshot::builder(table_root.clone())
///     .at_version(5) // Optional: specify a time-travel version (default is latest version)
///     .build(engine)?;
///
/// // Build incrementally from an existing snapshot
/// let updated_snapshot = Snapshot::builder(table_root.clone())
///     .from_snapshot(snapshot.clone())
///     .at_version(10)
///     .build(engine)?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct SnapshotBuilder {
    table_root: Option<Url>,
    version: Option<Version>,
    existing_snapshot: Option<Arc<Snapshot>>,
}

impl SnapshotBuilder {
    /// Set the table root URL. This is required when there is no existing snapshot hint (see
    /// [Self::from_snapshot]).
    pub fn with_table_root(mut self, table_root: Url) -> Self {
        self.table_root = Some(table_root);
        self
    }

    /// Set the target version of the [`Snapshot`]. When omitted, the Snapshot is created at the
    /// latest version of the table.
    pub fn at_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Create a new [`Snapshot`] instance from an existing [`Snapshot`]. This is useful when you
    /// already have a [`Snapshot`] lying around and want to do the minimal work to 'update' the
    /// snapshot to a later version.
    ///
    /// We implement a simple heuristic:
    /// 1. if the new version == existing version, just return the existing snapshot
    /// 2. if the new version < existing version, error: there is no optimization to do here
    /// 3. list from (existing checkpoint version + 1) onward (or just existing snapshot version if
    ///    no checkpoint)
    /// 4. a. if new checkpoint is found: just create a new snapshot from that checkpoint (and
    ///    commits after it)
    ///    b. if no new checkpoint is found: do lightweight P+M replay on the latest commits (after
    ///    ensuring we only retain commits > any checkpoints)
    ///
    /// NOTE: if a table_root is already set, it must match the table_root of the existing
    /// snapshot.
    pub fn from_snapshot(mut self, snapshot: Arc<Snapshot>) -> Self {
        self.existing_snapshot = Some(snapshot);
        self
    }

    /// Create a new [`Snapshot`] instance.
    ///
    /// Returns an `Arc<Snapshot>` to support efficient incremental updates.
    /// When building from an existing snapshot and the version hasn't changed,
    /// this returns the same `Arc` without creating a new snapshot.
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Arc<Snapshot>> {
        // Note: since both table_root AND existing_snapshot are optional but one is actually
        // required, we check here that one is set and if both are set they must match.
        // TODO: we could improve this
        match (&self.table_root, &self.existing_snapshot) {
            (Some(table_root), Some(existing)) => require!(
                table_root == existing.table_root(),
                Error::generic("table_root must match the table_root of existing snapshot")
            ),
            (None, None) => return Err(Error::generic(
                "Either table_root or existing_snapshot must be provided to construct a Snapshot",
            )),
            _ => (),
        }

        // If we don't have an existing snapshot, build from scratch.
        let Some(existing_snapshot) = self.existing_snapshot else {
            // if we don't have an existing snapshot the log_root is required
            let table_root = self.table_root.ok_or_else(|| {
                Error::generic(
                    "table_root is required to build a Snapshot without existing snapshot hint",
                )
            })?;
            let log_segment = LogSegment::for_snapshot(
                engine.storage_handler().as_ref(),
                table_root.join("_delta_log/")?,
                self.version,
            )?;
            let snapshot = Snapshot::try_new_from_log_segment(table_root, log_segment, engine)?;
            return Ok(Arc::new(snapshot));
        };

        let old_log_segment = &existing_snapshot.log_segment;
        let old_version = existing_snapshot.version();
        let new_version = self.version;
        if let Some(new_version) = new_version {
            if new_version == old_version {
                // Re-requesting the same version
                return Ok(existing_snapshot.clone());
            }
            if new_version < old_version {
                // Hint is too new: error since this is effectively an incorrect optimization
                return Err(Error::Generic(format!(
                "Requested snapshot version {new_version} is older than snapshot hint version {old_version}"
                )));
            }
        }

        let log_root = old_log_segment.log_root.clone();
        let storage = engine.storage_handler();

        // Start listing just after the previous segment's checkpoint, if any
        let listing_start = old_log_segment.checkpoint_version.unwrap_or(0) + 1;

        // Check for new commits (and CRC)
        let new_listed_files = ListedLogFiles::list(
            storage.as_ref(),
            &log_root,
            Some(listing_start),
            new_version,
        )?;

        // NB: we need to check both checkpoints and commits since we filter commits at and below
        // the checkpoint version. Example: if we have a checkpoint + commit at version 1, the log
        // listing above will only return the checkpoint and not the commit.
        if new_listed_files.ascending_commit_files.is_empty()
            && new_listed_files.checkpoint_parts.is_empty()
        {
            match new_version {
                Some(new_version) if new_version != old_version => {
                    // No new commits, but we are looking for a new version
                    return Err(Error::Generic(format!(
                        "Requested snapshot version {new_version} is newer than the latest version {old_version}"
                    )));
                }
                _ => {
                    // No new commits, just return the same snapshot
                    return Ok(existing_snapshot.clone());
                }
            }
        }

        // create a log segment just from existing_checkpoint.version -> new_version
        // OR could be from 1 -> new_version
        let mut new_log_segment =
            LogSegment::try_new(new_listed_files, log_root.clone(), new_version)?;

        let new_end_version = new_log_segment.end_version;
        if new_end_version < old_version {
            // we should never see a new log segment with a version < the existing snapshot
            // version, that would mean a commit was incorrectly deleted from the log
            return Err(Error::Generic(format!(
                "Unexpected state: The newest version in the log {new_end_version} is older than the
                 old version {old_version}")));
        }
        if new_end_version == old_version {
            // No new commits, just return the same snapshot
            return Ok(existing_snapshot.clone());
        }

        if new_log_segment.checkpoint_version.is_some() {
            // we have a checkpoint in the new LogSegment, just construct a new snapshot from that
            let snapshot = Snapshot::try_new_from_log_segment(
                existing_snapshot.table_root().clone(),
                new_log_segment,
                engine,
            );
            return Ok(Arc::new(snapshot?));
        }

        // after this point, we incrementally update the snapshot with the new log segment.
        // first we remove the 'overlap' in commits, example:
        //
        //    old logsegment checkpoint1-commit1-commit2-commit3
        // 1. new logsegment             commit1-commit2-commit3
        // 2. new logsegment             commit1-commit2-commit3-commit4
        // 3. new logsegment                     checkpoint2+commit2-commit3-commit4
        //
        // retain does
        // 1. new logsegment             [empty] -> caught above
        // 2. new logsegment             [commit4]
        // 3. new logsegment             [checkpoint2-commit3] -> caught above
        new_log_segment
            .ascending_commit_files
            .retain(|log_path| old_version < log_path.version);

        // we have new commits and no new checkpoint: we replay new commits for P+M and then
        // create a new snapshot by combining LogSegments and building a new TableConfiguration
        let (new_metadata, new_protocol) = new_log_segment.protocol_and_metadata(engine)?;
        let table_configuration = TableConfiguration::try_new_from(
            existing_snapshot.table_configuration(),
            new_metadata,
            new_protocol,
            new_log_segment.end_version,
        )?;

        // NB: we must add the new log segment to the existing snapshot's log segment
        let mut ascending_commit_files = old_log_segment.ascending_commit_files.clone();
        ascending_commit_files.extend(new_log_segment.ascending_commit_files);
        let mut ascending_compaction_files = old_log_segment.ascending_compaction_files.clone();
        ascending_compaction_files.extend(new_log_segment.ascending_compaction_files);

        // Note that we _could_ go backwards if someone deletes a CRC:
        // old listing: 1, 2, 2.crc, 3, 3.crc (latest is 3.crc)
        // new listing: 1, 2, 2.crc, 3        (latest is 2.crc)
        // and we would still pick the new listing's (older) CRC file since it ostensibly still
        // exists
        let latest_crc_file = new_log_segment
            .latest_crc_file
            .or_else(|| old_log_segment.latest_crc_file.clone());

        // we can pass in just the old checkpoint parts since by the time we reach this line, we
        // know there are no checkpoints in the new log segment.
        let combined_log_segment = LogSegment::try_new(
            ListedLogFiles {
                ascending_commit_files,
                ascending_compaction_files,
                checkpoint_parts: old_log_segment.checkpoint_parts.clone(),
                latest_crc_file,
            },
            log_root,
            new_version,
        )?;
        Ok(Arc::new(Snapshot::new(
            combined_log_segment,
            table_configuration,
        )))
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

        let snapshot = SnapshotBuilder::default()
            .with_table_root(table_root.clone())
            .build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::default()
            .with_table_root(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }

    #[test]
    fn test_snapshot_builder_incremental() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_table(&store, &table_root)?;

        // Create initial snapshot at version 0
        let snapshot_v0 = SnapshotBuilder::default()
            .with_table_root(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot_v0.version(), 0);

        // Create incremental snapshot to version 1
        let snapshot_v1 = SnapshotBuilder::default()
            .with_table_root(table_root.clone())
            .from_snapshot(snapshot_v0.clone())
            .at_version(1)
            .build(engine)?;
        assert_eq!(snapshot_v1.version(), 1);

        // Try to get the same version - should return the same Arc
        let snapshot_v1_again = SnapshotBuilder::default()
            .with_table_root(table_root.clone())
            .from_snapshot(snapshot_v1.clone())
            .at_version(1)
            .build(engine)?;
        assert!(Arc::ptr_eq(&snapshot_v1, &snapshot_v1_again));

        Ok(())
    }
}
