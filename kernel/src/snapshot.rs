//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)

use std::sync::Arc;

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::actions::set_transaction::SetTransactionScanner;
use crate::actions::{Metadata, Protocol, INTERNAL_DOMAIN_PREFIX};
use crate::checkpoint::CheckpointWriter;
use crate::log_segment::{self, ListedLogFiles, LogSegment};
use crate::scan::ScanBuilder;
use crate::schema::{Schema, SchemaRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
use crate::table_properties::TableProperties;
use crate::transaction::Transaction;
use crate::utils::{calculate_transaction_expiration_timestamp, try_parse_uri};
use crate::{DeltaResult, Engine, Error, StorageHandler, Version};
use delta_kernel_derive::internal_api;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use url::Url;

/// Name of the _last_checkpoint file that provides metadata about the last checkpoint
/// created for the table. This file is used as a hint for the engine to quickly locate
/// the latest checkpoint without a full directory listing.
pub(crate) const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
#[derive(PartialEq, Eq)]
pub struct Snapshot {
    log_segment: LogSegment,
    table_configuration: TableConfiguration,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        debug!("Dropping snapshot");
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version())
            .field("metadata", &self.metadata())
            .finish()
    }
}

impl Snapshot {
    #[internal_api]
    pub(crate) fn new(log_segment: LogSegment, table_configuration: TableConfiguration) -> Self {
        Self {
            log_segment,
            table_configuration,
        }
    }

    /// Create a new [`Snapshot`] instance for the given version by parsing the given uri string.
    ///
    /// # Parameters
    ///
    /// - `table_root`: string-encoded URI pointing at the table root (where `_delta_log` folder is
    ///   located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]. None will create a snapshot at the latest
    ///   version of the table.
    pub fn try_from_uri(
        uri: impl AsRef<str>,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let url = try_parse_uri(uri)?;
        Self::try_new(url, engine, version)
    }

    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]. None will create a snapshot at the latest
    ///   version of the table.
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let storage = engine.storage_handler();
        let log_root = table_root.join("_delta_log/")?;

        let checkpoint_hint = read_last_checkpoint(storage.as_ref(), &log_root)?;

        let log_segment =
            LogSegment::for_snapshot(storage.as_ref(), log_root, checkpoint_hint, version)?;

        // try_new_from_log_segment will ensure the protocol is supported
        Self::try_new_from_log_segment(table_root, log_segment, engine)
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
    /// # Parameters
    ///
    /// - `existing_snapshot`: reference to an existing [`Snapshot`]
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]. None will create a snapshot at the latest
    ///   version of the table.
    pub fn try_new_from(
        existing_snapshot: Arc<Snapshot>,
        engine: &dyn Engine,
        version: impl Into<Option<Version>>,
    ) -> DeltaResult<Arc<Self>> {
        let old_log_segment = &existing_snapshot.log_segment;
        let old_version = existing_snapshot.version();
        let new_version = version.into();
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
        let new_listed_files = log_segment::list_log_files_with_version(
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
                "Unexpected state: The newest version in the log {new_end_version} is older than the old version {old_version}")));
        }
        if new_end_version == old_version {
            // No new commits, just return the same snapshot
            return Ok(existing_snapshot.clone());
        }

        if new_log_segment.checkpoint_version.is_some() {
            // we have a checkpoint in the new LogSegment, just construct a new snapshot from that
            let snapshot = Self::try_new_from_log_segment(
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

    /// Create a new [`Snapshot`] instance.
    pub(crate) fn try_new_from_log_segment(
        location: Url,
        log_segment: LogSegment,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let (metadata, protocol) = log_segment.read_metadata(engine)?;
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, location, log_segment.end_version)?;
        Ok(Self {
            log_segment,
            table_configuration,
        })
    }

    /// Creates a [`CheckpointWriter`] for generating a checkpoint from this snapshot.
    ///
    /// See the [`crate::checkpoint`] module documentation for more details on checkpoint types
    /// and the overall checkpoint process.
    pub fn checkpoint(self: Arc<Self>) -> DeltaResult<CheckpointWriter> {
        CheckpointWriter::try_new(self)
    }

    /// Log segment this snapshot uses
    #[internal_api]
    pub(crate) fn log_segment(&self) -> &LogSegment {
        &self.log_segment
    }

    pub fn table_root(&self) -> &Url {
        self.table_configuration.table_root()
    }

    /// Version of this `Snapshot` in the table.
    pub fn version(&self) -> Version {
        self.table_configuration().version()
    }

    /// Table [`type@Schema`] at this `Snapshot`s version.
    pub fn schema(&self) -> SchemaRef {
        self.table_configuration.schema()
    }

    /// Table [`Metadata`] at this `Snapshot`s version.
    #[internal_api]
    pub(crate) fn metadata(&self) -> &Metadata {
        self.table_configuration.metadata()
    }

    /// Table [`Protocol`] at this `Snapshot`s version.
    #[allow(dead_code)]
    #[internal_api]
    pub(crate) fn protocol(&self) -> &Protocol {
        self.table_configuration.protocol()
    }

    /// Get the [`TableProperties`] for this [`Snapshot`].
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration().table_properties()
    }

    /// Get the [`TableConfiguration`] for this [`Snapshot`].
    #[internal_api]
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        &self.table_configuration
    }

    /// Get the [column mapping
    /// mode](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) at this
    /// `Snapshot`s version.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.table_configuration.column_mapping_mode()
    }

    /// Create a [`ScanBuilder`] for an `Arc<Snapshot>`.
    pub fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Consume this `Snapshot` to create a [`ScanBuilder`]
    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Create a [`Transaction`] for this `Arc<Snapshot>`.
    pub fn transaction(self: Arc<Self>) -> DeltaResult<Transaction> {
        Transaction::try_new(self)
    }

    /// Fetch the latest version of the provided `application_id` for this snapshot. Filters the txn based on the SetTransactionRetentionDuration property and lastUpdated
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    // TODO: add a get_app_id_versions to fetch all at once using SetTransactionScanner::get_all
    pub fn get_app_id_version(
        self: Arc<Self>,
        application_id: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<i64>> {
        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(self.table_properties())?;
        let txn = SetTransactionScanner::get_one(
            self.log_segment(),
            application_id,
            engine,
            expiration_timestamp,
        )?;
        Ok(txn.map(|t| t.version))
    }

    /// Fetch the domainMetadata for a specific domain in this snapshot. This returns the latest
    /// configuration for the domain, or None if the domain does not exist.
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    pub fn get_domain_metadata(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<String>> {
        if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
            return Err(Error::generic(
                "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain",
            ));
        }

        domain_metadata_configuration(self.log_segment(), domain, engine)
    }
}

// Note: Schema can not be derived because the checkpoint schema is only known at runtime.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct LastCheckpointHint {
    /// The version of the table when the last checkpoint was made.
    #[allow(unreachable_pub)] // used by acceptance tests (TODO make an fn accessor?)
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub(crate) parts: Option<usize>,
    /// The number of bytes of the checkpoint.
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint.
    pub(crate) num_of_add_files: Option<i64>,
    /// The schema of the checkpoint file.
    pub(crate) checkpoint_schema: Option<Schema>,
    /// The checksum of the last checkpoint JSON.
    pub(crate) checksum: Option<String>,
}

/// Try reading the `_last_checkpoint` file.
///
/// Note that we typically want to ignore a missing/invalid `_last_checkpoint` file without failing
/// the read. Thus, the semantics of this function are to return `None` if the file is not found or
/// is invalid JSON. Unexpected/unrecoverable errors are returned as `Err` case and are assumed to
/// cause failure.
// TODO(#1047): weird that we propagate FileNotFound as part of the iterator instead of top-level
// result coming from storage.read_files
fn read_last_checkpoint(
    storage: &dyn StorageHandler,
    log_root: &Url,
) -> DeltaResult<Option<LastCheckpointHint>> {
    let file_path = log_root.join(LAST_CHECKPOINT_FILE_NAME)?;
    match storage.read_files(vec![(file_path, None)])?.next() {
        Some(Ok(data)) => Ok(serde_json::from_slice(&data)
            .inspect_err(|e| warn!("invalid _last_checkpoint JSON: {e}"))
            .ok()),
        Some(Err(Error::FileNotFound(_))) => Ok(None),
        Some(Err(err)) => Err(err),
        None => {
            warn!("empty _last_checkpoint file");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::object_store::local::LocalFileSystem;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStore;
    use serde_json::json;

    use crate::arrow::array::StringArray;
    use crate::arrow::record_batch::RecordBatch;
    use crate::parquet::arrow::ArrowWriter;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::engine::default::DefaultEngine;
    use crate::engine::sync::SyncEngine;
    use crate::path::ParsedLogPath;
    use crate::utils::test_utils::string_array_to_engine_data;
    use test_utils::{add_commit, delta_path_for_version};

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, Some(1)).unwrap();

        let expected =
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"])).unwrap();
        assert_eq!(snapshot.protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    #[test]
    fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, None).unwrap();

        let expected =
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"])).unwrap();
        assert_eq!(snapshot.protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    // TODO: unify this and lots of stuff in LogSegment tests and test_utils
    async fn commit(store: &InMemory, version: Version, commit: Vec<serde_json::Value>) {
        let commit_data = commit
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join("\n");
        add_commit(store, version, commit_data).await.unwrap();
    }

    // interesting cases for testing Snapshot::new_from:
    // 1. new version < existing version
    // 2. new version == existing version
    // 3. new version > existing version AND
    //   a. log segment hasn't changed
    //   b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
    //   b. log segment for old..=new version has no checkpoint
    //     i. commits have (new protocol, new metadata)
    //     ii. commits have (new protocol, no metadata)
    //     iii. commits have (no protocol, new metadata)
    //     iv. commits have (no protocol, no metadata)
    #[tokio::test]
    async fn test_snapshot_new_from() -> DeltaResult<()> {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let old_snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, Some(1)).unwrap());
        // 1. new version < existing version: error
        let snapshot_res = Snapshot::try_new_from(old_snapshot.clone(), &engine, Some(0));
        assert!(matches!(
            snapshot_res,
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 0 is older than snapshot hint version 1"
        ));

        // 2. new version == existing version
        let snapshot = Snapshot::try_new_from(old_snapshot.clone(), &engine, Some(1)).unwrap();
        let expected = old_snapshot.clone();
        assert_eq!(snapshot, expected);

        // tests Snapshot::new_from by:
        // 1. creating a snapshot with new API for commits 0..=2 (based on old snapshot at 0)
        // 2. comparing with a snapshot created directly at version 2
        //
        // the commits tested are:
        // - commit 0 -> base snapshot at this version
        // - commit 1 -> final snapshots at this version
        //
        // in each test we will modify versions 1 and 2 to test different scenarios
        fn test_new_from(store: Arc<InMemory>) -> DeltaResult<()> {
            let url = Url::parse("memory:///")?;
            let engine = DefaultEngine::new(store, Arc::new(TokioBackgroundExecutor::new()));
            let base_snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, Some(0))?);
            let snapshot = Snapshot::try_new_from(base_snapshot.clone(), &engine, Some(1))?;
            let expected = Snapshot::try_new(url.clone(), &engine, Some(1))?;
            assert_eq!(snapshot, expected.into());
            Ok(())
        }

        // for (3) we will just engineer custom log files
        let store = Arc::new(InMemory::new());
        // everything will have a starting 0 commit with commitInfo, protocol, metadata
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];
        commit(store.as_ref(), 0, commit0.clone()).await;
        // 3. new version > existing version
        // a. no new log segment
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(
            Arc::new(store.fork()),
            Arc::new(TokioBackgroundExecutor::new()),
        );
        let base_snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, Some(0))?);
        let snapshot = Snapshot::try_new_from(base_snapshot.clone(), &engine, None)?;
        let expected = Snapshot::try_new(url.clone(), &engine, Some(0))?;
        assert_eq!(snapshot, expected.into());
        // version exceeds latest version of the table = err
        assert!(matches!(
            Snapshot::try_new_from(base_snapshot.clone(), &engine, Some(1)),
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 1 is newer than the latest version 0"
        ));

        // b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
        let store_3a = store.fork();
        let mut checkpoint1 = commit0.clone();
        commit(&store_3a, 1, commit0.clone()).await;
        checkpoint1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        checkpoint1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint1
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler
            .parse_json(
                string_array_to_engine_data(json_strings),
                crate::actions::get_log_schema().clone(),
            )
            .unwrap();
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let checkpoint: RecordBatch = checkpoint.into();

        // Write the record batch to a Parquet file
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        store
            .put(
                &delta_path_for_version(1, "checkpoint.parquet"),
                buffer.into(),
            )
            .await
            .unwrap();
        test_new_from(store_3a.into())?;

        // c. log segment for old..=new version has no checkpoint
        // i. commits have (new protocol, new metadata)
        let store_3c_i = Arc::new(store.fork());
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit(store_3c_i.as_ref(), 1, commit1).await;
        test_new_from(store_3c_i.clone())?;

        // new commits AND request version > end of log
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(store_3c_i, Arc::new(TokioBackgroundExecutor::new()));
        let base_snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, Some(0))?);
        assert!(matches!(
            Snapshot::try_new_from(base_snapshot.clone(), &engine, Some(2)),
            Err(Error::Generic(msg)) if msg == "LogSegment end version 1 not the same as the specified end version 2"
        ));

        // ii. commits have (new protocol, no metadata)
        let store_3c_ii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1.remove(2); // remove metadata
        commit(&store_3c_ii, 1, commit1).await;
        test_new_from(store_3c_ii.into())?;

        // iii. commits have (no protocol, new metadata)
        let store_3c_iii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit1.remove(1); // remove protocol
        commit(&store_3c_iii, 1, commit1).await;
        test_new_from(store_3c_iii.into())?;

        // iv. commits have (no protocol, no metadata)
        let store_3c_iv = store.fork();
        let commit1 = vec![commit0[0].clone()];
        commit(&store_3c_iv, 1, commit1).await;
        test_new_from(store_3c_iv.into())?;

        Ok(())
    }

    // test new CRC in new log segment (old log segment has old CRC)
    #[tokio::test]
    async fn test_snapshot_new_from_crc() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let protocol = |reader_version, writer_version| {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": writer_version
                }
            })
        };
        let metadata = json!({
            "metaData": {
                "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        });
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 1),
            metadata.clone(),
        ];
        let commit1 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 2),
        ];

        // commit 0 and 1 jsons
        commit(&store, 0, commit0.clone()).await;
        commit(&store, 1, commit1).await;

        // a) CRC: old one has 0.crc, no new one (expect 0.crc)
        // b) CRC: old one has 0.crc, new one has 1.crc (expect 1.crc)
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 1),
        });

        // put the old crc
        let path = delta_path_for_version(0, "crc");
        store.put(&path, crc.to_string().into()).await?;

        // base snapshot is at version 0
        let base_snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, Some(0))?);

        // first test: no new crc
        let snapshot = Snapshot::try_new_from(base_snapshot.clone(), &engine, Some(1))?;
        let expected = Snapshot::try_new(url.clone(), &engine, Some(1))?;
        assert_eq!(snapshot, expected.into());
        assert_eq!(
            snapshot
                .log_segment
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            0
        );

        // second test: new crc
        // put the new crc
        let path = delta_path_for_version(1, "crc");
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 2),
        });
        store.put(&path, crc.to_string().into()).await?;
        let snapshot = Snapshot::try_new_from(base_snapshot.clone(), &engine, Some(1))?;
        let expected = Snapshot::try_new(url.clone(), &engine, Some(1))?;
        assert_eq!(snapshot, expected.into());
        assert_eq!(
            snapshot
                .log_segment
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            1
        );

        Ok(())
    }

    #[test]
    fn test_read_table_with_missing_last_checkpoint() {
        // this table doesn't have a _last_checkpoint file
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);
        let cp = read_last_checkpoint(&storage, &url).unwrap();
        assert!(cp.is_none());
    }

    fn valid_last_checkpoint() -> Vec<u8> {
        r#"{"size":8,"sizeInBytes":21857,"version":1}"#.as_bytes().to_vec()
    }

    #[test]
    fn test_read_table_with_empty_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // do a _last_checkpoint file with "{}" as content
        let empty = "{}".as_bytes().to_vec();
        let invalid_path = Path::from("invalid/_last_checkpoint");

        tokio::runtime::Runtime::new()
            .expect("create tokio runtime")
            .block_on(async {
                store
                    .put(&invalid_path, empty.into())
                    .await
                    .expect("put _last_checkpoint");
            });

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = read_last_checkpoint(&storage, &url).expect("read last checkpoint");
        assert!(invalid.is_none())
    }

    #[test]
    fn test_read_table_with_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // put a valid/invalid _last_checkpoint file
        let data = valid_last_checkpoint();
        let invalid_data = "invalid".as_bytes().to_vec();
        let path = Path::from("valid/_last_checkpoint");
        let invalid_path = Path::from("invalid/_last_checkpoint");

        tokio::runtime::Runtime::new()
            .expect("create tokio runtime")
            .block_on(async {
                store
                    .put(&path, data.into())
                    .await
                    .expect("put _last_checkpoint");
                store
                    .put(&invalid_path, invalid_data.into())
                    .await
                    .expect("put _last_checkpoint");
            });

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);
        let url = Url::parse("memory:///valid/").expect("valid url");
        let valid = read_last_checkpoint(&storage, &url).expect("read last checkpoint");
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = read_last_checkpoint(&storage, &url).expect("read last checkpoint");
        let expected = LastCheckpointHint {
            version: 1,
            size: 8,
            parts: None,
            size_in_bytes: Some(21857),
            num_of_add_files: None,
            checkpoint_schema: None,
            checksum: None,
        };
        assert_eq!(valid.unwrap(), expected);
        assert!(invalid.is_none());
    }

    #[test_log::test]
    fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(location, &engine, None).unwrap();

        assert_eq!(snapshot.log_segment.checkpoint_parts.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(snapshot.log_segment.checkpoint_parts[0].location.clone())
                .unwrap()
                .unwrap()
                .version,
            2,
        );
        assert_eq!(snapshot.log_segment.ascending_commit_files.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(
                snapshot.log_segment.ascending_commit_files[0]
                    .location
                    .clone()
            )
            .unwrap()
            .unwrap()
            .version,
            3,
        );
    }

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let url = Url::parse("memory:///")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // commit0
        // - domain1: not removed
        // - domain2: not removed
        let commit = [
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain3",
                    "configuration": "domain3_commit0",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(store.clone().as_ref(), 0, commit).await.unwrap();

        // commit1
        // - domain1: removed
        // - domain2: not-removed
        // - internal domain
        let commit = [
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit1",
                    "removed": true
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit1",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "delta.domain3",
                    "configuration": "domain3_commit1",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(store.as_ref(), 1, commit).await.unwrap();

        let snapshot = Arc::new(Snapshot::try_new(url.clone(), &engine, None)?);

        assert_eq!(snapshot.get_domain_metadata("domain1", &engine)?, None);
        assert_eq!(
            snapshot.get_domain_metadata("domain2", &engine)?,
            Some("domain2_commit1".to_string())
        );
        assert_eq!(
            snapshot.get_domain_metadata("domain3", &engine)?,
            Some("domain3_commit0".to_string())
        );
        let err = snapshot
            .get_domain_metadata("delta.domain3", &engine)
            .unwrap_err();
        assert!(matches!(err, Error::Generic(msg) if
                msg == "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"));
        Ok(())
    }
}
