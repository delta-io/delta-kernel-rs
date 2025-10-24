//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)

use std::sync::Arc;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::actions::set_transaction::SetTransactionScanner;
use crate::actions::INTERNAL_DOMAIN_PREFIX;
use crate::checkpoint::CheckpointWriter;
use crate::committer::Committer;
use crate::listed_log_files::ListedLogFiles;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::SchemaRef;
use crate::table_configuration::{InCommitTimestampEnablement, TableConfiguration};
use crate::table_properties::TableProperties;
use crate::transaction::Transaction;
use crate::LogCompactionWriter;
use crate::{DeltaResult, Engine, Error, Version};
use delta_kernel_derive::internal_api;

mod builder;
pub use builder::SnapshotBuilder;

use tracing::debug;
use url::Url;

pub type SnapshotRef = Arc<Snapshot>;

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
            .field("metadata", &self.table_configuration().metadata())
            .finish()
    }
}

impl Snapshot {
    /// Create a new [`SnapshotBuilder`] to build a new [`Snapshot`] for a given table root. If you
    /// instead have an existing [`Snapshot`] you would like to do minimal work to update, consider
    /// using
    pub fn builder_for(table_root: Url) -> SnapshotBuilder {
        SnapshotBuilder::new_for(table_root)
    }

    /// Create a new [`SnapshotBuilder`] to incrementally update a [`Snapshot`] to a more recent
    /// version.
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
    pub fn builder_from(existing_snapshot: SnapshotRef) -> SnapshotBuilder {
        SnapshotBuilder::new_from(existing_snapshot)
    }

    #[internal_api]
    pub(crate) fn new(log_segment: LogSegment, table_configuration: TableConfiguration) -> Self {
        Self {
            log_segment,
            table_configuration,
        }
    }

    /// Create a new [`Snapshot`] instance from an existing [`Snapshot`]. This is useful when you
    /// already have a [`Snapshot`] lying around and want to do the minimal work to 'update' the
    /// snapshot to a later version.
    fn try_new_from(
        existing_snapshot: Arc<Snapshot>,
        log_tail: Vec<ParsedLogPath>,
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
        let new_listed_files = ListedLogFiles::list(
            storage.as_ref(),
            &log_root,
            log_tail,
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
        // Save the latest_commit before moving new_listed_files
        let new_latest_commit_file = new_listed_files.latest_commit_file.clone();
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

        // Use the new latest_commit if available, otherwise use the old one
        // This handles the case where the new listing returned no commits
        let latest_commit_file =
            new_latest_commit_file.or_else(|| old_log_segment.latest_commit_file.clone());
        // we can pass in just the old checkpoint parts since by the time we reach this line, we
        // know there are no checkpoints in the new log segment.
        let combined_log_segment = LogSegment::try_new(
            ListedLogFiles {
                ascending_commit_files,
                ascending_compaction_files,
                checkpoint_parts: old_log_segment.checkpoint_parts.clone(),
                latest_crc_file,
                latest_commit_file,
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

    /// Creates a [`LogCompactionWriter`] for generating a log compaction file.
    ///
    /// Log compaction aggregates commit files in a version range into a single compacted file,
    /// improving performance by reducing the number of files to process during log replay.
    ///
    /// # Parameters
    /// - `start_version`: The first version to include in the compaction (inclusive)
    /// - `end_version`: The last version to include in the compaction (inclusive)
    ///
    /// # Returns
    /// A [`LogCompactionWriter`] that can be used to generate the compaction file.
    pub fn log_compaction_writer(
        self: Arc<Self>,
        start_version: Version,
        end_version: Version,
    ) -> DeltaResult<LogCompactionWriter> {
        LogCompactionWriter::try_new(self, start_version, end_version)
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

    /// Table [`Schema`] at this `Snapshot`s version.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> SchemaRef {
        self.table_configuration.schema()
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

    /// Create a [`ScanBuilder`] for an `SnapshotRef`.
    pub fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Create a [`Transaction`] for this `SnapshotRef`. With the specified [`Committer`].
    pub fn transaction(self: Arc<Self>, committer: Box<dyn Committer>) -> DeltaResult<Transaction> {
        Transaction::try_new(self, committer)
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

    /// Get the In-Commit Timestamp (ICT) for this snapshot.
    ///
    /// Returns the `inCommitTimestamp` from the CommitInfo action of the commit that created this snapshot.
    ///
    /// # Returns
    /// - `Ok(Some(timestamp))` - ICT is enabled and available for this version
    /// - `Ok(None)` - ICT is not enabled
    /// - `Err(...)` - ICT is enabled but cannot be read, or enablement version is invalid
    pub(crate) fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        // Get ICT enablement info and check if we should read ICT for this version
        let enablement = self
            .table_configuration()
            .in_commit_timestamp_enablement()?;

        // Return None if ICT is not enabled at all
        if matches!(enablement, InCommitTimestampEnablement::NotEnabled) {
            return Ok(None);
        }

        // If ICT is enabled with an enablement version, verify the enablement version is not in the future
        if let InCommitTimestampEnablement::Enabled {
            enablement: Some((enablement_version, _)),
        } = enablement
        {
            if self.version() < enablement_version {
                return Err(Error::generic(format!(
                    "Invalid state: snapshot at version {} has ICT enablement version {} in the future",
                    self.version(),
                    enablement_version
                )));
            }
        }

        // Read the ICT from latest_commit_file
        match &self.log_segment.latest_commit_file {
            Some(commit_file_meta) => {
                let ict = commit_file_meta.read_in_commit_timestamp(engine)?;
                Ok(Some(ict))
            }
            None => Err(Error::generic("Last commit file not found in log segment")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use serde_json::json;
    use test_utils::{add_commit, delta_path_for_version};

    use crate::actions::Protocol;
    use crate::arrow::array::StringArray;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::engine::default::DefaultEngine;
    use crate::engine::sync::SyncEngine;
    use crate::last_checkpoint_hint::LastCheckpointHint;
    use crate::listed_log_files::ListedLogFiles;
    use crate::log_segment::LogSegment;
    use crate::parquet::arrow::ArrowWriter;
    use crate::path::ParsedLogPath;
    use crate::utils::test_utils::{assert_result_error_with_message, string_array_to_engine_data};

    /// Helper function to create a commitInfo action with optional ICT
    fn create_commit_info(timestamp: i64, ict: Option<i64>) -> serde_json::Value {
        let mut commit_info = json!({
            "timestamp": timestamp,
            "operation": "WRITE",
        });

        if let Some(ict_value) = ict {
            commit_info["inCommitTimestamp"] = json!(ict_value);
        }

        json!({
            "commitInfo": commit_info
        })
    }

    fn create_protocol(ict_enabled: bool, min_reader_version: Option<u32>) -> serde_json::Value {
        let reader_version = min_reader_version.unwrap_or(1);

        if ict_enabled {
            let mut protocol = json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": 7,
                    "writerFeatures": ["inCommitTimestamp"]
                }
            });

            // Only include readerFeatures if minReaderVersion >= 3
            if reader_version >= 3 {
                protocol["protocol"]["readerFeatures"] = json!([]);
            }

            protocol
        } else {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": 2
                }
            })
        }
    }

    fn create_metadata(
        id: Option<&str>,
        schema_string: Option<&str>,
        created_time: Option<u64>,
        ict_config: Option<(String, String)>,
        ict_enabled_but_missing_version: bool,
    ) -> serde_json::Value {
        let config = if ict_enabled_but_missing_version {
            // Special case for testing ICT enabled but missing enablement info
            json!({
                "delta.enableInCommitTimestamps": "true"
            })
        } else if let Some((enablement_version, enablement_timestamp)) = ict_config {
            json!({
                "delta.enableInCommitTimestamps": "true",
                "delta.inCommitTimestampEnablementVersion": enablement_version,
                "delta.inCommitTimestampEnablementTimestamp": enablement_timestamp
            })
        } else {
            json!({})
        };

        json!({
            "metaData": {
                "id": id.unwrap_or("testId"),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string.unwrap_or("{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"),
                "partitionColumns": [],
                "configuration": config,
                "createdTime": created_time.unwrap_or(1587968586154u64)
            }
        })
    }

    fn create_basic_commit(ict_enabled: bool, ict_config: Option<(String, String)>) -> String {
        let protocol = create_protocol(ict_enabled, None);
        let metadata = create_metadata(None, None, None, ict_config, false);
        format!("{}\n{}", protocol, metadata)
    }

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();

        let expected =
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"])).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

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
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let expected =
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"])).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

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
        let old_snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        // 1. new version < existing version: error
        let snapshot_res = Snapshot::builder_from(old_snapshot.clone())
            .at_version(0)
            .build(&engine);
        assert!(matches!(
            snapshot_res,
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 0 is older than snapshot hint version 1"
        ));

        // 2. new version == existing version
        let snapshot = Snapshot::builder_from(old_snapshot.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
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
            let base_snapshot = Snapshot::builder_for(url.clone())
                .at_version(0)
                .build(&engine)?;
            let snapshot = Snapshot::builder_from(base_snapshot.clone())
                .at_version(1)
                .build(&engine)?;
            let expected = Snapshot::builder_for(url.clone())
                .at_version(1)
                .build(&engine)?;
            assert_eq!(snapshot, expected);
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
        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;
        let snapshot = Snapshot::builder_from(base_snapshot.clone()).build(&engine)?;
        let expected = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
        // version exceeds latest version of the table = err
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(1).build(&engine),
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
                crate::actions::get_commit_schema().clone(),
            )
            .unwrap();
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let checkpoint: RecordBatch = checkpoint.into();

        // Write the record batch to a Parquet file
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        store_3a
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
        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(2).build(&engine),
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
        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;

        // first test: no new crc
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        let expected = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
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
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        let expected = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
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
        let cp = LastCheckpointHint::try_read(&storage, &url).unwrap();
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
        let invalid = LastCheckpointHint::try_read(&storage, &url).expect("read last checkpoint");
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
        let valid = LastCheckpointHint::try_read(&storage, &url).expect("read last checkpoint");
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = LastCheckpointHint::try_read(&storage, &url).expect("read last checkpoint");
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
        let snapshot = Snapshot::builder_for(location).build(&engine).unwrap();

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

        let snapshot = Snapshot::builder_for(url.clone()).build(&engine)?;

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

    #[test]
    fn test_log_compaction_writer() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        // Test creating a log compaction writer
        let writer = snapshot.clone().log_compaction_writer(0, 1).unwrap();
        let path = writer.compaction_path();

        // Verify the path format is correct
        let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
        assert!(path.to_string().ends_with(expected_filename));

        // Test invalid version range (start >= end)
        let result = snapshot.clone().log_compaction_writer(2, 1);
        assert_result_error_with_message(result, "Invalid version range");

        // Test equal version range (also invalid)
        let result = snapshot.log_compaction_writer(1, 1);
        assert_result_error_with_message(result, "Invalid version range");
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_disabled() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let url = url::Url::parse("memory://test/")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create a basic commit without ICT enabled
        let commit0 = create_basic_commit(false, None);
        add_commit(store.as_ref(), 0, commit0).await?;

        let snapshot = Snapshot::builder_for(url).build(&engine)?;

        // When ICT is disabled, get_timestamp should return None
        let result = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_enablement_timeline() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = Arc::new(InMemory::new());
        let url = url::Url::parse("memory://test/")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create initial commit without ICT
        let commit0 = create_basic_commit(false, None);
        add_commit(store.as_ref(), 0, commit0).await?;

        // Create commit that enables ICT (version 1 = enablement version)
        let commit1 =
            create_basic_commit(true, Some(("1".to_string(), "1587968586154".to_string())));
        add_commit(store.as_ref(), 1, commit1).await?;

        // Create commit with ICT enabled
        let expected_timestamp = 1587968586200i64;
        let commit2 = format!(
            r#"{{"commitInfo":{{"timestamp":1587968586154,"inCommitTimestamp":{expected_timestamp},"operation":"WRITE"}}}}"#,
        );
        add_commit(store.as_ref(), 2, commit2.to_string()).await?;

        // Read snapshot at version 0 (before ICT enablement)
        let snapshot_v0 = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;
        // This snapshot version predates ICT enablement, so ICT is not available
        let result_v0 = snapshot_v0.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v0, None);

        // Read snapshot at version 2 (after ICT enabled)
        let snapshot_v2 = Snapshot::builder_for(url).at_version(2).build(&engine)?;
        // When ICT is enabled and available, timestamp() should return inCommitTimestamp
        let result_v2 = snapshot_v2.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v2, Some(expected_timestamp));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_enablement_version_in_future() -> DeltaResult<()> {
        // Test invalid state where snapshot has enablement version in the future - should error
        let url = Url::parse("memory:///table2")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let commit_data = [
            json!({
                "protocol": {
                    "minReaderVersion": 3,
                    "minWriterVersion": 7,
                    "readerFeatures": [],
                    "writerFeatures": ["inCommitTimestamp"]
                }
            }),
            json!({
                "metaData": {
                    "id": "test_id2",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {
                        "delta.enableInCommitTimestamps": "true",
                        "delta.inCommitTimestampEnablementVersion": "5", // Enablement after version 1
                        "delta.inCommitTimestampEnablementTimestamp": "1612345678"
                    },
                    "createdTime": 1677811175819u64
                }
            }),
        ];
        commit(store.as_ref(), 0, commit_data.to_vec()).await;

        // Create commit that predates ICT enablement (no inCommitTimestamp)
        let commit_predates = [create_commit_info(1234567890, None)];
        commit(store.as_ref(), 1, commit_predates.to_vec()).await;

        let snapshot_predates = Snapshot::builder_for(url).at_version(1).build(&engine)?;
        let result_predates = snapshot_predates.get_in_commit_timestamp(&engine);

        // Version 1 with enablement at version 5 is invalid - should error
        assert_result_error_with_message(
            result_predates,
            "Invalid state: snapshot at version 1 has ICT enablement version 5 in the future",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_missing_ict_when_enabled() -> DeltaResult<()> {
        // Test missing ICT when it should be present - should error
        let url = Url::parse("memory:///table3")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let commit_data = [
            create_protocol(true, Some(3)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())),
                false,
            ),
        ];
        commit(store.as_ref(), 0, commit_data.to_vec()).await; // ICT enabled from version 0

        // Create commit without ICT despite being enabled (corrupt case)
        let commit_missing_ict = [create_commit_info(1234567890, None)];
        commit(store.as_ref(), 1, commit_missing_ict.to_vec()).await;

        let snapshot_missing = Snapshot::builder_for(url).at_version(1).build(&engine)?;
        let result = snapshot_missing.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "In-Commit Timestamp not found");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_fails_when_commit_missing() -> DeltaResult<()> {
        // When ICT is enabled but commit file is not found in log segment,
        // get_in_commit_timestamp should return an error

        let url = Url::parse("memory:///missing_commit_test")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create initial commit with ICT enabled
        let commit_data = [
            create_protocol(true, Some(3)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())), // ICT enabled from version 0
                false,
            ),
        ];
        commit(store.as_ref(), 0, commit_data.to_vec()).await;

        // Build snapshot to get table configuration
        let snapshot = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;

        // Create a log segment with only checkpoint and no commit file (simulating scenario
        // where a checkpoint exists but the commit file has been cleaned up)
        let checkpoint_parts = vec![ParsedLogPath::try_from(crate::FileMeta {
            location: url.join("_delta_log/00000000000000000000.checkpoint.parquet")?,
            last_modified: 0,
            size: 100,
        })?
        .unwrap()];

        let listed_files = ListedLogFiles {
            ascending_commit_files: vec![],
            ascending_compaction_files: vec![],
            checkpoint_parts,
            latest_crc_file: None,
            latest_commit_file: None, // No commit file
        };

        let log_segment = LogSegment::try_new(listed_files, url.join("_delta_log/")?, Some(0))?;
        let table_config = snapshot.table_configuration().clone();

        // Create snapshot without commit file in log segment
        let snapshot_no_commit = Snapshot::new(log_segment, table_config);

        // Should return an error when commit file is missing
        let result = snapshot_no_commit.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "Last commit file not found in log segment");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_with_checkpoint_and_commit_same_version() -> DeltaResult<()> {
        // Test the scenario where both checkpoint and commit exist at the same version with ICT enabled.
        let url = Url::parse("memory:///checkpoint_commit_test")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create 00000000000000000000.json with ICT enabled
        let commit0_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(3)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];
        commit(store.as_ref(), 0, commit0_data.to_vec()).await;

        // Create 00000000000000000001.checkpoint.parquet
        let checkpoint_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(3)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint_data
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler.parse_json(
            string_array_to_engine_data(json_strings),
            crate::actions::get_commit_schema().clone(),
        )?;
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed)?;
        let checkpoint: RecordBatch = checkpoint.into();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        let checkpoint_path = delta_path_for_version(1, "checkpoint.parquet");
        store.put(&checkpoint_path, buffer.into()).await?;

        // Create 00000000000000000001.json with ICT
        let expected_ict = 1587968586200i64;
        let commit1_data = [create_commit_info(1587968586200, Some(expected_ict))];
        commit(store.as_ref(), 1, commit1_data.to_vec()).await;

        // Build snapshot - LogSegment will filter out the commit file because checkpoint exists at same version
        let snapshot = Snapshot::builder_for(url).at_version(1).build(&engine)?;

        // We should successfully read ICT by falling back to storage
        let timestamp = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(timestamp, Some(expected_ict));

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_empty_log_tail() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create initial commit
        let commit0 = vec![
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            }),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];
        commit(store.as_ref(), 0, commit0).await;

        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(0)
            .build(&engine)?;

        // Test with empty log tail - should return same snapshot
        let result = Snapshot::try_new_from(base_snapshot.clone(), vec![], &engine, None)?;
        assert_eq!(result, base_snapshot);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_latest_commit_preservation() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create commits 0-2
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(store.as_ref(), 0, base_commit.clone()).await;
        commit(
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;
        commit(
            store.as_ref(),
            2,
            vec![json!({"commitInfo": {"timestamp": 5678}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)?;

        // Verify base snapshot has latest_commit_file at version 1
        assert_eq!(
            base_snapshot
                .log_segment
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(1)
        );

        // Create log_tail with FileMeta for version 2
        let commit_2_url = url.join("_delta_log/")?.join("00000000000000000002.json")?;
        let file_meta = crate::FileMeta {
            location: commit_2_url,
            last_modified: 1234567890,
            size: 100,
        };
        let parsed_path = ParsedLogPath::try_from(file_meta)?
            .ok_or_else(|| Error::Generic("Failed to parse log path".to_string()))?;
        let log_tail = vec![parsed_path];

        // Create new snapshot from base to version 2 using try_new_from directly
        let new_snapshot =
            Snapshot::try_new_from(base_snapshot.clone(), log_tail, &engine, Some(2))?;

        // Latest commit should now be version 2
        assert_eq!(
            new_snapshot
                .log_segment
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_version_boundary_cases() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // Create commits
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(store.as_ref(), 0, base_commit).await;
        commit(
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)?;

        // Test requesting same version - should return same snapshot
        let same_version = Snapshot::try_new_from(base_snapshot.clone(), vec![], &engine, Some(1))?;
        assert!(Arc::ptr_eq(&same_version, &base_snapshot));

        // Test requesting older version - should error
        let older_version = Snapshot::try_new_from(base_snapshot.clone(), vec![], &engine, Some(0));
        assert!(matches!(
            older_version,
            Err(Error::Generic(msg)) if msg.contains("older than snapshot hint version")
        ));

        Ok(())
    }
}
