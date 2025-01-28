//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::log_segment::LogSegment;
use crate::scan::ScanBuilder;
use crate::schema::Schema;
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Engine, Error, FileSystemClient, Version};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
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
    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let fs_client = engine.get_file_system_client();
        let log_root = table_root.join("_delta_log/")?;

        let checkpoint_hint = read_last_checkpoint(fs_client.as_ref(), &log_root)?;

        let log_segment =
            LogSegment::for_snapshot(fs_client.as_ref(), log_root, checkpoint_hint, version)?;

        // try_new_from_log_segment will ensure the protocol is supported
        Self::try_new_from_log_segment(table_root, log_segment, engine)
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

    /// Log segment this snapshot uses
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
    pub fn schema(&self) -> &Schema {
        self.table_configuration.schema()
    }

    /// Table [`Metadata`] at this `Snapshot`s version.
    pub fn metadata(&self) -> &Metadata {
        self.table_configuration.metadata()
    }

    /// Table [`Protocol`] at this `Snapshot`s version.
    pub fn protocol(&self) -> &Protocol {
        self.table_configuration.protocol()
    }

    /// Get the [`TableProperties`] for this [`Snapshot`].
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration().table_properties()
    }
    /// Get the [`TableConfiguration`] for this [`Snapshot`].
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct CheckpointMetadata {
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
///
/// TODO: java kernel retries three times before failing, should we do the same?
fn read_last_checkpoint(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<Option<CheckpointMetadata>> {
    let file_path = log_root.join(LAST_CHECKPOINT_FILE_NAME)?;
    match fs_client
        .read_files(vec![(file_path, None)])
        .and_then(|mut data| data.next().expect("read_files should return one file"))
    {
        Ok(data) => Ok(serde_json::from_slice(&data)
            .inspect_err(|e| warn!("invalid _last_checkpoint JSON: {e}"))
            .ok()),
        Err(Error::FileNotFound(_)) => Ok(None),
        Err(err) => Err(err),
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

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreFileSystemClient;
    use crate::engine::sync::SyncEngine;
    use crate::path::ParsedLogPath;
    use crate::schema::StructType;

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
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), &expected);
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
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), &expected);
    }

    #[test]
    fn test_read_table_with_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(
            store,
            false, // don't have ordered listing
            prefix,
            Arc::new(TokioBackgroundExecutor::new()),
        );
        let cp = read_last_checkpoint(&client, &url).unwrap();
        assert!(cp.is_none())
    }

    fn valid_last_checkpoint() -> Vec<u8> {
        r#"{"size":8,"size_in_bytes":21857,"version":1}"#.as_bytes().to_vec()
    }

    #[test]
    fn test_read_table_with_invalid_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // put _last_checkpoint file
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

        let client = ObjectStoreFileSystemClient::new(
            store,
            false, // don't have ordered listing
            Path::from("/"),
            Arc::new(TokioBackgroundExecutor::new()),
        );
        let url = Url::parse("memory:///valid/").expect("valid url");
        let valid = read_last_checkpoint(&client, &url).expect("read last checkpoint");
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = read_last_checkpoint(&client, &url).expect("read last checkpoint");
        assert!(valid.is_some());
        assert!(invalid.is_none())
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
}
