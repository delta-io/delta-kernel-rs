//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::cmp::Ordering;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::features::{ColumnMappingMode, COLUMN_MAPPING_MODE_KEY};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::Schema;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileSystemClient, Version};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.

pub struct Snapshot {
    pub(crate) table_root: Url,
    pub(crate) log_segment: LogSegment,
    version: Version,
    metadata: Metadata,
    protocol: Protocol,
    schema: Schema,
    pub(crate) column_mapping_mode: ColumnMappingMode,
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
            .field("version", &self.version)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `location`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let fs_client = engine.get_file_system_client();
        let log_url = table_root.join("_delta_log/").unwrap();

        // List relevant files from log
        let (mut commit_files, checkpoint_files) =
            match (read_last_checkpoint(fs_client.as_ref(), &log_url)?, version) {
                (Some(cp), None) => {
                    list_log_files_with_checkpoint(&cp, fs_client.as_ref(), &log_url)?
                }
                (Some(cp), Some(version)) if cp.version >= version => {
                    list_log_files_with_checkpoint(&cp, fs_client.as_ref(), &log_url)?
                }
                _ => list_log_files(fs_client.as_ref(), &log_url)?,
            };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|log_path| log_path.version <= version);
        }
        // only keep commit files above the checkpoint we found
        if let Some(checkpoint_file) = checkpoint_files.first() {
            commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
        }

        // get the effective version from chosen files
        let version_eff = commit_files
            .first()
            .or(checkpoint_files.first())
            .ok_or(Error::MissingVersion)? // TODO: A more descriptive error
            .version;

        if let Some(v) = version {
            require!(
                version_eff == v,
                Error::MissingVersion // TODO more descriptive error
            );
        }

        let log_segment = LogSegment {
            log_root: log_url,
            commit_files: commit_files
                .into_iter()
                .map(|log_path| log_path.location)
                .collect(),
            checkpoint_files: checkpoint_files
                .into_iter()
                .map(|log_path| log_path.location)
                .collect(),
        };

        Self::try_new_from_log_segment(table_root, log_segment, version_eff, engine)
    }

    /// Create a new [`Snapshot`] instance.
    pub(crate) fn try_new_from_log_segment(
        location: Url,
        log_segment: LogSegment,
        version: Version,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let (metadata, protocol) = log_segment.read_metadata(engine)?;
        let schema = metadata.schema()?;
        let column_mapping_mode = match metadata.configuration.get(COLUMN_MAPPING_MODE_KEY) {
            Some(mode) if protocol.min_reader_version >= 2 => mode.as_str().try_into(),
            _ => Ok(ColumnMappingMode::None),
        }?;
        Ok(Self {
            table_root: location,
            log_segment,
            version,
            metadata,
            protocol,
            schema,
            column_mapping_mode,
        })
    }

    /// Log segment this snapshot uses
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    fn _log_segment(&self) -> &LogSegment {
        &self.log_segment
    }

    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// Version of this `Snapshot` in the table.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Table [`Schema`] at this `Snapshot`s version.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Table [`Metadata`] at this `Snapshot`s version.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Table [`Protocol`] at this `Snapshot`s version.
    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// Get the [column mapping
    /// mode](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) at this
    /// `Snapshot`s version.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
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

/// List all log files after a given checkpoint.
fn list_log_files_with_checkpoint(
    checkpoint_metadata: &CheckpointMetadata,
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    let version_prefix = format!("{:020}", checkpoint_metadata.version);
    let start_from = log_root.join(&version_prefix)?;

    let mut max_checkpoint_version = checkpoint_metadata.version;
    let mut checkpoint_files = vec![];
    // We expect 10 commit files per checkpoint, so start with that size. We could adjust this based
    // on config at some point
    let mut commit_files = Vec::with_capacity(10);

    for meta_res in fs_client.list_from(&start_from)? {
        let meta = meta_res?;
        let parsed_path = ParsedLogPath::try_from(meta)?;
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        if let Some(parsed_path) = parsed_path {
            if parsed_path.is_commit() {
                commit_files.push(parsed_path);
            } else if parsed_path.is_checkpoint() {
                match parsed_path.version.cmp(&max_checkpoint_version) {
                    Ordering::Greater => {
                        max_checkpoint_version = parsed_path.version;
                        checkpoint_files.clear();
                        checkpoint_files.push(parsed_path);
                    }
                    Ordering::Equal => checkpoint_files.push(parsed_path),
                    Ordering::Less => {}
                }
            }
        }
    }

    if checkpoint_files.is_empty() {
        // TODO: We could potentially recover here
        return Err(Error::generic(
            "Had a _last_checkpoint hint but didn't find any checkpoints",
        ));
    }

    if max_checkpoint_version != checkpoint_metadata.version {
        warn!(
            "_last_checkpoint hint is out of date. _last_checkpoint version: {}. Using actual most recent: {}",
            checkpoint_metadata.version,
            max_checkpoint_version
        );
        // we (may) need to drop commits that are before the _actual_ last checkpoint (that
        // is, commits between a stale _last_checkpoint and the _actual_ last checkpoint)
        commit_files.retain(|parsed_path| parsed_path.version > max_checkpoint_version);
    } else if checkpoint_files.len() != checkpoint_metadata.parts.unwrap_or(1) {
        return Err(Error::Generic(format!(
            "_last_checkpoint indicated that checkpoint should have {} parts, but it has {}",
            checkpoint_metadata.parts.unwrap_or(1),
            checkpoint_files.len()
        )));
    }

    debug_assert!(
        commit_files
            .windows(2)
            .all(|cfs| cfs[0].version <= cfs[1].version),
        "fs_client.list_from() didn't return a sorted listing! {:?}",
        commit_files
    );
    // We assume listing returned ordered, we want reverse order
    let commit_files = commit_files.into_iter().rev().collect();

    Ok((commit_files, checkpoint_files))
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
fn list_log_files(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    let version_prefix = format!("{:020}", 0);
    let start_from = log_root.join(&version_prefix)?;

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::new();
    let mut checkpoint_files = Vec::with_capacity(10);

    let log_paths = fs_client
        .list_from(&start_from)?
        .flat_map(|file| file.and_then(ParsedLogPath::try_from).transpose());
    for log_path in log_paths {
        let log_path = log_path?;
        if log_path.is_checkpoint() {
            let version = log_path.version as i64;
            match version.cmp(&max_checkpoint_version) {
                Ordering::Greater => {
                    max_checkpoint_version = version;
                    checkpoint_files.clear();
                    checkpoint_files.push(log_path);
                }
                Ordering::Equal => {
                    checkpoint_files.push(log_path);
                }
                _ => {}
            }
        } else if log_path.is_commit() {
            commit_files.push(log_path);
        }
    }

    commit_files.retain(|f| f.version as i64 > max_checkpoint_version);

    debug_assert!(
        commit_files
            .windows(2)
            .all(|cfs| cfs[0].version <= cfs[1].version),
        "fs_client.list_from() didn't return a sorted listing! {:?}",
        commit_files
    );
    // We assume listing returned ordered, we want reverse order
    let commit_files = commit_files.into_iter().rev().collect();

    Ok((commit_files, checkpoint_files))
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
    use crate::schema::StructType;

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, Some(1)).unwrap();

        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
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

        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
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

    #[test]
    fn test_read_log_with_out_of_date_last_checkpoint() {
        let store = Arc::new(InMemory::new());

        fn get_path(index: usize, suffix: &str) -> Path {
            let path = format!("_delta_log/{index:020}.{suffix}");
            Path::from(path.as_str())
        }
        let data = bytes::Bytes::from("kernel-data");

        let checkpoint_metadata = CheckpointMetadata {
            version: 3,
            size: 10,
            parts: None,
            size_in_bytes: None,
            num_of_add_files: None,
            checkpoint_schema: None,
            checksum: None,
        };

        // add log files to store
        tokio::runtime::Runtime::new()
            .expect("create tokio runtime")
            .block_on(async {
                for path in [
                    get_path(0, "json"),
                    get_path(1, "checkpoint.parquet"),
                    get_path(2, "json"),
                    get_path(3, "checkpoint.parquet"),
                    get_path(4, "json"),
                    get_path(5, "checkpoint.parquet"),
                    get_path(6, "json"),
                    get_path(7, "json"),
                ] {
                    store
                        .put(&path, data.clone().into())
                        .await
                        .expect("put log file in store");
                }
                let checkpoint_str =
                    serde_json::to_string(&checkpoint_metadata).expect("Serialize checkpoint");
                store
                    .put(
                        &Path::from("_delta_log/_last_checkpoint"),
                        checkpoint_str.into(),
                    )
                    .await
                    .expect("Write _last_checkpoint");
            });

        let client = ObjectStoreFileSystemClient::new(
            store,
            false, // don't have ordered listing
            Path::from("/"),
            Arc::new(TokioBackgroundExecutor::new()),
        );

        let url = Url::parse("memory:///_delta_log/").expect("valid url");
        let (commit_files, checkpoint_files) =
            list_log_files_with_checkpoint(&checkpoint_metadata, &client, &url).unwrap();
        assert_eq!(checkpoint_files.len(), 1);
        assert_eq!(commit_files.len(), 2);
        assert_eq!(checkpoint_files[0].version, 5);
        assert_eq!(commit_files[0].version, 7);
        assert_eq!(commit_files[1].version, 6);
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

        assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(snapshot.log_segment.checkpoint_files[0].location.clone())
                .unwrap()
                .unwrap()
                .version,
            2,
        );
        assert_eq!(snapshot.log_segment.commit_files.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(snapshot.log_segment.commit_files[0].location.clone())
                .unwrap()
                .unwrap()
                .version,
            3,
        );
    }
}
