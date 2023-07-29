//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::sync::Arc;
use std::sync::RwLock;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actions::{parse_action, Action, ActionType, Metadata, Protocol};
use crate::path::LogPath;
use crate::scan::ScanBuilder;
use crate::schema::Schema;
use crate::{DeltaResult, Error, FileMeta, FileSystemClient, TableClient, Version};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

#[derive(Debug)]
pub struct LogSegment {
    log_root: Url,
    /// Reverse order soprted commit files in the log segment
    pub(crate) commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segement.
    pub(crate) checkpoint_files: Vec<FileMeta>,
}

impl LogSegment {
    pub(crate) fn commit_files(&self) -> impl Iterator<Item = &FileMeta> {
        self.commit_files.iter()
    }

    // TODO just a stop gap implementation, eventually we likely want a stream of batches...
    async fn replay<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
    ) -> DeltaResult<Vec<RecordBatch>> {
        let read_schema = Arc::new(ArrowSchema {
            fields: Fields::from_iter([ActionType::Metadata.field(), ActionType::Protocol.field()]),
            metadata: Default::default(),
        });

        let mut commit_files: Vec<_> = self.commit_files().cloned().collect();
        // NOTE this will already sort in reverse order
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        let json_client = table_client.get_json_handler();
        let read_contexts = json_client.contextualize_file_reads(commit_files, None)?;
        let commit_stream = json_client
            .read_json_files(read_contexts, Arc::new(read_schema.clone().try_into()?))?;

        let parquet_client = table_client.get_parquet_handler();
        let read_contexts =
            parquet_client.contextualize_file_reads(self.checkpoint_files.clone(), None)?;
        let checkpoint_stream = parquet_client
            .read_parquet_files(read_contexts, Arc::new(read_schema.clone().try_into()?))?;

        let batches = commit_stream
            .chain(checkpoint_stream)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(batches)
    }

    async fn read_metadata<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
    ) -> DeltaResult<Option<(Metadata, Protocol)>> {
        let batches = self.replay(table_client).await?;
        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for batch in batches {
            if let Ok(mut metas) = parse_action(&batch, &ActionType::Metadata) {
                match metas.next() {
                    Some(Action::Metadata(meta)) => {
                        metadata_opt = Some(meta.clone());
                    }
                    _ => (),
                }
            }

            if let Ok(mut protos) = parse_action(&batch, &ActionType::Protocol) {
                match protos.next() {
                    Some(Action::Protocol(proto)) => {
                        protocol_opt = Some(proto.clone());
                    }
                    _ => (),
                }
            }

            if let (Some(metadata), Some(protocol)) = (&metadata_opt, &protocol_opt) {
                return Ok(Some((metadata.clone(), protocol.clone())));
            }
        }
        Ok(None)
    }
}

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
pub struct Snapshot<JRC: Send, PRC: Send> {
    table_root: Url,
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    log_segment: LogSegment,
    version: Version,
    metadata: Arc<RwLock<Option<(Metadata, Protocol)>>>,
}

impl<JRC: Send, PRC: Send> std::fmt::Debug for Snapshot<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<JRC: Send, PRC: Send + Sync> Snapshot<JRC, PRC> {
    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `location`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `table_client`: Implementation of [`TableClient`] apis.
    /// - `version`: target version of the [`Snapshot`]
    pub async fn try_new(
        table_root: Url,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let fs_client = table_client.get_file_system_client();
        let log_url = LogPath(&table_root).child("_delta_log/").unwrap();

        // List relevant files from log
        let (mut commit_files, checkpoint_files) =
            match read_last_checkpoint(fs_client.as_ref(), &log_url).await {
                Err(err) => return Err(err),
                Ok(Some(cp)) => {
                    if let Some(v) = version {
                        if cp.version >= v {
                            list_log_files_with_checkpoint(&cp, fs_client.as_ref(), &log_url)
                                .await?
                        } else {
                            list_log_files(fs_client.as_ref(), &log_url).await?
                        }
                    } else {
                        list_log_files(fs_client.as_ref(), &log_url).await?
                    }
                }
                Ok(None) => list_log_files(fs_client.as_ref(), &log_url).await?,
            };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| {
                if let Some(v) = LogPath(&meta.location).commit_version() {
                    v <= version
                } else {
                    false
                }
            });
        }

        // get the effective version from chosen files
        let version_eff = if !commit_files.is_empty() {
            commit_files
                .first()
                .and_then(|f| LogPath(&f.location).commit_version())
                .unwrap()
        } else if !checkpoint_files.is_empty() {
            checkpoint_files
                .first()
                .and_then(|f| LogPath(&f.location).commit_version())
                .unwrap()
        } else {
            // TODO more descriptive error
            return Err(Error::MissingVersion);
        };

        if let Some(v) = version {
            if version_eff != v {
                // TODO more descriptive error
                return Err(Error::MissingVersion);
            }
        }

        let log_segment = LogSegment {
            log_root: log_url,
            commit_files,
            checkpoint_files,
        };

        Ok(Self {
            table_root,
            table_client,
            log_segment,
            version: version_eff,
            metadata: Default::default(),
        })
    }

    /// Create a new [`Snapshot`] instance.
    pub fn new(
        location: Url,
        client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        log_segment: LogSegment,
        version: Version,
    ) -> Self {
        Self {
            table_root: location,
            table_client: client,
            log_segment,
            version,
            metadata: Default::default(),
        }
    }

    /// Version of this [`Snapshot`] in the table.
    pub fn version(&self) -> Version {
        self.version
    }

    async fn get_or_insert_metadata(&self) -> DeltaResult<(Metadata, Protocol)> {
        let read_lock = self
            .metadata
            .read()
            .map_err(|_| Error::Generic("filed to get read lock".into()))?;
        if let Some((metadata, protocol)) = read_lock.as_ref() {
            return Ok((metadata.clone(), protocol.clone()));
        }
        drop(read_lock);

        let (metadata, protocol) = self
            .log_segment
            .read_metadata(self.table_client.as_ref())
            .await?
            .ok_or(Error::MissingMetadata)?;
        let mut meta = self
            .metadata
            .write()
            .map_err(|_| Error::Generic("filed to get write lock".into()))?;
        *meta = Some((metadata.clone(), protocol.clone()));

        Ok((metadata, protocol))
    }

    /// Table [`Schema`] at this [`Snapshot`]s version.
    pub async fn schema(&self) -> DeltaResult<Schema> {
        self.metadata().await?.schema()
    }

    /// Table [`Metadata`] at this [`Snapshot`]s version.
    pub async fn metadata(&self) -> DeltaResult<Metadata> {
        let (metadata, _) = self.get_or_insert_metadata().await?;
        Ok(metadata)
    }

    pub async fn protocol(&self) -> DeltaResult<Protocol> {
        let (_, protocol) = self.get_or_insert_metadata().await?;
        Ok(protocol)
    }

    pub async fn scan(self) -> DeltaResult<ScanBuilder<JRC, PRC>> {
        let schema = Arc::new(self.schema().await?);
        Ok(ScanBuilder::new(
            self.table_root,
            schema,
            self.log_segment,
            self.table_client,
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointMetadata {
    /// The version of the table when the last checkpoint was made.
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub size: i32,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    pub size_in_bytes: Option<i32>,
    /// The number of AddFile actions in the checkpoint.
    pub num_of_add_files: Option<i32>,
    /// The schema of the checkpoint file.
    pub checkpoint_schema: Option<Schema>,
    /// The checksum of the last checkpoint JSON.
    pub checksum: Option<String>,
}

/// Try reading the `_last_checkpoint` file.
///
/// In case the file is not found, `None` is returned.
async fn read_last_checkpoint(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<Option<CheckpointMetadata>> {
    let file_path = LogPath(log_root).child(LAST_CHECKPOINT_FILE_NAME)?;
    match fs_client.read_files(vec![(file_path, None)]).await {
        Ok(data) => match data.first() {
            Some(data) => Ok(Some(serde_json::from_slice(data)?)),
            None => Ok(None),
        },
        Err(Error::FileNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// List all log files after a givben checkpoint.
async fn list_log_files_with_checkpoint(
    cp: &CheckpointMetadata,
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let version_prefix = format!("{:020}", cp.version);
    let start_from = log_root.join(&version_prefix)?;

    let files = fs_client
        .list_from(&start_from)
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        .filter(|f| LogPath(&f.location).commit_version().is_some())
        .collect::<Vec<_>>();

    let mut commit_files = files
        .iter()
        .filter_map(|f| {
            if LogPath(&f.location).is_commit_file() {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    let checkpoint_files = files
        .iter()
        .filter_map(|f| {
            if LogPath(&f.location).is_checkpoint_file() {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    // TODO raise a proper error
    assert_eq!(checkpoint_files.len(), cp.parts.unwrap_or(1) as usize);

    Ok((commit_files, checkpoint_files))
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
async fn list_log_files(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let version_prefix = format!("{:020}", 0);
    let start_from = log_root.join(&version_prefix)?;

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::new();
    let mut checkpoint_files = Vec::with_capacity(10);

    let mut stream = fs_client.list_from(&start_from).await?;
    while let Some(maybe_meta) = stream.next().await {
        let meta = maybe_meta?;
        if LogPath(&meta.location).is_checkpoint_file() {
            let version = LogPath(&meta.location).commit_version().unwrap_or(0) as i64;
            if version > max_checkpoint_version {
                max_checkpoint_version = version;
                checkpoint_files.clear();
                checkpoint_files.push(meta);
            } else if version == max_checkpoint_version {
                checkpoint_files.push(meta);
            }
        } else if LogPath(&meta.location).is_commit_file() {
            commit_files.push(meta);
        }
    }

    commit_files.retain(|f| {
        LogPath(&f.location).commit_version().unwrap_or(0) as i64 > max_checkpoint_version
    });
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;

    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use crate::client::DefaultTableClient;
    use crate::filesystem::ObjectStoreFileSystemClient;
    use crate::schema::StructType;

    #[tokio::test]
    async fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let client =
            Arc::new(DefaultTableClient::try_new(&url, HashMap::<String, String>::new()).unwrap());
        let snapshot = Snapshot::try_new(url, client, Some(1)).await.unwrap();

        let protocol = snapshot.protocol().await.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_wrriter_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(protocol, expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        let schema = snapshot.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let client =
            Arc::new(DefaultTableClient::try_new(&url, HashMap::<String, String>::new()).unwrap());
        let snapshot = Snapshot::try_new(url, client, None).await.unwrap();

        let protocol = snapshot.protocol().await.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_wrriter_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(protocol, expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        let schema = snapshot.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_read_table_with_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(store, prefix);
        let cp = read_last_checkpoint(&client, &url).await.unwrap();
        assert!(cp.is_none())
    }

    #[tokio::test]
    async fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let table_client = Arc::new(
            DefaultTableClient::try_new(&location, HashMap::<String, String>::new()).unwrap(),
        );
        let snapshot = Snapshot::try_new(location, table_client, None)
            .await
            .unwrap();

        assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
        assert_eq!(
            LogPath(&snapshot.log_segment.checkpoint_files[0].location).commit_version(),
            Some(2)
        );
        assert_eq!(snapshot.log_segment.commit_files.len(), 1);
        assert_eq!(
            LogPath(&snapshot.log_segment.commit_files[0].location).commit_version(),
            Some(3)
        );
    }
}
