//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::sync::Arc;
use std::sync::RwLock;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actions::{parse_action, Action, ActionType, Metadata, Protocol};
use crate::path::LogPath;
use crate::scan::ScanBuilder;
use crate::schema::Schema;
use crate::{DeltaResult, Error, FileMeta, FileSystemClient, TableClient, Version};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

#[derive(Debug)]
pub struct LogSegmentNew {
    log_path: Url,
    files: Vec<FileMeta>,
}

impl LogSegmentNew {
    pub(crate) fn new(log_path: Url, files: Vec<FileMeta>) -> Self {
        Self { log_path, files }
    }

    pub(crate) fn checkpoint_files(&self) -> impl Iterator<Item = &FileMeta> {
        self.files
            .iter()
            .filter(|f| LogPath(&f.location).is_checkpoint_file())
    }

    pub(crate) fn commit_files(&self) -> impl Iterator<Item = &FileMeta> {
        self.files
            .iter()
            .filter(|f| LogPath(&f.location).is_commit_file())
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

        let checkpoint_files: Vec<_> = self.checkpoint_files().cloned().collect();
        let parquet_client = table_client.get_parquet_handler();
        let read_contexts = parquet_client.contextualize_file_reads(checkpoint_files, None)?;
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
    log_segment: LogSegmentNew,
    version: Version,
    metadata: Arc<RwLock<Option<(Metadata, Protocol)>>>,
}

impl<JRC: Send, PRC: Send> std::fmt::Debug for Snapshot<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_path.as_str())
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
        location: Url,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        // TODO properly create log segment
        // - read _last_checkpoint
        // - make sure we do not have nunnecessary files in segment

        let fs_client = table_client.get_file_system_client();
        let log_url = LogPath(&location).child("_delta_log/").unwrap();

        let mut files = fs_client
            .list_from(&log_url)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        if let Some(version) = version {
            files = files
                .into_iter()
                .filter(|meta| {
                    if let Some(v) = LogPath(&meta.location).commit_version() {
                        v <= version
                    } else {
                        false
                    }
                })
                .collect();
        }

        let version = files
            .iter()
            .filter_map(|f| LogPath(&f.location).commit_version())
            .max()
            .unwrap_or_default();
        let log_segment = LogSegmentNew::new(log_url, files);

        Ok(Self {
            table_root: location,
            table_client,
            log_segment,
            version,
            metadata: Default::default(),
        })
    }

    /// Create a new [`Snapshot`] instance.
    pub fn new(
        location: Url,
        client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        log_segment: LogSegmentNew,
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

async fn read_last_checkpoint(
    fs_client: &dyn FileSystemClient,
    log_path: &Url,
) -> DeltaResult<CheckpointMetadata> {
    let file_path = LogPath(log_path).child(LAST_CHECKPOINT_FILE_NAME)?;
    let data = fs_client.read_files(vec![(file_path, None)]).await?;
    let data = data
        .first()
        .ok_or(Error::Generic("no checkpoint data".into()))?;
    Ok(serde_json::from_slice(data)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use futures::stream::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use super::*;
    use crate::client::filesystem::ObjectStoreFileSystemClient;
    use crate::client::DefaultTableClient;
    use crate::schema::StructType;

    #[test]
    fn test_checkpoint_serde() {
        let file = std::fs::File::open("./tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/_last_checkpoint").unwrap();
        let cp: CheckpointMetadata = serde_json::from_reader(file).unwrap();
        assert_eq!(cp.version, 2)
    }

    #[tokio::test]
    async fn test_log_segment_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let client =
            Arc::new(DefaultTableClient::try_new(&url, HashMap::<String, String>::new()).unwrap());
        let fs_client = client.get_file_system_client();
        let log_url = LogPath(&url).child("_delta_log/").unwrap();

        let files = fs_client
            .list_from(&log_url)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let log_segment = LogSegmentNew::new(log_url, files);
        let snapshot = Snapshot::new(url, client, log_segment, 1);

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
        let mut url = url::Url::from_file_path(path).unwrap();
        // FIXME - be more robust against trailing slashes ...
        url.set_path(&format!("{}/", url.path().trim_end_matches('/')));

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
    async fn test_read_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/",
        ))
        .unwrap();
        let mut url = url::Url::from_file_path(path).unwrap();
        // FIXME - be more robust against trailing slashes ...
        url.set_path(&format!("{}/", url.path().trim_end_matches('/')));

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(store, prefix);
        let cp = read_last_checkpoint(&client, &url).await.unwrap();
        assert_eq!(cp.version, 2);

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let mut url = url::Url::from_file_path(path).unwrap();
        // FIXME - be more robust against trailing slashes ...
        url.set_path(&format!("{}/", url.path().trim_end_matches('/')));

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(store, prefix);
        let cp = read_last_checkpoint(&client, &url).await.unwrap_err();
        assert!(matches!(cp, Error::FileNotFound(_)))
    }
}
