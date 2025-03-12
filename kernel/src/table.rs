//! In-memory representation of a Delta table, which acts as an immutable root entity for reading
//! the different versions

use std::borrow::Cow;
use std::collections::VecDeque;
use std::iter::Peekable;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::SystemTime;

use url::Url;

use crate::checkpoint::iterators::{SidecarIterator, V1CheckpointFileIterator};
use crate::checkpoint::log_replay::{sidecar_actions_iter, v1_checkpoint_actions_iter};
use crate::checkpoint::{get_sidecar_file_schema, get_v1_checkpoint_schema};
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::table_changes::TableChanges;
use crate::transaction::Transaction;
use crate::{DeltaResult, Engine, EngineData, Error, Version};

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [`Snapshot`]) of the table located in storage.
#[derive(Clone)]
pub struct Table {
    location: Url,
}

pub type FileData = Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send>;
pub type FileDataWithUrl = (FileData, Url);
pub type FileDataIter = Box<dyn Iterator<Item = DeltaResult<FileDataWithUrl>> + Send>;

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Table")
            .field("location", &self.location)
            .finish()
    }
}

impl Table {
    /// Create a new Delta table with the given parameters
    pub fn new(location: Url) -> Self {
        Self { location }
    }

    /// Try to create a new table from a string uri. This will do it's best to handle things like
    /// `/local/paths`, and even `../relative/paths`.
    pub fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let uri = uri.as_ref();
        let uri_type = resolve_uri_type(uri)?;
        let url = match uri_type {
            UriType::LocalPath(path) => {
                if !path.exists() {
                    // When we support writes, create a directory if we can
                    return Err(Error::InvalidTableLocation(format!(
                        "Path does not exist: {path:?}"
                    )));
                }
                if !path.is_dir() {
                    return Err(Error::InvalidTableLocation(format!(
                        "{path:?} is not a directory"
                    )));
                }
                let path = std::fs::canonicalize(path).map_err(|err| {
                    let msg = format!("Invalid table location: {} Error: {:?}", uri, err);
                    Error::InvalidTableLocation(msg)
                })?;
                Url::from_directory_path(path.clone()).map_err(|_| {
                    let msg = format!(
                        "Could not construct a URL from canonicalized path: {:?}.\n\
                         Something must be very wrong with the table path.",
                        path
                    );
                    Error::InvalidTableLocation(msg)
                })?
            }
            UriType::Url(url) => url,
        };
        Ok(Self::new(url))
    }

    /// Fully qualified location of the Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Create a [`Snapshot`] of the table corresponding to `version`.
    ///
    /// If no version is supplied, a snapshot for the latest version will be created.
    pub fn snapshot(&self, engine: &dyn Engine, version: Option<Version>) -> DeltaResult<Snapshot> {
        Snapshot::try_new(self.location.clone(), engine, version)
    }

    /// Returns a [`V1CheckpointFileIterator`] object using the V1 checkpoint format.
    ///
    /// # Example Usage
    ///
    /// ```
    /// /* Phase 1: Write V1 checkpoint file  */
    /// let mut v1_checkpoint_file_iter = table.checkpoint_v1(engine);
    /// let (data_iter, v1_checkpoint_file_path) = v1_checkpoint_file_iter.get_file_data_iter()?;
    ///
    /// let metadata = /* engine writes data_iter to v1_checkpoint_file_path */
    ///
    /// /* Phase 2: Write _last_checkpoint file */
    /// /* We require the checkpoint file to be written before transitioning to next phase
    /// otherwise we risk data loss */
    /// v1_checkpoint_file_iter.checkpoint_metadata(metadata)?;
    /// ```
    pub fn checkpoint_v1(
        &self,
        engine: &dyn Engine,
        version: Option<u64>,
    ) -> DeltaResult<V1CheckpointFileIterator> {
        // Get a snapshot of the table at the requested version (or latest)
        let snapshot = self.snapshot(engine, version)?;

        // Create counters for tracking actions
        let total_actions_counter = Arc::new(AtomicUsize::new(0));
        let total_add_actions_counter = Arc::new(AtomicUsize::new(0));

        // Calculate file retention timestamp
        let minimum_file_retention_timestamp =
            self.calculate_minimum_retention_timestamp(&snapshot)?;

        let schema = get_v1_checkpoint_schema();

        // Retrieve the iterator of actions over the log segment
        let iter = snapshot
            .log_segment()
            .replay(engine, schema.clone(), schema.clone(), None)?;

        // Replay the actions iterator to build selection vectors for each batch of actions
        let actions = v1_checkpoint_actions_iter(
            iter,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            minimum_file_retention_timestamp,
        );

        // Create and return the V1 checkpoint iterator
        Ok(V1CheckpointFileIterator::new(
            Box::new(actions),
            self.location.clone(),
            snapshot.version(),
            total_actions_counter,
            total_add_actions_counter,
        ))
    }

    /// Returns a [`CheckpointIterator`] object using the V2 checkpoint format.
    ///
    /// # Example Usage
    ///
    /// ```
    /// let engine = SyncEngine::new();
    /// let checkpoint_iter = table.checkpoint_v2(engine, Some(5))?;
    /// // Use the iterator to write checkpoint files
    /// ```
    pub fn checkpoint_v2(
        &self,
        engine: &dyn Engine,
        version: Option<u64>,
    ) -> DeltaResult<SidecarIterator> {
        // Get a snapshot of the table at the requested version (or latest)
        let snapshot = self.snapshot(engine, version)?;

        // Create counters for tracking actions
        let total_actions_counter = Arc::new(AtomicUsize::new(0));
        let total_add_actions_counter = Arc::new(AtomicUsize::new(0));

        // Calculate file retention timestamp
        let minimum_file_retention_timestamp =
            self.calculate_minimum_retention_timestamp(&snapshot)?;

        // Get schema for sidecar files
        let schema = get_sidecar_file_schema();

        // Retrieve the iterator of actions over the log segment
        let iter = snapshot
            .log_segment()
            .replay(engine, schema.clone(), schema.clone(), None)?;

        // Replay the actions iterator to build selection vectors for each batch of actions for sidecar files
        let actions_iter = sidecar_actions_iter(
            iter,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            minimum_file_retention_timestamp,
        );

        // Create and return the sidecar iterator
        Ok(SidecarIterator::new(
            Box::new(actions_iter),
            self.location.clone(),
            snapshot,
            total_actions_counter,
            total_add_actions_counter,
        ))
    }

    /// Calculates the minimum timestamp for file retention based on table policy.
    ///
    /// Remove actions with deletion timestamps _later_ than this value will be retained
    /// in the table's log for VACUUM operations. The default retention duration is 7 days.
    fn calculate_minimum_retention_timestamp(&self, snapshot: &Snapshot) -> DeltaResult<i64> {
        // Get the retention duration from table properties, defaulting to 7 days
        let deleted_file_retention_duration = snapshot
            .table_properties()
            .deleted_file_retention_duration
            .unwrap_or_else(|| std::time::Duration::from_secs(60 * 60 * 24 * 7));

        // Calculate the current time
        let duration_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| {
                Error::generic(format!(
                    "Error encountered when computing duration since epoch: {:?}",
                    e
                ))
            })?;

        // Calculate the cutoff time by subtracting retention duration from current time
        let minimum_file_retention_timestamp = duration_since_epoch
            .as_millis()
            .checked_sub(deleted_file_retention_duration.as_millis())
            .ok_or_else(|| {
                Error::generic(
                    "Overflow in timestamp calculation for minimumFileRetentionTimestamp",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::generic("Failed to convert minimum_file_retention_timestamp to i64")
            })?;

        Ok(minimum_file_retention_timestamp)
    }
    // Here actions: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>>
    /// Create a [`TableChanges`] to get a change data feed for the table between `start_version`,
    /// and `end_version`. If no `end_version` is supplied, the latest version will be used as the
    /// `end_version`.
    pub fn table_changes(
        &self,
        engine: &dyn Engine,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<TableChanges> {
        TableChanges::try_new(
            self.location.clone(),
            engine,
            start_version,
            end_version.into(),
        )
    }

    /// Create a new write transaction for this table.
    pub fn new_transaction(&self, engine: &dyn Engine) -> DeltaResult<Transaction> {
        Transaction::try_new(self.snapshot(engine, None)?)
    }
}

#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let table_uri = if table_uri.ends_with('/') {
        Cow::Borrowed(table_uri)
    } else {
        Cow::Owned(format!("{table_uri}/"))
    };
    if let Ok(url) = Url::parse(&table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(
                url.to_file_path()
                    .map_err(|_| Error::invalid_table_location(table_uri))?,
            ))
        } else if scheme.len() == 1 {
            // NOTE this check is required to support absolute windows paths which may properly
            // parse as url we assume here that a single character scheme is a windows drive letter
            Ok(UriType::LocalPath(PathBuf::from(table_uri.as_ref())))
        } else {
            Ok(UriType::Url(url))
        }
    } else {
        Ok(UriType::LocalPath(table_uri.deref().into()))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use arrow_53::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;
    use crate::{
        actions::{Metadata, Protocol, Sidecar},
        arrow,
        checkpoint::get_checkpoint_metadata_schema,
        engine::{
            arrow_data::ArrowEngineData,
            default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
            sync::SyncEngine,
        },
        scan::test_utils::add_batch_with_remove,
    };

    #[test]
    fn test_table() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        assert_eq!(snapshot.version(), 1)
    }

    #[test]
    fn test_v1_checkpoint() -> DeltaResult<()> {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let mut v1_checkpoint_file_iterator = table.checkpoint_v1(&engine, None).unwrap();

        let (iter, file_path) = v1_checkpoint_file_iterator.get_file_data_iter()?;

        // assert!(file_path.as_str().ends_with(&format!("{:020}.parquet", i)));
        println!("{:?}", file_path);

        for batch_result in iter {
            let (data, selection_vector) = batch_result.unwrap();
            println!("{:?}", selection_vector);
            let engine_data: Box<dyn EngineData> = data;

            let record_batch = engine_data
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .unwrap()
                .record_batch();

            // Print record batch nicely
            let pretty_format = arrow::util::pretty::pretty_format_batches(&[record_batch.clone()])
                .unwrap()
                .to_string();
            println!("{}", pretty_format);
        }

        let field = Arc::new(Field::new("size", DataType::Int64, false));
        let schema = Schema::new([field.clone()]);

        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let result = v1_checkpoint_file_iterator
            .checkpoint_metadata(&ArrowEngineData::new(record_batch), &engine);

        if let Err(err) = result {
            panic!("Error: {:?}", err);
        }
        Ok(())
    }

    #[test]
    fn test_v2_checkpoint() -> DeltaResult<()> {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let mut sidecar_iterator = table.checkpoint_v2(&engine, None).unwrap();

        let (iter, file_path) = sidecar_iterator.get_file_data_iter()?;

        // Write sidecar files
        for batch_result in iter {
            let (data, selection_vector) = batch_result.unwrap();
            println!("{:?}", selection_vector);
            let engine_data: Box<dyn EngineData> = data;

            let record_batch = engine_data
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .unwrap()
                .record_batch();

            // Print record batch nicely
            let pretty_format = arrow::util::pretty::pretty_format_batches(&[record_batch.clone()])
                .unwrap()
                .to_string();
            println!("{}", pretty_format);
        }

        let mut v2_checkpoint_file_iterator = sidecar_iterator.sidecar_metadata(vec![], &engine)?;

        let (iter, file_path) = v2_checkpoint_file_iterator.get_file_data_iter()?;
        // assert!(file_path.as_str().ends_with(&format!("{:020}.parquet", i)));
        println!("{:?}", file_path);

        for batch_result in iter {
            let (data, selection_vector) = batch_result.unwrap();
            println!("{:?}", selection_vector);
            let engine_data: Box<dyn EngineData> = data;

            let record_batch = engine_data
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .unwrap()
                .record_batch();

            // Print record batch nicely
            let pretty_format = arrow::util::pretty::pretty_format_batches(&[record_batch.clone()])
                .unwrap()
                .to_string();
            println!("{}", pretty_format);
        }

        let field = Arc::new(Field::new("size", DataType::Int64, false));
        let schema = Schema::new([field.clone()]);

        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let result = v2_checkpoint_file_iterator
            .checkpoint_metadata(&ArrowEngineData::new(record_batch), &engine);

        if let Err(err) = result {
            panic!("Error: {:?}", err);
        }

        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_path_parsing() {
        for x in [
            // windows parsing of file:/// is... odd
            #[cfg(not(windows))]
            "file:///foo/bar",
            #[cfg(not(windows))]
            "file:///foo/bar/",
            "/foo/bar",
            "/foo/bar/",
            "../foo/bar",
            "../foo/bar/",
            "c:/foo/bar",
            "c:/",
            "file:///C:/",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::LocalPath(_)) => {}
                x => panic!("Should have parsed as a local path {x:?}"),
            }
        }

        for x in [
            "s3://foo/bar",
            "s3a://foo/bar",
            "memory://foo/bar",
            "gs://foo/bar",
            "https://foo/bar/",
            "unknown://foo/bar",
            "s2://foo/bar",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::Url(_)) => {}
                x => panic!("Should have parsed as a url {x:?}"),
            }
        }

        #[cfg(not(windows))]
        resolve_uri_type("file://foo/bar").expect_err("file://foo/bar should not have parsed");
    }

    #[test]
    fn try_from_uri_without_trailing_slash() {
        let location = "s3://foo/__unitystorage/catalogs/cid/tables/tid";
        let table = Table::try_from_uri(location).unwrap();

        assert_eq!(
            table.location.join("_delta_log/").unwrap().as_str(),
            "s3://foo/__unitystorage/catalogs/cid/tables/tid/_delta_log/"
        );
    }

    fn create_mock_data(bools: Vec<bool>) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        let data = add_batch_with_remove();
        Ok((data, bools))
    }

    // #[test]
    // fn test_multiple_chunks() {
    //     let base_url = Url::parse("file:///").unwrap();
    //     let data = vec![
    //         create_mock_data(vec![true; 3]),
    //         create_mock_data(vec![true; 3]),
    //         create_mock_data(vec![true; 3]),
    //     ];
    //     let chunker = CheckpointIterator::new(data.into_iter(), 8, base_url.clone());
    //     let result: Vec<_> = chunker.collect();

    //     assert_eq!(result.len(), 2);
    //     for (i, sidecar_file_result) in result.iter().enumerate() {
    //         let (iter, path) = sidecar_file_result.as_ref().unwrap();
    //         assert!(path.as_str().ends_with(&format!("{:020}.parquet", i)));
    //         println!("{:?}", path);
    //         // assert!(path.as_str().contains("_delta_log/sidecars"));
    //     }
    // }
}
