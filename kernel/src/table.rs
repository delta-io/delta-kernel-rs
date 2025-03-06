//! In-memory representation of a Delta table, which acts as an immutable root entity for reading
//! the different versions

use std::borrow::Cow;
use std::collections::VecDeque;
use std::iter::Peekable;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use url::Url;

use crate::actions::{get_log_schema, get_v1_checkpoint_schema, ADD_NAME, REMOVE_NAME};
use crate::checkpoint::iterators::{CheckpointIterator, SidecarIterator, V1CheckpointFileIterator};
use crate::checkpoint::log_replay::checkpoint_actions_iter;
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

pub type FileDataToWrite = DeltaResult<(
    Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send>,
    Url,
)>;

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

    /// Returns a [`CheckpointIterator`] that can be used to write the table's state to storage.
    ///
    /// If `version` is not supplied, the latest version will be used.
    /// If the `v2Checkpoints` feature is enabled, the iterator will provide data to write the
    /// new V2 checkpoint format. Otherwise, the iterator will provide data to write the V1
    /// checkpoint format.
    pub fn checkpoint(
        &self,
        engine: &dyn Engine,
        version: Option<u64>,
    ) -> DeltaResult<Box<dyn CheckpointIterator<Item = FileDataToWrite>>> {
        let snapshot = self.snapshot(engine, version)?;

        // todo!(update table config)
        // let isV2CheckpointsSupported = snapshot.table_configuration().is_v2_checkpoint_write_supported();

        if true {
            self.v1checkpoint(engine, snapshot)
        } else {
            // TODO!
            self.v1checkpoint(engine, snapshot)
        }
    }

    //TODO!
    pub fn v2checkpoint(
        &self,
        engine: &dyn Engine,
        snapshot: Snapshot,
    ) -> DeltaResult<Box<dyn CheckpointIterator<Item = FileDataToWrite>>> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;

        let iter = snapshot.log_segment().replay(
            engine,
            commit_read_schema,
            checkpoint_read_schema,
            None,
        )?;
        let total_actions_counter = Arc::<AtomicUsize>::new(AtomicUsize::new(0));
        let total_add_actions_counter = Arc::<AtomicUsize>::new(AtomicUsize::new(0));

        let actions = checkpoint_actions_iter(
            iter,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
        );

        Ok(Box::new(SidecarIterator::new(
            actions,
            2,
            self.location.clone(),
            snapshot.protocol().clone(),
            snapshot.metadata().clone(),
        )))
    }

    /// Returns a [`V1CheckpointFileIterator`] which yields the actions to write the V1 checkpoint file.
    ///
    /// The generic is required as rust can not infer the concrete type for I in V1CheckpointFileIterator
    /// as replay returns an opaque type.
    pub fn v1checkpoint(
        &self,
        engine: &dyn Engine,
        snapshot: Snapshot,
    ) -> DeltaResult<Box<dyn CheckpointIterator<Item = FileDataToWrite>>> {
        let schema = get_v1_checkpoint_schema();

        let iter = snapshot
            .log_segment()
            .replay(engine, schema.clone(), schema.clone(), None)?;

        let total_actions_counter = Arc::<AtomicUsize>::new(AtomicUsize::new(0));
        let total_add_actions_counter = Arc::<AtomicUsize>::new(AtomicUsize::new(0));

        let actions = checkpoint_actions_iter(
            iter,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
        );

        Ok(Box::new(V1CheckpointFileIterator::new(
            actions,
            self.location.clone(),
            snapshot.version(),
            total_actions_counter,
            total_add_actions_counter,
        )))
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
        actions::{Metadata, Protocol},
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
    fn test_v1_checkpoint() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let mut checkpointer = table.checkpoint(&engine, None).unwrap();

        for checkpoint_file_iterator in &mut checkpointer {
            let (iter, file_path) = checkpoint_file_iterator.unwrap();
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
                let pretty_format =
                    arrow::util::pretty::pretty_format_batches(&[record_batch.clone()])
                        .unwrap()
                        .to_string();
                println!("{}", pretty_format);
            }

            // write the parquet file
        }

        let result = checkpointer.sidecar_metadata(vec![]);
        assert!(result.is_err());

        let field = Arc::new(Field::new("size", DataType::Int64, false));
        let schema = Schema::new([field.clone()]);

        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let result = checkpointer.checkpoint_metadata(&ArrowEngineData::new(record_batch), &engine);

        if let Err(err) = result {
            panic!("Error: {:?}", err);
        }

        assert!(result.is_ok())
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

    #[test]
    fn test_sidecar_iterator() {
        // Use a base URL for file paths.
        let base_url = Url::parse("file:///").unwrap();
        let data = vec![
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
            create_mock_data(vec![true; 1]),
        ];
        let chunker = SidecarIterator::new(
            data.into_iter(),
            3,
            base_url.clone(),
            Protocol::default(),
            Metadata::default(),
        );
        // Collect all sidecar file outputs.
        let result: Vec<_> = chunker.collect();

        // We expect 4 chunks.
        assert_eq!(result.len(), 4);
        for sidecar_file_result in result {
            let (file_iter, path) = sidecar_file_result.unwrap();
            // Verify that the sidecar file name matches the expected format.
            assert!(path.as_str().ends_with(&format!(".parquet")));
            println!("{:?}", path);
            // Optionally, you can also test that the file_iter produces the expected data.
            let collected: Vec<_> = file_iter.collect();
            assert!(!collected.is_empty(), "Expected file data to be non-empty");
        }
    }
}
