use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Fields, Schema};
use arrow_select::filter::filter_record_batch;
use bytes::Buf;
use futures::future::Either;
use futures::prelude::*;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::DeltaResult;
use crate::*;

#[derive(Debug, Clone)]
pub(crate) struct LogSegment {
    pub(crate) log_files: Vec<LogFile>,
}

impl std::ops::Deref for LogSegment {
    type Target = Vec<LogFile>;
    fn deref(&self) -> &Self::Target {
        &self.log_files
    }
}

impl LogSegment {
    pub(crate) async fn try_new(
        location: Path,
        version: Version,
        storage: Arc<dyn ObjectStore>,
    ) -> DeltaResult<Self> {
        // get log files from storage and sort them appropriately for replay (descending commits
        // first then checkpoints)
        // TODO read last checkpoint file, do a list_from, etc. to prevent reading entire log
        let delta_log_root = location.child("_delta_log");
        let mut log_files: Vec<LogFile> = vec![];
        let mut list_stream = storage.list(Some(&delta_log_root)).await?;

        while let Some(result) = list_stream.next().await {
            match result {
                Ok(object_meta) => {
                    if let Ok(log_file) = object_meta.location.try_into() {
                        log_files.push(log_file);
                    }
                }
                Err(e) => {
                    error!("Failed to list item: {:?}", e);
                }
            }
        }

        // Sort deltas (commits) by newest file first, so we always return the latest version of
        // each action. Order doesn't matter for checkpoint files, because they are disjoint.
        log_files.sort();
        debug!("log segment for table {delta_log_root:?} at version {version}. log files: ");
        for f in &log_files {
            debug!("- {f:?}");
        }

        Ok(Self { log_files })
    }
}

/// metadata required to read a valid table data file
// TODO add projection/schema and predicate
// TODO pub?
#[derive(Debug)]
pub(crate) struct DataFile {
    pub(crate) path: Path,
    pub(crate) dv: Option<DvId>,
}

impl DataFile {
    pub(crate) fn new(path: Path, dv: Option<DvId>) -> Self {
        DataFile { path, dv }
    }
}

#[derive(Debug)]
pub(crate) struct LogReplay {
    // TODO change to HashSet<DataFile>
    seen: HashSet<(Path, Option<DvId>)>,
    // ages: HashMap<Version, HashSet<PathBuf>>
}

impl LogReplay {
    pub(crate) fn new() -> Self {
        LogReplay {
            seen: HashSet::new(),
        }
    }

    /// replay the batch of actions: note (1) and (2) below are independent
    /// 1. add all RemoveFile actions to seen_set
    /// 2. return(AddFiles - seen_set)
    /// or by-row:
    /// for each action => if add and not seen, add to seen and yield; if remove, add to seen
    ///
    /// this batch-wise log replay should take a record batch of actions and replay all
    /// simultaneously. there is no ordering of actions within a commit, so as long as all actions
    /// come from the same commit (or checkpoint) then they can be applied to replay in any order.
    ///
    /// note commit/checkpoint read chunking determines parallelism.
    ///
    /// commit2 -> actions_batch_1 -> actions_batch_1_after_data_skipping -> files_batch_1
    ///         -> actions_batch_2 -> actions_batch_2_after_data_skipping -> files_batch_2
    /// commit1 -> ...
    ///
    /// in the future, this can be implemented as a join for step (1) above and anti-join for step
    /// (2) when integrated with query engines that support joins.
    pub(crate) fn replay(&mut self, actions_batch: RecordBatch) -> DeltaResult<RecordBatch> {
        debug!("replay actions: {:?}", actions_batch);
        debug!("last seen set: {:?}", self.seen);

        // TODO for the internal implementation of log replay, we should implement 'ages' to reduce
        // the size of the seen set
        let filter_vec: Vec<bool> = parse_record_batch(actions_batch.clone())?
            .map(|action| match action {
                Some(Action::AddFile(path, dv))
                    if !self.seen.contains(&(path.clone(), dv.clone())) =>
                {
                    self.seen.insert((path, dv));
                    true
                }
                Some(Action::RemoveFile(path, dv)) | Some(Action::AddFile(path, dv)) => {
                    self.seen.insert((path, dv));
                    false
                }
                None => false,
            })
            .collect();
        Ok(filter_record_batch(
            &actions_batch,
            &BooleanArray::from(filter_vec),
        )?)
    }
}

// TODO de-normalize checkpoint/commit files?
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogFile {
    /// CheckpointFile is a path to parquet checkpoint file
    CheckpointFile(LogFileInfo),
    /// CommitFile is a path to json commit file
    CommitFile(LogFileInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogFileInfo {
    path: Path,
    version: Version,
}

impl LogFileInfo {
    pub fn new(version: Version, path: Path) -> Self {
        LogFileInfo { version, path }
    }
}

impl TryFrom<Path> for LogFile {
    type Error = object_store::Error;

    /// Convert the given [object_store::path::Path] into a [LogFile]
    ///
    /// This will error for all cases unless the provided path is a versioned commit file
    /// - `00000000000000000001.json`
    /// - `00000000000000000001.checkpoint.parquet`
    ///
    /// ```rust
    /// # use object_store::path::Path;
    /// # use deltakernel::LogFile;
    /// let path = Path::from("/tmp/test/_delta_log/00000000000000000001.checkpoint.parquet");
    /// let log_file: LogFile = path.try_into().unwrap();
    /// ```
    fn try_from(path: Path) -> Result<Self, Self::Error> {
        let info = LogFileInfo {
            path: path.clone(),
            version: version_from_path(&path)?,
        };

        if let Some(extension) = path.extension() {
            match extension {
                "json" => Ok(LogFile::CommitFile(info)),
                "parquet" => Ok(LogFile::CheckpointFile(info)),
                _unknown => {
                    let e = format!("Unknown extension for file: {:?}", path);
                    error!("{}", &e);
                    let err = Error::new(ErrorKind::Other, e);
                    Err(object_store::Error::Generic {
                        store: "invalid",
                        source: Box::new(err),
                    })
                }
            }
        } else {
            let e = format!("Missing extension for file: {:?}", path);
            error!("{}", &e);
            let err = Error::new(ErrorKind::Other, e);
            Err(object_store::Error::Generic {
                store: "invalid",
                source: Box::new(err),
            })
        }
    }
}

impl LogFile {
    pub(crate) fn schema() -> Schema {
        // commits and checkpoints will be deserialized into arrow with a column per action type
        let dv_schema = DataType::Struct(Fields::from(vec![
            Field::new("storageType", DataType::Utf8, false),
            Field::new("pathOrInlineDv", DataType::Utf8, false),
            Field::new("offset", DataType::UInt64, true),
            Field::new("sizeInBytes", DataType::UInt64, false),
            Field::new("cardinality", DataType::UInt64, false),
        ]));
        let add_schema = DataType::Struct(Fields::from(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("stats", DataType::Utf8, true),
            Field::new("dataChange", DataType::Boolean, false),
            Field::new("deletionVector", dv_schema, true),
            /*
             * TODO: Missing required fields: partitionValues, modificationTime
             */
        ]));
        let remove_schema = DataType::Struct(Fields::from(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("dataChange", DataType::Boolean, false),
            Field::new("size", DataType::UInt64, true),
        ]));
        let metadata_schema =
            DataType::Struct(Fields::from(vec![Field::new("id", DataType::Utf8, false)]));

        let add_col = Field::new("add", add_schema, true);
        let remove_col = Field::new("remove", remove_schema, true);
        let metadata_col = Field::new("metaData", metadata_schema, true);

        Schema::new(vec![add_col, remove_col, metadata_col])
    }
    // 1. read commit or checkpoint file 'pointed to' by LogFile
    // 2. deserialize json or parquet into arrow record batches of actions
    // FIXME we need to pass predicate columns used down to this read so that we only parse a
    // subset of the stats we need.
    pub(crate) async fn read(
        &self,
        storage: Arc<dyn ObjectStore>,
    ) -> impl Iterator<Item = RecordBatch> {
        debug!("processing file: {self:?}");
        // FIXME: Error handling on this read must occur
        let actions = match self {
            Self::CommitFile(LogFileInfo { version: _, path }) => {
                storage.get(path).await.expect("Failed to get path")
            }
            Self::CheckpointFile(LogFileInfo { version: _, path }) => {
                storage.get(path).await.expect("Failed to get path")
            }
        };

        let bytes = actions
            .bytes()
            .await
            .expect("Failed to get bytes from log file");

        ReaderBuilder::new(Self::schema().into())
            .build(bytes.reader())
            .unwrap()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|batch| batch.unwrap())
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    AddFile(Path, Option<DvId>),
    RemoveFile(Path, Option<DvId>),
}

pub(crate) type DvId = String;

/// Consume a stream of [RecordBatch] which represent "actions" from the Delta Log
///
/// This function will then emit a new stream of the parsed actions from that stream
pub(crate) fn from_actions_batch(
    actions_stream: impl Stream<Item = DeltaResult<RecordBatch>>,
) -> impl Stream<Item = DeltaResult<Option<Action>>> {
    actions_stream
        .map_ok(|batch| match parse_record_batch(batch) {
            Ok(res) => Either::Left(stream::iter(res.map(Ok))),
            Err(err) => Either::Right(stream::once(async { Err(err) })),
        })
        .try_flatten()
}

pub(crate) fn parse_record_batch(
    actions: RecordBatch,
) -> DeltaResult<impl Iterator<Item = Option<Action>>> {
    let actions = Box::leak(Box::new(actions)); // FIXME
    let add_paths = actions
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let remove_paths = actions
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let dvs = actions
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>() //adds
        .unwrap()
        .column_by_name("deletionVector")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column_by_name("pathOrInlineDv")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    Ok(add_paths
        .into_iter()
        .zip(remove_paths.into_iter())
        .zip(dvs.into_iter())
        .map(|((add, remove), dv)| {
            if let Some(add_path) = add {
                return Some(Action::AddFile(add_path.into(), dv.map(|s| s.to_owned())));
            }
            if let Some(remove_path) = remove {
                return Some(Action::RemoveFile(remove_path.into(), None));
            }
            None
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_sorting() {
        let commit1 = LogFile::CommitFile(LogFileInfo {
            version: 1,
            path: "_delta_log/0001.json".into(),
        });
        let commit2 = LogFile::CommitFile(LogFileInfo {
            version: 2,
            path: "_delta_log/0002.json".into(),
        });
        let commit3 = LogFile::CommitFile(LogFileInfo {
            version: 3,
            path: "_delta_log/0003.json".into(),
        });
        let checkpoint3a = LogFile::CheckpointFile(LogFileInfo {
            version: 3,
            path: "_delta_log/0002.checkpoint.0001.0002.parquet".into(),
        });
        let checkpoint3b = LogFile::CheckpointFile(LogFileInfo {
            version: 3,
            path: "_delta_log/0002.checkpoint.0002.0002.parquet".into(),
        });

        let expected: Vec<LogFile> = vec![
            checkpoint3a.clone(),
            checkpoint3b.clone(),
            commit1.clone(),
            commit2.clone(),
            commit3.clone(),
        ];
        let mut log: Vec<LogFile> = vec![commit3, commit1, checkpoint3b, commit2, checkpoint3a];
        log.sort();
        assert!(expected.iter().eq(log.iter()));
    }

    #[test]
    fn test_logfile_tryfrom_path() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001.json");
        let log_file: LogFile = path.try_into().expect("Should be able to convert");

        match log_file {
            LogFile::CommitFile(info) => {
                // This is correct
                assert_eq!(1, info.version);
            }
            LogFile::CheckpointFile(_) => {
                assert!(false, "The LogFile is a CommitFile not a checkpoint");
            }
        }
    }

    #[test]
    fn test_logfile_tryfrom_path_with_checkpoint() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001.checkpoint.parquet");
        let log_file: LogFile = path.try_into().expect("Should be able to convert");

        match log_file {
            LogFile::CommitFile(_) => {
                assert!(false, "The LogFile should be a checkpoint");
            }
            LogFile::CheckpointFile(info) => {
                // This is correct
                assert_eq!(1, info.version);
            }
        }
    }

    #[test]
    fn test_logfile_tryfrom_path_with_invalid_file_ext() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001.checkpoint.png");
        let result: Result<LogFile, object_store::Error> = path.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_logfile_tryfrom_path_with_absent_ext() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001");
        let result: Result<LogFile, object_store::Error> = path.try_into();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_from_actions_batch() {
        use std::fs::File;
        use std::io::BufReader;
        let cursor = BufReader::new(
            File::open("tests/data/table-with-dv-small/_delta_log/00000000000000000000.json")
                .unwrap(),
        );

        let schema = LogFile::schema();

        let json = arrow::json::ReaderBuilder::new(schema.into())
            .build(cursor)
            .unwrap();
        // `json` is an Iterable<Item=Result<RecordBatch, ArrowError>>
        let mut stream = from_actions_batch(stream::iter(json).map(|a| Ok(a.unwrap()))).boxed();
        let mut total = 0;

        while let Some(Ok(action)) = stream.next().await {
            match action {
                Some(Action::AddFile(_path, _dv)) => total += 1,
                _ => {}
            }
        }
        assert_eq!(1, total, "Failed to stream the right number of actions");
    }
}
