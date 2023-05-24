use crate::storage::StorageClient;
use crate::Version;
use arrow::json::RawReaderBuilder;
use arrow::{
    array::{BooleanArray, StringArray, StructArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use std::collections::HashSet;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::debug;

#[derive(Debug, Clone)]
pub(crate) struct LogSegment {
    log_files: Vec<LogFile>,
}

impl std::ops::Deref for LogSegment {
    type Target = Vec<LogFile>;
    fn deref(&self) -> &Self::Target {
        &self.log_files
    }
}

impl LogSegment {
    pub(crate) fn new<S: StorageClient>(
        location: PathBuf,
        version: Version,
        storage_client: &S,
    ) -> Self {
        // get log files from storage and sort them appropriately for replay (descending commits
        // first then checkpoints)
        // TODO read last checkpoint file, do a list_from, etc. to prevent reading entire log
        let mut delta_log_root = location.clone();
        delta_log_root.push("_delta_log");
        let mut log_files: Vec<LogFile> = storage_client
            .list(delta_log_root.to_str().unwrap())
            .into_iter()
            .flat_map(|path| path.try_into())
            .collect();
        // Sort deltas (commits) by newest file first, so we always return the latest version of
        // each action. Order doesn't matter for checkpoint files, because they are disjoint.
        log_files.sort();
        debug!("log segment for table {delta_log_root:?} at version {version}. log files: ");
        for f in &log_files {
            debug!("- {f:?}");
        }
        LogSegment { log_files }
    }
}

/// metadata required to read a valid table data file
// TODO add projection/schema and predicate
// TODO pub?
#[derive(Debug)]
pub(crate) struct DataFile {
    pub(crate) path: PathBuf,
    pub(crate) dv: Option<DvId>,
}

impl DataFile {
    pub(crate) fn new(path: PathBuf, dv: Option<DvId>) -> Self {
        DataFile { path, dv }
    }
}

#[derive(Debug)]
pub(crate) struct LogReplay {
    // TODO change to HashSet<DataFile>
    seen: HashSet<(PathBuf, Option<DvId>)>,
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
    pub(crate) fn replay(&mut self, actions_batch: RecordBatch) -> RecordBatch {
        debug!("replay actions: {:?}", actions_batch);
        debug!("last seen set: {:?}", self.seen);

        // TODO for the internal implementation of log replay, we should implement 'ages' to reduce
        // the size of the seen set
        let filter_vec: Vec<bool> = parse_record_batch(actions_batch.clone())
            .map(|action| match action {
                Some(Action::AddFile(path, dv))
                    if !self.seen.contains(&(path.to_path_buf(), dv.clone())) =>
                {
                    self.seen.insert((path.clone(), dv.clone()));
                    true
                }
                Some(Action::RemoveFile(path, dv)) | Some(Action::AddFile(path, dv)) => {
                    self.seen.insert((path, dv));
                    false
                }
                None => false,
            })
            .collect();
        arrow::compute::filter_record_batch(&actions_batch, &BooleanArray::from(filter_vec))
            .unwrap()
    }
}

// TODO de-normalize checkpoint/commit files?
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LogFile {
    /// CheckpointFile is a path to parquet checkpoint file
    CheckpointFile(LogFileInfo),
    /// CommitFile is a path to json commit file
    CommitFile(LogFileInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LogFileInfo {
    path: PathBuf,
    version: Version,
}

impl LogFileInfo {
    pub(crate) fn new(version: Version, path: PathBuf) -> Self {
        LogFileInfo { version, path }
    }
}

impl TryFrom<PathBuf> for LogFile {
    type Error = bool;
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let json = std::ffi::OsStr::new("json");
        let version = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<Version>()
            .unwrap();
        match path.extension() {
            Some(ext) if ext == json => Ok(LogFile::CommitFile(LogFileInfo::new(version, path))),
            _ => Ok(LogFile::CheckpointFile(LogFileInfo::new(version, path))),
        }
    }
}

impl LogFile {
    // 1. read commit or checkpoint file 'pointed to' by LogFile
    // 2. deserialize json or parquet into arrow record batches of actions
    // FIXME we need to pass predicate columns used down to this read so that we only parse a
    // subset of the stats we need.
    pub(crate) fn read<S: StorageClient>(
        &self,
        storage_client: &S,
    ) -> impl Iterator<Item = RecordBatch> {
        debug!("processing file: {self:?}");
        // FIXME
        let actions = match self {
            Self::CommitFile(LogFileInfo { version: _, path }) => storage_client.read(path),
            Self::CheckpointFile(LogFileInfo { version: _, path }) => storage_client.read(path),
        };

        // commits and checkpoints will be deserialized into arrow with a column per action type
        let dv_schema = arrow::datatypes::DataType::Struct(vec![
            Field::new("storageType", arrow::datatypes::DataType::Utf8, false),
            Field::new("pathOrInlineDv", arrow::datatypes::DataType::Utf8, false),
            Field::new("offset", arrow::datatypes::DataType::UInt64, false),
            Field::new("sizeInBytes", arrow::datatypes::DataType::UInt64, false),
            Field::new("cardinality", arrow::datatypes::DataType::UInt64, false),
        ]);
        let add_schema = arrow::datatypes::DataType::Struct(vec![
            Field::new("path", arrow::datatypes::DataType::Utf8, false),
            Field::new("size", arrow::datatypes::DataType::UInt64, false),
            Field::new("stats", arrow::datatypes::DataType::Utf8, false),
            Field::new("deletionVector", dv_schema, true),
        ]);
        let remove_schema = arrow::datatypes::DataType::Struct(vec![
            Field::new("path", arrow::datatypes::DataType::Utf8, false),
            Field::new("size", arrow::datatypes::DataType::UInt64, false),
            Field::new("stats", arrow::datatypes::DataType::Utf8, false),
        ]);
        let metadata_schema = arrow::datatypes::DataType::Struct(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Utf8,
            false,
        )]);

        let add_col = Field::new("add", add_schema, true);
        let remove_col = Field::new("remove", remove_schema, true);
        let metadata_col = Field::new("metaData", metadata_schema, true);

        let actions_schema = Schema::new(vec![add_col, remove_col, metadata_col]);
        RawReaderBuilder::new(actions_schema.into())
            .build(BufReader::new(actions.as_slice()))
            .unwrap()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|i| i.unwrap())
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    AddFile(PathBuf, Option<DvId>),
    RemoveFile(PathBuf, Option<DvId>),
}

pub(crate) type DvId = String;

pub(crate) fn parse_record_batch(actions: RecordBatch) -> impl Iterator<Item = Option<Action>> {
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

    add_paths
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
        })
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
}
