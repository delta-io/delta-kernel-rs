//! Various utility functions/macros used throughout the kernel

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

#[cfg(test)]
pub(crate) mod test_utils {
    use arrow_array::RecordBatch;
    use itertools::Itertools;
    use object_store::ObjectStore;
    use object_store::{local::LocalFileSystem, path};
    use parquet::arrow::ArrowWriter;
    use serde::Serialize;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;
    use test_utils::delta_path_for_version;
    use url::Url;

    use crate::{
        actions::{Add, Cdc, CommitInfo, Metadata, Protocol, Remove},
        engine::arrow_data::ArrowEngineData,
        EngineData,
    };

    #[derive(Serialize)]
    pub(crate) enum Action {
        #[serde(rename = "add")]
        Add(Add),
        #[serde(rename = "remove")]
        Remove(Remove),
        #[serde(rename = "cdc")]
        Cdc(Cdc),
        #[serde(rename = "metaData")]
        Metadata(Metadata),
        #[serde(rename = "protocol")]
        Protocol(Protocol),
        #[allow(unused)]
        #[serde(rename = "commitInfo")]
        CommitInfo(CommitInfo),
    }

    /// A mock table that writes commits to a local temporary delta log. This can be used to
    /// construct a delta log used for testing.
    pub(crate) struct LocalMockTable {
        commit_num: u64,
        store: Arc<LocalFileSystem>,
        dir: TempDir,
    }

    impl LocalMockTable {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            Self {
                commit_num: 0,
                store,
                dir,
            }
        }
        /// Writes all `actions` to a new commit in the log
        pub(crate) async fn commit(&mut self, actions: impl IntoIterator<Item = Action>) {
            let data = actions
                .into_iter()
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");

            let path = delta_path_for_version(self.commit_num, "json");
            self.commit_num += 1;

            self.store
                .put(&path, data.into())
                .await
                .expect("put log file in store");
        }

        /// Writes all `actions` to a new json checkpoint in the log
        pub(crate) async fn json_checkpoint(
            &mut self,
            actions: impl IntoIterator<Item = Action>,
            filename: &str,
        ) {
            let data = actions
                .into_iter()
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");

            let path = format!("_delta_log/{filename:020}");

            self.store
                .put(&path::Path::from(path), data.into())
                .await
                .expect("put log file in store");
        }

        /// Writes all `actions` to a new parquet checkpoint in the log
        pub(crate) async fn parquet_checkpoint(
            &mut self,
            data: Box<dyn EngineData>,
            filename: &str,
        ) {
            let batch: Box<_> = ArrowEngineData::try_from_engine_data(data).unwrap();
            let record_batch = batch.record_batch();

            let mut buffer = vec![];
            let mut writer =
                ArrowWriter::try_new(&mut buffer, record_batch.schema(), None).unwrap();
            writer.write(record_batch).unwrap();
            writer.close().unwrap(); // writer must be closed to write footer

            let path = format!("_delta_log/{filename:020}");

            self.store
                .put(&path::Path::from(path), buffer.into())
                .await
                .expect("put sidecar file in store");
        }

        /// Writes all `actions` as EngineData to a new sidecar file in the `_delta_log/_sidecars` directory
        pub(crate) async fn sidecar(&mut self, data: Box<dyn EngineData>, filename: &str) {
            let batch: Box<_> = ArrowEngineData::try_from_engine_data(data).unwrap();
            let record_batch = batch.record_batch();

            let mut buffer = vec![];
            let mut writer =
                ArrowWriter::try_new(&mut buffer, record_batch.schema(), None).unwrap();
            writer.write(record_batch).unwrap();
            writer.close().unwrap(); // writer must be closed to write footer

            let path = format!("_delta_log/_sidecars/{filename:020}");

            self.store
                .put(&path::Path::from(path), buffer.into())
                .await
                .expect("put sidecar file in store");
        }

        /// Get the path to the root of the table.
        pub(crate) fn table_root(&self) -> &Path {
            self.dir.path()
        }

        /// Get the path to the root of the log in the table.
        pub(crate) fn log_root(&self) -> Url {
            Url::from_directory_path(self.dir.path().join("_delta_log")).unwrap()
        }
    }

    /// Try to convert an `EngineData` into a `RecordBatch`. Panics if not using `ArrowEngineData` from
    /// the default module
    pub(crate) fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(engine_data)
            .unwrap()
            .into()
    }
}
