//! A [`StorageHandler`] implementation backed by a [`PlanExecutor`].
//!
//! [`PlanBasedStorageHandler`] translates each [`StorageHandler`] method into a
//! [`DeclarativePlanNode`], delegates execution to a [`PlanExecutor`], and unpacks the
//! [`PlanResult`] back into the expected return type.

use std::fmt;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use url::Url;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::{DeltaResult, EngineData, Error, FileMeta, FileSlice, StorageHandler};

/// A [`StorageHandler`] that delegates to a [`PlanExecutor`].
///
/// Each handler method constructs the appropriate [`DeclarativePlanNode`], executes it via
/// the plan executor, and converts the [`PlanResult`] back to the handler's return type.
pub struct PlanBasedStorageHandler {
    executor: Arc<dyn PlanExecutor>,
}

impl fmt::Debug for PlanBasedStorageHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBasedStorageHandler")
            .finish_non_exhaustive()
    }
}

impl PlanBasedStorageHandler {
    /// Create a new `PlanBasedStorageHandler` backed by the given plan executor.
    pub fn new(executor: Arc<dyn PlanExecutor>) -> Self {
        Self { executor }
    }
}

impl StorageHandler for PlanBasedStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let plan = DeclarativePlanNode::FileListing { url: path.clone() };
        let result = self.executor.execute_plan(plan)?;
        match result {
            PlanResult::Data(iter) => {
                let meta_iter = iter.flat_map(|batch_result| match batch_result {
                    Ok(batch) => match engine_data_to_file_metas(batch.as_ref()) {
                        Ok(metas) => metas.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
                        Err(e) => vec![Err(e)].into_iter(),
                    },
                    Err(e) => vec![Err(e)].into_iter(),
                });
                Ok(Box::new(meta_iter))
            }
            other => Err(Error::generic(format!(
                "expected PlanResult::Data for FileListing, got {other:?}"
            ))),
        }
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let plan = DeclarativePlanNode::ReadBytes { files };
        let result = self.executor.execute_plan(plan)?;
        match result {
            PlanResult::ByteStream(iter) => Ok(iter),
            other => Err(Error::generic(format!(
                "expected PlanResult::ByteStream for ReadBytes, got {other:?}"
            ))),
        }
    }

    #[allow(clippy::panic)]
    fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
        unimplemented!("PlanBasedStorageHandler does not yet support copy_atomic")
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        let plan = DeclarativePlanNode::WriteBytes {
            url: path.clone(),
            data,
            overwrite,
        };
        let result = self.executor.execute_plan(plan)?;
        match result {
            PlanResult::Unit => Ok(()),
            other => Err(Error::generic(format!(
                "expected PlanResult::Unit for WriteBytes, got {other:?}"
            ))),
        }
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        let plan = DeclarativePlanNode::HeadFile { url: path.clone() };
        let result = self.executor.execute_plan(plan)?;
        match result {
            PlanResult::Data(mut iter) => {
                let batch = iter
                    .next()
                    .ok_or_else(|| Error::generic("HeadFile returned no data batches"))??;
                let metas = engine_data_to_file_metas(batch.as_ref())?;
                metas
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::generic("HeadFile returned an empty batch with no rows"))
            }
            other => Err(Error::generic(format!(
                "expected PlanResult::Data for HeadFile, got {other:?}"
            ))),
        }
    }
}

/// Convert an [`EngineData`] batch (with [`FILE_META_SCHEMA`](crate::plan::FILE_META_SCHEMA)
/// columns) back into a `Vec<FileMeta>`.
///
/// This is the inverse of `file_metas_to_engine_data` in the [`naive`](super::naive) module.
/// Uses the [`RowVisitor`] pattern to extract data engine-agnostically.
fn engine_data_to_file_metas(data: &dyn EngineData) -> DeltaResult<Vec<FileMeta>> {
    let mut visitor = FileMetaVisitor {
        metas: Vec::with_capacity(data.len()),
    };
    visitor.visit_rows_of(data)?;
    Ok(visitor.metas)
}

/// A [`RowVisitor`] that extracts [`FileMeta`] from
/// [`FILE_META_SCHEMA`](crate::plan::FILE_META_SCHEMA) columnar data.
struct FileMetaVisitor {
    metas: Vec<FileMeta>,
}

impl RowVisitor for FileMetaVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    ColumnName::new(["path"]),
                    ColumnName::new(["last_modified"]),
                    ColumnName::new(["size"]),
                ],
                vec![DataType::STRING, DataType::LONG, DataType::LONG],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let path: &str = getters[0].get(i, "path")?;
            let last_modified: i64 = getters[1].get(i, "last_modified")?;
            let size: i64 = getters[2].get(i, "size")?;
            let location = Url::parse(path)
                .map_err(|e| Error::generic(format!("invalid URL in path column: {e}")))?;
            self.metas.push(FileMeta {
                location,
                last_modified,
                size: size as u64,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::PlanBasedStorageHandler;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::plan::NaivePlanExecutor;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::StorageHandler;

    /// Create a `PlanBasedStorageHandler` backed by a `NaivePlanExecutor` with an in-memory store.
    fn make_handler(store: Arc<InMemory>) -> PlanBasedStorageHandler {
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = Arc::new(NaivePlanExecutor::new(store, executor));
        PlanBasedStorageHandler::new(plan_exec)
    }

    // ==================== list_from tests ====================

    #[test]
    fn list_from_empty_directory() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store);

        let url = url::Url::parse("memory:///some_dir/").unwrap();
        let results: Vec<_> = handler
            .list_from(&url)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn list_from_returns_sorted_file_metadata() {
        let store = Arc::new(InMemory::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        for name in ["aaa.parquet", "bbb.parquet", "ccc.parquet"] {
            let path = Path::from(format!("table/{name}"));
            rt.block_on(store.put(&path, bytes::Bytes::from("data").into()))
                .unwrap();
        }

        let handler = make_handler(store);
        let url = url::Url::parse("memory:///table/").unwrap();
        let results: Vec<_> = handler
            .list_from(&url)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(results.len(), 3);

        let paths: Vec<&str> = results.iter().map(|m| m.location.as_str()).collect();
        assert!(paths.windows(2).all(|w| w[0] <= w[1]));

        assert!(paths.iter().any(|p| p.contains("aaa.parquet")));
        assert!(paths.iter().any(|p| p.contains("bbb.parquet")));
        assert!(paths.iter().any(|p| p.contains("ccc.parquet")));

        for meta in &results {
            assert!(meta.size > 0, "file size should be > 0");
        }
    }

    // ==================== read_files tests ====================

    #[test]
    fn read_files_returns_file_contents() {
        let store = Arc::new(InMemory::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let contents = [
            ("dir/a.txt", "hello"),
            ("dir/b.txt", "world"),
            ("dir/c.txt", "!"),
        ];
        for (path, data) in &contents {
            rt.block_on(store.put(&Path::from(*path), bytes::Bytes::from(*data).into()))
                .unwrap();
        }

        let handler = make_handler(store);
        let files: Vec<crate::FileSlice> = contents
            .iter()
            .map(|(p, _)| (url::Url::parse(&format!("memory:///{p}")).unwrap(), None))
            .collect();

        let collected: Vec<bytes::Bytes> = handler
            .read_files(files)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].as_ref(), b"hello");
        assert_eq!(collected[1].as_ref(), b"world");
        assert_eq!(collected[2].as_ref(), b"!");
    }

    #[test]
    fn read_files_with_range() {
        let store = Arc::new(InMemory::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(
            &Path::from("file.bin"),
            bytes::Bytes::from("abcdefghij").into(),
        ))
        .unwrap();

        let handler = make_handler(store);
        let files = vec![(url::Url::parse("memory:///file.bin").unwrap(), Some(2..5))];

        let mut iter = handler.read_files(files).unwrap();
        let chunk = iter.next().unwrap().unwrap();

        assert_eq!(chunk.as_ref(), b"cde");
        assert!(iter.next().is_none());
    }

    // ==================== put + head round-trip tests ====================

    #[test]
    fn put_then_head_round_trip() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store);

        let url = url::Url::parse("memory:///out/file.txt").unwrap();
        handler
            .put(&url, bytes::Bytes::from("payload"), false)
            .unwrap();

        let meta = handler.head(&url).unwrap();
        assert!(meta.location.as_str().contains("out/file.txt"));
        assert_eq!(meta.size, 7); // "payload".len()
    }

    #[test]
    fn put_no_overwrite_errors_on_existing_file() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store);

        let url = url::Url::parse("memory:///dup.txt").unwrap();
        handler
            .put(&url, bytes::Bytes::from("first"), false)
            .unwrap();

        let err = handler
            .put(&url, bytes::Bytes::from("second"), false)
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::FileAlreadyExists(_)),
            "expected FileAlreadyExists, got: {err:?}"
        );
    }

    // ==================== head tests ====================

    #[test]
    fn head_returns_metadata() {
        let store = Arc::new(InMemory::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(
            &Path::from("meta/test.dat"),
            bytes::Bytes::from("contents").into(),
        ))
        .unwrap();

        let handler = make_handler(store);
        let url = url::Url::parse("memory:///meta/test.dat").unwrap();
        let meta = handler.head(&url).unwrap();

        assert!(meta.location.as_str().contains("meta/test.dat"));
        assert_eq!(meta.size, 8); // "contents".len()
    }

    #[test]
    fn head_returns_error_for_missing_file() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store);

        let url = url::Url::parse("memory:///nonexistent.txt").unwrap();
        let err = handler.head(&url).unwrap_err();
        assert!(
            format!("{err:?}").contains("not found") || format!("{err:?}").contains("NotFound"),
            "expected a not-found error, got: {err:?}"
        );
    }
}
