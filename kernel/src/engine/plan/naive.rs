//! A naive [`PlanExecutor`] implementation backed by [`ObjectStoreStorageHandler`].
//!
//! [`NaivePlanExecutor`] interprets each [`DeclarativePlanNode`] variant by delegating to the
//! default engine's existing handler implementations and converting results into columnar
//! [`EngineData`] batches.

use std::sync::Arc;

use bytes::Bytes;

use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::object_store::DynObjectStore;
use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult, FILE_META_SCHEMA};
use crate::{DeltaResult, EngineData, Error, FileMeta, FileSlice, StorageHandler as _};

/// A naive [`PlanExecutor`] that delegates to [`ObjectStoreStorageHandler`] and converts
/// results into [`ArrowEngineData`] batches.
#[derive(Debug)]
pub struct NaivePlanExecutor<E: TaskExecutor> {
    storage: ObjectStoreStorageHandler<E>,
}

impl<E: TaskExecutor> NaivePlanExecutor<E> {
    /// Create a new `NaivePlanExecutor` backed by the given object store and task executor.
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            storage: ObjectStoreStorageHandler::new(store, task_executor),
        }
    }
}

impl<E: TaskExecutor> PlanExecutor for NaivePlanExecutor<E> {
    fn execute_plan(&self, plan: DeclarativePlanNode) -> DeltaResult<PlanResult> {
        match plan {
            DeclarativePlanNode::FileListing { url } => self.execute_file_listing(&url),
            DeclarativePlanNode::ReadBytes { files } => self.execute_read_bytes(files),
            DeclarativePlanNode::WriteBytes {
                url,
                data,
                overwrite,
            } => self.execute_write_bytes(&url, data, overwrite),
            DeclarativePlanNode::HeadFile { url } => self.execute_head_file(&url),
        }
    }
}

impl<E: TaskExecutor> NaivePlanExecutor<E> {
    fn execute_file_listing(&self, url: &url::Url) -> DeltaResult<PlanResult> {
        let file_metas: Vec<FileMeta> = self
            .storage
            .list_from(url)?
            .collect::<DeltaResult<Vec<_>>>()?;
        let batch = file_metas_to_engine_data(&file_metas)?;
        let iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>> =
            Box::new(std::iter::once(Ok(batch)));
        Ok(PlanResult::Data(iter))
    }

    fn execute_read_bytes(&self, files: Vec<FileSlice>) -> DeltaResult<PlanResult> {
        let iter = self.storage.read_files(files)?;
        Ok(PlanResult::ByteStream(iter))
    }

    fn execute_write_bytes(
        &self,
        url: &url::Url,
        data: Bytes,
        overwrite: bool,
    ) -> DeltaResult<PlanResult> {
        self.storage.put(url, data, overwrite)?;
        Ok(PlanResult::Unit)
    }

    fn execute_head_file(&self, url: &url::Url) -> DeltaResult<PlanResult> {
        let meta = self.storage.head(url)?;
        let batch = file_metas_to_engine_data(std::slice::from_ref(&meta))?;
        let iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>> =
            Box::new(std::iter::once(Ok(batch)));
        Ok(PlanResult::Data(iter))
    }
}

/// Convert a slice of [`FileMeta`] into a single [`ArrowEngineData`] batch matching
/// [`FILE_META_SCHEMA`].
fn file_metas_to_engine_data(metas: &[FileMeta]) -> DeltaResult<Box<dyn EngineData>> {
    let paths: StringArray = metas.iter().map(|m| Some(m.location.as_str())).collect();
    let last_modified: Int64Array = metas.iter().map(|m| Some(m.last_modified)).collect();
    let sizes: Int64Array = metas
        .iter()
        .map(|m| {
            i64::try_from(m.size)
                .ok()
                .map(Some)
                .unwrap_or(Some(i64::MAX))
        })
        .collect();

    let _ = &*FILE_META_SCHEMA;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("path", ArrowDataType::Utf8, false),
        ArrowField::new("last_modified", ArrowDataType::Int64, false),
        ArrowField::new("size", ArrowDataType::Int64, false),
    ]));

    let columns: Vec<ArrayRef> = vec![Arc::new(paths), Arc::new(last_modified), Arc::new(sizes)];

    let batch = RecordBatch::try_new(arrow_schema, columns)
        .map_err(|e| Error::generic(format!("Failed to create RecordBatch: {e}")))?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::NaivePlanExecutor;
    use crate::arrow::array::{Array as _, AsArray as _, Int64Array, RecordBatch, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult, FILE_META_SCHEMA};

    /// Unwrap a [`PlanResult::Data`] variant, panicking with a clear message on mismatch.
    fn unwrap_data(
        result: PlanResult,
    ) -> Box<dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>>> {
        match result {
            PlanResult::Data(iter) => iter,
            other => panic!("expected PlanResult::Data, got {other:?}"),
        }
    }

    /// Unwrap a [`PlanResult::ByteStream`] variant, panicking with a clear message on mismatch.
    fn unwrap_byte_stream(
        result: PlanResult,
    ) -> Box<dyn Iterator<Item = crate::DeltaResult<bytes::Bytes>>> {
        match result {
            PlanResult::ByteStream(iter) => iter,
            other => panic!("expected PlanResult::ByteStream, got {other:?}"),
        }
    }

    // ==================== FileListing tests ====================

    #[test]
    fn file_listing_returns_correct_schema_for_empty_directory() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = NaivePlanExecutor::new(store, executor);

        let plan = DeclarativePlanNode::FileListing {
            url: url::Url::parse("memory:///some_dir/").unwrap(),
        };

        let result = plan_exec.execute_plan(plan).unwrap();
        let mut iter = unwrap_data(result);
        let batch = iter.next().unwrap().unwrap();

        assert_eq!(batch.len(), 0);
        assert!(iter.next().is_none());
    }

    #[test]
    fn file_listing_returns_file_metadata_as_engine_data() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        for name in ["aaa.parquet", "bbb.parquet", "ccc.parquet"] {
            let path = Path::from(format!("table/{name}"));
            rt.block_on(store.put(&path, bytes::Bytes::from("data").into()))
                .unwrap();
        }

        let plan_exec = NaivePlanExecutor::new(store, executor);
        let plan = DeclarativePlanNode::FileListing {
            url: url::Url::parse("memory:///table/").unwrap(),
        };

        let result = plan_exec.execute_plan(plan).unwrap();
        let mut iter = unwrap_data(result);
        let batch_box = iter.next().unwrap().unwrap();

        assert_eq!(batch_box.len(), 3);
        assert!(iter.next().is_none());

        let arrow_data = batch_box.into_any().downcast::<ArrowEngineData>().unwrap();
        let batch: RecordBatch = (*arrow_data).into();

        let expected_names: Vec<&str> =
            FILE_META_SCHEMA.fields().map(|f| f.name.as_str()).collect();
        let schema = batch.schema();
        let actual_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(actual_names, expected_names);

        let paths: &StringArray = batch.column(0).as_string();
        let path_values: Vec<&str> = (0..paths.len()).map(|i| paths.value(i)).collect();
        assert!(path_values.windows(2).all(|w| w[0] <= w[1]));

        assert!(path_values.iter().any(|p| p.contains("aaa.parquet")));
        assert!(path_values.iter().any(|p| p.contains("bbb.parquet")));
        assert!(path_values.iter().any(|p| p.contains("ccc.parquet")));

        let sizes: &Int64Array = batch.column(2).as_primitive();
        for i in 0..sizes.len() {
            assert!(sizes.value(i) > 0, "file size should be > 0");
        }
    }

    // ==================== ReadBytes tests ====================

    #[test]
    fn read_bytes_returns_file_contents() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());

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

        let plan_exec = NaivePlanExecutor::new(store, executor);
        let files: Vec<crate::FileSlice> = contents
            .iter()
            .map(|(p, _)| (url::Url::parse(&format!("memory:///{p}")).unwrap(), None))
            .collect();

        let plan = DeclarativePlanNode::ReadBytes { files };
        let result = plan_exec.execute_plan(plan).unwrap();
        let iter = unwrap_byte_stream(result);
        let collected: Vec<bytes::Bytes> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].as_ref(), b"hello");
        assert_eq!(collected[1].as_ref(), b"world");
        assert_eq!(collected[2].as_ref(), b"!");
    }

    #[test]
    fn read_bytes_with_range() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(
            &Path::from("file.bin"),
            bytes::Bytes::from("abcdefghij").into(),
        ))
        .unwrap();

        let plan_exec = NaivePlanExecutor::new(store, executor);
        let plan = DeclarativePlanNode::ReadBytes {
            files: vec![(url::Url::parse("memory:///file.bin").unwrap(), Some(2..5))],
        };

        let result = plan_exec.execute_plan(plan).unwrap();
        let mut iter = unwrap_byte_stream(result);
        let chunk = iter.next().unwrap().unwrap();

        assert_eq!(chunk.as_ref(), b"cde");
        assert!(iter.next().is_none());
    }

    // ==================== WriteBytes tests ====================

    #[test]
    fn write_bytes_creates_file() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = NaivePlanExecutor::new(store, executor);

        let url = url::Url::parse("memory:///out/file.txt").unwrap();
        let plan = DeclarativePlanNode::WriteBytes {
            url: url.clone(),
            data: bytes::Bytes::from("payload"),
            overwrite: false,
        };

        let result = plan_exec.execute_plan(plan).unwrap();
        assert!(matches!(result, PlanResult::Unit));

        let head_plan = DeclarativePlanNode::HeadFile { url };
        let head_result = plan_exec.execute_plan(head_plan).unwrap();
        let mut iter = unwrap_data(head_result);
        let batch_box = iter.next().unwrap().unwrap();
        assert_eq!(batch_box.len(), 1);

        let arrow_data = batch_box.into_any().downcast::<ArrowEngineData>().unwrap();
        let batch: RecordBatch = (*arrow_data).into();
        let sizes: &Int64Array = batch.column(2).as_primitive();
        assert_eq!(sizes.value(0), 7); // "payload".len()
    }

    #[test]
    fn write_bytes_no_overwrite_errors_on_existing_file() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = NaivePlanExecutor::new(store, executor);

        let url = url::Url::parse("memory:///dup.txt").unwrap();
        let plan = DeclarativePlanNode::WriteBytes {
            url: url.clone(),
            data: bytes::Bytes::from("first"),
            overwrite: false,
        };
        plan_exec.execute_plan(plan).unwrap();

        let plan2 = DeclarativePlanNode::WriteBytes {
            url,
            data: bytes::Bytes::from("second"),
            overwrite: false,
        };
        let err = plan_exec.execute_plan(plan2).unwrap_err();
        assert!(
            matches!(err, crate::Error::FileAlreadyExists(_)),
            "expected FileAlreadyExists, got: {err:?}"
        );
    }

    // ==================== HeadFile tests ====================

    #[test]
    fn head_file_returns_metadata() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(
            &Path::from("meta/test.dat"),
            bytes::Bytes::from("contents").into(),
        ))
        .unwrap();

        let plan_exec = NaivePlanExecutor::new(store, executor);
        let url = url::Url::parse("memory:///meta/test.dat").unwrap();
        let plan = DeclarativePlanNode::HeadFile { url: url.clone() };

        let result = plan_exec.execute_plan(plan).unwrap();
        let mut iter = unwrap_data(result);
        let batch_box = iter.next().unwrap().unwrap();

        assert_eq!(batch_box.len(), 1);
        assert!(iter.next().is_none());

        let arrow_data = batch_box.into_any().downcast::<ArrowEngineData>().unwrap();
        let batch: RecordBatch = (*arrow_data).into();

        let expected_names: Vec<&str> =
            FILE_META_SCHEMA.fields().map(|f| f.name.as_str()).collect();
        let schema = batch.schema();
        let actual_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(actual_names, expected_names);

        let paths: &StringArray = batch.column(0).as_string();
        assert!(paths.value(0).contains("meta/test.dat"));

        let sizes: &Int64Array = batch.column(2).as_primitive();
        assert_eq!(sizes.value(0), 8); // "contents".len()
    }

    #[test]
    fn head_file_returns_error_for_missing_file() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = NaivePlanExecutor::new(store, executor);

        let plan = DeclarativePlanNode::HeadFile {
            url: url::Url::parse("memory:///nonexistent.txt").unwrap(),
        };

        let err = plan_exec.execute_plan(plan).unwrap_err();
        assert!(
            format!("{err:?}").contains("not found") || format!("{err:?}").contains("NotFound"),
            "expected a not-found error, got: {err:?}"
        );
    }
}
