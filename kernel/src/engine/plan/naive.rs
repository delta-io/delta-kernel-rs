//! A naive [`PlanExecutor`] implementation backed by [`ObjectStoreStorageHandler`].
//!
//! [`NaivePlanExecutor`] interprets each [`DeclarativePlanNode`] variant by delegating to the
//! default engine's existing handler implementations and converting results into columnar
//! [`EngineData`](crate::EngineData) batches.

use std::sync::Arc;

use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::object_store::DynObjectStore;
use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult, FILE_META_SCHEMA};
use crate::{DeltaResult, EngineData, Error, FileMeta, StorageHandler as _};

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

    #[test]
    fn file_listing_returns_correct_schema_for_empty_directory() {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = NaivePlanExecutor::new(store, executor);

        let plan = DeclarativePlanNode::FileListing {
            url: url::Url::parse("memory:///some_dir/").unwrap(),
        };

        let result = plan_exec.execute_plan(plan).unwrap();
        let PlanResult::Data(mut iter) = result;
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
        let PlanResult::Data(mut iter) = result;
        let batch_box = iter.next().unwrap().unwrap();

        assert_eq!(batch_box.len(), 3);
        assert!(iter.next().is_none());

        let arrow_data = batch_box.into_any().downcast::<ArrowEngineData>().unwrap();
        let batch: RecordBatch = (*arrow_data).into();

        // Verify schema field names match FILE_META_SCHEMA
        let expected_names: Vec<&str> =
            FILE_META_SCHEMA.fields().map(|f| f.name.as_str()).collect();
        let schema = batch.schema();
        let actual_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(actual_names, expected_names);

        // Verify paths are sorted
        let paths: &StringArray = batch.column(0).as_string();
        let path_values: Vec<&str> = (0..paths.len()).map(|i| paths.value(i)).collect();
        assert!(path_values.windows(2).all(|w| w[0] <= w[1]));

        // Verify all paths contain the expected file names
        assert!(path_values.iter().any(|p| p.contains("aaa.parquet")));
        assert!(path_values.iter().any(|p| p.contains("bbb.parquet")));
        assert!(path_values.iter().any(|p| p.contains("ccc.parquet")));

        // Verify size column has reasonable values (> 0)
        let sizes: &Int64Array = batch.column(2).as_primitive();
        for i in 0..sizes.len() {
            assert!(sizes.value(i) > 0, "file size should be > 0");
        }
    }
}
