//! A [`ParquetHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use url::Url;

use crate::plans::{IoOperation, Operation, PlanBuilder, PlanExecutor};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileDataReadResultIterator,
    FileMeta, ParquetFooter, ParquetHandler, PredicateRef,
};

/// A [`ParquetHandler`] that delegates to a [`PlanExecutor`].
///
/// Operations not yet implemented on the plan-execution path delegate to `fallback` when present,
/// and otherwise return an unsupported error.
pub struct PlanBasedParquetHandler {
    executor: Arc<dyn PlanExecutor>,
    fallback: Option<Arc<dyn ParquetHandler>>,
}

impl PlanBasedParquetHandler {
    /// Construct a handler with no fallback.
    pub fn new(plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            executor: plan_executor,
            fallback: None,
        }
    }

    /// Construct a handler that delegates not-yet-implemented operations to `fallback`.
    pub fn with_fallback(
        plan_executor: Arc<dyn PlanExecutor>,
        fallback: Arc<dyn ParquetHandler>,
    ) -> Self {
        Self {
            executor: plan_executor,
            fallback: Some(fallback),
        }
    }
}

impl ParquetHandler for PlanBasedParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // TODO: `_predicate` is dropped. Re-apply it as a Filter node over the scan; the
        // single-node executor can then match the filter -> scan shape.
        let query = PlanBuilder::scan_parquet(files.to_vec(), &[], physical_schema)?.build()?;
        self.executor
            .execute_op(Operation::QueryPlan(query))?
            .into_data()
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        match &self.fallback {
            Some(fallback) => fallback.write_parquet_file(location, data),
            None => Err(Error::unsupported(
                "PlanBasedParquetHandler does not support write_parquet_file yet",
            )),
        }
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        let op = IoOperation::parquet_footer(file.clone());
        self.executor
            .execute_op(Operation::IoOperation(op))?
            .into_parquet_footer()
    }
}

#[cfg(test)]
mod tests {
    // TODO(#2618): Refactor and share a test suite with sync engine tests.

    use std::path::Path;
    use std::sync::Arc;

    use tempfile::tempdir;
    use url::Url;

    use super::PlanBasedParquetHandler;
    use crate::arrow::array::{Array, Int64Array, RecordBatch};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::sync::SyncEngine;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::{Engine as _, EngineData, Error, FileMeta, ParquetHandler as _};

    fn make_handler() -> PlanBasedParquetHandler {
        PlanBasedParquetHandler::new(Arc::new(SyncPlanExecutor::new()))
    }

    fn make_handler_with_fallback() -> PlanBasedParquetHandler {
        PlanBasedParquetHandler::with_fallback(
            Arc::new(SyncPlanExecutor::new()),
            SyncEngine::new().parquet_handler(),
        )
    }

    fn single_column_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        )])
        .unwrap()
    }

    #[test]
    fn test_write_parquet_file_without_fallback_is_unsupported() {
        let temp_dir = tempdir().unwrap();
        let url = Url::from_file_path(temp_dir.path().join("out.parquet")).unwrap();
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(single_column_batch()));
        let err = make_handler()
            .write_parquet_file(url, Box::new(std::iter::once(Ok(data))))
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn test_write_parquet_file_delegates_to_fallback() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("out.parquet");
        let url = Url::from_file_path(&path).unwrap();
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(single_column_batch()));
        make_handler_with_fallback()
            .write_parquet_file(url.clone(), Box::new(std::iter::once(Ok(data))))
            .unwrap();

        // Read it back to confirm the fallback actually wrote the rows.
        let schema: SchemaRef = Arc::new(
            StructType::try_new([StructField::nullable("value", DataType::LONG)]).unwrap(),
        );
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: std::fs::metadata(&path).unwrap().len(),
        };
        let rows: usize = make_handler_with_fallback()
            .read_parquet_files(&[file_meta], schema, None)
            .unwrap()
            .map(|batch| batch.unwrap().len())
            .sum();
        assert_eq!(rows, 3);
    }

    fn file_meta_for(path: &Path) -> FileMeta {
        let url = Url::from_file_path(path).unwrap();
        let size = std::fs::metadata(path).unwrap().len();
        FileMeta {
            location: url,
            last_modified: 0,
            size,
        }
    }

    fn make_test_parquet_file(path: &Path, batch: &RecordBatch) -> (FileMeta, SchemaRef) {
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let file_meta = file_meta_for(path);
        let schema = Arc::new(batch.schema().as_ref().try_into_kernel().unwrap());
        (file_meta, schema)
    }

    #[test]
    fn test_read_parquet_files() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("data.parquet");
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn Array>,
        )])
        .unwrap();
        let (file_meta, schema) = make_test_parquet_file(&file_path, &batch);

        let batches: Vec<RecordBatch> = make_handler()
            .read_parquet_files(&[file_meta], schema, None)
            .unwrap()
            .map(|r| {
                ArrowEngineData::try_from_engine_data(r.unwrap())
                    .unwrap()
                    .into()
            })
            .collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20, 30]
        );
    }

    /// No files -> an absent plan -> a zero-row result (no rows, no error).
    #[test]
    fn test_read_parquet_files_empty_is_empty() {
        let schema: SchemaRef = Arc::new(
            StructType::try_new([StructField::nullable("value", DataType::LONG)]).unwrap(),
        );
        let rows: usize = make_handler()
            .read_parquet_files(&[], schema, None)
            .unwrap()
            .map(|batch| batch.unwrap().len())
            .sum();
        assert_eq!(rows, 0);
    }

    // TODO(#2618): Restore once `PlanBasedParquetHandler` moves to delta_kernel_default_engine
    // and can use the test_utils::engine_contract helpers without the kernel-cfg-test cycle issue.
    //
    // #[test]
    // fn test_parquet_handler_reads_contract() {
    //     test_parquet_handler_reads_file_with_arrow_schema(&make_handler());
    // }
    //
    // #[test]
    // fn test_read_parquet_footer_contract() {
    //     test_parquet_handler_reads_footer(&make_handler());
    // }
    //
    // #[test]
    // fn test_read_parquet_footer_missing_file_contract() {
    //     test_parquet_handler_footer_errors_on_missing_file(&make_handler());
    // }
    //
    // #[test]
    // fn test_read_parquet_footer_preserves_field_ids_contract() {
    //     test_parquet_handler_footer_preserves_field_ids(&make_handler());
    // }
}
