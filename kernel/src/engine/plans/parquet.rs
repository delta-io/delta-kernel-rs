//! A [`ParquetHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use url::Url;

use crate::plans::{IoOperation, Operation, PlanExecutor, QueryPlanBuilder};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileDataReadResultIterator,
    FileMeta, ParquetFooter, ParquetHandler, PredicateRef,
};

/// A [`ParquetHandler`] that delegates to a [`PlanExecutor`].
pub struct PlanBasedParquetHandler {
    executor: Arc<dyn PlanExecutor>,
}

impl PlanBasedParquetHandler {
    pub fn new(plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            executor: plan_executor,
        }
    }
}

impl ParquetHandler for PlanBasedParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let query =
            QueryPlanBuilder::scan_parquet(files.to_vec(), physical_schema, predicate).build()?;
        self.executor
            .execute_op(Operation::QueryPlan(query))?
            .into_data()
    }

    fn write_parquet_file(
        &self,
        _location: Url,
        _data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        Err(Error::unsupported(
            "PlanBasedParquetHandler does not support write_parquet_file yet",
        ))
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

    use super::PlanBasedParquetHandler;
    use crate::arrow::array::{Array, Int64Array, RecordBatch};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::tests::{
        file_meta_for, test_parquet_handler_footer_errors_on_missing_file,
        test_parquet_handler_footer_preserves_field_ids,
        test_parquet_handler_reads_file_with_arrow_schema, test_parquet_handler_reads_footer,
    };
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::schema::SchemaRef;
    use crate::{FileMeta, ParquetHandler as _};

    fn make_handler() -> PlanBasedParquetHandler {
        PlanBasedParquetHandler::new(Arc::new(SyncPlanExecutor::new()))
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

    #[test]
    fn test_parquet_handler_reads_contract() {
        test_parquet_handler_reads_file_with_arrow_schema(&make_handler());
    }

    #[test]
    fn test_read_parquet_footer_contract() {
        test_parquet_handler_reads_footer(&make_handler());
    }

    #[test]
    fn test_read_parquet_footer_missing_file_contract() {
        test_parquet_handler_footer_errors_on_missing_file(&make_handler());
    }

    #[test]
    fn test_read_parquet_footer_preserves_field_ids_contract() {
        test_parquet_handler_footer_preserves_field_ids(&make_handler());
    }
}
