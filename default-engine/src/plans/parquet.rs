//! A [`ParquetHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use delta_kernel::plans::{IoOperation, Operation, PlanBuilder, PlanExecutor};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileDataReadResultIterator,
    FileMeta, ParquetFooter, ParquetHandler, PredicateRef,
};
use url::Url;

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
