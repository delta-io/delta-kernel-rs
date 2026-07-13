//! A [`JsonHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use delta_kernel::engine::arrow_utils;
use delta_kernel::plans::{Operation, PlanBuilder, PlanExecutor};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, DeltaResultIterator, EngineData, Error, FileDataReadResultIterator, FileMeta,
    FilteredEngineData, JsonHandler, PredicateRef,
};
use url::Url;

/// A [`JsonHandler`] that delegates to a [`PlanExecutor`].
pub struct PlanBasedJsonHandler {
    executor: Arc<dyn PlanExecutor>,
}

impl PlanBasedJsonHandler {
    pub fn new(plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            executor: plan_executor,
        }
    }
}

impl JsonHandler for PlanBasedJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_utils::parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // TODO: `_predicate` is dropped. Re-apply it as a Filter node over the scan; the
        // single-node executor can then match the filter -> scan shape.
        let query = PlanBuilder::scan_json(files.to_vec(), &[], physical_schema)?.build()?;
        self.executor
            .execute_op(Operation::QueryPlan(query))?
            .into_data()
    }

    fn write_json_file(
        &self,
        _path: &Url,
        _data: DeltaResultIterator<'_, FilteredEngineData>,
        _overwrite: bool,
    ) -> DeltaResult<()> {
        Err(Error::unsupported(
            "PlanBasedJsonHandler does not support write_json_file yet",
        ))
    }
}
