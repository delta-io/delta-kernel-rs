//! A [`JsonHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use url::Url;

use crate::plans::{Operation, PlanExecutor, QueryPlanBuilder};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, DeltaResultIterator, EngineData, Error, FileDataReadResultIterator, FileMeta,
    FilteredEngineData, JsonHandler, PredicateRef,
};

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
        _json_strings: Box<dyn EngineData>,
        _output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::unsupported(
            "PlanBasedJsonHandler does not support parse_json yet",
        ))
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let query =
            QueryPlanBuilder::scan_json(files.to_vec(), physical_schema, predicate).build()?;
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

#[cfg(test)]
mod tests {
    // TODO(#2618): Refactor and share a test suite with sync engine tests.

    use std::sync::Arc;

    use super::PlanBasedJsonHandler;
    use crate::arrow::array::{Int32Array, RecordBatch};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::tests::{make_temp_json_file, test_json_handler_file_path_contract};
    use crate::schema::{DataType, StructField, StructType};
    use crate::JsonHandler as _;

    fn make_handler() -> PlanBasedJsonHandler {
        PlanBasedJsonHandler::new(Arc::new(SyncPlanExecutor::new()))
    }

    #[test]
    fn test_read_json_files() {
        let (_temp, file_meta) =
            make_temp_json_file(&[r#"{"x": 1}"#, r#"{"x": 2}"#, r#"{"x": 3}"#]);
        let schema =
            Arc::new(StructType::try_new([StructField::not_null("x", DataType::INTEGER)]).unwrap());

        let mut iter = make_handler()
            .read_json_files(&[file_meta], schema, None)
            .unwrap();

        let batch = iter.next().expect("expected at least one batch").unwrap();
        let record_batch: RecordBatch =
            ArrowEngineData::try_from_engine_data(batch).unwrap().into();
        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(record_batch.num_columns(), 1);
        assert_eq!(
            record_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
        assert!(iter.next().is_none(), "expected exactly one batch");
    }

    #[test]
    fn test_file_path_contract() {
        test_json_handler_file_path_contract(&make_handler());
    }
}
