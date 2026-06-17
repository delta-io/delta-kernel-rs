//! A [`JsonHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use url::Url;

use crate::engine::arrow_utils;
use crate::plans::ir::nodes::ScanFile;
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
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
        let scan_files = files.iter().cloned().map(ScanFile::from).collect();
        let mut builder = PlanBuilder::new();
        builder.scan_json(scan_files, vec![], physical_schema)?;
        let Some(plan) = builder.build()? else {
            return Ok(Box::new(std::iter::empty()));
        };
        self.executor
            .execute_op(Operation::QueryPlan(plan))?
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

    use std::io::Write as _;
    use std::sync::Arc;

    use tempfile::NamedTempFile;
    use url::Url;

    use super::PlanBasedJsonHandler;
    use crate::arrow::array::{Int32Array, RecordBatch, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::schema::{DataType, StructField, StructType};
    use crate::{EngineData, FileMeta, JsonHandler as _};

    fn make_handler() -> PlanBasedJsonHandler {
        PlanBasedJsonHandler::new(Arc::new(SyncPlanExecutor::new()))
    }

    fn temp_json_file(lines: &[&str]) -> (NamedTempFile, FileMeta) {
        let mut temp_file = NamedTempFile::new().unwrap();
        for line in lines {
            writeln!(temp_file, "{line}").unwrap();
        }
        let path = temp_file.path();
        let file_url = Url::from_file_path(path).unwrap();
        let size = std::fs::metadata(path).unwrap().len();
        let file_meta = FileMeta {
            location: file_url,
            last_modified: 0,
            size,
        };
        (temp_file, file_meta)
    }

    #[test]
    fn test_read_json_files() {
        let (_temp, file_meta) = temp_json_file(&[r#"{"x": 1}"#, r#"{"x": 2}"#, r#"{"x": 3}"#]);
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
    fn test_parse_json() {
        let json_strings = StringArray::from(vec![r#"{"x": 1}"#, r#"{"x": 2}"#, r#"{"x": 3}"#]);
        let input_batch = RecordBatch::try_from_iter(vec![(
            "json",
            Arc::new(json_strings) as Arc<dyn crate::arrow::array::Array>,
        )])
        .unwrap();
        let input: Box<dyn EngineData> = Box::new(ArrowEngineData::new(input_batch));

        let output_schema =
            Arc::new(StructType::try_new([StructField::not_null("x", DataType::INTEGER)]).unwrap());

        let parsed = make_handler().parse_json(input, output_schema).unwrap();
        let record_batch: RecordBatch = ArrowEngineData::try_from_engine_data(parsed)
            .unwrap()
            .into();
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
    }

    // TODO(#2618): Restore once `PlanBasedJsonHandler` moves to delta_kernel_default_engine and
    // can use the test_utils::engine_contract helpers without the kernel-cfg-test cycle issue.
    //
    // #[test]
    // fn test_file_path_contract() {
    //     test_json_handler_file_path_contract(&make_handler());
    // }
}
