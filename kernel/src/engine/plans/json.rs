//! A [`JsonHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use url::Url;

use crate::engine::arrow_utils;
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, DeltaResultIterator, EngineData, Error, FileDataReadResultIterator, FileMeta,
    FilteredEngineData, JsonHandler, PredicateRef,
};

/// A [`JsonHandler`] that delegates to a [`PlanExecutor`].
///
/// Operations not yet implemented on the plan-execution path delegate to `fallback` when present,
/// and otherwise return an unsupported error.
pub struct PlanBasedJsonHandler {
    executor: Arc<dyn PlanExecutor>,
    fallback: Option<Arc<dyn JsonHandler>>,
}

impl PlanBasedJsonHandler {
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
        fallback: Arc<dyn JsonHandler>,
    ) -> Self {
        Self {
            executor: plan_executor,
            fallback: Some(fallback),
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
        path: &Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        match &self.fallback {
            Some(fallback) => fallback.write_json_file(path, data, overwrite),
            None => Err(Error::unsupported(
                "PlanBasedJsonHandler does not support write_json_file yet",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO(#2618): Refactor and share a test suite with sync engine tests.

    use std::io::Write as _;
    use std::sync::Arc;

    use rstest::rstest;
    use tempfile::{tempdir, NamedTempFile};
    use url::Url;

    use super::PlanBasedJsonHandler;
    use crate::arrow::array::{Array, Int32Array, RecordBatch, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::plans::parquet::PlanBasedParquetHandler;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::{
        DeltaResult, Engine as _, EngineData, Error, FileDataReadResultIterator, FileMeta,
        JsonHandler as _, ParquetHandler as _,
    };

    fn make_handler() -> PlanBasedJsonHandler {
        PlanBasedJsonHandler::new(Arc::new(SyncPlanExecutor::new()))
    }

    fn make_handler_with_fallback() -> PlanBasedJsonHandler {
        PlanBasedJsonHandler::with_fallback(
            Arc::new(SyncPlanExecutor::new()),
            SyncEngine::new().json_handler(),
        )
    }

    fn single_column_data(values: Vec<&str>) -> Box<dyn EngineData> {
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(StringArray::from(values)) as Arc<dyn Array>,
        )])
        .unwrap();
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_write_json_file_without_fallback_is_unsupported() {
        let dir = tempdir().unwrap();
        let url = Url::from_file_path(dir.path().join("out.json")).unwrap();
        let filtered = Ok(FilteredEngineData::with_all_rows_selected(
            single_column_data(vec!["a"]),
        ));
        let err = make_handler()
            .write_json_file(&url, Box::new(std::iter::once(filtered)), false)
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn test_write_json_file_delegates_to_fallback() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("out.json");
        let url = Url::from_file_path(&path).unwrap();
        let filtered = Ok(FilteredEngineData::with_all_rows_selected(
            single_column_data(vec!["a", "b"]),
        ));
        make_handler_with_fallback()
            .write_json_file(&url, Box::new(std::iter::once(filtered)), false)
            .unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"x\":\"a\"}\n{\"x\":\"b\"}\n");
    }

    fn make_parquet_handler() -> PlanBasedParquetHandler {
        PlanBasedParquetHandler::new(Arc::new(SyncPlanExecutor::new()))
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

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::INTEGER)]).unwrap())
    }

    /// No files -> an absent plan -> a zero-row result (no rows, no error), for either handler.
    #[rstest]
    #[case(make_handler().read_json_files(&[], test_schema(), None))]
    #[case(make_parquet_handler().read_parquet_files(&[], test_schema(), None))]
    fn empty_input_yields_no_rows(#[case] res: DeltaResult<FileDataReadResultIterator>) {
        let rows: usize = res.unwrap().map(|batch| batch.unwrap().len()).sum();
        assert_eq!(rows, 0);
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
