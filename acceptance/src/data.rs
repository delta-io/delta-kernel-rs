use std::{path::Path, sync::Arc};

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::Schema;

use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error};
use futures::{stream::TryStreamExt, StreamExt};
use itertools::Itertools;
use object_store::{local::LocalFileSystem, ObjectStore};

use crate::{TestCaseInfo, TestResult};

pub async fn read_golden(path: &Path, _version: Option<&str>) -> DeltaResult<RecordBatch> {
    let expected_root = path.join("expected").join("latest").join("table_content");
    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_root)?);
    let files: Vec<_> = store.list(None).try_collect().await?;
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta.location);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
    }
    let all_data = concat_batches(&schema.unwrap(), &batches)?;
    Ok(all_data)
}

pub fn assert_data_matches(
    result: Vec<RecordBatch>,
    result_schema: &delta_kernel::schema::Schema,
    expected: RecordBatch,
    expected_rows: Option<usize>,
) -> DeltaResult<()> {
    use delta_kernel::arrow::util::pretty::pretty_format_batches;

    let arrow_schema = Schema::try_from_kernel(result_schema)?;
    let arrow_schema = std::sync::Arc::new(arrow_schema);
    let all_data = concat_batches(&arrow_schema, result.iter())?;

    // Format both batches as strings for order-independent comparison
    let actual_str = pretty_format_batches(std::slice::from_ref(&all_data))
        .map_err(|e| Error::generic(format!("Failed to format actual: {}", e)))?
        .to_string();
    let expected_str = pretty_format_batches(std::slice::from_ref(&expected))
        .map_err(|e| Error::generic(format!("Failed to format expected: {}", e)))?
        .to_string();

    let mut actual_lines: Vec<&str> = actual_str.trim().lines().collect();
    let mut expected_lines: Vec<&str> = expected_str.trim().lines().collect();

    // Sort data lines (skip header at indices 0-1 and footer at last index)
    let num_actual = actual_lines.len();
    let num_expected = expected_lines.len();
    if num_actual > 3 {
        actual_lines[2..num_actual - 1].sort_unstable();
    }
    if num_expected > 3 {
        expected_lines[2..num_expected - 1].sort_unstable();
    }

    // Compare sorted lines
    if actual_lines != expected_lines {
        return Err(Error::generic(format!(
            "Data mismatch:\nExpected:\n{}\nActual:\n{}",
            expected_lines.join("\n"),
            actual_lines.join("\n")
        )));
    }

    // Validate row counts
    if all_data.num_rows() != expected.num_rows() {
        return Err(Error::generic_err("Didn't have same number of rows"));
    }
    if let Some(expected_row_count) = expected_rows {
        if all_data.num_rows() != expected_row_count {
            return Err(Error::generic("Didn't have expected number of rows"));
        }
    }

    Ok(())
}

pub async fn assert_scan_metadata(
    engine: Arc<dyn Engine>,
    test_case: &TestCaseInfo,
) -> TestResult<()> {
    let table_root = test_case.table_root()?;
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            Ok(record_batch)
        })
        .try_collect()?;
    let kernel_schema = scan.logical_schema();
    let golden = read_golden(test_case.root_dir(), None).await?;
    assert_data_matches(batches, kernel_schema.as_ref(), golden, None)?;

    Ok(())
}
