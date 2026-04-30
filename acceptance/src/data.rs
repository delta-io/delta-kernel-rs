use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::ObjectStore;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error};
use futures::stream::TryStreamExt;
use futures::StreamExt;
use itertools::Itertools;

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

fn assert_schema_fields_match(schema: &Schema, golden: &Schema) -> DeltaResult<()> {
    let schema_stripped = strip_metadata(schema);
    let golden_stripped = strip_metadata(golden);
    if schema_stripped.fields() != golden_stripped.fields() {
        return Err(Error::generic(format!(
            "Schema mismatch:\nActual: {:?}\nExpected: {:?}",
            schema_stripped.fields(),
            golden_stripped.fields()
        )));
    }
    Ok(())
}

/// Recursively strip metadata from schema and all nested fields.
fn strip_metadata(schema: &Schema) -> Schema {
    fn strip_field(field: &Field) -> Field {
        Field::new(
            field.name(),
            strip_type(field.data_type()),
            field.is_nullable(),
        )
    }

    fn strip_type(dt: &DataType) -> DataType {
        match dt {
            DataType::Struct(fields) => DataType::Struct(Fields::from(
                fields.iter().map(|f| strip_field(f)).collect_vec(),
            )),
            DataType::List(f) => DataType::List(Arc::new(strip_field(f))),
            DataType::LargeList(f) => DataType::LargeList(Arc::new(strip_field(f))),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(Arc::new(strip_field(f)), *n),
            DataType::Map(f, sorted) => DataType::Map(Arc::new(strip_field(f)), *sorted),
            other => other.clone(),
        }
    }
    Schema::new(schema.fields().iter().map(|f| strip_field(f)).collect_vec())
}

pub fn assert_data_matches(
    result: Vec<RecordBatch>,
    result_schema: &SchemaRef,
    expected: RecordBatch,
) -> DeltaResult<()> {
    let all_data = concat_batches(result_schema, result.iter())?;

    // Validate schemas match
    assert_schema_fields_match(all_data.schema().as_ref(), expected.schema().as_ref())?;

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

    Ok(())
}

pub async fn assert_scan_metadata(
    engine: Arc<dyn Engine>,
    test_case: &TestCaseInfo,
) -> TestResult<()> {
    let table_root = test_case.table_root()?;
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let mut schema = None;
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            if schema.is_none() {
                schema = Some(record_batch.schema());
            }
            Ok(record_batch)
        })
        .try_collect()?;
    let golden = read_golden(test_case.root_dir(), None).await?;
    assert_data_matches(batches, &schema.unwrap(), golden)?;

    Ok(())
}
