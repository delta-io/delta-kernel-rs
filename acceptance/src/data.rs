use std::{path::Path, sync::Arc};

use delta_kernel::arrow::array::{Array, RecordBatch};
use delta_kernel::arrow::compute::{
    concat_batches, filter_record_batch, lexsort_to_indices, take, SortColumn,
};
use delta_kernel::arrow::datatypes::{DataType, Schema};

use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{engine::arrow_data::ArrowEngineData, DeltaResult, Engine, Error};
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

pub fn sort_record_batch(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    // Sort by all columns
    let mut sort_columns = vec![];
    for col in batch.columns() {
        match col.data_type() {
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                // can't sort structs, lists, or maps
            }
            _ => sort_columns.push(SortColumn {
                values: col.clone(),
                options: None,
            }),
        }
    }
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

// Ensure that two schema have the same field names, and dict_is_ordered
// We ignore:
//  - data type: This is checked already in `assert_columns_match`
//  - nullability: parquet marks many things as nullable that we don't in our schema
//  - metadata: because that diverges from the real data to the golden tabled data
fn assert_schema_fields_match(schema: &Schema, golden: &Schema) {
    for (schema_field, golden_field) in schema.fields.iter().zip(golden.fields.iter()) {
        assert!(
            schema_field.name() == golden_field.name(),
            "Field names don't match"
        );
        assert!(
            schema_field.dict_is_ordered() == golden_field.dict_is_ordered(),
            "Field dict_is_ordered doesn't match"
        );
    }
}

// some things are equivalent, but don't show up as equivalent for `==`, so we normalize here
fn normalize_col(col: Arc<dyn Array>) -> Arc<dyn Array> {
    if let DataType::Timestamp(unit, Some(zone)) = col.data_type() {
        if **zone == *"+00:00" {
            let data_type = DataType::Timestamp(*unit, Some("UTC".into()));
            delta_kernel::arrow::compute::cast(&col, &data_type).expect("Could not cast to UTC")
        } else {
            col
        }
    } else {
        col
    }
}

fn assert_columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) {
    for (actual, expected) in actual.iter().zip(expected) {
        let actual = normalize_col(actual.clone());
        let expected = normalize_col(expected.clone());
        // note that array equality includes data_type equality
        // See: https://arrow.apache.org/rust/arrow_data/equal/fn.equal.html
        assert_eq!(
            &actual, &expected,
            "Column data didn't match. Got {actual:?}, expected {expected:?}"
        );
    }
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
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .unwrap()
                .into();
            if schema.is_none() {
                schema = Some(record_batch.schema());
            }
            if let Some(mask) = mask {
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()?;
    let all_data = concat_batches(&schema.unwrap(), batches.iter()).map_err(Error::from)?;
    let all_data = sort_record_batch(all_data)?;
    let golden = read_golden(test_case.root_dir(), None).await?;
    let golden = sort_record_batch(golden)?;

    assert_columns_match(all_data.columns(), golden.columns());
    assert_schema_fields_match(all_data.schema().as_ref(), golden.schema().as_ref());
    assert!(
        all_data.num_rows() == golden.num_rows(),
        "Didn't have same number of rows"
    );

    Ok(())
}
