use std::{collections::HashMap, path::Path, sync::Arc};

use arrow_array::RecordBatch;
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::DataType;
use arrow_select::{concat::concat_batches, filter::filter_record_batch, take::take};

use delta_kernel::{
    engine::arrow_data::ArrowEngineData, scan::ScanBuilder, DeltaResult, Engine, Error, Table,
};
use futures::{stream::TryStreamExt, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::{TestCaseInfo, TestResult};

pub async fn read_golden(path: &Path, _version: Option<&str>) -> DeltaResult<Option<RecordBatch>> {
    let expected_root = path.join("expected").join("latest").join("table_content");
    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_root)?);
    let files = store.list(None).try_collect::<Vec<_>>().await?;
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta);
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
    Ok(Some(all_data))
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

static SKIPPED_TESTS: &[&str; 3] = &[
    // Kernel does not support column mapping yet
    "column_mapping",
    // iceberg compat requires column mapping
    "iceberg_compat_v1",
    // For multi_partitioned_2: The golden table stores the timestamp as an INT96 (which is
    // nanosecond precision), while the spec says we should read partition columns as
    // microseconds. This means the read and golden data don't line up. When this is released in
    // `dat` upstream, we can stop skipping this test
    "multi_partitioned_2",
];

pub async fn assert_scan_data(engine: Arc<dyn Engine>, test_case: &TestCaseInfo) -> TestResult<()> {
    let root_dir = test_case.root_dir();
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            return Ok(());
        }
    }

    let engine = engine.as_ref();
    let table_root = test_case.table_root()?;
    let table = Table::new(table_root);
    let snapshot = table.snapshot(engine, None)?;
    let scan = ScanBuilder::new(snapshot).build();
    let mut schema = None;
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .into_iter()
        .map(|res| {
            let data = res.raw_data.unwrap();
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .unwrap()
                .into();
            if schema.is_none() {
                schema = Some(record_batch.schema());
            }
            if let Some(mask) = res.mask {
                filter_record_batch(&record_batch, &mask.into()).unwrap()
            } else {
                record_batch
            }
        })
        .collect();
    let all_data = concat_batches(&schema.unwrap(), batches.iter()).map_err(Error::from)?;
    let all_data = sort_record_batch(all_data)?;

    let golden = read_golden(test_case.root_dir(), None)
        .await?
        .expect("Didn't find golden data");
    let golden = sort_record_batch(golden)?;
    let golden_schema = golden
        .schema()
        .as_ref()
        .clone()
        .with_metadata(HashMap::new());

    assert!(
        all_data.columns() == golden.columns(),
        "Read data does not equal golden data"
    );
    assert!(
        all_data.schema() == Arc::new(golden_schema),
        "Schemas not equal"
    );
    assert!(
        all_data.num_rows() == golden.num_rows(),
        "Didn't have same number of rows"
    );

    Ok(())
}
