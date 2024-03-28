use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arrow::{
    compute::{concat_batches, filter_record_batch, lexsort_to_indices, take, SortColumn},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use deltakernel::{client::arrow_data::ArrowEngineData, scan::ScanBuilder, EngineInterface, Table};
use futures::{stream::TryStreamExt, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::{TestCaseInfo, TestResult};

pub async fn read_golden(path: &PathBuf, _version: Option<&str>) -> Option<RecordBatch> {
    let expected_root = path.join("expected").join("latest").join("table_content");
    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_root).unwrap());
    let files = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build().unwrap();
                while let Some(batch) = stream.next().await {
                    batches.push(batch.unwrap());
                }
            }
        }
    }
    let all_data = concat_batches(&schema.unwrap(), batches.iter()).unwrap();
    Some(all_data)
}

pub fn sort_record_batch(batch: RecordBatch) -> RecordBatch {
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
    let indices = lexsort_to_indices(&sort_columns, None).unwrap();
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(&*c, &indices, None).unwrap())
        .collect();
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

static SKIPPED_TESTS: &[&str; 3] = &[
    // Kernel does not support column mapping yet
    "column_mapping",
    // We don't support iceberg yet
    "iceberg_compat_v1",
    // For multi_partitioned_2: The golden table stores the timestamp as an INT96 (which is
    // nanosecond precision), while the spec says we should read partition columns as
    // microseconds. This means the read and golden data don't line up. When this is released in
    // `dat` upstream, we can stop skipping this test
    "multi_partitioned_2",
];

pub async fn assert_scan_data(
    engine_interface: Arc<dyn EngineInterface>,
    test_case: &TestCaseInfo,
) -> TestResult<()> {
    let root_dir = test_case.root_dir();
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            return Ok(());
        }
    }

    let engine_interface = engine_interface.as_ref();
    let table_root = test_case.table_root()?;
    let table = Table::new(table_root);
    let snapshot = table.snapshot(engine_interface, None)?;
    let scan = ScanBuilder::new(snapshot).build();
    let mut schema = None;
    let batches: Vec<RecordBatch> = scan
        .execute(engine_interface)?
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
    let all_data = concat_batches(&schema.unwrap(), batches.iter()).unwrap();
    let all_data = sort_record_batch(all_data);

    let golden = read_golden(test_case.root_dir(), None)
        .await
        .expect("Didn't find golden data");
    let golden = sort_record_batch(golden);
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
