//! Cross-engine contract tests: verifies that handler implementations exhibit consistent behavior
//! for [`JsonHandler`] and [`ParquetHandler`].
//!
//! Contract tests (things any handler implementation must satisfy) call into [`super::tests`].
//! Internal implementation tests (Arrow-specific behavior) are defined as local helpers here.

use std::fs::File;
use std::sync::Arc;

use tempfile::tempdir;
use url::Url;

use crate::arrow::array::{Array, Int64Array, RecordBatch};
use crate::engine::arrow_conversion::TryIntoKernel as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::json::SyncJsonHandler;
use crate::engine::sync::SyncParquetHandler;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::arrow::ARROW_SCHEMA_META_KEY;
use crate::{EngineData, ParquetHandler};

#[test]
fn test_reads_footer() {
    super::tests::test_parquet_handler_reads_footer(&SyncParquetHandler::new());
}

#[test]
fn test_footer_errors_on_missing_file() {
    super::tests::test_parquet_handler_footer_errors_on_missing_file(&SyncParquetHandler::new());
}

#[test]
fn test_footer_preserves_field_ids() {
    super::tests::test_parquet_handler_footer_preserves_field_ids(&SyncParquetHandler::new());
}

#[test]
fn test_write_always_overwrites() {
    super::tests::test_parquet_handler_write_always_overwrites(&SyncParquetHandler::new());
}

// Both kernel engines configure their parquet readers and writers to skip the Arrow IPC schema
// (ARROW:schema) in file metadata. The following tests verify this shared behavior.

fn assert_no_arrow_schema(handler: &dyn ParquetHandler) {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("no_arrow_schema.parquet");
    let url = Url::from_file_path(&file_path).unwrap();

    let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
        RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
        )])
        .unwrap(),
    ));
    handler
        .write_parquet_file(url, Box::new(std::iter::once(Ok(data))))
        .unwrap();

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&file_path).unwrap()).unwrap();
    let kv = builder.metadata().file_metadata().key_value_metadata();
    let has = kv
        .map(|kv| kv.iter().any(|e| e.key == ARROW_SCHEMA_META_KEY))
        .unwrap_or(false);
    assert!(
        !has,
        "Parquet file should not contain embedded Arrow schema metadata"
    );
}

fn assert_reads_file_with_arrow_schema_metadata(handler: &dyn ParquetHandler) {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("with_arrow_schema.parquet");

    let batch = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn Array>,
    )])
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&file_path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let file_meta = super::tests::file_meta_for(&file_path);
    let schema = Arc::new(batch.schema().as_ref().try_into_kernel().unwrap());
    let batches: Vec<RecordBatch> = handler
        .read_parquet_files(&[file_meta], schema, None)
        .unwrap()
        .map(|r| {
            ArrowEngineData::try_from_engine_data(r.unwrap())
                .unwrap()
                .into()
        })
        .collect();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[10, 20, 30]
    );
}

#[test]
fn test_write_file_omits_arrow_schema() {
    assert_no_arrow_schema(&SyncParquetHandler::new());
}

#[test]
fn test_reads_file_with_arrow_schema_metadata() {
    assert_reads_file_with_arrow_schema_metadata(&SyncParquetHandler::new());
}

#[test]
fn test_json_file_path_contract() {
    super::tests::test_json_handler_file_path_contract(&SyncJsonHandler::new());
}
