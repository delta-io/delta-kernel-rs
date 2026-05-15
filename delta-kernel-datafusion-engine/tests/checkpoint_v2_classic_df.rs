//! Classic V2 checkpoint parquet via DF SM driving (`checkpoint_parquet_write` phase).

use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use serde_json::json;
use tempfile::tempdir;
use url::Url;

fn write_commit_json(log: &std::path::Path, version: u64, lines: &[serde_json::Value]) {
    let mut buf = String::new();
    for v in lines {
        buf.push_str(&serde_json::to_string(v).expect("json"));
        buf.push('\n');
    }
    let name = format!("{:020}.json", version);
    std::fs::write(log.join(name), buf).expect("commit write");
}

#[tokio::test]
async fn checkpoint_v2_classic_parquet_df_sm_happy_path() {
    let dir = tempdir().expect("tempdir");
    let log = dir.path().join("_delta_log");
    std::fs::create_dir_all(&log).expect("mkdir log");

    write_commit_json(
        &log,
        0,
        &[
            json!({
                "add": {
                    "path": "fake_path_2",
                    "partitionValues": {},
                    "size": 50_i64,
                    "modificationTime": 1_i64,
                    "dataChange": true,
                    "stats": r#"{"numRecords":50,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#
                }
            }),
            json!({
                "remove": {
                    "path": "fake_path_1",
                    "deletionTimestamp": 9223372036854775807_i64,
                    "dataChange": true,
                }
            }),
        ],
    );

    write_commit_json(
        &log,
        1,
        &[
            json!({
                "metaData": {
                    "id": "388876aa-094f-49fb-aabd-be833707970b",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0_i64,
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 3_i32,
                    "minWriterVersion": 7_i32,
                    "readerFeatures": ["v2Checkpoint"],
                    "writerFeatures": ["v2Checkpoint"],
                }
            }),
        ],
    );

    let table_root = Url::from_directory_path(dir.path()).expect("table url");
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Arc::new(
        Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .expect("snapshot"),
    );
    let writer =
        Snapshot::create_checkpoint_writer(Arc::clone(&snapshot)).expect("checkpoint writer");

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    ex.checkpoint_write_classic_parquet_and_finalize(writer)
        .await
        .expect("checkpoint via DF SM");

    let cp_path = log.join("00000000000000000001.checkpoint.parquet");
    assert!(
        cp_path.exists(),
        "expected classic parquet checkpoint at {}",
        cp_path.display()
    );

    let lc_path = log.join("_last_checkpoint");
    assert!(lc_path.exists());
    let lc: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&lc_path).expect("open hint")).expect("parse");
    assert_eq!(lc["version"], 1);
    assert_eq!(lc["numOfAddFiles"], 1);

    let file = std::fs::File::open(&cp_path).expect("open parquet");
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader");
    let arrow_schema = builder.schema();
    assert!(
        arrow_schema
            .fields()
            .iter()
            .any(|f| f.name() == "checkpointMetadata"),
        "v2 checkpoint should materialize checkpointMetadata column"
    );
}
