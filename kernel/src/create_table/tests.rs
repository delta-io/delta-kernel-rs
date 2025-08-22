use std::sync::Arc;

use serde_json::json;
use url::Url;

use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
use crate::object_store::memory::InMemory;
use crate::object_store::{path::Path, ObjectStore};
use crate::schema::{DataType, StructField, StructType};

use super::CreateTableBuilder;

#[tokio::test]
async fn create_table_simple() {
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
    let table_root = Url::parse("memory:///test_table").unwrap();

    let schema = StructType::new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("name", DataType::STRING, true),
    ]);

    CreateTableBuilder::new(table_root.clone(), schema)
        .with_table_name("test_table")
        .with_description("A test table")
        .with_timestamp(1234567890)
        .create(&engine)
        .unwrap();

    let commit_path = Path::from("_delta_log/00000000000000000000.json");
    let data = store.get(&commit_path).await.unwrap();
    let content = String::from_utf8(data.bytes().await.unwrap().to_vec()).unwrap();

    let mut lines: Vec<serde_json::Value> = content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    lines.sort_by_key(|v| {
        if v.get("commitInfo").is_some() {
            0
        } else if v.get("protocol").is_some() {
            1
        } else if v.get("metaData").is_some() {
            2
        } else {
            3
        }
    });

    let expected_commit_info = json!({
        "commitInfo": {
            "timestamp": 1234567890,
            "operation": "CREATE",
            "kernelVersion": "v0.13.0",
            "operationParameters": {}
        }
    });

    let expected_protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": [
                "columnMapping",
                "deletionVectors",
                "timestampNtz",
                "typeWidening",
                "typeWidening-preview",
                "vacuumProtocolCheck",
                "v2Checkpoint",
                "variantType",
                "variantType-preview",
                "variantShredding-preview"
            ],
            "writerFeatures": [
                "appendOnly",
                "deletionVectors",
                "invariants",
                "timestampNtz",
                "variantType",
                "variantType-preview",
                "variantShredding-preview"
            ]
        }
    });

    let expected_metadata = json!({
        "metaData": {
            "id": lines[2]["metaData"]["id"].clone(),
            "name": "test_table",
            "description": "A test table",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1234567890
        }
    });

    assert_eq!(lines[0], expected_commit_info);
    assert_eq!(lines[1], expected_protocol);
    assert_eq!(lines[2], expected_metadata);
    assert_eq!(lines.len(), 3);
}
