use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::{Format, Metadata, Protocol};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::FileMeta;
use delta_kernel::Snapshot;
use delta_kernel::{ParsedLogPath, Version};

use url::Url;

mod test_utils {
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::object_store::{ObjectStore, PutPayload};
    use delta_kernel::parquet;

    use serde_json::json;

    pub struct AddFile {
        pub path: String,
        pub data: RecordBatch,
        pub size: i64,
        pub modification_time: i64,
        pub data_change: bool,
    }

    impl AddFile {
        pub fn new(path: String, data: RecordBatch) -> Self {
            Self {
                path,
                size: 100, // dummy size for testing
                data,
                modification_time: 1234567890,
                data_change: true,
            }
        }
    }

    pub async fn put_commit(
        object_store: &dyn ObjectStore,
        version: u64,
        add_files: Vec<AddFile>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Write the commit JSON file
        let commit_path = format!("test_table/_delta_log/{:020}.json", version);

        let mut commit_lines = vec![];

        // Add pm action for version 0
        if version == 0 {
            commit_lines.push(
                json!({
                    "protocol": {
                        "minReaderVersion": 3,
                        "minWriterVersion": 7,
                        "readerFeatures": ["v2Checkpoint"],
                        "writerFeatures": ["v2Checkpoint"]
                    }
                })
                .to_string(),
            );
            commit_lines.push(json!({
                "metaData": {
                    "id": "1234",
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "createdTime": 1234567890,
                    "configuration": {}
                }
            }).to_string());
        }

        // Add commit info
        commit_lines.push(
            json!({
                "commitInfo": {
                    "timestamp": 1234567890,
                    "operation": "WRITE",
                    "operationParameters": {},
                    "readVersion": version.saturating_sub(1),
                    "isolationLevel": "WriteSerializable",
                    "isBlindAppend": true,
                    "operationMetrics": {},
                    "engineInfo": "test",
                    "userMetadata": "{}"
                }
            })
            .to_string(),
        );

        // Add file actions
        for add_file in &add_files {
            let file_path = format!("memory:///test_table/{}", add_file.path);
            commit_lines.push(
                json!({
                    "add": {
                        "path": file_path,
                        "partitionValues": {},
                        "size": add_file.size,
                        "modificationTime": add_file.modification_time,
                        "dataChange": add_file.data_change,
                        "stats": "{\"numRecords\": 1}"
                    }
                })
                .to_string(),
            );
        }

        let commit_content = commit_lines.join("\n");
        let path = delta_kernel::object_store::path::Path::from(commit_path);
        println!("putting commit at path: {}", path);
        object_store
            .put(&path, PutPayload::from(commit_content))
            .await?;

        // Write the parquet files
        for add_file in add_files {
            let file_path = delta_kernel::object_store::path::Path::from(format!(
                "test_table/{}",
                add_file.path
            ));

            // Convert RecordBatch to parquet bytes
            let mut buf = Vec::new();
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                &mut buf,
                add_file.data.schema(),
                Some(props),
            )?;
            writer.write(&add_file.data)?;
            writer.close()?;

            println!("putting data at path: {}", file_path);
            object_store.put(&file_path, PutPayload::from(buf)).await?;
        }

        Ok(())
    }
}

fn mock_catalog_get_table(
) -> Result<(Protocol, Metadata, Version, Vec<ParsedLogPath>), Box<dyn std::error::Error>> {
    let schema = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
    let protocol = Protocol::try_new(3, 7, Some(["v2Checkpoint"]), Some(["v2Checkpoint"]))?;
    let metadata = Metadata::new(
        "1234".to_string(),
        None,
        None,
        Format::default(),
        schema.to_string(),
        vec![],
        Some(1234567890),
        HashMap::new(),
    );
    let log_files = vec![
        ParsedLogPath::try_from(FileMeta::new(
            Url::parse("memory:///test_table/_delta_log/00000000000000000000.json")?,
            1,          // non-zero size
            1234567890, // some timestamp
        ))?
        .unwrap(),
        ParsedLogPath::try_from(FileMeta::new(
            Url::parse("memory:///test_table/_delta_log/00000000000000000001.json")?,
            1,          // non-zero size
            1234567891, // some timestamp
        ))?
        .unwrap(),
    ];
    let version = 1;
    Ok((protocol, metadata, version, log_files))
}

fn setup_test() -> Result<Arc<DefaultEngine<TokioBackgroundExecutor>>, Box<dyn std::error::Error>> {
    // note with our mock client we do in-mem store
    let object_store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngine::new(
        object_store.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    // Create test data using our helper functions
    let runtime = tokio::runtime::Runtime::new()?;

    // Create arrow data for the parquet files
    let arrow_schema = delta_kernel::arrow::datatypes::Schema::new(vec![
        delta_kernel::arrow::datatypes::Field::new(
            "value",
            delta_kernel::arrow::datatypes::DataType::Int32,
            true,
        ),
    ]);
    let value_array = delta_kernel::arrow::array::Int32Array::from(vec![Some(1)]);
    let batch = delta_kernel::arrow::record_batch::RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(value_array)],
    )?;

    // Write commit 0 (protocol and metadata only)
    runtime.block_on(test_utils::put_commit(object_store.as_ref(), 0, vec![]))?;

    // Write commit 1 with an add file
    runtime.block_on(test_utils::put_commit(
        object_store.as_ref(),
        1,
        vec![test_utils::AddFile::new("1.parquet".to_string(), batch)],
    ))?;

    Ok(engine)
}

// some notes:
// - need to fix the duplication of table_root
// - this is super verbose

#[test]
fn test_path_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let engine = setup_test()?;
    let table_root = Url::parse("memory:///test_table/")?;

    let snapshot = Snapshot::try_new(table_root, vec![], engine.as_ref(), None)?;

    // now we have the usual kernel APIs
    let scan = snapshot.into_scan_builder().build()?;
    let data = scan.execute(engine)?;
    for batch in data {
        let batch = ArrowEngineData::try_from_engine_data(batch?.raw_data?)?;
        println!("RecordBatch: {:?}", batch.record_batch());
    }
    Ok(())
}

#[test]
fn test_catalog_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let engine = setup_test()?;
    let table_root = Url::parse("memory:///test_table")?;

    let (protocol, metadata, version, log_files) = mock_catalog_get_table()?;

    let table_configuration =
        TableConfiguration::try_new(metadata, protocol, table_root.clone(), version)?;

    let snapshot = Snapshot::try_new_from_metadata(
        table_root,
        log_files,
        engine.as_ref(),
        table_configuration,
    )?;

    // now we have the usual kernel APIs
    let scan = snapshot.into_scan_builder().build()?;
    let data = scan.execute(engine)?;
    for batch in data {
        let batch = ArrowEngineData::try_from_engine_data(batch?.raw_data?)?;
        println!("RecordBatch: {:?}", batch.record_batch());
    }
    Ok(())
}
