use std::collections::HashMap;
use std::sync::Arc;

use crate::actions::{get_log_schema, Format, Metadata, Protocol};
use crate::arrow::array::StringArray;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::DefaultEngine;
use crate::engine::sync::json::SyncJsonHandler;
use crate::log_segment::tests::{add_checkpoint_to_store, create_log_path};
use crate::log_segment::{ListedLogFiles, LogPathFileType, LogSegment, ParsedLogPath};
use crate::object_store::{memory::InMemory, path::Path, ObjectStore};
use crate::table_features::{ReaderFeature, WriterFeature};
use crate::utils::test_utils::{string_array_to_engine_data, Action};
use crate::{DeltaResult, FileMeta, JsonHandler};
use serde_json;
use url::Url;

// Helper function to create checkpoint data with Protocol and Metadata actions
fn create_checkpoint_data(
    metadata: Metadata,
    protocol: Protocol,
) -> DeltaResult<Box<ArrowEngineData>> {
    let handler = SyncJsonHandler {};
    let actions = vec![Action::Metadata(metadata), Action::Protocol(protocol)];
    let json_strings: Vec<String> = actions
        .iter()
        .map(|action| serde_json::to_string(action).unwrap())
        .collect();
    let string_array: StringArray = json_strings.into();
    let parsed = handler.parse_json(
        string_array_to_engine_data(string_array),
        get_log_schema().clone(),
    )?;
    Ok(ArrowEngineData::try_from_engine_data(parsed)?)
}

#[tokio::test]
async fn test_protocol_metadata_with_crc_at_target_version() -> DeltaResult<()> {
    // Test case: CRC file exists at the exact target version
    // Should return P+M directly from CRC without replay
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file with protocol and metadata at version 5
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": {
            "id": "test-table-5",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        },
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;

    // Create LogSegment with CRC at version 5, reading version 5
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![
            // Need at least one commit file for LogSegment to be valid
            create_log_path("file:///_delta_log/00000000000000000005.json"),
        ],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;

    // Call protocol_and_metadata - should return P+M directly from CRC
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert!(metadata.is_some());
    assert!(protocol.is_some());

    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");

    let protocol = protocol.unwrap();
    assert_eq!(protocol.min_reader_version(), 3);
    assert_eq!(protocol.min_writer_version(), 7);

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_with_crc_newer_than_checkpoint() -> DeltaResult<()> {
    // Test case: CRC file is newer than checkpoint
    // Should create pruned log segment starting from CRC version + 1
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file at version 5
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": {
            "id": "test-table-5",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        },
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;

    // Create commit at version 7 with new metadata
    let metadata_7 = Metadata {
        id: "test-table-7".to_string(),
        name: None,
        description: None,
        format: Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        },
        schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
        partition_columns: vec![],
        created_time: Some(1677811175),
        configuration: HashMap::new(),
    };

    let protocol_7 = Protocol::try_new(
        3,
        7,
        Some(vec![ReaderFeature::ColumnMapping]),
        Some(vec![WriterFeature::ColumnMapping]),
    )?;

    let actions_7 = vec![Action::Metadata(metadata_7), Action::Protocol(protocol_7)];
    let json_lines_7: Vec<String> = actions_7
        .iter()
        .map(|action| serde_json::to_string(action).unwrap())
        .collect();
    let commit_7_content = json_lines_7.join("\n");

    store
        .put(
            &Path::from("_delta_log/00000000000000000007.json"),
            commit_7_content.into(),
        )
        .await?;

    // Create commit at version 6 (empty commit)
    let commit_6_content = serde_json::json!({
        "add": {
            "path": "file-6.parquet",
            "size": 1000,
            "modificationTime": 1677811175,
            "dataChange": true
        }
    })
    .to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000006.json"),
            commit_6_content.into(),
        )
        .await?;

    // Create commit at version 8 (empty commit)
    let commit_8_content = serde_json::json!({
        "add": {
            "path": "file-8.parquet",
            "size": 1000,
            "modificationTime": 1677811175,
            "dataChange": true
        }
    })
    .to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000008.json"),
            commit_8_content.into(),
        )
        .await?;

    // Create LogSegment with checkpoint at v3, CRC at v5, reading v8
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![
            // Commits 6, 7, 8 (only these should be used in replay)
            create_log_path("file:///_delta_log/00000000000000000006.json"),
            create_log_path("file:///_delta_log/00000000000000000007.json"),
            create_log_path("file:///_delta_log/00000000000000000008.json"),
        ],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let mut log_segment = LogSegment::try_new(listed_files, log_root, Some(8))?;
    log_segment.checkpoint_version = Some(3); // Set checkpoint version

    // Call protocol_and_metadata - should use pruned segment and find P+M from commit 7
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert!(metadata.is_some());
    assert!(protocol.is_some());

    // Should get P+M from commit 7, not from CRC
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-7");

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_fallback_to_crc() -> DeltaResult<()> {
    // Test case: No P+M found in replay, should fallback to CRC
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file with P+M at version 5
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": {
            "id": "test-table-5",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        },
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;

    // Create commits 6, 7, 8 without P+M actions (just add actions)
    for v in 6..=8 {
        let commit_content = serde_json::json!({
            "add": {
                "path": format!("file-{}.parquet", v),
                "size": 1000,
                "modificationTime": 1677811175,
                "dataChange": true
            }
        })
        .to_string();

        store
            .put(
                &Path::from(format!("_delta_log/{v:020}.json").as_str()),
                commit_content.into(),
            )
            .await?;
    }

    // Create LogSegment with CRC at v5, reading v8
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![
            create_log_path("file:///_delta_log/00000000000000000006.json"),
            create_log_path("file:///_delta_log/00000000000000000007.json"),
            create_log_path("file:///_delta_log/00000000000000000008.json"),
        ],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(8))?;

    // Call protocol_and_metadata - should fallback to CRC when no P+M in commits
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert!(metadata.is_some());
    assert!(protocol.is_some());

    // Should get P+M from CRC at version 5
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_with_crc_older_than_checkpoint() -> DeltaResult<()> {
    // Test case: CRC file is older than checkpoint
    // Should use normal log segment (full replay from checkpoint)
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file at version 2 (older than checkpoint)
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": {
            "id": "test-table-2",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        },
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000002.crc"),
            crc_content.into(),
        )
        .await?;

    // Create checkpoint at version 5 with P+M
    let metadata = Metadata {
        id: "test-table-5".to_string(),
        name: None,
        description: None,
        format: Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        },
        schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
        partition_columns: vec![],
        created_time: Some(1677811175),
        configuration: HashMap::new(),
    };

    let protocol = Protocol::try_new(
        3,
        7,
        Some(vec![ReaderFeature::ColumnMapping]),
        Some(vec![WriterFeature::ColumnMapping]),
    )?;

    let checkpoint_data = create_checkpoint_data(metadata, protocol)?;
    add_checkpoint_to_store(
        &store,
        checkpoint_data,
        "00000000000000000005.checkpoint.parquet",
    )?;

    // Create commits for versions 6, 7, 8
    for v in 6..=8 {
        let commit_content = serde_json::json!({
            "add": {
                "path": format!("file-{}.parquet", v),
                "size": 1000,
                "modificationTime": 1677811175,
                "dataChange": true
            }
        })
        .to_string();
        store
            .put(
                &Path::from(format!("_delta_log/{v:020}.json").as_str()),
                commit_content.into(),
            )
            .await?;
    }

    // Create LogSegment with CRC at v2, checkpoint at v5, reading v8
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![
            create_log_path("file:///_delta_log/00000000000000000006.json"),
            create_log_path("file:///_delta_log/00000000000000000007.json"),
            create_log_path("file:///_delta_log/00000000000000000008.json"),
        ],
        vec![],
        vec![ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::SinglePartCheckpoint,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.checkpoint.parquet").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.checkpoint.parquet".to_string(),
            extension: "parquet".to_string(),
        }],
        Some(ParsedLogPath {
            version: 2,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000002.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000002.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let mut log_segment = LogSegment::try_new(listed_files, log_root, Some(8))?;
    log_segment.checkpoint_version = Some(5);

    // When CRC is older than checkpoint, should use normal replay from checkpoint
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert!(metadata.is_some());
    assert!(protocol.is_some());

    // Should get P+M from checkpoint, not CRC
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_no_crc_file() -> DeltaResult<()> {
    // Test case: No CRC file exists
    // Should use normal log segment replay
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create checkpoint at version 3 with P+M
    let metadata = Metadata {
        id: "test-table-3".to_string(),
        name: None,
        description: None,
        format: Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        },
        schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
        partition_columns: vec![],
        created_time: Some(1677811175),
        configuration: HashMap::new(),
    };

    let protocol = Protocol::try_new(
        3,
        7,
        Some(vec![ReaderFeature::ColumnMapping]),
        Some(vec![WriterFeature::ColumnMapping]),
    )?;

    let checkpoint_data = create_checkpoint_data(metadata, protocol)?;
    add_checkpoint_to_store(
        &store,
        checkpoint_data,
        "00000000000000000003.checkpoint.parquet",
    )?;

    // Create commits 4 and 5
    for v in 4..=5 {
        let commit_content = serde_json::json!({
            "add": {
                "path": format!("file-{}.parquet", v),
                "size": 1000,
                "modificationTime": 1677811175,
                "dataChange": true
            }
        })
        .to_string();
        store
            .put(
                &Path::from(format!("_delta_log/{v:020}.json").as_str()),
                commit_content.into(),
            )
            .await?;
    }

    // Create LogSegment with no CRC file
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![
            create_log_path("file:///_delta_log/00000000000000000004.json"),
            create_log_path("file:///_delta_log/00000000000000000005.json"),
        ],
        vec![],
        vec![ParsedLogPath {
            version: 3,
            file_type: LogPathFileType::SinglePartCheckpoint,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000003.checkpoint.parquet").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000003.checkpoint.parquet".to_string(),
            extension: "parquet".to_string(),
        }],
        None, // No CRC file
    );

    let mut log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;
    log_segment.checkpoint_version = Some(3);

    // With no CRC file, should use normal replay
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert!(metadata.is_some());
    assert!(protocol.is_some());

    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-3");

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_crc_without_protocol_should_error() -> DeltaResult<()> {
    // Test case: CRC file missing Protocol should error
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC without protocol field
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 0,
        "metadata": {
            "id": "test-table-5",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        }
    }).to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;

    // Create a dummy commit file
    let commit_content = serde_json::json!({
        "add": {
            "path": "file-5.parquet",
            "size": 1000,
            "modificationTime": 1677811175,
            "dataChange": true
        }
    })
    .to_string();
    store
        .put(
            &Path::from("_delta_log/00000000000000000005.json"),
            commit_content.into(),
        )
        .await?;

    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![create_log_path(
            "file:///_delta_log/00000000000000000005.json",
        )],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;

    // Should error when CRC is missing protocol
    let result = log_segment.protocol_and_metadata(&engine);
    assert!(result.is_err());

    if let Err(e) = result {
        // Current implementation returns JSON parsing error rather than specific message
        assert!(e.to_string().contains("whilst decoding field 'protocol'"));
    }

    Ok(())
}

#[tokio::test]
async fn test_protocol_metadata_crc_without_metadata_should_error() -> DeltaResult<()> {
    // Test case: CRC file missing Metadata should error
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC without metadata field
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 0,
        "numProtocol": 1,
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    })
    .to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;

    // Create a dummy commit file
    let commit_content = serde_json::json!({
        "add": {
            "path": "file-5.parquet",
            "size": 1000,
            "modificationTime": 1677811175,
            "dataChange": true
        }
    })
    .to_string();
    store
        .put(
            &Path::from("_delta_log/00000000000000000005.json"),
            commit_content.into(),
        )
        .await?;

    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    let listed_files = ListedLogFiles::new(
        vec![create_log_path(
            "file:///_delta_log/00000000000000000005.json",
        )],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;

    // Should error when CRC is missing metadata
    let result = log_segment.protocol_and_metadata(&engine);
    assert!(result.is_err());

    if let Err(e) = result {
        // Current implementation returns JSON parsing error rather than specific message
        assert!(e.to_string().contains("whilst decoding field 'metadata'"));
    }

    Ok(())
}

#[test]
fn test_find_commit_cover_with_crc_pruned_segment() -> DeltaResult<()> {
    // Test case: Verify find_commit_cover works correctly with pruned segments
    let log_root = Url::parse("memory:///_delta_log/").unwrap();

    // Create a segment with checkpoint at v3, CRC at v5, commits up to v8
    let listed_files = ListedLogFiles::new(
        vec![
            create_log_path("file:///_delta_log/00000000000000000000.json"),
            create_log_path("file:///_delta_log/00000000000000000001.json"),
            create_log_path("file:///_delta_log/00000000000000000002.json"),
            create_log_path("file:///_delta_log/00000000000000000003.json"),
            create_log_path("file:///_delta_log/00000000000000000004.json"),
            create_log_path("file:///_delta_log/00000000000000000005.json"),
            create_log_path("file:///_delta_log/00000000000000000006.json"),
            create_log_path("file:///_delta_log/00000000000000000007.json"),
            create_log_path("file:///_delta_log/00000000000000000008.json"),
        ],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                Url::parse("memory:///_delta_log/00000000000000000005.crc").unwrap(),
                0,
                0,
            ),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    );

    let mut log_segment = LogSegment::try_new(listed_files, log_root, Some(8))?;
    log_segment.checkpoint_version = Some(3);

    // Simulate pruning (what happens in protocol_and_metadata)
    log_segment.checkpoint_version = None;
    log_segment.checkpoint_parts.clear();
    log_segment.ascending_commit_files.retain(|c| c.version > 5);

    // find_commit_cover should only return commits 6, 7, 8
    let covered_files = log_segment.find_commit_cover();
    assert_eq!(covered_files.len(), 3);

    // Verify the versions are correct
    let versions: Vec<u64> = covered_files
        .iter()
        .filter_map(|f| {
            let path_str = f.location.path();
            if path_str.contains(".json") {
                // Extract version from filename like "00000000000000000006.json"
                path_str
                    .split('/')
                    .next_back()
                    .and_then(|filename| filename.split('.').next())
                    .and_then(|version_str| version_str.parse::<u64>().ok())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(versions, vec![8, 7, 6]); // Reversed order as find_commit_cover returns descending

    Ok(())
}
