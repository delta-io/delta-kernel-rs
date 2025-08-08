/// CRC tests: currently we support reading Protocol/Metadata from CRC files to speed log replay.
///
/// Every test here is effectively a function of files written to the store and the LogSegment we
/// 'ask' for the Protocol/Metadata from.
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
use crate::{DeltaResult, Error, FileMeta, JsonHandler};

use serde_json;
use url::Url;
use uuid::Uuid;

// Helper function to create checkpoint data with optional Protocol and Metadata actions
fn create_checkpoint_data(
    metadata: Option<Metadata>,
    protocol: Option<Protocol>,
) -> DeltaResult<Box<ArrowEngineData>> {
    let handler = SyncJsonHandler {};
    let mut actions = Vec::new();
    if let Some(metadata) = metadata {
        actions.push(Action::Metadata(metadata));
    }
    if let Some(protocol) = protocol {
        actions.push(Action::Protocol(protocol));
    }

    // If no P+M actions, add a dummy add action
    if actions.is_empty() {
        let add_json = r#"{"add":{"path":"dummy.parquet","size":1000,"modificationTime":1000,"dataChange":true}}"#;
        let string_array: StringArray = vec![add_json].into();
        let parsed = handler.parse_json(
            string_array_to_engine_data(string_array),
            get_log_schema().clone(),
        )?;
        return ArrowEngineData::try_from_engine_data(parsed);
    }

    let json_strings: Vec<String> = actions
        .iter()
        .map(|action| serde_json::to_string(action).unwrap())
        .collect();
    let string_array: StringArray = json_strings.into();
    let parsed = handler.parse_json(
        string_array_to_engine_data(string_array),
        get_log_schema().clone(),
    )?;
    ArrowEngineData::try_from_engine_data(parsed)
}

// Write a CRC files to the store with given version, metadata, and protocol
async fn write_crc(
    store: &InMemory,
    version: u64,
    metadata: Metadata,
    protocol: Protocol,
) -> DeltaResult<()> {
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": metadata,
        "protocol": protocol
    })
    .to_string();

    let crc_path = format!("_delta_log/{version:020}.crc");
    store.put(&Path::from(crc_path), crc_content.into()).await?;

    Ok(())
}

// Return unique protocol and metadata actions for testing
fn unique_protocol_metadata() -> (Protocol, Metadata) {
    (
        Protocol::try_new(
            chrono::Utc::now().timestamp_subsec_nanos() as i32 + 3,
            chrono::Utc::now().timestamp_subsec_nanos() as i32 + 7,
            Some(vec![ReaderFeature::ColumnMapping]),
            Some(vec![WriterFeature::ColumnMapping]),
        ).unwrap(),
        Metadata {
            id: Uuid::new_v4().to_string(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".to_string(),
                options: HashMap::new(),
            },
            schema_string:
                r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#
                    .to_string(),
            partition_columns: vec![],
            created_time: Some(chrono::Utc::now().timestamp_millis()),
            configuration: HashMap::new(),
        }
    )
}

struct TestCrc {
    version: u64,
    metadata: Metadata,
    protocol: Protocol,
}

struct TestCommit {
    version: u64,
    metadata: Option<Metadata>,
    protocol: Option<Protocol>,
}

struct TestCheckpoint {
    version: u64,
    metadata: Option<Metadata>,
    protocol: Option<Protocol>,
}

#[allow(dead_code)]
struct TestCompaction {
    lo_version: u64,
    hi_version: u64,
    metadata: Option<Metadata>,
    protocol: Option<Protocol>,
}

/// setup the test with in-memory store and default engine
fn setup_test() -> (Arc<InMemory>, DefaultEngine<TokioBackgroundExecutor>, Url) {
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    (store, engine, log_root)
}

// Convenience function to create a commit with P+M
fn commit_with_pm(version: u64, metadata: Metadata, protocol: Protocol) -> TestCommit {
    TestCommit {
        version,
        metadata: Some(metadata),
        protocol: Some(protocol),
    }
}

// Convenience function to create a commit without P+M (just add actions)
fn commit_without_pm(version: u64) -> TestCommit {
    TestCommit {
        version,
        metadata: None,
        protocol: None,
    }
}

// Convenience function to create a checkpoint with P+M
fn checkpoint_with_pm(version: u64, metadata: Metadata, protocol: Protocol) -> TestCheckpoint {
    TestCheckpoint {
        version,
        metadata: Some(metadata),
        protocol: Some(protocol),
    }
}

// Convenience function to create a CRC file
fn crc_with_pm(version: u64, metadata: Metadata, protocol: Protocol) -> TestCrc {
    TestCrc {
        version,
        metadata,
        protocol,
    }
}

/// Do the test according to the given commits, compactions, checkpoint, and latest CRC.
/// Returns the LogSegment and the engine for specific test case to do assertions.
async fn crc_pm_test(
    commits: Vec<TestCommit>,
    compactions: Vec<TestCompaction>,
    checkpoint: Option<TestCheckpoint>,
    latest_crc: Option<TestCrc>,
    target_version: Option<u64>,
) -> DeltaResult<(LogSegment, DefaultEngine<TokioBackgroundExecutor>)> {
    let (store, engine, log_root) = setup_test();

    // Write commits to store and collect ParsedLogPaths
    let mut commit_paths = Vec::new();
    for commit in commits {
        // Write the commit with optional metadata and protocol actions
        let mut actions = Vec::new();
        if let Some(metadata) = commit.metadata {
            actions.push(Action::Metadata(metadata));
        }
        if let Some(protocol) = commit.protocol {
            actions.push(Action::Protocol(protocol));
        }

        // Add a dummy add action if no P+M actions to make it a valid log file
        if actions.is_empty() {
            let add_json = r#"{"add":{"path":"dummy.parquet","size":1000,"modificationTime":1000,"dataChange":true}}"#;
            let commit_path = format!("_delta_log/{:020}.json", commit.version);
            store
                .put(&Path::from(commit_path.as_str()), add_json.into())
                .await?;
        } else {
            let json_lines: Vec<String> = actions
                .iter()
                .map(|action| serde_json::to_string(action).unwrap())
                .collect();
            let content = json_lines.join("\n");
            let commit_path = format!("_delta_log/{:020}.json", commit.version);
            store
                .put(&Path::from(commit_path.as_str()), content.into())
                .await?;
        }

        // Create ParsedLogPath for this commit
        let url = log_root.join(&format!("{:020}.json", commit.version))?;
        commit_paths.push(create_log_path(url.as_ref()));
    }

    // Write compactions to store and collect ParsedLogPaths
    let mut compaction_paths = Vec::new();
    for compaction in compactions {
        // Write the compaction with optional metadata and protocol actions
        let mut actions = Vec::new();
        if let Some(metadata) = compaction.metadata {
            actions.push(Action::Metadata(metadata));
        }
        if let Some(protocol) = compaction.protocol {
            actions.push(Action::Protocol(protocol));
        }

        // Add a dummy add action if no P+M actions to make it a valid log file
        if actions.is_empty() {
            let add_json = r#"{"add":{"path":"dummy.parquet","size":1000,"modificationTime":1000,"dataChange":true}}"#;
            let compaction_path = format!(
                "_delta_log/{:020}.{:020}.compacted.json",
                compaction.lo_version, compaction.hi_version
            );
            store
                .put(&Path::from(compaction_path.as_str()), add_json.into())
                .await?;
        } else {
            let json_lines: Vec<String> = actions
                .iter()
                .map(|action| serde_json::to_string(action).unwrap())
                .collect();
            let content = json_lines.join("\n");
            let compaction_path = format!(
                "_delta_log/{:020}.{:020}.compacted.json",
                compaction.lo_version, compaction.hi_version
            );
            store
                .put(&Path::from(compaction_path.as_str()), content.into())
                .await?;
        }

        // Create ParsedLogPath for this compaction
        let url = log_root.join(&format!(
            "{:020}.{:020}.compacted.json",
            compaction.lo_version, compaction.hi_version
        ))?;
        compaction_paths.push(create_log_path(url.as_ref()));
    }

    // Write checkpoint if provided
    let mut checkpoint_paths = vec![];
    if let Some(checkpoint) = checkpoint {
        let checkpoint_data = create_checkpoint_data(checkpoint.metadata, checkpoint.protocol)?;
        let checkpoint_filename = format!("{:020}.checkpoint.parquet", checkpoint.version);
        add_checkpoint_to_store(&store, checkpoint_data, &checkpoint_filename)?;

        let url = log_root.join(&checkpoint_filename)?;
        checkpoint_paths.push(create_log_path(url.as_ref()));
    }

    // Write CRC if provided
    let crc_path = if let Some(latest_crc) = latest_crc {
        write_crc(
            &store,
            latest_crc.version,
            latest_crc.metadata,
            latest_crc.protocol,
        )
        .await?;

        Some(ParsedLogPath {
            version: latest_crc.version,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(
                log_root.join(&format!("{:020}.crc", latest_crc.version))?,
                0,
                0,
            ),
            filename: format!("{:020}.crc", latest_crc.version),
            extension: "crc".to_string(),
        })
    } else {
        None
    };

    let listed_files =
        ListedLogFiles::try_new(commit_paths, compaction_paths, checkpoint_paths, crc_path)
            .unwrap();

    let log_segment = LogSegment::try_new(listed_files, log_root, target_version)?;
    Ok((log_segment, engine))
}

// CRC file exists at the exact target version: should return P+M directly from CRC without replay
#[tokio::test]
async fn test_crc_at_target_version() -> DeltaResult<()> {
    // setup
    let (expected_protocol, expected_metadata) = unique_protocol_metadata();

    // Write out crc at version 5 with P+M, expect the same P+M and no other reads
    let latest_crc = crc_with_pm(5, expected_metadata.clone(), expected_protocol.clone());

    // Need at least one commit file at version 5 for LogSegment to be valid
    // We put other metadata/protocol to ensure they are not used
    let (other_protocol, other_metadata) = unique_protocol_metadata();
    let commit = commit_with_pm(5, other_metadata, other_protocol);

    let (log_segment, engine) = crc_pm_test(
        vec![commit], // commit at version 5
        vec![],       // no compactions
        None,         // no checkpoint
        Some(latest_crc),
        Some(5), // target version 5
    )
    .await?;

    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert_eq!(metadata.unwrap(), expected_metadata);
    assert_eq!(protocol.unwrap(), expected_protocol);

    Ok(())
}

// Example: Test with no CRC file - should use normal replay from commits
#[tokio::test]
async fn test_no_crc_file() -> DeltaResult<()> {
    let (expected_protocol, expected_metadata) = unique_protocol_metadata();

    let commits = vec![
        commit_without_pm(1), // Just add actions
        commit_with_pm(2, expected_metadata.clone(), expected_protocol.clone()), // P+M
    ];

    let (log_segment, engine) = crc_pm_test(
        commits,
        vec![],  // no compactions
        None,    // no checkpoint
        None,    // no CRC
        Some(2), // target version 2
    )
    .await?;

    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    assert_eq!(metadata.unwrap(), expected_metadata);
    assert_eq!(protocol.unwrap(), expected_protocol);

    Ok(())
}

// Test CRC exists before target version - should replay from CRC and find newer P+M
#[tokio::test]
async fn test_crc_before_target_version() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();
    let (new_protocol, new_metadata) = unique_protocol_metadata();

    let commits = vec![
        commit_with_pm(3, crc_metadata.clone(), crc_protocol.clone()),
        commit_without_pm(4),
        commit_with_pm(5, new_metadata.clone(), new_protocol.clone()),
    ];

    let latest_crc = crc_with_pm(3, crc_metadata, crc_protocol);

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], None, Some(latest_crc), Some(5)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should get new P+M from commit 5, not from CRC
    assert_eq!(metadata.unwrap(), new_metadata);
    assert_eq!(protocol.unwrap(), new_protocol);
    Ok(())
}

// Test CRC newer than checkpoint - should use CRC's P+M
#[tokio::test]
async fn test_crc_newer_than_checkpoint() -> DeltaResult<()> {
    let (checkpoint_protocol, checkpoint_metadata) = unique_protocol_metadata();
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();

    let checkpoint = checkpoint_with_pm(2, checkpoint_metadata, checkpoint_protocol);
    let latest_crc = crc_with_pm(4, crc_metadata.clone(), crc_protocol.clone()); // CRC newer
    let commits = vec![commit_without_pm(3), commit_without_pm(4)];

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], Some(checkpoint), Some(latest_crc), Some(4)).await?;

    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should get P+M from CRC (newer than checkpoint)
    assert_eq!(metadata.unwrap(), crc_metadata);
    assert_eq!(protocol.unwrap(), crc_protocol);
    Ok(())
}

// Test CRC older than checkpoint - should use checkpoint's P+M
#[tokio::test]
async fn test_crc_older_than_checkpoint() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();
    let (checkpoint_protocol, checkpoint_metadata) = unique_protocol_metadata();

    let latest_crc = crc_with_pm(2, crc_metadata, crc_protocol); // CRC older
    let checkpoint =
        checkpoint_with_pm(4, checkpoint_metadata.clone(), checkpoint_protocol.clone());
    let commits = vec![
        commit_without_pm(3),
        commit_without_pm(4),
        commit_without_pm(5),
    ];

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], Some(checkpoint), Some(latest_crc), Some(5)).await?;

    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should get P+M from checkpoint (CRC is older)
    assert_eq!(metadata.unwrap(), checkpoint_metadata);
    assert_eq!(protocol.unwrap(), checkpoint_protocol);
    Ok(())
}

// Test CRC file missing Protocol - should error
#[tokio::test]
async fn test_crc_missing_protocol() -> DeltaResult<()> {
    let (store, engine, log_root) = setup_test();

    // Write CRC without protocol field
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 1,
        "numProtocol": 0,
        "metadata": {
            "id": "test-table",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": r#"{"type":"struct","fields":[]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1000
        }
    })
    .to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;
    store.put(&Path::from("_delta_log/00000000000000000005.json"),
        r#"{"add":{"path":"file.parquet","size":1000,"modificationTime":1000,"dataChange":true}}"#.into()).await?;

    let listed_files = ListedLogFiles::try_new(
        vec![create_log_path(
            log_root.join("00000000000000000005.json")?.as_ref(),
        )],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(log_root.join("00000000000000000005.crc")?, 0, 0),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    )
    .unwrap();

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;
    let error = log_segment.protocol_and_metadata(&engine).unwrap_err();

    assert!(matches!(error, Error::Arrow(s) if s.to_string().contains(
            "Json error: whilst decoding field 'protocol': expected { got null")));
    Ok(())
}

// Test CRC file missing Metadata - should error
#[tokio::test]
async fn test_crc_missing_metadata() -> DeltaResult<()> {
    let (store, engine, log_root) = setup_test();

    // Write CRC without metadata field
    let crc_content = serde_json::json!({
        "tableSizeBytes": 1000,
        "numFiles": 10,
        "numMetadata": 0,
        "numProtocol": 1,
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7
        }
    })
    .to_string();

    store
        .put(
            &Path::from("_delta_log/00000000000000000005.crc"),
            crc_content.into(),
        )
        .await?;
    store.put(&Path::from("_delta_log/00000000000000000005.json"),
        r#"{"add":{"path":"file.parquet","size":1000,"modificationTime":1000,"dataChange":true}}"#.into()).await?;

    let listed_files = ListedLogFiles::try_new(
        vec![create_log_path(
            log_root.join("00000000000000000005.json")?.as_ref(),
        )],
        vec![],
        vec![],
        Some(ParsedLogPath {
            version: 5,
            file_type: LogPathFileType::Crc,
            location: FileMeta::new(log_root.join("00000000000000000005.crc")?, 0, 0),
            filename: "00000000000000000005.crc".to_string(),
            extension: "crc".to_string(),
        }),
    )
    .unwrap();

    let log_segment = LogSegment::try_new(listed_files, log_root, Some(5))?;
    let error = log_segment.protocol_and_metadata(&engine).unwrap_err();

    assert!(matches!(error, Error::Arrow(s) if s.to_string().contains(
            "Json error: whilst decoding field 'metadata': expected { got null")));
    Ok(())
}

// Test CRC with compaction containing new P+M - should use compaction's P+M
#[tokio::test]
async fn test_crc_with_compaction_new_pm() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();
    let (new_protocol, new_metadata) = unique_protocol_metadata();

    let commits = vec![
        commit_without_pm(3),
        commit_without_pm(4),
        commit_without_pm(5),
    ];
    let latest_crc = crc_with_pm(3, crc_metadata, crc_protocol);
    let compaction = TestCompaction {
        lo_version: 2,
        hi_version: 5, // Starts before CRC, ends after
        metadata: Some(new_metadata.clone()),
        protocol: Some(new_protocol.clone()),
    };

    let (log_segment, engine) =
        crc_pm_test(commits, vec![compaction], None, Some(latest_crc), Some(5)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should get new P+M from compaction
    assert_eq!(metadata.unwrap(), new_metadata);
    assert_eq!(protocol.unwrap(), new_protocol);
    Ok(())
}

// Test CRC with compaction but P+M comes from CRC - should use CRC's P+M
#[tokio::test]
async fn test_crc_with_compaction_crc_pm() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();

    let commits = vec![commit_without_pm(4), commit_without_pm(5)];
    let latest_crc = crc_with_pm(5, crc_metadata.clone(), crc_protocol.clone()); // CRC at target version
    let compaction = TestCompaction {
        lo_version: 3,
        hi_version: 5, // No new P+M in compaction
        metadata: None,
        protocol: None,
    };

    let (log_segment, engine) =
        crc_pm_test(commits, vec![compaction], None, Some(latest_crc), Some(5)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should use P+M from CRC (at target version)
    assert_eq!(metadata.unwrap(), crc_metadata);
    assert_eq!(protocol.unwrap(), crc_protocol);
    Ok(())
}

// Test CRC fallback - CRC before target, replay finds no P+M, should fallback to CRC
#[tokio::test]
async fn test_crc_fallback() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();

    let commits = vec![
        commit_without_pm(4), // No P+M in replay
        commit_without_pm(5), // No P+M in replay
    ];
    let latest_crc = crc_with_pm(3, crc_metadata.clone(), crc_protocol.clone());

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], None, Some(latest_crc), Some(5)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should fallback to CRC's P+M since replay found nothing
    assert_eq!(metadata.unwrap(), crc_metadata);
    assert_eq!(protocol.unwrap(), crc_protocol);
    Ok(())
}

// Test CRC version equals checkpoint version - should use CRC
#[tokio::test]
async fn test_crc_equals_checkpoint_version() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();
    let (checkpoint_protocol, checkpoint_metadata) = unique_protocol_metadata();

    let commits = vec![commit_without_pm(3), commit_without_pm(4)];
    let checkpoint = checkpoint_with_pm(3, checkpoint_metadata, checkpoint_protocol); // same version
    let latest_crc = crc_with_pm(3, crc_metadata.clone(), crc_protocol.clone()); // same version

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], Some(checkpoint), Some(latest_crc), Some(4)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // CRC version >= checkpoint version, so should use CRC PM
    assert_eq!(metadata.unwrap(), crc_metadata);
    assert_eq!(protocol.unwrap(), crc_protocol);
    Ok(())
}

// Test partial CRC fallback - replay finds only Protocol, should fallback to CRC for Metadata
#[tokio::test]
async fn test_crc_partial_fallback() -> DeltaResult<()> {
    let (crc_protocol, crc_metadata) = unique_protocol_metadata();
    let (replay_protocol, _) = unique_protocol_metadata();

    let commits = vec![
        TestCommit {
            // Only Protocol in replay, no Metadata
            version: 4,
            metadata: None,
            protocol: Some(replay_protocol.clone()),
        },
        commit_without_pm(5),
    ];
    let latest_crc = crc_with_pm(3, crc_metadata.clone(), crc_protocol);

    let (log_segment, engine) =
        crc_pm_test(commits, vec![], None, Some(latest_crc), Some(5)).await?;
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;

    // Should use Protocol from replay, Metadata from CRC fallback
    assert_eq!(metadata.unwrap(), crc_metadata); // From CRC
    assert_eq!(protocol.unwrap(), replay_protocol); // From replay
    Ok(())
}
