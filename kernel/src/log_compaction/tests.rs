use super::COMPACTION_ACTIONS_SCHEMA;
use super::{should_compact, LogCompactionWriter};
use crate::engine::sync::SyncEngine;
use crate::snapshot::Snapshot;
use std::sync::Arc;

// Helper to create a mock snapshot for testing
fn create_mock_snapshot() -> Arc<Snapshot> {
    let path = std::fs::canonicalize(std::path::PathBuf::from(
        "./tests/data/table-with-dv-small/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    Arc::new(Snapshot::builder(url).build(&engine).unwrap())
}

// Helper removed - not needed for these simplified tests

#[test]
fn test_log_compaction_writer_creation() {
    let snapshot = create_mock_snapshot();
    let start_version = 0;
    let end_version = 1;

    let writer = LogCompactionWriter::try_new(snapshot, start_version, end_version).unwrap();

    // Test that compaction path is correct
    let path = writer.compaction_path().unwrap();
    let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
    assert!(path.to_string().ends_with(expected_filename));
}

#[test]
fn test_invalid_version_range() {
    let start_version = 20;
    let end_version = 10; // Invalid: start > end

    let result = LogCompactionWriter::try_new(create_mock_snapshot(), start_version, end_version);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid version range"));
}

#[test]
fn test_should_compact() {
    assert!(should_compact(9, 10));
    assert!(!should_compact(5, 10));
    assert!(!should_compact(10, 0));
    assert!(!should_compact(0, 10));
    assert!(should_compact(19, 10));
}

#[test]
fn test_compaction_actions_schema_access() {
    // Test that we can access the compaction schema
    let schema = &*COMPACTION_ACTIONS_SCHEMA;
    assert!(schema.fields().len() > 0);

    // Check for expected action types
    let field_names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"add"));
    assert!(field_names.contains(&"remove"));
    assert!(field_names.contains(&"metaData"));
    assert!(field_names.contains(&"protocol"));
}

#[test]
fn test_writer_debug_impl() {
    let snapshot = create_mock_snapshot();
    let writer = LogCompactionWriter::try_new(snapshot, 1, 5).unwrap();

    let debug_str = format!("{:?}", writer);
    assert!(debug_str.contains("LogCompactionWriter"));
}

#[test]
fn test_equal_version_range() {
    let snapshot = create_mock_snapshot();
    let writer = LogCompactionWriter::try_new(snapshot, 5, 5).unwrap();

    let path = writer.compaction_path().unwrap();
    let expected_filename = "00000000000000000005.00000000000000000005.compacted.json";
    assert!(path.to_string().ends_with(expected_filename));
}
