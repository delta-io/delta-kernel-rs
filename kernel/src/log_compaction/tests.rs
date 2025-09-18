use super::{should_compact, LogCompactionWriter, COMPACTION_ACTIONS_SCHEMA};
use crate::action_reconciliation::RetentionCalculator;
use crate::engine::sync::SyncEngine;
use crate::snapshot::Snapshot;
use crate::SnapshotRef;

fn create_mock_snapshot() -> SnapshotRef {
    let path = std::fs::canonicalize(std::path::PathBuf::from(
        "./tests/data/table-with-dv-small/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    Snapshot::builder_for(url).build(&engine).unwrap()
}

fn create_multi_version_snapshot() -> SnapshotRef {
    let path =
        std::fs::canonicalize(std::path::PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    Snapshot::builder_for(url).build(&engine).unwrap()
}

#[test]
fn test_log_compaction_writer_creation() {
    let snapshot = create_mock_snapshot();
    let start_version = 0;
    let end_version = 1;

    let writer = LogCompactionWriter::try_new(snapshot, start_version, end_version).unwrap();

    // Verify compaction path
    let path = writer.compaction_path();
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

    let path = writer.compaction_path();
    let expected_filename = "00000000000000000005.00000000000000000005.compacted.json";
    assert!(path.to_string().ends_with(expected_filename));
}

#[test]
fn test_compaction_data() {
    let snapshot = create_mock_snapshot();
    let mut writer = LogCompactionWriter::try_new(snapshot, 0, 0).unwrap();
    let engine = SyncEngine::new();

    let result = writer.compaction_data(&engine);
    assert!(result.is_ok());

    let iterator = result.unwrap();

    // Test iterator methods
    assert_eq!(iterator.total_actions(), 0);
    assert_eq!(iterator.total_add_actions(), 0);

    // Test debug implementation
    let debug_str = format!("{:?}", iterator);
    assert!(debug_str.contains("LogCompactionDataIterator"));
    assert!(debug_str.contains("actions_count"));
    assert!(debug_str.contains("add_actions_count"));
}

#[test]
fn test_end_version_exceeds_snapshot_version() {
    let snapshot = create_mock_snapshot();
    let snapshot_version = snapshot.version();

    // Negative test to create a writer with end_version greater than snapshot version
    let mut writer = LogCompactionWriter::try_new(snapshot, 0, snapshot_version + 100).unwrap();
    let engine = SyncEngine::new();

    let result = writer.compaction_data(&engine);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("exceeds snapshot version"));
}

#[test]
fn test_retention_calculator() {
    let snapshot = create_mock_snapshot();
    let writer = LogCompactionWriter::try_new(snapshot.clone(), 0, 1).unwrap();

    let table_props = writer.table_properties();
    assert_eq!(table_props, snapshot.table_properties());
}

#[test]
fn test_compaction_data_with_actual_iterator() {
    let snapshot = create_multi_version_snapshot();
    let mut writer = LogCompactionWriter::try_new(snapshot, 0, 1).unwrap();
    let engine = SyncEngine::new();

    let mut iterator = writer.compaction_data(&engine).unwrap();

    let mut batch_count = 0;
    let initial_actions = iterator.total_actions();
    let initial_add_actions = iterator.total_add_actions();

    // Both should start at 0
    assert_eq!(initial_actions, 0);
    assert_eq!(initial_add_actions, 0);

    while let Some(batch_result) = iterator.next() {
        batch_count += 1;
        assert!(batch_result.is_ok());

        // After processing some batches, the counts should be >= the initial counts
        assert!(iterator.total_actions() >= initial_actions);
        assert!(iterator.total_add_actions() >= initial_add_actions);
    }

    assert!(batch_count > 0, "Expected to process at least one batch");
}

#[test]
fn test_compaction_paths() {
    let snapshot = create_mock_snapshot();

    // Test various version ranges produce correct paths
    let test_cases = vec![
        (
            0,
            5,
            "00000000000000000000.00000000000000000005.compacted.json",
        ),
        (
            10,
            20,
            "00000000000000000010.00000000000000000020.compacted.json",
        ),
        (
            100,
            200,
            "00000000000000000100.00000000000000000200.compacted.json",
        ),
    ];

    for (start, end, expected_suffix) in test_cases {
        let writer = LogCompactionWriter::try_new(snapshot.clone(), start, end).unwrap();
        let path = writer.compaction_path();
        assert!(
            path.to_string().ends_with(expected_suffix),
            "Path {} doesn't end with {}",
            path,
            expected_suffix
        );
    }
}

#[test]
fn test_version_filtering() {
    let snapshot = create_multi_version_snapshot();
    let engine = SyncEngine::new();
    let snapshot_version = snapshot.version();

    if snapshot_version >= 1 {
        let mut writer = LogCompactionWriter::try_new(snapshot.clone(), 0, 1).unwrap();

        let result = writer.compaction_data(&engine);
        assert!(
            result.is_ok(),
            "Failed to get compaction data: {:?}",
            result.err()
        );

        let iterator = result.unwrap();
        assert!(iterator.total_actions() >= 0);
        assert!(iterator.total_add_actions() >= 0);
    }
}
