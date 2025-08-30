use super::writer::{should_compact, LogCompactionDataIterator, LogCompactionWriter};
use crate::engine::sync::SyncEngine;
use crate::path::ParsedLogPath;
use crate::{DeltaResult, EngineData, FileMeta};
use url::Url;

#[test]
fn test_log_compaction_writer_creation() {
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let start_version = 10;
    let end_version = 20;
    let retention_millis = 7 * 24 * 60 * 60 * 1000; // 7 days

    let writer = LogCompactionWriter::new(
        table_root.clone(),
        start_version,
        end_version,
        retention_millis,
    )
    .unwrap();

    // Test that compaction path is correct
    let path = writer.compaction_path().unwrap();
    let expected_filename = "00000000000000000010.00000000000000000020.compacted.json";
    assert!(path.to_string().ends_with(expected_filename));
}

#[test]
fn test_invalid_version_range() {
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let start_version = 20;
    let end_version = 10; // Invalid: start > end
    let retention_millis = 7 * 24 * 60 * 60 * 1000;

    let result = LogCompactionWriter::new(table_root, start_version, end_version, retention_millis);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid version range"));
}

#[test]
fn test_log_compaction_data_iterator() {
    let mut iterator = LogCompactionDataIterator {
        batches: vec![].into_iter(),
        total_actions: 0,
        total_add_actions: 0,
    };

    // Test empty iterator
    assert!(iterator.next().is_none());
}

#[test]
fn test_should_compact() {
    // Test basic functionality - (commitVersion + 1) % interval == 0
    assert!(!should_compact(10, 10)); // (10 + 1) % 10 != 0
    assert!(should_compact(9, 10)); // (9 + 1) % 10 == 0
    assert!(should_compact(19, 10)); // (19 + 1) % 10 == 0
    assert!(!should_compact(20, 10)); // (20 + 1) % 10 != 0
    assert!(!should_compact(5, 10)); // (5 + 1) % 10 != 0
    assert!(!should_compact(15, 10)); // (15 + 1) % 10 != 0

    // Test edge cases
    assert!(!should_compact(0, 10)); // commit_version must be > 0
    assert!(!should_compact(10, 0)); // compaction_interval must be > 0
    assert!(!should_compact(0, 0)); // both must be > 0

    // Test typical intervals
    assert!(should_compact(49, 50)); // (49 + 1) % 50 == 0 - Every 50 commits
    assert!(should_compact(99, 100)); // (99 + 1) % 100 == 0 - Every 100 commits
    assert!(!should_compact(100, 50)); // (100 + 1) % 50 != 0
    assert!(!should_compact(101, 50)); // (101 + 1) % 50 != 0

    // Test with interval of 1 (compact every commit after first)
    assert!(should_compact(1, 2)); // (1 + 1) % 2 == 0
    assert!(should_compact(3, 2)); // (3 + 1) % 2 == 0
    assert!(!should_compact(2, 2)); // (2 + 1) % 2 != 0
}

#[test]
fn test_compaction_path_formats() {
    // Test various version ranges to ensure correct path formatting
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let retention_millis = 7 * 24 * 60 * 60 * 1000;

    let test_cases = vec![
        (
            0,
            0,
            "00000000000000000000.00000000000000000000.compacted.json",
        ),
        (
            0,
            1,
            "00000000000000000000.00000000000000000001.compacted.json",
        ),
        (
            10,
            20,
            "00000000000000000010.00000000000000000020.compacted.json",
        ),
        (
            100,
            999,
            "00000000000000000100.00000000000000000999.compacted.json",
        ),
        (
            9999,
            10000,
            "00000000000000009999.00000000000000010000.compacted.json",
        ),
    ];

    for (start, end, expected_filename) in test_cases {
        let writer =
            LogCompactionWriter::new(table_root.clone(), start, end, retention_millis).unwrap();

        let path = writer.compaction_path().unwrap();
        assert!(
            path.to_string().ends_with(expected_filename),
            "Path {} does not end with {}",
            path,
            expected_filename
        );
    }
}

#[test]
fn test_equal_version_range() {
    // Test that equal start and end versions are valid
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let retention_millis = 7 * 24 * 60 * 60 * 1000;

    let writer = LogCompactionWriter::new(
        table_root.clone(),
        5,
        5, // Same version for start and end
        retention_millis,
    );

    assert!(writer.is_ok());
    let writer = writer.unwrap();
    let path = writer.compaction_path().unwrap();
    assert!(path
        .to_string()
        .ends_with("00000000000000000005.00000000000000000005.compacted.json"));
}

#[test]
fn test_iterator_methods() {
    let iterator = LogCompactionDataIterator {
        batches: vec![].into_iter(),
        total_actions: 100,
        total_add_actions: 50,
    };

    assert_eq!(iterator.total_actions(), 100);
    assert_eq!(iterator.total_add_actions(), 50);
}

#[test]
fn test_large_version_numbers() {
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let retention_millis = 7 * 24 * 60 * 60 * 1000;

    // Test with large version numbers
    let start_version = 999999;
    let end_version = 1000000;

    let writer = LogCompactionWriter::new(
        table_root.clone(),
        start_version,
        end_version,
        retention_millis,
    )
    .unwrap();

    let path = writer.compaction_path().unwrap();
    let expected_filename = "00000000000000999999.00000000000001000000.compacted.json";
    assert!(path.to_string().ends_with(expected_filename));
}

#[test]
fn test_list_commit_files_empty() {
    // Test list_commit_files when no files exist
    let table_root = Url::parse("file:///tmp/test-empty-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 0, 5, 0).unwrap();

    let engine = SyncEngine::new();
    let result = writer.list_commit_files(&engine);

    // Should return Ok with empty vector when no files exist
    assert!(result.is_ok());
    let files = result.unwrap();
    assert_eq!(files.len(), 0);
}

#[test]
fn test_create_log_segment_empty() {
    // Test create_log_segment with empty commit files
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 0, 5, 0).unwrap();

    let commit_files = vec![];
    let result = writer.create_log_segment(commit_files);

    // LogSegment doesn't allow empty files, so this should error
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("No files in log segment"));
}

#[test]
fn test_create_log_segment_with_files() {
    // Test create_log_segment with actual commit files
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 1, 3, 0).unwrap();

    // Create mock commit files
    let mut commit_files = vec![];
    for version in 1..=3 {
        let location = table_root
            .join(&format!("_delta_log/{:020}.json", version))
            .unwrap();
        let file_meta = FileMeta {
            location: location.clone(),
            last_modified: 1000,
            size: 100,
        };
        let parsed = ParsedLogPath::try_from(file_meta).unwrap().unwrap();
        commit_files.push(parsed);
    }

    let result = writer.create_log_segment(commit_files);
    assert!(result.is_ok());
    let segment = result.unwrap();
    assert_eq!(segment.end_version, 3);
}

#[test]
fn test_compaction_data_missing_files() {
    // Test compaction_data when expected files are missing
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let mut writer = LogCompactionWriter::new(table_root.clone(), 10, 20, 0).unwrap();

    let engine = SyncEngine::new();
    let result = writer.compaction_data(&engine);

    // Should error when commit files are missing
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Expected 11 commit files"));
}

#[test]
fn test_finalize() {
    // Test the finalize method
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 0, 5, 0).unwrap();

    let engine = SyncEngine::new();
    let metadata = FileMeta {
        location: table_root
            .join("_delta_log/00000000000000000000.00000000000000000005.compacted.json")
            .unwrap(),
        last_modified: 1000,
        size: 500,
    };

    let data_iterator = LogCompactionDataIterator {
        batches: vec![].into_iter(),
        total_actions: 10,
        total_add_actions: 5,
    };

    let result = writer.finalize(&engine, &metadata, data_iterator);
    assert!(result.is_ok());
}

// Mock EngineData implementation for testing
struct MockEngineData {
    _value: i32,
}

impl EngineData for MockEngineData {
    fn visit_rows(
        &self,
        _column_names: &[crate::schema::ColumnName],
        _visitor: &mut dyn crate::engine_data::RowVisitor,
    ) -> DeltaResult<()> {
        Ok(())
    }

    fn len(&self) -> usize {
        1
    }
}

#[test]
fn test_log_compaction_data_iterator_with_data() {
    // Test iterator with actual data batches
    let batch1: Box<dyn EngineData> = Box::new(MockEngineData { _value: 1 });
    let batch2: Box<dyn EngineData> = Box::new(MockEngineData { _value: 2 });
    let batch3: Box<dyn EngineData> = Box::new(MockEngineData { _value: 3 });

    let iterator = LogCompactionDataIterator {
        batches: vec![batch1, batch2, batch3].into_iter(),
        total_actions: 100,
        total_add_actions: 50,
    };

    // Verify we can iterate through all batches
    let mut count = 0;
    for result in iterator {
        assert!(result.is_ok());
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn test_compaction_writer_methods_coverage() {
    // Additional test to ensure all public methods are covered
    let table_root = Url::parse("file:///tmp/test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 5, 10, 1000).unwrap();

    // Test compaction_path returns expected format
    let path = writer.compaction_path().unwrap();
    assert!(path
        .to_string()
        .contains("00000000000000000005.00000000000000000010.compacted.json"));

    // Create another writer to test validation
    let writer2 = LogCompactionWriter::new(table_root.clone(), 0, 0, 0).unwrap();
    let path2 = writer2.compaction_path().unwrap();
    assert!(path2
        .to_string()
        .contains("00000000000000000000.00000000000000000000.compacted.json"));
}
