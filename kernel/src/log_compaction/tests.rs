use super::writer::{should_compact, LogCompactionDataIterator, LogCompactionWriter};
use super::COMPACTION_ACTIONS_SCHEMA;
use crate::checkpoint::log_replay::CheckpointBatch;
use crate::engine_data::FilteredEngineData;
use crate::path::ParsedLogPath;
use crate::{DeltaResult, EngineData, FileMeta, Version};
use std::sync::Arc;
use url::Url;

// Helper to create a  LogCompactionDataIterator with mock data
fn create_test_iterator<I>(
    iter: I,
    initial_actions_count: i64,
    initial_add_actions_count: i64,
) -> LogCompactionDataIterator
where
    I: Iterator<Item = DeltaResult<CheckpointBatch>> + Send + 'static,
{
    LogCompactionDataIterator {
        compaction_batch_iterator: Box::new(iter),
        actions_count: initial_actions_count,
        add_actions_count: initial_add_actions_count,
    }
}

// Helper to create a mock CheckpointBatch from EngineData
fn create_mock_batch(
    data: Box<dyn EngineData>,
    actions_count: i64,
    add_actions_count: i64,
) -> DeltaResult<CheckpointBatch> {
    Ok(CheckpointBatch {
        filtered_data: FilteredEngineData {
            data,
            selection_vector: vec![true],
        },
        actions_count,
        add_actions_count,
    })
}

#[test]
fn test_log_compaction_writer_creation() {
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    let mut iterator = create_test_iterator(std::iter::empty(), 0, 0);

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
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    let iterator = create_test_iterator(std::iter::empty(), 100, 50);

    assert_eq!(iterator.total_actions(), 100);
    assert_eq!(iterator.total_add_actions(), 50);
}

#[test]
fn test_large_version_numbers() {
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    // Test that we can create a writer and it has the expected methods
    // We don't actually call list_commit_files since it would require
    // a real filesystem or mock engine
    let table_root = Url::parse("memory:///test-empty-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 0, 5, 0).unwrap();

    // Just verify the writer was created successfully
    assert!(writer.compaction_path().is_ok());
}

#[test]
fn test_create_log_segment_empty() {
    // Test create_log_segment with empty commit files
    let table_root = Url::parse("memory:///test-table").unwrap();
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
    let table_root = Url::parse("memory:///test-table").unwrap();
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

    let mock_batches = vec![
        create_mock_batch(batch1, 30, 15),
        create_mock_batch(batch2, 30, 15),
        create_mock_batch(batch3, 40, 20),
    ];
    let iterator = create_test_iterator(mock_batches.into_iter(), 0, 0);

    // Verify we can iterate through all batches
    let mut count = 0;
    for result in iterator {
        assert!(result.is_ok());
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn test_compaction_writer() {
    // Additional test to ensure all public methods are covered
    let table_root = Url::parse("memory:///test-table").unwrap();
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

#[test]
fn test_compaction_actions_schema_access() {
    // Test that COMPACTION_ACTIONS_SCHEMA can be accessed and is properly initialized
    let schema = &*COMPACTION_ACTIONS_SCHEMA;

    // Verify expected fields are present
    let field_names: Vec<String> = schema.fields().map(|f| f.name().to_string()).collect();
    assert!(!field_names.is_empty());
    assert!(field_names.contains(&"add".to_string()));
    assert!(field_names.contains(&"remove".to_string()));
    assert!(field_names.contains(&"metaData".to_string()));
    assert!(field_names.contains(&"protocol".to_string()));
    assert!(field_names.contains(&"domainMetadata".to_string()));
    assert!(field_names.contains(&"txn".to_string()));
}

#[test]
fn test_log_compaction_data_iterator_debug() {
    // Test Debug implementation for LogCompactionDataIterator
    let iterator = create_test_iterator(std::iter::empty(), 100, 50);

    let debug_str = format!("{:?}", iterator);
    assert!(debug_str.contains("LogCompactionDataIterator"));
    assert!(debug_str.contains("actions_count: 100"));
    assert!(debug_str.contains("add_actions_count: 50"));
}

#[test]
fn test_finalize() {
    // Test finalize method with actual parameters
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 1, 5, 1000).unwrap();

    // Create mock metadata
    let metadata = FileMeta {
        location: table_root
            .join("_delta_log/00000000000000000001.00000000000000000005.compacted.json")
            .unwrap(),
        last_modified: 1234567890,
        size: 5000,
    };

    // Create mock iterator
    let iterator = create_test_iterator(std::iter::empty(), 100, 50);

    // Mock engine - we can't use real engine in first commit
    // but we can test that finalize accepts the parameters
    // The actual finalize just returns Ok(()) for now
    struct MockEngine;
    impl crate::Engine for MockEngine {
        fn evaluation_handler(&self) -> Arc<dyn crate::EvaluationHandler> {
            unimplemented!()
        }
        fn storage_handler(&self) -> Arc<dyn crate::StorageHandler> {
            unimplemented!()
        }
        fn parquet_handler(&self) -> Arc<dyn crate::ParquetHandler> {
            unimplemented!()
        }
        fn json_handler(&self) -> Arc<dyn crate::JsonHandler> {
            unimplemented!()
        }
    }

    let engine = MockEngine;
    let result = writer.finalize(&engine, &metadata, iterator);
    assert!(result.is_ok());
}

#[test]
fn test_writer_debug_impl() {
    // Test Debug implementation for LogCompactionWriter
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 5, 10, 1000).unwrap();

    let debug_str = format!("{:?}", writer);
    assert!(debug_str.contains("LogCompactionWriter"));
    assert!(debug_str.contains("start_version: 5"));
    assert!(debug_str.contains("end_version: 10"));
}

#[test]
fn test_compaction_data_iterator_consumption() {
    // Test that iterator can be consumed properly
    let batch1: Box<dyn EngineData> = Box::new(MockEngineData { _value: 1 });
    let batch2: Box<dyn EngineData> = Box::new(MockEngineData { _value: 2 });

    let mock_batches = vec![
        create_mock_batch(batch1, 50, 25),
        create_mock_batch(batch2, 0, 0),
    ];
    let mut iterator = create_test_iterator(mock_batches.into_iter(), 0, 0);

    // Consume first batch
    assert!(iterator.next().is_some());
    assert_eq!(iterator.total_actions(), 50);
    assert_eq!(iterator.total_add_actions(), 25);

    // Consume second batch
    assert!(iterator.next().is_some());

    // Iterator should be exhausted
    assert!(iterator.next().is_none());
}

#[test]
fn test_should_compact_edge_cases() {
    // Test more edge cases for should_compact
    assert!(should_compact(999, 1000)); // (999 + 1) % 1000 == 0
    assert!(!should_compact(1000, 1000)); // (1000 + 1) % 1000 != 0
    assert!(should_compact(1999, 1000)); // (1999 + 1) % 1000 == 0

    // Test with interval of 1 - would compact every version after 0
    assert!(should_compact(1, 1)); // commit_version > 0 && (1 + 1) % 1 == 0 -> true
    assert!(should_compact(2, 1)); // commit_version > 0 && (2 + 1) % 1 == 0 -> true
    assert!(should_compact(100, 1)); // commit_version > 0 && (100 + 1) % 1 == 0 -> true
}

#[test]
fn test_parsed_log_path_filtering() {
    // Test the file filtering logic in list_commit_files
    let table_root = Url::parse("memory:///test-table").unwrap();
    let log_root = table_root.join("_delta_log/").unwrap();

    // Create test file metadata for various file types
    let test_files = vec![
        // Commit files in range
        FileMeta {
            location: log_root.join("00000000000000000005.json").unwrap(),
            last_modified: 1000,
            size: 100,
        },
        FileMeta {
            location: log_root.join("00000000000000000006.json").unwrap(),
            last_modified: 1000,
            size: 100,
        },
        // Commit file outside range
        FileMeta {
            location: log_root.join("00000000000000000020.json").unwrap(),
            last_modified: 1000,
            size: 100,
        },
        // Checkpoint file (should be ignored)
        FileMeta {
            location: log_root
                .join("00000000000000000010.checkpoint.parquet")
                .unwrap(),
            last_modified: 1000,
            size: 100,
        },
        // Compaction file (should be ignored)
        FileMeta {
            location: log_root
                .join("00000000000000000005.00000000000000000010.compacted.json")
                .unwrap(),
            last_modified: 1000,
            size: 100,
        },
    ];

    // Parse and filter files
    let mut commit_files = Vec::new();
    let start_version = 5;
    let end_version = 10;

    for file in test_files {
        if let Ok(Some(parsed_path)) = ParsedLogPath::try_from(file) {
            if parsed_path.is_commit()
                && parsed_path.version >= start_version
                && parsed_path.version <= end_version
            {
                commit_files.push(parsed_path);
            }
        }
    }

    // Should only have 2 commit files in range
    assert_eq!(commit_files.len(), 2);
    assert_eq!(commit_files[0].version, 5);
    assert_eq!(commit_files[1].version, 6);
}

#[test]
fn test_writer_getters() {
    // Test that all getter methods work correctly
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(
        table_root.clone(),
        42,
        100,
        7 * 24 * 60 * 60 * 1000, // 7 days
    )
    .unwrap();

    // Test compaction_path getter
    let path = writer.compaction_path().unwrap();
    assert!(path
        .to_string()
        .contains("00000000000000000042.00000000000000000100.compacted.json"));
}

#[test]
fn test_zero_versions() {
    // Test with version 0 which is valid
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 0, 0, 0);
    assert!(writer.is_ok());

    let writer = writer.unwrap();
    let path = writer.compaction_path().unwrap();
    assert!(path
        .to_string()
        .contains("00000000000000000000.00000000000000000000.compacted.json"));
}

#[test]
fn test_very_large_retention_millis() {
    // Test with very large retention timestamp
    let table_root = Url::parse("memory:///test-table").unwrap();
    let retention_millis = i64::MAX;

    let writer = LogCompactionWriter::new(table_root.clone(), 1, 10, retention_millis);

    assert!(writer.is_ok());
    let writer = writer.unwrap();
    assert!(writer.compaction_path().is_ok());
}

#[test]
fn test_negative_retention_millis() {
    // Test with negative retention (which might mean no retention)
    let table_root = Url::parse("memory:///test-table").unwrap();
    let retention_millis = -1;

    let writer = LogCompactionWriter::new(table_root.clone(), 1, 10, retention_millis);

    assert!(writer.is_ok());
}

#[test]
fn test_multiple_version_ranges() {
    // Test various version range sizes
    let table_root = Url::parse("memory:///test-table").unwrap();

    // Single version range
    let single = LogCompactionWriter::new(table_root.clone(), 5, 5, 0).unwrap();
    assert!(single
        .compaction_path()
        .unwrap()
        .to_string()
        .contains("00000000000000000005.00000000000000000005"));

    // Small range
    let small = LogCompactionWriter::new(table_root.clone(), 0, 9, 0).unwrap();
    assert!(small
        .compaction_path()
        .unwrap()
        .to_string()
        .contains("00000000000000000000.00000000000000000009"));

    // Large range
    let large = LogCompactionWriter::new(table_root.clone(), 100, 999, 0).unwrap();
    assert!(large
        .compaction_path()
        .unwrap()
        .to_string()
        .contains("00000000000000000100.00000000000000000999"));

    // Very large version numbers
    let huge = LogCompactionWriter::new(table_root.clone(), 1000000, 1000010, 0).unwrap();
    assert!(huge
        .compaction_path()
        .unwrap()
        .to_string()
        .contains("00000000000001000000.00000000000001000010"));
}

#[test]
fn test_boundary_conditions() {
    let table_root = Url::parse("memory:///test-table").unwrap();

    // Test with maximum safe version number
    let max_safe = (i64::MAX / 2) as Version;
    let writer = LogCompactionWriter::new(table_root.clone(), max_safe - 10, max_safe, 0);
    assert!(writer.is_ok());
}

#[test]
fn test_path_validation() {
    // Test various URL schemes
    let memory_url = Url::parse("memory:///test").unwrap();
    let writer1 = LogCompactionWriter::new(memory_url, 0, 5, 0);
    assert!(writer1.is_ok());

    // Test with file URL
    let file_url = Url::parse("file:///tmp/test").unwrap();
    let writer2 = LogCompactionWriter::new(file_url, 0, 5, 0);
    assert!(writer2.is_ok());

    // Test path component construction
    let http_url = Url::parse("http://example.com/delta").unwrap();
    let writer3 = LogCompactionWriter::new(http_url, 0, 5, 0);
    assert!(writer3.is_ok());
}

#[test]
fn test_create_log_segment_various_sizes() {
    let table_root = Url::parse("memory:///test-table").unwrap();

    // Test with single file
    let writer = LogCompactionWriter::new(table_root.clone(), 10, 10, 0).unwrap();
    let single_file = vec![ParsedLogPath::try_from(FileMeta {
        location: table_root
            .join("_delta_log/00000000000000000010.json")
            .unwrap(),
        last_modified: 1000,
        size: 100,
    })
    .unwrap()
    .unwrap()];
    let result = writer.create_log_segment(single_file);
    assert!(result.is_ok());

    // Test with many files
    let writer2 = LogCompactionWriter::new(table_root.clone(), 0, 99, 0).unwrap();
    let mut many_files = vec![];
    for i in 0..100 {
        let location = table_root
            .join(&format!("_delta_log/{:020}.json", i))
            .unwrap();
        let file_meta = FileMeta {
            location,
            last_modified: 1000,
            size: 100,
        };
        if let Ok(Some(parsed)) = ParsedLogPath::try_from(file_meta) {
            many_files.push(parsed);
        }
    }
    let result2 = writer2.create_log_segment(many_files);
    assert!(result2.is_ok());
}

#[test]
fn test_create_log_segment_sorting() {
    // Test that create_log_segment properly sorts commit files
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 1, 5, 0).unwrap();

    // Create commit files in reverse order
    let mut commit_files = vec![];
    for version in (1..=5).rev() {
        let location = table_root
            .join(&format!("_delta_log/{:020}.json", version))
            .unwrap();
        let file_meta = FileMeta {
            location,
            last_modified: 1000,
            size: 100,
        };
        if let Ok(Some(parsed)) = ParsedLogPath::try_from(file_meta) {
            commit_files.push(parsed);
        }
    }

    // Files should be unsorted initially
    assert_eq!(commit_files[0].version, 5);

    let result = writer.create_log_segment(commit_files);
    assert!(result.is_ok());

    let segment = result.unwrap();
    // Segment should have the correct end version
    assert_eq!(segment.end_version, 5);

    // Files in segment should be sorted ascending
    for i in 1..segment.ascending_commit_files.len() {
        assert!(
            segment.ascending_commit_files[i].version
                >= segment.ascending_commit_files[i - 1].version
        );
    }
}

#[test]
fn test_compaction_actions_schema_fields() {
    // More thorough test of the schema
    let schema = &*COMPACTION_ACTIONS_SCHEMA;

    // Check all expected fields are present and nullable
    let fields: Vec<_> = schema.fields().collect();
    for field in &fields {
        assert!(field.nullable, "Field {} should be nullable", field.name());

        // Verify field types match expected action types
        let name = field.name();
        match name.as_str() {
            "add" | "remove" | "metaData" | "protocol" | "domainMetadata" | "txn" => {}
            _ => panic!("Unexpected field in compaction schema: {}", name),
        }
    }

    // Ensure we have exactly 6 fields
    assert_eq!(fields.len(), 6, "Should have exactly 6 action types");
}

#[test]
fn test_log_compaction_writer_fields() {
    // Test that we can access internal fields through Debug
    let table_root = Url::parse("memory:///test-table").unwrap();
    let writer = LogCompactionWriter::new(table_root.clone(), 123, 456, 7890).unwrap();

    let debug_str = format!("{:?}", writer);
    assert!(debug_str.contains("123"));
    assert!(debug_str.contains("456"));
    assert!(debug_str.contains("7890"));
}

#[test]
fn test_iterator_empty() {
    // Test behavior with empty iterator
    let mut empty_iter = create_test_iterator(std::iter::empty(), 0, 0);

    assert_eq!(empty_iter.total_actions(), 0);
    assert_eq!(empty_iter.total_add_actions(), 0);
    assert!(empty_iter.next().is_none());
    assert!(empty_iter.next().is_none()); // Should remain None
}

#[test]
fn test_should_compact_comprehensive() {
    // Comprehensive tests for should_compact

    // Test intervals of different sizes
    for interval in [1, 2, 5, 10, 50, 100, 1000] {
        // Version 0 should never trigger compaction
        assert!(!should_compact(0, interval));

        // Test versions that should trigger compaction
        for multiplier in 1..5 {
            let version = interval * multiplier - 1;
            if version > 0 {
                assert!(
                    should_compact(version, interval),
                    "Version {} should trigger compaction with interval {}",
                    version,
                    interval
                );
            }
        }

        // Test versions that should not trigger compaction
        for offset in 1..interval.min(10) {
            let version = interval + offset;
            if (version + 1) % interval != 0 {
                assert!(
                    !should_compact(version, interval),
                    "Version {} should not trigger compaction with interval {}",
                    version,
                    interval
                );
            }
        }
    }
}

#[test]
fn test_retention_timestamp_edge_cases() {
    let table_root = Url::parse("memory:///test-table").unwrap();

    // Test with 0 retention
    let zero_retention = LogCompactionWriter::new(table_root.clone(), 1, 5, 0).unwrap();
    assert!(zero_retention.compaction_path().is_ok());

    // Test with common retention periods
    let hour_millis = 60 * 60 * 1000;
    let day_millis = 24 * hour_millis;
    let week_millis = 7 * day_millis;
    let month_millis = 30 * day_millis;

    for retention in [hour_millis, day_millis, week_millis, month_millis] {
        let writer = LogCompactionWriter::new(table_root.clone(), 1, 5, retention).unwrap();
        assert!(writer.compaction_path().is_ok());
    }
}

#[test]
fn test_version_range_calculations() {
    let table_root = Url::parse("memory:///test-table").unwrap();

    // Test different range sizes
    let test_cases = vec![
        (0, 0, 1),          // Single version
        (0, 1, 2),          // Two versions
        (5, 10, 6),         // Six versions
        (100, 199, 100),    // 100 versions
        (1000, 1999, 1000), // 1000 versions
    ];

    for (start, end, expected_count) in test_cases {
        let writer = LogCompactionWriter::new(table_root.clone(), start, end, 0).unwrap();
        let actual_count = (end - start + 1) as usize;
        assert_eq!(
            actual_count, expected_count,
            "Version range [{}, {}] should have {} files",
            start, end, expected_count
        );
        assert!(writer.compaction_path().is_ok());
    }
}

#[test]
fn test_url_joining() {
    // Test URL joining for different schemes
    let test_urls = vec![
        "memory:///table",
        "file:///data/table",
        "s3://bucket/prefix/table",
        "hdfs://namenode:9000/path/table",
    ];

    for url_str in test_urls {
        let url = Url::parse(url_str).unwrap();
        let writer = LogCompactionWriter::new(url, 0, 10, 0).unwrap();
        let path = writer.compaction_path().unwrap();
        assert!(path.to_string().contains("_delta_log"));
        assert!(path.to_string().contains(".compacted.json"));
    }
}
