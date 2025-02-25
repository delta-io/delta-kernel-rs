use std::{path::PathBuf, sync::Arc};

use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use url::Url;

use crate::actions::{get_log_add_schema, PROTOCOL_NAME};
use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::filesystem::ObjectStoreFileSystemClient;
use crate::engine::sync::SyncEngine;
use crate::log_segment::LogSegment;
use crate::scan::test_utils::add_batch_simple;
use crate::snapshot::CheckpointMetadata;
use crate::utils::test_utils::MockEngineContext;
use crate::{EngineData, Expression, FileMeta, FileSystemClient, Table};
use test_utils::delta_path_for_version;

fn create_file_meta(path: &str) -> FileMeta {
    FileMeta {
        location: Url::parse(path).expect("Invalid file URL"),
        last_modified: 0,
        size: 0,
    }
}

#[test]
fn test_log_replay() {
    // Retrieve the engine context
    let engine_context = MockEngineContext::new();

    // Define checkpoint and commit files
    let checkpoint_meta = create_file_meta("file:///00000000000000000001.checkpoint1.json");
    let checkpoint_parts = vec![checkpoint_meta.to_parsed_log_path()];
    let commit_files = [
        "file:///00000000000000000001.version1.json",
        "file:///00000000000000000002.version2.json",
    ]
    .into_iter()
    .map(|path| create_file_meta(path).to_parsed_log_path())
    .collect::<Vec<_>>();

    // Define log segment
    let log_segment = LogSegment {
        end_version: 0,
        log_root: Url::parse("file:///root.json").expect("Invalid log root URL"),
        ascending_commit_files: commit_files.clone(),
        checkpoint_parts,
    };

    // Expected commit files should be read in reverse order
    let expected_commit_files_read = [
        "file:///00000000000000000002.version2.json",
        "file:///00000000000000000001.version1.json",
    ]
    .into_iter()
    .map(create_file_meta)
    .collect::<Vec<_>>();

    // Define predicate and projected schemas
    let predicate = Some(Arc::new(
        Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
    ));
    let commit_schema = get_log_add_schema().clone();
    let checkpoint_schema = get_log_add_schema().clone();

    // Expect the JSON and Parquet handlers to receive the correct files, schemas, and predicates
    engine_context.json_handler.expect_read_json_files(
        expected_commit_files_read.clone(),
        commit_schema.clone(),
        predicate.clone(),
        Ok(Box::new(std::iter::once(Ok(
            add_batch_simple() as Box<dyn EngineData>
        )))),
    );
    engine_context.parquet_handler.expect_read_parquet_files(
        vec![checkpoint_meta],
        checkpoint_schema.clone(),
        predicate.clone(),
        Ok(Box::new(std::iter::once(Ok(
            add_batch_simple() as Box<dyn EngineData>
        )))),
    );

    // Run the log replay
    let mut iter = log_segment
        .replay(
            &engine_context.engine,
            commit_schema,
            checkpoint_schema,
            predicate,
        )
        .unwrap()
        .into_iter();

    // Verify the commit batch and checkpoint batch are chained together in order
    assert!(
        iter.next().unwrap().unwrap().1,
        "First element should be a commit batch"
    );
    assert!(
        !iter.next().unwrap().unwrap().1,
        "Second element should be a checkpoint batch"
    );
    assert!(iter.next().is_none(), "Iterator should only have 2 batches");
}

// NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
// that the parquet reader properly infers nullcount = rowcount for missing columns. The two
// checkpoint part files that contain transaction app ids have truncated schemas that would
// otherwise fail skipping due to their missing nullcount stat:
//
// Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
// --------------------------------------------------------------------------------
//              type    nulls  min / max
// txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
// txn.version  INT64   0      "4390" / "4390"
#[test]
fn test_replay_for_metadata() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None).unwrap();
    let data: Vec<_> = snapshot
        .log_segment()
        .replay_for_metadata(&engine)
        .unwrap()
        .try_collect()
        .unwrap();

    // The checkpoint has five parts, each containing one action:
    // 1. txn (physically missing P&M columns)
    // 2. metaData
    // 3. protocol
    // 4. add
    // 5. txn (physically missing P&M columns)
    //
    // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
    // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
    //
    // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
    //
    // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
    // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
    // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
    assert_eq!(data.len(), 4);
}

// get an ObjectStore path for a checkpoint file, based on version, part number, and total number of parts
fn delta_path_for_multipart_checkpoint(version: u64, part_num: u32, num_parts: u32) -> Path {
    let path =
        format!("_delta_log/{version:020}.checkpoint.{part_num:010}.{num_parts:010}.parquet");
    Path::from(path.as_str())
}

// Utility method to build a log using a list of log paths and an optional checkpoint hint. The
// CheckpointMetadata is written to `_delta_log/_last_checkpoint`.
fn build_log_with_paths_and_checkpoint(
    paths: &[Path],
    checkpoint_metadata: Option<&CheckpointMetadata>,
) -> (Box<dyn FileSystemClient>, Url) {
    let store = Arc::new(InMemory::new());

    let data = bytes::Bytes::from("kernel-data");

    // add log files to store
    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async {
            for path in paths {
                store
                    .put(path, data.clone().into())
                    .await
                    .expect("put log file in store");
            }
            if let Some(checkpoint_metadata) = checkpoint_metadata {
                let checkpoint_str =
                    serde_json::to_string(checkpoint_metadata).expect("Serialize checkpoint");
                store
                    .put(
                        &Path::from("_delta_log/_last_checkpoint"),
                        checkpoint_str.into(),
                    )
                    .await
                    .expect("Write _last_checkpoint");
            }
        });

    let client = ObjectStoreFileSystemClient::new(
        store,
        false, // don't have ordered listing
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table_root = Url::parse("memory:///").expect("valid url");
    let log_root = table_root.join("_delta_log/").unwrap();
    (Box::new(client), log_root)
}

#[test]
fn build_snapshot_with_unsupported_uuid_checkpoint() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_multiple_incomplete_multipart_checkpoints() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_multipart_checkpoint(1, 1, 3),
            // Part 2 of 3 at version 1 is missing!
            delta_path_for_multipart_checkpoint(1, 3, 3),
            delta_path_for_multipart_checkpoint(2, 1, 2),
            // Part 2 of 2 at version 2 is missing!
            delta_path_for_version(2, "json"),
            delta_path_for_multipart_checkpoint(3, 1, 3),
            // Part 2 of 3 at version 3 is missing!
            delta_path_for_multipart_checkpoint(3, 3, 3),
            delta_path_for_multipart_checkpoint(3, 1, 4),
            delta_path_for_multipart_checkpoint(3, 2, 4),
            delta_path_for_multipart_checkpoint(3, 3, 4),
            delta_path_for_multipart_checkpoint(3, 4, 4),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 4);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_out_of_date_last_checkpoint() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}
#[test]
fn build_snapshot_with_correct_last_multipart_checkpoint() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            delta_path_for_multipart_checkpoint(5, 2, 3),
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 3);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}

#[test]
fn build_snapshot_with_missing_checkpoint_part_from_hint_fails() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None);
    assert!(log_segment.is_err())
}
#[test]
fn build_snapshot_with_bad_checkpoint_hint_fails() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(1),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 2),
            delta_path_for_multipart_checkpoint(5, 2, 2),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None);
    assert!(log_segment.is_err())
}

#[test]
fn build_snapshot_with_missing_checkpoint_part_no_hint() {
    // Part 2 of 3 is missing from checkpoint 5. The Snapshot should be made of checkpoint
    // number 3 and commit files 4 to 7.
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();

    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_out_of_date_last_checkpoint_and_incomplete_recent_checkpoint() {
    // When the _last_checkpoint is out of date and the most recent checkpoint is incomplete, the
    // Snapshot should be made of the most recent complete checkpoint and the commit files that
    // follow it.
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_without_checkpoints() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    ///////// Specify no checkpoint or end version /////////
    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root.clone(), None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    // All commit files should still be there
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);

    ///////// Specify  only end version /////////
    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, Some(2)).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 1);

    // All commit files should still be there
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![2];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_checkpoint_greater_than_time_travel_version() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, Some(4)).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 4);
}

#[test]
fn build_snapshot_with_start_checkpoint_and_time_travel_version() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, Some(4)).unwrap();

    assert_eq!(log_segment.checkpoint_parts[0].version, 3);
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    assert_eq!(log_segment.ascending_commit_files[0].version, 4);
}
#[test]
fn build_table_changes_with_commit_versions() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    ///////// Specify start version and end version /////////

    let log_segment =
        LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 2, 5).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // Commits between 2 and 5 (inclusive) should be returned
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = (2..=5).collect_vec();
    assert_eq!(versions, expected_versions);

    ///////// Start version and end version are the same /////////
    let log_segment =
        LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 0, Some(0)).unwrap();

    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;
    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // There should only be commit version 0
    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 0);

    ///////// Specify no start or end version /////////
    let log_segment = LogSegment::for_table_changes(client.as_ref(), log_root, 0, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // Commits between 2 and 7 (inclusive) should be returned
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = (0..=7).collect_vec();
    assert_eq!(versions, expected_versions);
}

#[test]
fn test_non_contiguous_log() {
    // Commit with version 1 is missing
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(2, "json"),
        ],
        None,
    );

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 0, None);
    assert!(log_segment_res.is_err());

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 1, None);
    assert!(log_segment_res.is_err());

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root, 0, Some(1));
    assert!(log_segment_res.is_err());
}

#[test]
fn table_changes_fails_with_larger_start_version_than_end() {
    // Commit with version 1 is missing
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
        ],
        None,
    );
    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root, 1, Some(0));
    assert!(log_segment_res.is_err());
}
