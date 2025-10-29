use std::sync::LazyLock;
use std::{path::PathBuf, sync::Arc};

use futures::executor::block_on;
use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use test_log::test;
use url::Url;

use crate::actions::visitors::AddVisitor;
use crate::actions::{
    get_all_actions_schema, get_commit_schema, Add, Sidecar, ADD_NAME, METADATA_NAME, REMOVE_NAME,
    SIDECAR_NAME,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::engine::default::DefaultEngine;
use crate::engine::sync::SyncEngine;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_replay::ActionsBatch;
use crate::log_segment::{ListedLogFiles, LogSegment};
use crate::parquet::arrow::ArrowWriter;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::scan::test_utils::{
    add_batch_simple, add_batch_with_remove, sidecar_batch_with_given_paths,
};
use crate::utils::test_utils::{assert_batch_matches, assert_result_error_with_message, Action};
use crate::{
    DeltaResult, Engine as _, EngineData, Expression, FileMeta, PredicateRef, RowVisitor, Snapshot,
    StorageHandler,
};
use test_utils::{compacted_log_path_for_versions, delta_path_for_version};

use super::*;

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

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
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
// LastCheckpointHint is written to `_delta_log/_last_checkpoint`.
fn build_log_with_paths_and_checkpoint(
    paths: &[Path],
    checkpoint_metadata: Option<&LastCheckpointHint>,
) -> (Box<dyn StorageHandler>, Url) {
    let store = Arc::new(InMemory::new());

    let data = bytes::Bytes::from("kernel-data");

    // add log files to store
    block_on(async {
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

    let storage = ObjectStoreStorageHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

    let table_root = Url::parse("memory:///").expect("valid url");
    let log_root = table_root.join("_delta_log/").unwrap();
    (Box::new(storage), log_root)
}

// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (
        Arc::new(InMemory::new()),
        Url::parse("memory:///")
            .unwrap()
            .join("_delta_log/")
            .unwrap(),
    )
}

// Writes a record batch obtained from engine data to the in-memory store at a given path.
fn write_parquet_to_store(
    store: &Arc<InMemory>,
    path: String,
    data: Box<dyn EngineData>,
) -> DeltaResult<()> {
    let batch = ArrowEngineData::try_from_engine_data(data)?;
    let record_batch = batch.record_batch();

    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
    writer.write(record_batch)?;
    writer.close()?;

    block_on(async { store.put(&Path::from(path), buffer.into()).await })?;

    Ok(())
}

/// Writes all actions to a _delta_log parquet checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
pub(crate) fn add_checkpoint_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    let path = format!("_delta_log/{filename}");
    write_parquet_to_store(store, path, data)
}

/// Writes all actions to a _delta_log/_sidecars file in the store.
/// This function formats the provided filename into the _sidecars subdirectory.
fn add_sidecar_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    let path = format!("_delta_log/_sidecars/{filename}");
    write_parquet_to_store(store, path, data)
}

/// Writes all actions to a _delta_log json checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
fn write_json_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    filename: &str,
) -> DeltaResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");
    let checkpoint_path = format!("_delta_log/{filename}");

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async {
            store
                .put(&Path::from(checkpoint_path), content.into())
                .await
        })?;

    Ok(())
}

fn create_log_path(path: &str) -> ParsedLogPath<FileMeta> {
    ParsedLogPath::try_from(FileMeta {
        location: Url::parse(path).expect("Invalid file URL"),
        last_modified: 0,
        size: 0,
    })
    .unwrap()
    .unwrap()
}

#[test]
fn build_snapshot_with_uuid_checkpoint_parquet() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        None,
        None,
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_uuid_checkpoint_json() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        None,
        None,
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_correct_last_uuid_checkpoint() {
    let checkpoint_metadata = LastCheckpointHint {
        version: 5,
        size: 10,
        parts: Some(1),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}
#[test]
fn build_snapshot_with_multiple_incomplete_multipart_checkpoints() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        None,
        None,
    )
    .unwrap();
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
    let checkpoint_metadata = LastCheckpointHint {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    )
    .unwrap();
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
    let checkpoint_metadata = LastCheckpointHint {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    )
    .unwrap();
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
    let checkpoint_metadata = LastCheckpointHint {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    );
    assert_result_error_with_message(
        log_segment,
        "Invalid Checkpoint: Had a _last_checkpoint hint but didn't find any checkpoints",
    )
}
#[test]
fn build_snapshot_with_bad_checkpoint_hint_fails() {
    let checkpoint_metadata = LastCheckpointHint {
        version: 5,
        size: 10,
        parts: Some(1),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    );
    assert_result_error_with_message(
        log_segment,
        "Invalid Checkpoint: _last_checkpoint indicated that checkpoint should have 1 parts, but \
        it has 2",
    )
}

#[test]
fn build_snapshot_with_missing_checkpoint_part_no_hint() {
    // Part 2 of 3 is missing from checkpoint 5. The Snapshot should be made of checkpoint
    // number 3 and commit files 4 to 7.
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        None,
        None,
    )
    .unwrap();

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
    let checkpoint_metadata = LastCheckpointHint {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        None,
    )
    .unwrap();
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
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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
    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root.clone(),
        vec![], // log_tail
        None,
        None,
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    // All commit files should still be there
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);

    ///////// Specify  only end version /////////
    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        None,
        Some(2),
    )
    .unwrap();
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
    let checkpoint_metadata = LastCheckpointHint {
        version: 5,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        Some(4),
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 4);
}

#[test]
fn build_snapshot_with_start_checkpoint_and_time_travel_version() {
    let checkpoint_metadata = LastCheckpointHint {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root,
        vec![], // log_tail
        Some(checkpoint_metadata),
        Some(4),
    )
    .unwrap();

    assert_eq!(log_segment.checkpoint_parts[0].version, 3);
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    assert_eq!(log_segment.ascending_commit_files[0].version, 4);
}
#[test]
fn build_table_changes_with_commit_versions() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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
        LogSegment::for_table_changes(storage.as_ref(), log_root.clone(), 2, 5).unwrap();
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
        LogSegment::for_table_changes(storage.as_ref(), log_root.clone(), 0, Some(0)).unwrap();

    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;
    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // There should only be commit version 0
    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 0);

    ///////// Specify no start or end version /////////
    let log_segment = LogSegment::for_table_changes(storage.as_ref(), log_root, 0, None).unwrap();
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
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(2, "json"),
        ],
        None,
    );

    let log_segment_res =
        LogSegment::for_table_changes(storage.as_ref(), log_root.clone(), 0, None);
    // check the error message up to the timestamp
    let expected_error_pattern = "Generic delta kernel error: Expected ordered contiguous \
        commit files [ParsedLogPath { location: FileMeta { location: Url { scheme: \"memory\", \
        cannot_be_a_base: false, username: \"\", password: None, host: None, port: None, path: \
        \"/_delta_log/00000000000000000000.json\", query: None, fragment: None }, last_modified:";
    assert_result_error_with_message(log_segment_res, expected_error_pattern);

    let log_segment_res =
        LogSegment::for_table_changes(storage.as_ref(), log_root.clone(), 1, None);
    assert_result_error_with_message(
        log_segment_res,
        "Generic delta kernel error: Expected the first commit to have version 1",
    );

    let log_segment_res = LogSegment::for_table_changes(storage.as_ref(), log_root, 0, Some(1));
    assert_result_error_with_message(
        log_segment_res,
        "Generic delta kernel error: LogSegment end version 0 not the same as the specified end \
        version 1",
    );
}

#[test]
fn table_changes_fails_with_larger_start_version_than_end() {
    // Commit with version 1 is missing
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
        ],
        None,
    );
    let log_segment_res = LogSegment::for_table_changes(storage.as_ref(), log_root, 1, Some(0));
    assert_result_error_with_message(log_segment_res, "Generic delta kernel error: Failed to build LogSegment: start_version cannot be greater than end_version");
}
#[test]
fn test_sidecar_to_filemeta_valid_paths() -> DeltaResult<()> {
    let log_root = Url::parse("file:///var/_delta_log/")?;
    let test_cases = [
        (
            "example.parquet",
            "file:///var/_delta_log/_sidecars/example.parquet",
        ),
        (
            "file:///var/_delta_log/_sidecars/example.parquet",
            "file:///var/_delta_log/_sidecars/example.parquet",
        ),
        (
            "test/test/example.parquet",
            "file:///var/_delta_log/_sidecars/test/test/example.parquet",
        ),
    ];

    for (input_path, expected_url) in test_cases.into_iter() {
        let sidecar = Sidecar {
            path: expected_url.to_string(),
            modification_time: 0,
            size_in_bytes: 1000,
            tags: None,
        };

        let filemeta = sidecar.to_filemeta(&log_root)?;
        assert_eq!(
            filemeta.location.as_str(),
            expected_url,
            "Mismatch for input path: {input_path}"
        );
    }
    Ok(())
}

#[test]
fn test_checkpoint_batch_with_no_sidecars_returns_none() -> DeltaResult<()> {
    let (_, log_root) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new());
    let checkpoint_batch = add_batch_simple(get_all_actions_schema().clone());

    let mut iter = LogSegment::process_sidecars(
        engine.parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
        get_all_actions_schema().project(&[ADD_NAME, REMOVE_NAME, SIDECAR_NAME])?,
        None,
    )?
    .into_iter()
    .flatten();

    // Assert no batches are returned
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_checkpoint_batch_with_sidecars_returns_sidecar_batches() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
    let read_schema = get_all_actions_schema().project(&[ADD_NAME, REMOVE_NAME, SIDECAR_NAME])?;

    add_sidecar_to_store(
        &store,
        add_batch_simple(read_schema.clone()),
        "sidecarfile1.parquet",
    )?;
    add_sidecar_to_store(
        &store,
        add_batch_with_remove(read_schema.clone()),
        "sidecarfile2.parquet",
    )?;

    let checkpoint_batch = sidecar_batch_with_given_paths(
        vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
        read_schema.clone(),
    );

    let mut iter = LogSegment::process_sidecars(
        engine.parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
        read_schema.clone(),
        None,
    )?
    .into_iter()
    .flatten();

    // Assert the correctness of batches returned
    assert_batch_matches(iter.next().unwrap()?, add_batch_simple(read_schema.clone()));
    assert_batch_matches(iter.next().unwrap()?, add_batch_with_remove(read_schema));
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_checkpoint_batch_with_sidecar_files_that_do_not_exist() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    let checkpoint_batch = sidecar_batch_with_given_paths(
        vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
        get_all_actions_schema().clone(),
    );

    let mut iter = LogSegment::process_sidecars(
        engine.parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
        get_all_actions_schema().project(&[ADD_NAME, REMOVE_NAME, SIDECAR_NAME])?,
        None,
    )?
    .into_iter()
    .flatten();

    // Assert that an error is returned when trying to read sidecar files that do not exist
    let err = iter.next().unwrap();
    assert_result_error_with_message(err, "Arrow error: External: Object at location _delta_log/_sidecars/sidecarfile1.parquet not found: No data in memory found. Location: _delta_log/_sidecars/sidecarfile1.parquet");

    Ok(())
}

#[test]
fn test_reading_sidecar_files_with_predicate() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
    let read_schema = get_all_actions_schema().project(&[ADD_NAME, REMOVE_NAME, SIDECAR_NAME])?;

    let checkpoint_batch =
        sidecar_batch_with_given_paths(vec!["sidecarfile1.parquet"], read_schema.clone());

    // Add a sidecar file with only add actions
    add_sidecar_to_store(
        &store,
        add_batch_simple(read_schema.clone()),
        "sidecarfile1.parquet",
    )?;

    // Filter out sidecar files that do not contain remove actions
    let remove_predicate: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
        Some(Arc::new(
            Expression::column([REMOVE_NAME, "path"]).is_not_null(),
        ))
    });

    let mut iter = LogSegment::process_sidecars(
        engine.parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
        read_schema.clone(),
        remove_predicate.clone(),
    )?
    .into_iter()
    .flatten();

    // As the sidecar batch contains only add actions, the batch should be filtered out
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_returns_checkpoint_batches_as_is_if_schema_has_no_file_actions(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
    add_checkpoint_to_store(
        &store,
        // Create a checkpoint batch with sidecar actions to verify that the sidecar actions are not read.
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], get_commit_schema().clone()),
        "00000000000000000001.checkpoint.parquet",
    )?;

    let checkpoint_one_file = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_commit_schema().project(&[METADATA_NAME])?;

    let log_segment = LogSegment::try_new(
        ListedLogFiles::try_new(
            vec![],
            vec![],
            vec![create_log_path(&checkpoint_one_file)],
            None,
            Some(create_log_path("file:///00000000000000000001.json")),
        )?,
        log_root,
        None,
    )?;
    let mut iter =
        log_segment.create_checkpoint_stream(&engine, v2_checkpoint_read_schema.clone(), None)?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let ActionsBatch {
        actions: first_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        first_batch,
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], v2_checkpoint_read_schema),
    );
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_returns_checkpoint_batches_if_checkpoint_is_multi_part(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Multi-part checkpoints should never contain sidecar actions.
    // This test intentionally includes batches with sidecar actions in multi-part checkpoints
    // to verify that the reader does not process them. Instead, the reader should short-circuit
    // and return the checkpoint batches as-is when encountering a multi-part checkpoint.
    // Note: This is a test-only scenario; real tables should never have multi-part
    // checkpoints with sidecar actions.
    let checkpoint_part_1 = "00000000000000000001.checkpoint.0000000001.0000000002.parquet";
    let checkpoint_part_2 = "00000000000000000001.checkpoint.0000000002.0000000002.parquet";

    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], get_all_actions_schema().clone()),
        checkpoint_part_1,
    )?;
    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(vec!["sidecar2.parquet"], get_all_actions_schema().clone()),
        checkpoint_part_2,
    )?;

    let checkpoint_one_file = log_root.join(checkpoint_part_1)?.to_string();
    let checkpoint_two_file = log_root.join(checkpoint_part_2)?.to_string();

    let v2_checkpoint_read_schema = get_commit_schema().project(&[ADD_NAME])?;

    let log_segment = LogSegment::try_new(
        ListedLogFiles::try_new(
            vec![],
            vec![],
            vec![
                create_log_path(&checkpoint_one_file),
                create_log_path(&checkpoint_two_file),
            ],
            None,
            Some(create_log_path("file:///00000000000000000001.json")),
        )?,
        log_root,
        None,
    )?;
    let mut iter =
        log_segment.create_checkpoint_stream(&engine, v2_checkpoint_read_schema.clone(), None)?;

    // Assert the correctness of batches returned
    for expected_sidecar in ["sidecar1.parquet", "sidecar2.parquet"].iter() {
        let ActionsBatch {
            actions: batch,
            is_log_batch,
        } = iter.next().unwrap()?;
        assert!(!is_log_batch);
        assert_batch_matches(
            batch,
            sidecar_batch_with_given_paths(
                vec![expected_sidecar],
                v2_checkpoint_read_schema.clone(),
            ),
        );
    }
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_reads_parquet_checkpoint_batch_without_sidecars() -> DeltaResult<()>
{
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    add_checkpoint_to_store(
        &store,
        add_batch_simple(get_commit_schema().clone()),
        "00000000000000000001.checkpoint.parquet",
    )?;

    let checkpoint_one_file = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_all_actions_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let log_segment = LogSegment::try_new(
        ListedLogFiles::try_new(
            vec![],
            vec![],
            vec![create_log_path(&checkpoint_one_file)],
            None,
            Some(create_log_path("file:///00000000000000000001.json")),
        )?,
        log_root,
        None,
    )?;
    let mut iter =
        log_segment.create_checkpoint_stream(&engine, v2_checkpoint_read_schema.clone(), None)?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let ActionsBatch {
        actions: first_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(first_batch, add_batch_simple(v2_checkpoint_read_schema));
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_reads_json_checkpoint_batch_without_sidecars() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    let filename = "00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json";

    write_json_to_store(
        &store,
        vec![Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })],
        filename,
    )?;

    let checkpoint_one_file = log_root.join(filename)?.to_string();

    let v2_checkpoint_read_schema = get_all_actions_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let log_segment = LogSegment::try_new(
        ListedLogFiles::try_new(
            vec![],
            vec![],
            vec![create_log_path(&checkpoint_one_file)],
            None,
            Some(create_log_path("file:///00000000000000000001.json")),
        )?,
        log_root,
        None,
    )?;
    let mut iter =
        log_segment.create_checkpoint_stream(&engine, v2_checkpoint_read_schema, None)?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let ActionsBatch {
        actions: first_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    let mut visitor = AddVisitor::default();
    visitor.visit_rows_of(&*first_batch)?;
    assert!(visitor.adds.len() == 1);
    assert!(visitor.adds[0].path == "fake_path_1");

    assert!(iter.next().is_none());

    Ok(())
}

// Tests the end-to-end process of creating a checkpoint stream.
// Verifies that:
// - The checkpoint file is read and produces batches containing references to sidecar files.
// - As sidecar references are present, the corresponding sidecar files are processed correctly.
// - Batches from both the checkpoint file and sidecar files are returned.
// - Each returned batch is correctly flagged with is_log_batch set to false
#[test]
fn test_create_checkpoint_stream_reads_checkpoint_file_and_returns_sidecar_batches(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(
            vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
            get_all_actions_schema().clone(),
        ),
        "00000000000000000001.checkpoint.parquet",
    )?;

    add_sidecar_to_store(
        &store,
        add_batch_simple(get_commit_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile1.parquet",
    )?;
    add_sidecar_to_store(
        &store,
        add_batch_with_remove(get_commit_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile2.parquet",
    )?;

    let checkpoint_file_path = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_all_actions_schema().project(&[ADD_NAME])?;

    let log_segment = LogSegment::try_new(
        ListedLogFiles::try_new(
            vec![],
            vec![],
            vec![create_log_path(&checkpoint_file_path)],
            None,
            Some(create_log_path("file:///00000000000000000001.json")),
        )?,
        log_root,
        None,
    )?;
    let mut iter =
        log_segment.create_checkpoint_stream(&engine, v2_checkpoint_read_schema.clone(), None)?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let ActionsBatch {
        actions: first_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    // TODO: per contract this batch is not required to have sidecars, but leaving this test in to
    // verify no behavior change.
    assert_batch_matches(
        first_batch,
        sidecar_batch_with_given_paths(
            vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
            get_all_actions_schema().project(&[ADD_NAME, SIDECAR_NAME])?,
        ),
    );
    // Assert that the second batch returned is from reading sidecarfile1
    let ActionsBatch {
        actions: second_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        second_batch,
        add_batch_simple(v2_checkpoint_read_schema.clone()),
    );

    // Assert that the second batch returned is from reading sidecarfile2
    let ActionsBatch {
        actions: third_batch,
        is_log_batch,
    } = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        third_batch,
        add_batch_with_remove(v2_checkpoint_read_schema),
    );

    assert!(iter.next().is_none());

    Ok(())
}

fn create_segment_for(
    commit_versions: &[u64],
    compaction_versions: &[(u64, u64)],
    checkpoint_version: Option<u64>,
    version_to_load: Option<u64>,
) -> LogSegment {
    let mut paths: Vec<Path> = commit_versions
        .iter()
        .map(|version| delta_path_for_version(*version, "json"))
        .chain(
            compaction_versions
                .iter()
                .map(|(start, end)| compacted_log_path_for_versions(*start, *end, "json")),
        )
        .collect();
    if let Some(version) = checkpoint_version {
        paths.push(delta_path_for_version(
            version,
            "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        ));
    }
    let (storage, log_root) = build_log_with_paths_and_checkpoint(&paths, None);
    LogSegment::for_snapshot_impl(
        storage.as_ref(),
        log_root.clone(),
        vec![], // log_tail
        None,
        version_to_load,
    )
    .unwrap()
}

#[test]
fn test_list_log_files_with_version() -> DeltaResult<()> {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(0, "crc"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "crc"),
            delta_path_for_version(2, "json"),
        ],
        None,
    );
    let result = ListedLogFiles::list(
        storage.as_ref(),
        &log_root,
        vec![], // log_tail
        Some(0),
        None,
    )?;
    let latest_crc = result.latest_crc_file.unwrap();
    assert_eq!(
        latest_crc.location.location.path(),
        "/_delta_log/00000000000000000001.crc".to_string()
    );
    assert_eq!(latest_crc.version, 1);
    assert_eq!(latest_crc.filename, "00000000000000000001.crc".to_string());
    assert_eq!(latest_crc.extension, "crc".to_string());
    assert_eq!(latest_crc.file_type, LogPathFileType::Crc);
    Ok(())
}

fn test_compaction_listing(
    commit_versions: &[u64],
    compaction_versions: &[(u64, u64)],
    checkpoint_version: Option<u64>,
    version_to_load: Option<u64>,
) {
    let log_segment = create_segment_for(
        commit_versions,
        compaction_versions,
        checkpoint_version,
        version_to_load,
    );
    let version_to_load = version_to_load.unwrap_or(u64::MAX);
    let checkpoint_cuttoff = checkpoint_version.map(|v| v as i64).unwrap_or(-1);
    let expected_commit_versions: Vec<&u64> = commit_versions
        .iter()
        .filter(|v| **v as i64 > checkpoint_cuttoff && **v <= version_to_load)
        .collect();
    let expected_compaction_versions: Vec<&(u64, u64)> = compaction_versions
        .iter()
        .filter(|(start, end)| *start as i64 > checkpoint_cuttoff && *end <= version_to_load)
        .collect();

    assert_eq!(
        log_segment.ascending_commit_files.len(),
        expected_commit_versions.len()
    );
    assert_eq!(
        log_segment.ascending_compaction_files.len(),
        expected_compaction_versions.len()
    );

    for (commit_file, expected_version) in log_segment
        .ascending_commit_files
        .iter()
        .zip(expected_commit_versions.iter())
    {
        assert!(commit_file.is_commit());
        assert_eq!(commit_file.version, **expected_version);
    }

    for (compaction_file, (expected_start, expected_end)) in log_segment
        .ascending_compaction_files
        .iter()
        .zip(expected_compaction_versions.iter())
    {
        assert!(matches!(
            compaction_file.file_type,
            LogPathFileType::CompactedCommit { .. }
        ));
        assert_eq!(compaction_file.version, *expected_start);
        if let LogPathFileType::CompactedCommit { hi } = compaction_file.file_type {
            assert_eq!(hi, *expected_end);
        } else {
            panic!("File was compaction but type was not CompactedCommit");
        }
    }
}

#[test]
fn test_compaction_simple() {
    test_compaction_listing(
        &[0, 1, 2],
        &[(1, 2)],
        None, // checkpoint version
        None, // version to load
    );
}

#[test]
fn test_compaction_in_version_range() {
    test_compaction_listing(
        &[0, 1, 2, 3],
        &[(1, 2)],
        None,    // checkpoint version
        Some(2), // version to load
    );
}

#[test]
fn test_compaction_out_of_version_range() {
    test_compaction_listing(
        &[0, 1, 2, 3, 4],
        &[(1, 3)],
        None,    // checkpoint version
        Some(2), // version to load
    );
}

#[test]
fn test_multi_compaction() {
    test_compaction_listing(
        &[0, 1, 2, 3, 4, 5],
        &[(1, 2), (3, 5)],
        None, // checkpoint version
        None, //version to load
    );
}

#[test]
fn test_multi_compaction_one_out_of_range() {
    test_compaction_listing(
        &[0, 1, 2, 3, 4, 5],
        &[(1, 2), (3, 5)],
        None,    // checkpoint version
        Some(4), // version to load
    );
}

#[test]
fn test_compaction_with_checkpoint() {
    test_compaction_listing(
        &[0, 1, 2, 4, 5],
        &[(1, 2), (4, 5)],
        Some(3), // checkpoint version
        None,    // version to load
    );
}

#[test]
fn test_compaction_to_early_with_checkpoint() {
    test_compaction_listing(
        &[0, 1, 2, 4, 5],
        &[(1, 2)],
        Some(3), // checkpoint version
        None,    // version to load
    );
}

#[test]
fn test_compaction_starts_at_checkpoint() {
    test_compaction_listing(
        &[0, 1, 2, 4, 5],
        &[(3, 5)],
        Some(3), // checkpoint version
        None,    // version to load
    );
}

enum ExpectedFile {
    Commit(Version),
    Compaction(Version, Version),
}

fn test_commit_cover(
    commit_versions: &[u64],
    compaction_versions: &[(u64, u64)],
    checkpoint_version: Option<u64>,
    version_to_load: Option<u64>,
    expected_files: &[ExpectedFile],
) {
    let log_segment = create_segment_for(
        commit_versions,
        compaction_versions,
        checkpoint_version,
        version_to_load,
    );
    let cover = log_segment.find_commit_cover();
    // our test-utils include "_delta_log" in the path, which is already in log_segment.log_root, so
    // we don't use them. TODO: Unify this
    let expected_locations = expected_files.iter().map(|ef| match ef {
        ExpectedFile::Commit(version) => log_segment
            .log_root
            .join(&format!("{version:020}.json"))
            .expect("Couldn't join"),
        ExpectedFile::Compaction(lo, hi) => log_segment
            .log_root
            .join(&format!("{lo:020}.{hi:020}.compacted.json"))
            .expect("Couldn't join"),
    });
    assert_eq!(cover.len(), expected_locations.len());
    for (location, expected_location) in cover.iter().zip(expected_locations) {
        assert_eq!(location.location, expected_location);
    }
}

#[test]
fn test_commit_cover_one_compaction() {
    test_commit_cover(
        &[0, 1, 2],
        &[(1, 2)],
        None, // checkpoint version
        None, // version to load
        &[ExpectedFile::Compaction(1, 2), ExpectedFile::Commit(0)],
    );
}

#[test]
fn test_commit_cover_in_version_range() {
    test_commit_cover(
        &[0, 1, 2, 3],
        &[(1, 2)],
        None,    // checkpoint version
        Some(2), // version to load
        &[ExpectedFile::Compaction(1, 2), ExpectedFile::Commit(0)],
    );
}

#[test]
fn test_commit_cover_out_of_version_range() {
    test_commit_cover(
        &[0, 1, 2, 3, 4],
        &[(1, 3)],
        None,    // checkpoint version
        Some(2), // version to load
        &[
            ExpectedFile::Commit(2),
            ExpectedFile::Commit(1),
            ExpectedFile::Commit(0),
        ],
    );
}

#[test]
fn test_commit_cover_multi_compaction() {
    test_commit_cover(
        &[0, 1, 2, 3, 4, 5],
        &[(1, 2), (3, 5)],
        None, // checkpoint version
        None, //version to load
        &[
            ExpectedFile::Compaction(3, 5),
            ExpectedFile::Compaction(1, 2),
            ExpectedFile::Commit(0),
        ],
    );
}

#[test]
fn test_commit_cover_multi_compaction_one_out_of_range() {
    test_commit_cover(
        &[0, 1, 2, 3, 4, 5],
        &[(1, 2), (3, 5)],
        None,    // checkpoint version
        Some(4), // version to load
        &[
            ExpectedFile::Commit(4),
            ExpectedFile::Commit(3),
            ExpectedFile::Compaction(1, 2),
            ExpectedFile::Commit(0),
        ],
    );
}

#[test]
fn test_commit_cover_compaction_with_checkpoint() {
    test_commit_cover(
        &[0, 1, 2, 4, 5],
        &[(1, 2), (4, 5)],
        Some(3), // checkpoint version
        None,    // version to load
        &[ExpectedFile::Compaction(4, 5)],
    );
}

#[test]
fn test_commit_cover_too_early_with_checkpoint() {
    test_commit_cover(
        &[0, 1, 2, 4, 5],
        &[(1, 2)],
        Some(3), // checkpoint version
        None,    // version to load
        &[ExpectedFile::Commit(5), ExpectedFile::Commit(4)],
    );
}

#[test]
fn test_commit_cover_starts_at_checkpoint() {
    test_commit_cover(
        &[0, 1, 2, 4, 5],
        &[(3, 5)],
        Some(3), // checkpoint version
        None,    // version to load
        &[ExpectedFile::Commit(5), ExpectedFile::Commit(4)],
    );
}

#[test]
fn test_commit_cover_wider_range() {
    test_commit_cover(
        &Vec::from_iter(0..20),
        &[(0, 5), (0, 10), (5, 10), (13, 19)],
        None, // checkpoint version
        None, // version to load
        &[
            ExpectedFile::Compaction(13, 19),
            ExpectedFile::Commit(12),
            ExpectedFile::Commit(11),
            ExpectedFile::Compaction(0, 10),
        ],
    );
}

#[test]
fn test_commit_cover_no_compactions() {
    test_commit_cover(
        &Vec::from_iter(0..4),
        &[],
        None, // checkpoint version
        None, // version to load
        &[
            ExpectedFile::Commit(3),
            ExpectedFile::Commit(2),
            ExpectedFile::Commit(1),
            ExpectedFile::Commit(0),
        ],
    );
}

#[test]
fn test_commit_cover_minimal_overlap() {
    test_commit_cover(
        &Vec::from_iter(0..6),
        &[(0, 2), (2, 5)],
        None, // checkpoint version
        None, // version to load
        &[
            ExpectedFile::Commit(5),
            ExpectedFile::Commit(4),
            ExpectedFile::Commit(3),
            ExpectedFile::Compaction(0, 2),
        ],
    );
}

#[test]
#[cfg(debug_assertions)]
fn test_debug_assert_listed_log_file_in_order_compaction_files() {
    let _ = ListedLogFiles::try_new(
        vec![],
        vec![
            create_log_path("file:///00000000000000000000.00000000000000000004.compacted.json"),
            create_log_path("file:///00000000000000000001.00000000000000000002.compacted.json"),
        ],
        vec![],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn test_debug_assert_listed_log_file_out_of_order_compaction_files() {
    let _ = ListedLogFiles::try_new(
        vec![],
        vec![
            create_log_path("file:///00000000000000000000.00000000000000000004.compacted.json"),
            create_log_path("file:///00000000000000000000.00000000000000000003.compacted.json"),
        ],
        vec![],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn test_debug_assert_listed_log_file_different_multipart_checkpoint_versions() {
    let _ = ListedLogFiles::try_new(
        vec![],
        vec![],
        vec![
            create_log_path("00000000000000000010.checkpoint.0000000001.0000000002.parquet"),
            create_log_path("00000000000000000011.checkpoint.0000000002.0000000002.parquet"),
        ],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn test_debug_assert_listed_log_file_invalid_multipart_checkpoint() {
    let _ = ListedLogFiles::try_new(
        vec![],
        vec![],
        vec![
            create_log_path("00000000000000000010.checkpoint.0000000001.0000000003.parquet"),
            create_log_path("00000000000000000011.checkpoint.0000000002.0000000003.parquet"),
        ],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );
}

#[test]
fn commits_since() {
    // simple
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=4),
        &[],
        None, // No checkpoint
        None, // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 4);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 4);

    // with compaction, no checkpoint
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=4),
        &[(0, 2)],
        None, // No checkpoint
        None, // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 4);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 2);

    // checkpoint, no compaction
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=6),
        &[],
        Some(3), // Checkpoint @ 3
        None,    // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 3);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 3);

    // checkpoint and compaction less than checkpoint
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=6),
        &[(0, 2)],
        Some(3), // Checkpoint @ 3
        None,    // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 3);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 3);

    // checkpoint and compaction greater than checkpoint
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=6),
        &[(3, 4)],
        Some(2), // Checkpoint @ 2
        None,    // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 4);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 2);

    // multiple compactions
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=6),
        &[(1, 2), (3, 4)],
        None, // No Checkpoint
        None, // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 6);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 2);

    // multiple compactions, out of order
    let log_segment = create_segment_for(
        &Vec::from_iter(0..=10),
        &[(1, 2), (3, 9), (4, 6)],
        None, // No Checkpoint
        None, // Version to load
    );
    assert_eq!(log_segment.commits_since_checkpoint(), 10);
    assert_eq!(log_segment.commits_since_log_compaction_or_checkpoint(), 1);
}

#[test]
fn for_timestamp_conversion_gets_commit_range() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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
        LogSegment::for_timestamp_conversion(storage.as_ref(), log_root.clone(), 7, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert!(checkpoint_parts.is_empty());

    let versions = commit_files.iter().map(|x| x.version).collect_vec();
    assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], versions);
}

#[test]
fn for_timestamp_conversion_with_old_end_version() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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
        LogSegment::for_timestamp_conversion(storage.as_ref(), log_root.clone(), 5, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert!(checkpoint_parts.is_empty());

    let versions = commit_files.iter().map(|x| x.version).collect_vec();
    assert_eq!(vec![0, 1, 2, 3, 4, 5], versions);
}

#[test]
fn for_timestamp_conversion_only_contiguous_ranges() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            // version 4 is missing
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_timestamp_conversion(storage.as_ref(), log_root.clone(), 7, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert!(checkpoint_parts.is_empty());

    let versions = commit_files.iter().map(|x| x.version).collect_vec();
    assert_eq!(vec![5, 6, 7], versions);
}

#[test]
fn for_timestamp_conversion_with_limit() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_timestamp_conversion(
        storage.as_ref(),
        log_root.clone(),
        7,
        Some(NonZero::new(3).unwrap()),
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert!(checkpoint_parts.is_empty());

    let versions = commit_files.iter().map(|x| x.version).collect_vec();
    assert_eq!(vec![5, 6, 7], versions);
}
#[test]
fn for_timestamp_conversion_with_large_limit() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
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

    let log_segment = LogSegment::for_timestamp_conversion(
        storage.as_ref(),
        log_root.clone(),
        7,
        Some(NonZero::new(20).unwrap()),
    )
    .unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert!(checkpoint_parts.is_empty());

    let versions = commit_files.iter().map(|x| x.version).collect_vec();
    assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], versions);
}

#[test]
fn for_timestamp_conversion_no_commit_files() {
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[delta_path_for_version(5, "checkpoint.parquet")],
        None,
    );

    let res = LogSegment::for_timestamp_conversion(storage.as_ref(), log_root.clone(), 0, None);
    assert_result_error_with_message(res, "Generic delta kernel error: No files in log segment");
}

#[test]
fn test_latest_commit_file_field_is_captured() {
    // Test that the latest commit is preserved even after checkpoint filtering
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(2, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(storage.as_ref(), log_root.clone(), vec![], None).unwrap();

    // The latest commit should be version 5
    assert_eq!(log_segment.latest_commit_file.unwrap().version, 5);

    // The log segment should only contain commits 3, 4, 5 (after checkpoint 2)
    assert_eq!(log_segment.ascending_commit_files.len(), 3);
    assert_eq!(log_segment.ascending_commit_files[0].version, 3);
    assert_eq!(log_segment.ascending_commit_files[2].version, 5);
}

#[test]
fn test_latest_commit_file_with_checkpoint_filtering() {
    // Test when commits get filtered by checkpoint
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(storage.as_ref(), log_root.clone(), vec![], None).unwrap();

    // The latest commit should be version 4
    assert_eq!(log_segment.latest_commit_file.unwrap().version, 4);

    // The log segment should have only commit 4 (after checkpoint 3)
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    assert_eq!(log_segment.ascending_commit_files[0].version, 4);
}

#[test]
fn test_latest_commit_file_with_no_commits() {
    // Test when there are only checkpoints and no commits at all
    // This should now succeed with latest_commit_file as None
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[delta_path_for_version(2, "checkpoint.parquet")],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(storage.as_ref(), log_root.clone(), vec![], None).unwrap();

    // latest_commit_file should be None when there are no commits
    assert!(log_segment.latest_commit_file.is_none());

    // The checkpoint should be at version 2
    assert_eq!(log_segment.checkpoint_version, Some(2));
}

#[test]
fn test_latest_commit_file_with_checkpoint_at_same_version() {
    // Test when checkpoint is at the same version as the latest commit
    // This tests: 0.json, 1.json, 1.checkpoint.parquet
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(storage.as_ref(), log_root.clone(), vec![], None).unwrap();

    // The latest commit should be version 1 (saved before filtering)
    assert_eq!(log_segment.latest_commit_file.unwrap().version, 1);

    // The log segment should have no commit files (all filtered by checkpoint at version 1)
    assert_eq!(log_segment.ascending_commit_files.len(), 0);

    // The checkpoint should be at version 1
    assert_eq!(log_segment.checkpoint_version, Some(1));
}

#[test]
fn test_latest_commit_file_edge_case_commit_before_checkpoint() {
    // Test edge case: 0.json, 1.checkpoint.parquet
    // The latest_commit_file should NOT be set to version 0 since there's no commit at version 1
    let (storage, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(storage.as_ref(), log_root.clone(), vec![], None).unwrap();

    // latest_commit_file should be None since there's no commit at the checkpoint version
    assert!(log_segment.latest_commit_file.is_none());

    // The checkpoint should be at version 1
    assert_eq!(log_segment.checkpoint_version, Some(1));

    // There should be no commits in the log segment (all filtered by checkpoint)
    assert_eq!(log_segment.ascending_commit_files.len(), 0);
}

#[test]
fn test_log_segment_contiguous_commit_files() {
    let res = ListedLogFiles::try_new(
        vec![
            create_log_path("file:///00000000000000000001.json"),
            create_log_path("file:///00000000000000000002.json"),
            create_log_path("file:///00000000000000000003.json"),
        ],
        vec![],
        vec![],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );
    assert!(res.is_ok());

    // allow gaps in ListedLogFiles
    let listed = ListedLogFiles::try_new(
        vec![
            create_log_path("file:///00000000000000000001.json"),
            create_log_path("file:///00000000000000000003.json"),
        ],
        vec![],
        vec![],
        None,
        Some(create_log_path("file:///00000000000000000001.json")),
    );

    // disallow gaps in LogSegment
    let log_segment = LogSegment::try_new(listed.unwrap(), Url::parse("file:///").unwrap(), None);
    assert_result_error_with_message(
        log_segment,
        "Generic delta kernel error: Expected ordered \
        contiguous commit files [ParsedLogPath { location: FileMeta { location: Url { scheme: \
        \"file\", cannot_be_a_base: false, username: \"\", password: None, host: None, port: \
        None, path: \"/00000000000000000001.json\", query: None, fragment: None }, last_modified: \
        0, size: 0 }, filename: \"00000000000000000001.json\", extension: \"json\", version: 1, \
        file_type: Commit }, ParsedLogPath { location: FileMeta { location: Url { scheme: \
        \"file\", cannot_be_a_base: false, username: \"\", password: None, host: None, port: \
        None, path: \"/00000000000000000003.json\", query: None, fragment: None }, last_modified: \
        0, size: 0 }, filename: \"00000000000000000003.json\", extension: \"json\", version: 3, \
        file_type: Commit }]",
    );
}

#[test]
fn test_publish_validation() {
    use crate::Error;

    // Test with only regular committed files - should pass validation
    let regular_commits = vec![
        create_log_path("file:///path/_delta_log/00000000000000000000.json"),
        create_log_path("file:///path/_delta_log/00000000000000000001.json"),
        create_log_path("file:///path/_delta_log/00000000000000000002.json"),
    ];

    let log_segment = LogSegment {
        ascending_commit_files: regular_commits,
        ascending_compaction_files: vec![],
        checkpoint_parts: vec![],
        checkpoint_version: None,
        log_root: Url::parse("file:///path/").unwrap(),
        end_version: 2,
        latest_crc_file: None,
        latest_commit_file: None,
    };

    assert!(log_segment.validate_no_staged_commits().is_ok());

    // Test with a staged commit - should fail validation
    let with_staged = vec![
        create_log_path("file:///path/_delta_log/00000000000000000000.json"),
        create_log_path("file:///path/_delta_log/00000000000000000001.json"),
        create_log_path("file:///path/_delta_log/_staged_commits/00000000000000000002.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json"),
    ];

    let log_segment_with_staged = LogSegment {
        ascending_commit_files: with_staged,
        ascending_compaction_files: vec![],
        checkpoint_parts: vec![],
        checkpoint_version: None,
        log_root: Url::parse("file:///path/").unwrap(),
        end_version: 2,
        latest_crc_file: None,
        latest_commit_file: None,
    };

    // Should fail with staged commits
    let result = log_segment_with_staged.validate_no_staged_commits();
    assert!(result.is_err());
    if let Err(Error::Generic(msg)) = result {
        assert_eq!(msg, "Found staged commit file in log segment");
    } else {
        panic!("Expected Error::Generic");
    }
}
