//! Scans of adaptiveMetadata (AMT) tables, whose state at or below the latest `checkpoint` action
//! is read from that action's content tree.

use std::sync::Arc;

use itertools::Itertools as _;
use rstest::rstest;
use test_utils::add_commit;

use super::get_files_for_scan;
use crate::actions::{CheckpointAction, ContentRoot};
use crate::content_tree::test_utils::{added_data_entry, data_manifest_entry, write_manifest};
use crate::content_tree::{ContentTreeNodeEntry, TrackingInfo, TrackingStatus};
use crate::engine::sync::SyncEngine;
use crate::expressions::{col, lit, Predicate as Pred};
use crate::object_store::memory::InMemory;
use crate::scan::ScanMetadata;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    adaptive_metadata_partitioned_by, adaptive_metadata_protocol_and_metadata, checkpoint_line,
    protocol_and_metadata_lines,
};
use crate::unit_test_utils::assert_result_error_with_message;
use crate::{EngineData, Snapshot, Version};

const TABLE_ROOT: &str = "memory:///";

fn table_root() -> url::Url {
    url::Url::parse(TABLE_ROOT).unwrap()
}

fn add_line(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":1,"dataChange":true}}}}"#
    )
}

/// An `add` whose stats bound the `id` column (physical name `phys_id`) to `[min, max]`.
fn add_line_with_id_range(path: &str, min: i64, max: i64) -> String {
    let stats = format!(
        r#"{{\"numRecords\":1,\"minValues\":{{\"phys_id\":{min}}},\"maxValues\":{{\"phys_id\":{max}}},\"nullCount\":{{\"phys_id\":0}}}}"#
    );
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":1,"dataChange":true,"stats":"{stats}"}}}}"#
    )
}

fn remove_line(path: &str) -> String {
    format!(r#"{{"remove":{{"path":"{path}","deletionTimestamp":1,"dataChange":true}}}}"#)
}

/// Writes `entries` as the root manifest at `path` and returns a `checkpoint` action line
/// referencing it at `version`.
fn root_checkpoint_line(
    engine: &SyncEngine,
    path: &str,
    version: i64,
    entries: &[ContentTreeNodeEntry],
    partition_columns: &[&str],
) -> String {
    let root = write_manifest(engine, table_root().join(path).unwrap(), entries);
    checkpoint_action_line(path, root.size as i64, version, partition_columns)
}

/// A `checkpoint` action line referencing a `size`-byte root manifest at `path`, at `version`.
fn checkpoint_action_line(
    path: &str,
    size: i64,
    version: i64,
    partition_columns: &[&str],
) -> String {
    let (protocol, _) = adaptive_metadata_protocol_and_metadata();
    checkpoint_line(&CheckpointAction::new(
        version,
        ContentRoot::new(path.to_string(), size, version),
        protocol,
        adaptive_metadata_partitioned_by(partition_columns),
        vec![],
        vec![],
    ))
}

fn deleted_entry(path: &str) -> ContentTreeNodeEntry {
    let mut entry = added_data_entry(path, 1, 1, 0, 0);
    entry.tracking.status = TrackingStatus::Deleted;
    entry
}

/// A table whose log is:
/// - v0: protocol, metadata, and an add of `stale.parquet`, which no tree lists;
/// - v1: a checkpoint action whose tree lists `a`, `b`, and a deleted `gone`;
/// - v2: an add of `g` and a remove of `a`;
/// - v3: a checkpoint action whose tree lists `b`, `g`, and `h`.
async fn two_checkpoint_table() -> SyncEngine {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let tree1 = [
        added_data_entry("a.parquet", 1, 1, 0, 0),
        deleted_entry("gone.parquet"),
        added_data_entry("b.parquet", 1, 1, 1, 0),
    ];
    let tree2 = [
        added_data_entry("b.parquet", 1, 1, 1, 0),
        added_data_entry("g.parquet", 1, 1, 2, 2),
        added_data_entry("h.parquet", 1, 1, 3, 3),
    ];
    let commits = [
        format!(
            "{}\n{}",
            protocol_and_metadata_lines(&[]),
            add_line("stale.parquet")
        ),
        root_checkpoint_line(&engine, "metadata/root-1.parquet", 1, &tree1, &[]),
        format!("{}\n{}", add_line("g.parquet"), remove_line("a.parquet")),
        root_checkpoint_line(&engine, "metadata/root-3.parquet", 3, &tree2, &[]),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
    engine
}

fn snapshot_at(engine: &SyncEngine, version: Version) -> Arc<Snapshot> {
    Snapshot::builder_for(TABLE_ROOT)
        .at_version(version)
        .build(engine)
        .unwrap()
}

fn scan_paths(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<String> {
    let scan = snapshot.scan_builder().build().unwrap();
    get_files_for_scan(scan, engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

#[rstest]
#[case::at_first_checkpoint(1, &["a.parquet", "b.parquet"])]
#[case::commits_after_first_checkpoint(2, &["b.parquet", "g.parquet"])]
#[case::at_second_checkpoint(3, &["b.parquet", "g.parquet", "h.parquet"])]
#[tokio::test]
async fn scan_reads_latest_tree_then_later_commits(
    #[case] version: Version,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table().await;
    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, version)),
        strings(expected)
    );
}

#[tokio::test]
async fn scan_reads_leaf_manifests() {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let leaf_path = "metadata/leaf.parquet";
    let leaf = write_manifest(
        &engine,
        table_root().join(leaf_path).unwrap(),
        &[
            added_data_entry("l1.parquet", 1, 1, 0, 0),
            added_data_entry("l2.parquet", 1, 1, 1, 0),
        ],
    );
    let mut leaf_entry = data_manifest_entry(leaf_path, leaf.size as i64, TrackingStatus::Added);
    leaf_entry.tracking = TrackingInfo {
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        first_row_id: Some(10),
        ..leaf_entry.tracking
    };
    let root = [added_data_entry("r.parquet", 1, 1, 0, 1), leaf_entry];
    let commits = [
        protocol_and_metadata_lines(&[]),
        root_checkpoint_line(&engine, "metadata/root.parquet", 1, &root, &[]),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, 1)),
        strings(&["l1.parquet", "l2.parquet", "r.parquet"])
    );
}

#[tokio::test]
async fn scan_of_partitioned_table_with_checkpoint_action_is_unsupported() {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let commits = [
        protocol_and_metadata_lines(&["id"]),
        root_checkpoint_line(
            &engine,
            "metadata/root.parquet",
            1,
            &[added_data_entry("a.parquet", 1, 1, 0, 0)],
            &["id"],
        ),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
    let scan = snapshot_at(&engine, 1).scan_builder().build().unwrap();
    assert_result_error_with_message(
        scan.scan_metadata(&engine).map(|_| ()),
        r#"partition columns: ["id"]"#,
    );
}

#[tokio::test]
async fn scan_with_missing_root_manifest_names_it() {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let commits = [
        protocol_and_metadata_lines(&[]),
        checkpoint_action_line("metadata/root.parquet", 1, 1, &[]),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
    let scan = snapshot_at(&engine, 1).scan_builder().build().unwrap();
    assert_result_error_with_message(get_files_for_scan(scan, &engine), "metadata/root.parquet");
}

#[tokio::test]
async fn data_skipping_prunes_commit_files_but_keeps_tree_files_without_stats() {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let commits = [
        protocol_and_metadata_lines(&[]),
        root_checkpoint_line(
            &engine,
            "metadata/root.parquet",
            1,
            &[added_data_entry("tree.parquet", 1, 1, 0, 0)],
            &[],
        ),
        format!(
            "{}\n{}",
            add_line_with_id_range("low.parquet", 1, 5),
            add_line_with_id_range("high.parquet", 200, 300)
        ),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
    let predicate = Arc::new(Pred::gt(col!("id"), lit(100i64)));
    let scan = snapshot_at(&engine, 2)
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let paths: Vec<_> = get_files_for_scan(scan, &engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect();
    assert_eq!(paths, strings(&["high.parquet", "tree.parquet"]));
}

/// The scan metadata of `snapshot`, with selection vectors applied, as cached by a caller of
/// `scan_metadata_from`.
fn cached_scan_metadata(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<Box<dyn EngineData>> {
    let scan = snapshot.scan_builder().build().unwrap();
    scan.scan_metadata(engine)
        .unwrap()
        .map(|metadata| {
            let ScanMetadata { scan_files, .. } = metadata.unwrap();
            scan_files.apply_selection_vector().unwrap()
        })
        .collect()
}

#[rstest]
// From before the checkpoint action, the cached rows (here deliberately empty) are discarded and
// the table is fully rescanned: advancing commit by commit would miss `b`.
#[case::before_checkpoint_action(0, true, &["b.parquet", "g.parquet"])]
#[case::at_checkpoint_action(1, false, &["b.parquet", "g.parquet"])]
#[tokio::test]
async fn scan_metadata_from_hint_before_checkpoint_action_rescans(
    #[case] existing_version: Version,
    #[case] empty_cache: bool,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table().await;
    let existing_data = if empty_cache {
        vec![]
    } else {
        cached_scan_metadata(&engine, snapshot_at(&engine, existing_version))
    };
    let scan = snapshot_at(&engine, 2).scan_builder().build().unwrap();
    let mut paths = vec![];
    for metadata in scan
        .scan_metadata_from(&engine, existing_version, existing_data, None)
        .unwrap()
    {
        paths = metadata
            .unwrap()
            .visit_scan_files(paths, |paths: &mut Vec<String>, file| {
                paths.push(file.path.to_string())
            })
            .unwrap();
    }
    paths.sort();
    assert_eq!(paths, strings(expected));
}
