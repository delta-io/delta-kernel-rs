//! Conflict detection with winners from real tables.

use ::test_utils::{AddFilePartitionKeyModify, modify_add_file_partition_keys, replace_column};
use rstest::rstest;

use super::*;
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::ArrowEngineData;
use crate::scan::state::ScanFile as ScanStateFile;
use crate::table_features::ColumnMappingMode;
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::occ::ConflictKind::Occ;
use crate::unit_test_utils::{copy_test_table, nullable_add_file, one_record_stats};
use crate::{Snapshot, SnapshotRef};

// === Table and commit helpers ===

/// Creates a table partitioned by `part`, with row tracking and in-commit timestamps. At version 1,
/// it has one file in partition `a` and one in partition `b`.
fn kernel_table() -> (String, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let url = Url::from_directory_path(dir.path()).unwrap().to_string();
    let engine = SyncEngine::new();
    create_table(
        &url,
        schema_ref! { nullable "id": INTEGER, nullable "part": STRING },
        "Test/1.0",
        true,
    )
    .with_data_layout(DataLayout::partitioned(["part"]))
    .with_table_properties([
        ("delta.enableRowTracking", "true"),
        ("delta.enableInCommitTimestamps", "true"),
    ])
    .build(&engine, Box::new(FileSystemCommitter::new()))
    .unwrap()
    .commit(&engine)
    .unwrap()
    .unwrap_committed();
    let files = [
        ("part=a/file-a.parquet", "a"),
        ("part=b/file-b.parquet", "b"),
    ];
    let version = commit_adds(&url, "part", &files);
    assert_eq!(version, 1);
    (url, dir)
}

/// Commits a Kernel WRITE that adds `files` to the latest version of the table at `url`.
///
/// Each file is a `(path, value)` pair, and `value` is its value of partition column `key`.
fn commit_adds(url: &str, key: &str, files: &[(&str, &str)]) -> Version {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "Test/1.0", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    for (path, value) in files {
        let batch = nullable_add_file(path);
        let batch = replace_column(&batch, "stats", one_record_stats(&batch));
        let batch = modify_add_file_partition_keys(
            batch,
            &[AddFilePartitionKeyModify::Insert {
                key,
                value: Some(value),
            }],
        );
        txn.add_files(Box::new(ArrowEngineData::new(batch)));
    }
    txn.commit(&engine)
        .unwrap()
        .unwrap_committed()
        .commit_version()
}

// === Read-side helpers ===

fn snapshot_at(url: &str, version: Version) -> SnapshotRef {
    Snapshot::builder_for(url)
        .at_version(version)
        .build(&SyncEngine::new())
        .unwrap()
}

/// The `path` values that a scan of `snapshot` returns for `predicate`, as written in the log.
fn scan_paths(snapshot: &SnapshotRef, predicate: Option<PredicateRef>) -> Vec<String> {
    let engine = SyncEngine::new();
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let mut paths = Vec::new();
    for metadata in scan.scan_metadata(&engine).unwrap() {
        paths = metadata
            .unwrap()
            .visit_scan_files(paths, |paths: &mut Vec<String>, file: ScanStateFile| {
                paths.push(file.path)
            })
            .unwrap();
    }
    paths
}

/// Relations and facts of a DELETE that reads and removes the files that `partition_predicate`
/// selects. With no predicate, the DELETE reads and removes every file.
fn delete_selecting(
    snapshot: &SnapshotRef,
    partition_schema: SchemaRef,
    partition_predicate: Option<PredicateRef>,
) -> (LoserRelations, LoserFacts) {
    let paths = scan_paths(snapshot, partition_predicate.clone());
    let read = Vec::from_iter(paths.iter().map(String::as_str));
    let relations = LoserRelations::new(&read, &read);
    let facts = partitioned_delete_facts(
        snapshot.version(),
        &relations.read_files,
        partition_schema,
        partition_predicate,
    );
    (relations, facts)
}

fn detect_in_table(relations: &LoserRelations, facts: &LoserFacts, url: &str) -> Option<Conflict> {
    let winners = winners_after(url, facts.checked_version);
    outcome(relations.detect(facts, &winners).unwrap())
}

fn no_conflict_summaries(detection: ConflictDetection) -> Vec<WinnerSummary> {
    let ConflictDetection::NoConflict(summaries) = detection else {
        panic!("expected no conflict, got {detection:?}");
    };
    summaries
}

// === Kernel-written winners ===

#[test]
fn kernel_row_tracking_winners_report_their_marks_and_timestamps() {
    // Both winners append to partition `b`, so T passes. Each summary carries the winner's row ID
    // high-water mark, and the in-commit timestamp that the snapshot at that version reports.
    let (url, _dir) = kernel_table();
    assert_eq!(
        commit_adds(&url, "part", &[("part=b/winner-1.parquet", "b")]),
        2
    );
    assert_eq!(
        commit_adds(&url, "part", &[("part=b/winner-2.parquet", "b")]),
        3
    );
    // T deletes partition `a` at version 1.
    let (relations, facts) = delete_selecting(
        &snapshot_at(&url, 1),
        schema_ref! { nullable "part": STRING },
        Some(equals("part", "a")),
    );
    let detection = relations.detect(&facts, &winners_after(&url, 1)).unwrap();
    let summaries = no_conflict_summaries(detection);
    let engine = SyncEngine::new();
    assert_eq!(Vec::from_iter(summaries.iter().map(|s| s.version)), [2, 3]);
    // Version 1 ends at mark 1. Each winner adds one row, so the marks rise to 2 and 3.
    assert_eq!(
        Vec::from_iter(summaries.iter().map(|s| s.row_id_high_water_mark)),
        [Some(2), Some(3)]
    );
    for summary in &summaries {
        let snapshot = snapshot_at(&url, summary.version);
        assert_eq!(summary.operation.as_deref(), Some("WRITE"));
        // `commit_adds` doesn't call `with_blind_append`.
        assert!(!summary.is_blind_append);
        assert_eq!(
            summary.in_commit_timestamp,
            Some(snapshot.reader().get_timestamp(&engine).unwrap())
        );
    }
}

#[rstest]
#[case::predicate_on_physical_name_matches(
    "x",
    conflict(1, Some("WRITE"), Occ(OccCheck::ReadAppend))
)]
#[case::other_partition_value_passes("y", None)]
fn kernel_append_matches_physical_partition_column(
    #[case] predicate_value: &str,
    #[case] expected: Option<Conflict>,
) {
    // `partition_cm/name` uses name-mode column mapping, so the partition column has a physical
    // name. The winner adds a file under that name, and T's predicate must use it too.
    let (url, _dir) = copy_test_table("partition_cm/name").unwrap();
    let url = url.as_str();
    let snapshot = snapshot_at(url, 0);
    let physical = snapshot
        .schema()
        .field("category")
        .unwrap()
        .physical_name(ColumnMappingMode::Name)
        .to_string();
    commit_adds(url, &physical, &[("added.parquet", "x")]);
    let relations = LoserRelations::new(&[], &[]);
    let facts = partitioned_delete_facts(
        0,
        &relations.read_files,
        schema_ref! { nullable (physical.as_str()): STRING },
        Some(equals(&physical, predicate_value)),
    );
    assert_eq!(detect_in_table(&relations, &facts, url), expected);
}

// === Fixture-table winners ===

// A DV update removes a file and adds it back with the same path. The re-added file is a non-blind
// add, so a DELETE conflicts on the add. An OPTIMIZE drops to SnapshotIsolation, so only the
// removal conflicts.
#[rstest]
#[case::dv_update_with_commit_info_removes_read_file_for_optimize(
    "table-with-dv-small",
    0,
    make_optimize,
    conflict(1, Some("DELETE"), Occ(OccCheck::ReadDelete))
)]
#[case::dv_update_with_commit_info_appends_for_delete(
    "table-with-dv-small",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, Some("DELETE"), Occ(OccCheck::ReadAppend))
)]
// Version 3 is an OPTIMIZE with `dataChange=false`. It removes both files that T read at version 2.
// Delta Spark's read-delete check does not depend on `dataChange`, and an OPTIMIZE loser skips the
// add check.
#[case::optimize_removes_read_files_under_snapshot_isolation(
    "cdf-table-with-cdc-and-dvs",
    2,
    make_optimize,
    conflict(3, Some("OPTIMIZE"), Occ(OccCheck::ReadDelete))
)]
fn fixture_loser_that_read_every_file(
    #[case] table: &str,
    #[case] read_version: Version,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<Conflict>,
) {
    let (url, _tmp) = copy_test_table(table).unwrap();
    let (relations, mut facts) = delete_selecting(
        &snapshot_at(url.as_str(), read_version),
        schema_ref! {},
        None,
    );
    adjust(&mut facts);
    assert_eq!(detect_in_table(&relations, &facts, url.as_str()), expected);
}

#[test]
fn cdc_only_winners_pass_and_keep_their_operations() {
    // Each winner has only `commitInfo` and `cdc` actions. Delta Spark does not count `cdc` as an
    // add or a remove.
    let (url, _tmp) = copy_test_table("cdf-table-update-ops").unwrap();
    let (relations, facts) = delete_selecting(&snapshot_at(url.as_str(), 0), schema_ref! {}, None);
    let detection = relations
        .detect(&facts, &winners_after(url.as_str(), 0))
        .unwrap();
    let summaries = no_conflict_summaries(detection);
    let operations = Vec::from_iter(summaries.iter().map(|s| s.operation.as_deref()));
    assert_eq!(operations, [Some("Manual Update"), Some("Manual Update")]);
    assert!(summaries.iter().all(|s| !s.is_blind_append));
}
