//! Conflict detection with winners from real tables.
//!
//! T is the losing transaction. Each test builds T's facts by hand. T's read and remove sets come
//! from a scan of T's snapshot. Kernel-written winners come from Kernel transactions on a table
//! that each test creates, or on the `partition_cm/name` fixture. The other winners come from
//! fixture tables that Delta Spark, Databricks Runtime and delta-rs wrote. Each expected result
//! matches the Delta Spark `ConflictChecker` outcome. Where Kernel reports a different rule or
//! version, the test comment says so.

use ::test_utils::{AddFilePartitionKeyModify, modify_add_file_partition_keys, replace_column};
use rstest::rstest;
use tempfile::TempDir;
use url::Url;

use super::decision::{Conflict, WinnerRule};
use super::occ_plan::OccCheck;
use super::tests::{
    LoserRelations, conflict, equals, make_optimize, no_conflict_summaries, occ, only_summary,
    outcome, partitioned_delete_facts, winner, winners_after,
};
use super::*;
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::scan::state::ScanFile as ScanStateFile;
use crate::schema::{SchemaRef, schema_ref};
use crate::table_features::ColumnMappingMode;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::{Transaction, UpdateTableOperation, UpdateTableTransactionBuilder};
use crate::unit_test_utils::{copy_test_table, nullable_add_file, one_record_stats};
use crate::{Snapshot, SnapshotRef};

const FILE_A: &str = "part=a/file-a.parquet";
const FILE_B: &str = "part=b/file-b.parquet";

// === Table and commit helpers ===

/// Creates a table partitioned by `part`, with `FILE_A` in partition `a` and `FILE_B` in partition
/// `b`, at version 1.
fn kernel_table(properties: &[(&str, &str)]) -> (String, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("table");
    std::fs::create_dir(&table_dir).unwrap();
    let url = Url::from_directory_path(table_dir).unwrap().to_string();
    let mut all_properties = vec![("delta.feature.domainMetadata", "supported")];
    all_properties.extend_from_slice(properties);
    let engine = SyncEngine::new();
    create_table(
        &url,
        schema_ref! { nullable "id": INTEGER, nullable "part": STRING },
        "Test/1.0",
        true,
    )
    .with_data_layout(DataLayout::partitioned(["part"]))
    .with_table_properties(all_properties)
    .build(&engine, Box::new(FileSystemCommitter::new()))
    .unwrap()
    .commit(&engine)
    .unwrap()
    .unwrap_committed();
    let version = commit_adds(&url, "part", &[(FILE_A, "a"), (FILE_B, "b")], false);
    assert_eq!(version, 1);
    (url, dir)
}

/// Commits one Kernel transaction on the latest version of the table at `url`, and returns the
/// commit version.
///
/// `configure` sets the builder options, and `stage` adds the file actions.
fn commit_txn(
    url: &str,
    operation: UpdateTableOperation,
    data_change: bool,
    configure: impl FnOnce(UpdateTableTransactionBuilder) -> UpdateTableTransactionBuilder,
    stage: impl FnOnce(&mut Transaction, &SnapshotRef),
) -> Version {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let builder = snapshot
        .clone()
        .update_table(operation, "Test/1.0", data_change);
    let mut txn = configure(builder)
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    stage(&mut txn, &snapshot);
    txn.commit(&engine)
        .unwrap()
        .unwrap_committed()
        .commit_version()
}

/// Commits a Kernel transaction that adds `files`, as `(path, partition value)` pairs under `key`.
fn commit_adds(url: &str, key: &str, files: &[(&str, &str)], blind_append: bool) -> Version {
    commit_txn(
        url,
        UpdateTableOperation::Write,
        true,
        |builder| {
            if blind_append {
                builder.with_blind_append()
            } else {
                builder
            }
        },
        |txn, _| {
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
        },
    )
}

/// Commits a Kernel transaction that removes every file in partition `value`.
fn commit_remove(
    url: &str,
    operation: UpdateTableOperation,
    data_change: bool,
    value: &str,
) -> Version {
    commit_txn(
        url,
        operation,
        data_change,
        |builder| builder,
        |txn, snapshot| {
            let engine = SyncEngine::new();
            let scan = snapshot
                .clone()
                .scan_builder()
                .with_predicate(equals("part", value))
                .build()
                .unwrap();
            for metadata in scan.scan_metadata(&engine).unwrap() {
                txn.remove_files(metadata.unwrap().scan_files);
            }
        },
    )
}

/// Commits a Kernel transaction that writes the user domain `domain`.
fn commit_domain(url: &str, domain: &str) -> Version {
    commit_txn(
        url,
        UpdateTableOperation::Write,
        true,
        |builder| builder.with_domain_metadata(domain, "{}"),
        |_, _| {},
    )
}

/// Commits a Kernel transaction that writes a `txn` action for `app_id`.
fn commit_app_id(url: &str, app_id: &str) -> Version {
    commit_txn(
        url,
        UpdateTableOperation::Write,
        true,
        |builder| builder.with_transaction_id(app_id, 1),
        |_, _| {},
    )
}

/// Copies the fixture `name` from `kernel/tests/data` into a temporary directory.
fn fixture_table(name: &str) -> (String, TempDir) {
    let (url, dir) = copy_test_table(name).unwrap();
    (url.to_string(), dir)
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

fn as_strs(paths: &[String]) -> Vec<&str> {
    Vec::from_iter(paths.iter().map(String::as_str))
}

/// Relations and facts of a DELETE that reads and removes the files that `partition_predicate`
/// selects. With no predicate, the DELETE reads and removes every file.
fn delete_selecting(
    snapshot: &SnapshotRef,
    partition_schema: SchemaRef,
    partition_predicate: Option<PredicateRef>,
) -> (LoserRelations, LoserFacts) {
    let paths = scan_paths(snapshot, partition_predicate.clone());
    let read = as_strs(&paths);
    let relations = LoserRelations::new(&read, &read);
    let facts = partitioned_delete_facts(
        snapshot.version(),
        &relations.read_files,
        partition_schema,
        partition_predicate,
    );
    (relations, facts)
}

/// Relations and facts of a DELETE of every file in an unpartitioned `snapshot`.
fn delete_all(snapshot: &SnapshotRef) -> (LoserRelations, LoserFacts) {
    delete_selecting(snapshot, schema_ref! {}, None)
}

/// Relations and facts of a DELETE of partition `a` at version 1 of a [`kernel_table`] at `url`.
fn delete_partition_a(url: &str) -> (LoserRelations, LoserFacts) {
    delete_selecting(
        &snapshot_at(url, 1),
        schema_ref! { nullable "part": STRING },
        Some(equals("part", "a")),
    )
}

/// Runs conflict detection for T against the winners after T's read version in the table at
/// `url`.
fn detect_in_table(relations: &LoserRelations, facts: &LoserFacts, url: &str) -> Option<Conflict> {
    outcome(relations.detect(facts, &winners_after(url, facts.read_version)))
}

// === Kernel-written winners ===

#[rstest]
#[case::non_blind_append_fails_under_write_serializable(
    false,
    IsolationLevel::WriteSerializable,
    "a",
    conflict(2, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
#[case::blind_append_passes_under_write_serializable(
    true,
    IsolationLevel::WriteSerializable,
    "a",
    None
)]
#[case::blind_append_fails_under_serializable(
    true,
    IsolationLevel::Serializable,
    "a",
    conflict(2, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
#[case::append_to_other_partition_passes_under_serializable(
    false,
    IsolationLevel::Serializable,
    "b",
    None
)]
fn kernel_append_in_read_partition(
    #[case] blind_append: bool,
    #[case] isolation_level: IsolationLevel,
    #[case] winner_partition: &str,
    #[case] expected: Option<Conflict>,
) {
    // T deletes partition `a` and reads `FILE_A`. The winner appends one file to
    // `winner_partition`. Delta Spark's `checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn`
    // checks the added files. Under WriteSerializable, it skips a blind-append winner when T
    // changes no metadata.
    let (url, _dir) = kernel_table(&[]);
    let path = format!("part={winner_partition}/added.parquet");
    commit_adds(&url, "part", &[(&path, winner_partition)], blind_append);
    let (relations, mut facts) = delete_partition_a(&url);
    facts.isolation_level = isolation_level;
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[rstest]
#[case::delete_of_read_file_conflicts(
    UpdateTableOperation::Delete,
    true,
    "a",
    conflict(2, Some("DELETE"), occ(OccCheck::ReadDelete))
)]
#[case::optimize_without_data_change_conflicts(
    UpdateTableOperation::Optimize,
    false,
    "a",
    conflict(2, Some("OPTIMIZE"), occ(OccCheck::ReadDelete))
)]
#[case::delete_of_unread_file_passes(UpdateTableOperation::Delete, true, "b", None)]
fn kernel_winner_remove_conflicts_only_when_t_read_the_file(
    #[case] operation: UpdateTableOperation,
    #[case] data_change: bool,
    #[case] removed_partition: &str,
    #[case] expected: Option<Conflict>,
) {
    // The winner removes the only file in `removed_partition`. Delta Spark's
    // `checkForDeletedFilesAgainstCurrentTxnReadFiles` fails T when the removed file is one that
    // T read. The `dataChange` flag of the winner does not matter. When T also reads and removes
    // the file, Delta Spark reports a read-delete, because it runs the read check first.
    let (url, _dir) = kernel_table(&[]);
    commit_remove(&url, operation, data_change, removed_partition);
    assert_eq!(scan_paths(&snapshot_at(&url, 2), None).len(), 1);
    let (relations, facts) = delete_partition_a(&url);
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[rstest]
// Delta Spark's `checkIfDomainMetadataConflict` fails T when both sides write one user domain.
#[case::same_domain_fails(
    |url: &str| commit_domain(url, "app.x"),
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".to_string()],
    conflict(2, Some("WRITE"), winner(WinnerRule::DomainConflict))
)]
// Delta Spark's `checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn` fails T when the
// winner writes an app ID that T read.
#[case::read_app_id_fails(
    |url: &str| commit_app_id(url, "app-a"),
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".to_string()],
    conflict(2, Some("WRITE"), winner(WinnerRule::TxnConflict))
)]
fn kernel_domain_or_txn_winner(
    #[case] commit: fn(&str) -> Version,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<Conflict>,
) {
    let (url, _dir) = kernel_table(&[]);
    commit(&url);
    let (relations, mut facts) = delete_partition_a(&url);
    adjust(&mut facts);
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[test]
fn kernel_row_tracking_winners_report_their_marks_and_timestamps() {
    // Both winners append to partition `b`, so T passes. Each summary carries the winner's
    // row-ID high-water mark and in-commit timestamp, which the snapshot at that version reports.
    let (url, _dir) = kernel_table(&[
        ("delta.enableRowTracking", "true"),
        ("delta.enableInCommitTimestamps", "true"),
    ]);
    assert_eq!(
        commit_adds(&url, "part", &[("part=b/winner-1.parquet", "b")], false),
        2
    );
    assert_eq!(
        commit_adds(&url, "part", &[("part=b/winner-2.parquet", "b")], false),
        3
    );
    let (relations, facts) = delete_partition_a(&url);
    let summaries = no_conflict_summaries(relations.detect(&facts, &winners_after(&url, 1)));
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
        // Kernel writes `isBlindAppend` only when it is true. An absent flag counts as not blind.
        assert_eq!(summary.is_blind_append, None);
        assert_eq!(
            summary.in_commit_timestamp,
            Some(snapshot.reader().get_timestamp(&engine).unwrap())
        );
        assert_eq!(
            summary.row_id_high_water_mark,
            snapshot
                .reader()
                .get_row_tracking_high_water_mark(&engine)
                .unwrap()
        );
    }
}

#[rstest]
#[case::predicate_on_physical_name_matches(
    "x",
    conflict(1, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
#[case::other_partition_value_passes("y", None)]
fn kernel_append_matches_physical_partition_column(
    #[case] predicate_value: &str,
    #[case] expected: Option<Conflict>,
) {
    // `partition_cm/name` uses name-mode column mapping, so the partition column has a physical
    // name. The winner adds a file under that name, and T's predicate must use it too.
    let (url, _dir) = fixture_table("partition_cm/name");
    let snapshot = snapshot_at(&url, 0);
    let physical = snapshot
        .schema()
        .field("category")
        .unwrap()
        .physical_name(ColumnMappingMode::Name)
        .to_string();
    commit_adds(&url, &physical, &[("added.parquet", "x")], false);
    let relations = LoserRelations::new(&[], &[]);
    let facts = partitioned_delete_facts(
        0,
        &relations.read_files,
        schema_ref! { nullable (physical.as_str()): STRING },
        Some(equals(&physical, predicate_value)),
    );
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

// === Fixture-table winners ===

#[rstest]
#[case::delete_of_partition_zero_conflicts_with_winner_delete(
    0,
    conflict(2, Some("DELETE"), occ(OccCheck::ReadDelete))
)]
#[case::delete_of_partition_one_conflicts_with_merge_append(
    1,
    conflict(1, Some("MERGE"), occ(OccCheck::ReadAppend))
)]
fn cdf_table_partitioned_delete(#[case] partition: i64, #[case] expected: Option<Conflict>) {
    // Fixture commits: 0 is a WRITE of four files in partitions 0 and 1. 1 is a MERGE that adds a
    // file to partition 1 and removes the two partition-1 files. 2 is a DELETE of partition 0.
    let (url, _tmp) = fixture_table("cdf-table-partitioned");
    let (relations, facts) = delete_selecting(
        &snapshot_at(&url, 0),
        schema_ref! { nullable "part": LONG },
        Some(equals("part", partition)),
    );
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[test]
fn winners_in_other_partitions_give_no_conflict() {
    // Version 2 is the only winner of T at version 1. It is a DELETE with no added files. Delta
    // Spark's checks find no overlap with T's files, so T passes.
    let (url, _tmp) = fixture_table("cdf-table-partitioned");
    let (relations, facts) = delete_selecting(
        &snapshot_at(&url, 1),
        schema_ref! { nullable "part": LONG },
        Some(equals("part", 1i64)),
    );
    let summaries = no_conflict_summaries(relations.detect(&facts, &winners_after(&url, 1)));
    let summary = only_summary(&summaries);
    assert_eq!(summary.version, 2);
    assert_eq!(summary.operation.as_deref(), Some("DELETE"));
    assert_eq!(summary.is_blind_append, Some(false));
}

#[rstest]
#[case::delete_conflicts_with_delete_removing_its_files(
    "cdf-table-delete-unconditional",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, Some("DELETE"), occ(OccCheck::ReadDelete))
)]
// Under WriteSerializable, a non-blind add conflicts on an unpartitioned table. Delta Spark has no
// partition predicate to filter it. A removed file that T read also conflicts. When both checks
// fire, the add check reports first.
#[case::delete_conflicts_with_non_blind_add_on_unpartitioned_table(
    "cdf-table-delete-conditional-two-rows",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, Some("DELETE"), occ(OccCheck::ReadAppend))
)]
// Version 1 of `type-widening` sets a table property. It upgrades the protocol and changes the
// metadata. Delta Spark rejects T at version 1 on its metadata rule. Kernel rejects T at the same
// version, but reports the protocol rule, because `WinnerRule` lists it first.
#[case::winner_that_changes_protocol_and_metadata_fails_at_that_version(
    "type-widening",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, Some("SET TBLPROPERTIES"), winner(WinnerRule::ProtocolChanged))
)]
// A DV update removes a file and adds it back with the same path. The re-added file is a non-blind
// add, so a DELETE conflicts on the add. An OPTIMIZE drops to SnapshotIsolation, so only the
// removal conflicts.
#[case::dv_update_with_commit_info_removes_read_file_for_optimize(
    "table-with-dv-small",
    0,
    make_optimize,
    conflict(1, Some("DELETE"), occ(OccCheck::ReadDelete))
)]
#[case::dv_update_with_commit_info_appends_for_delete(
    "table-with-dv-small",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, Some("DELETE"), occ(OccCheck::ReadAppend))
)]
#[case::dv_update_without_commit_info_removes_read_file_for_optimize(
    "cdf-table-with-dv",
    0,
    make_optimize,
    conflict(1, None, occ(OccCheck::ReadDelete))
)]
#[case::dv_update_without_commit_info_appends_for_delete(
    "cdf-table-with-dv",
    0,
    |_: &mut LoserFacts| {},
    conflict(1, None, occ(OccCheck::ReadAppend))
)]
// Version 3 is an OPTIMIZE with `dataChange=false`. It removes both files that T read at version 2.
// Delta Spark's read-delete check does not depend on `dataChange`, and an OPTIMIZE loser skips the
// add check.
#[case::optimize_removes_read_files_under_snapshot_isolation(
    "cdf-table-with-cdc-and-dvs",
    2,
    make_optimize,
    conflict(3, Some("OPTIMIZE"), occ(OccCheck::ReadDelete))
)]
// T reads the single file at version 3. Version 4 is a blind-append WRITE that adds a file. Under
// WriteSerializable, Delta Spark skips that add, so version 5 reports first. Version 5 is a
// non-blind DELETE that adds a file, which conflicts with T. Under Serializable, the blind add at
// version 4 counts.
#[case::write_serializable_skips_blind_append_winner(
    "cdf-table-with-cdc-and-dvs",
    3,
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::WriteSerializable,
    conflict(5, Some("DELETE"), occ(OccCheck::ReadAppend))
)]
#[case::serializable_counts_blind_append_winner(
    "cdf-table-with-cdc-and-dvs",
    3,
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::Serializable,
    conflict(4, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
fn fixture_loser_that_read_every_file(
    #[case] table: &str,
    #[case] read_version: Version,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<Conflict>,
) {
    // T reads and removes every file at `read_version`.
    let (url, _tmp) = fixture_table(table);
    let (relations, mut facts) = delete_all(&snapshot_at(&url, read_version));
    adjust(&mut facts);
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[test]
fn cdc_only_winners_pass_and_keep_their_operations() {
    // Each winner has only `cdc` actions, which Delta Spark does not count as adds or removes.
    let (url, _tmp) = fixture_table("cdf-table-update-ops");
    let (relations, facts) = delete_all(&snapshot_at(&url, 0));
    let summaries = no_conflict_summaries(relations.detect(&facts, &winners_after(&url, 0)));
    let operations = Vec::from_iter(summaries.iter().map(|s| s.operation.as_deref()));
    assert_eq!(operations, [Some("Manual Update"), Some("Manual Update")]);
    assert!(summaries.iter().all(|s| s.is_blind_append == Some(false)));
}

#[rstest]
#[case::optimize_of_partition_zero_conflicts_with_dv_update(
    make_optimize,
    0,
    conflict(1, Some("DELETE"), occ(OccCheck::ReadDelete))
)]
#[case::delete_of_partition_two_conflicts_with_dv_append(
    |_: &mut LoserFacts| {},
    2,
    conflict(2, Some("DELETE"), occ(OccCheck::ReadAppend))
)]
#[case::delete_of_partition_one_passes_because_no_winner_touches_it(
    |_: &mut LoserFacts| {},
    1,
    None
)]
fn partitioned_dv_winners_conflict_only_in_their_partitions(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] partition: i32,
    #[case] expected: Option<Conflict>,
) {
    // The winners on this golden table change only partitions 0, 2, 4, 6 and 8.
    let dir = ::test_utils::load_test_data("tests/golden_data", "dv-partitioned-with-checkpoint")
        .unwrap();
    let url = Url::from_directory_path(dir.path().join("dv-partitioned-with-checkpoint/delta"))
        .unwrap()
        .to_string();
    let (relations, mut facts) = delete_selecting(
        &snapshot_at(&url, 0),
        schema_ref! { nullable "part": INTEGER },
        Some(equals("part", partition)),
    );
    adjust(&mut facts);
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[rstest]
#[case::read_app_id_of_winner_fails(
    "my-app",
    "2021-02-01",
    conflict(1, Some("WRITE"), winner(WinnerRule::TxnConflict))
)]
#[case::unread_app_id_passes("other-app", "2021-02-03", None)]
fn fixture_txn_winner_of_read_app_id(
    #[case] read_app_id: &str,
    #[case] modified: &str,
    #[case] expected: Option<Conflict>,
) {
    // Version 1 of `app-txn-no-checkpoint` writes `txn` actions for `my-app` and `my-app2`. The
    // winner's `txn` rule fails T before the add checks run.
    let (url, _tmp) = fixture_table("app-txn-no-checkpoint");
    let (relations, mut facts) = delete_selecting(
        &snapshot_at(&url, 0),
        schema_ref! { nullable "modified": STRING },
        Some(equals("modified", modified)),
    );
    facts.read_app_ids = vec![read_app_id.to_string()];
    assert_eq!(detect_in_table(&relations, &facts, &url), expected);
}

#[test]
fn winner_rule_is_reported_before_add_check_of_earlier_winner() {
    // Version 1 adds a file that conflicts with T, and version 2 writes `new_app_2`, which T read.
    // Kernel reports version 2, because it checks every winner rule first. Delta Spark reports
    // version 1.
    let (url, _tmp) = fixture_table("app-txn-with-last-updated");
    let (relations, mut facts) = delete_all(&snapshot_at(&url, 0));
    facts.read_app_ids = vec!["new_app_2".to_string()];
    assert_eq!(
        detect_in_table(&relations, &facts, &url),
        conflict(2, Some("WRITE"), winner(WinnerRule::TxnConflict))
    );
}
