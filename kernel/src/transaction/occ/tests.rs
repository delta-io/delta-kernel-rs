use std::sync::Arc;

use rstest::rstest;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::decision::{ConflictKind, WinnerRule};
use super::occ_plan::{OccBranchVersions, OccCheck};
use super::winner_check_plan::ROW_TRACKING_BACKFILL_OPERATION;
use super::*;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::{ScopedSyncPlanExecutor, SyncPlanExecutor};
use crate::expressions::{Predicate, Scalar, StructData, col, lit};
use crate::plans::{PlanBuilder, PlanExecutor};
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::{DataType, PrimitiveType, StructField, schema_ref};
use crate::unit_test_utils::assert_result_error_with_message;

const READ_VERSION: Version = 6;
const DELETED_DATE: &str = "2026-10-01";
const OTHER_DATE: &str = "2026-10-02";
/// The file that T reads and removes.
const DELETED_FILE: &str = "date=2026-10-01/part-a.parquet";

// === Action JSON ===

fn commit_info(operation: &str) -> Value {
    json!({"commitInfo": {"timestamp": 0, "operation": operation, "isBlindAppend": false}})
}

fn blind_append_commit_info() -> Value {
    json!({"commitInfo": {"timestamp": 0, "operation": "WRITE", "isBlindAppend": true}})
}

fn identity_only_commit_info(tag: &str) -> Value {
    json!({"commitInfo": {
        "timestamp": 0,
        "operation": "UPDATE SCHEMA",
        "isBlindAppend": false,
        "tags": {"delta.identity.schemaUpdate": tag},
    }})
}

fn add(path: &str, date: &str) -> Value {
    add_with_partition_values(path, json!({"date": date}))
}

fn add_with_partition_values(path: &str, partition_values: Value) -> Value {
    json!({"add": {
        "path": path,
        "partitionValues": partition_values,
        "size": 1,
        "modificationTime": 0,
        "dataChange": true,
    }})
}

fn remove(path: &str, data_change: bool) -> Value {
    json!({"remove": {"path": path, "deletionTimestamp": 0, "dataChange": data_change}})
}

fn protocol() -> Value {
    json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
}

fn metadata() -> Value {
    json!({"metaData": {
        "id": "test-table",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": r#"{"type":"struct","fields":[]}"#,
        "partitionColumns": ["date"],
        "configuration": {},
        "createdTime": 0,
    }})
}

fn domain(name: &str, configuration: &str) -> Value {
    json!({"domainMetadata": {"domain": name, "configuration": configuration, "removed": false}})
}

fn removed_domain(name: &str) -> Value {
    json!({"domainMetadata": {"domain": name, "configuration": "{}", "removed": true}})
}

fn row_tracking(high_water_mark: i64) -> Value {
    domain(
        ROW_TRACKING_DOMAIN_NAME,
        &format!(r#"{{"rowIdHighWaterMark":{high_water_mark}}}"#),
    )
}

fn txn(app_id: &str) -> Value {
    json!({"txn": {"appId": app_id, "version": 3, "lastUpdated": 0}})
}

// === Fixtures ===

/// A table whose `_delta_log` holds only the given hand-written commits.
struct TestTable {
    _dir: TempDir,
    root: String,
}

impl TestTable {
    fn new(commits: &[(Version, Vec<Value>)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("_delta_log");
        std::fs::create_dir(&log).unwrap();
        for (version, actions) in commits {
            let lines = Vec::from_iter(actions.iter().map(Value::to_string)).join("\n");
            std::fs::write(log.join(format!("{version:020}.json")), lines).unwrap();
        }
        let root = url::Url::from_directory_path(dir.path())
            .unwrap()
            .to_string();
        Self { _dir: dir, root }
    }

    fn winners(&self, read_version: Version) -> Vec<WinningCommit> {
        let range = CommitRange::builder_for(&self.root, read_version + 1)
            .build(&SyncEngine::new())
            .unwrap();
        WinningCommit::from_commit_range(&range).unwrap()
    }
}

/// A read relation with no files, for tests that inspect only T's facts.
fn no_read_files() -> RelationRef {
    RelationRef::new("read_files", schema_ref! { not_null "path": STRING })
}

/// A partitioned DELETE of `DELETED_DATE` that reads the files in `read_files`. It runs as
/// copy-on-write under WriteSerializable.
fn delete_facts(read_files: &RelationRef) -> LoserFacts {
    LoserFacts {
        read_version: READ_VERSION,
        isolation_level: IsolationLevel::WriteSerializable,
        is_blind_append: false,
        has_data_change: true,
        operation_changes_data: true,
        changes_metadata: false,
        has_other_actions: false,
        is_row_tracking_backfill: false,
        rewrites_files: true,
        written_domains: vec![],
        read_app_ids: vec![],
        read_scope: ReadScope::Files {
            read_files: read_files.clone(),
            partition_schema: schema_ref! { nullable "date": DATE },
            partition_predicate: Some(Arc::new(
                col!("date").eq(lit(PrimitiveType::Date.parse_scalar(DELETED_DATE).unwrap())),
            )),
            map_to_struct_options: MapToStructOptions::default(),
        },
    }
}

/// T's file actions and read files, retained in a fresh scoped executor.
struct LoserRelations {
    executor: ScopedSyncPlanExecutor,
    file_actions: RelationRef,
    read_files: RelationRef,
}

impl LoserRelations {
    fn new(removed: &[&str], read: &[&str]) -> Self {
        let executor = SyncPlanExecutor::default().get_scoped();
        let path_field = StructField::not_null("path", DataType::STRING);
        let removes = Vec::from_iter(removed.iter().map(|&path| {
            let remove = StructData::try_new(vec![path_field.clone()], vec![path.into()]).unwrap();
            vec![Scalar::Struct(remove)]
        }));
        let file_actions = schema_ref! { nullable "remove": { not_null "path": STRING } };
        let file_actions = retain(&executor, "file_actions", file_actions, removes);
        let reads = Vec::from_iter(read.iter().map(|&path| vec![Scalar::from(path)]));
        let read_files = schema_ref! { not_null "path": STRING };
        let read_files = retain(&executor, "read_files", read_files, reads);
        Self {
            executor,
            file_actions,
            read_files,
        }
    }

    /// The partitioned DELETE, reading the files in `self.read_files`.
    fn delete_facts(&self) -> LoserFacts {
        delete_facts(&self.read_files)
    }

    fn detect(&self, facts: &LoserFacts, winners: &[WinningCommit]) -> ConflictDetection {
        detect_conflicts(&self.executor, facts, winners, &self.file_actions).unwrap()
    }
}

fn retain(
    executor: &ScopedSyncPlanExecutor,
    name: &str,
    schema: SchemaRef,
    rows: Vec<Vec<Scalar>>,
) -> RelationRef {
    let plan = PlanBuilder::values(schema, rows).unwrap().build().unwrap();
    executor.execute_and_retain(name, plan).unwrap()
}

/// Runs conflict detection for the partitioned DELETE over `commits`.
fn detect_delete(commits: &[(Version, Vec<Value>)]) -> Option<(Version, ConflictKind)> {
    detect_with(|_| {}, &[DELETED_FILE], &[DELETED_FILE], commits)
}

/// Runs conflict detection for the partitioned DELETE after `adjust`.
fn detect_with(
    adjust: impl FnOnce(&mut LoserFacts),
    removed: &[&str],
    read: &[&str],
    commits: &[(Version, Vec<Value>)],
) -> Option<(Version, ConflictKind)> {
    let table = TestTable::new(commits);
    let winners = table.winners(READ_VERSION);
    let relations = LoserRelations::new(removed, read);
    let mut facts = relations.delete_facts();
    adjust(&mut facts);
    conflict_of(relations.detect(&facts, &winners))
}

/// The conflict in `detection`, or `None` when no winner conflicts.
fn conflict_of(detection: ConflictDetection) -> Option<(Version, ConflictKind)> {
    match detection {
        ConflictDetection::NoConflict(_) => None,
        ConflictDetection::Conflict(conflict) => Some((conflict.version, conflict.kind)),
    }
}

fn winner_summaries(commits: &[(Version, Vec<Value>)]) -> Vec<WinnerSummary> {
    let table = TestTable::new(commits);
    let facts = delete_facts(&no_read_files());
    let plan = winner_check_plan(&facts, &table.winners(READ_VERSION))
        .unwrap()
        .unwrap();
    let executor = SyncPlanExecutor::default();
    read_winner_summaries(executor.execute_op(Operation::QueryPlan(plan)).unwrap()).unwrap()
}

fn winner_rule(version: Version, rule: WinnerRule) -> Option<(Version, ConflictKind)> {
    Some((version, ConflictKind::Winner(rule)))
}

fn occ_check(version: Version, check: OccCheck) -> Option<(Version, ConflictKind)> {
    Some((version, ConflictKind::Occ(check)))
}

// === Partitioned DELETE ===

#[test]
fn partitioned_delete_passes_with_row_tracking_marks_of_each_winner() {
    let table = TestTable::new(&[
        (
            7,
            vec![
                commit_info("WRITE"),
                add("f7", OTHER_DATE),
                row_tracking(1099),
            ],
        ),
        (
            8,
            vec![
                commit_info("WRITE"),
                add("f8", OTHER_DATE),
                row_tracking(1119),
            ],
        ),
    ]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let ConflictDetection::NoConflict(summaries) =
        relations.detect(&relations.delete_facts(), &table.winners(READ_VERSION))
    else {
        panic!("expected no conflict");
    };
    let marks = Vec::from_iter(
        summaries
            .iter()
            .map(|summary| (summary.version, summary.row_id_high_water_mark)),
    );
    assert_eq!(marks, [(7, Some(1099)), (8, Some(1119))]);
    assert!(
        summaries
            .iter()
            .all(|summary| summary.violations.is_empty())
    );
}

#[rstest]
#[case::add_in_deleted_partition_fails(true)]
#[case::add_without_data_change_in_deleted_partition_fails(false)]
fn partitioned_delete_read_append(#[case] data_change: bool) {
    let mut deleted_partition_add = add("f7", DELETED_DATE);
    deleted_partition_add["add"]["dataChange"] = json!(data_change);
    let conflict = detect_delete(&[
        (7, vec![commit_info("WRITE"), deleted_partition_add]),
        (8, vec![commit_info("WRITE"), add("f8", DELETED_DATE)]),
    ]);
    assert_eq!(conflict, occ_check(7, OccCheck::ReadAppend));
}

// === Winner check rules ===

#[rstest]
#[case::protocol_fails(
    vec![protocol()],
    |_: &mut LoserFacts| {},
    winner_rule(7, WinnerRule::ProtocolChanged),
)]
#[case::no_protocol_passes(vec![], |_: &mut LoserFacts| {}, None)]
#[case::metadata_fails(
    vec![metadata()],
    |_: &mut LoserFacts| {},
    winner_rule(7, WinnerRule::MetadataChanged),
)]
#[case::domain_written_by_both_fails(
    vec![domain("app.x", "{}")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()],
    winner_rule(7, WinnerRule::DomainConflict),
)]
#[case::removed_domain_written_by_both_fails(
    vec![removed_domain("app.x")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()],
    winner_rule(7, WinnerRule::DomainConflict),
)]
#[case::second_written_domain_fails(
    vec![domain("app.y", "{}")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into(), "app.y".into()],
    winner_rule(7, WinnerRule::DomainConflict),
)]
#[case::domain_written_by_winner_only_passes(
    vec![domain("app.x", "{}")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.y".into()],
    None,
)]
#[case::row_tracking_domain_on_both_sides_passes(
    vec![row_tracking(1099)],
    |facts: &mut LoserFacts| facts.written_domains = vec![ROW_TRACKING_DOMAIN_NAME.into()],
    None,
)]
#[case::txn_for_read_app_id_fails(
    vec![txn("app-a")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".into()],
    winner_rule(7, WinnerRule::TxnConflict),
)]
#[case::second_read_app_id_fails(
    vec![txn("app-b")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".into(), "app-b".into()],
    winner_rule(7, WinnerRule::TxnConflict),
)]
#[case::txn_for_other_app_id_passes(
    vec![txn("app-a")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-b".into()],
    None,
)]
#[case::protocol_reported_before_metadata(
    vec![protocol(), metadata()],
    |_: &mut LoserFacts| {},
    winner_rule(7, WinnerRule::ProtocolChanged),
)]
fn winner_rule_with_write_commit(
    #[case] actions: Vec<Value>,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    let mut winner = vec![commit_info("WRITE")];
    winner.extend(actions);
    let conflict = detect_with(adjust, &[DELETED_FILE], &[DELETED_FILE], &[(7, winner)]);
    assert_eq!(conflict, expected);
}

#[rstest]
#[case::identity_only_passes_without_metadata_change("true", false, None)]
#[case::identity_only_fails_with_metadata_change(
    "true",
    true,
    winner_rule(7, WinnerRule::MetadataChanged)
)]
#[case::false_tag_fails("false", false, winner_rule(7, WinnerRule::MetadataChanged))]
#[case::uppercase_tag_fails("TRUE", false, winner_rule(7, WinnerRule::MetadataChanged))]
fn identity_only_metadata_winner(
    #[case] tag: &str,
    #[case] changes_metadata: bool,
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    let winner = vec![identity_only_commit_info(tag), metadata()];
    let conflict = detect_with(
        |facts| facts.changes_metadata = changes_metadata,
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(7, winner)],
    );
    assert_eq!(conflict, expected);
}

#[test]
fn metadata_without_canonical_required_fields_still_fails_metadata_changed() {
    let commits = [(7, vec![json!({"metaData": {"id": "test-table"}})])];
    let summaries = winner_summaries(&commits);
    let [summary] = summaries.as_slice() else {
        panic!("expected one winner row, got {summaries:?}");
    };
    assert_eq!(summary.version, 7);
    assert_eq!(summary.violations, [WinnerRule::MetadataChanged]);
    assert_eq!(
        detect_delete(&commits),
        winner_rule(7, WinnerRule::MetadataChanged)
    );
}

#[rstest]
#[case::loser_rewrites_files_fails(true, winner_rule(7, WinnerRule::BackfillConflict))]
#[case::loser_rewrites_no_files_passes(false, None)]
fn row_tracking_backfill_winner(
    #[case] rewrites_files: bool,
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    let removed: &[&str] = match rewrites_files {
        true => &[DELETED_FILE],
        false => &[],
    };
    // The backfill adds a file in the deleted partition. `read_append` skips backfill winners, so
    // only the backfill rule can fail T.
    let winner = vec![
        commit_info(ROW_TRACKING_BACKFILL_OPERATION),
        add("date=2026-10-01/part-c.parquet", DELETED_DATE),
    ];
    let conflict = detect_with(
        |facts| facts.rewrites_files = rewrites_files,
        removed,
        &[DELETED_FILE],
        &[(7, winner)],
    );
    assert_eq!(conflict, expected);
}

// === OCC checks ===

#[rstest]
#[case::read_delete(&[], &[DELETED_FILE], OccCheck::ReadDelete)]
#[case::delete_delete(&[DELETED_FILE], &[], OccCheck::DeleteDelete)]
fn winner_remove_conflicts_whatever_its_data_change(
    #[case] removed: &[&str],
    #[case] read: &[&str],
    #[case] check: OccCheck,
    #[values(true, false)] data_change: bool,
) {
    let winner = vec![commit_info("OPTIMIZE"), remove(DELETED_FILE, data_change)];
    let conflict = detect_with(|_| {}, removed, read, &[(7, winner)]);
    assert_eq!(conflict, occ_check(7, check));
}

#[test]
fn remove_of_unrelated_file_passes() {
    let winner = vec![
        commit_info("DELETE"),
        remove("date=2026-10-02/part-b.parquet", true),
    ];
    assert_eq!(detect_delete(&[(7, winner)]), None);
}

#[test]
fn winner_without_commit_info_gets_file_checks() {
    let commits = [(7, vec![remove(DELETED_FILE, true)])];
    let summaries = winner_summaries(&commits);
    let [summary] = summaries.as_slice() else {
        panic!("expected one winner row, got {summaries:?}");
    };
    assert_eq!((summary.version, summary.operation.as_deref()), (7, None));
    assert_eq!(summary.is_blind_append, None);
    assert_eq!(detect_delete(&commits), occ_check(7, OccCheck::ReadDelete));
}

#[rstest]
#[case::serializable_fails(IsolationLevel::Serializable, occ_check(7, OccCheck::ReadAppend))]
#[case::write_serializable_passes(IsolationLevel::WriteSerializable, None)]
fn blind_append_winner(
    #[case] isolation_level: IsolationLevel,
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    let winner = vec![blind_append_commit_info(), add("f7", DELETED_DATE)];
    let conflict = detect_with(
        |facts| facts.isolation_level = isolation_level,
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(7, winner)],
    );
    assert_eq!(conflict, expected);
}

#[test]
fn winner_without_blind_append_flag_is_checked_under_write_serializable() {
    let winner = vec![
        json!({"commitInfo": {"timestamp": 0, "operation": "WRITE"}}),
        add("f7", DELETED_DATE),
    ];
    assert_eq!(
        detect_delete(&[(7, winner)]),
        occ_check(7, OccCheck::ReadAppend)
    );
}

#[test]
fn optimize_loser_drops_to_snapshot_isolation_and_passes_read_append() {
    let optimize = |facts: &mut LoserFacts| {
        facts.has_data_change = false;
        facts.operation_changes_data = false;
    };
    let mut facts = delete_facts(&no_read_files());
    optimize(&mut facts);
    assert_eq!(
        facts.effective_isolation_level(),
        IsolationLevel::SnapshotIsolation
    );
    let winner = vec![commit_info("WRITE"), add("f7", DELETED_DATE)];
    let conflict = detect_with(optimize, &[DELETED_FILE], &[DELETED_FILE], &[(7, winner)]);
    assert_eq!(conflict, None);
}

#[rstest]
#[case::metadata_change(|facts: &mut LoserFacts| facts.changes_metadata = true)]
#[case::domain_other_than_row_tracking(
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()]
)]
fn other_action_keeps_write_serializable(#[case] adjust: fn(&mut LoserFacts)) {
    let mut facts = delete_facts(&no_read_files());
    facts.has_data_change = false;
    facts.operation_changes_data = false;
    facts.has_other_actions = false;
    adjust(&mut facts);
    assert_eq!(
        facts.effective_isolation_level(),
        IsolationLevel::WriteSerializable
    );
}

#[rstest]
#[case::add_in_other_partition(add("f7", OTHER_DATE), OccCheck::ReadAppend)]
#[case::remove_of_unread_file(remove("date=2026-10-02/part-b.parquet", true), OccCheck::ReadDelete)]
fn whole_table_read_conflicts_with_any_file_change(#[case] action: Value, #[case] check: OccCheck) {
    let winner = vec![commit_info("WRITE"), action];
    let conflict = detect_with(
        |facts| facts.read_scope = ReadScope::WholeTable,
        &[DELETED_FILE],
        &[],
        &[(7, winner)],
    );
    assert_eq!(conflict, occ_check(7, check));
}

// === Reporting order ===

#[test]
fn earliest_winner_rule_violation_is_reported() {
    let conflict = detect_delete(&[
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ]);
    assert_eq!(conflict, winner_rule(7, WinnerRule::MetadataChanged));
}

#[test]
fn winner_rule_is_reported_before_earlier_occ_violation() {
    let conflict = detect_delete(&[
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ]);
    assert_eq!(conflict, winner_rule(8, WinnerRule::ProtocolChanged));
}

#[test]
fn winner_rule_is_reported_before_missing_row_tracking_mark() {
    let conflict = detect_delete(&[
        (7, vec![commit_info("WRITE"), metadata()]),
        (
            8,
            vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")],
        ),
    ]);
    assert_eq!(conflict, winner_rule(7, WinnerRule::MetadataChanged));
}

#[test]
fn earliest_occ_version_then_check_order_is_reported() {
    // Version 9 beats version 10 because the versions compare as numbers. Within version 9,
    // ReadDelete beats DeleteDelete.
    let table = TestTable::new(&[
        (9, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (10, vec![commit_info("WRITE"), add("f10", DELETED_DATE)]),
    ]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let mut facts = relations.delete_facts();
    facts.read_version = 8;
    let ConflictDetection::Conflict(conflict) = relations.detect(&facts, &table.winners(8)) else {
        panic!("expected a conflict");
    };
    assert_eq!(conflict.version, 9);
    assert_eq!(conflict.kind, ConflictKind::Occ(OccCheck::ReadDelete));
    assert_eq!(conflict.operation.as_deref(), Some("DELETE"));
}

#[test]
fn checks_of_one_version_report_read_append_first() {
    let conflict = detect_delete(&[(
        7,
        vec![
            commit_info("WRITE"),
            remove(DELETED_FILE, true),
            add("f7", DELETED_DATE),
        ],
    )]);
    assert_eq!(conflict, occ_check(7, OccCheck::ReadAppend));
}

// === Plan presence ===

#[test]
fn no_plans_without_winners_or_branches() {
    let relations = LoserRelations::new(&[], &[]);
    let facts = relations.delete_facts();
    assert!(winner_check_plan(&facts, &[]).unwrap().is_none());
    let plan = occ_plan(
        &facts,
        &[],
        &OccBranchVersions::default(),
        &relations.file_actions,
    )
    .unwrap();
    assert!(plan.is_none());
}

#[test]
fn detect_conflicts_without_winners_has_no_conflict() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let ConflictDetection::NoConflict(summaries) = relations.detect(&relations.delete_facts(), &[])
    else {
        panic!("expected no conflict");
    };
    assert!(summaries.is_empty());
}

// === Winner rows ===

#[test]
fn winner_row_reads_commit_info_timestamp_and_blind_append() {
    let info = json!({"commitInfo": {
        "timestamp": 0,
        "operation": "WRITE",
        "isBlindAppend": true,
        "inCommitTimestamp": 1_700_000_000_000i64,
    }});
    let summaries = winner_summaries(&[(7, vec![info, add("f7", OTHER_DATE)])]);
    let [summary] = summaries.as_slice() else {
        panic!("expected one winner row, got {summaries:?}");
    };
    assert_eq!(summary.in_commit_timestamp, Some(1_700_000_000_000));
    assert_eq!(summary.is_blind_append, Some(true));
}

#[rstest]
#[case::malformed_json("{not json")]
#[case::missing_mark("{}")]
fn row_tracking_configuration_without_mark_fails_detection(#[case] configuration: &str) {
    let table = TestTable::new(&[(
        7,
        vec![
            commit_info("WRITE"),
            add("f7", OTHER_DATE),
            domain(ROW_TRACKING_DOMAIN_NAME, configuration),
        ],
    )]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let result = detect_conflicts(
        &relations.executor,
        &relations.delete_facts(),
        &table.winners(READ_VERSION),
        &relations.file_actions,
    );
    assert_result_error_with_message(result, "without a readable rowIdHighWaterMark");
}

#[test]
fn winner_commit_without_actions_fails_detection() {
    let table = TestTable::new(&[(7, vec![])]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let error = detect_conflicts(
        &relations.executor,
        &relations.delete_facts(),
        &table.winners(READ_VERSION),
        &relations.file_actions,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Winner check rows do not match the winning commits"),
        "{error}"
    );
}

// === Branch version selection ===

fn summary(version: Version, operation: &str, is_blind_append: bool) -> WinnerSummary {
    WinnerSummary {
        version,
        operation: Some(operation.to_string()),
        is_blind_append: Some(is_blind_append),
        in_commit_timestamp: None,
        row_id_high_water_mark: None,
        row_tracking_count: 0,
        violations: vec![],
    }
}

#[rstest]
#[case::write_serializable_skips_blind_append_winner(|_: &mut LoserFacts| {}, vec![7, 9])]
#[case::write_serializable_metadata_change_checks_blind_append(
    |facts: &mut LoserFacts| facts.changes_metadata = true,
    vec![7, 8, 9],
)]
#[case::serializable_checks_blind_append(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::Serializable,
    vec![7, 8, 9],
)]
#[case::blind_append_loser_skips_all(|facts: &mut LoserFacts| facts.is_blind_append = true, vec![])]
#[case::backfill_loser_skips_all(
    |facts: &mut LoserFacts| facts.is_row_tracking_backfill = true,
    vec![],
)]
#[case::snapshot_isolation_skips_all(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::SnapshotIsolation,
    vec![],
)]
#[case::other_actions_keep_isolation(
    |facts: &mut LoserFacts| {
        facts.has_data_change = false;
        facts.operation_changes_data = false;
        facts.has_other_actions = true;
    },
    vec![7, 9],
)]
#[case::write_serializable_data_changing_operation_keeps_isolation(
    |facts: &mut LoserFacts| facts.has_data_change = false,
    vec![7, 9],
)]
#[case::serializable_without_data_change_drops_isolation(
    |facts: &mut LoserFacts| {
        facts.isolation_level = IsolationLevel::Serializable;
        facts.has_data_change = false;
    },
    vec![],
)]
fn read_append_versions(#[case] adjust: fn(&mut LoserFacts), #[case] expected: Vec<Version>) {
    let mut facts = delete_facts(&no_read_files());
    adjust(&mut facts);
    let summaries = [
        summary(7, "WRITE", false),
        summary(8, "WRITE", true),
        summary(9, "MERGE", false),
        summary(10, ROW_TRACKING_BACKFILL_OPERATION, false),
    ];
    let branches = select_occ_branch_versions(&facts, &summaries);
    assert_eq!(branches.read_append, expected);
    assert_eq!(branches.read_delete, [7, 8, 9, 10]);
    assert_eq!(branches.delete_delete, [7, 8, 9, 10]);
}

// === Winner coverage ===

fn winning_commit(version: Version) -> WinningCommit {
    let location = url::Url::parse("memory:///_delta_log/commit.json").unwrap();
    WinningCommit {
        version,
        file: FileMeta {
            location,
            last_modified: 0,
            size: 0,
        },
    }
}

#[rstest]
#[case::no_winners_pass(&[], true)]
#[case::winners_from_read_version_plus_one_pass(&[READ_VERSION + 1, READ_VERSION + 2], true)]
#[case::first_winner_not_after_read_version_fails(&[READ_VERSION + 2, READ_VERSION + 3], false)]
#[case::gap_between_winners_fails(&[READ_VERSION + 1, READ_VERSION + 3], false)]
fn winners_must_follow_read_version(#[case] winners: &[Version], #[case] valid: bool) {
    let winners = Vec::from_iter(winners.iter().map(|&version| winning_commit(version)));
    let result = check_winners_follow_read_version(READ_VERSION, &winners);
    assert_eq!(result.is_ok(), valid, "{result:?}");
}

#[rstest]
#[case::contiguous_rows_pass(&[7, 8], &[7, 8], true)]
#[case::no_winners_pass(&[], &[], true)]
#[case::missing_row_fails(&[7, 8], &[7], false)]
#[case::extra_row_fails(&[7], &[7, 8], false)]
fn winner_rows_must_match_winners(
    #[case] winners: &[Version],
    #[case] rows: &[Version],
    #[case] valid: bool,
) {
    let winners = Vec::from_iter(winners.iter().map(|&version| winning_commit(version)));
    let summaries = Vec::from_iter(rows.iter().map(|&version| summary(version, "WRITE", false)));
    let result = check_winner_rows(&winners, &summaries);
    assert_eq!(result.is_ok(), valid, "{result:?}");
}

// === Partition values ===

#[test]
fn null_partition_value_does_not_match_equality_predicate() {
    let winner = vec![
        commit_info("WRITE"),
        add_with_partition_values("f7", json!({"date": null})),
    ];
    assert_eq!(detect_delete(&[(7, winner)]), None);
}

#[rstest]
#[case::utc_by_default_matches_utc_instant(
    "2026-10-02 06:30:00",
    None,
    occ_check(7, OccCheck::ReadAppend)
)]
#[case::utc_by_default_ignores_local_offset("2026-10-01 23:30:00", None, None)]
#[case::connector_timezone_matches_local_time(
    "2026-10-01 23:30:00",
    Some("America/Los_Angeles"),
    occ_check(7, OccCheck::ReadAppend)
)]
fn timestamp_partition_is_read_in_connector_timezone(
    #[case] partition_value: &str,
    #[case] timezone: Option<&str>,
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    // 2026-10-01 23:30 in Los Angeles (PDT) is 2026-10-02 06:30 UTC.
    let table = TestTable::new(&[(
        7,
        vec![
            commit_info("WRITE"),
            add_with_partition_values("f7", json!({"ts": partition_value})),
        ],
    )]);
    let instant = PrimitiveType::Timestamp
        .parse_scalar("2026-10-02T06:30:00Z")
        .unwrap();
    let options = match timezone {
        Some(zone) => MapToStructOptions::default().with_timestamp_timezone(zone),
        None => MapToStructOptions::default(),
    };
    let relations = LoserRelations::new(&[], &[]);
    let mut facts = relations.delete_facts();
    facts.read_scope = ReadScope::Files {
        read_files: relations.read_files.clone(),
        partition_schema: schema_ref! { nullable "ts": TIMESTAMP },
        partition_predicate: Some(Arc::new(col!("ts").eq(lit(instant)))),
        map_to_struct_options: options,
    };
    let conflict = conflict_of(relations.detect(&facts, &table.winners(READ_VERSION)));
    assert_eq!(conflict, expected);
}

/// Sets the partition predicate of a files-scoped T.
fn set_partition_predicate(facts: &mut LoserFacts, predicate: Option<PredicateRef>) {
    let ReadScope::Files {
        partition_predicate,
        ..
    } = &mut facts.read_scope
    else {
        panic!("expected a files-scoped read");
    };
    *partition_predicate = predicate;
}

#[rstest]
#[case::default_predicate_matches_its_partition(
    |_: &mut LoserFacts| {},
    DELETED_DATE,
    occ_check(7, OccCheck::ReadAppend),
)]
#[case::default_predicate_skips_other_partition(|_: &mut LoserFacts| {}, OTHER_DATE, None)]
#[case::none_matches_every_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, None),
    OTHER_DATE,
    occ_check(7, OccCheck::ReadAppend),
)]
#[case::false_matches_no_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, Some(Arc::new(Predicate::FALSE))),
    DELETED_DATE,
    None,
)]
fn partition_predicate_selects_adds(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] date: &str,
    #[case] expected: Option<(Version, ConflictKind)>,
) {
    let conflict = detect_with(
        adjust,
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(7, vec![commit_info("WRITE"), add("f7", date)])],
    );
    assert_eq!(conflict, expected);
}

#[test]
fn unpartitioned_table_ignores_partition_predicate() {
    let conflict = detect_with(
        |facts| {
            let ReadScope::Files {
                partition_schema,
                partition_predicate,
                ..
            } = &mut facts.read_scope
            else {
                panic!("expected a files-scoped read");
            };
            *partition_schema = schema_ref! {};
            *partition_predicate = Some(Arc::new(Predicate::FALSE));
        },
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(
            7,
            vec![
                commit_info("WRITE"),
                add_with_partition_values("f7", json!({})),
            ],
        )],
    );
    assert_eq!(conflict, occ_check(7, OccCheck::ReadAppend));
}

#[test]
fn partition_values_match_physical_column_names() {
    let relations = LoserRelations::new(&[], &[]);
    let mut facts = relations.delete_facts();
    facts.read_scope = ReadScope::Files {
        read_files: relations.read_files.clone(),
        partition_schema: schema_ref! { nullable "col_7f3a": DATE },
        partition_predicate: Some(Arc::new(
            col!("col_7f3a").eq(lit(PrimitiveType::Date.parse_scalar(DELETED_DATE).unwrap())),
        )),
        map_to_struct_options: MapToStructOptions::default(),
    };
    let table = TestTable::new(&[(
        7,
        vec![
            commit_info("WRITE"),
            add_with_partition_values("f7", json!({"col_7f3a": DELETED_DATE})),
        ],
    )]);
    let conflict = conflict_of(relations.detect(&facts, &table.winners(READ_VERSION)));
    assert_eq!(conflict, occ_check(7, OccCheck::ReadAppend));
}
