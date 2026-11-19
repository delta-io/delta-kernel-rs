use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ::test_utils::add_commit;
use futures::executor::block_on;
use rstest::rstest;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::decision::{Conflict, ConflictKind, WinnerRule};
use super::occ_plan::OccCheck;
use super::winner_check_plan::ROW_TRACKING_BACKFILL_OPERATION;
use super::*;
use crate::actions::{Add, CommitInfo, DomainMetadata, Metadata, Protocol, Remove, SetTransaction};
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::{ScopedSyncPlanExecutor, SyncPlanExecutor};
use crate::expressions::{Predicate, Scalar, StructData, col, lit};
use crate::object_store::local::LocalFileSystem;
use crate::plans::{Plan, PlanBuilder, PlanExecutor, PlanResult};
use crate::row_tracking::{ROW_TRACKING_DOMAIN_NAME, RowTrackingDomainMetadata};
use crate::schema::{DataType, PrimitiveType, StructField, schema, schema_ref};
use crate::unit_test_utils::{Action, assert_result_error_with_message};

const READ_VERSION: Version = 6;
const DELETED_DATE: &str = "2026-10-01";
const OTHER_DATE: &str = "2026-10-02";
/// The file that T reads and removes.
const DELETED_FILE: &str = "date=2026-10-01/part-a.parquet";

// === Actions ===

/// One commit line, serialised from Kernel's action struct.
fn line(action: Action) -> Value {
    serde_json::to_value(action).unwrap()
}

/// The `commitInfo` of a commit that runs `operation` and isn't a blind append.
fn base_commit_info(operation: &str) -> CommitInfo {
    CommitInfo {
        timestamp: Some(0),
        operation: Some(operation.into()),
        is_blind_append: Some(false),
        ..Default::default()
    }
}

fn commit_info(operation: &str) -> Value {
    line(Action::CommitInfo(base_commit_info(operation)))
}

fn identity_only_commit_info(tag: &str) -> Value {
    let tags = HashMap::from([("delta.identity.schemaUpdate".into(), Some(tag.into()))]);
    line(Action::CommitInfo(CommitInfo {
        tags: Some(tags),
        ..base_commit_info("UPDATE SCHEMA")
    }))
}

/// An add of `path` with data change, in the partition given by `partition_values`.
fn base_add(path: &str, partition_values: &[(&str, &str)]) -> Add {
    let partition_values = partition_values.iter().map(|&(k, v)| (k.into(), v.into()));
    Add {
        path: path.into(),
        partition_values: HashMap::from_iter(partition_values),
        size: 1,
        data_change: true,
        ..Default::default()
    }
}

fn add(path: &str, date: &str) -> Value {
    line(Action::Add(base_add(path, &[("date", date)])))
}

fn remove(path: &str, data_change: bool) -> Value {
    line(Action::Remove(Remove {
        path: path.into(),
        deletion_timestamp: Some(0),
        data_change,
        ..Default::default()
    }))
}

fn protocol() -> Value {
    line(Action::Protocol(Protocol::try_new_legacy(1, 2).unwrap()))
}

fn metadata() -> Value {
    let schema = schema_ref! { nullable "date": DATE };
    let metadata =
        Metadata::try_new(None, None, schema, vec!["date".into()], 0, HashMap::new()).unwrap();
    line(Action::Metadata(metadata))
}

fn domain(name: &str, configuration: &str) -> Value {
    line(Action::DomainMetadata(DomainMetadata::new(
        name.into(),
        configuration.into(),
    )))
}

fn removed_domain(name: &str) -> Value {
    line(Action::DomainMetadata(DomainMetadata::remove(
        name.into(),
        "{}".into(),
    )))
}

fn row_tracking(high_water_mark: i64) -> Value {
    let domain = RowTrackingDomainMetadata::new(high_water_mark);
    line(Action::DomainMetadata(domain.try_into().unwrap()))
}

fn txn(app_id: &str) -> Value {
    line(Action::Txn(SetTransaction::new(app_id.into(), 3, Some(0))))
}

// === Fixtures ===

/// A table whose `_delta_log` holds only the given commits.
struct TestTable {
    _dir: TempDir,
    root: String,
}

impl TestTable {
    fn new(commits: &[(Version, Vec<Value>)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = url::Url::from_directory_path(dir.path())
            .unwrap()
            .to_string();
        let store = LocalFileSystem::new();
        for (version, actions) in commits {
            let body = Vec::from_iter(actions.iter().map(Value::to_string)).join("\n");
            block_on(add_commit(&root, &store, *version, body)).unwrap();
        }
        Self { _dir: dir, root }
    }

    fn winners(&self, read_version: Version) -> Vec<WinningCommit> {
        winners_after(&self.root, read_version)
    }
}

/// The commits after `read_version` in the table at `root`, up to the latest version.
pub(super) fn winners_after(root: &str, read_version: Version) -> Vec<WinningCommit> {
    let range = CommitRange::builder_for(root, read_version + 1)
        .build(&SyncEngine::new())
        .unwrap();
    WinningCommit::from_commit_range(&range).unwrap()
}

/// A read relation with no files, for tests that inspect only T's facts.
fn no_read_files() -> RelationRef {
    RelationRef::new("read_files", schema_ref! { not_null "path": STRING })
}

/// A DELETE that rewrites files under WriteSerializable, with the given read scope.
pub(super) fn delete_loser(read_version: Version, read_scope: ReadScope) -> LoserFacts {
    LoserFacts {
        read_version,
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
        read_scope,
    }
}

/// The read scope of a files-scoped read of `read_files`, filtered by `partition_predicate`.
pub(super) fn files_scope(
    read_files: &RelationRef,
    partition_schema: SchemaRef,
    partition_predicate: Option<PredicateRef>,
    map_to_struct_options: MapToStructOptions,
) -> ReadScope {
    ReadScope::Files {
        read_files: read_files.clone(),
        partition_schema,
        partition_predicate,
        map_to_struct_options,
    }
}

/// Makes `facts` an OPTIMIZE. An OPTIMIZE changes no data and writes no other action, so under
/// WriteSerializable it drops to SnapshotIsolation.
pub(super) fn make_optimize(facts: &mut LoserFacts) {
    facts.has_data_change = false;
    facts.operation_changes_data = false;
}

/// A partitioned DELETE of `DELETED_DATE` that reads the files in `read_files`.
fn delete_facts(read_files: &RelationRef) -> LoserFacts {
    let deleted_date = lit(PrimitiveType::Date.parse_scalar(DELETED_DATE).unwrap());
    delete_loser(
        READ_VERSION,
        files_scope(
            read_files,
            schema_ref! { nullable "date": DATE },
            Some(Arc::new(col!("date").eq(deleted_date))),
            MapToStructOptions::default(),
        ),
    )
}

/// T's file actions and read files, retained in a fresh scoped executor.
pub(super) struct LoserRelations {
    executor: CountingExecutor,
    file_actions: RelationRef,
    pub(super) read_files: RelationRef,
}

impl LoserRelations {
    /// T reads the files in `read` and removes the files in `removed`. T's file actions also add a
    /// rewrite of each removed file at a new path.
    pub(super) fn new(removed: &[&str], read: &[&str]) -> Self {
        let executor = CountingExecutor {
            inner: SyncPlanExecutor::default().get_scoped(),
            plan_runs: AtomicUsize::new(0),
        };
        let path_struct = |path: &str| {
            let field = StructField::not_null("path", DataType::STRING);
            Scalar::Struct(StructData::try_new(vec![field], vec![path.into()]).unwrap())
        };
        let no_path = Scalar::Null(schema! { not_null "path": STRING }.into());
        let adds = removed.iter().map(|&path| {
            let rewrite = format!("{path}.rewrite");
            vec![path_struct(&rewrite), no_path.clone()]
        });
        let removes = removed
            .iter()
            .map(|&path| vec![no_path.clone(), path_struct(path)]);
        let file_actions = schema_ref! {
            nullable "add": { not_null "path": STRING },
            nullable "remove": { not_null "path": STRING },
        };
        let file_actions = retain(
            &executor,
            "file_actions",
            file_actions,
            Vec::from_iter(adds.chain(removes)),
        );
        let reads = Vec::from_iter(read.iter().map(|&path| vec![Scalar::from(path)]));
        let read_files = schema_ref! { not_null "path": STRING };
        let read_files = retain(&executor, "read_files", read_files, reads);
        Self {
            executor,
            file_actions,
            read_files,
        }
    }

    pub(super) fn detect(
        &self,
        facts: &LoserFacts,
        winners: &[WinningCommit],
    ) -> ConflictDetection {
        detect_conflicts(&self.executor, facts, winners, &self.file_actions).unwrap()
    }

    /// The number of query plans that conflict detection ran.
    fn plan_runs(&self) -> usize {
        self.executor.plan_runs.load(Ordering::Relaxed)
    }
}

/// A scoped sync executor that counts the query plans it runs.
struct CountingExecutor {
    inner: ScopedSyncPlanExecutor,
    plan_runs: AtomicUsize,
}

impl PlanExecutor for CountingExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        if let Operation::QueryPlan(_) = op {
            self.plan_runs.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.execute_op(op)
    }
}

impl ScopedPlanExecutor for CountingExecutor {
    fn execute_and_retain(&self, name: &str, plan: Plan) -> KernelResult<RelationRef> {
        self.inner.execute_and_retain(name, plan)
    }
}

fn retain(
    executor: &dyn ScopedPlanExecutor,
    name: &str,
    schema: SchemaRef,
    rows: Vec<Vec<Scalar>>,
) -> RelationRef {
    let plan = PlanBuilder::values(schema, rows).unwrap().build().unwrap();
    executor.execute_and_retain(name, plan).unwrap()
}

/// Runs conflict detection for the partitioned DELETE over `commits`.
fn detect_delete(commits: &[(Version, Vec<Value>)]) -> Option<Conflict> {
    detect_with(|_| {}, &[DELETED_FILE], &[DELETED_FILE], commits)
}

/// Runs conflict detection for the partitioned DELETE after `adjust`.
fn detect_with(
    adjust: impl FnOnce(&mut LoserFacts),
    removed: &[&str],
    read: &[&str],
    commits: &[(Version, Vec<Value>)],
) -> Option<Conflict> {
    let table = TestTable::new(commits);
    let winners = table.winners(READ_VERSION);
    let relations = LoserRelations::new(removed, read);
    let mut facts = delete_facts(&relations.read_files);
    adjust(&mut facts);
    outcome(relations.detect(&facts, &winners))
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

// === Expected results ===

/// The conflict in `detection`, or `None` when no winner conflicts.
pub(super) fn outcome(detection: ConflictDetection) -> Option<Conflict> {
    match detection {
        ConflictDetection::NoConflict(_) => None,
        ConflictDetection::Conflict(conflict) => Some(conflict),
    }
}

/// The winner summaries of a detection that has no conflict. Panics on a conflict.
pub(super) fn no_conflict_summaries(detection: ConflictDetection) -> Vec<WinnerSummary> {
    let ConflictDetection::NoConflict(summaries) = detection else {
        panic!("expected no conflict, got {detection:?}");
    };
    summaries
}

/// The expected conflict. `operation` is the winner's `commitInfo.operation`.
pub(super) fn conflict(
    version: Version,
    operation: Option<&str>,
    kind: ConflictKind,
) -> Option<Conflict> {
    Some(Conflict {
        version,
        operation: operation.map(str::to_string),
        kind,
    })
}

pub(super) fn occ(check: OccCheck) -> ConflictKind {
    ConflictKind::Occ(check)
}

pub(super) fn winner(rule: WinnerRule) -> ConflictKind {
    ConflictKind::Winner(rule)
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
    let facts = delete_facts(&relations.read_files);
    let summaries = no_conflict_summaries(relations.detect(&facts, &table.winners(READ_VERSION)));
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
    let deleted_partition_add = line(Action::Add(Add {
        data_change,
        ..base_add("f7", &[("date", DELETED_DATE)])
    }));
    let result = detect_delete(&[
        (7, vec![commit_info("WRITE"), deleted_partition_add]),
        (8, vec![commit_info("WRITE"), add("f8", DELETED_DATE)]),
    ]);
    assert_eq!(
        result,
        conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
    );
}

// === Winner check rules ===

#[rstest]
#[case::protocol_fails(
    vec![protocol()],
    |_: &mut LoserFacts| {},
    conflict(7, Some("WRITE"), winner(WinnerRule::ProtocolChanged)),
)]
#[case::no_protocol_passes(vec![], |_: &mut LoserFacts| {}, None)]
#[case::metadata_fails(
    vec![metadata()],
    |_: &mut LoserFacts| {},
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::domain_written_by_both_fails(
    vec![domain("app.x", "{}")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::DomainConflict)),
)]
#[case::removed_domain_written_by_both_fails(
    vec![removed_domain("app.x")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::DomainConflict)),
)]
#[case::second_written_domain_fails(
    vec![domain("app.y", "{}")],
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into(), "app.y".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::DomainConflict)),
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
#[case::row_tracking_tombstone_reports_protocol_first(
    vec![protocol(), removed_domain(ROW_TRACKING_DOMAIN_NAME)],
    |_: &mut LoserFacts| {},
    conflict(7, Some("WRITE"), winner(WinnerRule::ProtocolChanged)),
)]
#[case::txn_for_read_app_id_fails(
    vec![txn("app-a")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::TxnConflict)),
)]
#[case::second_read_app_id_fails(
    vec![txn("app-b")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".into(), "app-b".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::TxnConflict)),
)]
#[case::txn_for_other_app_id_passes(
    vec![txn("app-a")],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-b".into()],
    None,
)]
#[case::protocol_reported_before_metadata(
    vec![protocol(), metadata()],
    |_: &mut LoserFacts| {},
    conflict(7, Some("WRITE"), winner(WinnerRule::ProtocolChanged)),
)]
fn winner_rule_with_write_commit(
    #[case] actions: Vec<Value>,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<Conflict>,
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
    conflict(7, Some("UPDATE SCHEMA"), winner(WinnerRule::MetadataChanged))
)]
#[case::false_tag_fails(
    "false",
    false,
    conflict(7, Some("UPDATE SCHEMA"), winner(WinnerRule::MetadataChanged))
)]
#[case::uppercase_tag_fails(
    "TRUE",
    false,
    conflict(7, Some("UPDATE SCHEMA"), winner(WinnerRule::MetadataChanged))
)]
fn identity_only_metadata_winner(
    #[case] tag: &str,
    #[case] changes_metadata: bool,
    #[case] expected: Option<Conflict>,
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
    // `Metadata` can't leave out its required fields, so the test writes the JSON.
    let commits = [(7, vec![json!({"metaData": {"id": "test-table"}})])];
    let summaries = winner_summaries(&commits);
    let [summary] = summaries.as_slice() else {
        panic!("expected one winner row, got {summaries:?}");
    };
    assert_eq!(summary.version, 7);
    assert_eq!(summary.violations, [WinnerRule::MetadataChanged]);
    assert_eq!(
        detect_delete(&commits),
        conflict(7, None, winner(WinnerRule::MetadataChanged))
    );
}

#[rstest]
#[case::loser_rewrites_files_fails(
    true,
    conflict(
        7,
        Some(ROW_TRACKING_BACKFILL_OPERATION),
        winner(WinnerRule::BackfillConflict)
    )
)]
#[case::loser_rewrites_no_files_passes(false, None)]
fn row_tracking_backfill_winner(#[case] rewrites_files: bool, #[case] expected: Option<Conflict>) {
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
    let result = detect_with(|_| {}, removed, read, &[(7, winner)]);
    assert_eq!(result, conflict(7, Some("OPTIMIZE"), occ(check)));
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
    assert_eq!(
        detect_delete(&commits),
        conflict(7, None, occ(OccCheck::ReadDelete))
    );
}

#[test]
fn winner_without_blind_append_flag_is_checked_under_write_serializable() {
    // `CommitInfo` writes a missing flag as null, so the test removes the key from the JSON.
    let mut info = commit_info("WRITE");
    info["commitInfo"]
        .as_object_mut()
        .unwrap()
        .remove("isBlindAppend");
    let winner = vec![info, add("f7", DELETED_DATE)];
    assert_eq!(
        detect_delete(&[(7, winner)]),
        conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
    );
}

#[test]
fn optimize_loser_drops_to_snapshot_isolation_and_passes_read_append() {
    let mut facts = delete_facts(&no_read_files());
    make_optimize(&mut facts);
    assert_eq!(
        facts.effective_isolation_level(),
        IsolationLevel::SnapshotIsolation
    );
    let winner = vec![commit_info("WRITE"), add("f7", DELETED_DATE)];
    let conflict = detect_with(
        make_optimize,
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(7, winner)],
    );
    assert_eq!(conflict, None);
}

#[rstest]
#[case::metadata_change_keeps_write_serializable(
    |facts: &mut LoserFacts| facts.changes_metadata = true,
    IsolationLevel::WriteSerializable,
)]
#[case::domain_other_than_row_tracking_keeps_write_serializable(
    |facts: &mut LoserFacts| facts.written_domains = vec!["app.x".into()],
    IsolationLevel::WriteSerializable,
)]
#[case::row_tracking_domain_only_drops_to_snapshot_isolation(
    |facts: &mut LoserFacts| facts.written_domains = vec![ROW_TRACKING_DOMAIN_NAME.into()],
    IsolationLevel::SnapshotIsolation,
)]
fn effective_isolation_level_without_data_change(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: IsolationLevel,
) {
    let mut facts = delete_facts(&no_read_files());
    make_optimize(&mut facts);
    adjust(&mut facts);
    assert_eq!(facts.effective_isolation_level(), expected);
}

#[rstest]
#[case::add_in_other_partition(add("f7", OTHER_DATE), OccCheck::ReadAppend)]
#[case::remove_of_unread_file(remove("date=2026-10-02/part-b.parquet", true), OccCheck::ReadDelete)]
fn whole_table_read_conflicts_with_any_file_change(#[case] action: Value, #[case] check: OccCheck) {
    let winner = vec![commit_info("WRITE"), action];
    let result = detect_with(
        |facts| facts.read_scope = ReadScope::WholeTable,
        &[DELETED_FILE],
        &[],
        &[(7, winner)],
    );
    assert_eq!(result, conflict(7, Some("WRITE"), occ(check)));
}

// === Reporting order ===

#[rstest]
#[case::earlier_winner_rule_before_later_one(
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::winner_rule_before_earlier_occ_violation(
    vec![
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ],
    conflict(8, Some("WRITE"), winner(WinnerRule::ProtocolChanged)),
)]
#[case::winner_rule_before_missing_row_tracking_mark(
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")]),
    ],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
fn winner_rule_is_reported_first(
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] expected: Option<Conflict>,
) {
    assert_eq!(detect_delete(&commits), expected);
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
    let mut facts = delete_facts(&relations.read_files);
    facts.read_version = 8;
    assert_eq!(
        outcome(relations.detect(&facts, &table.winners(8))),
        conflict(9, Some("DELETE"), occ(OccCheck::ReadDelete))
    );
}

#[test]
fn checks_of_one_version_report_read_append_first() {
    let result = detect_delete(&[(
        7,
        vec![
            commit_info("WRITE"),
            remove(DELETED_FILE, true),
            add("f7", DELETED_DATE),
        ],
    )]);
    assert_eq!(
        result,
        conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
    );
}

// === Plan presence ===

#[rstest]
#[case::winner_rule_skips_occ_plan(
    vec![metadata()],
    conflict(7, Some("DELETE"), winner(WinnerRule::MetadataChanged)),
    1
)]
#[case::no_winner_rule_runs_occ_plan(
    vec![],
    conflict(7, Some("DELETE"), occ(OccCheck::ReadDelete)),
    2
)]
fn occ_plan_runs_only_if_no_winner_rule_fails(
    #[case] actions: Vec<Value>,
    #[case] expected: Option<Conflict>,
    #[case] plan_runs: usize,
) {
    // The winner removes a file that T read, so the OCC plan fails T whenever it runs. Kernel
    // reports a winner rule first, so with a winner rule only the plan count shows the skip.
    let mut winner = vec![commit_info("DELETE"), remove(DELETED_FILE, true)];
    winner.extend(actions);
    let table = TestTable::new(&[(7, winner)]);
    let relations = LoserRelations::new(&[], &[DELETED_FILE]);
    let facts = delete_facts(&relations.read_files);
    let detection = relations.detect(&facts, &table.winners(READ_VERSION));
    assert_eq!(outcome(detection), expected);
    assert_eq!(relations.plan_runs(), plan_runs);
}

#[test]
fn blind_append_loser_runs_only_the_winner_check_plan() {
    // A whole-table read conflicts with any winner add or remove. A blind append reads and removes
    // no file, so Kernel builds no OCC plan for it.
    let table = TestTable::new(&[(
        7,
        vec![
            commit_info("DELETE"),
            remove(DELETED_FILE, true),
            add("f7", DELETED_DATE),
        ],
    )]);
    let relations = LoserRelations::new(&[], &[]);
    let mut facts = delete_loser(READ_VERSION, ReadScope::WholeTable);
    facts.is_blind_append = true;
    facts.rewrites_files = false;
    let detection = relations.detect(&facts, &table.winners(READ_VERSION));
    assert_eq!(outcome(detection), None);
    assert_eq!(relations.plan_runs(), 1);
}

#[test]
fn detect_conflicts_without_winners_has_no_conflict() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = delete_facts(&relations.read_files);
    let summaries = no_conflict_summaries(relations.detect(&facts, &[]));
    assert!(summaries.is_empty());
    assert_eq!(relations.plan_runs(), 0);
}

// === Winner rows ===

#[test]
fn winner_row_reads_commit_info_timestamp_and_blind_append() {
    let info = CommitInfo {
        is_blind_append: Some(true),
        in_commit_timestamp: Some(1_700_000_000_000),
        ..base_commit_info("WRITE")
    };
    let summaries = winner_summaries(&[(
        7,
        vec![line(Action::CommitInfo(info)), add("f7", OTHER_DATE)],
    )]);
    let [summary] = summaries.as_slice() else {
        panic!("expected one winner row, got {summaries:?}");
    };
    assert_eq!(summary.in_commit_timestamp, Some(1_700_000_000_000));
    assert_eq!(summary.is_blind_append, Some(true));
}

#[rstest]
#[case::malformed_row_tracking_configuration(
    vec![
        commit_info("WRITE"),
        add("f7", OTHER_DATE),
        domain(ROW_TRACKING_DOMAIN_NAME, "{not json"),
    ],
    "without a readable rowIdHighWaterMark",
)]
#[case::row_tracking_configuration_without_mark(
    vec![
        commit_info("WRITE"),
        add("f7", OTHER_DATE),
        domain(ROW_TRACKING_DOMAIN_NAME, "{}"),
    ],
    "without a readable rowIdHighWaterMark",
)]
#[case::row_tracking_tombstone(
    vec![commit_info("WRITE"), removed_domain(ROW_TRACKING_DOMAIN_NAME)],
    "The winning commit at version 7 removes the `delta.rowTracking` domain",
)]
#[case::manifest_commit(
    vec![
        commit_info("WRITE"),
        json!({"checkpoint": [{"checkpointMetadata": {"version": 7}}]}),
    ],
    "Conflict detection does not support the manifest commit at version 7",
)]
#[case::commit_without_actions(vec![], "Winner check rows [] do not match the winning commits [7]")]
fn winner_that_kernel_cannot_check_fails_detection(
    #[case] actions: Vec<Value>,
    #[case] message: &str,
) {
    let table = TestTable::new(&[(7, actions)]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let result = detect_conflicts(
        &relations.executor,
        &delete_facts(&relations.read_files),
        &table.winners(READ_VERSION),
        &relations.file_actions,
    );
    assert_result_error_with_message(result, message);
}

#[test]
fn table_snapshot_isolation_fails_detection_before_any_plan() {
    let table = TestTable::new(&[(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let mut facts = delete_facts(&relations.read_files);
    facts.isolation_level = IsolationLevel::SnapshotIsolation;
    let result = detect_conflicts(
        &relations.executor,
        &facts,
        &table.winners(READ_VERSION),
        &relations.file_actions,
    );
    assert_result_error_with_message(
        result,
        "Conflict detection does not support the table isolation level SnapshotIsolation",
    );
    assert_eq!(relations.plan_runs(), 0);
}

// === Branch version selection ===

fn summary(version: Version, operation: &str, is_blind_append: bool) -> WinnerSummary {
    WinnerSummary {
        version,
        operation: Some(operation.to_string()),
        is_blind_append: Some(is_blind_append),
        in_commit_timestamp: None,
        is_manifest_commit: false,
        row_id_high_water_mark: None,
        row_tracking_count: 0,
        removes_row_tracking: false,
        violations: vec![],
    }
}

#[rstest]
#[case::write_serializable_skips_blind_append_winner(
    |_: &mut LoserFacts| {},
    vec![7, 9],
    vec![7, 8, 9, 10],
)]
#[case::write_serializable_metadata_change_checks_blind_append(
    |facts: &mut LoserFacts| facts.changes_metadata = true,
    vec![7, 8, 9],
    vec![7, 8, 9, 10],
)]
#[case::serializable_checks_blind_append(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::Serializable,
    vec![7, 8, 9],
    vec![7, 8, 9, 10],
)]
#[case::blind_append_loser_skips_all(
    |facts: &mut LoserFacts| facts.is_blind_append = true,
    vec![],
    vec![],
)]
#[case::backfill_loser_skips_all(
    |facts: &mut LoserFacts| facts.is_row_tracking_backfill = true,
    vec![],
    vec![7, 8, 9, 10],
)]
#[case::other_actions_keep_isolation(
    |facts: &mut LoserFacts| {
        make_optimize(facts);
        facts.has_other_actions = true;
    },
    vec![7, 9],
    vec![7, 8, 9, 10],
)]
#[case::write_serializable_data_changing_operation_keeps_isolation(
    |facts: &mut LoserFacts| facts.has_data_change = false,
    vec![7, 9],
    vec![7, 8, 9, 10],
)]
#[case::serializable_without_data_change_drops_isolation(
    |facts: &mut LoserFacts| {
        facts.isolation_level = IsolationLevel::Serializable;
        facts.has_data_change = false;
    },
    vec![],
    vec![7, 8, 9, 10],
)]
fn read_append_versions(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] read_append: Vec<Version>,
    #[case] delete_checks: Vec<Version>,
) {
    let mut facts = delete_facts(&no_read_files());
    adjust(&mut facts);
    let summaries = [
        summary(7, "WRITE", false),
        summary(8, "WRITE", true),
        summary(9, "MERGE", false),
        summary(10, ROW_TRACKING_BACKFILL_OPERATION, false),
    ];
    let branches = select_occ_branch_versions(&facts, &summaries);
    assert_eq!(branches.read_append, read_append);
    assert_eq!(branches.delete_checks, delete_checks);
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
    let result = check_winner_rows(READ_VERSION, &winners, &summaries);
    assert_eq!(result.is_ok(), valid, "{result:?}");
}

// === Partition values ===

#[test]
fn null_partition_value_does_not_match_equality_predicate() {
    // `Add` can't hold a null partition value, so the test edits the add's JSON.
    let mut null_partition_add = add("f7", DELETED_DATE);
    null_partition_add["add"]["partitionValues"]["date"] = Value::Null;
    let winner = vec![commit_info("WRITE"), null_partition_add];
    assert_eq!(detect_delete(&[(7, winner)]), None);
}

#[rstest]
#[case::utc_by_default_matches_utc_instant(
    "2026-10-02 06:30:00",
    None,
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
#[case::utc_by_default_ignores_local_offset("2026-10-01 23:30:00", None, None)]
#[case::connector_timezone_matches_local_time(
    "2026-10-01 23:30:00",
    Some("America/Los_Angeles"),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
)]
fn timestamp_partition_is_read_in_connector_timezone(
    #[case] partition_value: &str,
    #[case] timezone: Option<&str>,
    #[case] expected: Option<Conflict>,
) {
    // 2026-10-01 23:30 in Los Angeles (PDT) is 2026-10-02 06:30 UTC.
    let table = TestTable::new(&[(
        7,
        vec![
            commit_info("WRITE"),
            line(Action::Add(base_add("f7", &[("ts", partition_value)]))),
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
    let mut facts = delete_facts(&relations.read_files);
    facts.read_scope = files_scope(
        &relations.read_files,
        schema_ref! { nullable "ts": TIMESTAMP },
        Some(Arc::new(col!("ts").eq(lit(instant)))),
        options,
    );
    let detection = relations.detect(&facts, &table.winners(READ_VERSION));
    assert_eq!(outcome(detection), expected);
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
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::default_predicate_skips_other_partition(|_: &mut LoserFacts| {}, OTHER_DATE, None)]
#[case::none_matches_every_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, None),
    OTHER_DATE,
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::false_matches_no_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, Some(Arc::new(Predicate::FALSE))),
    DELETED_DATE,
    None,
)]
fn partition_predicate_selects_adds(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] date: &str,
    #[case] expected: Option<Conflict>,
) {
    let conflict = detect_with(
        adjust,
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(7, vec![commit_info("WRITE"), add("f7", date)])],
    );
    assert_eq!(conflict, expected);
}

#[rstest]
#[case::none_matches_every_add(None, conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)))]
#[case::false_matches_no_add(Some(Arc::new(Predicate::FALSE)), None)]
fn unpartitioned_table_applies_constant_partition_predicate(
    #[case] predicate: Option<PredicateRef>,
    #[case] expected: Option<Conflict>,
) {
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
            *partition_predicate = predicate;
        },
        &[DELETED_FILE],
        &[DELETED_FILE],
        &[(
            7,
            vec![commit_info("WRITE"), line(Action::Add(base_add("f7", &[])))],
        )],
    );
    assert_eq!(conflict, expected);
}
