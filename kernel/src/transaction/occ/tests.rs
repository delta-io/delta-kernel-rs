use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ::test_utils::add_commit;
use futures::executor::block_on;
use rstest::rstest;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::winner_check_plan::{
    ROW_TRACKING_BACKFILL_OPERATION, ROW_TRACKING_UNBACKFILL_OPERATION,
};
use super::*;
use crate::actions::{Add, CommitInfo, DomainMetadata, Metadata, Protocol, Remove, SetTransaction};
use crate::commit_range::CommitRange;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::{ScopedSyncPlanExecutor, SyncPlanExecutor};
use crate::expressions::{Expression, Predicate, Scalar, StructData, col, lit};
use crate::object_store::local::LocalFileSystem;
use crate::plans::ir::nodes::Operator;
use crate::plans::{Plan, PlanBuilder, PlanExecutor, PlanResult};
use crate::row_tracking::{ROW_TRACKING_DOMAIN_NAME, RowTrackingDomainMetadata};
use crate::schema::{DataType, PrimitiveType, StructField, schema, schema_ref};
use crate::unit_test_utils::Action;

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

/// The `checkpoint` action of an adaptiveMetadata manifest commit, with only its version element.
fn manifest_checkpoint(version: Version) -> Value {
    json!({"checkpoint": [{"checkpointMetadata": {"version": version}}]})
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
    let commits = range.commit_files().unwrap().map(|result| {
        let commit = result.unwrap();
        WinningCommit {
            version: commit.version,
            file: commit.location,
        }
    });
    Vec::from_iter(commits)
}

/// A read relation with no files, for tests that inspect only T's facts.
fn no_read_files() -> RelationRef {
    RelationRef::new("read_files", schema_ref! { not_null "path": STRING })
}

/// The read scope of a files-scoped read of `read_files`, filtered by `partition_predicate`.
fn files_scope(
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

/// Makes `facts` an OPTIMIZE, which drops to SnapshotIsolation under WriteSerializable.
pub(super) fn make_optimize(facts: &mut LoserFacts) {
    facts.has_data_change = false;
    facts.operation_changes_data = false;
}

/// A partitioned DELETE under WriteSerializable at `read_version` that read the files in
/// `read_files`, filtered by `partition_predicate`. It rewrites files.
pub(super) fn partitioned_delete_facts(
    read_version: Version,
    read_files: &RelationRef,
    partition_schema: SchemaRef,
    partition_predicate: Option<PredicateRef>,
) -> LoserFacts {
    let read_scope = files_scope(
        read_files,
        partition_schema,
        partition_predicate,
        MapToStructOptions::default(),
    );
    LoserFacts {
        checked_version: read_version,
        isolation_level: IsolationLevel::WriteSerializable,
        is_manifest_commit: false,
        has_data_change: true,
        operation_changes_data: true,
        changes_metadata: false,
        has_other_actions: false,
        rewrites_files: true,
        written_domains: vec![],
        read_app_ids: vec![],
        read_scope,
    }
}

/// The DELETE of partition `DELETED_DATE` that read the files in `read_files`.
fn deleted_date_facts(read_files: &RelationRef) -> LoserFacts {
    partitioned_delete_facts(
        READ_VERSION,
        read_files,
        schema_ref! { nullable "date": DATE },
        Some(equals("date", date(DELETED_DATE))),
    )
}

/// The predicate `column = value`.
pub(super) fn equals(column: &str, value: impl Into<Scalar>) -> PredicateRef {
    Arc::new(Expression::column([column]).eq(lit(value)))
}

fn date(value: &str) -> Scalar {
    PrimitiveType::Date.parse_scalar(value).unwrap()
}

/// T's file actions and read files, retained in a fresh scoped executor.
pub(super) struct LoserRelations {
    executor: CountingExecutor,
    file_actions: RelationRef,
    pub(super) read_files: RelationRef,
}

impl LoserRelations {
    /// T removes the files in `removed` and reads the files in `read`. T's file actions also add a
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
        self.try_detect(facts, winners).unwrap()
    }

    fn try_detect(
        &self,
        facts: &LoserFacts,
        winners: &[WinningCommit],
    ) -> KernelResult<ConflictDetection> {
        detect_conflicts(&self.executor, facts, winners, &self.file_actions)
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
    detect_delete_with(|_| {}, commits)
}

/// Runs conflict detection for the partitioned DELETE after `adjust`.
fn detect_delete_with(
    adjust: impl FnOnce(&mut LoserFacts),
    commits: &[(Version, Vec<Value>)],
) -> Option<Conflict> {
    detect_delete_with_files(adjust, &[DELETED_FILE], &[DELETED_FILE], commits)
}

/// Runs conflict detection for the partitioned DELETE that removes `removed` and reads `read`,
/// after `adjust`.
fn detect_delete_with_files(
    adjust: impl FnOnce(&mut LoserFacts),
    removed: &[&str],
    read: &[&str],
    commits: &[(Version, Vec<Value>)],
) -> Option<Conflict> {
    let table = TestTable::new(commits);
    let relations = LoserRelations::new(removed, read);
    let mut facts = deleted_date_facts(&relations.read_files);
    adjust(&mut facts);
    outcome(relations.detect(&facts, &table.winners(facts.checked_version)))
}

fn winner_summaries(commits: &[(Version, Vec<Value>)]) -> Vec<WinnerSummary> {
    let table = TestTable::new(commits);
    let facts = deleted_date_facts(&no_read_files());
    let plan = winner_check_plan(&facts, &table.winners(READ_VERSION)).unwrap();
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

/// The only summary in `summaries`. Panics unless there is exactly one.
pub(super) fn only_summary(summaries: &[WinnerSummary]) -> &WinnerSummary {
    let [summary] = summaries else {
        panic!("expected one winner row, got {summaries:?}");
    };
    summary
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

/// The `KernelError` variant that a failing test expects.
#[derive(Clone, Copy, Debug)]
enum ErrorKind {
    Generic,
    Internal,
    Unsupported,
}

/// Asserts that `result` fails with a `kind` error whose text contains `message`.
#[track_caller]
fn assert_error<T>(result: KernelResult<T>, kind: ErrorKind, message: &str) {
    let Err(error) = result else {
        panic!("expected a {kind:?} error containing {message:?}, got Ok");
    };
    let is_kind = match kind {
        ErrorKind::Generic => matches!(error.without_backtrace(), KernelError::Generic(_)),
        ErrorKind::Internal => matches!(error.without_backtrace(), KernelError::InternalError(_)),
        ErrorKind::Unsupported => matches!(error.without_backtrace(), KernelError::Unsupported(_)),
    };
    assert!(is_kind, "expected a {kind:?} error, got {error:?}");
    assert!(
        error.to_string().contains(message),
        "expected {message:?} in {error}"
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
#[case::protocol_rule_reported_before_row_tracking_tombstone_error(
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
#[case::metadata_reported_before_txn_listed_first(
    vec![txn("app-a"), metadata()],
    |facts: &mut LoserFacts| facts.read_app_ids = vec!["app-a".into()],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::domain_conflict_reported_before_txn_conflict(
    vec![domain("app.x", "{}"), txn("app-a")],
    |facts: &mut LoserFacts| {
        facts.written_domains = vec!["app.x".into()];
        facts.read_app_ids = vec!["app-a".into()];
    },
    conflict(7, Some("WRITE"), winner(WinnerRule::DomainConflict)),
)]
fn winner_rule_with_write_commit(
    #[case] actions: Vec<Value>,
    #[case] adjust: fn(&mut LoserFacts),
    #[case] expected: Option<Conflict>,
) {
    let mut winner_actions = vec![commit_info("WRITE")];
    winner_actions.extend(actions);
    let result = detect_delete_with(adjust, &[(7, winner_actions)]);
    assert_eq!(result, expected);
}

#[rstest]
#[case::identity_only_passes_without_metadata_change("true", false, None)]
#[case::identity_only_fails_with_metadata_change(
    "true",
    true,
    conflict(7, Some("UPDATE SCHEMA"), winner(WinnerRule::MetadataChanged))
)]
#[case::non_canonical_case_tag_is_not_identity_only(
    "TRUE",
    false,
    conflict(7, Some("UPDATE SCHEMA"), winner(WinnerRule::MetadataChanged))
)]
fn identity_only_metadata_winner(
    #[case] tag: &str,
    #[case] changes_metadata: bool,
    #[case] expected: Option<Conflict>,
) {
    let tags = HashMap::from([("delta.identity.schemaUpdate".into(), Some(tag.into()))]);
    let info = CommitInfo {
        tags: Some(tags),
        ..base_commit_info("UPDATE SCHEMA")
    };
    let winner_actions = vec![line(Action::CommitInfo(info)), metadata()];
    let result = detect_delete_with(
        |facts| facts.changes_metadata = changes_metadata,
        &[(7, winner_actions)],
    );
    assert_eq!(result, expected);
}

#[test]
fn domain_conflict_reported_before_backfill_conflict() {
    let winner_actions = vec![
        commit_info(ROW_TRACKING_BACKFILL_OPERATION),
        domain("app.x", "{}"),
    ];
    let result = detect_delete_with(
        |facts| facts.written_domains = vec!["app.x".into()],
        &[(7, winner_actions)],
    );
    let expected = conflict(
        7,
        Some(ROW_TRACKING_BACKFILL_OPERATION),
        winner(WinnerRule::DomainConflict),
    );
    assert_eq!(result, expected);
}

#[rstest]
fn row_tracking_backfill_winner(
    #[values(ROW_TRACKING_BACKFILL_OPERATION, ROW_TRACKING_UNBACKFILL_OPERATION)] operation: &str,
    #[values(true, false)] rewrites_files: bool,
) {
    let removed: &[&str] = if rewrites_files { &[DELETED_FILE] } else { &[] };
    // The winner adds a file in the deleted partition. `read_append` skips backfill and unbackfill
    // winners, so only the backfill rule can fail T.
    let winner_actions = vec![
        commit_info(operation),
        add("date=2026-10-01/part-c.parquet", DELETED_DATE),
    ];
    let result = detect_delete_with_files(
        |facts| facts.rewrites_files = rewrites_files,
        removed,
        &[DELETED_FILE],
        &[(7, winner_actions)],
    );
    let expected = if rewrites_files {
        conflict(7, Some(operation), winner(WinnerRule::BackfillConflict))
    } else {
        None
    };
    assert_eq!(result, expected);
}

// === OCC checks ===

#[rstest]
#[case::add_in_deleted_partition_fails(true)]
#[case::add_without_data_change_in_deleted_partition_fails(false)]
fn partitioned_delete_read_append(#[case] data_change: bool) {
    let deleted_partition_add = line(Action::Add(Add {
        data_change,
        ..base_add("f7", &[("date", DELETED_DATE)])
    }));
    let result = detect_delete(&[(7, vec![commit_info("WRITE"), deleted_partition_add])]);
    assert_eq!(
        result,
        conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
    );
}

#[rstest]
#[case::read_delete(&[], &[DELETED_FILE], OccCheck::ReadDelete)]
#[case::delete_delete(&[DELETED_FILE], &[], OccCheck::DeleteDelete)]
fn winner_remove_conflicts_whatever_its_data_change(
    #[case] removed: &[&str],
    #[case] read: &[&str],
    #[case] check: OccCheck,
    #[values(true, false)] data_change: bool,
    #[values(true, false)] rewrites_files: bool,
) {
    let winner_actions = vec![commit_info("OPTIMIZE"), remove(DELETED_FILE, data_change)];
    let result = detect_delete_with_files(
        |facts| facts.rewrites_files = rewrites_files,
        removed,
        read,
        &[(7, winner_actions)],
    );
    assert_eq!(result, conflict(7, Some("OPTIMIZE"), occ(check)));
}

#[rstest]
#[case::remove_of_read_file_fails_read_delete(
    remove(DELETED_FILE, true),
    occ(OccCheck::ReadDelete)
)]
// `Metadata` can't leave out its required fields, so the case writes the JSON.
#[case::metadata_without_required_fields_fails_metadata_changed(
    json!({"metaData": {"id": "test-table"}}),
    winner(WinnerRule::MetadataChanged),
)]
fn winner_without_commit_info(#[case] action: Value, #[case] kind: ConflictKind) {
    let commits = [(7, vec![action])];
    let summaries = winner_summaries(&commits);
    let summary = only_summary(&summaries);
    assert_eq!((summary.version, summary.operation.as_deref()), (7, None));
    assert_eq!(summary.is_blind_append, None);
    assert_eq!(detect_delete(&commits), conflict(7, None, kind));
}

#[test]
fn winner_without_blind_append_flag_is_checked_under_write_serializable() {
    // `CommitInfo` writes a missing flag as null, so the test removes the key from the JSON.
    let mut info = commit_info("WRITE");
    info["commitInfo"]
        .as_object_mut()
        .unwrap()
        .remove("isBlindAppend");
    let winner_actions = vec![info, add("f7", DELETED_DATE)];
    assert_eq!(
        detect_delete(&[(7, winner_actions)]),
        conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend))
    );
}

#[test]
fn optimize_loser_passes_read_append() {
    let winner_actions = vec![commit_info("WRITE"), add("f7", DELETED_DATE)];
    let result = detect_delete_with(make_optimize, &[(7, winner_actions)]);
    assert_eq!(result, None);
}

#[rstest]
#[case::add_in_other_partition(add("f7", OTHER_DATE), OccCheck::ReadAppend)]
#[case::remove_of_unread_file(remove("date=2026-10-02/part-b.parquet", true), OccCheck::ReadDelete)]
fn whole_table_read_conflicts_with_any_file_change(#[case] action: Value, #[case] check: OccCheck) {
    let winner_actions = vec![commit_info("WRITE"), action];
    let result = detect_delete_with_files(
        |facts| facts.read_scope = ReadScope::WholeTable,
        &[DELETED_FILE],
        &[],
        &[(7, winner_actions)],
    );
    assert_eq!(result, conflict(7, Some("WRITE"), occ(check)));
}

// === Reporting order ===

#[rstest]
#[case::earlier_winner_rule_before_later_one(
    READ_VERSION,
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::winner_rule_before_earlier_occ_violation(
    READ_VERSION,
    vec![
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ],
    conflict(8, Some("WRITE"), winner(WinnerRule::ProtocolChanged)),
)]
#[case::winner_rule_before_missing_row_tracking_mark(
    READ_VERSION,
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")]),
    ],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::winner_rule_before_later_manifest_commit(
    READ_VERSION,
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), manifest_checkpoint(8)]),
    ],
    conflict(7, Some("WRITE"), winner(WinnerRule::MetadataChanged)),
)]
#[case::occ_violation_before_row_tracking_tombstone(
    READ_VERSION,
    vec![(
        7,
        vec![
            commit_info("DELETE"),
            remove(DELETED_FILE, true),
            removed_domain(ROW_TRACKING_DOMAIN_NAME),
        ],
    )],
    conflict(7, Some("DELETE"), occ(OccCheck::ReadDelete)),
)]
#[case::read_append_before_other_checks_of_one_version(
    READ_VERSION,
    vec![(
        7,
        vec![
            commit_info("WRITE"),
            remove(DELETED_FILE, true),
            add("f7", DELETED_DATE),
        ],
    )],
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
// Versions compare as numbers, so 9 comes before 10.
#[case::earlier_version_then_check_order(
    8,
    vec![
        (9, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (10, vec![commit_info("WRITE"), add("f10", DELETED_DATE)]),
    ],
    conflict(9, Some("DELETE"), occ(OccCheck::ReadDelete)),
)]
fn reporting_order(
    #[case] checked_version: Version,
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] expected: Option<Conflict>,
) {
    let result = detect_delete_with(|facts| facts.checked_version = checked_version, &commits);
    assert_eq!(result, expected);
}

// === Plan runs and shape ===

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
    // The winner removes a file that T read, so the OCC plan would fail T. With a winner rule, only
    // the plan count shows that the OCC plan didn't run.
    let mut winner_actions = vec![commit_info("DELETE"), remove(DELETED_FILE, true)];
    winner_actions.extend(actions);
    let table = TestTable::new(&[(7, winner_actions)]);
    let relations = LoserRelations::new(&[], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let detection = relations.detect(&facts, &table.winners(READ_VERSION));
    assert_eq!(outcome(detection), expected);
    assert_eq!(relations.plan_runs(), plan_runs);
}

#[test]
fn delete_checks_share_one_scan_of_winner_removes() {
    // One `ScanJson` for `read_append`, and one scan that both delete checks share.
    let table = TestTable::new(&[(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let plan = occ_plan(
        &facts,
        &table.winners(READ_VERSION),
        &[7],
        &relations.file_actions,
    )
    .unwrap();
    let scans = plan
        .nodes
        .iter()
        .filter(|node| matches!(node.op, Operator::ScanJson(_)))
        .count();
    assert_eq!(scans, 2);
}

#[test]
fn detect_conflicts_without_winners_has_no_conflict() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let summaries = no_conflict_summaries(relations.detect(&facts, &[]));
    assert!(summaries.is_empty());
    assert_eq!(relations.plan_runs(), 0);
}

// === Read scope ===

#[rstest]
#[case::add_in_read_partition_passes(&[], add("f7", DELETED_DATE), None)]
#[case::remove_of_unremoved_file_passes(&[], remove(DELETED_FILE, true), None)]
// The flag is wrong for a T whose `file_actions` holds a remove, but `delete_delete` doesn't read
// it.
#[case::remove_of_removed_file_fails_without_rewrite_flag(
    &[DELETED_FILE],
    remove(DELETED_FILE, true),
    conflict(7, Some("WRITE"), occ(OccCheck::DeleteDelete)),
)]
fn loser_that_read_nothing(
    #[case] removed: &[&str],
    #[case] action: Value,
    #[case] expected: Option<Conflict>,
) {
    let table = TestTable::new(&[(7, vec![commit_info("WRITE"), action])]);
    let relations = LoserRelations::new(removed, &[]);
    let mut facts = deleted_date_facts(&relations.read_files);
    facts.read_scope = ReadScope::Nothing;
    facts.rewrites_files = false;
    let detection = relations.detect(&facts, &table.winners(READ_VERSION));
    assert_eq!(outcome(detection), expected);
    assert_eq!(relations.plan_runs(), 2);
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
    let summary = only_summary(&summaries);
    assert_eq!(summary.in_commit_timestamp, Some(1_700_000_000_000));
    assert_eq!(summary.is_blind_append, Some(true));
}

#[rstest]
#[case::malformed_configuration_of_other_domain("{not json")]
#[case::mark_in_configuration_of_other_domain(r#"{"rowIdHighWaterMark":5000}"#)]
fn row_tracking_mark_comes_only_from_live_row_tracking_domain(#[case] configuration: &str) {
    let summaries = winner_summaries(&[(
        7,
        vec![
            commit_info("WRITE"),
            domain("app.x", configuration),
            row_tracking(1099),
        ],
    )]);
    let summary = only_summary(&summaries);
    assert_eq!(summary.row_id_high_water_mark, Some(1099));
}

#[rstest]
#[case::malformed_row_tracking_configuration(
    vec![(
        7,
        vec![
            commit_info("WRITE"),
            add("f7", OTHER_DATE),
            domain(ROW_TRACKING_DOMAIN_NAME, "{not json"),
        ],
    )],
    ErrorKind::Generic,
    "domain in the winning commits at versions [7]",
)]
#[case::row_tracking_configuration_without_mark(
    vec![(
        7,
        vec![
            commit_info("WRITE"),
            add("f7", OTHER_DATE),
            domain(ROW_TRACKING_DOMAIN_NAME, "{}"),
        ],
    )],
    ErrorKind::Generic,
    "domain in the winning commits at versions [7]",
)]
#[case::malformed_row_tracking_configuration_of_one_winner(
    vec![
        (7, vec![commit_info("WRITE"), row_tracking(1099)]),
        (8, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{not json")]),
    ],
    ErrorKind::Generic,
    "domain in the winning commits at versions [8]",
)]
#[case::row_tracking_configurations_without_mark_in_two_winners(
    vec![
        (7, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")]),
        (8, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{not json")]),
    ],
    ErrorKind::Generic,
    "domain in the winning commits at versions [7, 8]",
)]
#[case::row_tracking_tombstone(
    vec![(7, vec![commit_info("WRITE"), removed_domain(ROW_TRACKING_DOMAIN_NAME)])],
    ErrorKind::Unsupported,
    "The winning commit at version 7 removes the `delta.rowTracking` domain",
)]
#[case::manifest_commit(
    vec![(7, vec![commit_info("WRITE"), manifest_checkpoint(7)])],
    ErrorKind::Unsupported,
    "Conflict detection does not support the manifest commit at version 7",
)]
#[case::manifest_commit_before_earlier_occ_violation(
    vec![
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), manifest_checkpoint(8)]),
    ],
    ErrorKind::Unsupported,
    "Conflict detection does not support the manifest commit at version 8",
)]
#[case::commit_without_actions(
    vec![(7, vec![])],
    ErrorKind::Generic,
    "Conflict detection read no action from the commit file of version 7",
)]
fn winner_that_kernel_cannot_check_fails_detection(
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] kind: ErrorKind,
    #[case] message: &str,
) {
    let table = TestTable::new(&commits);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let result = relations.try_detect(&facts, &table.winners(READ_VERSION));
    assert_error(result, kind, message);
}

#[test]
fn winner_list_with_gap_fails_detection_before_any_plan() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let winners = [fake_winner(READ_VERSION + 1), fake_winner(READ_VERSION + 3)];
    let result = relations.try_detect(&facts, &winners);
    assert_error(
        result,
        ErrorKind::Internal,
        "must start at checked version 6 + 1 with no version gap",
    );
    assert_eq!(relations.plan_runs(), 0);
}

#[rstest]
#[case::table_snapshot_isolation(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::SnapshotIsolation,
    ErrorKind::Unsupported,
    "Conflict detection does not support the table isolation level SnapshotIsolation",
)]
#[case::manifest_commit_loser(
    |facts: &mut LoserFacts| facts.is_manifest_commit = true,
    ErrorKind::Unsupported,
    "Conflict detection does not support a transaction that writes a manifest commit",
)]
#[case::read_nothing_with_rewrites(
    |facts: &mut LoserFacts| facts.read_scope = ReadScope::Nothing,
    ErrorKind::Internal,
    "must pass the files that it read",
)]
fn invalid_loser_fails_detection_before_any_plan(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] kind: ErrorKind,
    #[case] message: &str,
) {
    let table = TestTable::new(&[(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])]);
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let mut facts = deleted_date_facts(&relations.read_files);
    adjust(&mut facts);
    let result = relations.try_detect(&facts, &table.winners(READ_VERSION));
    assert_error(result, kind, message);
    assert_eq!(relations.plan_runs(), 0);
}

// === Read-append version selection ===

fn summary_row(version: Version, operation: &str, is_blind_append: bool) -> WinnerSummary {
    WinnerSummary {
        version,
        operation: Some(operation.to_string()),
        is_blind_append: Some(is_blind_append),
        in_commit_timestamp: None,
        is_manifest_commit: false,
        row_id_high_water_mark: None,
        writes_row_tracking: false,
        removes_row_tracking: false,
        first_violation: None,
    }
}

#[rstest]
#[case::write_serializable_skips_blind_append_winner(
    |_: &mut LoserFacts| {},
    vec![7, 9],
)]
#[case::write_serializable_metadata_change_checks_blind_append(
    |facts: &mut LoserFacts| facts.changes_metadata = true,
    vec![7, 8, 9],
)]
#[case::serializable_checks_blind_append(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::Serializable,
    vec![7, 8, 9],
)]
#[case::loser_that_read_nothing_skips_all(
    |facts: &mut LoserFacts| {
        facts.read_scope = ReadScope::Nothing;
        facts.rewrites_files = false;
    },
    vec![],
)]
#[case::optimize_drops_to_snapshot_isolation(make_optimize, vec![])]
#[case::optimize_with_metadata_change_keeps_write_serializable(
    |facts: &mut LoserFacts| {
        make_optimize(facts);
        facts.changes_metadata = true;
    },
    vec![7, 8, 9],
)]
#[case::optimize_with_other_domain_keeps_write_serializable(
    |facts: &mut LoserFacts| {
        make_optimize(facts);
        facts.written_domains = vec!["app.x".into()];
    },
    vec![7, 9],
)]
#[case::optimize_with_only_row_tracking_domain_drops_to_snapshot_isolation(
    |facts: &mut LoserFacts| {
        make_optimize(facts);
        facts.written_domains = vec![ROW_TRACKING_DOMAIN_NAME.into()];
    },
    vec![],
)]
#[case::other_actions_keep_isolation(
    |facts: &mut LoserFacts| {
        make_optimize(facts);
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
    let mut facts = deleted_date_facts(&no_read_files());
    adjust(&mut facts);
    let summaries = [
        summary_row(7, "WRITE", false),
        summary_row(8, "WRITE", true),
        summary_row(9, "MERGE", false),
        summary_row(10, ROW_TRACKING_BACKFILL_OPERATION, false),
    ];
    assert_eq!(select_read_append_versions(&facts, &summaries), expected);
}

// === Winner list and winner rows ===

fn fake_winner(version: Version) -> WinningCommit {
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
#[case::winners_from_checked_version_plus_one_pass(&[READ_VERSION + 1, READ_VERSION + 2], true)]
#[case::first_winner_that_skips_a_version_fails(&[READ_VERSION + 2, READ_VERSION + 3], false)]
#[case::first_winner_at_checked_version_fails(&[READ_VERSION, READ_VERSION + 1], false)]
fn winners_must_follow_checked_version(#[case] winners: &[Version], #[case] valid: bool) {
    let winners = Vec::from_iter(winners.iter().map(|&version| fake_winner(version)));
    let result = ensure_winners_follow_checked_version(READ_VERSION, &winners);
    if valid {
        result.unwrap();
    } else {
        assert_error(result, ErrorKind::Internal, "must start at checked version");
    }
}

#[rstest]
#[case::contiguous_rows_pass(&[7, 8], &[7, 8], None)]
#[case::no_winners_pass(&[], &[], None)]
#[case::missing_row_fails(&[7, 8], &[7], Some((ErrorKind::Generic, "read no action")))]
#[case::extra_row_fails(&[7], &[7, 8], Some((ErrorKind::Internal, "do not match")))]
fn winner_rows_must_match_winners(
    #[case] winners: &[Version],
    #[case] rows: &[Version],
    #[case] expected_error: Option<(ErrorKind, &str)>,
) {
    let winners = Vec::from_iter(winners.iter().map(|&version| fake_winner(version)));
    let summaries = Vec::from_iter(
        rows.iter()
            .map(|&version| summary_row(version, "WRITE", false)),
    );
    let result = ensure_winner_rows(READ_VERSION, &winners, &summaries);
    match expected_error {
        None => result.unwrap(),
        Some((kind, message)) => assert_error(result, kind, message),
    }
}

// === Partition values ===

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
    let mut facts = deleted_date_facts(&relations.read_files);
    facts.read_scope = files_scope(
        &relations.read_files,
        schema_ref! { nullable "ts": TIMESTAMP },
        Some(equals("ts", instant)),
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

/// Makes a files-scoped T read only the adds whose `date` is null.
fn set_is_null_predicate(facts: &mut LoserFacts) {
    set_partition_predicate(facts, Some(Arc::new(col!("date").is_null())));
}

/// An add of `f7` whose `partitionValues` JSON is `partition_values`. `Add` can't hold a null
/// partition value.
fn add_with_partition_values(partition_values: Value) -> Value {
    let mut winner_add = add("f7", DELETED_DATE);
    winner_add["add"]["partitionValues"] = partition_values;
    winner_add
}

#[rstest]
#[case::default_predicate_skips_other_partition(
    |_: &mut LoserFacts| {},
    add("f7", OTHER_DATE),
    None,
)]
#[case::none_matches_every_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, None),
    add("f7", OTHER_DATE),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::false_matches_no_partition(
    |facts: &mut LoserFacts| set_partition_predicate(facts, Some(Arc::new(Predicate::FALSE))),
    add("f7", DELETED_DATE),
    None,
)]
#[case::or_of_scan_predicates_matches_either_partition(
    |facts: &mut LoserFacts| {
        let read_partition = |value| col!("date").eq(lit(date(value)));
        let predicate = Predicate::or(read_partition(DELETED_DATE), read_partition(OTHER_DATE));
        set_partition_predicate(facts, Some(Arc::new(predicate)));
    },
    add("f7", OTHER_DATE),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::equality_predicate_skips_null_value(
    |_: &mut LoserFacts| {},
    add_with_partition_values(json!({"date": null})),
    None,
)]
#[case::is_null_matches_null_value(
    set_is_null_predicate,
    add_with_partition_values(json!({"date": null})),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::is_null_matches_missing_key(
    set_is_null_predicate,
    add_with_partition_values(json!({})),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::not_null_partition_field_reads_null_value_as_null(
    |facts: &mut LoserFacts| {
        set_is_null_predicate(facts);
        let ReadScope::Files { partition_schema, .. } = &mut facts.read_scope else {
            panic!("expected a files-scoped read");
        };
        *partition_schema = schema_ref! { not_null "date": DATE };
    },
    add_with_partition_values(json!({"date": null})),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
#[case::unpartitioned_table_ignores_false_predicate(
    |facts: &mut LoserFacts| {
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
    add_with_partition_values(json!({})),
    conflict(7, Some("WRITE"), occ(OccCheck::ReadAppend)),
)]
fn partition_predicate_selects_adds(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] winner_add: Value,
    #[case] expected: Option<Conflict>,
) {
    let result = detect_delete_with(adjust, &[(7, vec![commit_info("WRITE"), winner_add])]);
    assert_eq!(result, expected);
}
