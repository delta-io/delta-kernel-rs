//! Shared action builders and fixtures. T is the losing transaction. By default, T is a DELETE of
//! the partition `DELETED_DATE`. `delete_conflict` also makes T read and remove `DELETED_FILE`.

mod occ_plan;
mod winner_check_plan;

use std::collections::HashMap;
use std::sync::Arc;

use ::test_utils::add_commit;
use futures::executor::block_on;
use serde_json::{Value, json};
use tempfile::TempDir;
use url::Url;

use super::*;
use crate::actions::{Add, CommitInfo, DomainMetadata, Protocol, Remove, SetTransaction};
use crate::commit_range::CommitRange;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::{ScopedSyncPlanExecutor, SyncPlanExecutor};
use crate::expressions::{Expression, Scalar, StructData, lit};
use crate::object_store::local::LocalFileSystem;
use crate::plans::{Operation, PlanBuilder, PlanExecutor as _, ScopedPlanExecutor};
use crate::row_tracking::RowTrackingDomainMetadata;
use crate::schema::{PrimitiveType, schema, schema_ref};
use crate::transaction::occ::winner_check_plan::{read_winner_summaries, winner_check_plan};
use crate::unit_test_utils::{Action, metadata_action};

const READ_VERSION: Version = 6;
const DELETED_DATE: &str = "2026-10-01";
const OTHER_DATE: &str = "2026-10-02";
const DELETED_FILE: &str = "date=2026-10-01/part-a.parquet";

// === Actions ===

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
    line(metadata_action(
        "test-table",
        &schema! { nullable "date": DATE },
        0,
    ))
}

fn domain(name: &str, configuration: &str) -> Value {
    let domain = DomainMetadata::new(name.into(), configuration.into());
    line(Action::DomainMetadata(domain))
}

fn removed_domain(name: &str) -> Value {
    let domain = DomainMetadata::remove(name.into(), "{}".into());
    line(Action::DomainMetadata(domain))
}

fn row_tracking(high_water_mark: i64) -> Value {
    let domain = RowTrackingDomainMetadata::new(high_water_mark);
    line(Action::DomainMetadata(domain.try_into().unwrap()))
}

fn txn(app_id: &str) -> Value {
    line(Action::Txn(SetTransaction::new(app_id.into(), 3, Some(0))))
}

/// A manifest commit's `checkpoint` action.
///
/// Its one element holds only `checkpointMetadata.version`.
fn manifest_checkpoint(version: Version) -> Value {
    json!({"checkpoint": [{"checkpointMetadata": {"version": version}}]})
}

// === Fixtures ===

/// Writes `commits` to a new table, and returns the table and its commits after `read_version`.
fn table_winners(
    read_version: Version,
    commits: &[(Version, Vec<Value>)],
) -> (TempDir, Vec<WinningCommit>) {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap().to_string();
    let store = LocalFileSystem::new();
    for (version, actions) in commits {
        let body = Vec::from_iter(actions.iter().map(Value::to_string)).join("\n");
        block_on(add_commit(&root, &store, *version, body)).unwrap();
    }
    let winners = winners_after(&root, read_version);
    (dir, winners)
}

fn winners_after(root: &str, read_version: Version) -> Vec<WinningCommit> {
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

/// A winner for checks that don't read its commit file.
fn fake_winner(version: Version) -> WinningCommit {
    let location = Url::parse("memory:///_delta_log/commit.json").unwrap();
    let file = FileMeta::new(location, 0, 0);
    WinningCommit { version, file }
}

fn fake_summary(version: Version, operation: &str, is_blind_append: bool) -> WinnerSummary {
    WinnerSummary {
        version,
        operation: Some(operation.to_string()),
        is_blind_append,
        in_commit_timestamp: None,
        is_manifest_commit: false,
        row_id_high_water_mark: None,
        writes_row_tracking: false,
        removes_row_tracking: false,
        first_violation: None,
    }
}

/// A read relation that no executor retains, for plans that don't read it.
fn no_read_files() -> RelationRef {
    RelationRef::new("read_files", schema_ref! { not_null "path": STRING })
}

/// Makes `facts` an OPTIMIZE, which drops to SnapshotIsolation under WriteSerializable.
fn make_optimize(facts: &mut LoserFacts) {
    facts.has_data_change = false;
    facts.operation_changes_data = false;
}

/// A DELETE under WriteSerializable that read the files in `read_files` and rewrites files.
fn partitioned_delete_facts(
    read_version: Version,
    read_files: &RelationRef,
    partition_schema: SchemaRef,
    partition_predicate: Option<PredicateRef>,
) -> LoserFacts {
    LoserFacts {
        checked_version: read_version,
        isolation_level: IsolationLevel::WriteSerializable,
        has_data_change: true,
        operation_changes_data: true,
        changes_metadata: false,
        has_other_actions: false,
        rewrites_files: true,
        written_domains: vec![],
        read_app_ids: vec![],
        read_scope: ReadScope::Files {
            read_files: read_files.clone(),
            partition_schema,
            partition_predicate,
            map_to_struct_options: MapToStructOptions::default(),
        },
    }
}

fn deleted_date_facts(read_files: &RelationRef) -> LoserFacts {
    partitioned_delete_facts(
        READ_VERSION,
        read_files,
        schema_ref! { nullable "date": DATE },
        Some(equals("date", date(DELETED_DATE))),
    )
}

fn equals(column: &str, value: impl Into<Scalar>) -> PredicateRef {
    Arc::new(Expression::column([column]).eq(lit(value)))
}

fn date(value: &str) -> Scalar {
    PrimitiveType::Date.parse_scalar(value).unwrap()
}

fn winner_summaries(facts: &LoserFacts, winners: &[WinningCommit]) -> Vec<WinnerSummary> {
    try_winner_summaries(facts, winners).unwrap()
}

fn try_winner_summaries(
    facts: &LoserFacts,
    winners: &[WinningCommit],
) -> KernelResult<Vec<WinnerSummary>> {
    let plan = winner_check_plan(facts, winners).unwrap();
    let result = SyncPlanExecutor::default().execute_op(Operation::QueryPlan(plan));
    read_winner_summaries(result.unwrap(), winners)
}

/// T's file actions and read files, retained in a fresh scoped executor.
struct LoserRelations {
    executor: ScopedSyncPlanExecutor,
    file_actions: RelationRef,
    read_files: RelationRef,
}

impl LoserRelations {
    /// T removes the files in `removed` and reads the files in `read`. The OCC plan reads only the
    /// removes of T's file actions.
    fn new(removed: &[&str], read: &[&str]) -> Self {
        let executor = SyncPlanExecutor::default().get_scoped();
        let path_schema = schema_ref! { not_null "path": STRING };
        let remove = |&path: &&str| {
            vec![Scalar::Struct(
                StructData::try_new(path_schema.clone(), vec![path.into()]).unwrap(),
            )]
        };
        let file_actions = schema_ref! { nullable "remove": { not_null "path": STRING } };
        let removes = Vec::from_iter(removed.iter().map(remove));
        let file_actions = retain(&executor, "file_actions", file_actions, removes);
        let reads = Vec::from_iter(read.iter().map(|&path| vec![Scalar::from(path)]));
        let read_files = retain(&executor, "read_files", path_schema, reads);
        Self {
            executor,
            file_actions,
            read_files,
        }
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

// === Expected results ===

fn only_summary(summaries: &[WinnerSummary]) -> &WinnerSummary {
    let [summary] = summaries else {
        panic!("expected one winner summary, got {summaries:?}");
    };
    summary
}

fn conflict(version: Version, operation: Option<&str>, kind: ConflictKind) -> Option<Conflict> {
    Some(Conflict {
        version,
        operation: operation.map(str::to_string),
        kind,
    })
}
