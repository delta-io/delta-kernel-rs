//! Shared action builders and fixtures. T is the losing transaction.

mod winner_check_plan;

use ::test_utils::add_commit;
use futures::executor::block_on;
use serde_json::Value;
use tempfile::TempDir;
use url::Url;

use super::*;
use crate::actions::{CommitInfo, Remove};
use crate::commit_range::CommitRange;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::expressions::Scalar;
use crate::object_store::local::LocalFileSystem;
use crate::plans::{Operation, PlanExecutor as _};
use crate::transaction::occ::winner_check_plan::{
    WinnerSummary, read_winner_summaries, winner_check_plan,
};
use crate::unit_test_utils::Action;

const READ_VERSION: Version = 6;

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

fn remove(path: &str, data_change: bool) -> Value {
    line(Action::Remove(Remove {
        path: path.into(),
        deletion_timestamp: Some(0),
        data_change,
        ..Default::default()
    }))
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
    }
}

fn winner_summaries(winners: &[WinningCommit]) -> Vec<WinnerSummary> {
    try_winner_summaries(winners).unwrap()
}

fn try_winner_summaries(winners: &[WinningCommit]) -> KernelResult<Vec<WinnerSummary>> {
    let plan = winner_check_plan(winners).unwrap();
    let result = SyncPlanExecutor::default().execute_op(Operation::QueryPlan(plan));
    read_winner_summaries(result.unwrap(), winners)
}

// === Expected results ===

fn only_summary(summaries: &[WinnerSummary]) -> &WinnerSummary {
    let [summary] = summaries else {
        panic!("expected one winner summary, got {summaries:?}");
    };
    summary
}
