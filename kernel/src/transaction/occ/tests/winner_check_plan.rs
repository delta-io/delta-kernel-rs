use rstest::rstest;

use super::*;
use crate::transaction::occ::winner_check_plan::{
    WINNER_CHECK_OUTPUT_SCHEMA, ensure_winner_summaries,
};
use crate::unit_test_utils::assert_result_error_with_message;

fn summaries(commits: &[(Version, Vec<Value>)]) -> Vec<WinnerSummary> {
    let (_table, winners) = table_winners(READ_VERSION, commits);
    winner_summaries(&winners)
}

// === Winner summaries ===

#[rstest]
#[case::commit_info_fields(
    vec![line(Action::CommitInfo(CommitInfo {
        is_blind_append: Some(true),
        in_commit_timestamp: Some(1_700_000_000_000),
        ..base_commit_info("WRITE")
    }))],
    (Some("WRITE"), true, Some(1_700_000_000_000)),
)]
#[case::missing_commit_info(vec![remove("a.parquet", true)], (None, false, None))]
fn winner_summary_reads_commit_info(
    #[case] actions: Vec<Value>,
    #[case] expected: (Option<&str>, bool, Option<i64>),
) {
    let summaries = summaries(&[(7, actions)]);
    let summary = only_summary(&summaries);
    let commit_info = (
        summary.operation.as_deref(),
        summary.is_blind_append,
        summary.in_commit_timestamp,
    );
    assert_eq!((summary.version, commit_info), (7, expected));
}

#[test]
fn winner_summaries_match_winners_with_several_actions() {
    let (_table, winners) = table_winners(
        READ_VERSION,
        &[
            (7, vec![commit_info("WRITE"), remove("a.parquet", true)]),
            (8, vec![remove("b.parquet", true), commit_info("DELETE")]),
        ],
    );
    let summaries = winner_summaries(&winners);
    ensure_winner_summaries(&winners, &summaries).unwrap();
    let expected = [
        fake_summary(7, "WRITE", false),
        fake_summary(8, "DELETE", false),
    ];
    assert_eq!(summaries, expected);
}

#[test]
fn read_winner_summaries_sorts_by_version() {
    let row = |version: i64| {
        let fields = WINNER_CHECK_OUTPUT_SCHEMA.fields();
        Vec::from_iter(fields.map(|field| match field.name().as_str() {
            VERSION => Scalar::from(version),
            _ if field.is_nullable() => Scalar::Null(field.data_type().clone()),
            _ => Scalar::from(false),
        }))
    };
    let plan = PlanBuilder::values(WINNER_CHECK_OUTPUT_SCHEMA.clone(), vec![row(8), row(7)]);
    let result = SyncPlanExecutor::default()
        .execute_op(Operation::QueryPlan(plan.unwrap().build().unwrap()));
    let summaries = read_winner_summaries(result.unwrap()).unwrap();
    assert_eq!(Vec::from_iter(summaries.iter().map(|s| s.version)), [7, 8]);
}

#[test]
fn extra_winner_summary_fails() {
    let summaries = [
        fake_summary(7, "WRITE", false),
        fake_summary(8, "WRITE", false),
    ];
    assert_result_error_with_message(
        ensure_winner_summaries(&[fake_winner(7)], &summaries),
        "Internal error The winner check returned summaries for versions [7, 8], but the winning \
         commits are at versions [7]",
    );
}

#[test]
fn empty_winner_commit_fails_winner_summary_check() {
    let (_table, winners) = table_winners(READ_VERSION, &[(7, vec![])]);
    let summaries = winner_summaries(&winners);
    assert_result_error_with_message(
        ensure_winner_summaries(&winners, &summaries),
        "Generic delta kernel error: Conflict detection read no action from the commit file of \
         version 7",
    );
}
