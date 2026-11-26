use rstest::rstest;

use super::*;
use crate::transaction::occ::winner_check_plan::{COMMIT_INFO_ACTIONS, WINNER_CHECK_OUTPUT_SCHEMA};
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
#[case::commit_info_without_blind_append(
    vec![line(Action::CommitInfo(CommitInfo {
        is_blind_append: None,
        ..base_commit_info("WRITE")
    }))],
    (Some("WRITE"), false, None),
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
    let summaries = summaries(&[
        (7, vec![commit_info("WRITE"), remove("a.parquet", true)]),
        (8, vec![remove("b.parquet", true), commit_info("DELETE")]),
    ]);
    let expected = [
        fake_summary(7, "WRITE", false),
        fake_summary(8, "DELETE", false),
    ];
    assert_eq!(summaries, expected);
}

/// Reads a winner check output with one row at each of `versions` as the summaries of `winners`.
/// Each row counts one `commitInfo` action and has NULL or false in the other columns.
fn read_output(versions: &[i64], winners: &[WinningCommit]) -> KernelResult<Vec<WinnerSummary>> {
    let row = |version: i64| {
        let fields = WINNER_CHECK_OUTPUT_SCHEMA.fields();
        Vec::from_iter(fields.map(|field| match field.name().as_str() {
            VERSION => Scalar::from(version),
            COMMIT_INFO_ACTIONS => Scalar::from(1i64),
            _ if field.is_nullable() => Scalar::Null(field.data_type().clone()),
            _ => Scalar::from(false),
        }))
    };
    let rows = Vec::from_iter(versions.iter().copied().map(row));
    let plan = PlanBuilder::values(WINNER_CHECK_OUTPUT_SCHEMA.clone(), rows).unwrap();
    let result =
        SyncPlanExecutor::default().execute_op(Operation::QueryPlan(plan.build().unwrap()));
    read_winner_summaries(result.unwrap(), winners)
}

#[test]
fn read_winner_summaries_sorts_by_version() {
    let summaries = read_output(&[8, 7], &[fake_winner(7), fake_winner(8)]).unwrap();
    assert_eq!(Vec::from_iter(summaries.iter().map(|s| s.version)), [7, 8]);
}

#[test]
fn extra_winner_summary_fails() {
    assert_result_error_with_message(
        read_output(&[7, 8], &[fake_winner(7)]),
        "Internal error The winner check returned summaries for versions [7, 8], but the winning \
         commits are at versions [7]",
    );
}

#[test]
fn empty_winner_commit_fails_winner_summary_check() {
    let (_table, winners) = table_winners(
        READ_VERSION,
        &[
            (7, vec![commit_info("WRITE")]),
            (8, vec![]),
            (9, vec![commit_info("WRITE")]),
        ],
    );
    assert_result_error_with_message(
        try_winner_summaries(&winners),
        "Generic delta kernel error: Conflict detection read no action from the commit file of \
         version 8",
    );
}

#[test]
fn winner_with_several_commit_infos_fails_winner_summary_check() {
    let (_table, winners) = table_winners(
        READ_VERSION,
        &[
            (7, vec![commit_info("WRITE")]),
            (
                8,
                vec![
                    commit_info("DELETE"),
                    line(Action::CommitInfo(CommitInfo {
                        timestamp: Some(0),
                        ..Default::default()
                    })),
                ],
            ),
        ],
    );
    assert_result_error_with_message(
        try_winner_summaries(&winners),
        "Generic delta kernel error: Conflict detection can't check the winning commits at \
         versions [8], because each has more than one `commitInfo` action",
    );
}
