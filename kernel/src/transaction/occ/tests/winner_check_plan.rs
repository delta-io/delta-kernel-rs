use rstest::rstest;

use super::*;
use crate::transaction::occ::ConflictKind::Winner;
use crate::transaction::occ::winner_check_plan::{
    COMMIT_INFO_ACTIONS, ROW_TRACKING_BACKFILL_OPERATION, ROW_TRACKING_UNBACKFILL_OPERATION,
    WINNER_CHECK_OUTPUT_SCHEMA, ensure_no_manifest_commit, ensure_row_tracking_marks,
    first_winner_conflict,
};
use crate::unit_test_utils::assert_result_error_with_message;

/// The winner check summaries of `commits` for the default T after `adjust`.
fn summaries_with(
    adjust: impl FnOnce(&mut LoserFacts),
    commits: &[(Version, Vec<Value>)],
) -> Vec<WinnerSummary> {
    let (_table, winners) = table_winners(READ_VERSION, commits);
    let mut facts = deleted_date_facts(&no_read_files());
    adjust(&mut facts);
    winner_summaries(&facts, &winners)
}

fn summaries(commits: &[(Version, Vec<Value>)]) -> Vec<WinnerSummary> {
    summaries_with(|_| {}, commits)
}

fn rule_at_7(operation: &str, rule: WinnerRule) -> Option<Conflict> {
    conflict(7, Some(operation), Winner(rule))
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
        try_winner_summaries(&deleted_date_facts(&no_read_files()), &winners),
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
        try_winner_summaries(&deleted_date_facts(&no_read_files()), &winners),
        "Generic delta kernel error: Conflict detection can't check the winning commits at \
         versions [8], because each has more than one `commitInfo` action",
    );
}

// === Winner rules ===

#[rstest]
#[case::protocol_fails(vec![protocol()], &[], &[], rule_at_7("WRITE", WinnerRule::ProtocolChanged))]
#[case::metadata_fails(vec![metadata()], &[], &[], rule_at_7("WRITE", WinnerRule::MetadataChanged))]
#[case::domain_written_by_both_fails(
    vec![domain("app.x", "{}")],
    &["app.y", ROW_TRACKING_DOMAIN_NAME, "app.x"],
    &[],
    rule_at_7("WRITE", WinnerRule::DomainConflict),
)]
#[case::domain_tombstone_written_by_both_fails(
    vec![removed_domain("app.x")], &["app.x"], &[], rule_at_7("WRITE", WinnerRule::DomainConflict),
)]
#[case::domain_written_by_winner_only_passes(vec![domain("app.x", "{}")], &["app.y"], &[], None)]
#[case::row_tracking_domain_on_both_sides_passes(
    vec![row_tracking(1099)], &[ROW_TRACKING_DOMAIN_NAME], &[], None,
)]
#[case::txn_for_read_app_id_fails(
    vec![txn("app-b")], &[], &["app-a", "app-b"], rule_at_7("WRITE", WinnerRule::TxnConflict),
)]
#[case::txn_for_other_app_id_passes(vec![txn("app-a")], &[], &["app-b"], None)]
#[case::protocol_reported_before_metadata(
    vec![protocol(), metadata()], &[], &[], rule_at_7("WRITE", WinnerRule::ProtocolChanged),
)]
#[case::domain_reported_before_txn(
    vec![domain("app.x", "{}"), txn("app-a")],
    &["app.x"],
    &["app-a"],
    rule_at_7("WRITE", WinnerRule::DomainConflict),
)]
fn winner_rule(
    #[case] actions: Vec<Value>,
    #[case] written_domains: &[&str],
    #[case] read_app_ids: &[&str],
    #[case] expected: Option<Conflict>,
) {
    let mut winner_actions = vec![commit_info("WRITE")];
    winner_actions.extend(actions);
    let set_facts = |facts: &mut LoserFacts| {
        facts.written_domains = Vec::from_iter(written_domains.iter().map(|d| d.to_string()));
        facts.read_app_ids = Vec::from_iter(read_app_ids.iter().map(|a| a.to_string()));
    };
    let summaries = summaries_with(set_facts, &[(7, winner_actions)]);
    assert_eq!(first_winner_conflict(&summaries), expected);
}

#[rstest]
// `Metadata` can't leave out its required fields, so the case writes the JSON.
#[case::metadata_without_commit_info_or_required_fields_fails(
    vec![(7, vec![json!({"metaData": {"id": "test-table"}})])],
    conflict(7, None, Winner(WinnerRule::MetadataChanged)),
)]
#[case::earliest_winner_rule_is_reported(
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("DELETE"), protocol()]),
    ],
    rule_at_7("WRITE", WinnerRule::MetadataChanged),
)]
#[case::rule_after_clean_winner_is_reported(
    vec![
        (7, vec![commit_info("WRITE")]),
        (8, vec![commit_info("DELETE"), protocol()]),
    ],
    conflict(8, Some("DELETE"), Winner(WinnerRule::ProtocolChanged)),
)]
fn first_winner_rule(
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] expected: Option<Conflict>,
) {
    assert_eq!(first_winner_conflict(&summaries(&commits)), expected);
}

#[rstest]
#[case::identity_only_passes_without_metadata_change("true", false, false)]
#[case::identity_only_fails_with_metadata_change("true", true, true)]
#[case::non_canonical_case_tag_is_not_identity_only("TRUE", false, true)]
fn identity_only_metadata_winner(
    #[case] tag: &str,
    #[case] changes_metadata: bool,
    #[case] fails: bool,
) {
    let tags = HashMap::from([("delta.identity.schemaUpdate".into(), Some(tag.into()))]);
    let info = CommitInfo {
        tags: Some(tags),
        ..base_commit_info("UPDATE SCHEMA")
    };
    let actions = vec![line(Action::CommitInfo(info)), metadata()];
    let set_facts = |facts: &mut LoserFacts| facts.changes_metadata = changes_metadata;
    let summaries = summaries_with(set_facts, &[(7, actions)]);
    let expected = rule_at_7("UPDATE SCHEMA", WinnerRule::MetadataChanged).filter(|_| fails);
    assert_eq!(first_winner_conflict(&summaries), expected);
}

// === Row tracking and manifest commits ===

#[rstest]
#[case::backfill_fails_a_rewrite(ROW_TRACKING_BACKFILL_OPERATION, true)]
#[case::backfill_passes_without_a_rewrite(ROW_TRACKING_BACKFILL_OPERATION, false)]
#[case::unbackfill_fails_a_rewrite(ROW_TRACKING_UNBACKFILL_OPERATION, true)]
fn row_tracking_backfill_winner(#[case] operation: &str, #[case] rewrites_files: bool) {
    let set_facts = |facts: &mut LoserFacts| facts.rewrites_files = rewrites_files;
    let summaries = summaries_with(set_facts, &[(7, vec![commit_info(operation)])]);
    let expected = rule_at_7(operation, WinnerRule::BackfillConflict).filter(|_| rewrites_files);
    assert_eq!(first_winner_conflict(&summaries), expected);
}

#[test]
fn row_tracking_mark_comes_only_from_live_row_tracking_domain() {
    let actions = vec![
        commit_info("WRITE"),
        domain("app.x", "{not json"),
        row_tracking(1099),
    ];
    let summaries = summaries(&[(7, actions)]);
    assert_eq!(only_summary(&summaries).row_id_high_water_mark, Some(1099));
    ensure_no_manifest_commit(&summaries).unwrap();
    ensure_row_tracking_marks(&summaries).unwrap();
}

#[test]
fn removed_other_domain_passes_row_tracking_check() {
    let summaries = summaries(&[(7, vec![commit_info("WRITE"), removed_domain("app.x")])]);
    ensure_row_tracking_marks(&summaries).unwrap();
}

#[rstest]
#[case::row_tracking_configurations_without_mark_in_two_winners(
    vec![
        (7, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")]),
        (8, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{not json")]),
    ],
    "Generic delta kernel error: Conflict detection could not read the rowIdHighWaterMark of the \
     `delta.rowTracking` domain in the winning commits at versions [7, 8]",
)]
#[case::row_tracking_tombstone(
    vec![(7, vec![commit_info("WRITE"), removed_domain(ROW_TRACKING_DOMAIN_NAME)])],
    "Unsupported: The winning commit at version 7 removes the `delta.rowTracking` domain",
)]
#[case::row_tracking_tombstone_reported_before_earlier_unreadable_mark(
    vec![
        (7, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")]),
        (8, vec![commit_info("WRITE"), removed_domain(ROW_TRACKING_DOMAIN_NAME)]),
    ],
    "Unsupported: The winning commit at version 8 removes the `delta.rowTracking` domain",
)]
#[case::manifest_commit(
    vec![(7, vec![commit_info("WRITE"), manifest_checkpoint(7)])],
    "Unsupported: Conflict detection does not support the manifest commit at version 7",
)]
fn row_tracking_and_manifest_checks(
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] message: &str,
) {
    let summaries = summaries(&commits);
    let result =
        ensure_no_manifest_commit(&summaries).and_then(|()| ensure_row_tracking_marks(&summaries));
    assert_result_error_with_message(result, message);
}
