use rstest::rstest;

use super::*;
use crate::transaction::occ::ConflictKind::{Occ, Winner};
use crate::unit_test_utils::assert_result_error_with_message;

/// Runs conflict detection for the default T after `adjust`, against the winners in `commits`.
fn detect_delete_with(
    adjust: impl FnOnce(&mut LoserFacts),
    commits: &[(Version, Vec<Value>)],
) -> KernelResult<Option<Conflict>> {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let mut facts = deleted_date_facts(&relations.read_files);
    adjust(&mut facts);
    let (_table, winners) = table_winners(facts.checked_version, commits);
    relations.detect(&facts, &winners).map(outcome)
}

#[rstest]
#[case::winner_rule_before_earlier_occ_violation(
    vec![
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), protocol()]),
    ],
    Ok(conflict(8, Some("WRITE"), Winner(WinnerRule::ProtocolChanged))),
)]
#[case::empty_winner_commit_before_later_winner_rule(
    vec![(7, vec![]), (8, vec![commit_info("WRITE"), protocol()])],
    Err(
        "Generic delta kernel error: Conflict detection read no action from the commit file of \
         version 7",
    ),
)]
#[case::winner_rule_before_later_manifest_commit(
    vec![
        (7, vec![commit_info("WRITE"), metadata()]),
        (8, vec![commit_info("WRITE"), manifest_checkpoint(8)]),
    ],
    Ok(conflict(7, Some("WRITE"), Winner(WinnerRule::MetadataChanged))),
)]
#[case::manifest_commit_before_earlier_occ_violation(
    vec![
        (7, vec![commit_info("DELETE"), remove(DELETED_FILE, true)]),
        (8, vec![commit_info("WRITE"), manifest_checkpoint(8)]),
    ],
    Err("Unsupported: Conflict detection does not support the manifest commit at version 8"),
)]
#[case::occ_violation_before_row_tracking_tombstone(
    vec![(
        7,
        vec![
            commit_info("DELETE"),
            remove(DELETED_FILE, true),
            removed_domain(ROW_TRACKING_DOMAIN_NAME),
        ],
    )],
    Ok(conflict(7, Some("DELETE"), Occ(OccCheck::ReadDelete))),
)]
// A winner without `commitInfo` has a NULL operation, which breaks no winner rule.
#[case::winner_without_commit_info_conflicts_without_operation(
    vec![(7, vec![remove(DELETED_FILE, true)])],
    Ok(conflict(7, None, Occ(OccCheck::ReadDelete))),
)]
#[case::unreadable_row_tracking_mark_without_other_conflict(
    vec![(7, vec![commit_info("WRITE"), domain(ROW_TRACKING_DOMAIN_NAME, "{}")])],
    Err(
        "Conflict detection could not read the rowIdHighWaterMark of the `delta.rowTracking` \
         domain in the winning commits at versions [7]",
    ),
)]
#[case::row_tracking_tombstone_without_other_conflict(
    vec![(7, vec![commit_info("WRITE"), removed_domain(ROW_TRACKING_DOMAIN_NAME)])],
    Err("Unsupported: The winning commit at version 7 removes the `delta.rowTracking` domain"),
)]
fn check_order(
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] expected: Result<Option<Conflict>, &str>,
) {
    let result = detect_delete_with(|_| {}, &commits);
    match expected {
        Ok(expected) => assert_eq!(result.unwrap(), expected),
        Err(message) => assert_result_error_with_message(result, message),
    }
}

#[rstest]
#[case::blind_append_passes_winner_that_adds_and_removes_files(&[], &[], None)]
#[case::blind_append_conflicts_with_txn_for_read_app_id(
    &[],
    &["app-a"],
    conflict(7, Some("DELETE"), Winner(WinnerRule::TxnConflict)),
)]
// T removes a file but passes `ReadScope::Nothing` and `rewrites_files = false`, which
// `detect_conflicts` accepts.
#[case::inconsistent_facts_still_run_delete_delete(
    &[DELETED_FILE],
    &[],
    conflict(7, Some("DELETE"), Occ(OccCheck::DeleteDelete)),
)]
fn loser_that_read_nothing(
    #[case] removed: &[&str],
    #[case] read_app_ids: &[&str],
    #[case] expected: Option<Conflict>,
) {
    let relations = LoserRelations::new(removed, &[]);
    let mut facts = deleted_date_facts(&relations.read_files);
    facts.read_scope = ReadScope::Nothing;
    facts.rewrites_files = false;
    facts.read_app_ids = Vec::from_iter(read_app_ids.iter().map(|a| a.to_string()));
    let winner_actions = vec![
        commit_info("DELETE"),
        add("f7", DELETED_DATE),
        remove(DELETED_FILE, true),
        txn("app-a"),
    ];
    let (_table, winners) = table_winners(READ_VERSION, &[(7, winner_actions)]);
    let detection = relations.detect(&facts, &winners).unwrap();
    assert_eq!(outcome(detection), expected);
}

#[test]
fn detect_conflicts_without_winners_has_no_conflict() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let detection = relations.detect(&facts, &[]).unwrap();
    assert!(matches!(detection, ConflictDetection::NoConflict(summaries) if summaries.is_empty()));
}

#[test]
fn no_conflict_returns_winner_summaries() {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let commits = [
        (7, vec![commit_info("WRITE"), add("f7", OTHER_DATE)]),
        (
            8,
            vec![
                commit_info("DELETE"),
                remove("date=2026-10-02/part-b.parquet", true),
            ],
        ),
    ];
    let (_table, winners) = table_winners(READ_VERSION, &commits);
    let detection = relations.detect(&facts, &winners).unwrap();
    let ConflictDetection::NoConflict(summaries) = detection else {
        panic!("expected no conflict, got {detection:?}");
    };
    let expected = [
        fake_summary(7, "WRITE", false),
        fake_summary(8, "DELETE", false),
    ];
    assert_eq!(summaries, expected);
}

#[rstest]
#[case::gap(&[7, 9])]
#[case::first_winner_after_checked_version_plus_one(&[8, 9])]
#[case::first_winner_at_checked_version(&[6, 7])]
fn winner_versions_must_follow_checked_version(#[case] versions: &[Version]) {
    let relations = LoserRelations::new(&[DELETED_FILE], &[DELETED_FILE]);
    let facts = deleted_date_facts(&relations.read_files);
    let winners = Vec::from_iter(versions.iter().map(|&version| fake_winner(version)));
    assert_result_error_with_message(
        relations.detect(&facts, &winners),
        &format!(
            "Internal error Winning commits {versions:?} must start at checked version 6 + 1 with \
             no version gap"
        ),
    );
}

#[rstest]
#[case::table_snapshot_isolation(
    |facts: &mut LoserFacts| facts.isolation_level = IsolationLevel::SnapshotIsolation,
    vec![(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])],
    "Unsupported: Conflict detection does not support the table isolation level SnapshotIsolation",
)]
#[case::manifest_commit_loser(
    |facts: &mut LoserFacts| facts.is_manifest_commit = true,
    vec![(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])],
    "Unsupported: Conflict detection does not support a transaction that writes a manifest commit",
)]
#[case::read_nothing_with_rewrites(
    |facts: &mut LoserFacts| facts.read_scope = ReadScope::Nothing,
    vec![(7, vec![commit_info("WRITE"), add("f7", DELETED_DATE)])],
    "Internal error A transaction that removes or re-adds files must pass the files that it read",
)]
#[case::manifest_commit_loser_without_winners(
    |facts: &mut LoserFacts| facts.is_manifest_commit = true,
    vec![],
    "Unsupported: Conflict detection does not support a transaction that writes a manifest commit",
)]
fn invalid_loser_fails_detection(
    #[case] adjust: fn(&mut LoserFacts),
    #[case] commits: Vec<(Version, Vec<Value>)>,
    #[case] message: &str,
) {
    assert_result_error_with_message(detect_delete_with(adjust, &commits), message);
}
