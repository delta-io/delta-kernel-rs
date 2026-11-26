use rstest::rstest;

use super::*;
use crate::expressions::{Predicate, col};
use crate::transaction::occ::ConflictKind::Occ;
use crate::transaction::occ::occ_plan::{
    first_occ_conflict, occ_plan, select_read_append_versions,
};
use crate::transaction::occ::winner_check_plan::{
    ROW_TRACKING_BACKFILL_OPERATION, ROW_TRACKING_UNBACKFILL_OPERATION,
};

/// The first OCC violation of the default T after `adjust`.
fn delete_conflict(
    adjust: impl FnOnce(&mut LoserFacts),
    commits: &[(Version, Vec<Value>)],
) -> Option<Conflict> {
    let mut facts = deleted_date_facts();
    adjust(&mut facts);
    let (_table, winners) = table_winners(facts.checked_version, commits);
    let summaries = winner_summaries(&facts, &winners);
    let versions = select_read_append_versions(&facts, &summaries);
    let plan = occ_plan(&facts, &winners, &versions).unwrap();
    let result = SyncPlanExecutor::default().execute_op(Operation::QueryPlan(plan));
    first_occ_conflict(result.unwrap(), &summaries).unwrap()
}

fn write_at_7(check: OccCheck) -> Option<Conflict> {
    conflict(7, Some("WRITE"), Occ(check))
}

// === File checks ===

#[rstest]
#[case::add_without_data_change_in_deleted_partition_fails(
    vec![
        commit_info("WRITE"),
        line(Action::Add(Add { data_change: false, ..base_add("f7", &[("date", DELETED_DATE)]) })),
    ],
    Some("WRITE"),
)]
#[case::add_without_commit_info_fails_with_no_operation(vec![add("f7", DELETED_DATE)], None)]
fn add_in_read_partition_fails_read_append(
    #[case] actions: Vec<Value>,
    #[case] operation: Option<&str>,
) {
    let result = delete_conflict(|_| {}, &[(7, actions)]);
    assert_eq!(result, conflict(7, operation, Occ(OccCheck::ReadAppend)));
}

#[test]
fn write_serializable_passes_blind_append_in_read_partition() {
    let mut blind_append = base_commit_info("WRITE");
    blind_append.is_blind_append = Some(true);
    let blind_append = line(Action::CommitInfo(blind_append));
    let commits = [
        (7, vec![blind_append, add("f7", DELETED_DATE)]),
        (8, vec![commit_info("WRITE"), add("f8", OTHER_DATE)]),
    ];
    assert_eq!(delete_conflict(|_| {}, &commits), None);
}

#[test]
fn earliest_read_append_version_is_reported() {
    // Versions compare as numbers, so 9 comes before 10.
    let commits = [
        (9, vec![commit_info("WRITE"), add("f9", DELETED_DATE)]),
        (10, vec![commit_info("WRITE"), add("f10", DELETED_DATE)]),
    ];
    let result = delete_conflict(|facts| facts.checked_version = 8, &commits);
    let expected = conflict(9, Some("WRITE"), Occ(OccCheck::ReadAppend));
    assert_eq!(result, expected);
}

// === Read scope ===

#[rstest]
#[case::whole_table_conflicts_with_add_in_other_partition(
    ReadScope::WholeTable,
    add("f7", OTHER_DATE),
    write_at_7(OccCheck::ReadAppend)
)]
#[case::nothing_passes_add_in_read_partition(ReadScope::Nothing, add("f7", DELETED_DATE), None)]
fn whole_table_or_nothing_read_scope(
    #[case] read_scope: ReadScope,
    #[case] action: Value,
    #[case] expected: Option<Conflict>,
) {
    let set_scope = |facts: &mut LoserFacts| {
        facts.read_scope = read_scope;
        facts.rewrites_files = !facts.read_scope.is_nothing();
    };
    let commits = [(7, vec![commit_info("WRITE"), action])];
    assert_eq!(delete_conflict(set_scope, &commits), expected);
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
#[case::loser_that_read_nothing_skips_all(
    |facts: &mut LoserFacts| {
        facts.read_scope = ReadScope::Nothing;
        facts.rewrites_files = false;
    },
    vec![],
)]
#[case::optimize_drops_to_snapshot_isolation(make_optimize, vec![])]
#[case::optimize_with_metadata_change_keeps_write_serializable(
    |facts: &mut LoserFacts| { make_optimize(facts); facts.changes_metadata = true; },
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
    |facts: &mut LoserFacts| { make_optimize(facts); facts.has_other_actions = true; },
    vec![7, 9],
)]
#[case::write_serializable_data_changing_operation_keeps_isolation(
    |facts: &mut LoserFacts| facts.has_data_change = false,
    vec![7, 9],
)]
#[case::write_serializable_data_change_keeps_isolation_for_operation_without_data_change(
    |facts: &mut LoserFacts| facts.operation_changes_data = false,
    vec![7, 9],
)]
#[case::serializable_without_data_change_drops_isolation(
    |facts: &mut LoserFacts| {
        facts.isolation_level = IsolationLevel::Serializable;
        facts.has_data_change = false;
    },
    vec![],
)]
#[case::snapshot_isolation_without_data_change_checks_nothing(
    |facts: &mut LoserFacts| {
        facts.isolation_level = IsolationLevel::SnapshotIsolation;
        facts.has_data_change = false;
    },
    vec![],
)]
fn read_append_versions(#[case] adjust: fn(&mut LoserFacts), #[case] expected: Vec<Version>) {
    let mut facts = deleted_date_facts();
    adjust(&mut facts);
    // No case expects version 10 or 11: `read_append` never checks a row-tracking backfill or
    // unbackfill.
    let summaries = [
        fake_summary(7, "WRITE", false),
        fake_summary(8, "WRITE", true),
        fake_summary(9, "MERGE", false),
        fake_summary(10, ROW_TRACKING_BACKFILL_OPERATION, false),
        fake_summary(11, ROW_TRACKING_UNBACKFILL_OPERATION, false),
    ];
    assert_eq!(select_read_append_versions(&facts, &summaries), expected);
}

// === Partition values ===

/// The first OCC violation of the default T with the given partition read, against `winner_add`.
fn partition_conflict(
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    options: MapToStructOptions,
    winner_add: Value,
) -> Option<Conflict> {
    let set_partitions = |facts: &mut LoserFacts| {
        let ReadScope::Files {
            partition_schema,
            partition_predicate,
            map_to_struct_options,
            ..
        } = &mut facts.read_scope
        else {
            panic!("expected a files-scoped read");
        };
        *partition_schema = schema;
        *partition_predicate = predicate;
        *map_to_struct_options = options;
    };
    let commits = [(7, vec![commit_info("WRITE"), winner_add])];
    delete_conflict(set_partitions, &commits)
}

fn deleted_date() -> Option<PredicateRef> {
    Some(equals("date", date(DELETED_DATE)))
}

/// An add of `f7` whose `partitionValues` JSON is `values`. `Add` can't hold a null value.
fn add_with_partition_values(values: Value) -> Value {
    let mut winner_add = add("f7", DELETED_DATE);
    winner_add["add"]["partitionValues"] = values;
    winner_add
}

fn null_date() -> Value {
    add_with_partition_values(json!({"date": null}))
}

#[rstest]
#[case::predicate_matches_read_partition(
    deleted_date(),
    add("f7", DELETED_DATE),
    write_at_7(OccCheck::ReadAppend)
)]
#[case::predicate_skips_other_partition(deleted_date(), add("f7", OTHER_DATE), None)]
#[case::none_matches_every_partition(None, add("f7", OTHER_DATE), write_at_7(OccCheck::ReadAppend))]
#[case::equality_predicate_skips_null_value(deleted_date(), null_date(), None)]
fn partition_predicate_selects_adds(
    #[case] predicate: Option<PredicateRef>,
    #[case] winner_add: Value,
    #[case] expected: Option<Conflict>,
) {
    let schema = schema_ref! { nullable "date": DATE };
    let options = MapToStructOptions::default();
    assert_eq!(
        partition_conflict(schema, predicate, options, winner_add),
        expected
    );
}

#[rstest]
#[case::both_values_match(DELETED_DATE, "us", write_at_7(OccCheck::ReadAppend))]
#[case::other_region_passes(DELETED_DATE, "eu", None)]
#[case::other_date_passes(OTHER_DATE, "us", None)]
fn two_partition_columns_must_both_match(
    #[case] winner_date: &str,
    #[case] winner_region: &str,
    #[case] expected: Option<Conflict>,
) {
    let schema = schema_ref! { nullable "date": DATE, nullable "region": STRING };
    let predicate = Predicate::and(
        col!("date").eq(lit(date(DELETED_DATE))),
        col!("region").eq(lit("us")),
    );
    let predicate = Some(Arc::new(predicate));
    let winner_add =
        add_with_partition_values(json!({"date": winner_date, "region": winner_region}));
    let options = MapToStructOptions::default();
    assert_eq!(
        partition_conflict(schema, predicate, options, winner_add),
        expected
    );
}

#[rstest]
#[case::not_null_partition_field_reads_null_value_as_null(
    schema_ref! { not_null "date": DATE },
    Some(Arc::new(col!("date").is_null())),
    null_date()
)]
#[case::unpartitioned_table_ignores_false_predicate(
    schema_ref! {},
    Some(Arc::new(Predicate::FALSE)),
    add_with_partition_values(json!({}))
)]
fn partition_schema_selects_adds(
    #[case] schema: SchemaRef,
    #[case] predicate: Option<PredicateRef>,
    #[case] winner_add: Value,
) {
    let options = MapToStructOptions::default();
    assert_eq!(
        partition_conflict(schema, predicate, options, winner_add),
        write_at_7(OccCheck::ReadAppend)
    );
}

#[test]
fn timestamp_partition_is_read_in_connector_timezone() {
    // 2026-10-01 23:30 in Los Angeles (PDT) is 2026-10-02 06:30 UTC.
    let instant = PrimitiveType::Timestamp.parse_scalar("2026-10-02T06:30:00Z");
    let predicate = Some(equals("ts", instant.unwrap()));
    let options = MapToStructOptions::default().with_timestamp_timezone("America/Los_Angeles");
    let winner_add = line(Action::Add(base_add(
        "f7",
        &[("ts", "2026-10-01 23:30:00")],
    )));
    let schema = schema_ref! { nullable "ts": TIMESTAMP };
    assert_eq!(
        partition_conflict(schema, predicate, options, winner_add),
        write_at_7(OccCheck::ReadAppend)
    );
}
