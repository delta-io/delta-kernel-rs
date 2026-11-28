use rstest::rstest;
use test_utils::assert_result_error_with_message;

use super::*;
use crate::commit_range::tests::{
    COMMIT_INFO_NO_ICT, ICT_METADATA_ENABLED, ICT_PROTOCOL_LINE, UNSUPPORTED_PROTOCOL_LINE,
    VALID_METADATA_LINE, VALID_PROTOCOL_LINE, commit_info_with_ict, engine_with_commits,
    mapped_int,
};
use crate::commit_range::{CommitOrdering, DeltaAction, SchemaChange};
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::expressions::ColumnName;
use crate::schema::StructType;
use crate::unit_test_utils::MockTableConfigurationBuilder;
use crate::{Engine, Operation, Snapshot};

/// Runs `summary_plan` on `engine` and returns its rows.
fn execute_summary_plan(
    summary_plan: &CommitSummaryPlan,
    engine: &dyn Engine,
) -> Vec<KernelResult<Box<dyn EngineData>>> {
    engine
        .require_plan_executor()
        .unwrap()
        .execute_op(Operation::QueryPlan(summary_plan.plan().clone()))
        .unwrap()
        .into_data()
        .unwrap()
        .collect()
}

/// Summarizes the commits of `range` by executing its summary plan on `engine`.
fn summarize(
    range: &CommitRange,
    engine: &dyn Engine,
    start_snapshot: Option<SnapshotRef>,
) -> Result<Vec<CommitSummary>> {
    let summary_plan = range.get_commit_summary_plan(start_snapshot)?;
    summary_plan.validate(execute_summary_plan(&summary_plan, engine))
}

/// The range `[start_version, end_version]` of the table at `table_root`.
fn range_over(
    engine: &dyn Engine,
    table_root: &str,
    start_version: Version,
    end_version: Version,
) -> CommitRange {
    CommitRange::builder_for(table_root, start_version)
        .with_end_version(end_version)
        .build(engine)
        .unwrap()
}

fn snapshot_at(engine: &dyn Engine, table_root: &str, version: Version) -> SnapshotRef {
    Snapshot::builder_for(table_root)
        .at_version(version)
        .build(engine)
        .unwrap()
}

/// The version and timestamp of every commit of `range`, as [`CommitRange::commits`] reports
/// them.
fn imperative_commits(range: &CommitRange, engine: &Arc<dyn Engine>) -> Vec<(Version, i64)> {
    range
        .commits(engine.clone(), None, &[DeltaAction::Add])
        .unwrap()
        .map(|commit| commit.map(|commit| (commit.version(), commit.timestamp())))
        .try_collect()
        .unwrap()
}

/// Builds the range `[start_version, end_version]` over `commits` and summarizes it without a
/// start snapshot.
async fn summarize_commits(
    commits: &[(u64, &str)],
    start_version: Version,
    end_version: Version,
) -> Result<Vec<CommitSummary>> {
    let (engine, table_root) = engine_with_commits(commits).await;
    let range = CommitRange::builder_for(table_root, start_version)
        .with_end_version(end_version)
        .build(engine.as_ref())?;
    summarize(&range, engine.as_ref(), None)
}

fn add_line(path: &str, data_change: bool) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":1,"dataChange":{data_change}}}}}"#
    )
}

fn remove_line(path: &str, data_change: bool) -> String {
    format!(r#"{{"remove":{{"path":"{path}","dataChange":{data_change}}}}}"#)
}

fn commit_info_line(operation: &str, metrics: &[(&str, &str)]) -> String {
    let metrics: serde_json::Map<_, _> = metrics
        .iter()
        .map(|(name, value)| (name.to_string(), (*value).into()))
        .collect();
    serde_json::json!({
        "commitInfo": {"timestamp": 0, "operation": operation, "operationMetrics": metrics}
    })
    .to_string()
}

fn metadata_line(
    schema: &StructType,
    partition_columns: &[&str],
    configuration: &[(&str, &str)],
) -> String {
    let configuration: serde_json::Map<_, _> = configuration
        .iter()
        .map(|(name, value)| (name.to_string(), (*value).into()))
        .collect();
    serde_json::json!({
        "metaData": {
            "id": "00000000-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": serde_json::to_string(schema).unwrap(),
            "partitionColumns": partition_columns,
            "configuration": configuration,
            "createdTime": 1000,
        }
    })
    .to_string()
}

const COLUMN_MAPPING_PROTOCOL_LINE: &str =
    r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}"#;

fn column_mapping_metadata_line(schema: &StructType) -> String {
    metadata_line(schema, &[], &[("delta.columnMapping.mode", "name")])
}

/// The file facts of a summary: data change, then added and removed files.
type Facts = (Option<bool>, [Option<i64>; 2]);

fn facts(summary: &CommitSummary) -> Facts {
    (
        summary.has_data_change(),
        [summary.num_added_files(), summary.num_removed_files()],
    )
}

/// Operation metrics by name, sorted.
type Metrics<'a> = Vec<(&'a str, &'a str)>;

/// The operation of a summary and its metrics.
fn operation(summary: &CommitSummary) -> (Option<&str>, Option<Metrics<'_>>) {
    let metrics = summary.operation_metrics().map(|metrics| {
        let metrics = metrics.iter().map(|(name, value)| (&**name, &**value));
        metrics.sorted().collect()
    });
    (summary.operation(), metrics)
}

#[test]
fn summary_plan_produces_the_schema_that_validation_reads() {
    let plan = summary_plan(Vec::new()).unwrap();
    assert_eq!(plan.schema, *SUMMARY_SCHEMA);
}

#[tokio::test]
async fn summarizes_every_commit_of_a_range() {
    let schema = schema! { nullable "id": LONG };
    let commits = [
        [
            VALID_PROTOCOL_LINE.to_string(),
            metadata_line(&schema, &[], &[]),
            commit_info_line("WRITE", &[("numFiles", "2"), ("numOutputRows", "3")]),
            add_line("a", true),
            add_line("b", true),
        ]
        .join("\n"),
        [
            commit_info_line("DELETE", &[("numDeletedRows", "1"), ("numCopiedRows", "2")]),
            remove_line("a", true),
            add_line("c", true),
        ]
        .join("\n"),
        [
            commit_info_line("OPTIMIZE", &[("numAddedFiles", "1")]),
            remove_line("b", false),
            remove_line("c", false),
            add_line("d", false),
        ]
        .join("\n"),
        add_line("e", true),
    ];
    let commits = Vec::from_iter(
        commits
            .iter()
            .enumerate()
            .map(|(v, c)| (v as u64, c.as_str())),
    );
    let (engine, table_root) = engine_with_commits(&commits).await;
    let range = range_over(engine.as_ref(), table_root, 0, 3);

    let summaries = summarize(&range, engine.as_ref(), None).unwrap();

    let expected: [Facts; 4] = [
        (Some(true), [Some(2), Some(0)]),
        (Some(true), [Some(1), Some(1)]),
        (Some(false), [Some(1), Some(2)]),
        (Some(true), [Some(1), Some(0)]),
    ];
    assert_eq!(Vec::from_iter(summaries.iter().map(facts)), expected);

    // The operation and its metrics are reported as the writer recorded them.
    let expected = [
        (
            Some("WRITE"),
            Some(vec![("numFiles", "2"), ("numOutputRows", "3")]),
        ),
        (
            Some("DELETE"),
            Some(vec![("numCopiedRows", "2"), ("numDeletedRows", "1")]),
        ),
        (Some("OPTIMIZE"), Some(vec![("numAddedFiles", "1")])),
        (None, None),
    ];
    assert_eq!(Vec::from_iter(summaries.iter().map(operation)), expected);

    // Versions and timestamps match the imperative iterator.
    let declarative = Vec::from_iter(summaries.iter().map(|s| (s.version(), s.timestamp())));
    assert_eq!(declarative, imperative_commits(&range, &engine));

    // The first commit sets the schema, so the schema before it is unknown. Later commits leave
    // it unchanged.
    assert!(summaries[0].schema_changes().is_none());
    for summary in &summaries {
        assert_eq!(summary.schema().unwrap().as_ref(), &schema);
        assert!(summary.partition_columns().unwrap().is_empty());
    }
    for summary in &summaries[1..] {
        assert!(summary.schema_changes().unwrap().is_empty());
    }
}

#[rstest]
#[tokio::test]
async fn validation_does_not_depend_on_row_order_or_batching(
    #[values(CommitOrdering::AscendingOrder, CommitOrdering::DescendingOrder)]
    ordering: CommitOrdering,
) {
    let first = [
        VALID_PROTOCOL_LINE,
        VALID_METADATA_LINE,
        &add_line("a", true),
    ]
    .join("\n");
    let second = remove_line("a", true);
    let third = add_line("b", true);
    let (engine, table_root) = engine_with_commits(&[(0, &first), (1, &second), (2, &third)]).await;
    let range = CommitRange::builder_for(table_root, 0)
        .with_end_version(2)
        .with_ordering(ordering)
        .build(engine.as_ref())
        .unwrap();
    let summary_plan = range.get_commit_summary_plan(None).unwrap();

    // One single-row batch per commit, in reverse order.
    let mut rows = Vec::new();
    for batch in execute_summary_plan(&summary_plan, engine.as_ref()) {
        let batch = batch.unwrap().try_into_record_batch().unwrap();
        for row in 0..batch.num_rows() {
            let row: Box<dyn EngineData> = Box::new(ArrowEngineData::new(batch.slice(row, 1)));
            rows.push(Ok(row));
        }
    }
    rows.reverse();
    let summaries = summary_plan.validate(rows).unwrap();

    let mut expected = vec![
        (0, [Some(1), Some(0)]),
        (1, [Some(0), Some(1)]),
        (2, [Some(1), Some(0)]),
    ];
    if ordering == CommitOrdering::DescendingOrder {
        expected.reverse();
    }
    let actual = Vec::from_iter(
        summaries
            .iter()
            .map(|s| (s.version(), [s.num_added_files(), s.num_removed_files()])),
    );
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn operation_without_metrics_reports_no_metrics() {
    let create = [
        VALID_PROTOCOL_LINE,
        VALID_METADATA_LINE,
        r#"{"commitInfo":{"timestamp":0,"operation":"CREATE TABLE"}}"#,
    ];
    let commits = [(0, &*create.join("\n"))];

    let summaries = summarize_commits(&commits, 0, 0).await.unwrap();

    assert_eq!(operation(&summaries[0]), (Some("CREATE TABLE"), None));
    assert_eq!(facts(&summaries[0]), (Some(false), [Some(0), Some(0)]));
}

#[tokio::test]
async fn metrics_recorded_as_numbers_or_booleans_are_reported_as_their_json_text() {
    let write = r#"{"commitInfo":{"timestamp":0,"operation":"WRITE","operationMetrics":{"numFiles":1,"overwrite":true,"note":"x"}}}"#;
    let create = [VALID_PROTOCOL_LINE, VALID_METADATA_LINE, write];
    let commits = [(0, &*create.join("\n"))];

    let summaries = summarize_commits(&commits, 0, 0).await.unwrap();

    let metrics = vec![("note", "x"), ("numFiles", "1"), ("overwrite", "true")];
    assert_eq!(operation(&summaries[0]), (Some("WRITE"), Some(metrics)));
}

#[tokio::test]
async fn schema_is_unknown_until_a_commit_sets_the_metadata() {
    let before = schema! { nullable "id": LONG };
    let after = schema! { nullable "id": LONG, nullable "part": STRING };
    let sets_metadata = [VALID_PROTOCOL_LINE, &metadata_line(&before, &[], &[])].join("\n");
    let changes_metadata = metadata_line(&after, &["part"], &[]);
    let commits = [
        (0, COMMIT_INFO_NO_ICT),
        (1, &*sets_metadata),
        (2, &*add_line("a", true)),
        (3, &*changes_metadata),
    ];

    let summaries = summarize_commits(&commits, 0, 3).await.unwrap();

    assert!(summaries[0].schema().is_none());
    assert!(summaries[0].partition_columns().is_none());
    assert!(summaries[0].schema_changes().unwrap().is_empty());

    assert_eq!(summaries[1].schema().unwrap().as_ref(), &before);
    assert!(summaries[1].schema_changes().is_none());

    assert_eq!(summaries[2].schema().unwrap().as_ref(), &before);
    assert!(summaries[2].schema_changes().unwrap().is_empty());

    assert_eq!(summaries[3].schema().unwrap().as_ref(), &after);
    assert_eq!(summaries[3].partition_columns().unwrap(), ["part"]);
    let added = SchemaChange::AddColumn {
        parent: None,
        field: StructField::nullable("part", DataType::STRING),
    };
    assert_eq!(summaries[3].schema_changes().unwrap(), [added]);
}

#[tokio::test]
async fn start_snapshot_provides_the_schema_of_commits_that_do_not_set_it() {
    let schema = schema! { nullable "id": LONG };
    let create = [VALID_PROTOCOL_LINE, &metadata_line(&schema, &[], &[])].join("\n");
    let commits = [
        (0, &*create),
        (1, &*add_line("a", true)),
        (2, &*add_line("b", true)),
    ];
    let (engine, table_root) = engine_with_commits(&commits).await;
    let range = range_over(engine.as_ref(), table_root, 1, 2);
    let snapshot_at = |version| snapshot_at(engine.as_ref(), table_root, version);

    let without_snapshot = summarize(&range, engine.as_ref(), None).unwrap();
    assert!(without_snapshot.iter().all(|s| s.schema().is_none()));

    let with_snapshot = summarize(&range, engine.as_ref(), Some(snapshot_at(1))).unwrap();
    for summary in &with_snapshot {
        assert_eq!(summary.schema().unwrap().as_ref(), &schema);
        assert!(summary.schema_changes().unwrap().is_empty());
    }

    assert_result_error_with_message(
        range.get_commit_summary_plan(Some(snapshot_at(2))),
        "snapshot version 2 does not match start_version (1)",
    );
}

#[tokio::test]
async fn column_mapping_reports_a_column_dropped_and_added_back_under_the_same_name() {
    let before = StructType::try_new([mapped_int("id", 1), mapped_int("value", 2)]).unwrap();
    let after = StructType::try_new([mapped_int("id", 1), mapped_int("value", 3)]).unwrap();
    let renamed = StructType::try_new([mapped_int("key", 1), mapped_int("value", 3)]).unwrap();
    let create = [
        COLUMN_MAPPING_PROTOCOL_LINE,
        &column_mapping_metadata_line(&before),
    ]
    .join("\n");
    let commits = [
        (0, &*create),
        (1, &*column_mapping_metadata_line(&after)),
        (2, &*column_mapping_metadata_line(&renamed)),
    ];

    let summaries = summarize_commits(&commits, 0, 2).await.unwrap();

    let names = |summary: &CommitSummary| {
        Vec::from_iter(summary.schema().unwrap().fields().map(|f| f.name().clone()))
    };
    assert_eq!(names(&summaries[0]), names(&summaries[1]));
    let dropped_and_added = [
        SchemaChange::DropColumn {
            column: ColumnName::new(["value"]),
        },
        SchemaChange::AddColumn {
            parent: None,
            field: mapped_int("value", 3),
        },
    ];
    assert_eq!(summaries[1].schema_changes().unwrap(), dropped_and_added);

    let renamed = SchemaChange::RenameColumn {
        column: ColumnName::new(["id"]),
        new_name: "key".to_string(),
    };
    assert_eq!(summaries[2].schema_changes().unwrap(), [renamed]);
}

#[tokio::test]
async fn in_commit_timestamps_apply_once_the_table_enables_them() {
    let create = [VALID_PROTOCOL_LINE, VALID_METADATA_LINE].join("\n");
    let enable = [
        commit_info_with_ict(5000),
        ICT_PROTOCOL_LINE.to_string(),
        ICT_METADATA_ENABLED.to_string(),
    ]
    .join("\n");
    let commits = [
        (0, &*create),
        (1, &*enable),
        (2, &*commit_info_with_ict(6000)),
    ];
    let (engine, table_root) = engine_with_commits(&commits).await;
    let range = range_over(engine.as_ref(), table_root, 0, 2);

    let summaries = summarize(&range, engine.as_ref(), None).unwrap();

    let declarative = Vec::from_iter(summaries.iter().map(|s| (s.version(), s.timestamp())));
    assert_eq!(declarative, imperative_commits(&range, &engine));
    let timestamps = Vec::from_iter(summaries.iter().map(CommitSummary::timestamp));
    assert_eq!(timestamps[1..], [5000, 6000]);
}

#[tokio::test]
async fn start_snapshot_does_not_reveal_what_the_first_commit_changed() {
    let before = schema! { nullable "id": LONG };
    let after = schema! { nullable "id": LONG, nullable "value": STRING };
    let create = [VALID_PROTOCOL_LINE, &metadata_line(&before, &[], &[])].join("\n");
    let alter = metadata_line(&after, &[], &[]);
    let commits = [(0, &*create), (1, &*alter), (2, &*add_line("a", true))];
    let (engine, table_root) = engine_with_commits(&commits).await;
    let range = range_over(engine.as_ref(), table_root, 1, 2);
    let start_snapshot = snapshot_at(engine.as_ref(), table_root, 1);

    let summaries = summarize(&range, engine.as_ref(), Some(start_snapshot)).unwrap();

    assert_eq!(summaries[0].schema().unwrap().as_ref(), &after);
    assert!(summaries[0].schema_changes().is_none());
    assert!(summaries[1].schema_changes().unwrap().is_empty());
}

#[tokio::test]
async fn protocol_change_without_metadata_keeps_the_schema() {
    let schema = schema! { nullable "id": LONG };
    let create = [VALID_PROTOCOL_LINE, &metadata_line(&schema, &[], &[])].join("\n");
    let upgrade = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    let commits = [(0, &*create), (1, upgrade), (2, &*add_line("a", true))];

    let summaries = summarize_commits(&commits, 0, 2).await.unwrap();

    for summary in &summaries {
        assert_eq!(summary.schema().unwrap().as_ref(), &schema);
    }
    assert!(summaries[1].schema_changes().unwrap().is_empty());
}

fn mode_change(old_mode: ColumnMappingMode, new_mode: ColumnMappingMode) -> SchemaChange {
    SchemaChange::ChangeColumnMappingMode { old_mode, new_mode }
}

#[tokio::test]
async fn column_mapping_mode_change_is_reported_as_a_schema_change() {
    let plain = schema! { nullable "id": INTEGER };
    let mapped = StructType::try_new([mapped_int("id", 1)]).unwrap();
    // The table turns column mapping on in version 1 and off again in version 2. Its columns
    // keep their names and types throughout.
    let create = [
        COLUMN_MAPPING_PROTOCOL_LINE,
        &metadata_line(&plain, &[], &[]),
    ]
    .join("\n");
    let commits = [
        (0, &*create),
        (1, &*column_mapping_metadata_line(&mapped)),
        (2, &*metadata_line(&plain, &[], &[])),
    ];

    let summaries = summarize_commits(&commits, 0, 2).await.unwrap();

    assert_eq!(
        summaries[1].schema_changes().unwrap(),
        [mode_change(
            ColumnMappingMode::None,
            ColumnMappingMode::Name
        )]
    );
    assert_eq!(
        summaries[2].schema_changes().unwrap(),
        [mode_change(
            ColumnMappingMode::Name,
            ColumnMappingMode::None
        )]
    );
}

#[tokio::test]
async fn protocol_change_can_put_the_column_mapping_mode_into_effect() {
    let schema = StructType::try_new([mapped_int("id", 1)]).unwrap();
    // Reader version 1 does not support column mapping, so the property takes effect only once
    // version 1 raises the protocol, without setting the metadata again.
    let create = [VALID_PROTOCOL_LINE, &column_mapping_metadata_line(&schema)].join("\n");
    let commits = [(0, &*create), (1, COLUMN_MAPPING_PROTOCOL_LINE)];

    let summaries = summarize_commits(&commits, 0, 1).await.unwrap();

    assert_eq!(
        summaries[1].schema_changes().unwrap(),
        [mode_change(
            ColumnMappingMode::None,
            ColumnMappingMode::Name
        )]
    );
}

#[tokio::test]
async fn changes_are_unknown_for_the_commit_that_makes_the_column_mapping_mode_known() {
    let schema = StructType::try_new([mapped_int("id", 1)]).unwrap();
    // Without a start snapshot, version 0 shows the metadata but not the protocol, so whether
    // column mapping is in effect is unknown until version 1 sets the protocol.
    let commits = [
        (0, &*column_mapping_metadata_line(&schema)),
        (1, COLUMN_MAPPING_PROTOCOL_LINE),
        (2, &*add_line("a", true)),
    ];

    let summaries = summarize_commits(&commits, 0, 2).await.unwrap();

    assert!(summaries[1].schema_changes().is_none());
    assert!(summaries[2].schema_changes().unwrap().is_empty());
}

#[tokio::test]
async fn changes_are_unknown_while_the_column_mapping_mode_is_unknown() {
    let before = StructType::try_new([mapped_int("id", 1)]).unwrap();
    let after = StructType::try_new([mapped_int("id", 2)]).unwrap();
    // Without a start snapshot, the range shows the metadata but never the protocol. Whether
    // version 1 kept the column `id` or replaced it under the same name depends on whether column
    // mapping is in effect.
    let commits = [
        (0, &*column_mapping_metadata_line(&before)),
        (1, &*column_mapping_metadata_line(&after)),
    ];

    let summaries = summarize_commits(&commits, 0, 1).await.unwrap();

    assert!(summaries[1].schema().is_some());
    assert!(summaries[1].schema_changes().is_none());
}

#[tokio::test]
async fn changes_are_unknown_for_a_first_commit_that_only_sets_the_protocol() {
    let schema = StructType::try_new([mapped_int("id", 1)]).unwrap();
    // Version 1 raises the protocol, which puts column mapping into effect. As the first commit
    // of the range, it has no known state before it, not even with a start snapshot.
    let create = [VALID_PROTOCOL_LINE, &column_mapping_metadata_line(&schema)].join("\n");
    let commits = [(0, &*create), (1, COLUMN_MAPPING_PROTOCOL_LINE)];
    let (engine, table_root) = engine_with_commits(&commits).await;
    let range = range_over(engine.as_ref(), table_root, 1, 1);
    let start_snapshot = snapshot_at(engine.as_ref(), table_root, 1);

    let summaries = summarize(&range, engine.as_ref(), Some(start_snapshot)).unwrap();

    assert!(summaries[0].schema_changes().is_none());
}

#[tokio::test]
async fn column_mapping_property_without_protocol_support_matches_fields_by_name() {
    let before = schema! { nullable "id": LONG };
    let after = schema! { nullable "id": LONG, nullable "value": STRING };
    // Reader version 1 does not support column mapping, so the property has no effect and the
    // schema carries no column mapping IDs.
    let create = [VALID_PROTOCOL_LINE, &column_mapping_metadata_line(&before)].join("\n");
    let commits = [(0, &*create), (1, &*column_mapping_metadata_line(&after))];

    let summaries = summarize_commits(&commits, 0, 1).await.unwrap();

    let added = SchemaChange::AddColumn {
        parent: None,
        field: StructField::nullable("value", DataType::STRING),
    };
    assert_eq!(summaries[1].schema_changes().unwrap(), [added]);
}

#[rstest]
#[case::unsupported_protocol(
    &[UNSUPPORTED_PROTOCOL_LINE],
    "commit v=1: Feature 'futureFeature' is not supported",
)]
#[case::missing_in_commit_timestamp(
    &[ICT_PROTOCOL_LINE, ICT_METADATA_ENABLED, COMMIT_INFO_NO_ICT],
    "in-commit timestamp is enabled but missing ICT timestamp field in commit",
)]
#[case::two_metadata_actions(
    &[VALID_METADATA_LINE, VALID_METADATA_LINE],
    "commit v=1: found 2 metaData actions, but a commit may hold at most one",
)]
#[case::two_commit_info_actions(
    &[COMMIT_INFO_NO_ICT, COMMIT_INFO_NO_ICT],
    "commit v=1: found 2 commitInfo actions, but a commit may hold at most one",
)]
#[case::two_protocol_actions(
    &[VALID_PROTOCOL_LINE, VALID_PROTOCOL_LINE],
    "commit v=1: found 2 protocol actions, but a commit may hold at most one",
)]
#[case::malformed_protocol(
    &[r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#],
    "commit v=1: Reader features must be present when minimum reader version = 3",
)]
#[tokio::test]
async fn validation_rejects_commits_that_kernel_cannot_read(
    #[case] second_commit: &[&str],
    #[case] expected_error: &str,
) {
    let create = [VALID_PROTOCOL_LINE, VALID_METADATA_LINE].join("\n");
    let commits = [(0, &*create), (1, &*second_commit.join("\n"))];

    let result = summarize_commits(&commits, 0, 1).await;

    assert_result_error_with_message(result, expected_error);
}

#[tokio::test]
async fn validation_rejects_rows_that_do_not_match_the_range() {
    let create = [VALID_PROTOCOL_LINE, VALID_METADATA_LINE].join("\n");
    let commits = [(0, &*create), (1, &*add_line("a", true))];
    let (engine, table_root) = engine_with_commits(&commits).await;
    let summary_plan_of = |start_version, end_version| {
        range_over(engine.as_ref(), table_root, start_version, end_version)
            .get_commit_summary_plan(None)
            .unwrap()
    };
    let both = summary_plan_of(0, 1);
    let rows = || execute_summary_plan(&both, engine.as_ref());

    assert_result_error_with_message(
        summary_plan_of(0, 0).validate(rows()),
        "row for version 1, which is not a commit of the range",
    );
    assert_result_error_with_message(
        both.validate(rows().into_iter().chain(rows())),
        "more than one row for version",
    );
    assert_result_error_with_message(both.validate([]), "no row for version 0");
}

#[tokio::test]
async fn validation_rejects_a_commit_file_without_actions() {
    let create = [VALID_PROTOCOL_LINE, VALID_METADATA_LINE].join("\n");
    let commits = [(0, &*create), (1, "")];

    let result = summarize_commits(&commits, 0, 1).await;

    assert_result_error_with_message(result, "no row for version 1");
}

/// The row of a commit that sets neither the protocol nor the metadata, adds
/// `num_data_change_files` files that change data, and records `data_change` in its commit info.
fn file_row(data_change: Option<bool>, num_data_change_files: i64) -> SummaryRow {
    SummaryRow {
        data_change,
        num_commit_info_actions: 1,
        num_added_files: num_data_change_files,
        num_data_change_files,
        ..Default::default()
    }
}

/// Whether the commit of `row` changes data on a table with `table_config`, if it is known.
fn has_data_change(table_config: Option<&TableConfiguration>, row: SummaryRow) -> Option<bool> {
    let table_root = Url::parse("memory:///").unwrap();
    TableState::new(table_config)
        .unwrap()
        .advance(&table_root, 1, 0, row)
        .unwrap()
        .has_data_change()
}

#[cfg(feature = "adaptive-metadata-in-dev")]
#[test]
fn adaptive_metadata_commit_records_whether_it_changes_data() {
    use crate::unit_test_utils::MockProtocolBuilder;

    let protocol = MockProtocolBuilder::new()
        .with_features([TableFeature::AdaptiveMetadataPreview])
        .build();
    let adaptive = MockTableConfigurationBuilder::new()
        .with_protocol(protocol)
        .build();
    assert!(adaptive.is_feature_enabled(&TableFeature::AdaptiveMetadataPreview));
    let adaptive = Some(&adaptive);

    // What the commit records wins over its files. If it records nothing, the answer is unknown.
    assert_eq!(
        has_data_change(adaptive, file_row(Some(false), 2)),
        Some(false)
    );
    assert_eq!(
        has_data_change(adaptive, file_row(Some(true), 0)),
        Some(true)
    );
    assert_eq!(has_data_change(adaptive, file_row(None, 2)), None);
}

#[cfg(feature = "adaptive-metadata-in-dev")]
#[test]
fn protocol_alone_tells_that_a_table_has_adaptive_metadata() {
    use crate::unit_test_utils::MockProtocolBuilder;

    let protocol = MockProtocolBuilder::new()
        .with_features([TableFeature::AdaptiveMetadataPreview])
        .build();
    // Without a start snapshot, the range shows the protocol but not the metadata.
    let sets_protocol = SummaryRow {
        protocol: Some(protocol),
        num_protocol_actions: 1,
        ..file_row(None, 2)
    };

    assert_eq!(has_data_change(None, sets_protocol), None);
}

#[test]
fn commit_without_adaptive_metadata_changes_data_if_its_files_do() {
    let plain = MockTableConfigurationBuilder::new().build();
    let plain = Some(&plain);

    assert_eq!(has_data_change(plain, file_row(Some(false), 2)), Some(true));
    assert_eq!(has_data_change(plain, file_row(Some(true), 0)), Some(false));
    assert_eq!(has_data_change(plain, file_row(None, 2)), Some(true));
}

#[tokio::test]
async fn commit_that_records_its_data_change_is_taken_at_its_word_while_features_are_unknown() {
    // The range shows neither the protocol nor the metadata, so the table's features are unknown.
    let rearranges = [
        r#"{"commitInfo":{"timestamp":0,"operation":"OPTIMIZE","dataChange":false}}"#,
        &add_line("a", true),
    ]
    .join("\n");
    let appends = [COMMIT_INFO_NO_ICT, &add_line("b", true)].join("\n");
    let commits = [(0, &*rearranges), (1, &*appends)];

    let summaries = summarize_commits(&commits, 0, 1).await.unwrap();

    assert_eq!(facts(&summaries[0]), (Some(false), [Some(1), Some(0)]));
    assert_eq!(facts(&summaries[1]), (Some(true), [Some(1), Some(0)]));
}

#[tokio::test]
async fn file_facts_are_unknown_for_a_commit_that_records_its_files_in_the_metadata_tree() {
    // Versions 1 and 3 are manifest commits whose content root is at their own version, so
    // their file changes are in the metadata tree. Version 2 carries the pointer to version 1
    // forward, and its commit file holds its file changes.
    let in_tree = r#"{"commitInfo":{"timestamp":0,"dataChange":true,"lastManifestCommit":{"version":1,"contentRootVersion":1}}}"#;
    let in_commit_file = [
        r#"{"commitInfo":{"timestamp":0,"lastManifestCommit":{"version":1,"contentRootVersion":1}}}"#,
        &add_line("a", true),
    ]
    .join("\n");
    let in_tree_without_data_change = r#"{"commitInfo":{"timestamp":0,"lastManifestCommit":{"version":3,"contentRootVersion":3}}}"#;
    let commits = [
        (0, COMMIT_INFO_NO_ICT),
        (1, in_tree),
        (2, &*in_commit_file),
        (3, in_tree_without_data_change),
    ];

    let summaries = summarize_commits(&commits, 0, 3).await.unwrap();

    assert_eq!(facts(&summaries[1]), (Some(true), [None, None]));
    assert_eq!(facts(&summaries[2]), (Some(true), [Some(1), Some(0)]));
    assert_eq!(facts(&summaries[3]), (None, [None, None]));
}

#[test]
fn version_context_keeps_schema_errors_as_schema_errors() {
    let error = with_version_context(3, KernelError::schema("no column mapping ID"));

    assert!(matches!(
        error,
        KernelError::Schema(message) if message == "commit v=3: no column mapping ID"
    ));
}

#[test]
fn version_context_keeps_the_kind_and_the_backtrace_of_a_backtraced_schema_error() {
    let error = KernelError::Backtraced {
        source: Box::new(KernelError::schema("no column mapping ID")),
        backtrace: Box::new(std::backtrace::Backtrace::force_capture()),
    };

    let error = with_version_context(3, error);

    assert!(matches!(error, KernelError::Backtraced { .. }));
    assert!(matches!(
        error.without_backtrace(),
        KernelError::Schema(message) if message == "commit v=3: no column mapping ID"
    ));
}
