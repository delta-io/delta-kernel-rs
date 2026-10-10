//! AMT (adaptiveMetadata) tests for Protocol & Metadata replay: manifest commits whose P&M is
//! carried by a `checkpoint` action, exercised on both the plan and non-plan replay paths.

use std::sync::Arc;

use rstest::rstest;
use test_utils::add_commit;

use super::{CheckpointActionResolution, LogSegment};
use crate::engine::sync::SyncEngine;
#[cfg(feature = "declarative-plans")]
use crate::engine::test_delegating::DelegatingEngine;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::object_store::ObjectStoreExt as _;
use crate::schema::SchemaRef;
use crate::table_features::TableFeature;
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
};
use crate::{Engine, Snapshot};

fn one_column_schema() -> SchemaRef {
    test_schema_flat_with_column_mapping()
        .project(&["id"])
        .unwrap()
}

// Builds a commit line with a `checkpoint` action that carries protocol and metadata at
// `version`. The commit has no top-level protocol/metaData, so P&M comes only from that action.
fn checkpoint_commit(version: i64, extra_features: &[TableFeature], schema: SchemaRef) -> String {
    serde_json::json!({ "checkpoint": checkpoint_entries(version, extra_features, schema) })
        .to_string()
}

// The tagged entries of a `checkpoint` action carrying protocol and metadata at `version`.
fn checkpoint_entries(
    version: i64,
    extra_features: &[TableFeature],
    schema: SchemaRef,
) -> serde_json::Value {
    let config = adaptive_metadata_table_configuration(schema, extra_features);
    serde_json::json!([
        { "checkpointMetadata": { "version": version } },
        { "contentRoot": { "path": "metadata/root.parquet", "sizeInBytes": 1, "version": version } },
        { "protocol": config.protocol() },
        { "metaData": config.metadata() },
    ])
}

// Builds a commit line with only a `commitInfo` action.
fn commit_info_commit() -> String {
    serde_json::json!({ "commitInfo": { "timestamp": 0, "inCommitTimestamp": 0 } }).to_string()
}

// Builds a top-level `metaData` commit line with the given schema (no protocol).
fn metadata_commit(schema: SchemaRef) -> String {
    let config = adaptive_metadata_table_configuration(schema, &[]);
    serde_json::json!({ "metaData": config.metadata() }).to_string()
}

// Builds a commit line with top-level `protocol` and `metaData` from an AMT config, and no
// checkpoint action, so P&M come from standalone actions.
fn standalone_pm_commit(schema: SchemaRef) -> String {
    let config = adaptive_metadata_table_configuration(schema, &[]);
    format!(
        "{}\n{}",
        serde_json::json!({ "protocol": config.protocol() }),
        serde_json::json!({ "metaData": config.metadata() }),
    )
}

// Builds a top-level `protocol` commit line with the given reader/writer versions (no features).
fn protocol_commit(min_reader_version: i64, min_writer_version: i64) -> String {
    serde_json::json!({ "protocol": {
        "minReaderVersion": min_reader_version,
        "minWriterVersion": min_writer_version,
    } })
    .to_string()
}

// Removes SyncEngine's plan executor so replay uses the non-plan path even when
// `declarative-plans` is compiled in. Otherwise SyncEngine would use the plan path.
fn non_plan_engine(store: Arc<InMemory>) -> impl Engine {
    let engine = SyncEngine::new_with_store(store);
    #[cfg(feature = "declarative-plans")]
    let engine = DelegatingEngine::new(Arc::new(engine)).without_plan_executor();
    engine
}

#[tokio::test]
async fn test_load_resolves_pm_from_manifest_commit_checkpoint_action() {
    check_manifest_commit_checkpoint(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    check_manifest_commit_checkpoint(|store| SyncEngine::new_with_store(store)).await;
}

// A single commit whose only P&M is a checkpoint action. The build succeeds only if P&M came from
// that action, and the `id` column confirms it used the embedded metaData.
async fn check_manifest_commit_checkpoint<E: Engine>(make_engine: impl FnOnce(Arc<InMemory>) -> E) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        checkpoint_commit(0, &[], one_column_schema()),
    )
    .await
    .unwrap();

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(snapshot.version(), 0);
    assert!(snapshot.schema().field("id").is_some());
}

// The newest protocol and metadata win, ranked by version. Every case runs on both plan/non-plan
// replay paths. Each case gives its own expected metaData column count and protocol reader-feature
// count so the assertion states what that case is checking.
#[rstest]
// A newer checkpoint action beats the older top-level protocol and metaData.
#[case::newer_checkpoint_beats_older_pm(
    format!("{}\n{}", protocol_commit(1, 2), metadata_commit(one_column_schema())),
    checkpoint_commit(1, &[], test_schema_flat_with_column_mapping()),
    2,
    3
)]
// A newer checkpoint action's metaData beats the older top-level metaData.
#[case::newer_checkpoint_beats_older_metadata(
    metadata_commit(one_column_schema()),
    checkpoint_commit(1, &[], test_schema_flat_with_column_mapping()),
    2,
    3
)]
// A newer top-level metaData beats the older checkpoint action's metaData.
#[case::newer_metadata_beats_older_checkpoint(
    checkpoint_commit(0, &[], one_column_schema()),
    metadata_commit(test_schema_flat_with_column_mapping()),
    2,
    3
)]
// Feature count distinguishes protocols with the same reader version.
#[case::newer_checkpoint_protocol_wins(
    checkpoint_commit(0, &[], one_column_schema()),
    checkpoint_commit(1, &[TableFeature::TimestampWithoutTimezone], one_column_schema()),
    1,
    4
)]
#[tokio::test]
async fn resolve_pm_newest_action_wins(
    #[case] v0: String,
    #[case] v1: String,
    #[case] expected_fields: usize,
    #[case] expected_reader_features: usize,
) {
    assert_newest_pm_wins(v0, v1, expected_fields, expected_reader_features).await;
}

// Runs the check on the non-plan path, then on the plan path when it's compiled in, so every case
// is verified against both.
async fn assert_newest_pm_wins(
    v0: String,
    v1: String,
    expected_fields: usize,
    expected_reader_features: usize,
) {
    build_and_check_pm(
        &v0,
        &v1,
        expected_fields,
        expected_reader_features,
        non_plan_engine,
    )
    .await;
    #[cfg(feature = "declarative-plans")]
    build_and_check_pm(
        &v0,
        &v1,
        expected_fields,
        expected_reader_features,
        |store| SyncEngine::new_with_store(store),
    )
    .await;
}

// Commits v0 then v1 with `make_engine`, then checks the resolved metaData column count and the
// resolved protocol's reader-feature count.
async fn build_and_check_pm<E: Engine>(
    v0: &str,
    v1: &str,
    expected_fields: usize,
    expected_reader_features: usize,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(table_root.as_str(), store.as_ref(), 0, v0.to_string())
        .await
        .unwrap();
    add_commit(table_root.as_str(), store.as_ref(), 1, v1.to_string())
        .await
        .unwrap();

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    assert_eq!(snapshot.version(), 1);
    assert_eq!(
        snapshot.schema().num_fields(),
        expected_fields,
        "resolved metaData should have {expected_fields} column(s)"
    );
    let protocol = snapshot.table_configuration().protocol();
    assert_eq!(
        protocol.min_reader_version(),
        3,
        "AMT protocol is reader v3"
    );
    assert_eq!(
        protocol.reader_features().map_or(0, |f| f.len()),
        expected_reader_features,
        "resolved protocol should have {expected_reader_features} reader feature(s)"
    );
}

#[tokio::test]
async fn test_lagging_checkpoint_ranks_by_checkpoint_version() {
    assert_lagging_checkpoint_loses_to_gap_commit(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_lagging_checkpoint_loses_to_gap_commit(|store| SyncEngine::new_with_store(store)).await;
}

async fn assert_lagging_checkpoint_loses_to_gap_commit<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        checkpoint_commit(0, &[], one_column_schema()),
    )
    .await
    .unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        1,
        metadata_commit(test_schema_flat_with_column_mapping()),
    )
    .await
    .unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        2,
        checkpoint_commit(0, &[], one_column_schema()),
    )
    .await
    .unwrap();

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    assert_eq!(snapshot.version(), 2);
    let schema = snapshot.schema();
    assert!(schema.field("name").is_some());
    assert_eq!(schema.num_fields(), 2);
}

// A fresh (no-CRC) load runs a full replay that captures the checkpoint action onto the snapshot:
// `Snapshot::latest_checkpoint_action` returns `Some` with the action's version when a commit
// carries one, and `None` when P&M come from standalone actions (a replay miss leaves the
// resolution unset, so the accessor scans and still resolves `None`).
#[rstest]
#[case::commit_carries_checkpoint_action(checkpoint_commit(0, &[], one_column_schema()), Some(0))]
#[case::standalone_pm_has_no_checkpoint_action(standalone_pm_commit(one_column_schema()), None)]
#[tokio::test]
async fn replay_captures_latest_checkpoint_action(
    #[case] commit: String,
    #[case] expected_version: Option<i64>,
) {
    assert_latest_checkpoint_action(&commit, expected_version, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_latest_checkpoint_action(&commit, expected_version, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_latest_checkpoint_action<E: Engine>(
    commit: &str,
    expected_version: Option<i64>,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(table_root.as_str(), store.as_ref(), 0, commit.to_string())
        .await
        .unwrap();

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    let action = snapshot.latest_checkpoint_action(&engine).unwrap();
    assert_eq!(action.map(|action| action.version), expected_version);
}

// A snapshot built without P&M replay (`Snapshot::new`) leaves its checkpoint-action resolution
// unset, so `latest_checkpoint_action` resolves lazily by scanning the log.
#[tokio::test]
async fn latest_checkpoint_action_scans_when_resolution_unknown() {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        checkpoint_commit(0, &[], one_column_schema()),
    )
    .await
    .unwrap();

    let engine = non_plan_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None, None)
            .unwrap();
    let table_configuration = adaptive_metadata_table_configuration(one_column_schema(), &[]);
    let snapshot = Snapshot::new(log_segment, table_configuration).unwrap();

    let action = snapshot
        .latest_checkpoint_action(&engine)
        .unwrap()
        .expect("lazy scan should find the checkpoint action");
    assert_eq!(action.version, 0);
    assert_eq!(action.content_root.path, "metadata/root.parquet");
}

// `read_protocol_metadata_opt` captures the latest AMT checkpoint action during full replay and
// reports it on the resolution. Asserts the resolution directly (not the accessor) because the
// accessor returns the same action whether replay captured it or a later scan found it -- only the
// resolution shows that replay itself captured it. Runs on both the plan and non-plan paths. Cases:
// - a single checkpoint commit is captured;
// - a checkpoint action in a non-first batch is still captured: replay stops only once both P&M are
//   final, and a newer top-level `metaData` commit does not finalize Protocol, so replay continues
//   into the older checkpoint commit;
// - replay that resolves P&M from standalone actions reports no checkpoint action (`None`).
#[rstest]
#[case::single_checkpoint_commit(vec![checkpoint_commit(0, &[], one_column_schema())], Some(0))]
#[case::checkpoint_action_in_non_first_batch(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        metadata_commit(test_schema_flat_with_column_mapping()),
    ],
    Some(0)
)]
#[case::standalone_pm_has_no_checkpoint_action(
    vec![standalone_pm_commit(one_column_schema())],
    None
)]
#[tokio::test]
async fn replay_resolves_latest_checkpoint_action(
    #[case] commits: Vec<String>,
    #[case] expected_version: Option<i64>,
) {
    assert_replay_resolution(&commits, expected_version, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_resolution(&commits, expected_version, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_replay_resolution<E: Engine>(
    commits: &[String],
    expected_version: Option<i64>,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    for (version, commit) in commits.iter().enumerate() {
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            version as u64,
            commit.clone(),
        )
        .await
        .unwrap();
    }

    let engine = make_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None, None)
            .unwrap();

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    // No CRC is passed, so replay never produces a `Hint`: only `Captured` or `Unresolved`.
    let version = match resolution.checkpoint_action {
        CheckpointActionResolution::Captured(action) => Some(action.version),
        CheckpointActionResolution::Hint(_) | CheckpointActionResolution::Unresolved => None,
    };
    assert_eq!(version, expected_version);
}

// An incremental update resolves the latest checkpoint action for the updated snapshot: a new
// commit carrying a newer checkpoint action surfaces that newer action, while a new commit carrying
// none leaves the update's resolution unset so the accessor scans and still returns the base's
// older action.
#[rstest]
#[case::new_commit_carries_a_newer_action(checkpoint_commit(1, &[], one_column_schema()), 1)]
#[case::new_commit_carries_no_action(metadata_commit(one_column_schema()), 0)]
#[tokio::test]
async fn incremental_update_resolves_latest_checkpoint_action(
    #[case] new_commit: String,
    #[case] expected_version: i64,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        checkpoint_commit(0, &[], one_column_schema()),
    )
    .await
    .unwrap();

    let engine = non_plan_engine(store.clone());
    let base = Snapshot::builder_for(table_root.clone())
        .at_version(0)
        .build(&engine)
        .unwrap();
    assert_eq!(
        base.latest_checkpoint_action(&engine)
            .unwrap()
            .map(|a| a.version),
        Some(0)
    );

    add_commit(table_root.as_str(), store.as_ref(), 1, new_commit)
        .await
        .unwrap();
    let updated = Snapshot::builder_from(base).build(&engine).unwrap();

    assert_eq!(updated.version(), 1);
    assert_eq!(
        updated
            .latest_checkpoint_action(&engine)
            .unwrap()
            .map(|a| a.version),
        Some(expected_version)
    );
}

// Time travel to version 3 on a log listed from an AMT `_last_checkpoint` hint at content root 2
// whose manifest commit 4 is after the snapshot. Commit 4 is not part of the snapshot, so its
// checkpoint action and P&M come from the action embedded in the hint (one-column schema). Each
// case varies commit 3 or adds a CRC at version 3; every case runs on both plan/non-plan replay
// paths.
#[rstest]
#[case::no_pm_in_range(commit_info_commit(), false, 1)]
// An older manifest commit's action (content root 1) in range loses to the embedded action.
#[case::older_checkpoint_action_in_range(
    checkpoint_commit(1, &[], test_schema_flat_with_column_mapping()),
    false,
    1
)]
#[case::newer_metadata_in_range(
    metadata_commit(test_schema_flat_with_column_mapping()),
    false,
    test_schema_flat_with_column_mapping().fields().count()
)]
#[case::crc_at_target(commit_info_commit(), true, 1)]
#[tokio::test]
async fn time_travel_before_manifest_commit_uses_hint_checkpoint_action(
    #[case] commit_3: String,
    #[case] crc_at_target: bool,
    #[case] expected_fields: usize,
) {
    check_hint_checkpoint_action(&commit_3, crc_at_target, expected_fields, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    check_hint_checkpoint_action(&commit_3, crc_at_target, expected_fields, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn check_hint_checkpoint_action<E: Engine>(
    commit_3: &str,
    crc_at_target: bool,
    expected_fields: usize,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    let commits = [
        (2, commit_info_commit()),
        (3, commit_3.to_string()),
        (4, checkpoint_commit(2, &[], one_column_schema())),
    ];
    for (version, commit) in commits {
        add_commit(table_root.as_str(), store.as_ref(), version, commit)
            .await
            .unwrap();
    }
    let hint = serde_json::json!({
        "version": 2,
        "size": -1,
        "checkpointType": "AdaptiveMetadataTree",
        "amtCheckpoint": {
            "manifestCommitVersion": 4,
            "checkpoint": checkpoint_entries(2, &[], one_column_schema()),
        },
    });
    store
        .put(
            &Path::from("_delta_log/_last_checkpoint"),
            hint.to_string().into(),
        )
        .await
        .unwrap();
    if crc_at_target {
        let config = adaptive_metadata_table_configuration(one_column_schema(), &[]);
        let crc = serde_json::json!({
            "tableSizeBytes": 0,
            "numFiles": 0,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": config.metadata(),
            "protocol": config.protocol(),
        });
        store
            .put(
                &Path::from("_delta_log/00000000000000000003.crc"),
                crc.to_string().into(),
            )
            .await
            .unwrap();
    }

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(3)
        .build(&engine)
        .unwrap();
    assert_eq!(snapshot.schema().fields().count(), expected_fields);
    let action = snapshot
        .latest_checkpoint_action(&engine)
        .unwrap()
        .expect("checkpoint action present");
    assert_eq!(action.version(), 2);
}
