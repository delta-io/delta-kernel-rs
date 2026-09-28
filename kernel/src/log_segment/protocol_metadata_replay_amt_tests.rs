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
    let config = adaptive_metadata_table_configuration(schema, extra_features);
    serde_json::json!({ "checkpoint": [
        { "checkpointMetadata": { "version": version } },
        { "contentRoot": { "path": "metadata/root.parquet", "sizeInBytes": 1, "version": version } },
        { "protocol": config.protocol() },
        { "metaData": config.metadata() },
    ] })
    .to_string()
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
// resolution `Unresolved`, so the accessor scans and still resolves `None`).
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
// `Unresolved`, so `latest_checkpoint_action` resolves lazily by scanning the log, including the
// root manifest path.
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

// A fresh (no-CRC) replay captures the checkpoint action into the resolution rather than deferring
// to a scan, on both the plan and non-plan paths. Asserts the resolution directly because the
// `latest_checkpoint_action` accessor would return the same action either way (via the scan).
#[tokio::test]
async fn replay_resolves_checkpoint_action_as_captured() {
    assert_replay_captures_checkpoint_action(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_captures_checkpoint_action(|store| SyncEngine::new_with_store(store)).await;
}

async fn assert_replay_captures_checkpoint_action<E: Engine>(
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

    let engine = make_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None, None)
            .unwrap();

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(
        matches!(
            resolution.checkpoint_action,
            CheckpointActionResolution::Captured(action) if action.version == 0
        ),
        "replay should capture the checkpoint action, not defer to a scan"
    );
}

// Replay stops once both Protocol and Metadata are final. A newest commit that carries only
// top-level metaData does not finalize Protocol, so replay continues into the older commit whose
// checkpoint action supplies it -- and that action, found in a non-first batch, is still captured.
#[tokio::test]
async fn replay_captures_checkpoint_action_from_older_batch_when_newest_batch_not_final() {
    assert_older_batch_checkpoint_action_captured(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_older_batch_checkpoint_action_captured(|store| SyncEngine::new_with_store(store)).await;
}

async fn assert_older_batch_checkpoint_action_captured<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    // v0 carries P&M via a checkpoint action; v1 is a top-level metaData only (no protocol).
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

    let engine = make_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None, None)
            .unwrap();

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(
        matches!(
            resolution.checkpoint_action,
            CheckpointActionResolution::Captured(action) if action.version == 0
        ),
        "the checkpoint action in the older batch must be captured even though the newest commit \
         finalized Metadata"
    );
}

// An incremental update that replays new commits captures the latest checkpoint action onto the
// updated snapshot, so the accessor returns the newer action without a log scan.
#[tokio::test]
async fn incremental_update_captures_latest_checkpoint_action() {
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

    // A new manifest commit carries a newer checkpoint action; the incremental replay must pick
    // it up as the snapshot's latest.
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        1,
        checkpoint_commit(1, &[], one_column_schema()),
    )
    .await
    .unwrap();
    let updated = Snapshot::builder_from(base).build(&engine).unwrap();

    assert_eq!(updated.version(), 1);
    assert_eq!(
        updated
            .latest_checkpoint_action(&engine)
            .unwrap()
            .map(|a| a.version),
        Some(1)
    );
}
