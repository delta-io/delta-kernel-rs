//! AMT (adaptiveMetadata) tests for Protocol & Metadata replay: manifest commits whose P&M is
//! carried by a `checkpoint` action, exercised on both the plan and non-plan replay paths.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rstest::rstest;
use test_utils::{
    add_commit, assert_result_error_with_message, compacted_log_path_for_versions,
    delta_path_for_version,
};
use url::Url;

use super::tests::add_checkpoint_to_store;
use super::{LastManifestCommitResolution, LogSegment, PmResolution};
use crate::actions::{
    CheckpointAction, CommitInfo, ContentRoot, LastManifestCommit, Protocol, SetTransaction,
};
use crate::arrow::array::StringArray;
use crate::committer::FileSystemCommitter;
use crate::crc::{Crc, FileStats, FileStatsState};
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
#[cfg(feature = "declarative-plans")]
use crate::expressions::Scalar;
use crate::object_store::memory::InMemory;
use crate::object_store::ObjectStoreExt as _;
use crate::path::ParsedLogPath;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::Operator;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanExecutor, PlanResult};
use crate::schema::SchemaRef;
use crate::table_features::TableFeature;
use crate::transaction::UpdateTableOperation;
use crate::unit_test_utils::adaptive_metadata_fixtures::last_manifest_commit_info;
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, parse_json_batch, test_schema_flat_with_column_mapping,
};
use crate::{
    Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize, FilteredEngineData,
    JsonHandler, KernelError, KernelResult, KernelResultIterator, PredicateRef, Snapshot,
};

fn one_column_schema() -> SchemaRef {
    test_schema_flat_with_column_mapping()
        .project(&["id"])
        .unwrap()
}

// Builds a manifest commit at `version`: a `commitInfo.lastManifestCommit` pointing at itself and a
// `checkpoint` action carrying protocol and metadata at `version`. The commit has no top-level
// protocol/metaData, so P&M comes only from that action.
fn checkpoint_commit(version: i64, extra_features: &[TableFeature], schema: SchemaRef) -> String {
    format!(
        "{}\n{}",
        commit_info(Some((version, version))),
        checkpoint_action(version, extra_features, schema)
    )
}

// Builds a `checkpoint` action line carrying protocol and metadata at `version`, with no
// `lastManifestCommit` pointer.
fn checkpoint_action(version: i64, extra_features: &[TableFeature], schema: SchemaRef) -> String {
    let config = adaptive_metadata_table_configuration(schema, extra_features);
    let checkpoint = CheckpointAction {
        version,
        content_root: ContentRoot::new("metadata/root.parquet".to_string(), 1, version),
        protocol: config.protocol().clone(),
        metadata: config.metadata().clone(),
        transactions: vec![],
        domain_metadata: vec![],
        txn_sidecars: vec![],
        domain_metadata_sidecars: vec![],
    };
    serde_json::json!({ "checkpoint": checkpoint }).to_string()
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

// Builds a commit with standalone P&M and no `lastManifestCommit`, so replay finds P&M without
// following a pointer.
fn pointerless_pm_commit() -> String {
    format!(
        "{}\n{}",
        commit_info(None),
        standalone_pm_commit(one_column_schema())
    )
}

// The resolution of a replay that read the pointer `(version, content_root_version)`.
fn resolved_pointer(version: i64, content_root_version: i64) -> LastManifestCommitResolution {
    LastManifestCommitResolution::Resolved(Some(
        LastManifestCommit::new(version, content_root_version).unwrap(),
    ))
}

// Builds a commit holding only a `txn` action: no commitInfo, Protocol, or Metadata.
fn txn_commit() -> String {
    serde_json::json!({ "txn": SetTransaction::new("app".to_string(), 1, None) }).to_string()
}

// Builds a top-level `protocol` commit line with the given reader/writer versions (no features).
fn protocol_commit(min_reader_version: i32, min_writer_version: i32) -> String {
    let protocol = Protocol::try_new_legacy(min_reader_version, min_writer_version).unwrap();
    serde_json::json!({ "protocol": protocol }).to_string()
}

// Prepends to `commit` a `commitInfo` carrying `last_manifest_commit` forward, as a writer does on
// every commit after a manifest commit.
fn carrying_pointer(last_manifest_commit: (i64, i64), commit: String) -> String {
    format!("{}\n{commit}", commit_info(Some(last_manifest_commit)))
}

// Builds a `commitInfo` line, carrying `lastManifestCommit` when given.
fn commit_info(last_manifest_commit: Option<(i64, i64)>) -> String {
    match last_manifest_commit {
        Some((version, content_root_version)) => {
            last_manifest_commit_info(version, content_root_version)
        }
        None => serde_json::json!({ "commitInfo": CommitInfo {
            operation: Some("WRITE".to_string()),
            in_commit_timestamp: Some(1),
            ..Default::default()
        } }),
    }
    .to_string()
}

// Commits a root manifest commit at v0 (pointing at itself) followed by `later_commits`, and
// returns the store and table root.
async fn manifest_commit_table(later_commits: &[String]) -> (Arc<InMemory>, url::Url) {
    let manifest_commit = checkpoint_commit(0, &[], one_column_schema());
    let commits: Vec<String> = std::iter::once(manifest_commit)
        .chain(later_commits.iter().cloned())
        .collect();
    commit_table(&commits).await
}

// Commits `commits` as versions 0, 1, ... of an in-memory table, and returns the store and table
// root.
async fn commit_table(commits: &[String]) -> (Arc<InMemory>, url::Url) {
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
    (store, table_root)
}

// Lists the log segment for the latest version of the table at `table_root`.
fn latest_log_segment(table_root: &Url, engine: &impl Engine) -> LogSegment {
    let log_root = table_root.join("_delta_log/").unwrap();
    let storage = engine.storage_handler();
    LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None).unwrap()
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
    let (store, table_root) = manifest_commit_table(&[]).await;

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
    carrying_pointer((0, 0), metadata_commit(test_schema_flat_with_column_mapping())),
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
    let (store, table_root) = commit_table(&[v0.to_string(), v1.to_string()]).await;

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
    let (store, table_root) = manifest_commit_table(&[
        carrying_pointer(
            (0, 0),
            metadata_commit(test_schema_flat_with_column_mapping()),
        ),
        // A manifest commit at v2 whose checkpoint action lags at v0.
        carrying_pointer((2, 0), checkpoint_action(0, &[], one_column_schema())),
    ])
    .await;

    let engine = make_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    assert_eq!(snapshot.version(), 2);
    let schema = snapshot.schema();
    assert!(schema.field("name").is_some());
    assert_eq!(schema.num_fields(), 2);
}

// A fresh (no-CRC) load runs a full replay that captures the checkpoint action onto the snapshot.
// The commit is deleted before `Snapshot::latest_checkpoint_action` runs, so following the
// commit's `lastManifestCommit` would fail and only the captured action can answer.
#[tokio::test]
async fn replay_captures_latest_checkpoint_action() {
    assert_replay_captures_checkpoint_action(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_captures_checkpoint_action(|store| SyncEngine::new_with_store(store)).await;
}

async fn assert_replay_captures_checkpoint_action<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = commit_table(&[checkpoint_commit(0, &[], one_column_schema())]).await;

    let engine = make_engine(store.clone());
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    store
        .delete(&delta_path_for_version(0, "json"))
        .await
        .unwrap();

    let action = snapshot.latest_checkpoint_action(&engine).unwrap();
    assert_eq!(action.map(|action| action.version), Some(0));
}

// A snapshot built without P&M replay (`Snapshot::new`) captures no checkpoint action, so
// `latest_checkpoint_action` follows the commit's `lastManifestCommit` to it.
#[tokio::test]
async fn latest_checkpoint_action_without_replay_follows_commit_info_pointer() {
    let (store, table_root) = commit_table(&[checkpoint_commit(0, &[], one_column_schema())]).await;

    let engine = non_plan_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);
    let table_configuration = adaptive_metadata_table_configuration(one_column_schema(), &[]);
    let snapshot = Snapshot::new(log_segment, table_configuration).unwrap();

    let action = snapshot
        .latest_checkpoint_action(&engine)
        .unwrap()
        .expect("the pointer should locate the checkpoint action");
    assert_eq!(action.version, 0);
    assert_eq!(action.content_root.path, "metadata/root.parquet");
}

// `read_protocol_metadata_opt` captures the latest AMT checkpoint action during full replay and
// reports it on the resolution. Asserts the resolution directly (not the accessor) because the
// accessor returns the same action whether replay captured it or a later pointer lookup found it --
// only the resolution shows that replay itself captured it. Runs on both the plan and non-plan
// paths. Cases:
// - a single checkpoint commit is captured;
// - a checkpoint action in a non-first batch is still captured: replay stops only once both P&M are
//   final, and a newer top-level `metaData` commit does not finalize Protocol, so replay continues
//   into the older checkpoint commit;
// - replay that resolves P&M from standalone actions reports no checkpoint action (`None`);
// - a newest commit pointing at an older manifest commit captures that commit's action.
#[rstest]
#[case::single_checkpoint_commit(vec![checkpoint_commit(0, &[], one_column_schema())], Some(0))]
#[case::checkpoint_action_in_non_first_batch(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((0, 0), metadata_commit(test_schema_flat_with_column_mapping())),
    ],
    Some(0)
)]
#[case::standalone_pm_has_no_checkpoint_action(
    vec![standalone_pm_commit(one_column_schema())],
    None
)]
#[case::pointer_to_older_manifest_commit(
    vec![checkpoint_commit(0, &[], one_column_schema()), commit_info(Some((0, 0)))],
    Some(0)
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
    let version = replay_pm(commits, make_engine)
        .await
        .captured_checkpoint_action
        .map(|action| action.version);
    assert_eq!(version, expected_version);
}

// Writes `commits` as versions 0.. and replays P&M at the latest version.
async fn replay_pm<E: Engine>(
    commits: &[String],
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) -> PmResolution {
    let (store, table_root) = commit_table(commits).await;

    let engine = make_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);

    log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap()
}

// Replay finds checkpoint actions only through `lastManifestCommit`, so an older action is not
// captured when the newest commit carries no pointer.
#[tokio::test]
async fn replay_without_pointer_skips_older_checkpoint_action() {
    let commits = [
        checkpoint_commit(0, &[], one_column_schema()),
        standalone_pm_commit(one_column_schema()),
    ];
    assert_replay_resolution(&commits, None, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_resolution(&commits, None, |store| SyncEngine::new_with_store(store)).await;
}

// Once replay has both P&M, even at the pointed version, the pointed action's P&M cannot win, so a
// pointed commit without a checkpoint action does not fail the build. Non-plan replay leaves the
// action uncaptured, for the accessor to follow the pointer, without reading the pointed commit.
// The plan path reads the pointed commit and captures its action when it has one.
#[rstest]
#[case::pm_at_pointed_version(
    vec![standalone_pm_commit(one_column_schema()), commit_info(Some((0, 0)))],
    (0, 0),
    None
)]
#[case::intermediate_commit_has_newer_pm(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((0, 0), standalone_pm_commit(one_column_schema())),
        commit_info(Some((0, 0))),
    ],
    (0, 0),
    Some(0)
)]
#[case::pointed_commit_without_action_behind_newer_pm(
    vec![
        standalone_pm_commit(one_column_schema()),
        txn_commit(),
        carrying_pointer((1, 1), standalone_pm_commit(one_column_schema())),
        commit_info(Some((1, 1))),
    ],
    (1, 1),
    None
)]
#[tokio::test]
async fn replay_accepts_pointed_commit_it_does_not_need(
    #[case] commits: Vec<String>,
    #[case] (version, content_root_version): (i64, i64),
    #[cfg_attr(not(feature = "declarative-plans"), allow(unused_variables))]
    #[case]
    plan_captured_version: Option<i64>,
) {
    let expected = LastManifestCommit::new(version, content_root_version).unwrap();
    assert_pointer_only(replay_pm(&commits, non_plan_engine).await, &expected);
    #[cfg(feature = "declarative-plans")]
    assert_resolved_pointer(
        replay_pm(&commits, |store| SyncEngine::new_with_store(store)).await,
        &expected,
        plan_captured_version,
    );
}

// Asserts that replay captured no checkpoint action and resolved `lastManifestCommit` to
// `expected`, which the accessor then follows.
fn assert_pointer_only(resolution: PmResolution, expected: &LastManifestCommit) {
    assert_resolved_pointer(resolution, expected, None);
}

// Asserts that replay resolved `lastManifestCommit` to `expected` and captured the checkpoint
// action at `captured_version`, if any.
fn assert_resolved_pointer(
    resolution: PmResolution,
    expected: &LastManifestCommit,
    captured_version: Option<i64>,
) {
    let captured = resolution
        .captured_checkpoint_action
        .map(|action| action.version);
    assert_eq!(captured, captured_version);
    assert_eq!(
        resolution.last_manifest_commit,
        LastManifestCommitResolution::Resolved(Some(expected.clone()))
    );
}

// Replay captures the pointed action while it replays the pointed commit, so it reads that commit
// once rather than again to follow the pointer.
#[rstest]
#[case::pointer_to_older_commit(&[commit_info(Some((0, 0)))])]
#[case::manifest_commit_points_at_itself(&[])]
#[tokio::test]
async fn non_plan_replay_reads_pointed_commit_once(#[case] later_commits: &[String]) {
    let (store, table_root) = manifest_commit_table(later_commits).await;
    let inner: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store));
    let json = Arc::new(CountingJsonHandler::new(inner.json_handler()));
    let engine = DelegatingEngine::new(inner).with_json_handler(json.clone());
    #[cfg(feature = "declarative-plans")]
    let engine = engine.without_plan_executor();
    let log_segment = latest_log_segment(&table_root, &engine);

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert_eq!(
        resolution
            .captured_checkpoint_action
            .map(|action| action.version),
        Some(0)
    );
    let pointed_commit = table_root
        .join("_delta_log/00000000000000000000.json")
        .unwrap();
    assert_eq!(json.reads_of(&pointed_commit), 1);
}

// Replay fails the build when the checkpoint action it needs is missing or does not match the
// pointer.
#[rstest]
#[case::pointed_commit_without_action_and_metadata(
    vec![
        protocol_commit(1, 2),
        commit_info(Some((0, 0))),
    ],
    "that commit carries no checkpoint action"
)]
#[case::pointed_commit_without_action_and_protocol(
    vec![
        metadata_commit(one_column_schema()),
        commit_info(Some((0, 0))),
    ],
    "that commit carries no checkpoint action"
)]
#[case::pointed_action_has_other_content_root(
    vec![
        protocol_commit(1, 2),
        checkpoint_action(0, &[], one_column_schema()),
        commit_info(Some((1, 1))),
    ],
    "that commit's checkpoint action has content root version 0"
)]
#[case::newest_action_has_other_content_root(
    vec![
        standalone_pm_commit(one_column_schema()),
        carrying_pointer((1, 1), checkpoint_action(0, &[], one_column_schema())),
    ],
    "that commit's checkpoint action has content root version 0"
)]
#[case::only_older_manifest_commit_carries_action(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((1, 0), checkpoint_action(0, &[], one_column_schema())),
        commit_info(Some((2, 0))),
        commit_info(Some((2, 0))),
    ],
    "that commit carries no checkpoint action"
)]
#[case::newest_action_without_pointer(
    vec![
        standalone_pm_commit(one_column_schema()),
        checkpoint_action(1, &[], one_column_schema()),
    ],
    "commit 1 carries a checkpoint action but no lastManifestCommit"
)]
#[case::newest_action_with_older_pointer(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((0, 0), checkpoint_action(0, &[], one_column_schema())),
    ],
    "commit 1 carries a checkpoint action"
)]
#[case::pointer_names_newest_commit_without_action(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((1, 1), standalone_pm_commit(one_column_schema())),
    ],
    "that commit carries no checkpoint action"
)]
#[tokio::test]
async fn replay_errors_on_invalid_pointer(
    #[case] commits: Vec<String>,
    #[case] expected_error: &str,
) {
    assert_replay_errors(&commits, expected_error, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_errors(&commits, expected_error, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_replay_errors<E: Engine>(
    commits: &[String],
    expected_error: &str,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = commit_table(commits).await;

    let engine = make_engine(store);
    let result = Snapshot::builder_for(table_root).build(&engine);
    assert!(matches!(&result, Err(KernelError::InvalidLogSegment(_))));
    assert_result_error_with_message(result, expected_error);
}

#[cfg(feature = "declarative-plans")]
#[derive(Debug, Clone, Copy)]
enum ReplayBound {
    Crc,
    Checkpoint,
}

// A replay bounded by a CRC or a checkpoint file runs one P&M plan, which still resolves the
// newest commit's `lastManifestCommit`.
#[cfg(feature = "declarative-plans")]
#[rstest]
#[tokio::test]
async fn plan_replay_bounded_by_crc_or_checkpoint_runs_one_plan(
    #[values(ReplayBound::Crc, ReplayBound::Checkpoint)] bound: ReplayBound,
    #[values(false, true)] compacted: bool,
    #[values(false, true)] newest_has_commit_info: bool,
) {
    let pointer = commit_info(Some((0, 0)));
    let txn = serde_json::json!({ "txn": { "appId": "app", "version": 1 } }).to_string();
    let newest = if newest_has_commit_info {
        pointer.clone()
    } else {
        txn.clone()
    };
    let (store, table_root) = manifest_commit_table(&[pointer.clone(), pointer, newest]).await;
    match bound {
        ReplayBound::Crc => {
            put_crc_with_pointer(&store, 1, LastManifestCommit::new(0, 0).unwrap()).await
        }
        ReplayBound::Checkpoint => put_pm_checkpoint(&store, 1).await,
    }
    let compaction = compacted_log_path_for_versions(2, 3, "json");
    if compacted {
        store.put(&compaction, txn.into()).await.unwrap();
    }

    let plans = Arc::new(Mutex::new(Vec::new()));
    let engine = recording_plan_engine(store, plans.clone());
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let mut log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None).unwrap();
    if compacted {
        let location = table_root.join(compaction.as_ref()).unwrap();
        let compaction = ParsedLogPath::new(FileMeta::new(location, 0, 100)).unwrap();
        log_segment
            .listed
            .ascending_compaction_files
            .push(compaction);
    }
    let crc = log_segment.read_latest_crc(&engine).unwrap();

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, crc.as_ref())
        .unwrap();
    assert!(resolution.protocol.is_some() && resolution.metadata.is_some());
    assert!(resolution.captured_checkpoint_action.is_none());
    let expected = if newest_has_commit_info {
        LastManifestCommitResolution::Resolved(Some(LastManifestCommit::new(0, 0).unwrap()))
    } else {
        LastManifestCommitResolution::Unresolved
    };
    assert_eq!(resolution.last_manifest_commit, expected);
    assert_eq!(plans.lock().unwrap().len(), 1);
}

// An unbounded plan replay reads the newest commit alone, then the older commits only when the
// newest commit's P&M are not final. Each case lists the commit versions each plan scans.
#[cfg(feature = "declarative-plans")]
#[rstest]
#[case::newest_commit_is_the_manifest_commit(
    vec![checkpoint_commit(0, &[], one_column_schema())],
    Some(0),
    vec![vec![0]]
)]
#[case::newest_commit_has_pm(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((0, 0), standalone_pm_commit(one_column_schema())),
    ],
    None,
    vec![vec![1]]
)]
#[case::newest_manifest_commit_also_has_pm(
    vec![
        standalone_pm_commit(one_column_schema()),
        format!(
            "{}\n{}",
            checkpoint_commit(1, &[], one_column_schema()),
            standalone_pm_commit(one_column_schema())
        ),
    ],
    Some(1),
    vec![vec![1]]
)]
#[case::pointer_to_older_manifest_commit(
    vec![checkpoint_commit(0, &[], one_column_schema()), commit_info(Some((0, 0)))],
    Some(0),
    vec![vec![1], vec![0]]
)]
#[case::newest_action_lags_its_commit(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((0, 0), metadata_commit(test_schema_flat_with_column_mapping())),
        carrying_pointer((2, 0), checkpoint_action(0, &[], one_column_schema())),
    ],
    Some(0),
    vec![vec![2], vec![1]]
)]
#[case::no_older_commit_after_newest_action(
    vec![
        checkpoint_commit(0, &[], one_column_schema()),
        carrying_pointer((1, 0), checkpoint_action(0, &[], one_column_schema())),
    ],
    Some(0),
    vec![vec![1]]
)]
#[case::no_pointer(
    vec![standalone_pm_commit(one_column_schema()), commit_info(None)],
    None,
    vec![vec![1], vec![0]]
)]
#[tokio::test]
async fn plan_replay_runs_one_plan_beyond_the_newest_commit_only_when_needed(
    #[case] commits: Vec<String>,
    #[case] expected_version: Option<i64>,
    #[case] expected_plans: Vec<Vec<i64>>,
) {
    let plans = Arc::new(Mutex::new(Vec::new()));
    let recorded = plans.clone();
    assert_replay_resolution(&commits, expected_version, move |store| {
        recording_plan_engine(store, recorded)
    })
    .await;
    assert_eq!(*plans.lock().unwrap(), expected_plans);
}

// A pointer whose commit the segment does not list cannot bound the replay, so the second plan
// reads every older listed commit and captures no action. The listing drops v0 by hand.
#[cfg(feature = "declarative-plans")]
#[tokio::test]
async fn plan_replay_reads_all_older_commits_when_pointed_commit_is_unlisted() {
    let (store, table_root) = manifest_commit_table(&[
        carrying_pointer((0, 0), standalone_pm_commit(one_column_schema())),
        commit_info(Some((0, 0))),
    ])
    .await;
    let plans = Arc::new(Mutex::new(Vec::new()));
    let engine = recording_plan_engine(store, plans.clone());
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let mut log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None).unwrap();
    log_segment.listed.ascending_commit_files.remove(0);

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.protocol.is_some() && resolution.metadata.is_some());
    assert!(resolution.captured_checkpoint_action.is_none());
    assert_eq!(*plans.lock().unwrap(), vec![vec![2], vec![1]]);
}

// The pointed checkpoint action holds the P&M as of its `checkpointMetadata.version`, at least its
// content root version, so the plan path skips the commits at or below that content root, reading
// the manifest commit itself even though it falls just below the replayed range. The unreadable v0
// fails the build if replay reads it.
#[cfg(feature = "declarative-plans")]
#[tokio::test]
async fn plan_replay_skips_commits_below_pointed_content_root() {
    let commits = [
        "not json".to_string(),
        checkpoint_commit(1, &[], one_column_schema()),
        commit_info(Some((1, 1))),
    ];
    assert_replay_resolution(&commits, Some(1), |store| SyncEngine::new_with_store(store)).await;
}

// The pointed manifest commit is newer than the segment's checkpoint, but its content root is not.
// The non-plan replay reads the whole segment and captures the pointed action. A plan replay
// bounded by a checkpoint file reads no checkpoint action, leaving it to the pointer.
#[tokio::test]
async fn replay_resolves_pointed_action_above_segment_checkpoint() {
    check_pointed_action_above_segment_checkpoint(non_plan_engine, Some(1)).await;
    #[cfg(feature = "declarative-plans")]
    check_pointed_action_above_segment_checkpoint(|store| SyncEngine::new_with_store(store), None)
        .await;
}

async fn check_pointed_action_above_segment_checkpoint<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
    expected_captured_version: Option<i64>,
) {
    let (store, table_root) = manifest_commit_table(&[
        commit_info(Some((0, 0))),
        carrying_pointer((2, 1), checkpoint_action(1, &[], one_column_schema())),
        commit_info(Some((2, 1))),
    ])
    .await;
    put_pm_checkpoint(&store, 1).await;

    let engine = make_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);
    assert_eq!(log_segment.checkpoint_version, Some(1));
    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.protocol.is_some() && resolution.metadata.is_some());
    let version = resolution
        .captured_checkpoint_action
        .map(|action| action.version);
    assert_eq!(version, expected_captured_version);
    assert_eq!(
        resolution.last_manifest_commit,
        LastManifestCommitResolution::Resolved(Some(LastManifestCommit::new(2, 1).unwrap()))
    );
}

// A plan-path engine over `store` that records into `plans` the log versions each plan scans.
#[cfg(feature = "declarative-plans")]
fn recording_plan_engine(store: Arc<InMemory>, plans: Arc<Mutex<Vec<Vec<i64>>>>) -> impl Engine {
    let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store));
    let executor = Arc::new(RecordingPlanExecutor {
        inner: engine.plan_executor().unwrap(),
        plans,
    });
    DelegatingEngine::new(engine).with_plan_executor(executor)
}

// A plan executor that records the log versions each plan it runs scans.
#[cfg(feature = "declarative-plans")]
struct RecordingPlanExecutor {
    inner: Arc<dyn PlanExecutor>,
    plans: Arc<Mutex<Vec<Vec<i64>>>>,
}

#[cfg(feature = "declarative-plans")]
impl PlanExecutor for RecordingPlanExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        if let Operation::QueryPlan(plan) = &op {
            let versions = plan
                .nodes
                .iter()
                .flat_map(|node| match &node.op {
                    Operator::ScanJson(scan) => scan.files.as_slice(),
                    Operator::ScanParquet(scan) => scan.files.as_slice(),
                    _ => &[],
                })
                .filter_map(|file| match file.file_constants.first() {
                    Some(Scalar::Long(version)) => Some(*version),
                    _ => None,
                })
                .collect();
            self.plans.lock().unwrap().push(versions);
        }
        self.inner.execute_op(op)
    }
}

// A standalone checkpoint written after the manifest commit hides that commit from the segment.
// Replay still resolves P&M from the checkpoint and the newest commit's pointer without reading the
// pointed commit, which is deleted here, but following the pointer to the action fails because the
// segment does not list its commit.
// TODO(#3683): once standalone checkpoints are supported, replay reads the checkpoint action from
// the checkpoint file and this resolves.
#[tokio::test]
async fn standalone_checkpoint_above_pointed_commit_fails_to_read_its_action() {
    check_standalone_checkpoint_above_pointed_commit(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    check_standalone_checkpoint_above_pointed_commit(|store| SyncEngine::new_with_store(store))
        .await;
}

async fn check_standalone_checkpoint_above_pointed_commit<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let pointer = commit_info(Some((0, 0)));
    let (store, table_root) = manifest_commit_table(&[pointer.clone(), pointer]).await;
    put_pm_checkpoint(&store, 1).await;
    store
        .delete(&delta_path_for_version(0, "json"))
        .await
        .unwrap();

    let engine = make_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);
    assert_eq!(log_segment.checkpoint_version, Some(1));
    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.protocol.is_some() && resolution.metadata.is_some());
    assert!(resolution.captured_checkpoint_action.is_none());
    assert_eq!(
        resolution.last_manifest_commit,
        LastManifestCommitResolution::Resolved(Some(LastManifestCommit::new(0, 0).unwrap()))
    );

    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_result_error_with_message(
        snapshot.latest_checkpoint_action(&engine),
        "this log segment does not list that commit",
    );
}

// A segment that ends on a checkpoint does not list the commit at that version, so a checkpoint
// action among the checkpoint's rows is not taken as that commit's: replay resolves P&M from the
// checkpoint without requiring a `lastManifestCommit` and leaves the action uncaptured.
#[tokio::test]
async fn segment_ending_on_checkpoint_ignores_its_checkpoint_action_rows() {
    check_segment_ending_on_checkpoint(non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    check_segment_ending_on_checkpoint(|store| SyncEngine::new_with_store(store)).await;
}

async fn check_segment_ending_on_checkpoint<E: Engine>(
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = manifest_commit_table(&[]).await;
    let checkpoint_rows = format!(
        "{}\n{}\n{}",
        serde_json::json!({ "checkpointMetadata": { "version": 0 } }),
        standalone_pm_commit(one_column_schema()),
        checkpoint_action(0, &[], one_column_schema())
    );
    let checkpoint =
        delta_path_for_version(0, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json");
    store
        .put(&checkpoint, checkpoint_rows.into())
        .await
        .unwrap();

    let engine = make_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);
    assert_eq!(log_segment.checkpoint_version, Some(0));
    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.protocol.is_some() && resolution.metadata.is_some());
    assert!(resolution.captured_checkpoint_action.is_none());
}

// An incremental update resolves the latest checkpoint action for the updated snapshot: a new
// commit carrying a newer checkpoint action surfaces that newer action, while a new commit carrying
// none leaves the action uncaptured, so the accessor follows the new commit's `lastManifestCommit`
// back to the base's older action. Runs on both the plan and non-plan paths.
#[rstest]
#[case::new_commit_carries_a_newer_action(checkpoint_commit(1, &[], one_column_schema()), 1)]
#[case::new_commit_carries_no_action(
    format!("{}\n{}", commit_info(Some((0, 0))), metadata_commit(one_column_schema())),
    0
)]
#[tokio::test]
async fn incremental_update_resolves_latest_checkpoint_action(
    #[case] new_commit: String,
    #[case] expected_version: i64,
) {
    assert_incremental_update_action(&new_commit, expected_version, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_incremental_update_action(&new_commit, expected_version, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_incremental_update_action<E: Engine>(
    new_commit: &str,
    expected_version: i64,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = manifest_commit_table(&[]).await;

    let engine = make_engine(store.clone());
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

    add_commit(
        table_root.as_str(),
        store.as_ref(),
        1,
        new_commit.to_string(),
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
        Some(expected_version)
    );
}

// A CRC at the target version carrying `lastManifestCommit` makes `latest_checkpoint_action` read
// only the commit it points at, failing when that commit does not carry the expected action. Each
// case has checkpoint commits at v0 and v2, the given v1, and a CRC at v2 holding the pointer. A
// scan would find v2's action first, so any other expected version proves the pointer was followed.
// A pointer naming v1 deliberately disagrees with v2's own action for that reason.
#[rstest]
#[case::pointer_names_latest_manifest_commit(
    checkpoint_commit(1, &[], one_column_schema()),
    2,
    2,
    Ok(2)
)]
#[case::pointer_is_read_instead_of_scanning(
    checkpoint_commit(1, &[], one_column_schema()),
    1,
    1,
    Ok(1)
)]
#[case::pointer_names_commit_checkpointing_older_version(
    carrying_pointer((1, 0), checkpoint_action(0, &[], one_column_schema())),
    1,
    0,
    Ok(0)
)]
#[case::manifest_commit_without_action_fails(
    metadata_commit(one_column_schema()),
    1,
    1,
    Err("that commit carries no checkpoint action")
)]
#[case::manifest_commit_with_other_content_root_version_fails(
    carrying_pointer((1, 0), checkpoint_action(0, &[], one_column_schema())),
    1,
    1,
    Err("that commit's checkpoint action has content root version 0")
)]
#[tokio::test]
async fn latest_checkpoint_action_reads_commit_named_by_crc_pointer(
    #[case] v1: String,
    #[case] pointer_version: i64,
    #[case] pointer_content_root_version: i64,
    #[case] expected: Result<i64, &str>,
) {
    let commits = [
        checkpoint_commit(0, &[], one_column_schema()),
        v1,
        checkpoint_commit(2, &[], one_column_schema()),
    ];
    let (store, table_root) = commit_table(&commits).await;
    let pointer = LastManifestCommit::new(pointer_version, pointer_content_root_version).unwrap();
    put_crc_with_pointer(&store, 2, pointer).await;

    let engine = non_plan_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    let result = snapshot.latest_checkpoint_action(&engine);
    match expected {
        Ok(version) => assert_eq!(result.unwrap().map(|a| a.version), Some(version)),
        Err(message) => assert_result_error_with_message(result, message),
    }
}

// The protocol defines no source for `lastManifestCommit` besides the CRC and the snapshot
// version's `commitInfo`, so a checkpoint-only snapshot with neither cannot locate the checkpoint
// action. Such a log needs a classic checkpoint, which adaptiveMetadata forbids.
#[tokio::test]
async fn latest_checkpoint_action_fails_without_commit_or_crc_at_checkpoint_version() {
    let (store, table_root) = commit_table(&[checkpoint_commit(0, &[], one_column_schema())]).await;
    put_pm_checkpoint(&store, 1).await;

    let engine = non_plan_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(snapshot.version(), 1);

    assert_result_error_with_message(
        snapshot.latest_checkpoint_action(&engine),
        "Table version 1 is missing",
    );
}

// Without a CRC, a replay miss makes `latest_checkpoint_action` follow the `lastManifestCommit` in
// the latest commit's `commitInfo`. v0 and v1 carry checkpoint actions; v2 carries standalone P&M
// so replay stops there without capturing one, plus the given pointer. A scan would find v1's
// action, so any other result proves the pointer was followed, including its recorded absence.
#[rstest]
#[case::pointer_names_latest_manifest_commit(Some((1, 1)), Some(1))]
#[case::pointer_is_read_instead_of_scanning(Some((0, 0)), Some(0))]
#[case::no_pointer_means_no_checkpoint_action(None, None)]
#[tokio::test]
async fn latest_checkpoint_action_follows_commit_info_pointer(
    #[case] pointer: Option<(i64, i64)>,
    #[case] expected_version: Option<i64>,
) {
    let commits = [
        checkpoint_commit(0, &[], one_column_schema()),
        checkpoint_commit(1, &[], one_column_schema()),
        format!(
            "{}\n{}",
            commit_info(pointer),
            standalone_pm_commit(one_column_schema())
        ),
    ];
    let (store, table_root) = commit_table(&commits).await;

    let engine = non_plan_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    let action = snapshot.latest_checkpoint_action(&engine).unwrap();
    assert_eq!(action.map(|a| a.version), expected_version);
}

// The pointer alone decides the lookup: one naming a commit the segment does not list, below or
// above it, is inconsistent. The table has commits v0 and v1, and the segment keeps only v1, whose
// checkpoint action a scan would find. So v0 is below the segment, and v5 is past the table's end.
#[rstest]
#[case::below_segment_start(0)]
#[case::past_table_end(5)]
#[tokio::test]
async fn read_last_checkpoint_action_from_pointer_rejects_commit_outside_segment(
    #[case] pointer_version: i64,
) {
    let (store, table_root) =
        manifest_commit_table(&[checkpoint_commit(1, &[], one_column_schema())]).await;

    let engine = non_plan_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine).segment_after_version(0);

    let pointer = LastManifestCommit::new(pointer_version, pointer_version).unwrap();
    let result = log_segment.read_last_checkpoint_action_from_pointer(&engine, &pointer);
    assert_result_error_with_message(result, "this log segment does not list that commit");
}

#[rstest]
#[case::root_manifest_commit_captures_its_action(true, 1)]
#[case::plain_commit_carries_the_read_snapshot_action(false, 0)]
#[tokio::test]
async fn post_commit_snapshot_resolves_latest_checkpoint_action(
    #[case] write_root_manifest: bool,
    #[case] expected_version: i64,
) {
    let (store, table_root) = commit_table(&[checkpoint_commit(0, &[], one_column_schema())]).await;
    let engine = non_plan_engine(store.clone());
    let snapshot = Snapshot::builder_for(table_root.clone())
        .build(&engine)
        .unwrap();

    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    // A root manifest commit captures the action it writes; a plain commit carries the read
    // snapshot's action forward.
    if write_root_manifest {
        txn = txn
            .with_root_manifest_file(FileMeta {
                location: table_root.join("metadata/root-v1.parquet").unwrap(),
                last_modified: 0,
                size: 1024,
            })
            .unwrap();
    }
    let post_commit = txn.commit(&engine).unwrap().unwrap_post_commit_snapshot();

    // With every commit file deleted, resolving the action proves no log scan is needed.
    for version in 0..=1 {
        store
            .delete(&delta_path_for_version(version, "json"))
            .await
            .unwrap();
    }
    let action = post_commit
        .latest_checkpoint_action(&engine)
        .unwrap()
        .expect("post-commit snapshot should resolve the checkpoint action");
    assert_eq!(action.version, expected_version);
}

// Replay takes `lastManifestCommit` from the newest commit only: a manifest commit records itself,
// a later log commit carries it forward, and a newest commit without one is not back-filled from an
// older commit, so that miss stays `Unresolved`. The same holds when a CRC at v0 seeds the replay.
#[rstest]
#[case::manifest_commit_records_itself(vec![], false, Some((0, 0)))]
#[case::log_commit_carries_it_forward(vec![commit_info(Some((0, 0)))], false, Some((0, 0)))]
#[case::newest_commit_without_it_is_not_backfilled(vec![commit_info(None)], false, None)]
#[case::crc_seeded_log_commit_carries_it_forward(vec![commit_info(Some((0, 0)))], true, Some((0, 0)))]
#[case::crc_seeded_newest_commit_without_it(vec![commit_info(None)], true, None)]
#[tokio::test]
async fn replay_resolves_last_manifest_commit_from_newest_commit(
    #[case] later_commits: Vec<String>,
    #[case] crc_seeded: bool,
    #[case] expected: Option<(i64, i64)>,
) {
    let expected = match expected {
        Some((v, c)) => {
            LastManifestCommitResolution::Resolved(Some(LastManifestCommit::new(v, c).unwrap()))
        }
        None => LastManifestCommitResolution::Unresolved,
    };
    let crc = crc_seeded.then(|| Arc::new(Crc::default()));
    let crc = crc.as_ref();
    assert_replay_last_manifest_commit(&later_commits, crc, &expected, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_replay_last_manifest_commit(&later_commits, crc, &expected, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_replay_last_manifest_commit<E: Engine>(
    later_commits: &[String],
    crc: Option<&Arc<Crc>>,
    expected: &LastManifestCommitResolution,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = manifest_commit_table(later_commits).await;
    let engine = make_engine(store);
    let log_segment = latest_log_segment(&table_root, &engine);

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, crc)
        .unwrap();
    assert_eq!(&resolution.last_manifest_commit, expected);
}

// A `lastManifestCommit` naming a version after the commit that carries it is invalid, so replay
// fails instead of storing a pointer that later reads would trust, whether or not a checkpoint file
// bounds the replay.
#[rstest]
#[tokio::test]
async fn replay_rejects_pointer_after_newest_commit(#[values(false, true)] checkpointed: bool) {
    check_pointer_after_newest_commit(checkpointed, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    check_pointer_after_newest_commit(checkpointed, |store| SyncEngine::new_with_store(store))
        .await;
}

async fn check_pointer_after_newest_commit<E: Engine>(
    checkpointed: bool,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let later_commits = [commit_info(Some((0, 0))), commit_info(Some((5, 5)))];
    let (store, table_root) = manifest_commit_table(&later_commits).await;
    if checkpointed {
        put_pm_checkpoint(&store, 1).await;
    }
    let engine = make_engine(store);
    assert_result_error_with_message(
        Snapshot::builder_for(table_root).build(&engine),
        "lastManifestCommit version 5 exceeds commit version 2",
    );
}

// Replay reads the newest commit and the pointed manifest commit as their own files, not through
// a log compaction covering them.
#[rstest]
#[case::follows_pointer(
    vec![checkpoint_commit(1, &[], test_schema_flat_with_column_mapping()), commit_info(Some((1, 1)))],
    test_schema_flat_with_column_mapping(),
    resolved_pointer(1, 1),
)]
#[case::no_pointer(
    vec![pointerless_pm_commit(), commit_info(None), commit_info(None)],
    one_column_schema(),
    LastManifestCommitResolution::Unresolved,
)]
#[case::covers_manifest_commit_past_its_content_root(
    vec![
        carrying_pointer((0, 0), metadata_commit(test_schema_flat_with_column_mapping())),
        carrying_pointer(
            (2, 1),
            checkpoint_action(1, &[], test_schema_flat_with_column_mapping()),
        ),
        commit_info(Some((2, 1))),
    ],
    test_schema_flat_with_column_mapping(),
    resolved_pointer(2, 1),
)]
#[case::newest_commit_has_no_commit_info(
    vec![pointerless_pm_commit(), commit_info(None), txn_commit()],
    one_column_schema(),
    LastManifestCommitResolution::Unresolved,
)]
#[tokio::test]
async fn replay_reads_newest_commit_covered_by_compaction(
    #[case] later_commits: Vec<String>,
    #[case] expected_schema: SchemaRef,
    #[case] expected: LastManifestCommitResolution,
) {
    assert_compacted_replay(&later_commits, &expected_schema, &expected, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_compacted_replay(&later_commits, &expected_schema, &expected, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_compacted_replay<E: Engine>(
    later_commits: &[String],
    expected_schema: &SchemaRef,
    expected: &LastManifestCommitResolution,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = manifest_commit_table(later_commits).await;
    let engine = make_engine(store.clone());
    let log_segment =
        segment_with_newest_two_compacted(&store, &table_root, &engine, later_commits.len()).await;

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    let schema = resolution.metadata.unwrap().parse_schema().unwrap();
    assert_eq!(&schema, expected_schema.as_ref());
    // These fixtures' checkpoint actions are at their content root version.
    let captured_version = resolution
        .captured_checkpoint_action
        .map(|action| action.version);
    let expected_version = match expected {
        LastManifestCommitResolution::Resolved(Some(pointer)) => Some(pointer.content_root_version),
        _ => None,
    };
    assert_eq!(captured_version, expected_version);
    assert_eq!(&resolution.last_manifest_commit, expected);
}

// Writes a log compaction covering the newest two commits and returns a segment that uses it.
// Compaction listing is disabled, so the compaction is added to the segment by hand.
async fn segment_with_newest_two_compacted(
    store: &InMemory,
    table_root: &url::Url,
    engine: &impl Engine,
    newest_version: usize,
) -> LogSegment {
    let newest_version = newest_version as u64;
    let compaction = compacted_log_path_for_versions(newest_version - 1, newest_version, "json");
    store.put(&compaction, txn_commit().into()).await.unwrap();

    let mut log_segment = latest_log_segment(table_root, engine);
    let compaction_location = table_root.join(compaction.as_ref()).unwrap();
    let compaction = ParsedLogPath::new(FileMeta::new(compaction_location, 0, 100)).unwrap();
    log_segment
        .listed
        .ascending_compaction_files
        .push(compaction);
    log_segment
}

// A plan replay reads the pointed checkpoint action from its manifest commit, so it stops at that
// action even when a compaction hides it, never reading the older commits below it. The
// unparseable v0 would fail the build if replay read it.
#[cfg(feature = "declarative-plans")]
#[tokio::test]
async fn plan_replay_stops_at_pointed_checkpoint_action_covered_by_compaction() {
    let (store, table_root) = commit_table(&[
        "not json".to_string(),
        checkpoint_commit(1, &[], test_schema_flat_with_column_mapping()),
        commit_info(Some((1, 1))),
    ])
    .await;
    let engine = SyncEngine::new_with_store(store.clone());
    let log_segment = segment_with_newest_two_compacted(&store, &table_root, &engine, 2).await;

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    let schema = resolution.metadata.unwrap().parse_schema().unwrap();
    assert_eq!(schema, *test_schema_flat_with_column_mapping());
    assert_eq!(
        resolution
            .captured_checkpoint_action
            .map(|action| action.version),
        Some(1)
    );
    assert_eq!(resolution.last_manifest_commit, resolved_pointer(1, 1));
}

// A replayed snapshot serves `lastManifestCommit` from build-time state: it still resolves after
// the newest commit file is deleted.
#[rstest]
#[case::carried_pointer(commit_info(Some((0, 0))), Some((0, 0)))]
#[tokio::test]
async fn replayed_snapshot_serves_last_manifest_commit_without_reading_the_log(
    #[case] newest_commit: String,
    #[case] expected: Option<(i64, i64)>,
) {
    assert_snapshot_serves_last_manifest_commit(&newest_commit, expected, non_plan_engine).await;
    #[cfg(feature = "declarative-plans")]
    assert_snapshot_serves_last_manifest_commit(&newest_commit, expected, |store| {
        SyncEngine::new_with_store(store)
    })
    .await;
}

async fn assert_snapshot_serves_last_manifest_commit<E: Engine>(
    newest_commit: &str,
    expected: Option<(i64, i64)>,
    make_engine: impl FnOnce(Arc<InMemory>) -> E,
) {
    let (store, table_root) = manifest_commit_table(&[newest_commit.to_string()]).await;
    let engine = make_engine(store.clone());
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    store
        .delete(&delta_path_for_version(1, "json"))
        .await
        .unwrap();
    let expected = expected.map(|(v, c)| LastManifestCommit::new(v, c).unwrap());
    assert_eq!(snapshot.last_manifest_commit(&engine).unwrap(), expected);
}

// A read snapshot loaded from a CRC never captures the checkpoint action, so a plain commit's
// post-commit snapshot follows the transaction's own `lastManifestCommit`. With the new commit file
// deleted, only that pointer can name the manifest commit.
#[tokio::test]
async fn post_commit_snapshot_follows_transaction_last_manifest_commit() {
    let (store, table_root) = commit_table(&[checkpoint_commit(0, &[], one_column_schema())]).await;
    put_crc_with_pointer(&store, 0, LastManifestCommit::new(0, 0).unwrap()).await;
    let engine = non_plan_engine(store.clone());
    let snapshot = Snapshot::builder_for(table_root.clone())
        .build(&engine)
        .unwrap();

    let post_commit = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(&engine)
        .unwrap()
        .unwrap_post_commit_snapshot();

    store
        .delete(&delta_path_for_version(1, "json"))
        .await
        .unwrap();
    let action = post_commit
        .latest_checkpoint_action(&engine)
        .unwrap()
        .expect("the transaction's pointer should locate the checkpoint action");
    assert_eq!(action.version, 0);
}

// A snapshot handed a CRC at its version takes `lastManifestCommit` from that CRC, including its
// absence, so it resolves without the commit file. The commit carries `(0, 0)`, so a `None` result
// can only come from the CRC.
#[rstest]
#[case::crc_records_pointer(Some((0, 0)))]
#[case::crc_records_no_pointer(None)]
#[tokio::test]
async fn snapshot_with_crc_at_version_serves_last_manifest_commit_from_crc(
    #[case] crc_pointer: Option<(i64, i64)>,
) {
    let (store, table_root) = manifest_commit_table(&[commit_info(Some((0, 0)))]).await;
    let engine = non_plan_engine(store.clone());
    let loaded = Snapshot::builder_for(table_root).build(&engine).unwrap();
    let expected = crc_pointer.map(|(v, c)| LastManifestCommit::new(v, c).unwrap());
    let crc = Crc {
        version: loaded.version(),
        last_manifest_commit_opt: expected.clone(),
        ..Default::default()
    };
    let snapshot = Snapshot::new_with_crc(
        loaded.log_segment().clone(),
        loaded.table_configuration().clone(),
        Some(Arc::new(crc)),
        false, /* built_as_latest */
        false, /* skipped_new_checkpoints */
    )
    .unwrap();

    store
        .delete(&delta_path_for_version(1, "json"))
        .await
        .unwrap();
    assert_eq!(snapshot.last_manifest_commit(&engine).unwrap(), expected);
}

// A snapshot that sits on a checkpoint (no CRC, no later commits) cannot read commitInfo during
// replay, so it reads `lastManifestCommit` from the commit file at the checkpoint version, and
// errors with `MissingVersion` when that file is gone. P&M replay, and so the snapshot build,
// succeeds either way.
#[rstest]
#[case::commit_file_present(false)]
#[case::commit_file_deleted(true)]
#[tokio::test]
async fn snapshot_on_checkpoint_reads_last_manifest_commit_from_commit_file(
    #[case] delete_commit_file: bool,
) {
    let (store, table_root) = manifest_commit_table(&[]).await;
    put_pm_checkpoint(&store, 0).await;
    let engine = SyncEngine::new_with_store(store.clone());
    if delete_commit_file {
        store
            .delete(&delta_path_for_version(0, "json"))
            .await
            .unwrap();
    }

    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(0));
    let result = snapshot.last_manifest_commit(&engine);
    if delete_commit_file {
        // TODO(#3683): when the checkpoint is a standalone checkpoint, read lastManifestCommit
        // from it instead of failing on the missing commit file.
        assert!(matches!(result, Err(KernelError::MissingVersion(0))));
    } else {
        assert_eq!(
            result.unwrap(),
            Some(LastManifestCommit::new(0, 0).unwrap())
        );
    }
}

// Writes a parquet checkpoint at `version` holding only top-level protocol and metaData. Kernel
// refuses to write a classic checkpoint on an adaptiveMetadata table, so it is written by hand.
async fn put_pm_checkpoint(store: &Arc<InMemory>, version: u64) {
    let rows = standalone_pm_commit(one_column_schema());
    add_checkpoint_to_store(
        store,
        parse_json_batch(StringArray::from(rows.lines().collect::<Vec<_>>())),
        &format!("{version:020}.checkpoint.parquet"),
    )
    .await
    .unwrap();
}

// Writes a CRC at `version` carrying `pointer` as its `lastManifestCommit`.
async fn put_crc_with_pointer(store: &InMemory, version: u64, pointer: LastManifestCommit) {
    let config = adaptive_metadata_table_configuration(one_column_schema(), &[]);
    let crc = Crc {
        version,
        protocol: config.protocol().clone(),
        metadata: config.metadata().clone(),
        file_stats_state: FileStatsState::Complete(FileStats::try_new(0, 0, None).unwrap()),
        // AMT config enables ICT, so the CRC must carry an inCommitTimestampOpt.
        in_commit_timestamp_opt: Some(1000),
        last_manifest_commit_opt: Some(pointer),
        ..Default::default()
    };
    store
        .put(
            &delta_path_for_version(version, "crc"),
            serde_json::to_vec(&crc).unwrap().into(),
        )
        .await
        .unwrap();
}

// Counts `read_json_files` calls per file, so a test can assert how often replay reads a commit.
struct CountingJsonHandler {
    inner: Arc<dyn JsonHandler>,
    reads: Mutex<HashMap<Url, usize>>,
}

impl CountingJsonHandler {
    fn new(inner: Arc<dyn JsonHandler>) -> Self {
        Self {
            inner,
            reads: Mutex::default(),
        }
    }

    fn reads_of(&self, location: &Url) -> usize {
        self.reads
            .lock()
            .unwrap()
            .get(location)
            .copied()
            .unwrap_or(0)
    }
}

impl JsonHandler for CountingJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let mut reads = self.reads.lock().unwrap();
        for file in files {
            *reads.entry(file.location.clone()).or_default() += 1;
        }
        self.inner
            .read_json_files(files, physical_schema, predicate)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        self.inner.write_json_file(path, data, overwrite)
    }
}
