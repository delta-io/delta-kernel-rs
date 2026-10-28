//! AMT (adaptiveMetadata) tests for Protocol & Metadata replay: manifest commits whose P&M is
//! carried by a `checkpoint` action, exercised on both the plan and non-plan replay paths.

use std::sync::Arc;

use rstest::rstest;
use test_utils::{add_commit, assert_result_error_with_message, delta_path_for_version};

use super::LogSegment;
use super::tests::add_checkpoint_to_store;
use crate::actions::LastManifestCommit;
use crate::committer::FileSystemCommitter;
use crate::crc::{Crc, FileStats, FileStatsState};
use crate::engine::sync::SyncEngine;
#[cfg(feature = "declarative-plans")]
use crate::engine::test_delegating::DelegatingEngine;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::schema::SchemaRef;
use crate::table_features::TableFeature;
use crate::transaction::UpdateTableOperation;
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, parse_json_batch, test_schema_flat_with_column_mapping,
};
use crate::{Engine, FileMeta, Snapshot};

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

// Builds a `commitInfo` line with an In-Commit Timestamp and, when given, a `lastManifestCommit`
// `(version, contentRootVersion)`, as an AMT writer records on every commit.
fn commit_info_line(pointer: Option<(i64, i64)>) -> String {
    let mut commit_info = serde_json::json!({ "timestamp": 1, "inCommitTimestamp": 1 });
    if let Some((version, content_root_version)) = pointer {
        commit_info["lastManifestCommit"] =
            serde_json::json!({ "version": version, "contentRootVersion": content_root_version });
    }
    serde_json::json!({ "commitInfo": commit_info }).to_string()
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
// carries one, and `None` when P&M come from standalone actions (a replay miss leaves the action
// uncaptured, so the accessor follows the commit's `lastManifestCommit`, which records none).
#[rstest]
#[case::commit_carries_checkpoint_action(checkpoint_commit(0, &[], one_column_schema()), Some(0))]
#[case::standalone_pm_has_no_checkpoint_action(
    format!("{}\n{}", commit_info_line(None), standalone_pm_commit(one_column_schema())),
    None
)]
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

// A snapshot built without P&M replay (`Snapshot::new`) captures no checkpoint action, so
// `latest_checkpoint_action` follows the commit's `lastManifestCommit` to it.
#[tokio::test]
async fn latest_checkpoint_action_without_replay_follows_commit_info_pointer() {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        format!(
            "{}\n{}",
            commit_info_line(Some((0, 0))),
            checkpoint_commit(0, &[], one_column_schema())
        ),
    )
    .await
    .unwrap();

    let engine = non_plan_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment =
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None).unwrap();
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
        LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None).unwrap();

    let resolution = log_segment
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    let version = resolution
        .captured_checkpoint_action
        .map(|action| action.version);
    assert_eq!(version, expected_version);
}

// An incremental update resolves the latest checkpoint action for the updated snapshot: a new
// commit carrying a newer checkpoint action surfaces that newer action, while a new commit carrying
// none leaves the action uncaptured, so the accessor follows the new commit's `lastManifestCommit`
// back to the base's older action.
#[rstest]
#[case::new_commit_carries_a_newer_action(checkpoint_commit(1, &[], one_column_schema()), 1)]
#[case::new_commit_carries_no_action(
    format!("{}\n{}", commit_info_line(Some((0, 0))), metadata_commit(one_column_schema())),
    0
)]
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

// A CRC at the target version carrying `lastManifestCommit` makes `latest_checkpoint_action` read
// only the commit it points at, failing when that commit does not carry the expected action. Each
// case has checkpoint commits at v0 and v2, the given v1, and a CRC at v2 holding the pointer. A
// scan would find v2's action first, so any other expected version proves the pointer was followed.
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
    checkpoint_commit(0, &[], one_column_schema()),
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
    checkpoint_commit(0, &[], one_column_schema()),
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
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    let commits = [
        checkpoint_commit(0, &[], one_column_schema()),
        v1,
        checkpoint_commit(2, &[], one_column_schema()),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(table_root.as_str(), store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
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
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    let commit = format!(
        "{}\n{}",
        commit_info_line(Some((0, 0))),
        checkpoint_commit(0, &[], one_column_schema())
    );
    add_commit(table_root.as_str(), store.as_ref(), 0, commit)
        .await
        .unwrap();
    let pm = standalone_pm_commit(one_column_schema());
    add_checkpoint_to_store(
        &store,
        parse_json_batch(pm.lines().collect::<Vec<_>>().into()),
        "00000000000000000001.checkpoint.parquet",
    )
    .await
    .unwrap();

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
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    let commits = [
        format!(
            "{}\n{}",
            commit_info_line(Some((0, 0))),
            checkpoint_commit(0, &[], one_column_schema())
        ),
        format!(
            "{}\n{}",
            commit_info_line(Some((1, 1))),
            checkpoint_commit(1, &[], one_column_schema())
        ),
        format!(
            "{}\n{}",
            commit_info_line(pointer),
            standalone_pm_commit(one_column_schema())
        ),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(table_root.as_str(), store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }

    let engine = non_plan_engine(store);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

    let action = snapshot.latest_checkpoint_action(&engine).unwrap();
    assert_eq!(action.map(|a| a.version), expected_version);
}

// The pointer alone decides the lookup: one naming a commit the segment does not list, below or
// above it, is inconsistent. The segment keeps only v1, whose checkpoint action a scan would find.
#[rstest]
#[tokio::test]
async fn read_last_checkpoint_action_from_pointer_rejects_commit_outside_segment(
    #[values(0, 5)] pointer_version: i64,
) {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    for version in 0..2 {
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            version,
            checkpoint_commit(version as i64, &[], one_column_schema()),
        )
        .await
        .unwrap();
    }

    let engine = non_plan_engine(store);
    let storage = engine.storage_handler();
    let log_root = table_root.join("_delta_log/").unwrap();
    let log_segment = LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None)
        .unwrap()
        .segment_after_version(0);

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
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        format!(
            "{}\n{}",
            serde_json::json!({ "commitInfo": { "timestamp": 1, "inCommitTimestamp": 1 } }),
            checkpoint_commit(0, &[], one_column_schema()),
        ),
    )
    .await
    .unwrap();
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

// A read snapshot loaded from a CRC never captures the checkpoint action, so a plain commit's
// post-commit snapshot follows the transaction's own `lastManifestCommit`. With the new commit file
// deleted, only that pointer can name the manifest commit.
#[tokio::test]
async fn post_commit_snapshot_follows_transaction_last_manifest_commit() {
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        0,
        format!(
            "{}\n{}",
            commit_info_line(Some((0, 0))),
            checkpoint_commit(0, &[], one_column_schema())
        ),
    )
    .await
    .unwrap();
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
    let store = Arc::new(InMemory::new());
    let table_root = url::Url::parse("memory:///").unwrap();
    let commits = [
        checkpoint_commit(0, &[], one_column_schema()),
        serde_json::json!({ "commitInfo": {
            "inCommitTimestamp": 1,
            "lastManifestCommit": { "version": 0, "contentRootVersion": 0 },
        } })
        .to_string(),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(table_root.as_str(), store.as_ref(), version as u64, commit)
            .await
            .unwrap();
    }
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
