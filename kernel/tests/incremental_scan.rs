//! Integration tests for [`IncrementalScanBuilder`].
//!
//! These cover the dedup edge cases that were known to bite consumers doing their own
//! cross-commit deduplication: cross-commit cancellation, chained add/remove/add, log
//! compaction interactions, catalog-managed staged commits, and vacuum races.

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::json::DefaultJsonHandler;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::incremental_scan::{
    IncrementalListing, IncrementalScanStream, IncrementalScanSummary,
};
use delta_kernel::log_replay::FileActionKey;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path as ObjectStorePath;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::{
    Engine, EvaluationHandler, JsonHandler, ParquetHandler, Snapshot, StorageHandler,
};
use rstest::rstest;
use test_utils::{
    actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
    compacted_log_path_for_versions, create_log_path, delta_path_for_version, TestAction,
};
use url::Url;

fn setup_test() -> (
    Arc<InMemory>,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Url,
) {
    let storage = Arc::new(InMemory::new());
    let table_root = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    (storage, engine, table_root)
}

fn key(path: &str) -> FileActionKey {
    FileActionKey {
        path: path.to_string(),
        dv_unique_id: None,
    }
}

fn key_with_dv(path: &str, dv_unique_id: &str) -> FileActionKey {
    FileActionKey {
        path: path.to_string(),
        dv_unique_id: Some(dv_unique_id.to_string()),
    }
}

fn surviving_add_count(listing: &IncrementalListing) -> usize {
    listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum()
}

fn unwrap_listing(result: Option<IncrementalScanStream>) -> IncrementalListing {
    result
        .expect("expected Some(stream), got None (commits unavailable)")
        .collect_listing()
        .expect("collect_listing succeeded")
}

// Cancellation: walking newest-first with `(path, dv_unique_id)` first-seen-wins,
// every Add whose path matches a later Remove gets cancelled. Cases vary the chain
// depth: a single add-then-remove pair (no chain) versus a 3-commit compaction
// chain where every intermediate add is cancelled by the next commit's remove.
#[rstest]
#[case::single_pair(
    vec![
        vec![TestAction::Add("X".to_string())],
        vec![TestAction::Remove("X".to_string())],
    ],
    0,
    vec!["X"],
)]
#[case::compaction_chain(
    vec![
        vec![TestAction::Add("A".to_string())],
        vec![TestAction::Add("B".to_string()), TestAction::Remove("A".to_string())],
        vec![TestAction::Add("C".to_string()), TestAction::Remove("B".to_string())],
    ],
    1,
    vec!["A", "B"],
)]
#[tokio::test]
async fn within_range_cancellation(
    #[case] commits: Vec<Vec<TestAction>>,
    #[case] expected_surviving: usize,
    #[case] expected_removes: Vec<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    let target_version = commits.len() as u64;
    for (idx, body) in commits.into_iter().enumerate() {
        add_commit(
            table_root,
            storage.as_ref(),
            (idx + 1) as u64,
            actions_to_string(body),
        )
        .await?;
    }

    let target = Snapshot::builder_for(table_url)
        .at_version(target_version)
        .build(engine.as_ref())?;
    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(surviving_add_count(&listing), expected_surviving);
    let expected: HashSet<FileActionKey> = expected_removes.iter().map(|p| key(p)).collect();
    assert_eq!(listing.summary.removes, expected);

    Ok(())
}

// Catalog-managed: commits in the range live as staged JSONs under `_staged_commits/`
// (not in the published log) and are surfaced to the snapshot via `with_log_tail`.
// `IncrementalScanBuilder` must walk the snapshot's commit list -- not re-list storage --
// or those commits are silently invisible and the diff is wrong.
#[tokio::test]
async fn picks_up_staged_commits_from_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0: published, catalog-managed metadata. v1, v2: staged-only.
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    let staged1 = add_staged_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("staged-1.parquet".to_string())]),
    )
    .await?;
    let staged2 = add_staged_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("staged-2.parquet".to_string())]),
    )
    .await?;

    let log_tail = vec![
        create_log_path(&table_url, staged1),
        create_log_path(&table_url, staged2),
    ];
    let target = Snapshot::builder_for(table_url)
        .with_log_tail(log_tail)
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    assert_eq!(target.version(), 2);

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    // Both staged Adds must survive -- they're only reachable via log_tail.
    assert_eq!(
        surviving_add_count(&listing),
        2,
        "expected 2 surviving Adds from staged commits in log_tail"
    );
    assert_eq!(listing.summary.target_version, 2);

    Ok(())
}

// A commit in the range that contains only Remove actions (no Adds) should not crash,
// should produce no entries in `add_files`, and the Remove keys should land in `removes`.
#[tokio::test]
async fn removes_only_commit_in_range() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![
            TestAction::Remove("gone-a.parquet".to_string()),
            TestAction::Remove("gone-b.parquet".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(surviving_add_count(&listing), 0);
    assert!(listing.add_files.is_empty(), "no Adds means no add batches");
    assert_eq!(listing.summary.removes.len(), 2);
    assert!(listing.summary.removes.contains(&key("gone-a.parquet")));
    assert!(listing.summary.removes.contains(&key("gone-b.parquet")));

    Ok(())
}

// A commit in the range with only metadata actions (no Add or Remove) should be
// silently skipped: no add batches, no removes.
#[tokio::test]
async fn metadata_only_commit_in_range() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    // v1 contains only the standard Metadata; no Add or Remove.
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(surviving_add_count(&listing), 0);
    assert!(listing.add_files.is_empty());
    assert!(listing.summary.removes.is_empty());

    Ok(())
}

// === DV-aware dedup ===
// The kernel's dedup key is `(path, dv_unique_id)`, where `dv_unique_id` is built from
// `(storageType, pathOrInlineDv, offset)`. These tests construct raw commit JSON
// (TestAction doesn't emit DV-bearing Adds) to exercise the DV cases.

const ACTION_METADATA: &str = "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}";

fn add_with_dv(path: &str, storage_type: &str, path_or_inline: &str, offset: i32) -> String {
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":null,\"tags\":null,\"deletionVector\":{{\"storageType\":\"{storage_type}\",\"pathOrInlineDv\":\"{path_or_inline}\",\"offset\":{offset},\"sizeInBytes\":10,\"cardinality\":1}},\"baseRowId\":null,\"defaultRowCommitVersion\":null,\"clusteringProvider\":null}}}}"
    )
}

fn add_no_dv(path: &str) -> String {
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":null,\"tags\":null,\"deletionVector\":null,\"baseRowId\":null,\"defaultRowCommitVersion\":null,\"clusteringProvider\":null}}}}"
    )
}

// Same path with two different DVs across commits: both rows must survive. The dedup
// key includes dv_unique_id, so `(X, dv1)` and `(X, dv2)` are distinct and neither
// cancels the other. This is the protocol-correct outcome for DV-update flows where
// the file's tombstone state changes between commits.
#[tokio::test]
async fn same_path_different_dvs_both_survive() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(table_root, storage.as_ref(), 0, ACTION_METADATA.to_string()).await?;
    // v1: Add(X, dv=u/abc/1)
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        add_with_dv("X.parquet", "u", "abc", 1),
    )
    .await?;
    // v2: Add(X, dv=u/xyz/2) -- different DV id, distinct key.
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        add_with_dv("X.parquet", "u", "xyz", 2),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(
        surviving_add_count(&listing),
        2,
        "two Adds for the same path with different DVs must both survive (distinct keys)"
    );
    assert!(listing.summary.removes.is_empty());
    let expected_adds: HashSet<FileActionKey> = [
        key_with_dv("X.parquet", "uabc@1"),
        key_with_dv("X.parquet", "uxyz@2"),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        listing.summary.surviving_adds, expected_adds,
        "both `(X, dv1)` and `(X, dv2)` keys survive"
    );

    Ok(())
}

// Same path with the same DV across commits: only the newest Add survives. With dedup
// walking newest-first, the first occurrence of `(X, dv)` wins and the older copy is
// dropped. This is the standard "duplicate Add" case.
#[tokio::test]
async fn same_path_same_dv_only_newest_survives() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(table_root, storage.as_ref(), 0, ACTION_METADATA.to_string()).await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        add_with_dv("X.parquet", "u", "abc", 1),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        add_with_dv("X.parquet", "u", "abc", 1),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(
        surviving_add_count(&listing),
        1,
        "duplicate `(path, dv)` keys collapse to one surviving Add"
    );

    Ok(())
}

// DV-update pattern: v1 introduces the file with no DV; v2 emits Remove(X, no_dv) +
// Add(X, dv1). The Remove has key `(X, NULL)` and cancels the v1 Add. The new Add has
// key `(X, dv1)` -- distinct -- and survives. The consumer sees one surviving Add
// with the new DV and one surviving Remove for the old (no-DV) version.
#[tokio::test]
async fn dv_update_remove_no_dv_add_with_dv() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(table_root, storage.as_ref(), 0, ACTION_METADATA.to_string()).await?;
    // v1: Add(X) without a DV.
    add_commit(table_root, storage.as_ref(), 1, add_no_dv("X.parquet")).await?;
    // v2: Remove(X, no_dv) + Add(X, dv=u/abc/1).
    let remove_no_dv = "{\"remove\":{\"path\":\"X.parquet\",\"deletionTimestamp\":1700000000000,\"dataChange\":true,\"deletionVector\":null}}".to_string();
    let v2_actions = format!(
        "{}\n{}",
        remove_no_dv,
        add_with_dv("X.parquet", "u", "abc", 1),
    );
    add_commit(table_root, storage.as_ref(), 2, v2_actions).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    // The new DV-bearing Add (key `(X, dv1)`) survives. The v1 no-DV Add (key `(X, NULL)`)
    // is cancelled by the v2 no-DV Remove (same key).
    assert_eq!(
        surviving_add_count(&listing),
        1,
        "the new DV-bearing Add survives; the old no-DV Add is cancelled"
    );
    assert!(
        listing.summary.removes.contains(&key("X.parquet")),
        "the no-DV Remove survives in `removes` (key `(X, NULL)`)"
    );

    Ok(())
}

// A commit file the snapshot listed is removed before the stream reads it (e.g. a
// vacuum races between snapshot construction and the incremental scan). The build()
// call only does metadata-only checks and succeeds, but the stream surfaces
// `FileNotFound` while reading the missing commit. Consumers fall back to a full scan
// on any stream error.
#[tokio::test]
async fn missing_commit_file_surfaces_error_during_iteration(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("a.parquet".to_string())]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    let v1_path: ObjectStorePath = delta_path_for_version(1, "json");
    storage.delete(&v1_path).await?;

    let mut stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");

    let item = stream
        .next()
        .expect("stream should yield an error item before exhausting");
    let err = match item {
        Ok(_) => panic!("expected FileNotFound from the stream, got an Ok batch"),
        Err(e) => e,
    };
    assert!(
        matches!(err, delta_kernel::Error::FileNotFound(_)),
        "expected FileNotFound, got {err:?}"
    );

    let finish_err = stream
        .finish()
        .expect_err("finish should error on a previously-errored stream");
    assert!(
        finish_err.to_string().contains("previously errored"),
        "unexpected finish error: {finish_err}"
    );

    Ok(())
}

// Caller errors: a base_version that is not strictly less than the target snapshot's version
// must surface as `Err`, not as a `CommitsUnavailable` listing. The latter is reserved for
// log-retention / vacuum-race scenarios; equal or inverted ranges are programmer mistakes.
// Target snapshot is at v1; the parameter is the (invalid) base_version.
#[rstest]
#[case::base_equals_target(1)]
#[case::base_above_target(2)]
#[case::base_at_max(u64::MAX)]
#[tokio::test]
async fn rejects_invalid_base_version(
    #[case] bad_base: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("a.parquet".to_string())]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let err = target
        .incremental_scan_builder(bad_base)
        .build(engine.as_ref())
        .expect_err("expected Err for invalid base_version");
    assert!(
        err.to_string().contains("must be less than"),
        "unexpected error: {err}"
    );

    Ok(())
}

// A log compaction file alongside the JSON commits in the range must not affect the
// result. Kernel's incremental path reads commit JSONs directly (the snapshot's log
// segment exposes commits and compactions on separate fields, and this builder iterates
// only `ascending_commit_files`). The compaction is ignored. We assert the listing matches
// what we'd get without the compaction file present at all.
#[tokio::test]
async fn compaction_file_in_range_is_ignored() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0 Metadata; v1 add A,B; v2 remove A; v3 add C.
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![
            TestAction::Add("A".to_string()),
            TestAction::Add("B".to_string()),
        ]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Remove("A".to_string())]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        3,
        actions_to_string(vec![TestAction::Add("C".to_string())]),
    )
    .await?;

    // Drop a compaction file covering 1-3 next to the commit JSONs. Body is irrelevant
    // because the incremental scan never opens it.
    let compaction_path: ObjectStorePath = compacted_log_path_for_versions(1, 3, "json");
    storage
        .put(&compaction_path, bytes::Bytes::new().into())
        .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(3)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(
        surviving_add_count(&listing),
        2,
        "B (live) and C (live) survive; A is cancelled by the v2 Remove"
    );
    assert_eq!(
        listing.summary.removes,
        HashSet::from([key("A")]),
        "Remove(A) survives"
    );

    Ok(())
}

// `finish` returns the surviving Add and Remove file-key sets directly (without going
// through the iterator first). Connectors that don't need per-batch streaming can drain
// via `finish` and apply their own logic over the surviving keys.
#[tokio::test]
async fn finish_returns_surviving_keys() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![
            TestAction::Add("A".to_string()),
            TestAction::Add("B".to_string()),
        ]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![
            TestAction::Remove("A".to_string()),
            TestAction::Add("C".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");

    let footer: IncrementalScanSummary = stream.finish()?;
    assert_eq!(footer.base_version, 0);
    assert_eq!(footer.target_version, 2);
    assert_eq!(footer.surviving_adds, HashSet::from([key("B"), key("C")]),);
    assert_eq!(footer.removes, HashSet::from([key("A")]));

    Ok(())
}

// The iterator yields one batch per source commit that produced surviving Adds, in
// descending commit-version order. Commits whose Adds were all cancelled by later
// Removes do not produce an item, but they still update dedup state for older commits.
#[tokio::test]
async fn streaming_yields_batches_newest_first_skipping_cancelled_commits(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0 Metadata; v1 add(A); v2 add(B); v3 remove(A) remove(B); v4 add(C).
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("A".to_string())]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("B".to_string())]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        3,
        actions_to_string(vec![
            TestAction::Remove("A".to_string()),
            TestAction::Remove("B".to_string()),
        ]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        4,
        actions_to_string(vec![TestAction::Add("C".to_string())]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(4)
        .build(engine.as_ref())?;

    let mut stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");

    let mut batches: Vec<delta_kernel::engine_data::FilteredEngineData> = Vec::new();
    for item in stream.by_ref() {
        batches.push(item?);
    }

    // v4 produced the surviving add(C); v3 had only Removes (no add item);
    // v2 and v1 had their adds cancelled by v3 (no add items). So exactly one batch.
    assert_eq!(
        batches.len(),
        1,
        "expected one yielded batch (v4 only); v3 has no Adds, v1/v2 cancelled"
    );
    let surviving: usize = batches[0].selection_vector().iter().filter(|s| **s).count();
    assert_eq!(surviving, 1, "the single yielded batch contains add(C)");

    let footer = stream.finish()?;
    assert_eq!(footer.removes, HashSet::from([key("A"), key("B")]),);

    Ok(())
}

// A single batch where some Adds survive dedup and some are cancelled. Exercises the
// non-trivial selection-vector path in `process_batch` (mixed booleans, not all-true
// or all-false).
#[tokio::test]
async fn mixed_intra_batch_selection_vector() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0 Metadata; v1 add(A); v2 add(B), add(C), add(D); v3 remove(B), remove(D).
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("A".to_string())]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![
            TestAction::Add("B".to_string()),
            TestAction::Add("C".to_string()),
            TestAction::Add("D".to_string()),
        ]),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        3,
        actions_to_string(vec![
            TestAction::Remove("B".to_string()),
            TestAction::Remove("D".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(3)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    // Two yielded batches: v2 (with mixed selection [false,true,false]) and v1 (with [true]).
    assert_eq!(listing.add_files.len(), 2);
    assert_eq!(
        surviving_add_count(&listing),
        2,
        "C and A survive; B and D cancelled"
    );
    assert_eq!(listing.summary.removes, HashSet::from([key("B"), key("D")]),);

    // Find the batch whose selection has both true and false entries (the v2 batch).
    let mixed_batch = listing
        .add_files
        .iter()
        .find(|b| {
            let sv = b.selection_vector();
            sv.iter().any(|s| *s) && sv.iter().any(|s| !*s)
        })
        .expect("expected at least one batch with mixed selection");
    assert_eq!(mixed_batch.selection_vector().len(), 3);

    Ok(())
}

// === Batch-splitting invariance ===
//
// Engine wrapper that overrides only the JSON handler so we can force `read_json_files`
// to chunk a single commit file across many `ActionsBatch` yields. Used by the
// `single_commit_split_across_batches_dedups_correctly` test to verify the kernel does
// not carry per-batch dedup state. A naive consumer that assumes "one yield = one commit"
// would mis-cancel a same-commit `add(P, dv=new) + remove(P, dv=old)` pair when the two
// rows land in different batches. Our dedup is keyed on `(path, dv_unique_id)` against a
// global `seen_file_keys`, so output must be identical regardless of how the JSON reader
// chunks rows.
struct CustomBatchSizeEngine {
    inner: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    json: Arc<DefaultJsonHandler<TokioBackgroundExecutor>>,
}

impl CustomBatchSizeEngine {
    fn new(storage: Arc<InMemory>, batch_size: usize) -> Self {
        let task_executor = Arc::new(TokioBackgroundExecutor::new());
        let json = Arc::new(
            DefaultJsonHandler::new(storage.clone(), task_executor.clone())
                .with_batch_size(batch_size),
        );
        let inner = Arc::new(
            DefaultEngineBuilder::new(storage)
                .with_task_executor(task_executor)
                .build(),
        );
        Self { inner, json }
    }
}

impl Engine for CustomBatchSizeEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.inner.parquet_handler()
    }
}

// Regression test for batch-splitting invariance. A single commit contains a mix that
// stresses path-only state machines: plain Adds, two DV-replacement pairs (same path, Add
// with new DV plus Remove with old/null DV), and a standalone Remove. Forcing the JSON
// reader to split this commit into many `ActionsBatch` yields (batch_size = 1, 2, 3)
// must produce the exact same surviving Add and Remove path sets as the unsplit case
// (batch_size large enough to hold every row). If our dedup state ever becomes
// per-batch instead of global, this test fails.
#[rstest]
#[case::row_per_batch(1)]
#[case::two_per_batch(2)]
#[case::three_per_batch(3)]
#[case::all_in_one_batch(1000)]
#[tokio::test]
async fn single_commit_split_across_batches_dedups_correctly(
    #[case] batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(table_root, storage.as_ref(), 0, ACTION_METADATA.to_string()).await?;

    // v1 has 10 rows. The DV-replacement pairs are written `Remove(P, dv=old)` BEFORE
    // `Add(P, dv=new)`. This is the row order that breaks a path-only state machine: it
    // sees the Remove first, records P as removed, then cancels the matching Add when it
    // arrives in a later batch, losing both rows. Kernel's `(path, dv_unique_id)` keying
    // keeps `(P, null)` and `(P, dv=new)` distinct, so Add and Remove are independent
    // regardless of batch boundary.
    let remove_no_dv = |path: &str| -> String {
        format!(
            "{{\"remove\":{{\"path\":\"{path}\",\"deletionTimestamp\":1700000000000,\
             \"dataChange\":true,\"deletionVector\":null}}}}"
        )
    };
    let v1_actions = [
        add_no_dv("a.parquet"),
        add_no_dv("b.parquet"),
        // DV-replacement pair #1: Remove BEFORE matching Add. This is the bug-triggering
        // order for path-only consumers.
        remove_no_dv("d.parquet"),
        add_with_dv("d.parquet", "u", "abc", 1),
        add_no_dv("c.parquet"),
        // DV-replacement pair #2: Remove BEFORE matching Add.
        remove_no_dv("f.parquet"),
        add_with_dv("f.parquet", "u", "xyz", 2),
        add_no_dv("e.parquet"),
        // Standalone Remove for a path not added in this range.
        remove_no_dv("stale.parquet"),
        add_no_dv("g.parquet"),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1_actions).await?;

    let engine = CustomBatchSizeEngine::new(storage, batch_size);
    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(&engine)?;
    let listing = unwrap_listing(target.incremental_scan_builder(0).build(&engine)?);

    // 7 surviving Adds: a, b, c, e, g (plain) plus d and f (the new DV-bearing copies).
    // The no-DV Removes for d and f have key `(path, NULL)`, distinct from the new
    // DV-bearing Adds, so they survive too.
    let expected_adds: HashSet<FileActionKey> = [
        key("a.parquet"),
        key("b.parquet"),
        key("c.parquet"),
        key_with_dv("d.parquet", "uabc@1"),
        key("e.parquet"),
        key_with_dv("f.parquet", "uxyz@2"),
        key("g.parquet"),
    ]
    .into_iter()
    .collect();
    let expected_removes: HashSet<FileActionKey> =
        [key("d.parquet"), key("f.parquet"), key("stale.parquet")]
            .into_iter()
            .collect();

    assert_eq!(
        listing.summary.surviving_adds, expected_adds,
        "surviving_adds must not depend on batch_size (got batch_size={batch_size})"
    );
    assert_eq!(
        listing.summary.removes, expected_removes,
        "removes must not depend on batch_size (got batch_size={batch_size})"
    );
    assert_eq!(
        surviving_add_count(&listing),
        7,
        "selection-vector counts must not depend on batch_size (got batch_size={batch_size})"
    );

    Ok(())
}
