//! Integration tests for [`IncrementalScanBuilder`].
//!
//! These cover the dedup edge cases that were known to bite consumers doing their own
//! cross-commit deduplication: cross-commit cancellation, chained add/remove/add, and
//! metadata-only re-adds (paths in the consumer's base that get a new Add action in
//! the range).

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::incremental_scan::{
    IncrementalListing, IncrementalScanBuilder, IncrementalScanRawFooter, IncrementalScanResult,
};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path as ObjectStorePath;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::Snapshot;
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

fn surviving_add_count(listing: &IncrementalListing) -> usize {
    listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum()
}

fn unwrap_listing<P: AsRef<str>>(
    result: IncrementalScanResult<'_>,
    base_paths: impl IntoIterator<Item = P>,
) -> IncrementalListing {
    match result {
        IncrementalScanResult::Stream(s) => s
            .collect_listing(base_paths)
            .expect("collect_listing succeeded"),
        other => panic!("expected Stream, got {other:?}"),
    }
}

// Compaction-chain pattern: an Add in commit N is cancelled by a Remove for
// the same path in a later commit. The kernel's `(path, dv_unique_id)` dedup, walking
// newest-first with first-seen-wins, drops the older Add and keeps the surviving Remove.
#[tokio::test]
async fn newer_remove_cancels_older_add() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0: Metadata + Protocol (table creation).
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    // v1: add(X).
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("X".to_string())]),
    )
    .await?;
    // v2: remove(X).
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Remove("X".to_string())]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    assert_eq!(
        surviving_add_count(&listing),
        0,
        "older Add(X) is cancelled by newer Remove(X)"
    );
    assert!(
        listing.footer.remove_files.contains("X"),
        "Remove(X) survives in remove_files"
    );
    assert!(listing.footer.duplicate_add_paths.is_empty());

    Ok(())
}

// Compaction-chain (3 commits): every add is cancelled by a later remove except the
// last. Without correct cross-commit dedup, an Add for a removed file would leak through.
#[tokio::test]
async fn compaction_chain_cancels_intermediate_adds() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    // v1: add(A)
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("A".to_string())]),
    )
    .await?;
    // v2: add(B), remove(A)
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![
            TestAction::Add("B".to_string()),
            TestAction::Remove("A".to_string()),
        ]),
    )
    .await?;
    // v3: add(C), remove(B)
    add_commit(
        table_root,
        storage.as_ref(),
        3,
        actions_to_string(vec![
            TestAction::Add("C".to_string()),
            TestAction::Remove("B".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(3)
        .build(engine.as_ref())?;

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    // Only C survives as an add; A and B were cancelled by their respective newer Removes.
    assert_eq!(surviving_add_count(&listing), 1);
    // Both Removes survive in remove_files (A and B). They harmlessly target a base
    // that does not contain them, but the kernel does not assume base content here.
    assert!(listing.footer.remove_files.contains("A"));
    assert!(listing.footer.remove_files.contains("B"));
    assert!(listing.footer.duplicate_add_paths.is_empty());

    Ok(())
}

// Metadata-only re-add: a path that is already in the consumer's base listing gets a new
// Add action in the range (e.g., OPTIMIZE / liquid clustering re-tag). The Add row stays
// in `add_files` (so the new metadata is delivered); the path is also surfaced in
// `duplicate_add_paths` so the consumer can mask its stale base entry.
#[tokio::test]
async fn metadata_only_readd_surfaces_in_duplicate_add_paths(
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
        actions_to_string(vec![TestAction::Add("re-added.parquet".to_string())]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    // Consumer's base has the path that v1 re-adds.
    let base_paths: HashSet<&str> = ["re-added.parquet"].into_iter().collect();
    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        base_paths.iter().copied(),
    );

    assert_eq!(
        surviving_add_count(&listing),
        1,
        "the re-add row stays in add_files so new metadata is delivered"
    );
    assert!(
        listing
            .footer
            .duplicate_add_paths
            .contains("re-added.parquet"),
        "path is surfaced as duplicate so consumers mask stale base entries"
    );
    assert!(listing.footer.remove_files.is_empty());

    Ok(())
}

// Mix: one Add that's truly new and one Add that's a metadata-only re-add. The two
// classifications coexist and are reported correctly.
#[tokio::test]
async fn mixed_new_and_duplicate_adds() -> Result<(), Box<dyn std::error::Error>> {
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
            TestAction::Add("brand-new.parquet".to_string()),
            TestAction::Add("re-added.parquet".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let base_paths: HashSet<&str> = ["re-added.parquet"].into_iter().collect();
    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        base_paths.iter().copied(),
    );

    assert_eq!(surviving_add_count(&listing), 2);
    assert_eq!(listing.footer.duplicate_add_paths.len(), 1);
    assert!(listing
        .footer
        .duplicate_add_paths
        .contains("re-added.parquet"));

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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    // Both staged Adds must survive -- they're only reachable via log_tail.
    assert_eq!(
        surviving_add_count(&listing),
        2,
        "expected 2 surviving Adds from staged commits in log_tail"
    );
    assert_eq!(listing.footer.target_version, 2);

    Ok(())
}

// A commit in the range that contains only Remove actions (no Adds) should not crash,
// should produce no entries in `add_files`, and the Remove paths should land in
// `remove_files`.
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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    assert_eq!(surviving_add_count(&listing), 0);
    assert!(listing.add_files.is_empty(), "no Adds means no add batches");
    assert_eq!(listing.footer.remove_files.len(), 2);
    assert!(listing.footer.remove_files.contains("gone-a.parquet"));
    assert!(listing.footer.remove_files.contains("gone-b.parquet"));

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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    assert_eq!(surviving_add_count(&listing), 0);
    assert!(listing.add_files.is_empty());
    assert!(listing.footer.remove_files.is_empty());
    assert!(listing.footer.duplicate_add_paths.is_empty());

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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    assert_eq!(
        surviving_add_count(&listing),
        2,
        "two Adds for the same path with different DVs must both survive (distinct keys)"
    );
    assert!(listing.footer.remove_files.is_empty());

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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    // The new DV-bearing Add (key `(X, dv1)`) survives. The v1 no-DV Add (key `(X, NULL)`)
    // is cancelled by the v2 no-DV Remove (same key).
    assert_eq!(
        surviving_add_count(&listing),
        1,
        "the new DV-bearing Add survives; the old no-DV Add is cancelled"
    );
    assert!(
        listing.footer.remove_files.contains("X.parquet"),
        "the no-DV Remove survives in remove_files"
    );

    Ok(())
}

// A commit file that the snapshot listed is removed before the stream reads it (e.g. a
// vacuum races between snapshot construction and the incremental scan). The build() call
// only does metadata-only checks and succeeds, but the stream surfaces `FileNotFound`
// while reading the missing commit. Consumers fall back to a full scan on any stream
// error.
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

    // Simulate a vacuum: delete the v1 commit JSON from underlying storage AFTER the
    // snapshot is built (so the snapshot still lists it, but reading it will fail).
    let v1_path: ObjectStorePath = delta_path_for_version(1, "json");
    storage.delete(&v1_path).await?;

    let result = IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?;
    let mut stream = match result {
        IncrementalScanResult::Stream(s) => s,
        other => panic!("expected Stream, got {other:?}"),
    };

    // The stream reads commit JSONs lazily; the missing file surfaces here.
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

    // Subsequent finish_raw must also error rather than silently returning a
    // partial footer.
    let finish_err = stream
        .finish_raw()
        .expect_err("finish_raw should error on a previously-errored stream");
    assert!(
        finish_err.to_string().contains("previously errored"),
        "unexpected finish_raw error: {finish_err}"
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

    let err = IncrementalScanBuilder::new(target, bad_base)
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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    assert_eq!(
        surviving_add_count(&listing),
        2,
        "B (live) and C (live) survive; A is cancelled by the v2 Remove"
    );
    assert_eq!(
        listing.footer.remove_files,
        HashSet::from(["A".to_string()]),
        "Remove(A) survives"
    );
    assert!(listing.footer.duplicate_add_paths.is_empty());

    Ok(())
}

// `finish_raw` returns the surviving Add and Remove path sets without applying the
// cross-snapshot intersection. Connectors that own a non-HashSet base data structure
// take this path and apply their own intersection.
#[tokio::test]
async fn finish_raw_returns_surviving_paths() -> Result<(), Box<dyn std::error::Error>> {
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

    let stream = match IncrementalScanBuilder::new(target, 0).build(engine.as_ref())? {
        IncrementalScanResult::Stream(s) => s,
        other => panic!("expected Stream, got {other:?}"),
    };

    // Drain entirely via finish_raw without ever calling next() ourselves.
    let footer: IncrementalScanRawFooter = stream.finish_raw()?;
    assert_eq!(footer.base_version, 0);
    assert_eq!(footer.target_version, 2);
    assert_eq!(
        footer.surviving_add_paths,
        HashSet::from(["B".to_string(), "C".to_string()]),
    );
    assert_eq!(footer.remove_files, HashSet::from(["A".to_string()]));

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

    // v0: Metadata
    // v1: add(A)
    // v2: add(B)
    // v3: remove(A), remove(B)  -> all earlier Adds cancelled; v3 itself only has Removes,
    //                              which do not contribute to add_files. v1's and v2's
    //                              Adds are also cancelled.
    // v4: add(C)
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

    let mut stream = match IncrementalScanBuilder::new(target, 0).build(engine.as_ref())? {
        IncrementalScanResult::Stream(s) => s,
        other => panic!("expected Stream, got {other:?}"),
    };

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

    // Removes still flow into remove_files even though their commit didn't produce a batch.
    let footer = stream.finish(std::iter::empty::<&str>())?;
    assert_eq!(
        footer.remove_files,
        HashSet::from(["A".to_string(), "B".to_string()]),
    );

    Ok(())
}

// The pull-then-finalize flow: drive the iterator manually with `next()`, then call
// `finish(base_paths)` to classify duplicates. This is the documented streaming
// consumer pattern (the module-level example shows exactly this).
#[tokio::test]
async fn finish_after_manual_streaming_classifies_duplicates(
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
        actions_to_string(vec![
            TestAction::Add("brand-new.parquet".to_string()),
            TestAction::Add("re-added.parquet".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let mut stream = match IncrementalScanBuilder::new(target, 0).build(engine.as_ref())? {
        IncrementalScanResult::Stream(s) => s,
        other => panic!("expected Stream, got {other:?}"),
    };

    let mut yielded = 0;
    for item in stream.by_ref() {
        let _batch = item?;
        yielded += 1;
    }
    assert_eq!(yielded, 1, "v1 produced one Add batch");

    // base contains "re-added" and a path that's NOT in the surviving Adds; only the
    // intersection should appear in duplicate_add_paths.
    let footer = stream.finish(["re-added.parquet", "not-in-range.parquet"].iter().copied())?;
    assert_eq!(
        footer.duplicate_add_paths,
        HashSet::from(["re-added.parquet".to_string()]),
        "non-matching base path is filtered out"
    );
    assert!(footer.remove_files.is_empty());

    Ok(())
}

// A single batch where some Adds survive dedup and some are cancelled. Exercises the
// non-trivial selection-vector path in `process_batch` (mixed booleans, not all-true
// or all-false).
#[tokio::test]
async fn mixed_intra_batch_selection_vector() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // v0: Metadata
    // v1: add(A)
    // v2: add(B), add(C), add(D)   <- all three are Adds, but B and D get cancelled by v3
    // v3: remove(B), remove(D)
    // base = 0, target = 3. Newest-first: v3 records (B,-) and (D,-) seen. v2's Adds:
    //   B is seen -> cancelled, C is new -> survives, D is seen -> cancelled.
    // So v2's batch has selection vector = [false, true, false] (mixed).
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

    let listing = unwrap_listing(
        IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?,
        std::iter::empty::<&str>(),
    );

    // Two yielded batches: v2 (with mixed selection [false,true,false]) and v1 (with [true]).
    assert_eq!(listing.add_files.len(), 2);
    let total_surviving = surviving_add_count(&listing);
    assert_eq!(total_surviving, 2, "C and A survive; B and D cancelled");
    assert_eq!(
        listing.footer.remove_files,
        HashSet::from(["B".to_string(), "D".to_string()]),
    );

    // Find the batch whose selection has both true and false entries (the v2 batch),
    // confirming the non-trivial mask path was exercised.
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
