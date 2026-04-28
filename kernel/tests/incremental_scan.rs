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
    IncrementalListing, IncrementalScanBuilder, IncrementalScanResult,
};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::Snapshot;
use test_utils::{
    actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
    create_log_path, TestAction,
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

fn unwrap_listing(result: IncrementalScanResult) -> IncrementalListing {
    match result {
        IncrementalScanResult::Listing(l) => l,
        other => panic!("expected listing, got {other:?}"),
    }
}

// Reyden's "compaction-chain" pattern: an Add in commit N is cancelled by a Remove for
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
        IncrementalScanBuilder::new(target, 0)
            .build(engine.as_ref(), std::iter::empty::<&str>())?,
    );

    assert_eq!(
        surviving_add_count(&listing),
        0,
        "older Add(X) is cancelled by newer Remove(X)"
    );
    assert!(
        listing.remove_files.contains("X"),
        "Remove(X) survives in remove_files"
    );
    assert!(listing.duplicate_add_paths.is_empty());

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
        IncrementalScanBuilder::new(target, 0)
            .build(engine.as_ref(), std::iter::empty::<&str>())?,
    );

    // Only C survives as an add; A and B were cancelled by their respective newer Removes.
    assert_eq!(surviving_add_count(&listing), 1);
    // Both Removes survive in remove_files (A and B). They harmlessly target a base
    // that does not contain them, but the kernel does not assume base content here.
    assert!(listing.remove_files.contains("A"));
    assert!(listing.remove_files.contains("B"));
    assert!(listing.duplicate_add_paths.is_empty());

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
        IncrementalScanBuilder::new(target, 0)
            .build(engine.as_ref(), base_paths.iter().copied())?,
    );

    assert_eq!(
        surviving_add_count(&listing),
        1,
        "the re-add row stays in add_files so new metadata is delivered"
    );
    assert!(
        listing.duplicate_add_paths.contains("re-added.parquet"),
        "path is surfaced as duplicate so consumers mask stale base entries"
    );
    assert!(listing.remove_files.is_empty());

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
        IncrementalScanBuilder::new(target, 0)
            .build(engine.as_ref(), base_paths.iter().copied())?,
    );

    assert_eq!(surviving_add_count(&listing), 2);
    assert_eq!(listing.duplicate_add_paths.len(), 1);
    assert!(listing.duplicate_add_paths.contains("re-added.parquet"));

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
        IncrementalScanBuilder::new(target, 0)
            .build(engine.as_ref(), std::iter::empty::<&str>())?,
    );

    // Both staged Adds must survive -- they're only reachable via log_tail.
    assert_eq!(
        surviving_add_count(&listing),
        2,
        "expected 2 surviving Adds from staged commits in log_tail"
    );
    assert_eq!(listing.target_version, 2);

    Ok(())
}
