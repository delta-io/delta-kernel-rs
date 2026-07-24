//! Integration tests for [`IncrementalScanBuilder`].
//!
//! These cover the dedup edge cases that were known to bite consumers doing their own
//! cross-commit deduplication: cross-commit cancellation, chained add/remove/add, log
//! compaction interactions, catalog-managed staged commits, and vacuum races.

use std::collections::{HashMap, HashSet};
use std::num::NonZero;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::{column_expr, Expression as Expr, Predicate as Pred, PredicateRef};
use delta_kernel::incremental_scan::{
    IncrementalListing, IncrementalListingAgainstBase, IncrementalScanStream,
    IncrementalScanSummary,
};
use delta_kernel::log_replay::FileActionKey;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path as ObjectStorePath;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{
    Engine, EvaluationHandler, JsonHandler, ParquetHandler, Snapshot, SnapshotRef, StorageHandler,
};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use test_utils::delta_kernel_default_engine::json::DefaultJsonHandler;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::{
    actions_to_string, actions_to_string_catalog_managed, actions_to_string_partitioned,
    add_commit, add_staged_commit, assert_result_error_with_message,
    compacted_log_path_for_versions, create_log_path, create_table_and_load_snapshot,
    delta_path_for_version, test_table_setup_mt, write_batch_to_table, TestAction,
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
    FileActionKey::new(path, None)
}

fn key_with_dv(path: &str, dv_unique_id: &str) -> FileActionKey {
    FileActionKey::new(path, Some(dv_unique_id.to_string()))
}

fn live_add_count(listing: &IncrementalListing) -> usize {
    listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum()
}

fn unwrap_listing(result: Option<IncrementalScanStream>) -> IncrementalListing {
    result
        .expect("expected Some(stream), got None (commits unavailable)")
        .into_listing()
        .expect("into_listing succeeded")
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
    #[case] expected_live: usize,
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

    assert_eq!(live_add_count(&listing), expected_live);
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
        live_add_count(&listing),
        2,
        "expected 2 live Adds from staged commits in log_tail"
    );
    assert_eq!(listing.summary.target_version, 2);

    Ok(())
}

// A commit in the range that contains no Adds (removes-only or metadata-only) should
// produce no entries in `add_files` and surface only the Remove paths (if any) in
// `removes`.
#[rstest]
#[case::removes_only(
    vec![
        TestAction::Remove("gone-a.parquet".to_string()),
        TestAction::Remove("gone-b.parquet".to_string()),
    ],
    vec!["gone-a.parquet", "gone-b.parquet"],
)]
#[case::metadata_only(vec![TestAction::Metadata], vec![])]
#[tokio::test]
async fn commit_with_no_live_adds(
    #[case] v1_actions: Vec<TestAction>,
    #[case] expected_remove_paths: Vec<&'static str>,
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
        actions_to_string(v1_actions),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;
    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(live_add_count(&listing), 0);
    assert!(listing.add_files.is_empty(), "no Adds means no add batches");
    let expected_removes: HashSet<FileActionKey> =
        expected_remove_paths.iter().map(|p| key(p)).collect();
    assert_eq!(listing.summary.removes, expected_removes);

    Ok(())
}

// Locks the iterator contract: `next()` returns `None` once every in-range commit has been
// processed, even when no commit produced a live Add. Polling past exhaustion stays
// `None` and never panics.
#[tokio::test]
async fn next_returns_none_after_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
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
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;
    let mut stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");

    for item in stream.by_ref() {
        item?;
    }
    assert!(
        stream.next().is_none(),
        "polling past exhaustion stays None"
    );
    assert!(stream.next().is_none(), "still None on repeat poll");

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

// Same path with DV-bearing Adds across commits. Dedup keys on `(path, dv_unique_id)`:
// different DV ids produce distinct keys (both survive), same DV ids collapse to one
// live Add (newest-wins).
#[rstest]
#[case::different_dvs(
    ("u", "abc", 1),
    ("u", "xyz", 2),
    2,
    vec![("X.parquet", "uabc@1"), ("X.parquet", "uxyz@2")],
)]
#[case::same_dvs(
    ("u", "abc", 1),
    ("u", "abc", 1),
    1,
    vec![("X.parquet", "uabc@1")],
)]
#[tokio::test]
async fn same_path_across_commits_dedups_by_dv(
    #[case] v1_dv: (&'static str, &'static str, i32),
    #[case] v2_dv: (&'static str, &'static str, i32),
    #[case] expected_live: usize,
    #[case] expected_keys: Vec<(&'static str, &'static str)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(table_root, storage.as_ref(), 0, ACTION_METADATA.to_string()).await?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        add_with_dv("X.parquet", v1_dv.0, v1_dv.1, v1_dv.2),
    )
    .await?;
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        add_with_dv("X.parquet", v2_dv.0, v2_dv.1, v2_dv.2),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;
    let listing = unwrap_listing(target.incremental_scan_builder(0).build(engine.as_ref())?);

    assert_eq!(live_add_count(&listing), expected_live);
    assert!(listing.summary.removes.is_empty());
    let expected_adds: HashSet<FileActionKey> = expected_keys
        .iter()
        .map(|(p, dv)| key_with_dv(p, dv))
        .collect();
    assert_eq!(listing.summary.live_adds, expected_adds);

    Ok(())
}

// DV-update pattern: v1 introduces the file with no DV; v2 emits Remove(X, no_dv) +
// Add(X, dv1). The Remove has key `(X, NULL)` and cancels the v1 Add. The new Add has
// key `(X, dv1)` -- distinct -- and stays live. The consumer sees one live Add
// with the new DV and one Remove for the old (no-DV) version.
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
        live_add_count(&listing),
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
        .into_summary()
        .expect_err("into_summary should error on a previously-errored stream");
    assert!(
        finish_err.to_string().contains("previously errored"),
        "unexpected into_summary error: {finish_err}"
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
        live_add_count(&listing),
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

// `into_summary` returns the live Add and Remove file-key sets directly (without going
// through the iterator first). Connectors that don't need per-batch streaming can drain
// via `into_summary` and apply their own logic over the keys.
#[tokio::test]
async fn into_summary_returns_live_keys() -> Result<(), Box<dyn std::error::Error>> {
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

    let footer: IncrementalScanSummary = stream.into_summary()?;
    assert_eq!(footer.base_version, 0);
    assert_eq!(footer.target_version, 2);
    assert_eq!(footer.live_adds, HashSet::from([key("B"), key("C")]),);
    assert_eq!(footer.removes, HashSet::from([key("A")]));

    Ok(())
}

// The iterator yields one batch per source commit that produced live Adds, in
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

    // v4 produced the live add(C); v3 had only Removes (no add item);
    // v2 and v1 had their adds cancelled by v3 (no add items). So exactly one batch.
    assert_eq!(
        batches.len(),
        1,
        "expected one yielded batch (v4 only); v3 has no Adds, v1/v2 cancelled"
    );
    let live: usize = batches[0].selection_vector().iter().filter(|s| **s).count();
    assert_eq!(live, 1, "the single yielded batch contains add(C)");

    let footer = stream.into_summary()?;
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
        live_add_count(&listing),
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
                .with_batch_size(NonZero::new(batch_size).expect("batch_size is non-zero")),
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
// must produce the exact same live Add and Remove path sets as the unsplit case
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

    // 7 live Adds: a, b, c, e, g (plain) plus d and f (the new DV-bearing copies).
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
        listing.summary.live_adds, expected_adds,
        "live_adds must not depend on batch_size (got batch_size={batch_size})"
    );
    assert_eq!(
        listing.summary.removes, expected_removes,
        "removes must not depend on batch_size (got batch_size={batch_size})"
    );
    assert_eq!(
        live_add_count(&listing),
        7,
        "selection-vector counts must not depend on batch_size (got batch_size={batch_size})"
    );

    Ok(())
}

// === Classification ===
//
// `into_summary_against_base_iter(base_keys)` / `into_listing_against_base_iter(base_keys)`
// intersect the consumer's base file keys (`(path, dv_unique_id)`) against the live
// Adds to surface metadata-only re-adds in `duplicate_adds`. The Add row itself stays in
// the streamed Adds; the key is also surfaced separately so the consumer can mask the
// stale base entry.

fn unwrap_classified_listing<'a>(
    result: Option<IncrementalScanStream>,
    base_keys: impl IntoIterator<Item = &'a FileActionKey>,
) -> IncrementalListingAgainstBase {
    result
        .expect("expected Some(stream), got None (commits unavailable)")
        .into_listing_against_base_iter(base_keys)
        .expect("into_listing_against_base_iter succeeded")
}

fn classified_add_count(listing: &IncrementalListingAgainstBase) -> usize {
    listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum()
}

#[rstest]
#[case::all_re_adds(
    vec!["re-added.parquet"],
    vec!["re-added.parquet"],
    1,
    vec!["re-added.parquet"],
)]
#[case::mixed_new_and_re_added(
    vec!["brand-new.parquet", "re-added.parquet"],
    vec!["re-added.parquet"],
    2,
    vec!["re-added.parquet"],
)]
#[tokio::test]
async fn classifies_metadata_only_re_adds(
    #[case] range_adds: Vec<&'static str>,
    #[case] base_paths: Vec<&'static str>,
    #[case] expected_live: usize,
    #[case] expected_duplicates: Vec<&'static str>,
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
    let actions: Vec<TestAction> = range_adds
        .iter()
        .map(|p| TestAction::Add(p.to_string()))
        .collect();
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;
    let base_keys: Vec<FileActionKey> = base_paths.iter().map(|p| key(p)).collect();
    let listing = unwrap_classified_listing(
        target.incremental_scan_builder(0).build(engine.as_ref())?,
        &base_keys,
    );

    assert_eq!(classified_add_count(&listing), expected_live);
    let expected: HashSet<FileActionKey> = expected_duplicates.iter().map(|p| key(p)).collect();
    assert_eq!(listing.summary.duplicate_adds, expected);
    assert!(listing.summary.removes.is_empty());

    Ok(())
}

// The pull-then-finalize flow: drive the iterator manually with `next()`, then call
// `into_summary_against_base_iter(base_keys)`. This is the documented streaming consumer
// pattern.
#[tokio::test]
async fn into_summary_after_manual_streaming_classifies_duplicates(
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

    let mut stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");

    let mut yielded = 0;
    for item in stream.by_ref() {
        let _batch = item?;
        yielded += 1;
    }
    assert_eq!(yielded, 1, "v1 produced one Add batch");

    let base_keys = [key("re-added.parquet"), key("not-in-range.parquet")];
    let summary = stream.into_summary_against_base_iter(&base_keys)?;
    assert_eq!(
        summary.duplicate_adds,
        HashSet::from([key("re-added.parquet")]),
        "non-matching base key is filtered out"
    );
    assert!(summary.removes.is_empty());

    Ok(())
}

// Predicate variants (`*_with`) take a closure and produce the same `duplicate_adds`
// as the iterator variants. Asserts that for a fixed (range, base) pair both forms
// agree on the classified output. Demonstrates HashMap-backed base via closure.
#[tokio::test]
async fn predicate_variants_match_iterator_variants() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;

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

    // Base lives in a HashMap (key -> metadata). Closure does the contains-check.
    let base_index: HashMap<FileActionKey, i64> = [
        (key("re-added.parquet"), 100),
        (key("not-in-range.parquet"), 200),
    ]
    .into();

    let stream = target
        .clone()
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");
    let summary_with = stream.into_summary_against_base_closure(|k| base_index.contains_key(k))?;
    assert_eq!(
        summary_with.duplicate_adds,
        HashSet::from([key("re-added.parquet")]),
    );
    assert!(summary_with.removes.is_empty());

    let stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");
    let listing_with = stream.into_listing_against_base_closure(|k| base_index.contains_key(k))?;
    assert_eq!(
        listing_with.summary.duplicate_adds,
        summary_with.duplicate_adds
    );
    assert_eq!(listing_with.summary.removes, summary_with.removes);
    assert_eq!(classified_add_count(&listing_with), 2);

    Ok(())
}

// Empty base: `duplicate_adds` is empty (nothing in the range overlaps the empty base),
// but `removes` still surfaces every range remove. Exercises both against-base forms.
#[tokio::test]
async fn empty_base_keys_produces_no_duplicates() -> Result<(), Box<dyn std::error::Error>> {
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
            TestAction::Add("a.parquet".to_string()),
            TestAction::Remove("gone.parquet".to_string()),
        ]),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    // By-ref iterator with empty input.
    let stream = target
        .clone()
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");
    let empty: [FileActionKey; 0] = [];
    let summary_iter = stream.into_summary_against_base_iter(&empty)?;
    assert!(summary_iter.duplicate_adds.is_empty());
    assert_eq!(summary_iter.removes, HashSet::from([key("gone.parquet")]));

    // Predicate closure that always returns false.
    let stream = target
        .incremental_scan_builder(0)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");
    let summary_with = stream.into_summary_against_base_closure(|_| false)?;
    assert!(summary_with.duplicate_adds.is_empty());
    assert_eq!(summary_with.removes, HashSet::from([key("gone.parquet")]));

    Ok(())
}

// === Predicate pushdown ===
//
// `with_predicate` prunes streamed live Adds against file stats using the same
// `DataSkippingFilter` the read path uses. These tests use raw commit JSON so each Add can
// carry a distinct `id` min/max range. The metadata (`test_utils::METADATA`) declares an
// `id: integer` column, so `id`-predicates are eligible for data skipping. Skipping is
// conservative: an Add is dropped only when its stats prove no row matches; Adds with no
// stats and all Removes are always kept.

// Build a raw Add action carrying `id` stats over the closed range `[id_min, id_max]`.
fn add_with_id_stats(path: &str, id_min: i32, id_max: i32) -> String {
    let stats = format!(
        "{{\\\"numRecords\\\":10,\\\"nullCount\\\":{{\\\"id\\\":0}},\
         \\\"minValues\\\":{{\\\"id\\\":{id_min}}},\\\"maxValues\\\":{{\\\"id\\\":{id_max}}}}}"
    );
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{stats}\"}}}}"
    )
}

// Build a raw Add action with no stats (`stats` omitted). Data skipping must always keep it.
fn add_without_stats(path: &str) -> String {
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true}}}}"
    )
}

// Build a raw Remove action.
fn remove_action(path: &str) -> String {
    format!(
        "{{\"remove\":{{\"path\":\"{path}\",\"deletionTimestamp\":1700000000000,\
         \"dataChange\":true}}}}"
    )
}

// Build a raw Add action with a partition value for the `val` partition column and no stats.
fn add_partitioned(path: &str, val: &str) -> String {
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{\"val\":\"{val}\"}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true}}}}"
    )
}

// Build a raw Add carrying both a `val` partition value and `id` stats over `[id_min, id_max]`,
// for predicates that touch both a partition and a data column.
fn add_partitioned_with_id_stats(path: &str, val: &str, id_min: i32, id_max: i32) -> String {
    let stats = format!(
        "{{\\\"numRecords\\\":10,\\\"nullCount\\\":{{\\\"id\\\":0}},\
         \\\"minValues\\\":{{\\\"id\\\":{id_min}}},\\\"maxValues\\\":{{\\\"id\\\":{id_max}}}}}"
    );
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{\"val\":\"{val}\"}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{stats}\"}}}}"
    )
}

// Column-mapping (name mode) metadata whose `id` column maps to the physical name `col-id`.
const COLUMN_MAPPING_METADATA: &str = "{\"metaData\":{\"id\":\"cm-test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{\\\"delta.columnMapping.id\\\":1,\\\"delta.columnMapping.physicalName\\\":\\\"col-id\\\"}}]}\",\"partitionColumns\":[],\"configuration\":{\"delta.columnMapping.mode\":\"name\",\"delta.columnMapping.maxColumnId\":\"1\"},\"createdTime\":1700000000000}}\n{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"columnMapping\"],\"writerFeatures\":[\"columnMapping\"]}}";

// Build a raw Add whose stats are keyed by the physical column name `col-id` (column mapping).
fn add_with_physical_id_stats(path: &str, id_min: i32, id_max: i32) -> String {
    let stats = format!(
        "{{\\\"numRecords\\\":10,\\\"nullCount\\\":{{\\\"col-id\\\":0}},\
         \\\"minValues\\\":{{\\\"col-id\\\":{id_min}}},\\\"maxValues\\\":{{\\\"col-id\\\":{id_max}}}}}"
    );
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{stats}\"}}}}"
    )
}

// Metadata declaring an `id: integer` column AND the deletionVectors feature, so a DV-bearing
// Add carrying `id` stats parses and an `id`-predicate is skipping-eligible.
const DV_AND_ID_METADATA: &str = "{\"metaData\":{\"id\":\"dv-id-test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}";

// Build a raw DV-bearing Add carrying `id` stats over `[id_min, id_max]`.
fn add_with_dv_and_id_stats(
    path: &str,
    storage_type: &str,
    path_or_inline: &str,
    offset: i32,
    id_min: i32,
    id_max: i32,
) -> String {
    let stats = format!(
        "{{\\\"numRecords\\\":10,\\\"nullCount\\\":{{\\\"id\\\":0}},\
         \\\"minValues\\\":{{\\\"id\\\":{id_min}}},\\\"maxValues\\\":{{\\\"id\\\":{id_max}}}}}"
    );
    format!(
        "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\
         \"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{stats}\",\
         \"deletionVector\":{{\"storageType\":\"{storage_type}\",\"pathOrInlineDv\":\"{path_or_inline}\",\
         \"offset\":{offset},\"sizeInBytes\":10,\"cardinality\":1}}}}}}"
    )
}

// Pushdown drops added files whose stats prove they cannot match and keeps the rest; the
// `None` predicate keeps every live Add unchanged. Three files span disjoint `id` ranges over
// a single commit. Each case asserts the surviving key set and that the emitted selection
// vector agrees with `live_adds` (no summary/stream drift).
#[rstest]
#[case::pushdown(
    Some(Arc::new(column_expr!("id").gt(Expr::literal(25i32))) as PredicateRef),
    vec!["high.parquet"],
)]
#[case::no_predicate(None, vec!["low.parquet", "mid.parquet", "high.parquet"])]
#[tokio::test]
async fn pushdown_keeps_only_matching_added_files(
    #[case] predicate: Option<PredicateRef>,
    #[case] expected_live: Vec<&str>,
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
    // low: id in [0, 9]; mid: id in [10, 19]; high: id in [20, 30]. `id > 25` can only match
    // high; low and mid are provably excluded. No predicate keeps all three.
    let v1 = [
        add_with_id_stats("low.parquet", 0, 9),
        add_with_id_stats("mid.parquet", 10, 19),
        add_with_id_stats("high.parquet", 20, 30),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    let expected: HashSet<FileActionKey> = expected_live.iter().map(|p| key(p)).collect();
    assert_eq!(listing.summary.live_adds, expected);
    // The streamed selection vector must agree with the summary's live_adds count.
    assert_eq!(live_add_count(&listing), expected.len());
    assert!(listing.summary.removes.is_empty());

    Ok(())
}

// A file with no stats is always kept, even under a predicate whose range would exclude it if
// stats were present. Conservative skipping never drops a stats-less file.
#[tokio::test]
async fn added_file_without_stats_is_always_kept() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    // no_stats has no stats; in_range has id in [20, 30]. Both must survive `id > 25`:
    // no_stats because it can't be disproven, in_range because [20, 30] overlaps.
    let v1 = [
        add_without_stats("no_stats.parquet"),
        add_with_id_stats("in_range.parquet", 20, 30),
        add_with_id_stats("out_of_range.parquet", 0, 5),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("no_stats.parquet"), key("in_range.parquet")]),
        "the stats-less file is kept; only the provably-excluded file is dropped"
    );

    Ok(())
}

// Removes are never filtered by the predicate. A removed file whose stat range the predicate
// excludes must still appear in `removes` in full -- the consumer needs every removal to evict
// stale cached entries.
#[tokio::test]
async fn removes_are_never_filtered_by_predicate() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    // v1 adds gone.parquet (id in [0, 5], which `id > 25` would exclude).
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        add_with_id_stats("gone.parquet", 0, 5),
    )
    .await?;
    // v2 removes gone.parquet and adds kept.parquet (id in [20, 30]).
    let v2 = [
        remove_action("gone.parquet"),
        add_with_id_stats("kept.parquet", 20, 30),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 2, v2).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    // The Remove is reported in full even though the predicate excludes its stat range.
    assert_eq!(
        listing.summary.removes,
        HashSet::from([key("gone.parquet")]),
        "removes must never be filtered by the predicate"
    );
    // Only kept.parquet ([20, 30]) survives among the live Adds.
    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("kept.parquet")])
    );

    Ok(())
}

// A predicate that statically excludes every file (`id > 25 AND FALSE`) yields an empty live-Add
// stream, but Removes are still reported. A tautology (`id > 25 OR TRUE`) is useless for
// skipping and behaves like no predicate.
#[rstest]
#[case::static_skip_all(Pred::and(column_expr!("id").gt(Expr::literal(25i32)), Pred::literal(false)), 0)]
#[case::tautology(Pred::or(column_expr!("id").gt(Expr::literal(25i32)), Pred::literal(true)), 2)]
#[tokio::test]
async fn static_skip_all_and_tautology(
    #[case] predicate: Pred,
    #[case] expected_live: usize,
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
    // v1 adds two files and removes a third, so removes is non-empty regardless of the predicate.
    let v1 = [
        add_with_id_stats("a.parquet", 0, 9),
        add_with_id_stats("b.parquet", 20, 30),
        remove_action("removed.parquet"),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(Arc::new(predicate))
            .build(engine.as_ref())?,
    );

    assert_eq!(live_add_count(&listing), expected_live);
    // Removes are reported regardless of the Add-side skipping strategy.
    assert_eq!(
        listing.summary.removes,
        HashSet::from([key("removed.parquet")]),
        "removes are reported even when all Adds are statically skipped"
    );

    Ok(())
}

// Consumer classification (`into_summary_against_base_*`) composes with the predicate: base
// intersection runs over the predicate-pruned live Adds, so a base key whose file the
// predicate dropped is not reported as a duplicate.
#[tokio::test]
async fn against_base_classification_composes_with_predicate(
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
    // matches.parquet (id in [20, 30]) survives `id > 25`; dropped.parquet (id in [0, 5]) does not.
    let v1 = [
        add_with_id_stats("matches.parquet", 20, 30),
        add_with_id_stats("dropped.parquet", 0, 5),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    // Both files are in the consumer's base. Only the predicate-surviving one should classify
    // as a duplicate; the dropped file was pruned before base intersection.
    let base_keys = [key("matches.parquet"), key("dropped.parquet")];
    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let stream = target
        .incremental_scan_builder(0)
        .with_predicate(predicate)
        .build(engine.as_ref())?
        .expect("expected Some(stream)");
    let summary = stream.into_summary_against_base_iter(&base_keys)?;

    assert_eq!(
        summary.duplicate_adds,
        HashSet::from([key("matches.parquet")]),
        "the pruned file is not classified as a duplicate"
    );
    assert!(summary.removes.is_empty());

    Ok(())
}

// Newest-wins holds when the newest Add of a key is pruned but an older Add of the same key
// (with a matching range) exists earlier in the range. The pruned newest Add still records the
// key as `seen`, so the older duplicate must not leak into `live_adds`. The file's live (newest)
// metadata does not match the predicate, so a cold scan at the target would exclude it too --
// net zero live Adds. Guards the "record seen regardless of the mask" invariant.
#[tokio::test]
async fn pruned_newest_add_suppresses_older_matching_duplicate(
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
    // v1: dupe.parquet with a range that WOULD match `id > 25`.
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        add_with_id_stats("dupe.parquet", 20, 30),
    )
    .await?;
    // v2: dupe.parquet re-added with a range the predicate provably excludes ([0, 5]).
    add_commit(
        table_root,
        storage.as_ref(),
        2,
        add_with_id_stats("dupe.parquet", 0, 5),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert!(
        listing.summary.live_adds.is_empty(),
        "the pruned newest Add records `seen`, so the older matching duplicate must not leak"
    );
    assert_eq!(live_add_count(&listing), 0);

    Ok(())
}

// A predicate referencing a column absent from the table schema fails at `build` (fail-fast),
// not lazily mid-stream, matching the documented `# Errors` contract.
#[tokio::test]
async fn unknown_column_predicate_fails_at_build() -> Result<(), Box<dyn std::error::Error>> {
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
        add_with_id_stats("a.parquet", 0, 9),
    )
    .await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("nonexistent").gt(Expr::literal(1i32)));
    let result = target
        .incremental_scan_builder(0)
        .with_predicate(predicate)
        .build(engine.as_ref());

    // Fail-fast at build, surfacing the unresolved column. The concrete error is
    // `Error::MissingColumn`, but a captured backtrace wraps it in `Error::Backtraced`, so match
    // on the message rather than the variant.
    assert_result_error_with_message(result, "unknown column: nonexistent");

    Ok(())
}

// A predicate over a partition column prunes added files by their partition value: values are
// parsed from the raw `add.partitionValues` map via `map_to_struct`, so `val = 'x'` drops the
// file in partition `y`.
#[tokio::test]
async fn partition_column_predicate_prunes_added_files() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // METADATA_WITH_PARTITION_COLS partitions by `val`.
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_partitioned(vec![TestAction::Metadata]),
    )
    .await?;
    let v1 = [
        add_partitioned("a.parquet", "x"),
        add_partitioned("b.parquet", "y"),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("val").eq(Expr::literal("x")));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("a.parquet")]),
        "only the file in partition `val = 'x'` survives"
    );

    Ok(())
}

// A mixed predicate `id > 25 AND val = 'x'` prunes on both axes: the data conjunct skips on
// `add.stats`, the partition conjunct skips on `add.partitionValues`. Only the file satisfying
// both survives; a file failing either is dropped.
#[tokio::test]
async fn mixed_data_and_partition_predicate_prunes_on_both(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_partitioned(vec![TestAction::Metadata]),
    )
    .await?;
    // match: id in [20,30] and val=x. wrong_val: in-range id but val=y. wrong_id: val=x but
    // id in [0,5]. Only `match` satisfies `id > 25 AND val = 'x'`.
    let v1 = [
        add_partitioned_with_id_stats("match.parquet", "x", 20, 30),
        add_partitioned_with_id_stats("wrong_val.parquet", "y", 20, 30),
        add_partitioned_with_id_stats("wrong_id.parquet", "x", 0, 5),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(Pred::and(
        column_expr!("id").gt(Expr::literal(25i32)),
        column_expr!("val").eq(Expr::literal("x")),
    ));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("match.parquet")]),
        "only the file matching both the data and partition conjuncts survives"
    );

    Ok(())
}

// Under column mapping, `with_predicate` lowers the logical predicate column to its physical
// name before reading stats keyed by that physical name. A logical `id > 25` skips against the
// physical-name stats, so only the in-range file survives -- proving the physical-vs-logical
// lowering on this path.
#[tokio::test]
async fn pushdown_resolves_column_mapping_physical_names() -> Result<(), Box<dyn std::error::Error>>
{
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        COLUMN_MAPPING_METADATA.to_string(),
    )
    .await?;
    // Stats are keyed by the physical name `col-id`, while the predicate references logical `id`.
    let v1 = [
        add_with_physical_id_stats("low.parquet", 0, 5),
        add_with_physical_id_stats("high.parquet", 20, 30),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("high.parquet")]),
        "predicate on logical `id` resolves to physical `col-id` stats and drops the low file"
    );

    Ok(())
}

// A DV-bearing Add is pruned by its stats like any other, and the surviving live Add is keyed by
// its full `(path, dv_unique_id)`, not path alone. Two DV-bearing Adds with disjoint `id` ranges:
// `id > 25` drops the out-of-range one and keeps the in-range one under its DV key.
#[tokio::test]
async fn pushdown_prunes_dv_bearing_adds_keyed_by_dv() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        DV_AND_ID_METADATA.to_string(),
    )
    .await?;
    let v1 = [
        add_with_dv_and_id_stats("keep.parquet", "u", "abc", 1, 20, 30),
        add_with_dv_and_id_stats("prune.parquet", "u", "xyz", 2, 0, 5),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(25i32)));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key_with_dv("keep.parquet", "uabc@1")]),
        "the in-range DV Add survives keyed by its full (path, dv_unique_id)"
    );

    Ok(())
}

// A predicate that references no columns and is not statically false (`lit(true)`) resolves to
// `PhysicalPredicate::None`, which folds to keep-all. Distinct from the tautology case (which
// references `id` and keeps all via the filter), this exercises the `None -> KeepAll` arm.
#[tokio::test]
async fn column_free_predicate_keeps_all_added_files() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    let v1 = [
        add_with_id_stats("a.parquet", 0, 9),
        add_with_id_stats("b.parquet", 20, 30),
    ]
    .join("\n");
    add_commit(table_root, storage.as_ref(), 1, v1).await?;

    let target = Snapshot::builder_for(table_url)
        .at_version(1)
        .build(engine.as_ref())?;

    let predicate: PredicateRef = Arc::new(Pred::literal(true));
    let listing = unwrap_listing(
        target
            .incremental_scan_builder(0)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );

    assert_eq!(
        listing.summary.live_adds,
        HashSet::from([key("a.parquet"), key("b.parquet")]),
        "a column-free predicate is ineligible for skipping and keeps every live Add"
    );

    Ok(())
}

// === Round-trip against a full scan ===
//
// The correctness promise of `with_predicate` is that the incremental live-Add set, reconciled
// against the base, reproduces what a full `Scan` at the target with the same predicate keeps.
// The two paths parse stats differently: a full scan reads pre-parsed `stats_parsed` (from a
// checkpoint), while the incremental path parses `add.stats` JSON via `for_raw_action_batch`.
//
// The incremental scan reads only raw commit JSON and never touches checkpoints, so it cannot
// serve a range predating a checkpoint (the target snapshot's log segment drops pre-checkpoint
// commit JSON). The checkpoint therefore sits at the base version: the full scan at the base
// prunes via the checkpoint's `stats_parsed`, the incremental delta over `(base, target]` prunes
// via JSON, and `full_scan(base) union live_adds` must equal `full_scan(target)`.

type MtEngine = DefaultEngine<TokioMultiThreadExecutor>;

// Single-column `id: integer` table.
fn id_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        "id",
        DataType::INTEGER,
    )]))
}

// One `RecordBatch` holding a single `id` value, so each written file gets a tight [id, id] range.
fn id_batch(schema: &Arc<StructType>, id: i32) -> RecordBatch {
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();
    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int32Array::from(vec![id]))],
    )
    .unwrap()
}

// Surviving file paths from a full scan at `snapshot` under `predicate`. A snapshot loaded after a
// checkpoint prunes using the checkpoint's `stats_parsed`.
fn full_scan_surviving_paths(
    snapshot: SnapshotRef,
    engine: &MtEngine,
    predicate: PredicateRef,
) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    fn insert_path(paths: &mut HashSet<String>, scan_file: ScanFile) {
        paths.insert(scan_file.path);
    }

    let scan = snapshot.scan_builder().with_predicate(predicate).build()?;
    let mut paths: HashSet<String> = HashSet::new();
    for sm in scan.scan_metadata(engine)? {
        paths = sm?.visit_scan_files(paths, insert_path)?;
    }
    Ok(paths)
}

// Reconciling a base full scan with the incremental delta reproduces a full scan at the target,
// even though the base prunes via the checkpoint's `stats_parsed` while the incremental delta
// prunes via `add.stats` JSON. Guards the stats-parsing divergence the two paths could exhibit.
#[tokio::test(flavor = "multi_thread")]
async fn predicate_pushdown_reconciled_with_base_matches_full_scan_across_checkpoint(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).map_err(|_| "invalid table path")?;
    let schema = id_schema();
    let mut snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    // Files added before the checkpoint: id 5, 15, 25. The checkpoint captures their stats.
    for id in [5, 15, 25] {
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            id_batch(&schema, id),
            HashMap::new(),
        )
        .await?;
    }
    let (_, snapshot) = snapshot.checkpoint(engine.as_ref(), None)?;
    let base_version = snapshot.version();

    // Files added after the checkpoint: id 35, 45. These live only in raw commit JSON.
    let mut snapshot = snapshot;
    for id in [35, 45] {
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            id_batch(&schema, id),
            HashMap::new(),
        )
        .await?;
    }
    let target_version = snapshot.version();

    let predicate: PredicateRef = Arc::new(column_expr!("id").gt(Expr::literal(20i32)));

    // Base full scan (reads the checkpoint's `stats_parsed`) and target full scan, both loaded
    // fresh off disk so they replay through the checkpoint.
    let base = Snapshot::builder_for(table_url.clone())
        .at_version(base_version)
        .build(engine.as_ref())?;
    let base_paths = full_scan_surviving_paths(base, engine.as_ref(), predicate.clone())?;

    let target = Snapshot::builder_for(table_url)
        .at_version(target_version)
        .build(engine.as_ref())?;
    let target_paths = full_scan_surviving_paths(target, engine.as_ref(), predicate.clone())?;

    // Incremental delta over `(base, target]` (parses `add.stats` JSON).
    let listing = unwrap_listing(
        snapshot
            .incremental_scan_builder(base_version)
            .with_predicate(predicate)
            .build(engine.as_ref())?,
    );
    let live_adds: HashSet<String> = listing
        .summary
        .live_adds
        .iter()
        .map(|k| k.path().to_string())
        .collect();

    // Sanity: the checkpoint kept a pre-checkpoint match (id 25) that the incremental delta does
    // not re-report, so the reconciliation is doing real work.
    assert!(
        !base_paths.is_empty() && !live_adds.is_empty(),
        "both the base scan and the incremental delta should contribute matches"
    );
    assert!(
        listing.summary.removes.is_empty(),
        "no removes in this range"
    );

    // No in-range removes, so reconciliation is a union of the base and the incremental live Adds.
    let reconciled: HashSet<String> = base_paths.union(&live_adds).cloned().collect();
    assert_eq!(
        reconciled, target_paths,
        "base full scan plus incremental live_adds must equal the full scan at the target"
    );

    Ok(())
}
