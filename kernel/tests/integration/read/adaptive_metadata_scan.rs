//! Integration coverage for the adaptiveMetadata content-tree scan dispatch: a table whose live
//! files are described by a checkpoint `contentRoot` is read through
//! [`Scan::scan_metadata`](delta_kernel::scan::Scan::scan_metadata). These exercise the dispatch
//! guards that fire before any manifest is read (partitioned rejection, checkpoint-vs-snapshot
//! version guard), and confirm the feature gate leaves classic tables on the normal path.
#![cfg(feature = "adaptive-metadata-in-dev")]

use delta_kernel::schema::{schema_ref, SchemaRef};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, FileMeta};
use tempfile::TempDir;
use test_utils::{
    begin_transaction, create_table, create_table_with_column_mapping_mode, engine_store_setup,
};
use url::Url;

const READER_FEATURES: &[&str] = &[
    "columnMapping",
    "deletionVectors",
    "adaptiveMetadata-preview",
];
const WRITER_FEATURES: &[&str] = &[
    "columnMapping",
    "deletionVectors",
    "rowTracking",
    "domainMetadata",
    "inCommitTimestamp",
    "adaptiveMetadata-preview",
];

/// Creates a file-backed adaptiveMetadata table with `schema`/`partition_columns` and commits a
/// root manifest at version 1, so the latest `checkpoint` action's `contentRoot` describes the
/// table's live files. Returns the engine, the kept-alive temp dir, and the table url.
async fn setup_amt_table_with_root_manifest(
    table_name: &str,
    schema: SchemaRef,
    partition_columns: &[&str],
) -> Result<(impl Engine, TempDir, Url), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, engine, table_url) = engine_store_setup(table_name, Some(&dir_url));
    create_table_with_column_mapping_mode(
        store,
        table_url.clone(),
        schema,
        partition_columns,
        true,
        READER_FEATURES.to_vec(),
        WRITER_FEATURES.to_vec(),
        "id",
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(file)?
        .commit(&engine)?
        .unwrap_committed();
    Ok((engine, temp_dir, table_url))
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_partitioned_adaptive_metadata_table_is_rejected(
) -> Result<(), Box<dyn std::error::Error>> {
    // Partition values are not yet read from the content tree, so a partitioned AMT table must be
    // rejected rather than scanned with empty partition values.
    let schema = schema_ref! { nullable "id": INTEGER, nullable "part": INTEGER };
    let (engine, _temp_dir, table_url) =
        setup_amt_table_with_root_manifest("amt_scan_partitioned", schema, &["part"]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let err = scan
        .scan_metadata(&engine)
        .err()
        .expect("scanning a partitioned adaptiveMetadata table should be rejected");
    assert!(
        err.to_string().contains("partitioned"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_adaptive_metadata_table_with_stale_checkpoint_is_rejected(
) -> Result<(), Box<dyn std::error::Error>> {
    // The checkpoint's contentRoot must cover the snapshot version: post-checkpoint commit replay
    // is not wired in, so a snapshot ahead of the latest checkpoint action must be rejected.
    let schema = schema_ref! { nullable "id": INTEGER };
    let (engine, _temp_dir, table_url) =
        setup_amt_table_with_root_manifest("amt_scan_stale_checkpoint", schema, &[]).await?;

    // Commit a plain domain-metadata action at version 2 (no new checkpoint action), leaving the
    // latest checkpoint action at version 1 while the snapshot advances to version 2.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    begin_transaction(snapshot, &engine)?
        .with_domain_metadata("my.domain".to_string(), "v2".to_string())
        .commit(&engine)?
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let scan = snapshot.scan_builder().build()?;
    let err = scan
        .scan_metadata(&engine)
        .err()
        .expect("scanning past a stale checkpoint should be rejected");
    assert!(
        err.to_string().contains("cover the snapshot version"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_non_adaptive_metadata_table_uses_normal_replay(
) -> Result<(), Box<dyn std::error::Error>> {
    // A table without the adaptiveMetadata feature must skip the content-tree dispatch entirely and
    // replay the log normally (an empty table yields no scan metadata, but no error).
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, engine, table_url) =
        engine_store_setup("amt_scan_classic_fallthrough", Some(&dir_url));
    let schema = schema_ref! { nullable "id": INTEGER };
    create_table(store, table_url.clone(), schema, &[], true, vec![], vec![]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let count = scan.scan_metadata(&engine)?.count();
    assert_eq!(count, 0);
    Ok(())
}
