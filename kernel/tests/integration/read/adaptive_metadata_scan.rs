//! Read-path scan dispatch for adaptiveMetadata (AMT) tables: when the table has a `checkpoint`
//! action, a scan reads its content-tree root manifest instead of classic Add/Remove log replay.
//!
//! These tests exercise the dispatch and its MVP guard rejections (which fire before the root
//! manifest is read) plus the fall-through to classic replay. A successful end-to-end scan is not
//! covered here: it needs a real root manifest Parquet built from the crate-private content-tree
//! entry type, which no public API exposes yet (the reader itself is covered by a kernel unit
//! test).
#![cfg(feature = "adaptive-metadata-in-dev")]

use delta_kernel::schema::schema_ref;
use delta_kernel::{DeltaResult, Engine, FileMeta, Snapshot};
use tempfile::TempDir;
use test_utils::{begin_transaction, create_table_with_column_mapping_mode, engine_store_setup};
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

/// Creates a file-backed `adaptiveMetadata-preview` table (with its full dependency chain, column
/// mapping in `id` mode) at version 0. The returned [`TempDir`] must be kept alive for the table's
/// lifetime.
async fn setup_adaptive_metadata_table(
    table_name: &str,
    partition_columns: &[&str],
) -> Result<(impl Engine, TempDir, Url), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, engine, table_url) = engine_store_setup(table_name, Some(&dir_url));
    let schema = schema_ref! { nullable "id": INTEGER, nullable "part": INTEGER };

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

    Ok((engine, temp_dir, table_url))
}

/// A `FileMeta` referencing a (not necessarily existing) root manifest under the table's metadata
/// directory; the guard tests reject before this file is read.
fn root_manifest_file(table_url: &Url) -> FileMeta {
    FileMeta {
        location: table_url.join("metadata/root.parquet").unwrap(),
        last_modified: 0,
        size: 1024,
    }
}

/// The number of files a scan would read (selected rows across all metadata batches).
fn scan_file_count(
    scan_metadata: impl Iterator<Item = DeltaResult<delta_kernel::scan::ScanMetadata>>,
) -> DeltaResult<usize> {
    scan_metadata
        .map(|m| {
            Ok(m?
                .scan_files
                .selection_vector()
                .iter()
                .filter(|s| **s)
                .count())
        })
        .sum()
}

// An AMT table with no `checkpoint` action falls through to classic replay, which reads ordinary
// add/remove actions; a freshly created table has none, so the scan yields zero files.
#[tokio::test(flavor = "multi_thread")]
async fn scan_without_checkpoint_action_falls_back_to_classic_replay(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url) =
        setup_adaptive_metadata_table("amt_scan_no_checkpoint", &[]).await?;
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

    let scan = snapshot.scan_builder().build()?;
    assert_eq!(scan_file_count(scan.scan_metadata(&engine)?)?, 0);
    Ok(())
}

// Partitioned AMT tables are not yet supported by the content-tree reader (it emits empty partition
// values), so a scan of one that has a checkpoint action is rejected.
#[tokio::test(flavor = "multi_thread")]
async fn scan_rejects_partitioned_amt_table() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url) =
        setup_adaptive_metadata_table("amt_scan_partitioned", &["part"]).await?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(root_manifest_file(&table_url))?
        .commit(&engine)?
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let err = scan
        .scan_metadata(&engine)
        .err()
        .expect("scan of a partitioned AMT table should be rejected");
    assert!(
        err.to_string()
            .contains("does not yet support partitioned tables"),
        "unexpected error: {err}"
    );
    Ok(())
}

// AMT data files live only in the manifest tree, so a checkpoint older than the snapshot (commits
// exist after it) cannot be served by classic replay; the scan is rejected rather than silently
// dropping the pre-checkpoint files.
#[tokio::test(flavor = "multi_thread")]
async fn scan_rejects_checkpoint_older_than_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url) =
        setup_adaptive_metadata_table("amt_scan_stale_checkpoint", &[]).await?;

    // v1: commit the root manifest (checkpoint version 1).
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let snapshot = begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(root_manifest_file(&table_url))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();

    // v2: a plain domain-metadata commit, so the snapshot advances past the checkpoint version.
    begin_transaction(snapshot, &engine)?
        .with_domain_metadata("test.domain".into(), "{}".into())
        .commit(&engine)?
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let scan = snapshot.scan_builder().build()?;
    let err = scan
        .scan_metadata(&engine)
        .err()
        .expect("scan with a stale checkpoint should be rejected");
    assert!(
        err.to_string().contains("equal the snapshot version"),
        "unexpected error: {err}"
    );
    Ok(())
}
