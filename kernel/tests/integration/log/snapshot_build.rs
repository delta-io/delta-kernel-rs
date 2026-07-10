//! Integration tests for [`Snapshot`] build semantics.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array};
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{
    CheckpointWriteResult, ChecksumWriteResult, IncrementalReplay, SnapshotBuilder,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot, Version};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{insert_data_with, test_table_setup_mt, TestCatalogCommitter};

#[derive(Debug, Clone, Copy)]
enum TableKind {
    FileSystem,
    CatalogManaged,
}

impl TableKind {
    fn committer(self) -> Box<dyn Committer> {
        match self {
            TableKind::FileSystem => Box::new(FileSystemCommitter::new()),
            TableKind::CatalogManaged => Box::new(TestCatalogCommitter),
        }
    }
}

fn maybe_attach_max_catalog_version(
    builder: SnapshotBuilder,
    max_catalog_version: Version,
    kind: TableKind,
) -> SnapshotBuilder {
    match kind {
        TableKind::FileSystem => builder,
        TableKind::CatalogManaged => builder.with_max_catalog_version(max_catalog_version),
    }
}

async fn append_row<E: TaskExecutor>(
    snapshot: Arc<Snapshot>,
    engine: &Arc<DefaultEngine<E>>,
    kind: TableKind,
    value: i32,
) -> DeltaResult<Arc<Snapshot>> {
    let column: ArrayRef = Arc::new(Int32Array::from(vec![value]));
    Ok(insert_data_with(
        snapshot,
        engine,
        vec![column],
        kind.committer(),
        "WRITE",
        true,  /* data_change */
        false, /* is_blind_append */
    )
    .await?
    .unwrap_post_commit_snapshot())
}

/// Builds a table of the given kind at versions 0..=3 (create-table + three appends) and persists
/// a version-0 CRC so a later build with [`IncrementalReplay::Unlimited`] can resolve an in-memory
/// CRC and exercise `write_checksum`. The latest version is 3.
async fn setup_multi_version_table<E: TaskExecutor>(
    engine: &Arc<DefaultEngine<E>>,
    table_path: &str,
    kind: TableKind,
) -> DeltaResult<()> {
    let schema = schema_ref! { nullable "id": INTEGER };
    let builder = create_table(table_path, schema, "test_engine");
    let builder = match kind {
        TableKind::FileSystem => builder,
        TableKind::CatalogManaged => builder.with_table_properties([
            ("delta.feature.catalogManaged", "supported"),
            ("io.unitycatalog.tableId", "snapshot-build-test"),
        ]),
    };
    let create_snapshot = builder
        .build(engine.as_ref(), kind.committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    // The create-table snapshot is not built as latest.
    assert!(!create_snapshot.built_as_latest());
    create_snapshot.write_checksum(engine.as_ref())?;

    let mut snap = maybe_attach_max_catalog_version(Snapshot::builder_for(table_path), 0, kind)
        .build(engine.as_ref())?;
    for value in 1..=3 {
        snap = append_row(snap, engine, kind, value).await?;
    }
    Ok(())
}

/// `built_as_latest` reflects the builder's intent and is carried through the version-preserving
/// derivations `checkpoint` and `write_checksum`. A post-commit snapshot is not latest.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_as_latest_survives_version_preserving_ops(
    #[values(true, false)] built_base_snap_as_latest: bool,
    #[values(TableKind::FileSystem, TableKind::CatalogManaged)] kind: TableKind,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table(&engine, &table_path, kind).await?;

    let mut builder = maybe_attach_max_catalog_version(Snapshot::builder_for(&table_path), 3, kind)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited);
    if !built_base_snap_as_latest {
        builder = builder.at_version(3);
    }
    let base = builder.build(engine.as_ref())?;
    assert_eq!(base.version(), 3);
    assert_eq!(base.built_as_latest(), built_base_snap_as_latest);

    let (ckpt_result, after_checkpoint) = base.checkpoint(engine.as_ref(), None)?;
    assert_eq!(ckpt_result, CheckpointWriteResult::Written);
    assert_eq!(
        after_checkpoint.built_as_latest(),
        built_base_snap_as_latest
    );

    let (crc_result, after_checksum) = after_checkpoint.write_checksum(engine.as_ref())?;
    assert_eq!(crc_result, ChecksumWriteResult::Written);
    assert_eq!(after_checksum.built_as_latest(), built_base_snap_as_latest);

    // A post-commit snapshot was not considered built as latest.
    let post_commit = append_row(base, &engine, kind, 4).await?;
    assert_eq!(post_commit.version(), 4);
    assert!(!post_commit.built_as_latest());

    Ok(())
}

/// A fresh or incremental build is built as latest iff no explicit version is requested.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_as_latest_on_fresh_and_incremental_build(
    #[values(None, Some(1), Some(3))] time_travel_version: Option<Version>,
    #[values(TableKind::FileSystem, TableKind::CatalogManaged)] kind: TableKind,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table(&engine, &table_path, kind).await?;

    // Fresh build: latest iff no explicit version was requested (a time-travel version is not).
    let mut builder = maybe_attach_max_catalog_version(Snapshot::builder_for(&table_path), 3, kind);
    if let Some(v) = time_travel_version {
        builder = builder.at_version(v);
    }
    let base = builder.build(engine.as_ref())?;
    assert_eq!(base.version(), time_travel_version.unwrap_or(3));
    assert_eq!(base.built_as_latest(), time_travel_version.is_none());

    // Incremental build: latest iff no explicit version was requested (a time-travel version is
    // not).
    let refreshed = maybe_attach_max_catalog_version(Snapshot::builder_from(base.clone()), 3, kind)
        .build(engine.as_ref())?;
    assert_eq!(refreshed.version(), 3);
    assert!(refreshed.built_as_latest());

    let pinned = maybe_attach_max_catalog_version(Snapshot::builder_from(base), 3, kind)
        .at_version(3)
        .build(engine.as_ref())?;
    assert_eq!(pinned.version(), 3);
    assert!(!pinned.built_as_latest());

    Ok(())
}
