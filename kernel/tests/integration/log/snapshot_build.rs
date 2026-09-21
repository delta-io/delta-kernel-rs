//! Integration tests for [`Snapshot`] build semantics.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array};
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::schema::{schema, schema_ref};
use delta_kernel::snapshot::{
    CheckpointWriteResult, ChecksumWriteResult, IncrementalReplay, SnapshotBuilder,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Error, Snapshot, Version};
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, assert_result_error_with_message, engine_store_setup, insert_data_with,
    test_table_setup_mt, TestCatalogCommitter,
};

/// The newest version of the table built by [`setup_multi_version_table`].
const LATEST_VERSION: Version = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

fn maybe_attach_max_catalog_version<Mode>(
    builder: SnapshotBuilder<Mode>,
    max_catalog_version: Version,
    kind: TableKind,
) -> SnapshotBuilder<Mode> {
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

    // The create-table snapshot is built as latest (version 0 is necessarily the latest).
    assert!(create_snapshot.is_built_as_latest());
    create_snapshot.write_checksum(engine.as_ref())?;

    let mut snap = maybe_attach_max_catalog_version(Snapshot::builder_for(table_path), 0, kind)
        .build(engine.as_ref())?;
    for value in 1..=LATEST_VERSION as i32 {
        snap = append_row(snap, engine, kind, value).await?;
    }
    Ok(())
}

#[tokio::test]
async fn deeply_nested_schema_snapshot_load_returns_schema_error(
) -> Result<(), Box<dyn std::error::Error>> {
    let deeply_nested_schema = (0..42).fold(
        schema! { nullable "leaf": INTEGER },
        |nested, depth| schema! { nullable (format!("level_{depth}")): (nested) },
    );
    let (store, engine, table_url) = engine_store_setup("deeply_nested_schema", None);
    let commit = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({
            "metaData": {
                "id": "deeply-nested-schema",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&deeply_nested_schema)?,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            }
        }),
    ]
    .map(|action| action.to_string())
    .join("\n");
    add_commit(table_url.as_str(), store.as_ref(), 0, commit).await?;

    let result = Snapshot::builder_for(table_url).build(&engine);
    assert_result_error_with_message(
        result.as_ref(),
        concat!(
            "Schema error: Table schema is too deeply nested: decoding ",
            "metaData.schemaString exceeded serde_json's ",
            "recursion limit: recursion limit exceeded"
        ),
    );
    let error = match result.unwrap_err() {
        Error::Backtraced { source, .. } => *source,
        error => error,
    };
    assert!(matches!(error, Error::Schema(_)));
    Ok(())
}

#[rstest]
#[case::supported(SnapshotLoadProtocolCase {
    min_reader_version: 3,
    reader_features: &["deletionVectors"],
    writer_features: &["deletionVectors"],
    expected_error: None,
})]
#[case::unknown_reader(SnapshotLoadProtocolCase {
    min_reader_version: 3,
    reader_features: &["futureFeature"],
    writer_features: &["futureFeature"],
    expected_error: Some("Feature 'futureFeature' is not supported"),
})]
#[case::mixed_reader(SnapshotLoadProtocolCase {
    min_reader_version: 3,
    reader_features: &["deletionVectors", "futureFeature"],
    writer_features: &["deletionVectors", "futureFeature"],
    expected_error: Some("Feature 'futureFeature' is not supported"),
})]
#[case::unknown_writer_only(SnapshotLoadProtocolCase {
    min_reader_version: 3,
    reader_features: &["deletionVectors"],
    writer_features: &["deletionVectors", "futureFeature"],
    expected_error: None,
})]
#[case::unsupported_writer_only(SnapshotLoadProtocolCase {
    min_reader_version: 3,
    reader_features: &["deletionVectors"],
    writer_features: &["deletionVectors", "generatedColumns"],
    expected_error: None,
})]
#[case::future_reader_version(SnapshotLoadProtocolCase {
    min_reader_version: 4,
    reader_features: &[],
    writer_features: &[],
    expected_error: Some("Unsupported minimum reader version 4"),
})]
#[cfg_attr(
    not(feature = "adaptive-metadata-in-dev"),
    case::adaptive_metadata(SnapshotLoadProtocolCase {
        min_reader_version: 3,
        reader_features: &["adaptiveMetadata-preview"],
        writer_features: &["adaptiveMetadata-preview"],
        expected_error: Some("Feature 'adaptiveMetadata-preview' is not supported"),
    })
)]
#[tokio::test]
async fn snapshot_load_validates_reader_protocol(
    #[case] case: SnapshotLoadProtocolCase,
    #[values(false, true)] incremental: bool,
    #[values(false, true)] time_travel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup("snapshot_feature_validation", None);
    create_table(
        table_url.as_str(),
        schema_ref! { nullable "id": INTEGER },
        "test_engine",
    )
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?
    .unwrap_committed();
    let base = Snapshot::builder_for(table_url.as_str()).build(&engine)?;
    assert_eq!(base.version(), 0);

    // Unsupported protocols cannot be introduced through Kernel's write APIs.
    let mut commit = json!({
        "protocol": {
            "minReaderVersion": case.min_reader_version,
            "minWriterVersion": 7,
            "writerFeatures": case.writer_features,
        }
    });
    if case.min_reader_version == 3 {
        commit["protocol"]["readerFeatures"] = json!(case.reader_features);
    }
    add_commit(table_url.as_str(), store.as_ref(), 1, commit.to_string()).await?;

    let result = match (incremental, time_travel) {
        (false, false) => Snapshot::builder_for(table_url.as_str()).build(&engine),
        (false, true) => Snapshot::builder_for(table_url.as_str())
            .at_version(1)
            .build(&engine),
        (true, false) => Snapshot::builder_from(base).build(&engine),
        (true, true) => Snapshot::builder_from(base).at_version(1).build(&engine),
    };
    if let Some(expected_error) = case.expected_error {
        assert_result_error_with_message(result, expected_error);
    } else {
        assert_eq!(result?.version(), 1);
    }

    let original = Snapshot::builder_for(table_url.as_str())
        .at_version(0)
        .build(&engine)?;
    assert_eq!(original.version(), 0);
    Ok(())
}

#[rstest]
#[case(None, None, false)]
#[case(None, Some(false), false)]
#[case(None, Some(true), false)]
#[case(Some(false), None, false)]
#[case(Some(false), Some(false), false)]
#[case(Some(false), Some(true), false)]
#[case(Some(true), None, false)]
#[case(Some(true), Some(false), false)]
#[case(Some(true), Some(true), true)]
#[tokio::test]
async fn row_tracking_configuration_rejects_only_enabled_and_suspended(
    #[case] enabled: Option<bool>,
    #[case] suspended: Option<bool>,
    #[case] expect_error: bool,
    #[values(false, true)] incremental: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let configuration: HashMap<_, _> = [
        ("delta.enableRowTracking", enabled),
        ("delta.rowTrackingSuspended", suspended),
    ]
    .into_iter()
    .filter_map(|(key, value)| value.map(|value| (key, value.to_string())))
    .collect();

    // === Create version 0 with row-tracking support ===
    let schema = schema_ref! { nullable "value": INTEGER };
    let (store, engine, table_url) = engine_store_setup("row_tracking_configuration", None);
    create_table(&table_url, schema, "test_engine")
        .with_table_properties([("delta.feature.rowTracking", "supported")])
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();

    let base = Snapshot::builder_for(&table_url).build(&engine)?;

    // === Commit the case's enabled and suspended properties at version 1 ===
    let mut metadata = json!({"metaData": base.table_configuration().metadata()});
    metadata["metaData"]["configuration"] = json!(configuration);
    add_commit(table_url.as_str(), store.as_ref(), 1, metadata.to_string()).await?;

    // === Version 1 loads successfully unless row tracking is both enabled and suspended ===
    let result = if incremental {
        Snapshot::builder_from(base).build(&engine)
    } else {
        Snapshot::builder_for(table_url).build(&engine)
    };
    if expect_error {
        assert_result_error_with_message(
            result,
            "Row tracking cannot be enabled and suspended at the same time",
        );
    } else {
        assert_eq!(result.unwrap().version(), 1);
    }
    Ok(())
}

/// The version-preserving derivations `checkpoint`, `write_checksum`, and `publish` inherit the
/// source snapshot's `built_as_latest`, while a post-commit advance is always latest.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_as_latest_is_inherited_by_derived_snapshots(
    #[values(true, false)] base_snap_time_travel_to_latest: bool,
    #[values(TableKind::FileSystem, TableKind::CatalogManaged)] kind: TableKind,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table(&engine, &table_path, kind).await?;

    let mut builder =
        maybe_attach_max_catalog_version(Snapshot::builder_for(&table_path), LATEST_VERSION, kind)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited);
    if base_snap_time_travel_to_latest {
        builder = builder.at_version(LATEST_VERSION);
    }
    let base = builder.build(engine.as_ref())?;
    assert_eq!(base.version(), LATEST_VERSION);
    // A build with no time-travel version is latest; time-travel to the catalog's latest ratified
    // version is also considered built as latest.
    let base_is_latest = !base_snap_time_travel_to_latest || kind == TableKind::CatalogManaged;
    assert_eq!(base.is_built_as_latest(), base_is_latest);

    // checkpoint and write_checksum inherit the base's flag.
    let (ckpt_result, after_checkpoint) = base.checkpoint(engine.as_ref(), None)?;
    assert_eq!(ckpt_result, CheckpointWriteResult::Written);
    assert_eq!(after_checkpoint.is_built_as_latest(), base_is_latest);

    let (crc_result, after_checksum) = after_checkpoint.write_checksum(engine.as_ref())?;
    assert_eq!(crc_result, ChecksumWriteResult::Written);
    assert_eq!(after_checksum.is_built_as_latest(), base_is_latest);

    // publish() (catalog-managed only) also inherits the base's flag.
    if kind == TableKind::CatalogManaged {
        let published = base.publish(engine.as_ref(), &TestCatalogCommitter)?;
        assert_eq!(published.is_built_as_latest(), base_is_latest);
    }

    // A post-commit snapshot is latest.
    let post_commit = append_row(base, &engine, kind, 4).await?;
    assert_eq!(post_commit.version(), 4);
    assert!(post_commit.is_built_as_latest());

    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_as_latest_on_fresh_and_incremental_build(
    #[values(None, Some(1), Some(3))] time_travel_version: Option<Version>,
    #[values(TableKind::FileSystem, TableKind::CatalogManaged)] kind: TableKind,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table(&engine, &table_path, kind).await?;

    let pins_catalog_latest =
        kind == TableKind::CatalogManaged && time_travel_version == Some(LATEST_VERSION);

    let mut builder =
        maybe_attach_max_catalog_version(Snapshot::builder_for(&table_path), LATEST_VERSION, kind);
    if let Some(v) = time_travel_version {
        builder = builder.at_version(v);
    }
    let base = builder.build(engine.as_ref())?;
    assert_eq!(
        base.version(),
        time_travel_version.unwrap_or(LATEST_VERSION)
    );
    // Time travel to the catalog's latest version is also considered built as latest.
    let base_is_latest = time_travel_version.is_none() || pins_catalog_latest;
    assert_eq!(base.is_built_as_latest(), base_is_latest);

    // Incremental build without an explicit version is latest.
    let refreshed = maybe_attach_max_catalog_version(
        Snapshot::builder_from(base.clone()),
        LATEST_VERSION,
        kind,
    )
    .build(engine.as_ref())?;
    assert_eq!(refreshed.version(), LATEST_VERSION);
    assert!(refreshed.is_built_as_latest());

    // Incremental build time-travelling to LATEST_VERSION. `pinned` is built as latest iff either:
    // - the base was already built as latest(and we are traveling to the same version), or
    // - the time-travel version is the catalog's latest ratified version.
    let pinned =
        maybe_attach_max_catalog_version(Snapshot::builder_from(base), LATEST_VERSION, kind)
            .at_version(LATEST_VERSION)
            .build(engine.as_ref())?;
    assert_eq!(pinned.version(), LATEST_VERSION);
    assert_eq!(
        pinned.is_built_as_latest(),
        base_is_latest || kind == TableKind::CatalogManaged
    );

    Ok(())
}

struct SnapshotLoadProtocolCase {
    min_reader_version: i32,
    reader_features: &'static [&'static str],
    writer_features: &'static [&'static str],
    expected_error: Option<&'static str>,
}
