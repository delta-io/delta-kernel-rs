//! Snapshot tests for adaptiveMetadata (AMT) tables whose state is carried by a `checkpoint`
//! action.

use std::sync::Arc;

use itertools::Itertools as _;
use rstest::rstest;
use url::Url;

use crate::actions::{
    CheckpointAction, ContentRoot, DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA,
    LastManifestCommit, SetTransaction,
};
use crate::engine::sync::SyncEngine;
use crate::metrics::MeteredDeltaEngine;
use crate::snapshot::builder::tests::measuring_reporter;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    checkpoint_line, domain, domain_metadata_line, setup_adaptive_metadata_table, txn_line,
    write_ict_commit, write_sidecar,
};
use crate::unit_test_utils::is_io_event;
use crate::{Snapshot, SnapshotRef, Version};

/// The version [`table_with_checkpoint_action`] commits its checkpoint action at.
const CHECKPOINT_VERSION: Version = 2;

/// An AMT table that commits `before` at version 1, the action `checkpoint` builds at
/// [`CHECKPOINT_VERSION`], then `after`, with a CRC at `crc_version`. Returns its latest snapshot.
fn table_with_checkpoint_action(
    before: &str,
    checkpoint: impl FnOnce(&SyncEngine, &Url, &Snapshot) -> CheckpointAction,
    after: &str,
    crc_version: Option<Version>,
) -> (SyncEngine, SnapshotRef) {
    let (engine, table_root) = setup_adaptive_metadata_table();
    let write_crc_at = |version| {
        if crc_version == Some(version) {
            Snapshot::builder_for(table_root.clone())
                .at_version(version)
                .build(&engine)
                .unwrap()
                .write_checksum(&engine)
                .unwrap();
        }
    };
    write_ict_commit(&engine, &table_root, 1, None, before);
    write_crc_at(1);
    let snapshot = Snapshot::builder_for(table_root.clone())
        .build(&engine)
        .unwrap();
    let checkpoint = checkpoint(&engine, &table_root, &snapshot);
    // The manifest commit and every later commit point at the manifest commit, as an AMT writer
    // records them; a CRC carries the pointer forward. [`checkpoint_action`]'s content root is at
    // the action's version.
    let pointer = LastManifestCommit::new(checkpoint.version(), checkpoint.version()).unwrap();
    write_ict_commit(
        &engine,
        &table_root,
        CHECKPOINT_VERSION,
        Some(&pointer),
        &checkpoint_line(&checkpoint),
    );
    write_crc_at(CHECKPOINT_VERSION);
    write_ict_commit(
        &engine,
        &table_root,
        CHECKPOINT_VERSION + 1,
        Some(&pointer),
        after,
    );
    write_crc_at(CHECKPOINT_VERSION + 1);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(
        snapshot.crc_at_version().is_some(),
        crc_version == Some(CHECKPOINT_VERSION + 1)
    );
    assert!(
        snapshot
            .latest_checkpoint_action(&engine)
            .unwrap()
            .is_some()
    );
    (engine, snapshot)
}

/// A checkpoint action at [`CHECKPOINT_VERSION`] that carries `snapshot`'s protocol and metadata,
/// which CRC validation checks for continuity.
fn checkpoint_action(snapshot: &Snapshot) -> CheckpointAction {
    let config = snapshot.table_configuration();
    let version = CHECKPOINT_VERSION as i64;
    CheckpointAction::new(
        version,
        ContentRoot::new("metadata/root.parquet".to_string(), 1, version),
        config.protocol().clone(),
        config.metadata().clone(),
        vec![],
        vec![],
    )
}

#[rstest]
fn freshness_changes_preserve_populated_adaptive_metadata_caches(
    #[values(None, Some(CHECKPOINT_VERSION + 1))] crc_version: Option<Version>,
) {
    // GIVEN a latest snapshot with populated adaptive-metadata caches and an optional CRC.
    let (engine, source) = table_with_checkpoint_action(
        "",
        |_, _, snapshot| checkpoint_action(snapshot),
        "",
        crc_version,
    );
    let checkpoint = source.latest_checkpoint_action(&engine).unwrap().unwrap();
    let manifest = source.last_manifest_commit(&engine).unwrap().unwrap();
    let engine = MeteredDeltaEngine::new(Arc::new(engine));
    let (reporter, _guard) = measuring_reporter();

    // WHEN the same version is requested with a newer catalog bound.
    let historical = Snapshot::builder_from(source.clone())
        .at_version(source.version())
        .with_max_catalog_version(source.version() + 1)
        .build(&engine)
        .unwrap();
    // THEN the newer bound clears freshness without changing the source.
    assert!(!historical.is_built_as_latest());
    assert!(source.is_built_as_latest());

    // WHEN a matching bound confirms the same version as latest again.
    let promoted = Snapshot::builder_from(historical.clone())
        .with_max_catalog_version(historical.version())
        .build(&engine)
        .unwrap();

    // THEN both derived snapshots preserve populated caches without I/O or source mutation.
    assert!(promoted.is_built_as_latest());
    assert!(!historical.is_built_as_latest());
    for snapshot in [&historical, &promoted] {
        assert!(Arc::ptr_eq(
            snapshot.checkpoint_action.get().unwrap().as_ref().unwrap(),
            &checkpoint,
        ));
        assert_eq!(
            snapshot.last_manifest_commit.get().unwrap().as_ref(),
            Some(&manifest)
        );
    }
    assert!(!reporter.events().iter().any(is_io_event));
}

fn txn(app_id: &str, version: i64) -> SetTransaction {
    SetTransaction::new(app_id.to_string(), version, None)
}

/// The action's txns, which the consistent cases also commit before it.
const ACTION_TXNS: &[(&str, i64)] = &[("a", 1), ("b", 2), ("d", 4)];

/// Every case expects the same app-id versions; the cases differ only in which read path answers
/// (pinned by `expected_tail_base`, the CRC a tail-only replay starts from), and each must agree
/// with full replay. The checkpoint action's
/// inline (`a`, `b`) and sidecar (`d`) `txn` entries stand in for the commits at or below its
/// version, and a later commit (`b`) overrides them.
#[rstest]
// Full replay of the action and the later commit: `c`, committed only before the action, is
// absent because the action replaces those commits.
#[case::no_crc(&[("c", 9)], None, None)]
// The CRC predates the action, so it is skipped for full replay; its `c` must not leak through.
#[case::stale_crc_before_checkpoint_action(&[("c", 9)], Some(1), None)]
// The CRC at the action's version roots a scan of only the later commit. The CRC writer reads
// commits alone (TODO(#2866)), so the action's txns are committed first, as a writer would, to
// keep the CRC consistent with the action.
#[case::stale_crc_at_checkpoint_action(
    ACTION_TXNS,
    Some(CHECKPOINT_VERSION),
    Some(CHECKPOINT_VERSION)
)]
// The CRC at the snapshot version answers directly, without reading the log.
#[case::crc_at_snapshot_version(
    ACTION_TXNS,
    Some(CHECKPOINT_VERSION + 1),
    Some(CHECKPOINT_VERSION + 1)
)]
fn app_id_versions_come_from_checkpoint_action_and_later_commits(
    #[case] before: &[(&str, i64)],
    #[case] crc_version: Option<Version>,
    #[case] expected_tail_base: Option<Version>,
) {
    let before = before
        .iter()
        .map(|(app_id, version)| txn_line(app_id, *version))
        .join("\n");
    let (engine, snapshot) = table_with_checkpoint_action(
        &before,
        |engine, table_root, snapshot| CheckpointAction {
            transactions: vec![txn("a", 1), txn("b", 2)],
            txn_sidecars: vec![write_sidecar(
                engine,
                table_root,
                "txn.parquet",
                LOG_TXN_SCHEMA.clone(),
                vec![txn("d", 4).into()],
            )],
            ..checkpoint_action(snapshot)
        },
        &txn_line("b", 3),
        crc_version,
    );

    for (app_id, expected) in [("a", Some(1)), ("b", Some(3)), ("c", None), ("d", Some(4))] {
        assert_eq!(
            snapshot.get_app_id_version(app_id, &engine).unwrap(),
            expected,
            "app id {app_id}"
        );
    }
    let all = snapshot
        .get_app_id_versions(&engine)
        .unwrap()
        .into_values()
        .sorted_by(|x, y| x.app_id.cmp(&y.app_id))
        .collect_vec();
    assert_eq!(all, [txn("a", 1), txn("b", 3), txn("d", 4)]);
    let tail_base = snapshot.tail_replay_base_crc(&engine).unwrap();
    assert_eq!(tail_base.map(|crc| crc.version), expected_tail_base);
}

/// The action's domains, which the consistent cases also commit before it.
const ACTION_DOMAINS: &[(&str, &str)] = &[("a", "a1"), ("b", "b1"), ("d", "d1")];

/// Like [`app_id_versions_come_from_checkpoint_action_and_later_commits`], for domain metadata:
/// the action's inline (`a`, `b`) and sidecar (`d`) entries are read, and a later commit updates
/// `a` and removes `b`.
#[rstest]
#[case::no_crc(&[("c", "c0")], None)]
#[case::stale_crc_before_checkpoint_action(&[("c", "c0")], Some(1))]
#[case::stale_crc_at_checkpoint_action(ACTION_DOMAINS, Some(CHECKPOINT_VERSION))]
#[case::crc_at_snapshot_version(ACTION_DOMAINS, Some(CHECKPOINT_VERSION + 1))]
fn domain_metadata_comes_from_checkpoint_action_and_later_commits(
    #[case] before: &[(&str, &str)],
    #[case] crc_version: Option<Version>,
) {
    let before = before
        .iter()
        .map(|(name, configuration)| domain_metadata_line(domain(name, configuration)))
        .join("\n");
    let after = format!(
        "{}\n{}",
        domain_metadata_line(domain("a", "a2")),
        domain_metadata_line(DomainMetadata::remove("b".to_string(), String::new()))
    );
    let (engine, snapshot) = table_with_checkpoint_action(
        &before,
        |engine, table_root, snapshot| CheckpointAction {
            domain_metadata: vec![domain("a", "a1"), domain("b", "b1")],
            domain_metadata_sidecars: vec![write_sidecar(
                engine,
                table_root,
                "domains.parquet",
                LOG_DOMAIN_METADATA_SCHEMA.clone(),
                vec![domain("d", "d1").into()],
            )],
            ..checkpoint_action(snapshot)
        },
        &after,
        crc_version,
    );

    for (domain, expected) in [
        ("a", Some("a2")),
        ("b", None),
        ("c", None),
        ("d", Some("d1")),
    ] {
        assert_eq!(
            snapshot
                .reader()
                .get_domain_metadata(domain, &engine)
                .unwrap()
                .as_deref(),
            expected,
            "domain {domain}"
        );
    }
    let all = snapshot.reader().get_all_domain_metadata(&engine).unwrap();
    assert_eq!(
        all.iter().map(|dm| dm.domain()).sorted().collect_vec(),
        ["a", "d"]
    );
}
