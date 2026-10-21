//! Snapshot tests for adaptiveMetadata (AMT) tables whose state is carried by a `checkpoint`
//! action.

use itertools::Itertools as _;
use rstest::rstest;
use url::Url;

use crate::actions::{CheckpointAction, ContentRoot, SetTransaction, LOG_TXN_SCHEMA};
use crate::engine::sync::SyncEngine;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    checkpoint_line, setup_adaptive_metadata_table, write_ict_commit, write_sidecar,
};
use crate::unit_test_utils::Action;
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
    write_ict_commit(&engine, &table_root, 1, before);
    write_crc_at(1);
    let snapshot = Snapshot::builder_for(table_root.clone())
        .build(&engine)
        .unwrap();
    let checkpoint = checkpoint(&engine, &table_root, &snapshot);
    write_ict_commit(
        &engine,
        &table_root,
        CHECKPOINT_VERSION,
        &checkpoint_line(&checkpoint),
    );
    write_crc_at(CHECKPOINT_VERSION);
    write_ict_commit(&engine, &table_root, CHECKPOINT_VERSION + 1, after);
    write_crc_at(CHECKPOINT_VERSION + 1);
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(
        snapshot.crc_at_version().is_some(),
        crc_version == Some(CHECKPOINT_VERSION + 1)
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

fn txn(app_id: &str, version: i64) -> SetTransaction {
    SetTransaction::new(app_id.to_string(), version, None)
}

fn txn_line(app_id: &str, version: i64) -> String {
    serde_json::to_string(&Action::Txn(txn(app_id, version))).unwrap()
}

/// The action's txns, which the consistent cases also commit before it.
const ACTION_TXNS: &[(&str, i64)] = &[("a", 1), ("b", 2), ("d", 4)];

/// The checkpoint action's inline (`a`, `b`) and sidecar (`d`) `txn` entries are read for the
/// commits at or below its version, and a later commit (`b`) overrides them. Without a CRC, or
/// with one older than the action, only the action carries them, so `c` from an earlier commit
/// proves the action replaced the commits. A CRC is built from commits alone (TODO(#2866)), so the
/// CRC-rooted cases first commit the action's txns, as a writer would.
#[rstest]
#[case::no_crc(&[("c", 9)], None)]
#[case::stale_crc_before_checkpoint_action(&[("c", 9)], Some(1))]
#[case::stale_crc_at_checkpoint_action(ACTION_TXNS, Some(CHECKPOINT_VERSION))]
#[case::crc_at_snapshot_version(ACTION_TXNS, Some(CHECKPOINT_VERSION + 1))]
fn app_id_versions_come_from_checkpoint_action_and_later_commits(
    #[case] before: &[(&str, i64)],
    #[case] crc_version: Option<Version>,
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
}
