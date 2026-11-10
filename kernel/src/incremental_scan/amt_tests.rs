//! AMT (adaptiveMetadata) tests for [`IncrementalScanBuilder`].

use std::collections::HashSet;

use rstest::rstest;
use url::Url;

use crate::actions::LastManifestCommit;
use crate::engine::sync::SyncEngine;
use crate::log_replay::FileActionKey;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    checkpoint_action_with_content_root, checkpoint_line, setup_adaptive_metadata_table,
    write_ict_commit,
};
use crate::{KernelResult, Snapshot, SnapshotRef, Version};

/// How the target snapshot is loaded, which decides where its `lastManifestCommit` comes from.
#[derive(Clone, Copy, Debug)]
enum TargetLoad {
    /// Read lazily from the target commit.
    Fresh,
    /// Taken from a CRC at the target version.
    CrcAtTarget,
    /// Resolved while updating a snapshot at `base_version` to the latest version.
    UpdatedFromBase,
}

/// An AMT table whose commits 1 and 2 add `a.parquet` and `b.parquet` and carry the given
/// `lastManifestCommit` `(version, contentRootVersion)` pointers. A pointer naming its own commit
/// makes that commit a manifest commit, so it also gets the matching `checkpoint` action.
fn table_with_pointers(
    v1_pointer: Option<(i64, i64)>,
    v2_pointer: Option<(i64, i64)>,
) -> (SyncEngine, Url) {
    let (engine, table_root) = setup_adaptive_metadata_table();
    for (version, path, pointer) in [(1, "a.parquet", v1_pointer), (2, "b.parquet", v2_pointer)] {
        let pointer = pointer.map(|(v, c)| LastManifestCommit::new(v, c).unwrap());
        let add = serde_json::json!({ "add": {
            "path": path,
            "partitionValues": {},
            "size": 1,
            "modificationTime": 0,
            "dataChange": true,
        } });
        let mut actions = add.to_string();
        if let Some(pointer) = pointer.as_ref().filter(|p| p.version == version) {
            let checkpoint = checkpoint_action_with_content_root(
                "_delta_log/_manifests/root.avro",
                1024,
                pointer.version,
                pointer.content_root_version,
            );
            actions = format!("{actions}\n{}", checkpoint_line(&checkpoint));
        }
        write_ict_commit(
            &engine,
            &table_root,
            version as Version,
            pointer.as_ref(),
            &actions,
        );
    }
    (engine, table_root)
}

fn load_target(
    engine: &SyncEngine,
    table_root: &Url,
    base_version: Version,
    load: TargetLoad,
) -> SnapshotRef {
    let latest = || {
        Snapshot::builder_for(table_root.clone())
            .build(engine)
            .unwrap()
    };
    match load {
        TargetLoad::Fresh => latest(),
        TargetLoad::CrcAtTarget => {
            latest().write_checksum(engine).unwrap();
            let snapshot = latest();
            assert!(snapshot.crc_at_version().is_some());
            snapshot
        }
        TargetLoad::UpdatedFromBase => {
            let base = Snapshot::builder_for(table_root.clone())
                .at_version(base_version)
                .build(engine)
                .unwrap();
            // Resolve the base's pointer first so that wrongly reusing it would be observable.
            base.last_manifest_commit(engine).unwrap();
            Snapshot::builder_from(base).build(engine).unwrap()
        }
    }
}

/// The live adds of an incremental scan from `base_version` to `target`, or `None` on fallback.
fn incremental_live_adds(
    engine: &SyncEngine,
    target: SnapshotRef,
    base_version: Version,
) -> KernelResult<Option<HashSet<FileActionKey>>> {
    let stream = target
        .incremental_scan_builder(base_version)
        .build(engine)?;
    Ok(stream.map(|stream| stream.into_summary().unwrap().live_adds))
}

fn file_keys(paths: &[&str]) -> HashSet<FileActionKey> {
    paths
        .iter()
        .map(|path| FileActionKey::new(*path, None))
        .collect()
}

#[rstest]
#[case::no_manifest_commit(None, None, 0, Some(&["a.parquet", "b.parquet"][..]))]
#[case::manifest_commit_at_base(Some((1, 1)), Some((1, 1)), 1, Some(&["b.parquet"][..]))]
#[case::manifest_commit_is_target(None, Some((2, 2)), 1, None)]
#[case::manifest_commit_carried_forward_into_range(Some((1, 1)), Some((1, 1)), 0, None)]
// The RFC lets a manifest commit's content root lag its version; the guard must compare
// `version`, not `contentRootVersion`.
#[case::lagging_content_root_in_range(None, Some((2, 1)), 1, None)]
fn falls_back_iff_manifest_commit_in_range(
    #[case] v1_pointer: Option<(i64, i64)>,
    #[case] v2_pointer: Option<(i64, i64)>,
    #[case] base_version: Version,
    #[case] expected_live_adds: Option<&[&str]>,
    #[values(
        TargetLoad::Fresh,
        TargetLoad::CrcAtTarget,
        TargetLoad::UpdatedFromBase
    )]
    load: TargetLoad,
) {
    let (engine, table_root) = table_with_pointers(v1_pointer, v2_pointer);
    let target = load_target(&engine, &table_root, base_version, load);

    let live_adds = incremental_live_adds(&engine, target, base_version).unwrap();

    assert_eq!(live_adds, expected_live_adds.map(file_keys));
}

#[test]
fn manifest_commit_after_time_travel_target_does_not_fall_back() {
    let (engine, table_root) = table_with_pointers(None, Some((2, 2)));
    let target = Snapshot::builder_for(table_root)
        .at_version(1)
        .build(&engine)
        .unwrap();

    let live_adds = incremental_live_adds(&engine, target, 0).unwrap();

    assert_eq!(live_adds, Some(file_keys(&["a.parquet"])));
}
