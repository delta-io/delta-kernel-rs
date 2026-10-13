//! AMT (adaptiveMetadata) tests for [`IncrementalScanBuilder`]: ranges containing a manifest
//! commit fall back (`None`) because its file actions may live only in the metadata tree.

use std::collections::HashSet;
use std::sync::Arc;

use rstest::rstest;
use test_utils::add_commit;

use crate::actions::{CheckpointAction, ContentRoot, LastManifestCommit};
use crate::engine::sync::SyncEngine;
use crate::log_replay::FileActionKey;
use crate::object_store::memory::InMemory;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    adaptive_metadata_protocol_and_metadata, checkpoint_line, commit_info_line,
    protocol_and_metadata_lines,
};
use crate::unit_test_utils::test_schema_flat_with_column_mapping;
use crate::{Snapshot, Version};

const TABLE_ROOT: &str = "memory:///";

// A commit adding `path`, led by the In-Commit Timestamp `commitInfo` the table requires. A
// `last_manifest_commit` `(version, contentRootVersion)` naming `version` itself makes this a
// manifest commit, so it also carries the `checkpoint` action for that content root.
fn add_commit_with_last_manifest_commit(
    version: Version,
    path: &str,
    last_manifest_commit: Option<(i64, i64)>,
) -> String {
    let pointer = last_manifest_commit.map(|(v, c)| LastManifestCommit::new(v, c).unwrap());
    let add = serde_json::json!({ "add": {
        "path": path,
        "partitionValues": {},
        "size": 1,
        "modificationTime": 0,
        "dataChange": true,
    } });
    let mut lines = vec![commit_info_line(1, pointer.as_ref()), add.to_string()];
    if let Some(pointer) = pointer.filter(|p| p.version == version as i64) {
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let content_root = ContentRoot::new(
            "_delta_log/_manifests/root.avro".to_string(),
            1024,
            pointer.content_root_version,
        );
        let checkpoint = CheckpointAction::new(
            pointer.version,
            content_root,
            protocol,
            metadata,
            vec![],
            vec![],
        );
        lines.push(checkpoint_line(&checkpoint));
    }
    lines.join("\n")
}

#[rstest]
#[case::no_manifest_commit(None, None, 0, true)]
#[case::manifest_commit_at_base(Some((1, 1)), Some((1, 1)), 1, true)]
#[case::manifest_commit_is_target(None, Some((2, 2)), 1, false)]
#[case::manifest_commit_carried_forward_into_range(Some((1, 1)), Some((1, 1)), 0, false)]
#[case::lagging_content_root_in_range(None, Some((2, 1)), 1, false)]
#[tokio::test]
async fn manifest_commit_in_range_falls_back(
    #[case] v1_last_manifest_commit: Option<(i64, i64)>,
    #[case] v2_last_manifest_commit: Option<(i64, i64)>,
    #[case] base_version: Version,
    #[case] expect_stream: bool,
) {
    let store = Arc::new(InMemory::new());
    let commits = [
        format!(
            "{}\n{}",
            commit_info_line(1, None),
            protocol_and_metadata_lines(test_schema_flat_with_column_mapping(), &[])
        ),
        add_commit_with_last_manifest_commit(1, "a.parquet", v1_last_manifest_commit),
        add_commit_with_last_manifest_commit(2, "b.parquet", v2_last_manifest_commit),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as Version, commit)
            .await
            .unwrap();
    }
    let engine = SyncEngine::new_with_store(store);
    let snapshot = Snapshot::builder_for(TABLE_ROOT).build(&engine).unwrap();

    let stream = snapshot
        .incremental_scan_builder(base_version)
        .build(&engine)
        .unwrap();

    assert_eq!(stream.is_some(), expect_stream);
    if let Some(stream) = stream {
        let expected: HashSet<_> = ["a.parquet", "b.parquet"]
            .into_iter()
            .skip(base_version as usize)
            .map(|path| FileActionKey::new(path, None))
            .collect();
        assert_eq!(stream.into_summary().unwrap().live_adds, expected);
    }
}
