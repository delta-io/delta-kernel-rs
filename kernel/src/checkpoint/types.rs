//! Checkpoint format types.

use delta_kernel_derive::internal_api;

use crate::Version;

/// The selected checkpoint and the type established when its segment was constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) struct SelectedCheckpoint {
    /// The selected checkpoint's version.
    pub version: Version,
    /// The retained type observation. Reads do not refine this value.
    pub checkpoint_type: SelectedCheckpointType,
}

/// The specification observed for a snapshot's selected checkpoint.
///
/// A classic checkpoint filename alone does not distinguish V1 from V2.
/// Selection does not require reading the checkpoint; a CRC can supply protocol and metadata.
/// Classic-named checkpoints remain unresolved unless matching `_last_checkpoint` schema evidence,
/// a retained hint claim, or the checkpoint writer establishes their type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelectedCheckpointType {
    /// A checkpoint was selected, but its type was not established during snapshot construction.
    #[default]
    Unresolved,
    /// The checkpoint follows the V1 specification.
    V1,
    /// The checkpoint follows the V2 specification, with or without sidecars.
    V2,
}

#[cfg(test)]
mod tests {
    use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};

    use super::SelectedCheckpointType;
    use crate::Snapshot;
    use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};

    #[rstest::rstest]
    #[case::classic(FeatureSet::empty(), false, SelectedCheckpointType::V1)]
    #[case::v2_inline(FeatureSet::new().v2_checkpoint(), false, SelectedCheckpointType::V2)]
    #[case::v2_sidecars(FeatureSet::new().v2_checkpoint(), true, SelectedCheckpointType::V2)]
    fn checkpoint_type_is_unchanged_by_scan_and_hint_round_trip(
        #[case] features: FeatureSet,
        #[case] sidecars: bool,
        #[case] written_type: SelectedCheckpointType,
    ) {
        // Given a snapshot loaded from a classic or V2 checkpoint.
        let mut log = LogState::with_latest_version(3).with_checkpoint_at([1]);
        if sidecars {
            log = log.with_sidecars_if_enabled(Some(1));
        }
        let table = TestTableBuilder::new()
            .with_features(features)
            .with_log_state(log)
            .with_data(2, 2)
            .build()
            .unwrap();
        let engine = crate::engine::sync::SyncEngine::new_with_store(table.store().clone());
        let snapshot = Snapshot::builder_for(table.table_root())
            .at_version(2)
            .build(&engine)
            .unwrap();
        let expected = snapshot.checkpoint_type();
        assert!(expected.is_some());

        // When a scan reads the checkpoint contents.
        let scan = snapshot.clone().scan_builder().build().unwrap();
        for batch in scan.scan_metadata(&engine).unwrap() {
            batch.unwrap();
        }
        // Then the snapshot keeps its build-time observation.
        assert_eq!(snapshot.checkpoint_type(), expected);

        // When the snapshot is rebuilt from its hint.
        let hinted = Snapshot::builder_for(table.table_root())
            .with_snapshot_hint(snapshot.to_snapshot_hint().unwrap())
            .build(&engine)
            .unwrap();
        // Then the retained checkpoint type survives the round trip.
        assert_eq!(hinted.checkpoint_type(), expected);

        // When Kernel writes a checkpoint, it knows which specification it used.
        let spec = sidecars.then_some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }));
        let (_, checkpointed) = snapshot.checkpoint(&engine, spec.as_ref()).unwrap();
        // Then the writer records that type and the source observation remains unchanged.
        assert_eq!(checkpointed.checkpoint_type(), Some(written_type));
        assert_eq!(snapshot.checkpoint_type(), expected);

        // When the written checkpoint is reused through a hint and an incremental update.
        let hinted = Snapshot::builder_for(table.table_root())
            .with_snapshot_hint(checkpointed.to_snapshot_hint().unwrap())
            .build(&engine)
            .unwrap();
        assert_eq!(hinted.checkpoint_type(), Some(written_type));
        let updated = Snapshot::builder_from(hinted).build(&engine).unwrap();
        // Then advancing to the next commit retains the writer's checkpoint observation.
        assert_eq!(updated.version(), 3);
        assert_eq!(updated.log_segment().checkpoint_version(), Some(2));
        assert_eq!(updated.checkpoint_type(), Some(written_type));
    }
}
