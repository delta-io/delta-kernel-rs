//! Checkpoint format types.

use delta_kernel_derive::internal_api;

use crate::Version;

/// The selected checkpoint and its retained type observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) struct SelectedCheckpoint {
    /// The selected checkpoint's version.
    pub version: Version,
    /// The type retained by this segment, when known.
    pub checkpoint_type: Option<SelectedCheckpointType>,
}

/// The specification followed by a snapshot's selected checkpoint.
///
/// This describes the selected checkpoint, independently of the table's configured checkpoint
/// policy. With `declarative-plans`, [`crate::snapshot::SnapshotReader`] resolves it through
/// `get_checkpoint_type` or returns it as part of complete checkpoint information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedCheckpointType {
    /// The checkpoint follows the V1 specification.
    V1,
    /// The checkpoint follows the V2 specification, with or without sidecars.
    V2,
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use std::sync::Arc;

    use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};

    use super::SelectedCheckpointType;
    use crate::Snapshot;
    use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
    use crate::engine::PanicEngine;

    #[rstest::rstest]
    #[case::classic(FeatureSet::empty(), false, SelectedCheckpointType::V1)]
    #[case::v2_inline(FeatureSet::new().v2_checkpoint(), false, SelectedCheckpointType::V2)]
    #[case::v2_sidecars(FeatureSet::new().v2_checkpoint(), true, SelectedCheckpointType::V2)]
    fn checkpoint_type_is_shared_by_discovery_planning_and_hints(
        #[case] features: FeatureSet,
        #[case] sidecars: bool,
        #[case] written_type: SelectedCheckpointType,
        #[values(false, true)] with_crc: bool,
        #[values(false, true)] getter_first: bool,
    ) {
        // Given a snapshot selecting a classic-named V1 or V2 checkpoint.
        let mut log = LogState::with_latest_version(3).with_checkpoint_at([1]);
        if sidecars {
            log = log.with_sidecars_if_enabled(Some(1));
        }
        if with_crc {
            log = log.with_crc_at([2]);
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
        let reader = snapshot.reader();
        let expected = Some(written_type);
        let early_type = (written_type == SelectedCheckpointType::V1).then_some(written_type);
        assert_eq!(snapshot.known_checkpoint_type(), early_type);
        assert_eq!(
            snapshot.to_snapshot_hint().unwrap().checkpoint_type(),
            early_type
        );
        assert!(snapshot.cached_checkpoint_info().is_none());
        if early_type.is_some() {
            assert_eq!(
                reader.get_checkpoint_type(&PanicEngine).unwrap(),
                early_type
            );
        } else {
            assert!(reader.get_checkpoint_type(&PanicEngine).is_err());
        }
        assert!(snapshot.cached_checkpoint_info().is_none());

        // When the type is requested before or after declarative scan planning.
        if getter_first {
            assert_eq!(reader.get_checkpoint_type(&engine).unwrap(), expected);
        }
        snapshot
            .clone()
            .scan_builder()
            .build()
            .unwrap()
            .declarative_metadata_scan_plan(&engine)
            .unwrap();
        assert_eq!(reader.get_checkpoint_type(&engine).unwrap(), expected);
        let info = reader.get_checkpoint_info(&engine).unwrap().unwrap();
        // Then all readers share a definite type without further engine access.
        assert_eq!(info.checkpoint_type(), written_type);
        assert_eq!(reader.get_checkpoint_type(&PanicEngine).unwrap(), expected);
        let cached = reader.get_checkpoint_info(&PanicEngine).unwrap().unwrap();
        assert!(Arc::ptr_eq(&info, &cached));

        // When the snapshot loaded from storage is rebuilt from its resolved hint or advanced.
        let hint = snapshot.to_snapshot_hint().unwrap();
        assert_eq!(hint.checkpoint_type(), expected);
        let hinted = Snapshot::builder_for(table.table_root())
            .with_snapshot_hint(hint)
            .build(&PanicEngine)
            .unwrap();
        let updated = Snapshot::builder_from(snapshot.clone())
            .build(&engine)
            .unwrap();
        // Then the resolved type survives without rediscovery.
        assert_eq!(
            hinted.reader().get_checkpoint_type(&PanicEngine).unwrap(),
            expected
        );
        assert_eq!(updated.version(), 3);
        assert_eq!(
            updated.log_segment().checkpoint_version(),
            snapshot.log_segment().checkpoint_version()
        );
        assert!(updated.cached_checkpoint_info().is_none());
        assert_eq!(
            updated.reader().get_checkpoint_type(&PanicEngine).unwrap(),
            expected
        );

        // When Kernel writes a checkpoint, it already knows its specification.
        let spec = sidecars.then_some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }));
        let (_, checkpointed) = snapshot.checkpoint(&engine, spec.as_ref()).unwrap();
        // Then the writer's type is available without checkpoint discovery.
        assert_eq!(
            checkpointed
                .reader()
                .get_checkpoint_type(&PanicEngine)
                .unwrap(),
            expected
        );
        assert!(checkpointed.cached_checkpoint_info().is_none());

        // When an incremental load selects a newer checkpoint.
        updated.checkpoint(&engine, spec.as_ref()).unwrap();
        let reloaded = Snapshot::builder_from(checkpointed).build(&engine).unwrap();
        // Then it starts with the new checkpoint's evidence and can resolve its type.
        assert_eq!(reloaded.log_segment().checkpoint_version(), Some(3));
        assert_eq!(reloaded.known_checkpoint_type(), early_type);
        assert_eq!(
            reloaded.reader().get_checkpoint_type(&engine).unwrap(),
            expected
        );
    }
}
