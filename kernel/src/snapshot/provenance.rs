//! Observations retained while resolving a snapshot. These values never trigger storage I/O.

use crate::Version;

/// The specification observed for a snapshot's selected checkpoint.
///
/// A classic checkpoint filename alone does not distinguish V1 from V2.
/// Selection does not require reading the checkpoint; a CRC can supply protocol and metadata.
/// Classic-named checkpoints remain unresolved unless matching `_last_checkpoint` schema evidence,
/// a retained hint claim, or the checkpoint writer establishes their type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SelectedCheckpointType {
    /// A checkpoint was selected, but its type was not established during snapshot construction.
    #[default]
    Unresolved,
    /// The checkpoint follows the V1 specification.
    V1,
    /// The checkpoint follows the V2 specification, with or without sidecars.
    V2,
}

/// How the retained checksum relates to an observed checksum file and the snapshot version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion,
    /// An older on-disk checksum was advanced in memory to the snapshot version using committed
    /// actions.
    Reconstructed {
        /// Total commits incorporated since the on-disk baseline version, across all updates.
        num_commits: u64,
    },
    /// A usable checksum is retained behind the snapshot version.
    Stale,
    /// Loading found no checksum candidate in its listed version range and retained no in-memory
    /// CRC. Fresh loads consider the selected checkpoint version (or zero without a checkpoint)
    /// through the snapshot version, inclusive. Incremental loads may list only after the previous
    /// checkpoint, or after the previous snapshot when new checkpoints are ignored. Files below
    /// the selected checkpoint are not candidates. This does not describe unlisted versions.
    Missing,
    /// An observed checksum candidate was unusable and no usable baseline was resolved.
    /// This includes read failures as well as invalid checksum contents.
    Unusable,
    /// No observation of an on-disk checksum is available. This occurs when a connector supplies
    /// a hint without provenance, or a derived snapshot discards its known CRC at a checkpoint.
    /// It does not indicate that loading searched for a checksum and failed to find or read it.
    #[default]
    Unknown,
}

/// Checksum provenance retained by a snapshot, independent of aggregate availability.
///
/// This records observations made while resolving the snapshot, not the current contents of
/// storage. In particular, an in-memory checksum at the snapshot version does not by itself
/// establish that a checksum file exists at that version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CrcProvenance {
    /// How the checksum was resolved.
    pub source: CrcSource,
    /// Version of the on-disk checksum file used as the original baseline, when known.
    /// This stays at the disk file's version when the checksum is advanced in memory.
    pub base_version: Option<Version>,
}

impl CrcProvenance {
    /// Describes `source` and the on-disk baseline's `base_version`, if known.
    /// Snapshot-hint construction validates the claim against the retained checksum.
    pub fn new(source: CrcSource, base_version: Option<Version>) -> Self {
        Self {
            source,
            base_version,
        }
    }
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
    fn checkpoint_provenance_is_unchanged_by_scan_and_hint_round_trip(
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
