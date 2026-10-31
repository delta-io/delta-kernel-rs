//! Observations retained while resolving a snapshot. These values never trigger storage I/O.

use crate::Version;

/// The specification of the checkpoint selected for a snapshot.
///
/// This describes the selected checkpoint, independently of the table's checkpoint policy.
/// A classic checkpoint filename alone does not distinguish V1 from V2.
/// Classic-named V2 checkpoints without sidecars can remain unknown when ordinary reads do not
/// inspect their checkpoint metadata action.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
#[repr(C)]
pub enum SelectedCheckpointKind {
    /// No checkpoint was selected.
    None,
    /// The selected checkpoint follows the V1 specification.
    V1,
    /// The selected checkpoint follows the V2 specification, with or without sidecars.
    V2,
    /// Ordinary snapshot processing has not established the checkpoint specification.
    #[default]
    Unknown,
}

/// How the retained checksum relates to an observed checksum file and the snapshot version.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
#[repr(C)]
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
    /// Normal loading found no applicable checksum file.
    Missing,
    /// An observed checksum candidate was unusable and no usable baseline was resolved.
    /// This includes read failures as well as invalid checksum contents.
    Unusable,
    /// The checksum's disk provenance was not established.
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

    use super::*;
    use crate::Snapshot;

    #[rstest::rstest]
    fn checkpoint_kind_from_ordinary_scan_survives_hint(
        #[values(false, true)] v2: bool,
        #[values(false, true)] sidecars: bool,
    ) {
        let mut log = LogState::with_latest_version(2).with_checkpoint_at([1]);
        if sidecars {
            log = log.with_sidecars_if_enabled(Some(1));
        }
        let features = if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::empty()
        };
        let table = TestTableBuilder::new()
            .with_features(features)
            .with_log_state(log)
            .with_data(2, 2)
            .build()
            .unwrap();
        let engine = crate::engine::sync::SyncEngine::new_with_store(table.store().clone());
        let snapshot = Snapshot::builder_for(table.table_root())
            .build(&engine)
            .unwrap();
        let scan = snapshot.clone().scan_builder().build().unwrap();
        for batch in scan.scan_metadata(&engine).unwrap() {
            batch.unwrap();
        }
        let expected = if v2 {
            SelectedCheckpointKind::V2
        } else {
            SelectedCheckpointKind::V1
        };
        assert_eq!(snapshot.selected_checkpoint_kind(), expected);
        let hinted = Snapshot::builder_for(table.table_root())
            .with_snapshot_hint(snapshot.to_snapshot_hint().unwrap())
            .build(&engine)
            .unwrap();
        assert_eq!(hinted.selected_checkpoint_kind(), expected);
    }
}
