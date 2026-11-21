//! Checkpoint format types.

use crate::actions::Protocol;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::table_features::TableFeature;

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

impl SelectedCheckpointType {
    /// The type the Delta protocol fixes without reading the checkpoint, or `None` when its
    /// contents decide.
    ///
    /// UUID-named checkpoints follow the V2 spec and multi-part checkpoints the V1 spec. A
    /// classic-named checkpoint follows the V2 spec only on a table with the v2Checkpoint feature.
    pub(crate) fn from_naming_and_protocol(
        path: &ParsedLogPath,
        protocol: &Protocol,
    ) -> Option<Self> {
        match path.file_type {
            LogPathFileType::UuidCheckpoint => Some(Self::V2),
            LogPathFileType::MultiPartCheckpoint { .. } => Some(Self::V1),
            // TODO(#4057): this is the snapshot's protocol, not the one the checkpoint was written
            // under, so a V2 checkpoint selected after the table drops v2Checkpoint reports V1.
            _ if !protocol.has_table_feature(&TableFeature::V2Checkpoint) => Some(Self::V1),
            _ => None,
        }
    }

    /// The type of a checkpoint whose contents were read. Naming decides UUID-named and multi-part
    /// checkpoints; a classic-named checkpoint follows the V2 spec when it has a
    /// `checkpointMetadata` action.
    #[cfg(feature = "declarative-plans")]
    pub(crate) fn from_contents(path: &ParsedLogPath, has_checkpoint_metadata: bool) -> Self {
        match path.file_type {
            LogPathFileType::UuidCheckpoint => Self::V2,
            LogPathFileType::MultiPartCheckpoint { .. } => Self::V1,
            _ if has_checkpoint_metadata => Self::V2,
            _ => Self::V1,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "declarative-plans")]
    use std::sync::Arc;

    use rstest::rstest;
    #[cfg(feature = "declarative-plans")]
    use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};

    use super::SelectedCheckpointType::{V1, V2};
    use super::*;
    use crate::FileMeta;
    #[cfg(feature = "declarative-plans")]
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
    #[cfg(feature = "declarative-plans")]
    use crate::engine::PanicEngine;

    const CLASSIC: &str = "00000000000000000000.checkpoint.parquet";
    const MULTI_PART: &str = "00000000000000000000.checkpoint.0000000001.0000000002.parquet";
    const UUID: &str =
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet";

    fn path(filename: &str) -> ParsedLogPath {
        let location = url::Url::parse("memory:///_delta_log/")
            .unwrap()
            .join(filename)
            .unwrap();
        ParsedLogPath::new(FileMeta {
            location,
            last_modified: 0,
            size: 1,
        })
        .unwrap()
    }

    fn protocol(v2_checkpoint: bool) -> Protocol {
        let features = if v2_checkpoint {
            vec![TableFeature::V2Checkpoint]
        } else {
            vec![]
        };
        Protocol::try_new_modern(features.clone(), features).unwrap()
    }

    #[rstest]
    #[case::uuid(UUID, false, Some(V2))]
    #[case::uuid_with_feature(UUID, true, Some(V2))]
    #[case::multi_part(MULTI_PART, false, Some(V1))]
    #[case::multi_part_with_feature(MULTI_PART, true, Some(V1))]
    #[case::classic_without_feature(CLASSIC, false, Some(V1))]
    #[case::classic_with_feature_needs_contents(CLASSIC, true, None)]
    fn naming_and_protocol_decide_type_without_reading(
        #[case] filename: &str,
        #[case] v2_checkpoint: bool,
        #[case] expected: Option<SelectedCheckpointType>,
    ) {
        let actual = SelectedCheckpointType::from_naming_and_protocol(
            &path(filename),
            &protocol(v2_checkpoint),
        );
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "declarative-plans")]
    #[rstest]
    #[case::classic_v1(CLASSIC, false, V1)]
    #[case::classic_v2(CLASSIC, true, V2)]
    #[case::uuid_follows_naming(UUID, false, V2)]
    #[case::multi_part_follows_naming(MULTI_PART, true, V1)]
    fn contents_decide_classic_checkpoint_type(
        #[case] filename: &str,
        #[case] has_checkpoint_metadata: bool,
        #[case] expected: SelectedCheckpointType,
    ) {
        let actual =
            SelectedCheckpointType::from_contents(&path(filename), has_checkpoint_metadata);
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "declarative-plans")]
    #[rstest]
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

        // When the snapshot loaded from storage is rebuilt from its hint or advanced.
        let hint = snapshot.to_snapshot_hint().unwrap();
        let hinted = Snapshot::builder_for(table.table_root())
            .with_snapshot_hint(hint)
            .build(&PanicEngine)
            .unwrap();
        let updated = Snapshot::builder_from(snapshot.clone())
            .build(&engine)
            .unwrap();
        // Then the snapshot hint carries the resolved type, so it survives without rediscovery.
        assert_eq!(hinted.known_checkpoint_type(), expected);
        assert_eq!(
            hinted.reader().get_checkpoint_type(&PanicEngine).unwrap(),
            expected
        );
        // And an incremental update reusing the same checkpoint keeps the resolved type.
        assert_eq!(updated.version(), 3);
        assert_eq!(
            updated.log_segment().checkpoint_version,
            snapshot.log_segment().checkpoint_version
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
        assert_eq!(reloaded.log_segment().checkpoint_version, Some(3));
        assert_eq!(reloaded.known_checkpoint_type(), early_type);
        assert_eq!(
            reloaded.reader().get_checkpoint_type(&engine).unwrap(),
            expected
        );
    }
}
