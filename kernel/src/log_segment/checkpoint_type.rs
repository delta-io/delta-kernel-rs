use super::LogSegment;
use crate::actions::{CHECKPOINT_METADATA_NAME, Protocol};
use crate::checkpoint::{SelectedCheckpoint, SelectedCheckpointType};
use crate::error::SnapshotHintError;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::table_features::TableFeature;
use crate::{KernelResult, Version};

impl LogSegment {
    /// Returns the selected checkpoint's version, or `None` when no checkpoint was selected.
    #[delta_kernel_derive::internal_api]
    pub(crate) fn checkpoint_version(&self) -> Option<Version> {
        self.checkpoint.map(|checkpoint| checkpoint.version)
    }

    pub(crate) fn checkpoint_type(&self) -> Option<SelectedCheckpointType> {
        self.checkpoint
            .and_then(|checkpoint| checkpoint.checkpoint_type)
    }

    /// Refines an unresolved type using the protocol already loaded for this snapshot.
    pub(crate) fn resolve_checkpoint_type(&mut self, protocol: &Protocol) {
        let (Some(checkpoint), Some(path)) =
            (&mut self.checkpoint, self.listed.checkpoint_parts.first())
        else {
            return;
        };
        if checkpoint.checkpoint_type.is_none() {
            checkpoint.checkpoint_type =
                infer_checkpoint_type(path, self.last_checkpoint_metadata.as_ref(), Some(protocol));
        }
    }

    /// Returns the type for a combined segment, preserving a prior observation only when no new
    /// checkpoint was selected or this listing has unresolved evidence for identical paths.
    pub(crate) fn checkpoint_type_for_update(
        &self,
        previous: &Self,
        previous_type: Option<SelectedCheckpointType>,
    ) -> Option<SelectedCheckpointType> {
        match self.checkpoint {
            None => previous_type,
            Some(SelectedCheckpoint {
                checkpoint_type: None,
                ..
            }) if self
                .listed
                .checkpoint_parts
                .iter()
                .map(|part| &part.location.location)
                .eq(previous
                    .listed
                    .checkpoint_parts
                    .iter()
                    .map(|part| &part.location.location)) =>
            {
                previous_type
            }
            Some(checkpoint) => checkpoint.checkpoint_type,
        }
    }
}

impl SelectedCheckpoint {
    pub(super) fn try_new(
        path: &ParsedLogPath,
        hint: Option<&LastCheckpointHint>,
        retained_type: Option<SelectedCheckpointType>,
    ) -> KernelResult<Self> {
        let inferred = infer_checkpoint_type(path, hint, None);
        let checkpoint_type = match (inferred, retained_type) {
            (inferred, None) => inferred,
            (None, retained) => retained,
            (Some(inferred), Some(retained)) if inferred == retained => Some(inferred),
            _ => return Err(SnapshotHintError::CheckpointTypeConflict.into()),
        };
        Ok(Self {
            version: path.version,
            checkpoint_type,
        })
    }
}

fn infer_checkpoint_type(
    path: &ParsedLogPath,
    hint: Option<&LastCheckpointHint>,
    protocol: Option<&Protocol>,
) -> Option<SelectedCheckpointType> {
    match &path.file_type {
        LogPathFileType::MultiPartCheckpoint { .. } => Some(SelectedCheckpointType::V1),
        LogPathFileType::UuidCheckpoint => Some(SelectedCheckpointType::V2),
        // Classic-named V2 checkpoints require both protocol support and checkpointMetadata.
        _ if protocol
            .is_some_and(|protocol| !protocol.has_table_feature(&TableFeature::V2Checkpoint))
            || hint
                .filter(|hint| hint.applies_to(std::slice::from_ref(path)))
                .and_then(|hint| hint.checkpoint_schema.as_ref())
                .is_some_and(|schema| !schema.contains(CHECKPOINT_METADATA_NAME)) =>
        {
            Some(SelectedCheckpointType::V1)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::path::ParsedLogPath;
    use crate::schema::{SchemaRef, schema_ref};
    use crate::{FileMeta, KernelError};

    fn segment(
        filename: &str,
        hint: Option<LastCheckpointHint>,
        checkpoint_type: Option<SelectedCheckpointType>,
    ) -> KernelResult<LogSegment> {
        let root = url::Url::parse("memory:///_delta_log/").unwrap();
        let file = ParsedLogPath::new(FileMeta {
            location: root.join(filename).unwrap(),
            last_modified: 0,
            size: 1,
        })
        .unwrap();
        let files = if file.is_commit() {
            LogSegmentFiles {
                latest_commit_file: Some(file.clone()),
                ascending_commit_files: vec![file],
                ..Default::default()
            }
        } else {
            let mut parts = vec![file.clone()];
            if let LogPathFileType::MultiPartCheckpoint { .. } = file.file_type {
                let mut second = file.location;
                second.location = root
                    .join("00000000000000000000.checkpoint.0000000002.0000000002.parquet")
                    .unwrap();
                parts.push(ParsedLogPath::new(second).unwrap());
            }
            LogSegmentFiles {
                checkpoint_parts: parts,
                ..Default::default()
            }
        };
        LogSegment::try_new(files, root, Some(0), hint, checkpoint_type)
    }

    #[rstest::rstest]
    #[case("00000000000000000000.json", None, None, None)]
    #[case("00000000000000000000.checkpoint.parquet", None, None, None)]
    #[case(
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet",
        None,
        None,
        Some(SelectedCheckpointType::V1)
    )]
    #[case(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet",
        None,
        None,
        Some(SelectedCheckpointType::V2)
    )]
    #[case::v1_schema(
        "00000000000000000000.checkpoint.parquet",
        Some(schema_ref! { nullable "add": STRING }),
        None,
        Some(SelectedCheckpointType::V1)
    )]
    #[case::schema_with_checkpoint_metadata(
        "00000000000000000000.checkpoint.parquet",
        Some(schema_ref! { nullable "checkpointMetadata": STRING }),
        None,
        None
    )]
    #[case::retained_v1(
        "00000000000000000000.checkpoint.parquet",
        None,
        Some(SelectedCheckpointType::V1),
        Some(SelectedCheckpointType::V1)
    )]
    #[case::retained_v2(
        "00000000000000000000.checkpoint.parquet",
        None,
        Some(SelectedCheckpointType::V2),
        Some(SelectedCheckpointType::V2)
    )]
    fn checkpoint_type_from_constructor_evidence(
        #[case] filename: &str,
        #[case] checkpoint_schema: Option<SchemaRef>,
        #[case] retained_type: Option<SelectedCheckpointType>,
        #[case] expected: Option<SelectedCheckpointType>,
    ) {
        // Given a log path, optional matching schema evidence, and a retained type.
        let segment = segment(
            filename,
            checkpoint_schema.map(|schema| LastCheckpointHint {
                version: 0,
                checkpoint_schema: Some(schema),
                ..Default::default()
            }),
            retained_type,
        )
        .unwrap();
        // When the selected checkpoint type is observed.
        let checkpoint_type = segment.checkpoint_type();
        // Then the constructor reconciles the evidence without discarding a known type.
        assert_eq!(checkpoint_type, expected);
    }

    #[rstest::rstest]
    #[case::same_unknown(false, None, Some(SelectedCheckpointType::V2))]
    #[case::same_resolved(
        false,
        Some(SelectedCheckpointType::V1),
        Some(SelectedCheckpointType::V1)
    )]
    #[case::different_checkpoint(true, None, None)]
    fn derived_segment_retains_checkpoint_type_without_mutating_source(
        #[case] different_checkpoint: bool,
        #[case] new_type: Option<SelectedCheckpointType>,
        #[case] expected: Option<SelectedCheckpointType>,
    ) {
        // Given a resolved source checkpoint and a derived segment's own type claim.
        let filename = "00000000000000000000.checkpoint.parquet";
        let source = segment(filename, None, Some(SelectedCheckpointType::V2)).unwrap();
        let mut derived = segment(filename, None, new_type).unwrap();
        if different_checkpoint {
            derived.listed.checkpoint_parts[0].location.location = url::Url::parse(
                "memory:///other/_delta_log/00000000000000000000.checkpoint.parquet",
            )
            .unwrap();
        }
        // When a combined segment is constructed with the observation selected for the update.
        let checkpoint_type = derived.checkpoint_type_for_update(&source, source.checkpoint_type());
        let combined = LogSegment::try_new(
            derived.listed,
            derived.log_root,
            Some(derived.end_version),
            derived.last_checkpoint_metadata,
            checkpoint_type,
        )
        .unwrap();
        // Then only an unresolved claim for the same checkpoint inherits the source's type.
        assert_eq!(combined.checkpoint_type(), expected);
        assert_eq!(source.checkpoint_type(), Some(SelectedCheckpointType::V2));
    }

    #[rstest::rstest]
    #[case("00000000000000000000.json", SelectedCheckpointType::V1)]
    #[case("00000000000000000000.json", SelectedCheckpointType::V2)]
    #[case(
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet",
        SelectedCheckpointType::V2
    )]
    #[case(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet",
        SelectedCheckpointType::V1
    )]
    fn checkpoint_type_rejects_inconsistent_claims(
        #[case] filename: &str,
        #[case] checkpoint_type: SelectedCheckpointType,
    ) {
        // Given a path whose checkpoint presence or filename contradicts the supplied type.
        // When the segment is constructed with that type claim.
        let result = segment(filename, None, Some(checkpoint_type));
        // Then validation reports a conflicting checkpoint type.
        assert!(matches!(
            result,
            Err(KernelError::SnapshotHint(error)) if matches!(*error, SnapshotHintError::CheckpointTypeConflict)
        ));
    }
}
