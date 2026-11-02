use std::sync::LazyLock;

use super::LogSegment;
use crate::actions::CHECKPOINT_METADATA_NAME;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::error::SnapshotHintError;
use crate::path::LogPathFileType;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType, StructType};
use crate::snapshot::CheckpointType;
use crate::{KernelError, KernelResult, RowVisitor};

impl LogSegment {
    pub(crate) fn checkpoint_type(&self) -> Option<CheckpointType> {
        match self
            .listed
            .checkpoint_parts
            .first()
            .map(|part| &part.file_type)
        {
            None => None,
            Some(LogPathFileType::MultiPartCheckpoint { .. }) => Some(CheckpointType::V1),
            Some(LogPathFileType::UuidCheckpoint) => Some(CheckpointType::V2),
            _ => {
                if let Some(checkpoint_type) = self.checkpoint_type.get() {
                    return Some(*checkpoint_type);
                }
                if self
                    .checkpoint_hint_schema()
                    .is_some_and(|schema| !schema.contains(CHECKPOINT_METADATA_NAME))
                {
                    return Some(CheckpointType::V1);
                }
                self.checkpoint_type_hint
            }
        }
    }

    pub(crate) fn observe_checkpoint_schema(&self, schema: &StructType) {
        // V2 requires a checkpointMetadata action. Its absence from the full schema proves V1;
        // presence of a nullable column alone does not prove a non-null action exists.
        if !schema.contains(CHECKPOINT_METADATA_NAME) {
            self.observe_checkpoint_type(CheckpointType::V1);
        }
    }

    pub(crate) fn observe_checkpoint_type(&self, checkpoint_type: CheckpointType) {
        let _ = self.checkpoint_type.set(checkpoint_type);
    }

    pub(crate) fn inherit_checkpoint_observation(&mut self, previous: &Self) {
        if self
            .listed
            .checkpoint_parts
            .iter()
            .map(|part| &part.location.location)
            .eq(previous
                .listed
                .checkpoint_parts
                .iter()
                .map(|part| &part.location.location))
        {
            self.checkpoint_type = previous.checkpoint_type.clone();
            self.checkpoint_type_hint = previous.checkpoint_type_hint;
        }
    }

    pub(crate) fn with_checkpoint_type(
        mut self,
        checkpoint_type: Option<CheckpointType>,
    ) -> KernelResult<Self> {
        if let Some(checkpoint_type) = checkpoint_type {
            if self.checkpoint_version.is_none()
                || self
                    .checkpoint_type()
                    .is_some_and(|established| checkpoint_type != established)
            {
                return Err(SnapshotHintError::CheckpointProvenanceConflict.into());
            }
        }
        // Keep connector claims separate so ordinary reads can establish the actual type.
        self.checkpoint_type_hint = checkpoint_type;
        Ok(self)
    }
}

#[derive(Default)]
pub(super) struct CheckpointMetadataVisitor {
    pub found: bool,
}

impl RowVisitor for CheckpointMetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![column_name!("checkpointMetadata.version")],
                vec![DataType::LONG],
            )
                .into()
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [version] = getters else {
            return Err(KernelError::internal_error(
                "Expected checkpointMetadata.version getter",
            ));
        };
        for row in 0..row_count {
            let value: Option<i64> = version.get_opt(row, "checkpointMetadata.version")?;
            self.found |= value.is_some();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::CHECKPOINT_METADATA_FIELD;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::path::ParsedLogPath;
    use crate::FileMeta;

    fn segment(filename: &str) -> LogSegment {
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
        LogSegment::try_new(files, root, Some(0), None).unwrap()
    }

    #[rstest::rstest]
    #[case("00000000000000000000.json", None)]
    #[case("00000000000000000000.checkpoint.parquet", None)]
    #[case(
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet",
        Some(CheckpointType::V1)
    )]
    #[case(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet",
        Some(CheckpointType::V2)
    )]
    fn checkpoint_filename_evidence(
        #[case] filename: &str,
        #[case] expected: Option<CheckpointType>,
    ) {
        assert_eq!(segment(filename).checkpoint_type(), expected);
    }

    #[rstest::rstest]
    #[case("00000000000000000000.json", CheckpointType::V1)]
    #[case("00000000000000000000.json", CheckpointType::V2)]
    #[case(
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet",
        CheckpointType::V2
    )]
    #[case(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet",
        CheckpointType::V1
    )]
    fn checkpoint_type_rejects_inconsistent_claims(
        #[case] filename: &str,
        #[case] checkpoint_type: CheckpointType,
    ) {
        assert!(matches!(
            segment(filename).with_checkpoint_type(Some(checkpoint_type)),
            Err(KernelError::SnapshotHint(error)) if matches!(*error, SnapshotHintError::CheckpointProvenanceConflict)
        ));
    }

    #[test]
    fn checkpoint_reads_override_hint_claims() {
        let segment = segment("00000000000000000000.checkpoint.parquet")
            .with_checkpoint_type(Some(CheckpointType::V1))
            .unwrap();
        assert_eq!(segment.checkpoint_type(), Some(CheckpointType::V1));
        segment.observe_checkpoint_type(CheckpointType::V2);
        assert_eq!(segment.checkpoint_type(), Some(CheckpointType::V2));
        let mut inherited = segment.clone();
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(inherited.checkpoint_type(), Some(CheckpointType::V2));
    }

    #[test]
    fn nullable_metadata_column_is_not_v2_evidence_and_observations_do_not_affect_equality() {
        let segment = segment("00000000000000000000.checkpoint.parquet");
        let independent = LogSegment::try_new(
            segment.listed.clone(),
            segment.log_root.clone(),
            Some(0),
            None,
        )
        .unwrap();
        segment.observe_checkpoint_schema(&StructType::new_unchecked([
            CHECKPOINT_METADATA_FIELD.clone()
        ]));
        assert_eq!(segment.checkpoint_type(), None);
        segment.observe_checkpoint_type(CheckpointType::V2);
        assert_eq!(segment.checkpoint_type(), Some(CheckpointType::V2));
        assert_eq!(segment, independent);
        assert_eq!(independent.checkpoint_type(), None);
        let mut inherited = independent;
        // Listing metadata can change without changing the immutable checkpoint's identity.
        inherited.listed.checkpoint_parts[0].location.last_modified += 1;
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(inherited.checkpoint_type(), Some(CheckpointType::V2));
        inherited.listed.checkpoint_parts[0].location.location =
            url::Url::parse("memory:///other/_delta_log/00000000000000000000.checkpoint.parquet")
                .unwrap();
        inherited.checkpoint_type = Default::default();
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(inherited.checkpoint_type(), None);
    }
}
