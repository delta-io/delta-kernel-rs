use std::sync::LazyLock;

use super::LogSegment;
use crate::actions::CHECKPOINT_METADATA_NAME;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::error::SnapshotHintError;
use crate::path::LogPathFileType;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType, StructType};
use crate::snapshot::SelectedCheckpointKind;
use crate::{KernelError, KernelResult, RowVisitor};

impl LogSegment {
    pub(crate) fn selected_checkpoint_kind(&self) -> SelectedCheckpointKind {
        match self
            .listed
            .checkpoint_parts
            .first()
            .map(|part| &part.file_type)
        {
            Option::None => SelectedCheckpointKind::None,
            Some(LogPathFileType::MultiPartCheckpoint { .. }) => SelectedCheckpointKind::V1,
            Some(LogPathFileType::UuidCheckpoint) => SelectedCheckpointKind::V2,
            _ => {
                if let Some(kind) = self.checkpoint_kind.get() {
                    return *kind;
                }
                if self
                    .checkpoint_hint_schema()
                    .is_some_and(|schema| !schema.contains(CHECKPOINT_METADATA_NAME))
                {
                    return SelectedCheckpointKind::V1;
                }
                self.checkpoint_kind_hint
            }
        }
    }

    pub(crate) fn observe_checkpoint_schema(&self, schema: &StructType) {
        // V2 requires a checkpointMetadata action. Its absence from the full schema proves V1;
        // presence of a nullable column alone does not prove a non-null action exists.
        if !schema.contains(CHECKPOINT_METADATA_NAME) {
            self.observe_checkpoint_kind(SelectedCheckpointKind::V1);
        }
    }

    pub(crate) fn observe_checkpoint_kind(&self, kind: SelectedCheckpointKind) {
        if matches!(
            kind,
            SelectedCheckpointKind::V1 | SelectedCheckpointKind::V2
        ) {
            let _ = self.checkpoint_kind.set(kind);
        }
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
            self.checkpoint_kind = previous.checkpoint_kind.clone();
            self.checkpoint_kind_hint = previous.checkpoint_kind_hint;
        }
    }

    pub(crate) fn with_checkpoint_kind(
        mut self,
        kind: SelectedCheckpointKind,
    ) -> KernelResult<Self> {
        let established = self.selected_checkpoint_kind();
        if kind != SelectedCheckpointKind::Unknown
            && established != SelectedCheckpointKind::Unknown
            && kind != established
        {
            return Err(SnapshotHintError::CheckpointProvenanceConflict.into());
        }
        if kind == SelectedCheckpointKind::None && self.checkpoint_version.is_some() {
            return Err(SnapshotHintError::CheckpointProvenanceConflict.into());
        }
        // Keep connector claims separate so ordinary reads can establish the actual kind.
        self.checkpoint_kind_hint = kind;
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
    #[case("00000000000000000000.json", SelectedCheckpointKind::None)]
    #[case(
        "00000000000000000000.checkpoint.parquet",
        SelectedCheckpointKind::Unknown
    )]
    #[case(
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet",
        SelectedCheckpointKind::V1
    )]
    #[case(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.parquet",
        SelectedCheckpointKind::V2
    )]
    fn checkpoint_filename_evidence(
        #[case] filename: &str,
        #[case] expected: SelectedCheckpointKind,
    ) {
        assert_eq!(segment(filename).selected_checkpoint_kind(), expected);
    }

    #[test]
    fn checkpoint_reads_override_hint_claims() {
        let segment = segment("00000000000000000000.checkpoint.parquet")
            .with_checkpoint_kind(SelectedCheckpointKind::V1)
            .unwrap();
        assert_eq!(
            segment.selected_checkpoint_kind(),
            SelectedCheckpointKind::V1
        );
        segment.observe_checkpoint_kind(SelectedCheckpointKind::V2);
        assert_eq!(
            segment.selected_checkpoint_kind(),
            SelectedCheckpointKind::V2
        );
        let mut inherited = segment.clone();
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(
            inherited.selected_checkpoint_kind(),
            SelectedCheckpointKind::V2
        );
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
        assert_eq!(
            segment.selected_checkpoint_kind(),
            SelectedCheckpointKind::Unknown
        );
        segment.observe_checkpoint_kind(SelectedCheckpointKind::V2);
        assert_eq!(
            segment.selected_checkpoint_kind(),
            SelectedCheckpointKind::V2
        );
        assert_eq!(segment, independent);
        assert_eq!(
            independent.selected_checkpoint_kind(),
            SelectedCheckpointKind::Unknown
        );
        let mut inherited = independent;
        // Listing metadata can change without changing the immutable checkpoint's identity.
        inherited.listed.checkpoint_parts[0].location.last_modified += 1;
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(
            inherited.selected_checkpoint_kind(),
            SelectedCheckpointKind::V2
        );
        inherited.listed.checkpoint_parts[0].location.location =
            url::Url::parse("memory:///other/_delta_log/00000000000000000000.checkpoint.parquet")
                .unwrap();
        inherited.checkpoint_kind = Default::default();
        inherited.inherit_checkpoint_observation(&segment);
        assert_eq!(
            inherited.selected_checkpoint_kind(),
            SelectedCheckpointKind::Unknown
        );
    }
}
