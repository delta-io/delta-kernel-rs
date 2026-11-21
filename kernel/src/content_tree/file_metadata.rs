//! File metadata fields: `add` fields the table stores per file in the content tree rather than as
//! table columns. Each one has a field ID. An entry stores its value as the `lower_bound`
//! of the stats struct for that ID in `content_stats`. The IDs are recorded in the
//! `delta.adaptiveMetadata` domain.

use serde::Deserialize;

use crate::actions::{CheckpointAction, DomainMetadata};
use crate::{KernelError, KernelResult};

/// The domain whose configuration maps file metadata fields to their IDs.
pub(crate) const ADAPTIVE_METADATA_DOMAIN_NAME: &str = "delta.adaptiveMetadata";

/// The field IDs of the file metadata fields a table maps. The default maps none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FileMetadataFieldIds {
    /// The field ID of `add.clusteringProvider`, or `None` when the table does not map it.
    pub(crate) clustering_provider: Option<i32>,
}

/// The `delta.adaptiveMetadata` configuration. Unknown fields are ignored for forward
/// compatibility.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdaptiveMetadataConfiguration {
    file_metadata_field_ids: FileMetadataFieldIds,
}

impl FileMetadataFieldIds {
    /// Parses the configuration string of the `delta.adaptiveMetadata` domain.
    pub(crate) fn try_from_configuration(configuration: &str) -> Result<Self, String> {
        let config: AdaptiveMetadataConfiguration =
            serde_json::from_str(configuration).map_err(|err| {
                format!(
                    "{ADAPTIVE_METADATA_DOMAIN_NAME} configuration is not valid: {err}, \
                     found {configuration}"
                )
            })?;
        Ok(config.file_metadata_field_ids)
    }

    /// Reads the mapping from the checkpoint action's inline domain metadata. System domains are
    /// always inline in the checkpoint action, so sidecars are never consulted.
    pub(crate) fn try_from_checkpoint(checkpoint: &CheckpointAction) -> KernelResult<Self> {
        let domains: Vec<&DomainMetadata> = checkpoint
            .domain_metadata
            .iter()
            .filter(|domain| domain.domain() == ADAPTIVE_METADATA_DOMAIN_NAME)
            .collect();
        match domains.as_slice() {
            [] => Ok(Self::default()),
            [domain] if domain.is_removed() => Err(KernelError::invalid_checkpoint(format!(
                "{ADAPTIVE_METADATA_DOMAIN_NAME} domain metadata in the checkpoint action is removed"
            ))),
            [domain] => Self::try_from_configuration(domain.configuration())
                .map_err(KernelError::invalid_checkpoint),
            _ => Err(KernelError::invalid_checkpoint(format!(
                "checkpoint action has {} {ADAPTIVE_METADATA_DOMAIN_NAME} domain metadata entries, \
                 expected at most one",
                domains.len()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::content_tree::test_utils::{CLUSTERING_FIELD_ID, clustering_mapping_domain};
    use crate::unit_test_utils::adaptive_metadata_fixtures::checkpoint_action_with_root;
    use crate::unit_test_utils::assert_result_error_with_message;

    fn checkpoint_with_domains(domains: Vec<DomainMetadata>) -> CheckpointAction {
        CheckpointAction {
            domain_metadata: domains,
            ..checkpoint_action_with_root("metadata/root.parquet", 1, 1).unwrap()
        }
    }

    #[rstest]
    #[case::clustering_provider(
        r#"{"fileMetadataFieldIds":{"clusteringProvider":100}}"#,
        Some(100)
    )]
    #[case::unknown_map_key(
        r#"{"fileMetadataFieldIds":{"clusteringProvider":100,"other":5}}"#,
        Some(100)
    )]
    #[case::unknown_top_level_field(
        r#"{"fileMetadataFieldIds":{"clusteringProvider":100},"version":2}"#,
        Some(100)
    )]
    #[case::no_mapped_fields(r#"{"fileMetadataFieldIds":{}}"#, None)]
    fn try_from_configuration_parses_mapping(
        #[case] configuration: &str,
        #[case] clustering_provider: Option<i32>,
    ) {
        let ids = FileMetadataFieldIds::try_from_configuration(configuration).unwrap();
        assert_eq!(ids.clustering_provider, clustering_provider);
    }

    #[rstest]
    #[case::not_json("{")]
    #[case::missing_field_ids(r#"{"other":1}"#)]
    #[case::non_integer_id(r#"{"fileMetadataFieldIds":{"clusteringProvider":"x"}}"#)]
    #[case::id_outside_i32(r#"{"fileMetadataFieldIds":{"clusteringProvider":2147483648}}"#)]
    fn try_from_configuration_names_the_domain_and_value_on_error(#[case] configuration: &str) {
        let err = FileMetadataFieldIds::try_from_configuration(configuration).unwrap_err();
        assert!(err.starts_with(ADAPTIVE_METADATA_DOMAIN_NAME), "{err}");
        assert!(err.contains(configuration), "{err}");
    }

    #[rstest]
    #[case::no_domain(
        vec![DomainMetadata::new("other.domain".to_string(), "{}".to_string())],
        Ok(None)
    )]
    #[case::inline_domain(
        vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)],
        Ok(Some(CLUSTERING_FIELD_ID))
    )]
    #[case::malformed_configuration(
        vec![DomainMetadata::new(ADAPTIVE_METADATA_DOMAIN_NAME.to_string(), "not json".to_string())],
        Err("Invalid Checkpoint: delta.adaptiveMetadata configuration is not valid:")
    )]
    #[case::duplicate_entries(
        vec![
            clustering_mapping_domain(CLUSTERING_FIELD_ID),
            clustering_mapping_domain(CLUSTERING_FIELD_ID),
        ],
        Err("2 delta.adaptiveMetadata domain metadata entries")
    )]
    #[case::removed_entry(
        vec![DomainMetadata::remove(
            ADAPTIVE_METADATA_DOMAIN_NAME.to_string(),
            r#"{"fileMetadataFieldIds":{}}"#.to_string(),
        )],
        Err("delta.adaptiveMetadata domain metadata in the checkpoint action is removed")
    )]
    fn try_from_checkpoint_reads_or_rejects_inline_domain(
        #[case] domains: Vec<DomainMetadata>,
        #[case] expected: Result<Option<i32>, &str>,
    ) {
        let checkpoint = checkpoint_with_domains(domains);
        let result = FileMetadataFieldIds::try_from_checkpoint(&checkpoint);
        match expected {
            Ok(clustering_provider) => {
                assert_eq!(result.unwrap().clustering_provider, clustering_provider)
            }
            Err(message) => assert_result_error_with_message(result, message),
        }
    }
}
