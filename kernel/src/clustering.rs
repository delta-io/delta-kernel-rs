//! Clustering domain metadata parsing for clustered Delta tables.
//!
//! This module provides functionality to parse clustering column information from
//! Delta table domain metadata. Clustering columns are stored as physical column IDs
//! when column mapping is enabled.

use serde::Deserialize;

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};

/// Domain metadata for clustered tables.
///
/// The clustering domain metadata is stored as JSON in the format:
/// `{"clusteringColumns": ["col-uuid1", "col-uuid2"]}`
///
/// Note: Column names are physical column IDs when column mapping is enabled.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusteringDomainMetadata {
    clustering_columns: Vec<String>,
}

impl ClusteringDomainMetadata {
    /// Domain name for clustering metadata.
    /// This is an internal domain (delta.*) so it uses the internal API to access it.
    pub(crate) const DOMAIN_NAME: &'static str = "delta.clustering";

    /// Parse clustering columns from the log segment's domain metadata.
    ///
    /// Returns physical column names/IDs as stored in the domain metadata.
    /// Returns `None` if the table is not clustered (no clustering domain metadata).
    pub(crate) fn get_clustering_columns(
        log_segment: &LogSegment,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        let config = domain_metadata_configuration(log_segment, Self::DOMAIN_NAME, engine)?;

        match config {
            Some(json_str) => {
                let metadata: ClusteringDomainMetadata = serde_json::from_str(&json_str)?;
                let columns = metadata
                    .clustering_columns
                    .into_iter()
                    .map(|name| ColumnName::new([name]))
                    .collect();
                Ok(Some(columns))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_clustering_metadata() {
        let json = r#"{"clusteringColumns": ["col1", "col2", "col3"]}"#;
        let metadata: ClusteringDomainMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.clustering_columns, vec!["col1", "col2", "col3"]);
    }

    #[test]
    fn test_parse_clustering_metadata_empty() {
        let json = r#"{"clusteringColumns": []}"#;
        let metadata: ClusteringDomainMetadata = serde_json::from_str(json).unwrap();
        assert!(metadata.clustering_columns.is_empty());
    }

    #[test]
    fn test_parse_clustering_metadata_with_uuid_columns() {
        // Physical column IDs when column mapping is enabled
        let json = r#"{"clusteringColumns": ["col-abc123", "col-def456"]}"#;
        let metadata: ClusteringDomainMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(
            metadata.clustering_columns,
            vec!["col-abc123", "col-def456"]
        );
    }
}
