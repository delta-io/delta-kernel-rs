//! Clustering column support for Delta tables.
//!
//! This module provides functionality for reading clustering columns from domain metadata.
//! Per the Delta protocol, writers MUST write per-file statistics for clustering columns.
//!
//! Clustering columns are stored in domain metadata under the `delta.clustering` domain
//! and are cached at snapshot construction time for efficient access.

use serde::Deserialize;

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};

/// Domain metadata structure for clustering columns.
///
/// This is deserialized from the JSON configuration stored in the
/// `delta.clustering` domain metadata.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusteringDomainMetadata {
    clustering_columns: Vec<String>,
}

impl ClusteringDomainMetadata {
    /// The domain name for clustering metadata.
    const DOMAIN_NAME: &'static str = "delta.clustering";

    /// Parses clustering columns from a JSON configuration string.
    ///
    /// Returns `Ok(columns)` if the configuration is valid, or an error if malformed.
    fn parse_clustering_columns(json_str: &str) -> DeltaResult<Vec<ColumnName>> {
        let metadata: ClusteringDomainMetadata = serde_json::from_str(json_str)?;
        let columns = metadata
            .clustering_columns
            .into_iter()
            .map(|name| name.parse())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(columns)
    }

    /// Reads clustering columns from the log segment's domain metadata.
    ///
    /// Returns `Ok(Some(columns))` if clustering is enabled for the table,
    /// `Ok(None)` if clustering is not enabled, or an error if the metadata
    /// is malformed.
    pub(crate) fn get_clustering_columns(
        log_segment: &LogSegment,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        let config = domain_metadata_configuration(log_segment, Self::DOMAIN_NAME, engine)?;
        match config {
            Some(json_str) => Ok(Some(Self::parse_clustering_columns(&json_str)?)),
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
    fn test_parse_clustering_columns_nested() {
        let json = r#"{"clusteringColumns": ["id", "user.address.city", "metadata.tags"]}"#;
        let columns = ClusteringDomainMetadata::parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["user", "address", "city"]),
                ColumnName::new(["metadata", "tags"]),
            ]
        );
    }
}
