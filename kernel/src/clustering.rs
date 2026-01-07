//! Clustering support for Delta tables.
//!
//! This module provides support for clustering in Delta tables. Clustering
//! allows data to be physically co-located based on column values, enabling efficient
//! data skipping without the directory overhead of partitioning.
//!
//! Clustering metadata is stored as domain metadata with the domain name `delta.clustering`.

use serde::{Deserialize, Serialize};

use crate::actions::DomainMetadata;
use crate::schema::{ColumnName, StructType};
use crate::table_features::{resolve_logical_to_physical_path, ColumnMappingMode};
use crate::DeltaResult;

/// The domain name for clustering metadata in Delta tables.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";

/// Represents the clustering metadata stored as domain metadata in Delta tables.
///
/// This struct is serialized to JSON and stored in the `delta.clustering` domain
/// metadata action. The clustering columns are stored as a list of column paths,
/// where each path is a list of field names (to support nested columns).
///
/// If column mapping is enabled on the table, physical column names are stored;
/// otherwise, logical column names are used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusteringMetadataDomain {
    /// The columns used for clustering, stored as paths (list of field names).
    /// For example: `[["col1"], ["nested", "field"]]`
    clustering_columns: Vec<Vec<String>>,
}

impl ClusteringMetadataDomain {
    /// Creates a new clustering metadata domain from column names.
    ///
    /// When column mapping is enabled (`name` or `id` mode), this method resolves
    /// logical column names to their physical names using the schema metadata.
    /// When column mapping is disabled (`none` mode), logical names are stored as-is.
    ///
    /// # Arguments
    ///
    /// * `cluster_columns` - The columns to cluster by (logical names)
    /// * `schema` - The table schema with column mapping metadata
    /// * `column_mapping_mode` - The column mapping mode for the table
    ///
    /// # Returns
    ///
    /// A `ClusteringMetadataDomain` containing the resolved column paths (physical names
    /// if column mapping is enabled, logical names otherwise).
    ///
    /// # Errors
    ///
    /// Returns an error if a clustering column cannot be found in the schema.
    pub(crate) fn new(
        cluster_columns: &[ColumnName],
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        let clustering_columns = cluster_columns
            .iter()
            .map(|col| resolve_logical_to_physical_path(col.path(), schema, column_mapping_mode))
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Self { clustering_columns })
    }

    /// Returns the clustering columns as a slice of column paths.
    #[cfg(test)]
    pub(crate) fn clustering_columns(&self) -> &[Vec<String>] {
        &self.clustering_columns
    }

    /// Converts this clustering metadata to a [`DomainMetadata`] action.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata cannot be serialized to JSON.
    pub(crate) fn to_domain_metadata(&self) -> DeltaResult<DomainMetadata> {
        let configuration = serde_json::to_string(self)?;
        Ok(DomainMetadata::new(
            CLUSTERING_DOMAIN_NAME.to_string(),
            configuration,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    /// Helper to create a simple schema for testing
    fn test_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::nullable("col1", DataType::STRING),
            StructField::nullable(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::nullable("field", DataType::STRING),
                ]))),
            ),
        ])
    }

    impl ClusteringMetadataDomain {
        /// Creates a clustering metadata domain from a JSON configuration string.
        fn from_json(json: &str) -> DeltaResult<Self> {
            Ok(serde_json::from_str(json)?)
        }
    }

    #[test]
    fn test_clustering_metadata_new() {
        let columns = vec![
            ColumnName::new(["col1"]),
            ColumnName::new(["nested", "field"]),
        ];
        let schema = test_schema();
        let metadata =
            ClusteringMetadataDomain::new(&columns, &schema, ColumnMappingMode::None).unwrap();

        assert_eq!(
            metadata.clustering_columns(),
            &[
                vec!["col1".to_string()],
                vec!["nested".to_string(), "field".to_string()]
            ]
        );
    }

    #[test]
    fn test_clustering_metadata_empty() {
        let schema = StructType::new_unchecked(vec![]);
        let metadata =
            ClusteringMetadataDomain::new(&[], &schema, ColumnMappingMode::None).unwrap();
        assert!(metadata.clustering_columns().is_empty());
    }

    #[test]
    fn test_clustering_metadata_serialization() {
        let columns = vec![
            ColumnName::new(["col1"]),
            ColumnName::new(["nested", "field"]),
        ];
        let schema = test_schema();
        let metadata =
            ClusteringMetadataDomain::new(&columns, &schema, ColumnMappingMode::None).unwrap();

        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("clusteringColumns"));
        assert!(json.contains("col1"));
        assert!(json.contains("nested"));
        assert!(json.contains("field"));

        // Round-trip test
        let deserialized: ClusteringMetadataDomain = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metadata);
    }

    #[test]
    fn test_clustering_metadata_to_domain_metadata() {
        let columns = vec![ColumnName::new(["col1"])];
        let schema = test_schema();
        let metadata =
            ClusteringMetadataDomain::new(&columns, &schema, ColumnMappingMode::None).unwrap();

        let domain_metadata = metadata.to_domain_metadata().unwrap();
        assert_eq!(domain_metadata.domain(), CLUSTERING_DOMAIN_NAME);

        // Verify the configuration can be parsed back
        let parsed = ClusteringMetadataDomain::from_json(domain_metadata.configuration()).unwrap();
        assert_eq!(parsed, metadata);
    }

    #[test]
    fn test_clustering_metadata_from_json() {
        let json = r#"{"clusteringColumns":[["col1"],["nested","field"]]}"#;
        let metadata = ClusteringMetadataDomain::from_json(json).unwrap();

        assert_eq!(
            metadata.clustering_columns(),
            &[
                vec!["col1".to_string()],
                vec!["nested".to_string(), "field".to_string()]
            ]
        );
    }
}
