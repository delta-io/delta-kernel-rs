//! Clustering support for Delta tables.
//!
//! This module provides support for clustering in Delta tables. Clustering
//! allows data to be physically co-located based on column values, enabling efficient
//! data skipping without the directory overhead of partitioning.
//!
//! Clustering metadata is stored as domain metadata with the domain name `delta.clustering`.

use serde::{Deserialize, Serialize};

use crate::actions::DomainMetadata;
use crate::schema::{ColumnName, DataType, StructType};
use crate::table_features::{get_physical_name, ColumnMappingMode};
use crate::{DeltaResult, Error};

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
    /// Creates a new clustering metadata domain using logical column names.
    ///
    /// This method is for tables **without column mapping enabled**. It stores
    /// logical column names directly. Use [`new_with_schema`] when column mapping
    /// is enabled to resolve and store physical names instead.
    ///
    /// # Arguments
    ///
    /// * `columns` - The columns to cluster by. Each column is represented as a
    ///   [`ColumnName`] which supports nested paths.
    #[cfg(test)]
    pub(crate) fn new_with_logical_names(columns: &[ColumnName]) -> Self {
        let clustering_columns = columns
            .iter()
            .map(|col| col.path().iter().map(|s| s.to_string()).collect())
            .collect();
        Self { clustering_columns }
    }

    /// Creates a new clustering metadata domain, resolving logical column names to physical names.
    ///
    /// When column mapping is enabled (`name` or `id` mode), this method resolves
    /// logical column names to their physical names using the schema metadata.
    /// When column mapping is disabled (`none` mode), logical names are stored as-is.
    ///
    /// # Arguments
    ///
    /// * `columns` - The columns to cluster by (logical names)
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
    pub(crate) fn new_with_physical_names(
        columns: &[ColumnName],
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        let clustering_columns = columns
            .iter()
            .map(|col| {
                resolve_clustering_column_to_physical_path(col.path(), schema, column_mapping_mode)
            })
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

    /// Creates a clustering metadata domain from a JSON configuration string.
    ///
    /// # Arguments
    ///
    /// * `json` - The JSON string containing the clustering metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON cannot be parsed.
    #[cfg(test)]
    pub(crate) fn from_json(json: &str) -> DeltaResult<Self> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Resolves a single clustering column's logical field path to physical names.
///
/// This function is called once per clustering column. It takes a single column's
/// field path (which may be nested, e.g., `["address", "city"]`) and resolves each
/// field name to its physical name using the schema's column mapping metadata.
///
/// When column mapping is disabled (`None` mode), the logical names are returned as-is.
///
/// # Arguments
///
/// * `clustering_column_path` - A single clustering column's field path as logical names
///   (e.g., `["address", "city"]` for a nested column, or `["user_id"]` for a top-level column)
/// * `schema` - The table schema with column mapping metadata (if enabled)
/// * `column_mapping_mode` - The column mapping mode (`None`, `Name`, or `Id`)
///
/// # Returns
///
/// The resolved field path as physical names (if column mapping enabled) or logical names (if disabled).
///
/// # Example
///
/// Given a schema with column mapping enabled:
/// ```text
/// Schema:
///   - "address" (physicalName: "col-abc123")
///       - "city" (physicalName: "col-def456")
///
/// Input:  clustering_column_path = ["address", "city"]
/// Output: ["col-abc123", "col-def456"]
/// ```
///
/// The function traverses the schema using logical names to find each field,
/// then extracts the physical name from the field's metadata.
fn resolve_clustering_column_to_physical_path(
    clustering_column_path: &[String],
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<Vec<String>> {
    if column_mapping_mode == ColumnMappingMode::None {
        // No column mapping - use logical names as-is
        return Ok(clustering_column_path.to_vec());
    }

    // Column mapping enabled - resolve to physical names
    let mut result = Vec::with_capacity(clustering_column_path.len());
    let mut current_schema = schema;
    let last_field_index = clustering_column_path.len() - 1;

    for (field_index, field_name) in clustering_column_path.iter().enumerate() {
        let field = current_schema.field(field_name).ok_or_else(|| {
            Error::generic(format!(
                "Clustering column '{}' not found in schema",
                clustering_column_path.join(".")
            ))
        })?;

        // Get physical name (falls back to logical name if not present)
        result.push(get_physical_name(field).to_string());

        // If not the last element, we need to descend into a struct
        // The iteration here allows us to handle nested clustering columns and
        // go deeper into the schema. The Delta protocol however doesn't specify a
        // limit on the depth of the nesting. FIX-ME: We need to add a limit.
        if field_index < last_field_index {
            match &field.data_type {
                DataType::Struct(inner) => {
                    current_schema = inner;
                }
                _ => {
                    return Err(Error::generic(format!(
                        "Cannot traverse into non-struct field '{}' in clustering column path '{}'",
                        field_name,
                        clustering_column_path.join(".")
                    )));
                }
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clustering_metadata_new_with_logical_names() {
        let columns = vec![
            ColumnName::new(["col1"]),
            ColumnName::new(["nested", "field"]),
        ];
        let metadata = ClusteringMetadataDomain::new_with_logical_names(&columns);

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
        let metadata = ClusteringMetadataDomain::new_with_logical_names(&[]);
        assert!(metadata.clustering_columns().is_empty());
    }

    #[test]
    fn test_clustering_metadata_serialization() {
        let columns = vec![
            ColumnName::new(["col1"]),
            ColumnName::new(["nested", "field"]),
        ];
        let metadata = ClusteringMetadataDomain::new_with_logical_names(&columns);

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
        let metadata = ClusteringMetadataDomain::new_with_logical_names(&columns);

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
