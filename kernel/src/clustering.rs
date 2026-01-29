//! Clustering column support for Delta tables.
//!
//! This module provides functionality for reading clustering columns from domain metadata.
//! Per the Delta protocol, writers MUST write per-file statistics for clustering columns.
//!
//! Clustering columns are stored in domain metadata under the `delta.clustering` domain
//! as a JSON object with a `clusteringColumns` field containing an array of column paths,
//! where each path is an array of field names (to handle nested columns).

use serde::Deserialize;

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};

/// Domain metadata structure for clustering columns.
///
/// This is deserialized from the JSON configuration stored in the
/// `delta.clustering` domain metadata. Each clustering column is represented
/// as an array of field names to support nested columns.
///
/// The column names are **physical names**. If column mapping is enabled, these will be
/// the physical column identifiers (e.g., `col-uuid`); otherwise, they match the logical names.
///
/// Example JSON:
/// ```json
/// {"clusteringColumns": [["col1"], ["user", "address", "city"]]}
/// ```
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusteringDomainMetadata {
    clustering_columns: Vec<Vec<String>>,
}

/// The domain name for clustering metadata.
const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";

/// Parses clustering columns from a JSON configuration string.
///
/// Returns `Ok(columns)` if the configuration is valid, or an error if malformed.
fn parse_clustering_columns(json_str: &str) -> DeltaResult<Vec<ColumnName>> {
    let metadata: ClusteringDomainMetadata = serde_json::from_str(json_str)?;
    Ok(metadata
        .clustering_columns
        .into_iter()
        .map(ColumnName::new)
        .collect())
}

/// Reads clustering columns from the log segment's domain metadata.
///
/// This function performs a log scan to find the clustering domain metadata.
/// Callers should first check if the `ClusteredTable` feature is enabled via
/// the protocol before calling this function to avoid unnecessary I/O.
/// See [`Snapshot::get_clustering_columns`] which performs this check.
///
/// Returns `Ok(Some(columns))` if clustering domain metadata exists,
/// `Ok(None)` if no clustering domain metadata is found, or an error if the
/// metadata is malformed.
///
/// [`Snapshot::get_clustering_columns`]: crate::snapshot::Snapshot::get_clustering_columns
pub(crate) fn get_clustering_columns(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<Option<Vec<ColumnName>>> {
    let config = domain_metadata_configuration(log_segment, CLUSTERING_DOMAIN_NAME, engine)?;
    match config {
        Some(json_str) => Ok(Some(parse_clustering_columns(&json_str)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_clustering_columns_simple() {
        // Simple top-level columns: each is a single-element array
        let json = r#"{"clusteringColumns": [["col1"], ["col2"], ["col3"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["col1"]),
                ColumnName::new(["col2"]),
                ColumnName::new(["col3"]),
            ]
        );
    }

    #[test]
    fn test_parse_clustering_columns_empty() {
        let json = r#"{"clusteringColumns": []}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_parse_clustering_columns_nested() {
        // Nested columns are represented as arrays of field names
        let json =
            r#"{"clusteringColumns": [["id"], ["user", "address", "city"], ["metadata", "tags"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["user", "address", "city"]),
                ColumnName::new(["metadata", "tags"]),
            ]
        );
    }

    #[test]
    fn test_parse_clustering_columns_with_special_characters() {
        // Column names with dots, commas, or other special characters are stored as-is
        // since each path component is a separate array element
        let json = r#"{"clusteringColumns": [["col.with.dot"], ["col,with,comma"], ["normal"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["col.with.dot"]),
                ColumnName::new(["col,with,comma"]),
                ColumnName::new(["normal"]),
            ]
        );
    }

    #[test]
    fn test_parse_clustering_columns_column_mapping_uuids() {
        // With column mapping enabled, physical names are UUIDs
        let json = r#"{"clusteringColumns": [["col-daadafd7-7c20-4697-98f8-bff70199b1f9"], ["col-5abe0e80-cf57-47ac-9ffc-a861a3d1077e"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["col-daadafd7-7c20-4697-98f8-bff70199b1f9"]),
                ColumnName::new(["col-5abe0e80-cf57-47ac-9ffc-a861a3d1077e"]),
            ]
        );
    }

    #[test]
    fn test_parse_clustering_columns_tolerates_unknown_fields() {
        // Per the protocol, Delta clients should tolerate unrecognized fields
        let json =
            r#"{"clusteringColumns": [["col1"], ["col2"]], "foo": "bar", "futureField": 123}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![ColumnName::new(["col1"]), ColumnName::new(["col2"]),]
        );
    }

    #[test]
    fn test_parse_clustering_columns_deeply_nested() {
        // Deeply nested column path
        let json = r#"{"clusteringColumns": [["a", "b", "c", "d", "e"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(columns, vec![ColumnName::new(["a", "b", "c", "d", "e"]),]);
    }

    #[test]
    fn test_parse_clustering_columns_with_backticks_in_names() {
        // Field names can contain backticks and other special characters
        // since each path component is stored as a separate array element
        let json = r#"{"clusteringColumns":[["col1","`col2,col3`","`col4.col5`,col6"]]}"#;
        let columns = parse_clustering_columns(json).unwrap();
        assert_eq!(
            columns,
            vec![ColumnName::new(["col1", "`col2,col3`", "`col4.col5`,col6"]),]
        );
    }
}
