//! Clustering column support for Delta tables.
//!
//! This module provides functionality for reading and writing clustering columns
//! from/to domain metadata. Per the Delta protocol, writers MUST write per-file
//! statistics for clustering columns.
//!
//! Clustering columns are stored in domain metadata under the `delta.clustering` domain
//! as a JSON object with a `clusteringColumns` field containing an array of column paths,
//! where each path is an array of field names (to handle nested columns).

use serde::{Deserialize, Serialize};

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};

/// Domain metadata structure for clustering columns.
///
/// This is serialized to/deserialized from the JSON configuration stored in the
/// `delta.clustering` domain metadata. Each clustering column is represented
/// as an array of field names to support nested columns.
///
/// The column names are physical names. If column mapping is enabled, these will be
/// the physical column identifiers (e.g., `col-uuid`); otherwise, they match the logical names.
///
/// Example JSON:
/// ```json
/// {"clusteringColumns": [["col1"], ["user", "address", "city"]]}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusteringMetadataDomain {
    /// The columns used for clustering, in order.
    /// Each column is represented as a path (array of field names) to support nested columns.
    pub clustering_columns: Vec<Vec<String>>,
}

/// The domain name for clustering metadata.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";

/// Parses clustering columns from a JSON configuration string.
///
/// Returns `Ok(columns)` if the configuration is valid, or an error if malformed.
fn parse_clustering_columns(json_str: &str) -> DeltaResult<Vec<ColumnName>> {
    let metadata: ClusteringMetadataDomain = serde_json::from_str(json_str)?;
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

    #[rstest::rstest]
    #[case::simple(
        r#"{"clusteringColumns": [["col1"], ["col2"]]}"#,
        vec![vec!["col1"], vec!["col2"]]
    )]
    #[case::empty(
        r#"{"clusteringColumns": []}"#,
        vec![]
    )]
    #[case::nested(
        r#"{"clusteringColumns": [["id"], ["user", "address", "city"], ["a", "b", "c", "d", "e"]]}"#,
        vec![vec!["id"], vec!["user", "address", "city"], vec!["a", "b", "c", "d", "e"]]
    )]
    #[case::special_characters(
        r#"{"clusteringColumns": [["col.with.dot"], ["`backticks`", "nested"]]}"#,
        vec![vec!["col.with.dot"], vec!["`backticks`", "nested"]]
    )]
    #[case::tolerates_unknown_fields(
        r#"{"clusteringColumns": [["col1"]], "foo": "bar", "futureField": 123}"#,
        vec![vec!["col1"]]
    )]
    fn test_parse_clustering_columns(#[case] json: &str, #[case] expected: Vec<Vec<&str>>) {
        let columns = parse_clustering_columns(json).unwrap();
        let expected_cols: Vec<ColumnName> = expected.into_iter().map(ColumnName::new).collect();
        assert_eq!(columns, expected_cols);
    }

    #[test]
    fn test_serialize_clustering_metadata() {
        let metadata = ClusteringMetadataDomain {
            clustering_columns: vec![vec!["col1".to_string()], vec!["col2".to_string()]],
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert_eq!(json, r#"{"clusteringColumns":[["col1"],["col2"]]}"#);
    }

    #[test]
    fn test_serialize_nested_clustering_metadata() {
        let metadata = ClusteringMetadataDomain {
            clustering_columns: vec![
                vec!["id".to_string()],
                vec![
                    "user".to_string(),
                    "address".to_string(),
                    "city".to_string(),
                ],
            ],
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            json,
            r#"{"clusteringColumns":[["id"],["user","address","city"]]}"#
        );
    }
}
