//! Clustering support for Delta tables.
//!
//! This module provides support for clustered tables, including the domain metadata
//! format used to store clustering column information.

use serde::{Deserialize, Serialize};

/// Domain name for clustering metadata in the Delta log.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";

/// Domain metadata structure for clustering columns.
///
/// This is serialized to JSON and stored in the delta.clustering domain metadata
/// action when a table is clustered.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusteringMetadataDomain {
    /// The columns used for clustering, in order.
    pub clustering_columns: Vec<String>,
}
