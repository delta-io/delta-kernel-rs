//! Data layout specification for Delta tables.
//!
//! This module provides the [`DataLayout`] enum for specifying how data is organized
//! in a Delta table - either partitioned by columns or clustered by columns.
//!
//! # Example
//!
//! ```rust,ignore
//! use delta_kernel::transaction::DataLayout;
//! use delta_kernel::schema::ColumnName;
//!
//! // Create a clustered table layout
//! let layout = DataLayout::Clustered(vec![
//!     ColumnName::new(["col1"]),
//!     ColumnName::new(["nested", "field"]),
//! ]);
//!
//! // Create a partitioned table layout
//! let layout = DataLayout::Partitioned(vec!["date".to_string(), "region".to_string()]);
//! ```

use crate::schema::ColumnName;

/// Maximum number of clustering columns allowed per Delta specification.
pub const MAX_CLUSTERING_COLUMNS: usize = 4;

/// Specifies how data is organized in a Delta table.
///
/// Delta tables can organize data in different ways to optimize query performance:
/// - **Partitioned**: Data files are organized into directories based on partition column values.
///   This is best for columns with low cardinality that are frequently used in filters.
/// - **Clustered**: Data is physically co-located based on clustering column values within files.
///   This enables data skipping for high-cardinality columns without the directory overhead.
///
/// Partitioning and clustering are mutually exclusive - a table cannot have both.
/// This is enforced at the type level by this enum.
#[derive(Debug, Clone, Default)]
pub enum DataLayout {
    /// No special data layout (default for new tables).
    #[default]
    None,

    /// Data is partitioned by the specified columns.
    ///
    /// Partition columns must be top-level columns in the schema (no nested fields).
    /// The partition column values are extracted from data and used to organize files
    /// into directories like `partition_col=value/`.
    Partitioned(Vec<String>),

    /// Data is clustered by the specified columns.
    ///
    /// Clustering columns can be nested (e.g., `["nested", "field"]`).
    /// The clustering columns are stored as domain metadata and used by OPTIMIZE
    /// to physically co-locate similar data for better data skipping.
    ///
    /// Maximum of 4 clustering columns are allowed per the Delta specification.
    Clustered(Vec<ColumnName>),
}

impl DataLayout {
    /// Returns `true` if this layout specifies partitioning.
    pub fn is_partitioned(&self) -> bool {
        matches!(self, DataLayout::Partitioned(_))
    }

    /// Returns `true` if this layout specifies clustering.
    pub fn is_clustered(&self) -> bool {
        matches!(self, DataLayout::Clustered(_))
    }

    /// Returns `true` if this is a default layout with no special organization.
    pub fn is_none(&self) -> bool {
        matches!(self, DataLayout::None)
    }

    /// Returns the partition columns if this is a partitioned layout.
    pub fn partition_columns(&self) -> Option<&[String]> {
        match self {
            DataLayout::Partitioned(cols) => Some(cols),
            _ => None,
        }
    }

    /// Returns the clustering columns if this is a clustered layout.
    pub fn clustering_columns(&self) -> Option<&[ColumnName]> {
        match self {
            DataLayout::Clustered(cols) => Some(cols),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_layout_none() {
        let layout = DataLayout::None;
        assert!(layout.is_none());
        assert!(!layout.is_partitioned());
        assert!(!layout.is_clustered());
        assert!(layout.partition_columns().is_none());
        assert!(layout.clustering_columns().is_none());
    }

    #[test]
    fn test_data_layout_partitioned() {
        let layout = DataLayout::Partitioned(vec!["date".to_string(), "region".to_string()]);
        assert!(!layout.is_none());
        assert!(layout.is_partitioned());
        assert!(!layout.is_clustered());
        assert_eq!(
            layout.partition_columns(),
            Some(&["date".to_string(), "region".to_string()][..])
        );
        assert!(layout.clustering_columns().is_none());
    }

    #[test]
    fn test_data_layout_clustered() {
        let cols = vec![
            ColumnName::new(["col1"]),
            ColumnName::new(["nested", "field"]),
        ];
        let layout = DataLayout::Clustered(cols.clone());
        assert!(!layout.is_none());
        assert!(!layout.is_partitioned());
        assert!(layout.is_clustered());
        assert!(layout.partition_columns().is_none());
        assert_eq!(layout.clustering_columns(), Some(&cols[..]));
    }

    #[test]
    fn test_data_layout_default() {
        let layout = DataLayout::default();
        assert!(layout.is_none());
    }
}

