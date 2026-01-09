//! Data layout specification for Delta tables.
//!
//! This module provides the [`DataLayout`] enum for specifying how data is organized
//! in a Delta table - either partitioned by columns or clustered by columns.
//!
//! # Example
//!
//! ```rust,ignore
//! use delta_kernel::transaction::DataLayout;
//!
//! // Create a partitioned table layout
//! let layout = DataLayout::partitioned(["date", "region"])?;
//!
//! // Create a clustered table layout (supports nested columns via dot notation)
//! let layout = DataLayout::clustered(["col1", "nested.field"])?;
//! ```

use crate::schema::ColumnName;
use crate::DeltaResult;

/// Maximum number of clustering columns allowed per Delta specification.
pub const MAX_CLUSTERING_COLUMNS: usize = 4;

/// Specifies how data is organized in a Delta table.
///
/// Delta tables can organize data in different ways to optimize query performance:
/// - **Partitioned**: Data files are organized into directories based on partition column values.
/// - **Clustered**: Data is physically co-located based on clustering column values within files.
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
    Partitioned(Vec<ColumnName>),

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
    pub fn partition_columns(&self) -> Option<&[ColumnName]> {
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

    /// Creates a partitioned data layout from column names.
    ///
    /// Partition columns must be top-level columns in the schema (no nested fields).
    /// Column names are parsed using standard Delta column name syntax.
    ///
    /// # Arguments
    ///
    /// * `columns` - An iterable of column names (accepts `&str`, `String`, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use delta_kernel::transaction::DataLayout;
    ///
    /// let layout = DataLayout::partitioned(["date", "region"])?;
    /// ```
    pub fn partitioned<I, S>(columns: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let cols: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| s.as_ref().parse())
            .collect::<DeltaResult<_>>()?;
        Ok(DataLayout::Partitioned(cols))
    }

    /// Creates a clustered data layout from column names.
    ///
    /// Clustering columns can be nested using dot notation (e.g., `"address.city"`).
    /// Column names are parsed using standard Delta column name syntax, which supports
    /// backtick escaping for special characters.
    ///
    /// # Arguments
    ///
    /// * `columns` - An iterable of column names (accepts `&str`, `String`, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use delta_kernel::transaction::DataLayout;
    ///
    /// // Simple columns
    /// let layout = DataLayout::clustered(["col1", "col2"])?;
    ///
    /// // Nested columns using dot notation
    /// let layout = DataLayout::clustered(["address.city", "user.profile.name"])?;
    /// ```
    pub fn clustered<I, S>(columns: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let cols: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| s.as_ref().parse())
            .collect::<DeltaResult<_>>()?;
        Ok(DataLayout::Clustered(cols))
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
        let layout = DataLayout::partitioned(["date", "region"]).unwrap();
        assert!(!layout.is_none());
        assert!(layout.is_partitioned());
        assert!(!layout.is_clustered());

        let cols = layout.partition_columns().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnName::new(["date"]));
        assert_eq!(cols[1], ColumnName::new(["region"]));
        assert!(layout.clustering_columns().is_none());
    }

    #[test]
    fn test_data_layout_partitioned_with_strings() {
        // Test with Vec<String>
        let col_names = vec!["date".to_string(), "region".to_string()];
        let layout = DataLayout::partitioned(col_names).unwrap();
        assert!(layout.is_partitioned());
        assert_eq!(layout.partition_columns().unwrap().len(), 2);
    }

    #[test]
    fn test_data_layout_clustered() {
        let layout = DataLayout::clustered(["col1", "nested.field"]).unwrap();
        assert!(!layout.is_none());
        assert!(!layout.is_partitioned());
        assert!(layout.is_clustered());
        assert!(layout.partition_columns().is_none());

        let cols = layout.clustering_columns().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnName::new(["col1"]));
        assert_eq!(cols[1], ColumnName::new(["nested", "field"]));
    }

    #[test]
    fn test_data_layout_clustered_nested_columns() {
        // Test deeply nested columns
        let layout = DataLayout::clustered(["a.b.c", "x.y"]).unwrap();
        let cols = layout.clustering_columns().unwrap();
        assert_eq!(cols[0], ColumnName::new(["a", "b", "c"]));
        assert_eq!(cols[1], ColumnName::new(["x", "y"]));
    }

    #[test]
    fn test_data_layout_default() {
        let layout = DataLayout::default();
        assert!(layout.is_none());
    }
}
