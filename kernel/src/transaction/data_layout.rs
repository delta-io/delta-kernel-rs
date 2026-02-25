//! Data layout configuration for Delta tables.
//!
//! This module defines [`DataLayout`] which specifies how data files are organized
//! within a Delta table. Supported layouts are:
//!
//! - **None**: No special organization (default)
//! - **Clustered**: Data files optimized for queries on clustering columns

// Allow unreachable_pub because this module is pub when internal-api is enabled
// but pub(crate) otherwise. The items need to be pub for the public API.
#![allow(unreachable_pub)]
#![allow(dead_code)]

use crate::expressions::ColumnName;

/// Data layout configuration for a Delta table.
///
/// Determines how data files are organized within the table:
///
/// - [`DataLayout::None`]: No special organization (default)
/// - [`DataLayout::Clustered`]: Data files optimized for queries on clustering columns
///
/// TODO(#1795): Add `Partitioned` variant for partition column support.
#[derive(Debug, Clone, Default)]
pub enum DataLayout {
    /// No special data organization (default).
    #[default]
    None,

    /// Data files optimized for queries on clustering columns.
    /// Both top-level and nested columns are supported. Each column's leaf field must
    /// have a stats-eligible primitive type.
    Clustered {
        /// Columns to cluster by (in order).
        columns: Vec<ColumnName>,
    },
}

impl DataLayout {
    /// Create a clustered layout with the given top-level column names.
    ///
    /// Each string is treated as a single top-level column name. For nested columns,
    /// construct the [`DataLayout::Clustered`] variant directly with multi-segment
    /// [`ColumnName`] values.
    ///
    /// This method constructs the layout without validation. Full validation
    /// (duplicates, schema compatibility, data types) is performed during
    /// `CreateTableTransactionBuilder::build()` via `validate_clustering_columns()`.
    ///
    /// # Examples
    ///
    /// Top-level columns:
    ///
    /// ```ignore
    /// let layout = DataLayout::clustered(["id", "timestamp"]);
    /// ```
    ///
    /// Nested columns (construct the variant directly):
    ///
    /// ```ignore
    /// let layout = DataLayout::Clustered {
    ///     columns: vec![ColumnName::new(["user", "address", "city"])],
    /// };
    /// ```
    pub fn clustered<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let columns: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| ColumnName::new([s.as_ref()]))
            .collect();

        DataLayout::Clustered { columns }
    }

    /// Returns true if this layout specifies clustering.
    pub fn is_clustered(&self) -> bool {
        matches!(self, DataLayout::Clustered { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clustered_layout_construction() {
        let layout = DataLayout::clustered(["col1", "col2"]);
        assert!(layout.is_clustered());
        if let DataLayout::Clustered { columns } = layout {
            assert_eq!(columns.len(), 2);
        }
    }

    #[test]
    fn test_default_layout() {
        let layout = DataLayout::default();
        assert!(!layout.is_clustered());
    }

    // Note: Validation tests (empty, too many, duplicates) are in clustering.rs
    // since validate_clustering_columns() performs all validation at build time.
}
