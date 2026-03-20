//! Data layout configuration for Delta tables.
//!
//! This module defines [`DataLayout`] which specifies how data files are organized
//! within a Delta table. Supported layouts are:
//!
//! - **None**: No special organization (default)
//! - **Clustered**: Data files optimized for queries on clustering columns
//! - **Partitioned**: Data files organized into directories by partition column values

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
/// - [`DataLayout::Partitioned`]: Data files organized into directories by partition column values
///
/// Partitioning and clustering are mutually exclusive -- only one variant can be active at a time.
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

    /// Data files organized into directories by partition column values.
    /// Only top-level columns are supported. Partition column values are stored
    /// in the directory path rather than in the data files themselves.
    Partitioned {
        /// Columns to partition by (in order).
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

    /// Create a partitioned layout with the given top-level column names.
    ///
    /// Each string is treated as a single top-level column name. Partition columns
    /// must be top-level columns in the schema (nested columns are not supported).
    ///
    /// This method constructs the layout without validation. Full validation
    /// (duplicates, schema compatibility, data types) is performed during
    /// `CreateTableTransactionBuilder::build()` via `validate_partition_columns()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let layout = DataLayout::partitioned(["year", "month"]);
    /// ```
    pub fn partitioned<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let columns: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| ColumnName::new([s.as_ref()]))
            .collect();

        DataLayout::Partitioned { columns }
    }

    /// Returns true if this layout specifies clustering.
    #[cfg(test)]
    pub fn is_clustered(&self) -> bool {
        matches!(self, DataLayout::Clustered { .. })
    }

    /// Returns true if this layout specifies partitioning.
    #[cfg(test)]
    pub fn is_partitioned(&self) -> bool {
        matches!(self, DataLayout::Partitioned { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::clustered(DataLayout::clustered(["id"]), true, false)]
    #[case::partitioned(DataLayout::partitioned(["id"]), false, true)]
    #[case::none(DataLayout::default(), false, false)]
    fn test_data_layout_predicates(
        #[case] layout: DataLayout,
        #[case] expect_clustered: bool,
        #[case] expect_partitioned: bool,
    ) {
        assert_eq!(layout.is_clustered(), expect_clustered);
        assert_eq!(layout.is_partitioned(), expect_partitioned);
    }

    #[test]
    fn test_clustered_layout_construction() {
        let layout = DataLayout::clustered(["col1", "col2"]);
        if let DataLayout::Clustered { columns } = layout {
            assert_eq!(columns.len(), 2);
        } else {
            panic!("Expected Clustered variant");
        }
    }

    #[test]
    fn test_partitioned_layout_construction() {
        let layout = DataLayout::partitioned(["year", "month"]);
        if let DataLayout::Partitioned { columns } = layout {
            assert_eq!(columns.len(), 2);
        } else {
            panic!("Expected Partitioned variant");
        }
    }

    // Note: Validation tests (duplicates, schema compatibility, data types) are in
    // clustering.rs and builder/create_table.rs since validation is performed at build time.
}
