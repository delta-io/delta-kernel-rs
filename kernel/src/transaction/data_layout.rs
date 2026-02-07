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

use crate::clustering::MAX_CLUSTERING_COLUMNS;
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

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
    ///
    /// Clustering columns must be top-level columns in the schema.
    /// Maximum of 4 columns allowed.
    Clustered {
        /// Columns to cluster by (in order).
        columns: Vec<ColumnName>,
    },
}

impl DataLayout {
    /// Create a clustered layout with the given columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - Column names to cluster by. Must be non-empty and at most 4.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No columns are specified
    /// - More than 4 columns are specified
    ///
    /// # Example
    ///
    /// ```ignore
    /// let layout = DataLayout::clustered(["id", "timestamp"])?;
    /// ```
    pub fn clustered<I, S>(columns: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        use std::collections::HashSet;

        let columns: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| ColumnName::new([s.as_ref()]))
            .collect();

        if columns.is_empty() {
            return Err(Error::generic(
                "Clustered layout requires at least one column",
            ));
        }

        // TODO(#1794): This limit is a Delta-Spark connector configuration, not a protocol
        // requirement. Consider removing or making this configurable.
        if columns.len() > MAX_CLUSTERING_COLUMNS {
            return Err(Error::generic(format!(
                "Clustered layout supports at most {} columns, got {}",
                MAX_CLUSTERING_COLUMNS,
                columns.len()
            )));
        }

        // Check for duplicate columns
        let mut seen = HashSet::new();
        for col in &columns {
            let col_name = &col.path()[0];
            if !seen.insert(col_name) {
                return Err(Error::generic(format!(
                    "Duplicate clustering column: '{}'",
                    col_name
                )));
            }
        }

        Ok(DataLayout::Clustered { columns })
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
    fn test_clustered_layout_valid() {
        let layout = DataLayout::clustered(["col1", "col2"]).unwrap();
        assert!(layout.is_clustered());
        if let DataLayout::Clustered { columns } = layout {
            assert_eq!(columns.len(), 2);
        }
    }

    #[test]
    fn test_clustered_layout_empty_columns() {
        let result = DataLayout::clustered(Vec::<&str>::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_clustered_layout_too_many_columns() {
        let result = DataLayout::clustered(["a", "b", "c", "d", "e"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_clustered_layout_max_columns() {
        let result = DataLayout::clustered(["a", "b", "c", "d"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_default_layout() {
        let layout = DataLayout::default();
        assert!(!layout.is_clustered());
    }

    #[test]
    fn test_clustered_layout_duplicate_columns_rejected() {
        let result = DataLayout::clustered(["id", "id"]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate clustering column"));
    }
}
