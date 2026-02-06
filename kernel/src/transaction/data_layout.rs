//! Data layout configuration for Delta tables.
//!
//! This module defines [`DataLayout`] which specifies how data files are organized
//! within a Delta table. Supported layouts are:
//!
//! - **None**: No special organization (default)
//! - **Partitioned**: Data files organized by partition column values
//! - **Clustered**: Data files optimized for queries on clustering columns

// Allow unreachable_pub because this module is pub when internal-api is enabled
// but pub(crate) otherwise. The items need to be pub for the public API.
#![allow(unreachable_pub)]
#![allow(dead_code)]

use crate::schema::ColumnName;
use crate::{DeltaResult, Error};

/// Maximum number of columns that can be used for clustering.
///
/// This limit matches the Delta protocol specification.
pub const MAX_CLUSTERING_COLUMNS: usize = 4;

/// Data layout configuration for a Delta table.
///
/// Determines how data files are organized within the table:
///
/// - [`DataLayout::None`]: No special organization (default)
/// - [`DataLayout::Partitioned`]: Data files organized by partition column values
/// - [`DataLayout::Clustered`]: Data files optimized for queries on clustering columns
///
/// Note: Partitioning and clustering are mutually exclusive. A table can have one
/// or the other, but not both.
#[derive(Debug, Clone, Default)]
pub enum DataLayout {
    /// No special data organization (default).
    #[default]
    None,

    /// Data files organized by partition column values.
    ///
    /// Partition columns must be top-level columns in the schema.
    /// Data files are stored in directories named by partition values.
    Partitioned {
        /// Columns to partition by (in order).
        columns: Vec<ColumnName>,
    },

    /// Data files optimized for queries on clustering columns.
    ///
    /// Clustering columns must be top-level columns in the schema.
    /// Maximum of [`MAX_CLUSTERING_COLUMNS`] columns allowed.
    Clustered {
        /// Columns to cluster by (in order).
        columns: Vec<ColumnName>,
    },
}

impl DataLayout {
    /// Create a partitioned layout with the given columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - Column names to partition by. Must be non-empty.
    ///
    /// # Errors
    ///
    /// Returns an error if no columns are specified.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let layout = DataLayout::partitioned(["date", "region"])?;
    /// ```
    pub fn partitioned<I, S>(columns: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let columns: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| ColumnName::new([s.as_ref()]))
            .collect();

        if columns.is_empty() {
            return Err(Error::generic(
                "Partitioned layout requires at least one column",
            ));
        }

        Ok(DataLayout::Partitioned { columns })
    }

    /// Create a clustered layout with the given columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - Column names to cluster by. Must be non-empty and at most
    ///   [`MAX_CLUSTERING_COLUMNS`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No columns are specified
    /// - More than [`MAX_CLUSTERING_COLUMNS`] columns are specified
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
        let columns: Vec<ColumnName> = columns
            .into_iter()
            .map(|s| ColumnName::new([s.as_ref()]))
            .collect();

        if columns.is_empty() {
            return Err(Error::generic(
                "Clustered layout requires at least one column",
            ));
        }

        if columns.len() > MAX_CLUSTERING_COLUMNS {
            return Err(Error::generic(format!(
                "Clustered layout supports at most {} columns, got {}",
                MAX_CLUSTERING_COLUMNS,
                columns.len()
            )));
        }

        Ok(DataLayout::Clustered { columns })
    }

    /// Returns true if this layout specifies partitioning.
    pub fn is_partitioned(&self) -> bool {
        matches!(self, DataLayout::Partitioned { .. })
    }

    /// Returns true if this layout specifies clustering.
    pub fn is_clustered(&self) -> bool {
        matches!(self, DataLayout::Clustered { .. })
    }
}
