//! Column filtering logic for statistics based on table properties.
//!
//! This module contains [`StatsColumnFilter`], which determines which columns
//! should have statistics collected based on table configuration.

use crate::{
    column_trie::ColumnTrie,
    schema::{ColumnName, DataType, Schema, StructField},
    table_properties::{DataSkippingNumIndexedCols, TableProperties},
};

/// Handles column filtering logic for statistics based on table properties.
///
/// Filters columns according to:
/// * `dataSkippingStatsColumns` - explicit list of columns to include (takes precedence)
/// * `dataSkippingNumIndexedCols` - number of leaf columns to include (default 32)
/// * Clustering columns - always included per Delta protocol requirements
///
/// Per the Delta protocol, writers MUST write per-file statistics for clustering columns,
/// regardless of table property settings.
///
/// The lifetime `'col` ties this filter to the column names it was built from when
/// using `dataSkippingStatsColumns`.
pub(crate) struct StatsColumnFilter<'col> {
    /// Maximum number of leaf columns to include. Set from `dataSkippingNumIndexedCols` table
    /// property. `None` when `dataSkippingStatsColumns` is specified (which takes precedence).
    n_columns: Option<DataSkippingNumIndexedCols>,
    /// Counter for leaf columns included so far. Used to enforce the `n_columns` limit.
    added_columns: u64,
    /// Trie built from user-specified columns for O(path_length) prefix matching.
    /// `None` when using `n_columns` limit instead of explicit column list.
    column_trie: Option<ColumnTrie<'col>>,
    /// Trie built from clustering columns that must always be included.
    /// `None` when the table has no clustering columns.
    clustering_trie: Option<ColumnTrie<'col>>,
    /// Current path during schema traversal. Pushed on field entry, popped on exit.
    path: Vec<String>,
}

impl<'col> StatsColumnFilter<'col> {
    /// Creates a new StatsColumnFilter with optional clustering columns.
    ///
    /// Clustering columns are always included in statistics, even when `dataSkippingStatsColumns`
    /// or `dataSkippingNumIndexedCols` would otherwise exclude them.
    pub(crate) fn new(
        props: &'col TableProperties,
        clustering_columns: Option<&'col [ColumnName]>,
    ) -> Self {
        let clustering_trie = clustering_columns.map(ColumnTrie::from_columns);

        // If data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified.
        if let Some(column_names) = &props.data_skipping_stats_columns {
            let mut combined_trie = ColumnTrie::from_columns(column_names);

            // Warn about missing clustering columns, then add them
            if let Some(clustering_cols) = clustering_columns {
                for col in clustering_cols {
                    if !combined_trie.contains_prefix_of(col) {
                        tracing::warn!(
                            "Clustering column '{}' not in dataSkippingStatsColumns; adding anyway",
                            col
                        );
                    }
                    combined_trie.insert(col);
                }
            }

            Self {
                n_columns: None,
                added_columns: 0,
                column_trie: Some(combined_trie),
                clustering_trie,
                path: Vec::new(),
            }
        } else {
            let n_cols = props.data_skipping_num_indexed_cols.unwrap_or_default();
            Self {
                n_columns: Some(n_cols),
                added_columns: 0,
                column_trie: None,
                clustering_trie,
                path: Vec::new(),
            }
        }
    }

    // ==================== Public API ====================
    // These methods are used by consumers outside this module.

    /// Collects column names that should have statistics.
    pub(crate) fn collect_columns(&mut self, schema: &Schema, result: &mut Vec<ColumnName>) {
        for field in schema.fields() {
            self.collect_field(field, result);
        }
    }

    // ==================== BaseStatsTransform Integration ====================
    // These methods are used by BaseStatsTransform during schema traversal.

    /// Returns true if the column limit has been reached.
    pub(crate) fn at_column_limit(&self) -> bool {
        matches!(
            self.n_columns,
            Some(DataSkippingNumIndexedCols::NumColumns(n)) if self.added_columns >= n
        )
    }

    /// Returns true if the current path is a clustering column.
    fn is_clustering_column(&self) -> bool {
        self.clustering_trie
            .as_ref()
            .is_some_and(|trie| trie.contains_prefix_of(&self.path))
    }

    /// Returns true if the current path should be included based on column_trie config.
    pub(crate) fn should_include_current(&self) -> bool {
        if self.at_column_limit() {
            // Clustering columns are always included
            if self.is_clustering_column() {
                tracing::warn!(
                    "Clustering column '{}' exceeds dataSkippingNumIndexedCols limit; adding anyway",
                    self.path.join(".")
                );
                return true;
            }
            return false;
        }

        self.column_trie
            .as_ref()
            .map(|trie| trie.contains_prefix_of(&self.path))
            .unwrap_or(true)
    }

    /// Enters a field path for filtering decisions.
    pub(crate) fn enter_field(&mut self, name: &str) {
        self.path.push(name.to_string());
    }

    /// Exits the current field path.
    pub(crate) fn exit_field(&mut self) {
        self.path.pop();
    }

    /// Records that a leaf column was included.
    pub(crate) fn record_included(&mut self) {
        self.added_columns += 1;
    }

    // ==================== Internal Helpers ====================
    // These methods are private to this module.

    fn collect_field(&mut self, field: &StructField, result: &mut Vec<ColumnName>) {
        // When at the column limit and no clustering columns, we can skip entirely.
        // When clustering columns exist, we must continue traversing in case
        // nested clustering columns need to be included.
        if self.at_column_limit() && self.clustering_trie.is_none() {
            return;
        }

        self.path.push(field.name.clone());

        match field.data_type() {
            DataType::Struct(struct_type) => {
                for child in struct_type.fields() {
                    self.collect_field(child, result);
                }
            }
            // Map, Array, and Variant types are not eligible for statistics collection.
            // We skip them entirely so they don't count against the column limit.
            DataType::Map(_) | DataType::Array(_) | DataType::Variant(_) => {}
            _ => {
                if self.should_include_current() {
                    result.push(ColumnName::new(&self.path));
                    self.added_columns += 1;
                }
            }
        }

        self.path.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_props_with_num_cols(n: u64) -> TableProperties {
        [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            n.to_string(),
        )]
        .into()
    }

    fn make_props_with_stats_cols(cols: &str) -> TableProperties {
        [(
            "delta.dataSkippingStatsColumns".to_string(),
            cols.to_string(),
        )]
        .into()
    }

    #[test]
    fn test_clustering_columns_override_num_indexed_cols() {
        // With limit of 1, normally only the first column would be included
        let props = make_props_with_num_cols(1);
        let clustering_cols = vec![ColumnName::new(["c"])]; // Third column

        let schema = crate::schema::StructType::new_unchecked([
            crate::schema::StructField::nullable("a", DataType::LONG),
            crate::schema::StructField::nullable("b", DataType::STRING),
            crate::schema::StructField::nullable("c", DataType::INTEGER),
        ]);

        let mut filter = StatsColumnFilter::new(&props, Some(&clustering_cols));
        let mut columns = Vec::new();
        filter.collect_columns(&schema, &mut columns);

        // Should include both "a" (first column within limit) and "c" (clustering column)
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["c"])]
        );
    }

    #[test]
    fn test_clustering_columns_added_to_stats_columns() {
        // Only "a" is in stats columns, but "c" is a clustering column
        let props = make_props_with_stats_cols("a");
        let clustering_cols = vec![ColumnName::new(["c"])];

        let schema = crate::schema::StructType::new_unchecked([
            crate::schema::StructField::nullable("a", DataType::LONG),
            crate::schema::StructField::nullable("b", DataType::STRING),
            crate::schema::StructField::nullable("c", DataType::INTEGER),
        ]);

        let mut filter = StatsColumnFilter::new(&props, Some(&clustering_cols));
        let mut columns = Vec::new();
        filter.collect_columns(&schema, &mut columns);

        // Should include both "a" (explicit stats column) and "c" (clustering column)
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["c"])]
        );
    }

    #[test]
    fn test_clustering_columns_already_included() {
        // "a" is both in stats columns and is a clustering column
        let props = make_props_with_stats_cols("a,b");
        let clustering_cols = vec![ColumnName::new(["a"])];

        let schema = crate::schema::StructType::new_unchecked([
            crate::schema::StructField::nullable("a", DataType::LONG),
            crate::schema::StructField::nullable("b", DataType::STRING),
            crate::schema::StructField::nullable("c", DataType::INTEGER),
        ]);

        let mut filter = StatsColumnFilter::new(&props, Some(&clustering_cols));
        let mut columns = Vec::new();
        filter.collect_columns(&schema, &mut columns);

        // Should include "a" and "b" (both explicit stats columns), "a" is also clustering
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["b"])]
        );
    }

    #[test]
    fn test_no_clustering_columns() {
        // No clustering columns specified - should work as before
        let props = make_props_with_num_cols(2);

        let schema = crate::schema::StructType::new_unchecked([
            crate::schema::StructField::nullable("a", DataType::LONG),
            crate::schema::StructField::nullable("b", DataType::STRING),
            crate::schema::StructField::nullable("c", DataType::INTEGER),
        ]);

        let mut filter = StatsColumnFilter::new(&props, None);
        let mut columns = Vec::new();
        filter.collect_columns(&schema, &mut columns);

        // Should include first 2 columns within the limit
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["b"])]
        );
    }

    #[test]
    fn test_clustering_column_non_eligible_type() {
        // Array/Map clustering columns should be excluded as they're not eligible for stats
        let props = make_props_with_num_cols(32);
        let clustering_cols = vec![ColumnName::new(["arr"])];

        let schema = crate::schema::StructType::new_unchecked([
            crate::schema::StructField::nullable("a", DataType::LONG),
            crate::schema::StructField::nullable(
                "arr",
                DataType::Array(Box::new(crate::schema::ArrayType::new(
                    DataType::STRING,
                    false,
                ))),
            ),
        ]);

        let mut filter = StatsColumnFilter::new(&props, Some(&clustering_cols));
        let mut columns = Vec::new();
        filter.collect_columns(&schema, &mut columns);

        // Should only include "a" - arrays are not eligible for statistics
        assert_eq!(columns, vec![ColumnName::new(["a"])]);
    }
}
