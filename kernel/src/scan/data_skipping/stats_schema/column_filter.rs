//! Column filtering logic for statistics based on table properties.
//!
//! This module contains [`StatsColumnFilter`], which determines which columns
//! should have statistics collected based on table configuration.

use crate::{
    column_trie::ColumnTrie,
    schema::{ColumnName, DataType, Schema, StructField, StructType},
    table_properties::DataSkippingNumIndexedCols,
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
pub(crate) struct StatsColumnFilter<'col> {
    /// Maximum number of leaf columns to include. Set from `delta.dataSkippingNumIndexedCols`
    /// table property. `Some` when using column-count-based filtering, `None` when
    /// `delta.dataSkippingStatsColumns` is specified (which takes precedence).
    n_columns: Option<DataSkippingNumIndexedCols>,
    /// Counter for leaf columns included so far. Used to enforce the `n_columns` limit.
    added_columns: u64,
    /// Trie built from columns specified in `delta.dataSkippingStatsColumns` for O(path_length)
    /// prefix matching. `Some` when using explicit column list, `None` when using the
    /// `delta.dataSkippingNumIndexedCols` count-based approach.
    data_skipping_stats_trie: Option<ColumnTrie<'col>>,
    /// Trie built from clustering columns for O(path_length) lookup during traversal.
    /// Used by `should_include_current()` to allow clustering columns past the limit.
    clustering_trie: Option<ColumnTrie<'col>>,
    /// Clustering columns to add after the main traversal in `collect_columns()`.
    /// Only set when using `delta.dataSkippingNumIndexedCols` (when using
    /// `delta.dataSkippingStatsColumns`, clustering columns are merged into `data_skipping_stats_trie`).
    clustering_columns: Option<&'col [ColumnName]>,
    /// Current path during schema traversal. Pushed on field entry, popped on exit.
    path: Vec<String>,
}

impl<'col> StatsColumnFilter<'col> {
    /// Creates a new StatsColumnFilter with optional clustering columns.
    ///
    /// Clustering columns are always included in statistics, even when `dataSkippingStatsColumns`
    /// or `dataSkippingNumIndexedCols` would otherwise exclude them.
    ///
    /// Note: `data_skipping_stats_columns` should use physical column names (matching the physical
    /// schema) when column mapping is enabled. The caller is responsible for converting logical
    /// column names from table properties to physical names before passing them here.
    pub(crate) fn new(
        data_skipping_stats_columns: Option<&'col [ColumnName]>,
        data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,
        clustering_columns: Option<&'col [ColumnName]>,
    ) -> Self {
        // If data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified.
        if let Some(column_names) = data_skipping_stats_columns {
            let mut combined_trie = ColumnTrie::from_columns(column_names);

            // Add clustering columns to the trie so they're included during traversal
            if let Some(clustering_cols) = clustering_columns {
                for col in clustering_cols {
                    let col_path: Vec<String> = col.iter().map(|s| s.to_string()).collect();
                    if !combined_trie.contains_prefix_of(&col_path) {
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
                data_skipping_stats_trie: Some(combined_trie),
                clustering_trie: None,    // Already in data_skipping_stats_trie
                clustering_columns: None, // Already added to trie
                path: Vec::new(),
            }
        } else {
            let n_cols = data_skipping_num_indexed_cols.unwrap_or_default();
            let clustering_trie = clustering_columns.map(ColumnTrie::from_columns);
            Self {
                n_columns: Some(n_cols),
                added_columns: 0,
                data_skipping_stats_trie: None,
                clustering_trie,
                clustering_columns, // Will be handled in Pass 2 of collect_columns()
                path: Vec::new(),
            }
        }
    }

    // ==================== Public API ====================

    /// Collects column names that should have statistics.
    ///
    /// Traversal is done in two passes:
    /// 1. Pass 1: Traverse schema to collect columns up to the limit
    /// 2. Pass 2: Directly look up clustering columns not already included
    pub(crate) fn collect_columns(&mut self, schema: &Schema, result: &mut Vec<ColumnName>) {
        // Pass 1: Collect columns according to table properties
        for field in schema.fields() {
            self.collect_field(field, result);
        }

        // Pass 2: Add clustering columns not already included
        // Uses O(n) contains check, but clustering columns are typically few (1-4)
        if let Some(clustering_cols) = self.clustering_columns {
            for col in clustering_cols {
                if result.contains(col) {
                    continue;
                }
                // Verify the clustering column exists in schema before adding
                if lookup_column_type(schema, col).is_some() {
                    tracing::warn!(
                        "Clustering column '{}' exceeds dataSkippingNumIndexedCols limit; \
                         adding anyway",
                        col
                    );
                    result.push(col.clone());
                }
            }
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

    /// Returns true if the current path should be included based on filtering config.
    /// Clustering columns are always included, even past the column limit.
    pub(crate) fn should_include_current(&self) -> bool {
        // When using dataSkippingStatsColumns, check the trie
        if let Some(trie) = &self.data_skipping_stats_trie {
            return trie.contains_prefix_of(&self.path);
        }

        // When using dataSkippingNumIndexedCols, check limit but allow clustering columns
        if self.at_column_limit() {
            self.is_clustering_column()
        } else {
            true
        }
    }

    /// Returns true if the current path is a clustering column.
    fn is_clustering_column(&self) -> bool {
        self.clustering_trie
            .as_ref()
            .is_some_and(|trie| trie.contains_prefix_of(&self.path))
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

    /// Pass 1: Collect columns up to the limit, stopping when limit is reached.
    fn collect_field(&mut self, field: &StructField, result: &mut Vec<ColumnName>) {
        // Stop traversal once we've hit the column limit
        // Clustering columns will be added in Pass 2
        if self.at_column_limit() {
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

/// Looks up a column by path in the schema, returning its data type if found.
///
/// Navigates through nested structs following the path components.
/// For example, `lookup_column_type(schema, "user.address.city")` will:
/// 1. Find field "user" in schema
/// 2. Find field "address" in user's struct type
/// 3. Find field "city" in address's struct type
/// 4. Return city's data type
fn lookup_column_type<'a>(schema: &'a StructType, column: &ColumnName) -> Option<&'a DataType> {
    let mut parts = column.iter();

    // Get the first part to start navigation
    let first = parts.next()?;
    let mut current_field = schema.field(first)?;

    // Navigate through remaining parts
    for part in parts {
        match current_field.data_type() {
            DataType::Struct(struct_type) => {
                current_field = struct_type.field(part)?;
            }
            _ => return None, // Path continues but current field is not a struct
        }
    }

    Some(current_field.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard 3-column schema for clustering tests: a (LONG), b (STRING), c (INTEGER)
    fn abc_schema() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("c", DataType::INTEGER),
        ])
    }

    /// Helper to run column collection with data_skipping_num_indexed_cols and return results
    fn collect_with_num_cols(
        num_cols: u64,
        clustering_cols: Option<&[ColumnName]>,
        schema: &Schema,
    ) -> Vec<ColumnName> {
        let mut filter = StatsColumnFilter::new(
            None,
            Some(DataSkippingNumIndexedCols::NumColumns(num_cols)),
            clustering_cols,
        );
        let mut columns = Vec::new();
        filter.collect_columns(schema, &mut columns);
        columns
    }

    /// Helper to run column collection with stats_columns and return results
    fn collect_with_stats_cols(
        stats_cols: &[ColumnName],
        clustering_cols: Option<&[ColumnName]>,
        schema: &Schema,
    ) -> Vec<ColumnName> {
        let mut filter = StatsColumnFilter::new(Some(stats_cols), None, clustering_cols);
        let mut columns = Vec::new();
        filter.collect_columns(schema, &mut columns);
        columns
    }

    // ==================== Clustering column tests ====================

    #[rstest::rstest]
    #[case::clustering_overrides_limit(
        1,                              // data_skipping_num_indexed_cols limit
        vec!["c"],                      // clustering columns (3rd column)
        vec!["a", "c"]                  // expected: "a" (within limit) + "c" (clustering)
    )]
    #[case::no_clustering_uses_limit(
        2,                              // data_skipping_num_indexed_cols limit
        vec![],                         // no clustering columns
        vec!["a", "b"]                  // expected: first 2 columns within limit
    )]
    fn test_clustering_with_data_skipping_num_indexed_cols(
        #[case] num_cols: u64,
        #[case] clustering: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let clustering_cols: Vec<ColumnName> =
            clustering.iter().map(|c| ColumnName::new([*c])).collect();
        let clustering_ref = if clustering_cols.is_empty() {
            None
        } else {
            Some(clustering_cols.as_slice())
        };
        let schema = abc_schema();

        let columns = collect_with_num_cols(num_cols, clustering_ref, &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    #[rstest::rstest]
    #[case::clustering_added_to_stats(
        vec!["a"],                      // stats columns
        vec!["c"],                      // clustering columns
        vec!["a", "c"]                  // expected: "a" (explicit) + "c" (clustering)
    )]
    #[case::clustering_already_in_stats(
        vec!["a", "b"],                 // stats columns include clustering col
        vec!["a"],                      // clustering columns (already in stats)
        vec!["a", "b"]                  // expected: no duplicates
    )]
    fn test_clustering_with_stats_columns(
        #[case] stats_cols: Vec<&str>,
        #[case] clustering: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let stats_col_names: Vec<ColumnName> =
            stats_cols.iter().map(|c| ColumnName::new([*c])).collect();
        let clustering_cols: Vec<ColumnName> =
            clustering.iter().map(|c| ColumnName::new([*c])).collect();
        let schema = abc_schema();

        let columns = collect_with_stats_cols(&stats_col_names, Some(&clustering_cols), &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    #[test]
    fn test_nested_clustering_column_with_limit() {
        // Test that nested clustering columns are found even with a column limit.
        // Clustering column is deeply nested: user.address.city
        let clustering_cols = vec![ColumnName::new(["user", "address", "city"])];

        let address_struct = StructType::new_unchecked([
            StructField::nullable("street", DataType::STRING),
            StructField::nullable("city", DataType::STRING), // clustering column
            StructField::nullable("zip", DataType::STRING),
        ]);
        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("address", DataType::Struct(Box::new(address_struct))),
        ]);
        let other_struct = StructType::new_unchecked([
            StructField::nullable("foo", DataType::STRING),
            StructField::nullable("bar", DataType::STRING),
        ]);

        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct))),
            StructField::nullable("other", DataType::Struct(Box::new(other_struct))),
            StructField::nullable("extra1", DataType::STRING),
            StructField::nullable("extra2", DataType::STRING),
        ]);

        let columns = collect_with_num_cols(2, Some(&clustering_cols), &schema);

        // Should include: id, name (first 2 within limit) + user.address.city (clustering)
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["name"]),
                ColumnName::new(["user", "address", "city"]),
            ]
        );
    }

    #[test]
    fn test_clustering_column_not_in_schema() {
        // Clustering column that doesn't exist in schema should be silently ignored
        let clustering_cols = vec![ColumnName::new(["nonexistent", "column"])];
        let schema = abc_schema();

        let columns = collect_with_num_cols(2, Some(&clustering_cols), &schema);

        // Should only include normal columns, clustering column not found
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["b"]),]
        );
    }
}
