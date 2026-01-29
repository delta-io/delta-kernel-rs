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

    /// Returns true if the current path is a clustering column.
    fn is_clustering_column(&self) -> bool {
        self.clustering_trie
            .as_ref()
            .is_some_and(|trie| trie.contains_prefix_of(&self.path))
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

    /// Returns true if the current path could lead to any clustering column.
    fn could_reach_clustering_column(&self) -> bool {
        self.clustering_trie
            .as_ref()
            .is_some_and(|trie| trie.could_contain_descendant(&self.path))
    }

    fn collect_field(&mut self, field: &StructField, result: &mut Vec<ColumnName>) {
        self.path.push(field.name.clone());

        // Early termination: when at the column limit, only continue if this path
        // could lead to a clustering column. This avoids traversing the entire schema
        // (e.g., 1000 columns) when we only need to find a few nested clustering columns.
        if self.at_column_limit() && !self.could_reach_clustering_column() {
            self.path.pop();
            return;
        }

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
    use crate::schema::{ArrayType, StructField, StructType};

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

    /// Standard 3-column schema for clustering tests: a (LONG), b (STRING), c (INTEGER)
    fn abc_schema() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("c", DataType::INTEGER),
        ])
    }

    /// Helper to run column collection and return results
    fn collect_stats_columns(
        props: &TableProperties,
        clustering_cols: Option<&[ColumnName]>,
        schema: &Schema,
    ) -> Vec<ColumnName> {
        let mut filter = StatsColumnFilter::new(props, clustering_cols);
        let mut columns = Vec::new();
        filter.collect_columns(schema, &mut columns);
        columns
    }

    // Tests for clustering columns with num_indexed_cols limit
    #[rstest::rstest]
    #[case::clustering_overrides_limit(
        1,                              // num_indexed_cols limit
        vec!["c"],                      // clustering columns (3rd column)
        vec!["a", "c"]                  // expected: "a" (within limit) + "c" (clustering)
    )]
    #[case::no_clustering_uses_limit(
        2,                              // num_indexed_cols limit
        vec![],                         // no clustering columns
        vec!["a", "b"]                  // expected: first 2 columns within limit
    )]
    fn test_clustering_with_num_indexed_cols(
        #[case] num_cols: u64,
        #[case] clustering: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let props = make_props_with_num_cols(num_cols);
        let clustering_cols: Vec<ColumnName> =
            clustering.iter().map(|c| ColumnName::new([*c])).collect();
        let clustering_ref = if clustering_cols.is_empty() {
            None
        } else {
            Some(clustering_cols.as_slice())
        };
        let schema = abc_schema();

        let columns = collect_stats_columns(&props, clustering_ref, &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    // Tests for clustering columns with explicit stats_columns
    #[rstest::rstest]
    #[case::clustering_added_to_stats(
        "a",                            // stats columns
        vec!["c"],                      // clustering columns
        vec!["a", "c"]                  // expected: "a" (explicit) + "c" (clustering)
    )]
    #[case::clustering_already_in_stats(
        "a,b",                          // stats columns include clustering col
        vec!["a"],                      // clustering columns (already in stats)
        vec!["a", "b"]                  // expected: no duplicates
    )]
    fn test_clustering_with_stats_columns(
        #[case] stats_cols: &str,
        #[case] clustering: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let props = make_props_with_stats_cols(stats_cols);
        let clustering_cols: Vec<ColumnName> =
            clustering.iter().map(|c| ColumnName::new([*c])).collect();
        let schema = abc_schema();

        let columns = collect_stats_columns(&props, Some(&clustering_cols), &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    #[test]
    fn test_clustering_column_non_eligible_type() {
        // Array/Map clustering columns should be excluded as they're not eligible for stats
        let props = make_props_with_num_cols(32);
        let clustering_cols = vec![ColumnName::new(["arr"])];

        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable(
                "arr",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
        ]);

        let columns = collect_stats_columns(&props, Some(&clustering_cols), &schema);

        // Should only include "a" - arrays are not eligible for statistics
        assert_eq!(columns, vec![ColumnName::new(["a"])]);
    }

    #[test]
    fn test_nested_clustering_column_with_limit() {
        // Test that nested clustering columns are found even with a column limit,
        // and that irrelevant subtrees are skipped for performance.
        let props = make_props_with_num_cols(2);

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

        let columns = collect_stats_columns(&props, Some(&clustering_cols), &schema);

        // Should include: id, name (first 2 within limit) + user.address.city (clustering)
        // Should NOT include: user.name, user.address.street, user.address.zip, other.*, extra*
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["name"]),
                ColumnName::new(["user", "address", "city"]),
            ]
        );
    }
}
