//! Column filtering logic for statistics based on table properties.
//!
//! This module contains [`StatsColumnFilter`], which determines which columns
//! should have statistics collected based on table configuration.

use crate::{
    column_trie::ColumnTrie,
    schema::{ColumnName, DataType, Schema, StructField, StructType},
    table_properties::{DataSkippingNumIndexedCols, TableProperties},
};

/// Handles column filtering logic for statistics based on table properties.
///
/// Filters columns according to:
/// * `dataSkippingStatsColumns` - explicit list of columns to include (takes precedence)
/// * `dataSkippingNumIndexedCols` - number of leaf columns to include (default 32)
/// * Required columns (e.g. clustering columns) - always included per Delta protocol requirements
/// * Requested columns - optional output filter that does not affect column counting
///
/// Per the Delta protocol, writers MUST write per-file statistics for certain required columns
/// (such as clustering columns), regardless of table property settings.
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
    /// Trie built from required columns (e.g. clustering columns) for O(path_length) lookup
    /// during traversal. Used by `should_include_for_table()` to allow required columns past
    /// the limit.
    required_trie: Option<ColumnTrie<'col>>,
    /// Required columns (e.g. clustering columns) to add after the main traversal in
    /// `collect_columns()`. Only set when using `delta.dataSkippingNumIndexedCols` (when using
    /// `delta.dataSkippingStatsColumns`, required columns are merged into
    /// `data_skipping_stats_trie`).
    required_columns: Option<&'col [ColumnName]>,
    /// Trie built from requested columns for O(path_length) lookup. When `Some`, only columns
    /// matching this trie are included in the output. This filter does not affect column
    /// counting — it is applied after the table-level inclusion decision.
    requested_trie: Option<ColumnTrie<'col>>,
    /// Current path during schema traversal. Pushed on field entry, popped on exit.
    path: Vec<String>,
}

impl<'col> StatsColumnFilter<'col> {
    /// Creates a new StatsColumnFilter with optional required and requested columns.
    ///
    /// Required columns (e.g. clustering columns) are always included in statistics, even when
    /// `dataSkippingStatsColumns` or `dataSkippingNumIndexedCols` would otherwise exclude them.
    ///
    /// Requested columns optionally filter the output without affecting column counting. When
    /// `Some`, only columns matching the requested set are included in the final output.
    pub(crate) fn new(
        props: &'col TableProperties,
        required_columns: Option<&'col [ColumnName]>,
        requested_columns: Option<&'col [ColumnName]>,
    ) -> Self {
        let requested_trie = requested_columns
            .filter(|cols| !cols.is_empty())
            .map(ColumnTrie::from_columns);

        // If data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified.
        if let Some(column_names) = &props.data_skipping_stats_columns {
            let mut combined_trie = ColumnTrie::from_columns(column_names);

            // Add required columns to the trie so they're included during traversal
            if let Some(required_cols) = required_columns {
                for col in required_cols {
                    let col_path: Vec<String> = col.iter().map(|s| s.to_string()).collect();
                    if !combined_trie.contains_prefix_of(&col_path) {
                        tracing::warn!(
                            "Required column '{}' not in dataSkippingStatsColumns; adding anyway",
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
                required_trie: None,    // Already in data_skipping_stats_trie
                required_columns: None, // Already added to trie
                requested_trie,
                path: Vec::new(),
            }
        } else {
            let n_cols = props.data_skipping_num_indexed_cols.unwrap_or_default();
            let required_trie = required_columns.map(ColumnTrie::from_columns);
            Self {
                n_columns: Some(n_cols),
                added_columns: 0,
                data_skipping_stats_trie: None,
                required_trie,
                required_columns, // Will be handled in Pass 2 of collect_columns()
                requested_trie,
                path: Vec::new(),
            }
        }
    }

    // ==================== Public API ====================

    /// Collects logical column names that should have statistics.
    ///
    /// Traversal is done in two passes:
    /// 1. Pass 1: Traverse schema to collect columns up to the limit
    /// 2. Pass 2: Directly look up required columns not already included
    pub(crate) fn collect_columns(&mut self, schema: &Schema, result: &mut Vec<ColumnName>) {
        // Pass 1: Collect columns according to table properties
        for field in schema.fields() {
            self.collect_field(field, result);
        }

        // Pass 2: Add required columns not already included
        // Uses O(n) contains check, but required columns are typically few (1-4)
        if let Some(required_cols) = self.required_columns {
            for col in required_cols {
                if result.contains(col) {
                    continue;
                }
                // Verify the required column exists in schema before adding
                if lookup_column_type(schema, col).is_some() {
                    tracing::warn!(
                        "Required column '{}' exceeds dataSkippingNumIndexedCols limit; \
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

    /// Returns true if the current path should be included based on table-level filtering config.
    /// Required columns (e.g. clustering columns) are always included, even past the column limit.
    pub(crate) fn should_include_for_table(&self) -> bool {
        // When using dataSkippingStatsColumns, check the trie (which includes required)
        if let Some(trie) = &self.data_skipping_stats_trie {
            return trie.contains_prefix_of(&self.path);
        }

        // When using dataSkippingNumIndexedCols, check limit but allow required columns
        if self.at_column_limit() {
            self.is_required_column()
        } else {
            true
        }
    }

    /// Returns true if the current path should be included based on the requested columns
    /// filter. When no requested columns are set, all columns pass this check.
    pub(crate) fn should_include_for_requested(&self) -> bool {
        self.requested_trie
            .as_ref()
            .map(|trie| trie.contains_prefix_of(&self.path))
            .unwrap_or(true)
    }

    /// Returns true if the current path is a required column (e.g. clustering column).
    fn is_required_column(&self) -> bool {
        self.required_trie
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
        // Required columns will be added in Pass 2
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
                if self.should_include_for_table() {
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

    /// Standard 3-column schema for required column tests: a (LONG), b (STRING), c (INTEGER)
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
        required_cols: Option<&[ColumnName]>,
        schema: &Schema,
    ) -> Vec<ColumnName> {
        let mut filter = StatsColumnFilter::new(props, required_cols, None);
        let mut columns = Vec::new();
        filter.collect_columns(schema, &mut columns);
        columns
    }

    // ==================== Required column tests ====================

    #[rstest::rstest]
    #[case::required_overrides_limit(
        1,                              // num_indexed_cols limit
        vec!["c"],                      // required columns (3rd column)
        vec!["a", "c"]                  // expected: "a" (within limit) + "c" (required)
    )]
    #[case::no_required_uses_limit(
        2,                              // num_indexed_cols limit
        vec![],                         // no required columns
        vec!["a", "b"]                  // expected: first 2 columns within limit
    )]
    fn test_required_with_num_indexed_cols(
        #[case] num_cols: u64,
        #[case] required: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let props = make_props_with_num_cols(num_cols);
        let required_cols: Vec<ColumnName> =
            required.iter().map(|c| ColumnName::new([*c])).collect();
        let required_ref = if required_cols.is_empty() {
            None
        } else {
            Some(required_cols.as_slice())
        };
        let schema = abc_schema();

        let columns = collect_stats_columns(&props, required_ref, &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    #[rstest::rstest]
    #[case::required_added_to_stats(
        "a",                            // stats columns
        vec!["c"],                      // required columns
        vec!["a", "c"]                  // expected: "a" (explicit) + "c" (required)
    )]
    #[case::required_already_in_stats(
        "a,b",                          // stats columns include required col
        vec!["a"],                      // required columns (already in stats)
        vec!["a", "b"]                  // expected: no duplicates
    )]
    fn test_required_with_stats_columns(
        #[case] stats_cols: &str,
        #[case] required: Vec<&str>,
        #[case] expected: Vec<&str>,
    ) {
        let props = make_props_with_stats_cols(stats_cols);
        let required_cols: Vec<ColumnName> =
            required.iter().map(|c| ColumnName::new([*c])).collect();
        let schema = abc_schema();

        let columns = collect_stats_columns(&props, Some(&required_cols), &schema);

        let expected_cols: Vec<ColumnName> =
            expected.iter().map(|c| ColumnName::new([*c])).collect();
        assert_eq!(columns, expected_cols);
    }

    #[test]
    fn test_nested_required_column_with_limit() {
        // Test that nested required columns are found even with a column limit.
        let props = make_props_with_num_cols(2);

        // Required column is deeply nested: user.address.city
        let required_cols = vec![ColumnName::new(["user", "address", "city"])];

        let address_struct = StructType::new_unchecked([
            StructField::nullable("street", DataType::STRING),
            StructField::nullable("city", DataType::STRING), // required column
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

        let columns = collect_stats_columns(&props, Some(&required_cols), &schema);

        // Should include: id, name (first 2 within limit) + user.address.city (required)
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
    fn test_required_column_not_in_schema() {
        // Required column that doesn't exist in schema should be silently ignored
        let props = make_props_with_num_cols(2);
        let required_cols = vec![ColumnName::new(["nonexistent", "column"])];
        let schema = abc_schema();

        let columns = collect_stats_columns(&props, Some(&required_cols), &schema);

        // Should only include normal columns, required column not found
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["b"]),]
        );
    }
}
