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
    /// Current path during schema traversal. Pushed on field entry, popped on exit.
    path: Vec<String>,
}

impl<'col> StatsColumnFilter<'col> {
    pub(crate) fn new(props: &'col TableProperties) -> Self {
        // If data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified.
        if let Some(column_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_trie: Some(ColumnTrie::from_columns(column_names)),
                path: Vec::new(),
            }
        } else {
            let n_cols = props.data_skipping_num_indexed_cols.unwrap_or_default();
            Self {
                n_columns: Some(n_cols),
                added_columns: 0,
                column_trie: None,
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

    /// Returns true if the current path should be included based on column_trie config.
    pub(crate) fn should_include_current(&self) -> bool {
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
