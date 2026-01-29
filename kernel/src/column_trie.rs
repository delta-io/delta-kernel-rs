//! A trie (prefix tree) for efficient column path matching.
//!
//! Used to quickly determine if a column path matches or is a descendant of any
//! user-specified column. This provides O(path_length) lookup instead of
//! O(num_specified_columns * path_length).

use std::collections::HashMap;

use crate::expressions::ColumnName;

/// A trie (prefix tree) for efficient column path matching.
///
/// The lifetime `'col` ties this trie to the column names it was built from,
/// allowing it to borrow string slices instead of cloning.
///
/// The `Default` implementation creates an empty trie node with no children and
/// `is_terminal = false`. This is used both for creating a new root trie and for
/// creating intermediate nodes during insertion (via `or_default()`).
#[derive(Debug, Default)]
pub(crate) struct ColumnTrie<'col> {
    children: HashMap<&'col str, ColumnTrie<'col>>,
    /// True if this node represents the end of a specified column path.
    /// Intermediate nodes have `is_terminal = false`; only the final node of
    /// an inserted column path has `is_terminal = true`.
    is_terminal: bool,
}

impl<'col> ColumnTrie<'col> {
    /// Creates an empty trie.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Builds a trie from a list of column names.
    ///
    /// For example, `from_columns(&[column_name!("a.b"), column_name!("a.c")])` creates:
    /// ```text
    /// root (is_terminal=false)
    /// └── "a" (is_terminal=false)
    ///     ├── "b" (is_terminal=true)
    ///     └── "c" (is_terminal=true)
    /// ```
    pub(crate) fn from_columns(columns: &'col [ColumnName]) -> Self {
        let mut trie = Self::new();
        for column in columns {
            trie.insert(column);
        }
        trie
    }

    /// Inserts a column path into the trie.
    ///
    /// Walks down the trie for each path component, creating nodes as needed via `or_default()`
    /// (which initializes `is_terminal = false`). After the loop, only the final node is marked
    /// as terminal.
    ///
    /// For example, inserting `a.b.c` creates:
    /// ```text
    /// root (is_terminal=false)
    /// └── "a" (is_terminal=false)
    ///     └── "b" (is_terminal=false)
    ///         └── "c" (is_terminal=true)
    /// ```
    pub(crate) fn insert(&mut self, column: &'col ColumnName) {
        let mut node = self;
        for part in column.iter() {
            node = node.children.entry(part.as_str()).or_default();
        }
        node.is_terminal = true;
    }

    /// Returns true if `path` equals or is a descendant of any inserted column.
    ///
    /// For example, if the trie contains `["a", "b"]`:
    /// - `["a", "b"]` → true (exact match)
    /// - `["a", "b", "c"]` → true (descendant)
    /// - `["a"]` → false (ancestor, not descendant)
    /// - `["a", "x"]` → false (divergent path)
    pub(crate) fn contains_prefix_of(&self, path: &[String]) -> bool {
        let mut node = self;
        for part in path {
            if node.is_terminal {
                // We've matched a complete specified column, and path continues.
                // So path is a descendant of this specified column.
                return true;
            }
            match node.children.get(part.as_str()) {
                Some(child) => node = child,
                None => return false, // Path diverges from all specified columns
            }
        }
        // We've consumed the entire path. Match only if we're at a terminal.
        node.is_terminal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_trie() {
        // Build trie with specified column ["a", "b"]
        let columns = [ColumnName::new(["a", "b"])];
        let trie = ColumnTrie::from_columns(&columns);

        // Exact match: path = ["a", "b"] → include
        assert!(trie.contains_prefix_of(&["a".to_string(), "b".to_string()]));

        // Descendant of specified: path = ["a", "b", "c"] → include
        assert!(trie.contains_prefix_of(&["a".to_string(), "b".to_string(), "c".to_string()]));

        // Ancestor of specified: path = ["a"] → NOT include
        assert!(!trie.contains_prefix_of(&["a".to_string()]));

        // Unrelated paths → NOT include
        assert!(!trie.contains_prefix_of(&["a".to_string(), "c".to_string()]));
        assert!(!trie.contains_prefix_of(&["x".to_string(), "y".to_string()]));

        // Non-existent nested path: trie has ["a", "b", "c", "d"], path = ["a", "b"]
        // User asked for a.b.c.d but a.b is a leaf → NOT include
        let deep_columns = [ColumnName::new(["a", "b", "c", "d"])];
        let deep_trie = ColumnTrie::from_columns(&deep_columns);
        assert!(!deep_trie.contains_prefix_of(&["a".to_string(), "b".to_string()]));

        // Multiple specified columns
        let multi_columns = [
            ColumnName::new(["a", "b"]),
            ColumnName::new(["x", "y", "z"]),
        ];
        let multi_trie = ColumnTrie::from_columns(&multi_columns);
        assert!(multi_trie.contains_prefix_of(&["a".to_string(), "b".to_string()]));
        assert!(multi_trie.contains_prefix_of(&[
            "a".to_string(),
            "b".to_string(),
            "c".to_string()
        ]));
        assert!(multi_trie.contains_prefix_of(&[
            "x".to_string(),
            "y".to_string(),
            "z".to_string()
        ]));
        assert!(!multi_trie.contains_prefix_of(&["x".to_string(), "y".to_string()])); // ancestor
        assert!(!multi_trie.contains_prefix_of(&["a".to_string(), "c".to_string()]));
        // divergent
    }
}
