//! Post-commit context for CRC (version checksum) writing.
//!
//! This module provides the [`PostCommitContext`] struct that captures add/remove file
//! counts, sizes, domain metadata actions, ICT, and operation info collected during a
//! transaction commit. This context is passed into the post-commit snapshot to enable
//! CRC writing without redundant I/O.

use std::sync::LazyLock;

use crate::actions::DomainMetadata;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::column_name;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};

/// Information computed during a transaction commit that the post-commit snapshot
/// carries forward. Enables CRC writes and avoids redundant I/O.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)] // Fields used by CRC writer (future PR)
pub(crate) struct PostCommitContext {
    /// Number of add file actions in this transaction.
    pub(crate) num_add_files: i64,
    /// Total size in bytes of all added files.
    pub(crate) total_add_file_size_bytes: i64,
    /// Number of remove file actions in this transaction.
    pub(crate) num_remove_files: i64,
    /// Total size in bytes of all removed files.
    pub(crate) total_remove_file_size_bytes: i64,
    /// Domain metadata actions committed in this transaction (additions + tombstones).
    /// This is NOT all DMs -- just the subset the transaction already loaded.
    pub(crate) domain_metadata_actions: Vec<DomainMetadata>,
    /// The in-commit timestamp for this transaction, if ICT is enabled.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// The operation name (e.g., "WRITE", "MERGE", "DELETE").
    pub(crate) operation: Option<String>,
}

/// Visitor that counts file actions and sums their sizes from a `size` column.
///
/// Works with both add files (from `add_files_schema`) and remove/scan files
/// (from `scan_row_schema`), both of which have a top-level `size` column of type LONG.
///
/// When a selection vector is provided, only selected rows are counted.
#[derive(Default)]
pub(crate) struct FileSizeVisitor {
    /// Number of (selected) rows visited.
    pub(crate) count: i64,
    /// Sum of `size` values for (selected) rows.
    pub(crate) total_size: i64,
    /// Optional selection vector. If non-empty, only rows where `sv[i] == true` are counted.
    /// Rows beyond the selection vector length are treated as selected.
    selection_vector: Vec<bool>,
}

impl FileSizeVisitor {
    /// Create a visitor that only counts rows selected by the given selection vector.
    pub(crate) fn with_selection_vector(sv: &[bool]) -> Self {
        Self {
            selection_vector: sv.to_vec(),
            ..Default::default()
        }
    }

    fn is_selected(&self, i: usize) -> bool {
        self.selection_vector.get(i).copied().unwrap_or(true)
    }
}

impl RowVisitor for FileSizeVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("size")], vec![DataType::LONG]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "FileSizeVisitor expected 1 getter, got {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            if self.is_selected(i) {
                let size: i64 = getters[0].get(i, "file_size.size")?;
                self.count += 1;
                self.total_size += size;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_post_commit_context_default() {
        let ctx = PostCommitContext::default();
        assert_eq!(ctx.num_add_files, 0);
        assert_eq!(ctx.total_add_file_size_bytes, 0);
        assert_eq!(ctx.num_remove_files, 0);
        assert_eq!(ctx.total_remove_file_size_bytes, 0);
        assert!(ctx.domain_metadata_actions.is_empty());
        assert_eq!(ctx.in_commit_timestamp, None);
        assert_eq!(ctx.operation, None);
    }
}
