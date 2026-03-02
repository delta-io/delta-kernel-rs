//! Transaction changes context for post-commit snapshot construction.
//!
//! Captures changes introduced by a committed transaction so the post-commit snapshot and CRC
//! can be constructed without re-reading from storage.
//!
//! TODO: Add fields for protocol, metadata, domain metadata, set transactions, and file stats.

use crate::path::ParsedLogPath;

/// See [module-level documentation](self) for details.
#[derive(Debug, Clone)]
pub(crate) struct TxnChangesContext {
    /// The parsed commit path for the committed transaction.
    pub(crate) parsed_commit: ParsedLogPath,
    /// The in-commit timestamp of this transaction, if ICT is enabled.
    pub(crate) in_commit_timestamp: Option<i64>,
}
