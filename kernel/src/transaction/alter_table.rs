//! Alter table transaction types and constructor.
//!
//! This module defines the [`AlterTableTransaction`] type alias and the
//! [`try_new_alter_table`](AlterTableTransaction::try_new_alter_table) constructor.
//! The builder logic lives in [`builder::alter_table`](super::builder::alter_table).

#![allow(unreachable_pub)]

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::OnceLock;

use crate::committer::Committer;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::transaction::{AlterTable, Transaction};
use crate::utils::current_time_ms;
use crate::DeltaResult;

/// A type alias for alter-table transactions.
///
/// This provides a restricted API surface that only exposes operations valid during ALTER
/// commands. Data file operations are not available at compile time because `AlterTable`
/// does not implement [`SupportsDataFiles`](super::SupportsDataFiles).
pub type AlterTableTransaction = Transaction<AlterTable>;

impl AlterTableTransaction {
    /// Create a new transaction for altering a table's schema. Produces a metadata-only commit
    /// that emits an updated Metadata action with the evolved schema.
    ///
    /// The `effective_table_config` is the evolved table configuration (new schema, same
    /// protocol). It must be fully validated before calling this constructor (e.g. schema
    /// operations applied, protocol feature checks passed). The `read_snapshot` provides the
    /// pre-commit table state (version, previous protocol/metadata, ICT timestamps) used for
    /// commit versioning and post-commit snapshots.
    ///
    /// This is typically called via `AlterTableTransactionBuilder::build()` rather than directly.
    pub(crate) fn try_new_alter_table(
        read_snapshot: SnapshotRef,
        effective_table_config: TableConfiguration,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
            operation = "ALTER TABLE",
        );

        Ok(Transaction {
            span,
            read_snapshot_opt: Some(read_snapshot),
            effective_table_config,
            should_emit_protocol: false,
            should_emit_metadata: true,
            committer,
            operation: Some("ALTER TABLE".to_string()),
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp: current_time_ms()?,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            user_domain_removals: vec![],
            data_change: false,
            shared_write_state: OnceLock::new(),
            engine_commit_info: None,
            // TODO(#2446): match delta-spark's per-op isBlindAppend policy
            // (ADD/DROP/DROP NOT NULL -> true, SET NOT NULL -> false). Hardcoded false for
            // now: safe, but misses the true-case optimization delta-spark applies.
            is_blind_append: false,
            dv_matched_files: vec![],
            physical_clustering_columns: None,
            default_filled_columns: HashSet::new(),
            _state: PhantomData,
        })
    }
}
