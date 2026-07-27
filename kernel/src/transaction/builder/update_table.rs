//! Builder for update (write) transactions on an existing Delta table.
//!
//! This module contains [`UpdateTableTransactionBuilder`], which stages write configuration
//! (operation, engine info, blind-append, domain metadata, set-transaction, commit info) and
//! then builds a [`Transaction`] against an existing table.
//!
//! Use [`Snapshot::transaction()`](crate::snapshot::Snapshot::transaction) as the entry point
//! rather than constructing the builder directly.

use std::sync::Arc;

use crate::committer::Committer;
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::transaction::Transaction;
use crate::{DeltaResult, Engine, EngineData};

/// Builder for constructing a [`Transaction`] that updates an existing Delta table.
///
/// Write configuration is staged on the builder and applied when [`build`](Self::build) is
/// called. The resulting [`Transaction`] exposes only data-staging and commit operations
/// (`add_files`, `remove_files`, `update_deletion_vectors`, write contexts, `commit`).
///
/// Created via [`Snapshot::transaction()`](crate::snapshot::Snapshot::transaction).
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel::Engine;
/// # use delta_kernel::snapshot::Snapshot;
/// # use delta_kernel::committer::FileSystemCommitter;
/// # fn example(engine: Arc<dyn Engine>, table_url: url::Url) -> delta_kernel::DeltaResult<()> {
/// let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
/// let txn = snapshot
///     .transaction()
///     .with_operation("WRITE".to_string())
///     .with_engine_info("MyApp/1.0")
///     .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
/// # Ok(())
/// # }
/// ```
pub struct UpdateTableTransactionBuilder {
    snapshot: SnapshotRef,
    operation: Option<String>,
    engine_info: Option<String>,
    correlation_id: Option<Arc<str>>,
    is_blind_append: bool,
    data_change: Option<bool>,
    domain_metadata_additions: Vec<(String, String)>,
    domain_removals: Vec<String>,
    set_transactions: Vec<(String, i64)>,
    engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
}

impl UpdateTableTransactionBuilder {
    /// Create a new builder from a snapshot of an existing table.
    ///
    /// This is typically called via
    /// [`Snapshot::transaction()`](crate::snapshot::Snapshot::transaction) rather than directly.
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        Self {
            snapshot,
            operation: None,
            engine_info: None,
            correlation_id: None,
            is_blind_append: false,
            data_change: None,
            domain_metadata_additions: Vec::new(),
            domain_removals: Vec::new(),
            set_transactions: Vec::new(),
            engine_commit_info: None,
        }
    }

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commit and visible to anyone who describes the table history.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Set the engine info field of this transaction's commit info action. This field is optional.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    /// Attach an opaque, caller-supplied correlation id for joining this transaction's commit
    /// metric events to the caller's own request or operation id. An empty id is treated as unset.
    /// When unset, behavior is unchanged.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Mark this transaction as a blind append.
    ///
    /// Blind append transactions should only add new files and avoid write operations that
    /// depend on existing table state.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Set whether this transaction changes data. Defaults to `true`. Set to `false` for
    /// operations that only change metadata or move rows between files without logical changes
    /// (e.g. backfilling statistics, OPTIMIZE).
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = Some(data_change);
        self
    }

    /// Set domain metadata to be written to the Delta log.
    ///
    /// Note that each domain can only appear once per transaction. Setting and removing the same
    /// domain in a single transaction is disallowed. Duplicate domains cause the `commit` to fail.
    pub fn with_domain_metadata(mut self, domain: String, configuration: String) -> Self {
        self.domain_metadata_additions.push((domain, configuration));
        self
    }

    /// Remove domain metadata from the Delta log.
    ///
    /// If the domain exists in the Delta log, this creates a tombstone to logically delete the
    /// domain, preserving the previous configuration value. If the domain does not exist, this is
    /// a no-op. Each domain can only appear once per transaction; duplicates cause the `commit`
    /// to fail.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.domain_removals.push(domain);
        self
    }

    /// Include a SetTransaction (`app_id` and `version`) action for this transaction.
    ///
    /// Note that each `app_id` can only appear once per transaction. Duplicate `app_id`s cause the
    /// `commit` to fail.
    pub fn with_transaction_id(mut self, app_id: String, version: i64) -> Self {
        self.set_transactions.push((app_id, version));
        self
    }

    /// Set the content of the commitInfo action for this transaction. Kernel always writes a
    /// commitInfo; this lets engines add their own data. Kernel-owned fields (timestamp,
    /// inCommitTimestamp, operation, operationParameters, kernelVersion, isBlindAppend, engineInfo,
    /// txnId) are overridden and should not be set here.
    pub fn with_commit_info(
        mut self,
        engine_commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.engine_commit_info = Some((engine_commit_info, commit_info_schema));
        self
    }

    /// Validate the table supports writes and build the [`Transaction`], applying all staged
    /// write configuration.
    ///
    /// # Errors
    ///
    /// - The table does not support writes (unsupported reader/writer features).
    /// - Reading clustering columns from the snapshot fails.
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
        let mut txn = Transaction::try_new_existing_table(self.snapshot, committer, engine)?;

        if let Some(operation) = self.operation {
            txn = txn.with_operation(operation);
        }
        if let Some(engine_info) = self.engine_info {
            txn = txn.with_engine_info(engine_info);
        }
        if let Some(correlation_id) = self.correlation_id {
            txn = txn.with_correlation_id(correlation_id);
        }
        if self.is_blind_append {
            txn = txn.with_blind_append();
        }
        if let Some(data_change) = self.data_change {
            txn = txn.with_data_change(data_change);
        }
        for (domain, configuration) in self.domain_metadata_additions {
            txn = txn.with_domain_metadata(domain, configuration);
        }
        for domain in self.domain_removals {
            txn = txn.with_domain_metadata_removed(domain);
        }
        for (app_id, version) in self.set_transactions {
            txn = txn.with_transaction_id(app_id, version);
        }
        if let Some((engine_commit_info, schema)) = self.engine_commit_info {
            txn = txn.with_commit_info(engine_commit_info, schema);
        }

        Ok(txn)
    }
}
