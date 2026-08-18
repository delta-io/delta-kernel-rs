//! The `committer` module provides the Engine-based [`Committer`] compatibility API. For
//! catalog-managed tables, a committer supplied by the managing catalog ratifies staged commits
//! and publishes them to the Delta log. For non-catalog-managed tables, [`FileSystemCommitter`]
//! atomically writes commits directly to object storage.
//!
//! By implementing the [`Committer`] trait, different catalogs can define what happens when the
//! kernel needs to commit a transaction to a table. The goal terminal state of every
//! [`Transaction`] is to be committed to the table. This means writing the changes (we call these
//! actions) in the transaction as a new version of the table. Its [`commit`] method takes an
//! engine, an iterator of actions (as [`EngineData`] batches), and [`CommitMetadata`] (which
//! includes critical commit metadata like the version to commit) to allow different catalogs to
//! define what it means to 'commit' the actions to a table.
//! For some, this may mean writing staged commits to object storage and retaining an in-memory list
//! (server side) of commits. For others, this may mean writing new (version, actions) tuples to a
//! database.
//!
//! The legacy [`commit`] method owns the complete write. Coroutine-driven commits delegate a
//! prepared [`Commit`] to the connector. Catalog workflows page its actions into a staged commit
//! and ratify that commit through the catalog protocol.
//!
//! [`Transaction`]: crate::transaction::Transaction
//! [`commit`]: crate::committer::Committer::commit
//! [`log_tail`]: crate::snapshot::SnapshotBuilder::with_log_tail
//! [`EngineData`]: crate::EngineData

mod commit_types;
mod filesystem;
mod publish_types;

pub use commit_types::{CommitMetadata, CommitProtocolMetadata, CommitResponse, CommitType};
use derive_more::Constructor;
pub use filesystem::FileSystemCommitter;
pub use publish_types::{CatalogCommit, PublishMetadata};

use crate::coroutine::Generator;
use crate::{DeltaResult, DeltaResultIteratorStatic, Engine, FilteredEngineData};

/// A prepared transaction whose actions are ready for a committer to persist.
#[derive(Constructor)]
pub struct Commit {
    /// Metadata describing the target version and commit semantics.
    pub metadata: CommitMetadata,
    /// Commit actions in Delta log schema order.
    pub actions: Generator<FilteredEngineData>,
}

/// Engine-based compatibility driver for committing and publishing transactions.
///
/// [`commit`] performs the complete legacy write. Coroutine-driven connectors receive a prepared
/// [`Commit`] through the kernel request protocol instead.
///
/// [`commit`]: Committer::commit
/// [`EngineData`]: crate::EngineData
//
// Note: While we could omit the Send bound, we keep it here for simplicity - so usage can be
// Arc<dyn Committer> (instead of Arc<dyn Committer + Send>). If there is a strong case for a !Send
// Committer then we can remove this bound and possibly just do an alias like CommitterRef =
// Arc<dyn Committer + Send>.
pub trait Committer: Send {
    /// Commits actions to the table at the version specified in [`CommitMetadata`].
    ///
    /// Implementations must ensure that actions are committed atomically and either:
    /// 1. Persisted directly to object storage as published deltas (for filesystem-based tables),
    ///    or
    /// 2. Persisted as per the managing catalog's semantics (for catalog-managed tables)
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIteratorStatic<FilteredEngineData>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse>;

    /// Returns `true` if this committer is for a catalog-managed table, else `false`.
    fn is_catalog_committer(&self) -> bool;

    /// Publishes catalog commits to the Delta log. Applicable only to catalog-managed tables.
    ///
    /// Publishing is the act of copying ratified catalog commits to the Delta log as published
    /// Delta files (e.g., `_delta_log/00000000000000000001.json`).
    ///
    /// # When to call
    ///
    /// This method should only be called on catalog committers (i.e., when [`is_catalog_committer`]
    /// returns `true`). Filesystem committers will error if called with catalog commits to publish.
    ///
    /// # Benefits
    ///
    /// - Reduces the number of commits the catalog needs to store internally and serve to readers
    /// - Enables table maintenance operations that must operate on published versions only, such as
    ///   checkpointing and log compaction
    ///
    /// # Requirements
    ///
    /// - This method must ensure that all catalog commits are published to the Delta log up to and
    ///   including the snapshot version specified in [`PublishMetadata`]
    /// - Commits must be published in order: version V-1 must be published before version V
    ///
    /// # Catalog-specific semantics
    ///
    /// Each catalog implementation may specify its own rules and semantics for publishing,
    /// including whether it expects to be notified immediately upon publishing success, whether
    /// published commits must appear with PUT-if-absent semantics in the Delta log, and whether
    /// publishing happens in the client-side or server-side catalog component.
    ///
    /// # Errors
    ///
    /// Returns an error if the publish operation fails.
    ///
    /// [`is_catalog_committer`]: Committer::is_catalog_committer
    fn publish(&self, engine: &dyn Engine, publish_metadata: PublishMetadata) -> DeltaResult<()>;
}
