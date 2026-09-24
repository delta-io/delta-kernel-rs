//! Commit coordination for filesystem-managed and catalog-managed tables.
//!
//! [`FileSystemCommitter`] writes a new Delta version directly to object storage. A catalog
//! committer instead stages the commit, asks its catalog to ratify it, and later publishes
//! ratified commits to the Delta log.
//!
//! [`Committer`] is the Engine compatibility interface. [`Committer::commit_prepared`] receives a
//! lazy [`Commit`] and defaults to driving its actions as an Engine iterator for
//! [`Committer::commit`]. Coroutine-aware committers can override that bridge and consume the
//! prepared commit directly.

mod commit_types;
mod filesystem;
mod publish_types;

pub use commit_types::{CommitMetadata, CommitProtocolMetadata, CommitResponse, CommitType};
use derive_more::Constructor;
pub use filesystem::FileSystemCommitter;
pub use publish_types::{CatalogCommit, PublishMetadata};

use crate::coroutine::engine::EngineConnector;
use crate::coroutine::StaticGenerator;
use crate::{DeltaResult, DeltaResultIteratorStatic, Engine, FilteredEngineData};

/// An unpolled generator of commit actions.
///
/// cbindgen:ignore
pub type CommitActions = StaticGenerator<FilteredEngineData>;

/// A prepared transaction whose actions are ready for a committer to persist.
#[derive(Constructor)]
pub struct Commit {
    /// Metadata describing the target version and commit semantics.
    pub metadata: CommitMetadata,
    /// Commit actions in Delta log schema order.
    pub actions: CommitActions,
}

impl Commit {
    /// Construct a prepared commit from an Engine-based compatibility iterator.
    ///
    /// Advancing the commit actions calls `actions.next()` inline. Connector-driven paths should
    /// construct commit actions from kernel generators instead.
    pub fn from_engine_iterator(
        metadata: CommitMetadata,
        actions: DeltaResultIteratorStatic<FilteredEngineData>,
    ) -> Self {
        let actions = StaticGenerator::from_iterator(actions);
        Self { metadata, actions }
    }
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

    /// Commit a prepared transaction through the Engine compatibility interface.
    ///
    /// The default implementation drives the action generator as an Engine iterator and delegates
    /// to [`Self::commit`]. Coroutine-aware committers should override this method to consume the
    /// [`Commit`] directly. Overrides must validate that [`CommitMetadata::commit_type`] agrees
    /// with [`Self::is_catalog_committer`].
    fn commit_prepared(&self, engine: &dyn Engine, commit: Commit) -> DeltaResult<CommitResponse> {
        commit
            .metadata
            .validate_committer(self.is_catalog_committer())?;
        let actions = EngineConnector::new(engine).iterate_generator(commit.actions);
        self.commit(engine, Box::new(actions), commit.metadata)
    }

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
