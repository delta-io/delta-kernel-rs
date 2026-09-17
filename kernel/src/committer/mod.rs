//! Commit coordination for filesystem-managed and catalog-managed tables.
//!
//! [`FileSystemCommitter`] writes a new Delta version directly to object storage. A catalog
//! committer instead stages the commit, asks its catalog to ratify it, and later publishes
//! ratified commits to the Delta log.
//!
//! [`Committer`] is the Engine compatibility interface: its [`commit`] method receives an
//! [`Engine`], generated actions as [`EngineData`] batches, and the target [`CommitMetadata`].
//! Connector-driven transactions emit a prepared [`Commit`] request instead. Its [`CommitActions`]
//! can be consumed through a parent coroutine channel or started as a generator task.
//!
//! [`commit`]: crate::committer::Committer::commit
//! [`EngineData`]: crate::EngineData

mod commit_types;
mod filesystem;
mod publish_types;

pub use commit_types::{CommitMetadata, CommitProtocolMetadata, CommitResponse, CommitType};
use derive_more::Constructor;
pub use filesystem::FileSystemCommitter;
pub use publish_types::{CatalogCommit, PublishMetadata};

use crate::coroutine::kernel::generator::{
    BoundGenerator, BoxedGenerator, Generator, GeneratorImpl,
};
use crate::coroutine::{Channel, GeneratorTask};
use crate::{DeltaResult, DeltaResultIteratorStatic, Engine, FilteredEngineData};

/// A lazy stream of commit actions that can be consumed directly or started for a connector.
pub struct CommitActions(BoxedGenerator<FilteredEngineData>);

impl CommitActions {
    /// Erase the concrete generator body type.
    pub(crate) fn new<G>(actions: G) -> Self
    where
        G: Generator<FilteredEngineData> + 'static,
    {
        Self(BoxedGenerator::new(actions))
    }

    /// Adapt an Engine-based compatibility iterator into lazy commit actions.
    ///
    /// Advancing the result calls `actions.next()` inline. Connector-driven paths must construct
    /// commit actions from kernel generators instead.
    pub fn from_engine_iterator(actions: DeltaResultIteratorStatic<FilteredEngineData>) -> Self {
        Self::new(GeneratorImpl::new(async move |yielder| {
            for action in actions {
                yielder.yield_item(action?).await?;
            }
            Ok(())
        }))
    }

    /// Permanently bind these actions to `channel`.
    pub fn bind(self, channel: &Channel) -> BoundGenerator<'_, FilteredEngineData, ()> {
        self.0.bind(channel)
    }

    /// Start a task that exposes commit actions to a connector.
    pub fn start(self) -> GeneratorTask<FilteredEngineData> {
        self.0.start()
    }
}

/// A prepared transaction whose actions are ready for a committer to persist.
#[derive(Constructor)]
pub struct Commit {
    /// Metadata describing the target version and commit semantics.
    pub metadata: CommitMetadata,
    /// Commit actions in Delta log schema order.
    pub actions: CommitActions,
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
