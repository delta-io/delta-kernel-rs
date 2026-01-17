//! The `committer` module provides a [`Committer`] trait which allows different implementations to
//! define how to commit transactions to a catalog or filesystem. For catalog-managed tables, a
//! [`Committer`] specific to the managing catalog should be provided. For non-catalog-managed
//! tables, the [`FileSystemCommitter`] should be used to commit directly to the object store (via
//! put-if-absent call to storage to atomically write new commit files).
//!
//! By implementing the [`Committer`] trait, different catalogs can define what happens when the
//! kernel needs to commit a transaction to a table. The goal terminal state of every
//! [`Transaction`] is to be committed to the table. This means writing the changes (we call these
//! actions) in the transaction as a new version of the table. The [`Committer`] trait exposes a
//! single method, [`commit`] which takes an engine, an iterator of actions (as [`EngineData`]
//! batches), and [`CommitMetadata`] (which includes critical commit metadata like the version to
//! commit) to allow different catalogs to define what it means to 'commit' the actions to a table.
//! For some, this may mean writing staged commits to object storage and retaining an in-memory list
//! (server side) of commits. For others, this may mean writing new (version, actions) tuples to a
//! database.
//!
//! The implementation of [`commit`] must ensure that the actions are committed atomically to the
//! table at the given version and either (1) persisted directly to object storage as published
//! deltas as in non-catalog-managed tables or (2) persisted within the catalog and made available
//! to readers during snapshot contstruction via the [`log_tail`] API.
//!
//! [`Transaction`]: crate::transaction::Transaction
//! [`commit`]: crate::committer::Committer::commit
//! [`log_tail`]: crate::snapshot::SnapshotBuilder::with_log_tail
//! [`EngineData`]: crate::EngineData

mod commit_types;
mod filesystem;

pub use commit_types::{CommitMetadata, CommitResponse};
pub use filesystem::FileSystemCommitter;

use crate::{AsAny, DeltaResult, Engine, FilteredEngineData};

/// A Committer is the system by which transactions are committed to a table. Transactions are
/// effectively a collection of actions performed on the table at a specific version. The kernel
/// exposes this trait so different catalogs can build their own commit implementations. For
/// example, different catalogs may: commit directly to a database, commit to an object store, or
/// use another system entirely.
///
/// Critically, a Committer must implement [`commit`] which takes an engine and an iterator of
/// actions (as [`EngineData`] batches) to commit to the table at the given version
/// ([`CommitMetadata::version`]).
///
/// [`commit`]: Committer::commit
/// [`EngineData`]: crate::EngineData
//
// Note: While we could omit the Send bound, we keep it here for simplicity - so usage can be
// Arc<dyn Committer> (instead of Arc<dyn Committer + Send>). If there is a strong case for a !Send
// Committer then we can remove this bound and possibly just do an alias like CommitterRef =
// Arc<dyn Committer + Send>.
pub trait Committer: Send + AsAny {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse>;
}
