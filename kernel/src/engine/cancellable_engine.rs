//! Binds an operation's cancellation token to reads through the connector's engine handlers.
//!
//! Snapshot building and scan metadata operations use `CancellableEngine` when configured with a
//! token. The wrappers supply that token when loading code calls an ordinary read method, or a
//! cancellation-aware one with no token of its own, so discovery and replay share cancellation
//! without threading a token through each helper.

use std::sync::Arc;

use bytes::Bytes;
use derive_more::Constructor;
use url::Url;

use crate::cancellation::check_cancelled;
#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{
    CancellationTokenRef, Engine, EngineData, EvaluationHandler, FileDataReadResultIterator,
    FileMeta, FileSize, FileSlice, FilteredEngineData, JsonHandler, KernelResult,
    KernelResultIterator, ParquetFooter, ParquetHandler, PredicateRef, SchemaRef, StorageHandler,
};

/// An engine adapter that supplies a default cancellation token to storage, JSON, and
/// Parquet reads.
///
/// Operations with cancellation supply one token to [`Self::new`]. The wrapped handlers use it as
/// the default for reads, so loading helpers inherit cancellation through the engine.
///
/// At the handler API boundary, `*_with_cancellation` methods accept an optional per-call
/// token. `None` uses the stored default; `Some(token)` overrides it for that read and is
/// forwarded unchanged.
///
/// Write methods forward to the underlying handlers without applying this adapter's
/// cancellation token. Parsing, evaluation, and declarative execution retain their underlying
/// behavior.
///
/// Snapshots retain neither the adapter nor its token. Read iterators own their cancellation state
/// and can outlive the adapter. Cached metadata can be returned without consulting the default
/// token.
pub(crate) struct CancellableEngine {
    /// Original expression evaluator.
    evaluation: Arc<dyn EvaluationHandler>,
    /// Storage handler bound to the default read token.
    storage: Arc<dyn StorageHandler>,
    /// JSON handler bound to the default read token.
    json: Arc<dyn JsonHandler>,
    /// Parquet handler bound to the default read token.
    parquet: Arc<dyn ParquetHandler>,
    /// Original declarative executor, when available.
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
}

impl CancellableEngine {
    /// Captures `engine`'s handlers and binds `cancellation_token` without issuing I/O.
    pub(crate) fn new(engine: &dyn Engine, cancellation_token: CancellationTokenRef) -> Self {
        Self {
            evaluation: engine.evaluation_handler(),
            storage: Arc::new(ReadHandler::new(
                engine.storage_handler(),
                cancellation_token.clone(),
            )),
            json: Arc::new(ReadHandler::new(
                engine.json_handler(),
                cancellation_token.clone(),
            )),
            parquet: Arc::new(ReadHandler::new(
                engine.parquet_handler(),
                cancellation_token,
            )),
            #[cfg(feature = "declarative-plans")]
            plan_executor: engine.plan_executor(),
        }
    }
}

impl Engine for CancellableEngine {
    /// Returns the original expression evaluator.
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    /// Returns the storage handler with the default read token.
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    /// Returns the JSON handler with the default read token.
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    /// Returns the Parquet handler with the default read token.
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    /// Returns the original declarative plan executor.
    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        self.plan_executor.clone()
    }
}

/// Supplies a default cancellation token at a handler's read boundary.
#[derive(Constructor)]
struct ReadHandler<H: ?Sized> {
    /// Underlying handler receiving delegated operations.
    inner: Arc<H>,
    /// Default read token when the caller supplies none.
    cancellation_token: CancellationTokenRef,
}

/// Generates plain and cancellation-aware reads that share token selection.
macro_rules! cancellable_read_pair {
    (
        $(#[$doc:meta])*
        fn $plain:ident, $cancellable:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;
    ) => {
        $(#[$doc])*
        fn $plain(&self, $($arg: $ty),*) -> $result {
            self.$cancellable($($arg,)* None)
        }

        $(#[$doc])*
        fn $cancellable(
            &self,
            $($arg: $ty,)*
            token: Option<CancellationTokenRef>,
        ) -> $result {
            self.inner.$cancellable(
                $($arg,)*
                Some(token.unwrap_or_else(|| self.cancellation_token.clone())),
            )
        }
    };
}

/// Forwards methods to the inner handler unchanged.
macro_rules! forward_methods {
    ($(
        $(#[$doc:meta])*
        fn $method:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;
    )*) => {$(
        $(#[$doc])*
        fn $method(&self, $($arg: $ty),*) -> $result {
            self.inner.$method($($arg),*)
        }
    )*};
}

impl StorageHandler for ReadHandler<dyn StorageHandler> {
    cancellable_read_pair! {
        /// Lists files using a per-call token or the default token.
        fn list_from, list_from_with_cancellation(path: &Url)
            -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>>;
    }

    cancellable_read_pair! {
        /// Reads byte ranges using a per-call token or the default token.
        fn read_files, read_files_with_cancellation(files: Vec<FileSlice>)
            -> KernelResult<Box<dyn KernelResultIterator<Bytes>>>;
    }

    /// Returns file metadata after checking the default token for cancellation.
    ///
    /// `StorageHandler::head` accepts no cancellation token, so this wrapper cannot
    /// interrupt an in-flight request. Nested adapters each check their default token.
    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        check_cancelled(Some(&self.cancellation_token))?;
        self.inner.head(path)
    }

    forward_methods! {
        /// Writes bytes without applying read cancellation.
        fn put(path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()>;

        /// Copies a file atomically without applying read cancellation.
        fn copy_atomic(src: &Url, dest: &Url) -> KernelResult<()>;

        /// Deletes a file without applying read cancellation.
        fn delete(path: &Url) -> KernelResult<()>;
    }
}

impl JsonHandler for ReadHandler<dyn JsonHandler> {
    cancellable_read_pair! {
        /// Reads projected JSON batches using a per-call token or the default token.
        fn read_json_files, read_json_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
    }

    forward_methods! {
        /// Parses JSON batches without applying read cancellation.
        fn parse_json(data: Box<dyn EngineData>, schema: SchemaRef) -> KernelResult<Box<dyn EngineData>>;

        /// Writes JSON batches without applying read cancellation.
        fn write_json_file(
            path: &Url,
            data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            overwrite: bool,
        ) -> KernelResult<FileSize>;
    }
}

impl ParquetHandler for ReadHandler<dyn ParquetHandler> {
    cancellable_read_pair! {
        /// Reads projected Parquet batches using a per-call token or the default token.
        fn read_parquet_files, read_parquet_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
    }

    cancellable_read_pair! {
        /// Reads a Parquet footer using a per-call token or the default token.
        fn read_parquet_footer, read_parquet_footer_with_cancellation(file: &FileMeta)
            -> KernelResult<ParquetFooter>;
    }

    forward_methods! {
        /// Writes Parquet batches without applying read cancellation.
        fn write_parquet_file(
            location: Url, data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
        ) -> KernelResult<FileSize>;
    }
}

#[cfg(test)]
mod tests;
