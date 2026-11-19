//! Engine decorator that counts handler operations while delegating their work.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use crate::engine::RelaxedCounter;
use crate::expressions::{ExpressionRef, PredicateRef, Scalar};
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanExecutor, PlanResult};
use crate::schema::{DataType, SchemaRef};
use crate::{
    CancellationTokenRef, Engine, EngineData, EvaluationHandler, ExpressionEvaluator,
    FileDataReadResultIterator, FileMeta, FileSize, FileSlice, FilteredEngineData, JsonHandler,
    KernelResult, KernelResultIterator, ParquetFooter, ParquetHandler, PredicateEvaluator,
    StorageHandler,
};

/// A handler decorator that counts each dereference to its inner handler.
pub struct CountingHandler<T: ?Sized> {
    inner: Arc<T>,
    operations: RelaxedCounter,
}

impl<T: ?Sized> CountingHandler<T> {
    fn new(inner: Arc<T>) -> Self {
        let operations = RelaxedCounter::default();
        Self { inner, operations }
    }

    /// Returns the number of delegated operations.
    pub fn op_count(&self) -> u64 {
        self.operations.get()
    }
}

// Prints only the operation count: the inner handler is not required to implement `Debug`.
impl<T: ?Sized> fmt::Debug for CountingHandler<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.op_count(), f)
    }
}

/// Counts on deref. The handler trait impls take advantage of that by reaching the inner handler
/// through this path, so every forwarded method call increments the counter.
impl<T: ?Sized> Deref for CountingHandler<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.operations.inc();
        self.inner.as_ref()
    }
}

/// An [`Engine`] decorator that counts handler operations and delegates them to an inner engine.
///
/// Fetching a handler is not an operation; only calls on the handler's methods are counted.
#[derive(Clone, Debug)]
pub struct CountingEngine {
    /// Evaluation-handler operations.
    pub evaluation: Arc<CountingHandler<dyn EvaluationHandler>>,
    /// Storage-handler operations.
    pub storage: Arc<CountingHandler<dyn StorageHandler>>,
    /// JSON-handler operations.
    pub json: Arc<CountingHandler<dyn JsonHandler>>,
    /// Parquet-handler operations.
    pub parquet: Arc<CountingHandler<dyn ParquetHandler>>,
    /// Plan-executor operations, or `None` if the inner engine provides no executor.
    #[cfg(feature = "declarative-plans")]
    pub plans: Option<Arc<CountingHandler<dyn PlanExecutor>>>,
}

impl CountingEngine {
    /// Wraps `inner` with zeroed operation counters.
    pub fn new(inner: Arc<dyn Engine>) -> Self {
        Self {
            evaluation: Arc::new(CountingHandler::new(inner.evaluation_handler())),
            storage: Arc::new(CountingHandler::new(inner.storage_handler())),
            json: Arc::new(CountingHandler::new(inner.json_handler())),
            parquet: Arc::new(CountingHandler::new(inner.parquet_handler())),
            #[cfg(feature = "declarative-plans")]
            plans: inner
                .plan_executor()
                .map(|executor| Arc::new(CountingHandler::new(executor))),
        }
    }

    /// Returns the number of delegated operations across all handlers.
    pub fn total_op_count(&self) -> u64 {
        let plan_ops = cfg_select! {
            feature = "declarative-plans" => {
                self.plans.as_ref().map_or(0, |handler| handler.op_count())
            }
            _ => 0,
        };
        self.evaluation.op_count()
            + self.storage.op_count()
            + self.json.op_count()
            + self.parquet.op_count()
            + plan_ops
    }

    /// Asserts that no delegated handler operations have run.
    pub fn assert_no_io(&self) {
        assert_eq!(self.total_op_count(), 0, "expected no engine I/O: {self:?}");
    }

    /// Asserts that every delegated operation was a storage operation.
    pub fn assert_only_storage_io(&self) {
        assert_eq!(
            self.total_op_count(),
            self.storage.op_count(),
            "expected only storage I/O: {self:?}"
        );
    }
}

impl Engine for CountingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        Some(self.plans.as_ref()?.clone())
    }
}

/// Implements `$trait` for [`CountingHandler`]. Each call goes through [`Deref::deref`], which
/// counts it.
macro_rules! forward_counting_handler {
    ($trait:ident {
        $(fn $method:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;)*
    }) => {
        impl<T: ?Sized + $trait> $trait for CountingHandler<T> {
            $(
                fn $method(&self, $($arg: $ty),*) -> $result {
                    $trait::$method(Deref::deref(self), $($arg),*)
                }
            )*
        }
    };
}

forward_counting_handler! {
    EvaluationHandler {
        fn new_expression_evaluator(
            input_schema: SchemaRef,
            expression: ExpressionRef,
            output_type: DataType,
        ) -> KernelResult<Arc<dyn ExpressionEvaluator>>;
        fn new_predicate_evaluator(
            input_schema: SchemaRef,
            predicate: PredicateRef,
        ) -> KernelResult<Arc<dyn PredicateEvaluator>>;
        fn create_many(
            schema: SchemaRef,
            rows: Vec<Vec<Scalar>>,
        ) -> KernelResult<Box<dyn EngineData>>;
    }
}

forward_counting_handler! {
    StorageHandler {
        fn list_from(path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>>;
        fn list_from_with_cancellation(
            path: &Url,
            cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>>;
        fn read_files(files: Vec<FileSlice>) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>>;
        fn read_files_with_cancellation(
            files: Vec<FileSlice>,
            cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>>;
        fn copy_atomic(src: &Url, dest: &Url) -> KernelResult<()>;
        fn put(path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()>;
        fn head(path: &Url) -> KernelResult<FileMeta>;
        fn delete(path: &Url) -> KernelResult<()>;
    }
}

forward_counting_handler! {
    JsonHandler {
        fn parse_json(
            json_strings: Box<dyn EngineData>,
            output_schema: SchemaRef,
        ) -> KernelResult<Box<dyn EngineData>>;
        fn read_json_files(
            files: &[FileMeta],
            physical_schema: SchemaRef,
            predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
        fn read_json_files_with_cancellation(
            files: &[FileMeta],
            physical_schema: SchemaRef,
            predicate: Option<PredicateRef>,
            cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
        fn write_json_file(
            path: &Url,
            data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            overwrite: bool,
        ) -> KernelResult<FileSize>;
    }
}

forward_counting_handler! {
    ParquetHandler {
        fn read_parquet_files(
            files: &[FileMeta],
            physical_schema: SchemaRef,
            predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
        fn read_parquet_files_with_cancellation(
            files: &[FileMeta],
            physical_schema: SchemaRef,
            predicate: Option<PredicateRef>,
            cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
        fn write_parquet_file(
            location: Url,
            data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
        ) -> KernelResult<FileSize>;
        fn read_parquet_footer(file: &FileMeta) -> KernelResult<ParquetFooter>;
        fn read_parquet_footer_with_cancellation(
            file: &FileMeta,
            cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<ParquetFooter>;
    }
}

#[cfg(feature = "declarative-plans")]
forward_counting_handler! {
    PlanExecutor {
        fn execute_op(op: Operation) -> KernelResult<PlanResult>;
        fn read_parquet_footer(file: FileMeta) -> KernelResult<ParquetFooter>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;

    #[test]
    fn handler_access_does_not_count_as_an_operation() {
        let inner = Arc::new(SyncEngine::new());
        let engine = CountingEngine::new(inner);

        let _ = engine.evaluation_handler();
        let _ = engine.storage_handler();
        let _ = engine.json_handler();
        let _ = engine.parquet_handler();

        assert_eq!(engine.evaluation.op_count(), 0);
        assert_eq!(engine.storage.op_count(), 0);
        assert_eq!(engine.json.op_count(), 0);
        assert_eq!(engine.parquet.op_count(), 0);
    }

    #[test]
    fn handler_operation_increments_its_counter() {
        let inner = Arc::new(SyncEngine::in_memory());
        let engine = CountingEngine::new(inner);

        let result = engine
            .storage_handler()
            .head(&Url::parse("memory:///missing").unwrap());

        assert!(result.is_err());
        assert_eq!(engine.storage.op_count(), 1);
        assert_eq!(engine.json.op_count(), 0);
    }
}
