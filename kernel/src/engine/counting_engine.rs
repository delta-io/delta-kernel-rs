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
    /// Assertion run on drop.
    drop_check: DropCheck,
}

/// Operation-count assertion enforced when a [`CountingEngine`] is dropped.
#[derive(Debug)]
enum DropCheck {
    None,
    NoIo,
    OnlyStorageIo,
}

// Prints each handler's operation count.
impl fmt::Debug for CountingEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingEngine")
            .field("drop_check", &self.drop_check)
            .field("evaluation", &self.evaluation.op_count())
            .field("storage", &self.storage.op_count())
            .field("json", &self.json.op_count())
            .field("parquet", &self.parquet.op_count())
            .field("plans", &self.plan_op_count())
            .finish()
    }
}

impl CountingEngine {
    /// Wraps `inner` with zeroed operation counters and no drop-time assertion.
    pub fn new(inner: Arc<dyn Engine>) -> Self {
        Self::with_drop_check(inner, DropCheck::None)
    }

    /// Wraps `inner` and asserts on drop that no handler operations ran.
    pub fn new_no_io(inner: Arc<dyn Engine>) -> Self {
        Self::with_drop_check(inner, DropCheck::NoIo)
    }

    /// Wraps `inner` and asserts on drop that every handler operation was a storage operation.
    pub fn new_only_storage_io(inner: Arc<dyn Engine>) -> Self {
        Self::with_drop_check(inner, DropCheck::OnlyStorageIo)
    }

    fn with_drop_check(inner: Arc<dyn Engine>, drop_check: DropCheck) -> Self {
        Self {
            evaluation: Arc::new(CountingHandler::new(inner.evaluation_handler())),
            storage: Arc::new(CountingHandler::new(inner.storage_handler())),
            json: Arc::new(CountingHandler::new(inner.json_handler())),
            parquet: Arc::new(CountingHandler::new(inner.parquet_handler())),
            #[cfg(feature = "declarative-plans")]
            plans: inner
                .plan_executor()
                .map(|executor| Arc::new(CountingHandler::new(executor))),
            drop_check,
        }
    }

    /// Returns the plan-executor operation count, or `None` if there is no plan executor.
    pub fn plan_op_count(&self) -> Option<u64> {
        cfg_select! {
            feature = "declarative-plans" => self.plans.as_ref().map(|handler| handler.op_count()),
            _ => None,
        }
    }

    /// Returns the number of delegated operations across all handlers.
    pub fn total_op_count(&self) -> u64 {
        self.evaluation.op_count()
            + self.storage.op_count()
            + self.json.op_count()
            + self.parquet.op_count()
            + self.plan_op_count().unwrap_or(0)
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

impl Drop for CountingEngine {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Skip the check to avoid aborting the process with a double fault
            return;
        }
        match self.drop_check {
            DropCheck::None => {}
            DropCheck::NoIo => self.assert_no_io(),
            DropCheck::OnlyStorageIo => self.assert_only_storage_io(),
        }
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
    use crate::schema::StructType;

    #[test]
    fn handler_access_does_not_count_as_an_operation() {
        let engine = CountingEngine::new_no_io(Arc::new(SyncEngine::new()));
        let _ = engine.evaluation_handler();
        let _ = engine.storage_handler();
        let _ = engine.json_handler();
        let _ = engine.parquet_handler();
    }

    #[test]
    fn drop_check_for_storage_head() {
        let missing = Url::parse("memory:///missing").unwrap();
        let _ = CountingEngine::new_only_storage_io(Arc::new(SyncEngine::in_memory()))
            .storage_handler()
            .head(&missing);

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = CountingEngine::new_no_io(Arc::new(SyncEngine::in_memory()))
                    .storage_handler()
                    .head(&missing);
            }))
            .is_err()
        );
    }

    #[test]
    fn drop_check_for_json_read() {
        let empty_schema = Arc::new(StructType::new_unchecked(std::iter::empty()));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = CountingEngine::new_only_storage_io(Arc::new(SyncEngine::in_memory()))
                    .json_handler()
                    .read_json_files(&[], empty_schema.clone(), None);
            }))
            .is_err()
        );

        // Double fault: if Drop asserted during unwind, the process would abort.
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let engine = CountingEngine::new_no_io(Arc::new(SyncEngine::in_memory()));
            let _ = engine
                .json_handler()
                .read_json_files(&[], empty_schema, None);
            panic!("expected test panic");
        }))
        .unwrap_err();
        assert_eq!(err.downcast_ref::<&str>(), Some(&"expected test panic"));
    }
}
