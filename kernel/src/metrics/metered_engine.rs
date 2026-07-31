//! [`MeteredDeltaEngine`] wraps any [`Engine`] so its `storage_handler`,
//! `json_handler`, and `parquet_handler` emit the kernel's standard handler-completion
//! tracing spans. `evaluation_handler` passes through unchanged.
//!
//! ```ignore
//! let inner: Arc<dyn Engine> = Arc::new(MyEngine::build()?);
//! let engine: Arc<dyn Engine> = Arc::new(MeteredDeltaEngine::new(inner));
//! ```

use std::sync::Arc;

use crate::metrics::{MeteredJsonHandler, MeteredParquetHandler, MeteredStorageHandler};
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// Decorator over any [`Engine`] that meters its storage, JSON, and Parquet handlers.
/// See module docs.
pub struct MeteredDeltaEngine {
    inner: Arc<dyn Engine>,
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
}

impl MeteredDeltaEngine {
    /// Wrap `inner`. Debug-asserts that none of `inner`'s handlers are already metered
    /// wrappers, so the resulting engine emits each span exactly once.
    ///
    /// The check is shallow: it inspects the immediate concrete type returned by each
    /// handler accessor and does not walk intermediate wrapper types. Wrapping a metered
    /// handler behind a non-metered wrapper before re-wrapping (e.g.
    /// `Metered(Foo(Metered(...)))`) silently double-counts.
    pub fn new(inner: Arc<dyn Engine>) -> Self {
        let inner_storage = inner.storage_handler();
        debug_assert!(
            !inner_storage.any_ref().is::<MeteredStorageHandler>(),
            "MeteredDeltaEngine wraps an engine whose storage_handler is already a \
             MeteredStorageHandler; remove the outer wrap to avoid double-counting metrics",
        );
        let inner_json = inner.json_handler();
        debug_assert!(
            !inner_json.any_ref().is::<MeteredJsonHandler>(),
            "MeteredDeltaEngine wraps an engine whose json_handler is already a \
             MeteredJsonHandler; remove the outer wrap to avoid double-counting metrics",
        );
        let inner_parquet = inner.parquet_handler();
        debug_assert!(
            !inner_parquet.any_ref().is::<MeteredParquetHandler>(),
            "MeteredDeltaEngine wraps an engine whose parquet_handler is already a \
             MeteredParquetHandler; remove the outer wrap to avoid double-counting metrics",
        );
        let storage = Arc::new(MeteredStorageHandler::new(inner_storage));
        let json = Arc::new(MeteredJsonHandler::new(inner_json));
        let parquet = Arc::new(MeteredParquetHandler::new(inner_parquet));
        Self {
            inner,
            storage,
            json,
            parquet,
        }
    }
}

impl std::fmt::Debug for MeteredDeltaEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredDeltaEngine").finish_non_exhaustive()
    }
}

impl Engine for MeteredDeltaEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        Arc::clone(&self.storage)
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        Arc::clone(&self.json)
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        Arc::clone(&self.parquet)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use url::Url;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::metrics::MetricEvent;
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};
    use crate::{DeltaResult, FileMeta, FileSlice};

    /// Minimal storage handler used in tests: returns N preconfigured FileMeta items.
    #[derive(Debug)]
    struct StubStorageHandler {
        list_results: Vec<FileMeta>,
    }

    impl StorageHandler for StubStorageHandler {
        fn list_from(
            &self,
            _path: &Url,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
            let results: Vec<_> = self.list_results.iter().cloned().map(Ok).collect();
            Ok(Box::new(results.into_iter()))
        }
        fn read_files(
            &self,
            _files: Vec<FileSlice>,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
            Ok(Box::new(std::iter::empty()))
        }
        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
            Ok(())
        }
        fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> DeltaResult<()> {
            Ok(())
        }
        fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
            unreachable!("not exercised")
        }
        fn delete(&self, _path: &Url) -> DeltaResult<()> {
            Ok(())
        }
    }

    fn stub_engine() -> DelegatingEngine {
        DelegatingEngine::new(Arc::new(SyncEngine::new())).with_storage_handler(Arc::new(
            StubStorageHandler {
                list_results: vec![
                    FileMeta {
                        location: Url::parse("memory:///_delta_log/00000000000000000000.json")
                            .unwrap(),
                        last_modified: 0,
                        size: 0,
                    },
                    FileMeta {
                        location: Url::parse("memory:///_delta_log/00000000000000000001.json")
                            .unwrap(),
                        last_modified: 0,
                        size: 0,
                    },
                ],
            },
        ))
    }

    #[test]
    fn storage_handler_emits_metered_spans() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(Arc::clone(&reporter) as _);

        let engine = MeteredDeltaEngine::new(Arc::new(stub_engine()));
        let url = Url::parse("memory:///_delta_log/").unwrap();
        let iter = engine.storage_handler().list_from(&url).unwrap();
        let _: Vec<_> = iter.collect();

        let listed = reporter
            .events()
            .iter()
            .find(|e| matches!(e, MetricEvent::StorageListCompleted(_)))
            .cloned()
            .expect("expected StorageListCompleted via metering wrapper");
        let MetricEvent::StorageListCompleted(e) = listed else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
    }

    #[test]
    fn evaluation_handler_passes_through() {
        let inner: Arc<dyn Engine> = Arc::new(stub_engine());
        let inner_eval = inner.evaluation_handler();

        let engine = MeteredDeltaEngine::new(inner);
        assert!(Arc::ptr_eq(&inner_eval, &engine.evaluation_handler()));
    }

    #[test]
    fn json_and_parquet_handlers_are_metered() {
        let engine = MeteredDeltaEngine::new(Arc::new(stub_engine()));
        assert!(engine.json_handler().any_ref().is::<MeteredJsonHandler>());
        assert!(engine
            .parquet_handler()
            .any_ref()
            .is::<MeteredParquetHandler>());
    }

    #[test]
    #[should_panic(expected = "storage_handler is already a MeteredStorageHandler")]
    fn new_panics_when_inner_storage_already_metered() {
        let inner = stub_engine();
        let metered = Arc::new(MeteredStorageHandler::new(inner.storage_handler()));
        let engine = DelegatingEngine::new(Arc::new(inner)).with_storage_handler(metered);
        let _ = MeteredDeltaEngine::new(Arc::new(engine));
    }

    #[test]
    #[should_panic(expected = "json_handler is already a MeteredJsonHandler")]
    fn new_panics_when_inner_json_already_metered() {
        let inner = stub_engine();
        let metered = Arc::new(MeteredJsonHandler::new(inner.json_handler()));
        let engine = DelegatingEngine::new(Arc::new(inner)).with_json_handler(metered);
        let _ = MeteredDeltaEngine::new(Arc::new(engine));
    }

    #[test]
    #[should_panic(expected = "parquet_handler is already a MeteredParquetHandler")]
    fn new_panics_when_inner_parquet_already_metered() {
        let inner = stub_engine();
        let metered = Arc::new(MeteredParquetHandler::new(inner.parquet_handler()));
        let engine = DelegatingEngine::new(Arc::new(inner)).with_parquet_handler(metered);
        let _ = MeteredDeltaEngine::new(Arc::new(engine));
    }
}
