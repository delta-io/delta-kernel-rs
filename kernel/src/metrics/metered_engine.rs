//! [`MeteredDeltaEngine`] wraps any [`Engine`] so its `storage_handler` emits the
//! kernel's standard `"storage"` tracing spans. Other handler accessors pass through
//! unchanged.
//!
//! ```ignore
//! let inner: Arc<dyn Engine> = Arc::new(MyEngine::build()?);
//! let engine: Arc<dyn Engine> = Arc::new(MeteredDeltaEngine::new(inner));
//! ```

use std::sync::Arc;

use crate::metrics::MeteredStorageHandler;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// Decorator over any [`Engine`] that meters its storage handler. See module docs.
pub struct MeteredDeltaEngine {
    inner: Arc<dyn Engine>,
    storage: Arc<dyn StorageHandler>,
}

impl MeteredDeltaEngine {
    /// Wrap `inner`. Debug-asserts that `inner`'s storage handler is not already
    /// metered, so the resulting engine emits each storage span exactly once.
    pub fn new(inner: Arc<dyn Engine>) -> Self {
        let inner_storage = inner.storage_handler();
        debug_assert!(
            !inner_storage.any_ref().is::<MeteredStorageHandler>(),
            "MeteredDeltaEngine wraps an engine whose storage_handler is already a \
             MeteredStorageHandler; remove the outer wrap to avoid double-counting metrics",
        );
        let storage = Arc::new(MeteredStorageHandler::new(inner_storage));
        Self { inner, storage }
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
        self.inner.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.inner.parquet_handler()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use url::Url;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::MetricEvent;
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};
    use crate::{DeltaResult, FileMeta, FileSlice};

    // Use a stub engine so the wrapper sees an un-metered storage handler. `DefaultEngine`
    // already exposes `MeteredStorageHandler` from `storage_handler()`, which would trip
    // the double-wrap guard in `MeteredDeltaEngine::new`.

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
    }

    /// Engine that delegates everything to a [`SyncEngine`] except `storage_handler`, which
    /// returns a [`StubStorageHandler`] configured to yield two list entries.
    struct StubEngine {
        sync: Arc<SyncEngine>,
        storage: Arc<dyn StorageHandler>,
    }

    impl StubEngine {
        fn new() -> Self {
            Self {
                sync: Arc::new(SyncEngine::new()),
                storage: Arc::new(StubStorageHandler {
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
                }),
            }
        }
    }

    impl Engine for StubEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.sync.evaluation_handler()
        }
        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            Arc::clone(&self.storage)
        }
        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            self.sync.json_handler()
        }
        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            self.sync.parquet_handler()
        }
    }

    #[test]
    fn storage_handler_emits_metered_spans() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(Arc::clone(&reporter) as _);

        let engine = MeteredDeltaEngine::new(Arc::new(StubEngine::new()));
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
    fn other_handlers_pass_through() {
        let inner: Arc<dyn Engine> = Arc::new(StubEngine::new());
        let inner_eval = inner.evaluation_handler();
        let inner_json = inner.json_handler();
        let inner_parquet = inner.parquet_handler();

        let engine = MeteredDeltaEngine::new(inner);
        assert!(Arc::ptr_eq(&inner_eval, &engine.evaluation_handler()));
        assert!(Arc::ptr_eq(&inner_json, &engine.json_handler()));
        assert!(Arc::ptr_eq(&inner_parquet, &engine.parquet_handler()));
    }

    /// Engine that returns an already-metered storage handler; used to verify the
    /// debug-time double-wrap guard.
    struct AlreadyMeteredEngine(StubEngine);

    impl Engine for AlreadyMeteredEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.0.evaluation_handler()
        }
        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            Arc::new(crate::metrics::MeteredStorageHandler::new(
                self.0.storage_handler(),
            ))
        }
        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            self.0.json_handler()
        }
        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            self.0.parquet_handler()
        }
    }

    #[test]
    #[should_panic(expected = "storage_handler is already a MeteredStorageHandler")]
    fn new_panics_when_inner_already_metered() {
        let _ = MeteredDeltaEngine::new(Arc::new(AlreadyMeteredEngine(StubEngine::new())));
    }
}
