//! [`MeteredStorageHandler`] wraps any [`StorageHandler`] so it emits the kernel's
//! standard `"storage"` tracing spans. Usually reached via [`MeteredDeltaEngine`];
//! construct directly when wrapping a `StorageHandler` outside an `Engine`.
//!
//! [`MeteredDeltaEngine`]: crate::metrics::MeteredDeltaEngine

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use url::Url;

use crate::metrics::events::{StorageCopyCompleted, StorageListCompleted, StorageReadCompleted};
use crate::metrics::{emit_storage_span, MetricsIterator};
use crate::{DeltaResult, FileMeta, FileSlice, StorageHandler};

/// Decorator over an engine-provided `Arc<dyn StorageHandler>` that emits the kernel's
/// standard `"storage"` spans on operations that produce metrics. `put`, `head`, and `delete`
/// are pass-through and emit nothing.
pub struct MeteredStorageHandler {
    inner: Arc<dyn StorageHandler>,
}

impl MeteredStorageHandler {
    /// Wrap `inner`. Debug-asserts that `inner` is not already a
    /// [`MeteredStorageHandler`] so spans are emitted exactly once.
    pub fn new(inner: Arc<dyn StorageHandler>) -> Self {
        debug_assert!(
            !inner.any_ref().is::<MeteredStorageHandler>(),
            "MeteredStorageHandler wraps another MeteredStorageHandler; \
             remove the outer wrap to avoid double-counting metrics",
        );
        Self { inner }
    }
}

impl std::fmt::Debug for MeteredStorageHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredStorageHandler")
            .finish_non_exhaustive()
    }
}

impl StorageHandler for MeteredStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let start = Instant::now();
        let inner = self.inner.list_from(path)?;
        Ok(Box::new(MetricsIterator::<_, FileMeta>::new(
            inner,
            StorageListCompleted::NAME,
            start,
        )))
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let start = Instant::now();
        let inner = self.inner.read_files(files)?;
        Ok(Box::new(MetricsIterator::<_, Bytes>::new(
            inner,
            StorageReadCompleted::NAME,
            start,
        )))
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        let start = Instant::now();
        let result = self.inner.copy_atomic(src, dest);
        emit_storage_span(StorageCopyCompleted::NAME, start.elapsed(), 0, 0);
        result
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        self.inner.head(path)
    }

    fn delete(&self, path: &Url) -> DeltaResult<()> {
        self.inner.delete(path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::metrics::MetricEvent;
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

    /// Storage handler that returns N preconfigured FileMeta / byte slices.
    #[derive(Debug)]
    struct StubStorageHandler {
        list_results: Vec<FileMeta>,
        read_results: Vec<Bytes>,
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
            let results: Vec<_> = self.read_results.iter().cloned().map(Ok).collect();
            Ok(Box::new(results.into_iter()))
        }

        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
            Ok(())
        }

        fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> DeltaResult<()> {
            Ok(())
        }

        fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
            unreachable!("not exercised in these tests")
        }

        fn delete(&self, _path: &Url) -> DeltaResult<()> {
            Ok(())
        }
    }

    fn fake_url() -> Url {
        Url::parse("memory:///_delta_log/").unwrap()
    }

    fn fake_file_meta(name: &str) -> FileMeta {
        FileMeta {
            location: Url::parse(&format!("memory:///_delta_log/{name}")).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    fn install_capture() -> (Arc<CapturingReporter>, tracing::subscriber::DefaultGuard) {
        let reporter = Arc::new(CapturingReporter::default());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        (reporter, guard)
    }

    #[test]
    fn list_from_emits_storage_list_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn StorageHandler> = Arc::new(StubStorageHandler {
            list_results: vec![
                fake_file_meta("00000000000000000000.json"),
                fake_file_meta("00000000000000000001.json"),
            ],
            read_results: vec![],
        });
        let storage = MeteredStorageHandler::new(inner);

        let iter = storage.list_from(&fake_url()).unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let listed = events
            .iter()
            .find(|e| matches!(e, MetricEvent::StorageListCompleted(_)))
            .expect("expected StorageListCompleted event");
        let MetricEvent::StorageListCompleted(e) = listed else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
    }

    #[test]
    fn read_files_emits_storage_read_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn StorageHandler> = Arc::new(StubStorageHandler {
            list_results: vec![],
            read_results: vec![Bytes::from(vec![0u8; 32]), Bytes::from(vec![0u8; 8])],
        });
        let storage = MeteredStorageHandler::new(inner);

        let iter = storage.read_files(vec![]).unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::StorageReadCompleted(_)))
            .expect("expected StorageReadCompleted event");
        let MetricEvent::StorageReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
        assert_eq!(e.bytes_read, 40);
    }

    #[test]
    fn copy_atomic_emits_storage_copy_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn StorageHandler> = Arc::new(StubStorageHandler {
            list_results: vec![],
            read_results: vec![],
        });
        let storage = MeteredStorageHandler::new(inner);

        storage.copy_atomic(&fake_url(), &fake_url()).unwrap();

        let events = reporter.events();
        assert!(events
            .iter()
            .any(|e| matches!(e, MetricEvent::StorageCopyCompleted(_))));
    }

    #[test]
    #[should_panic(expected = "wraps another MeteredStorageHandler")]
    fn new_panics_on_double_wrap() {
        let inner: Arc<dyn StorageHandler> = Arc::new(StubStorageHandler {
            list_results: vec![],
            read_results: vec![],
        });
        let once: Arc<dyn StorageHandler> = Arc::new(MeteredStorageHandler::new(inner));
        let _twice = MeteredStorageHandler::new(once);
    }
}
