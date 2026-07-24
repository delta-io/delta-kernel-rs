//! [`MeteredParquetHandler`] wraps any [`ParquetHandler`] so its `read_parquet_files`
//! emits the kernel's standard `ParquetReadCompleted` span, carrying
//! `(num_files, bytes_read)` exactly once when the returned iterator is exhausted or
//! dropped. `read_parquet_footer` and `write_parquet_file` pass through.

use std::sync::Arc;

use crate::metrics::events::emit_parquet_read_completed;
use crate::metrics::PrecountedMetricsIterator;
use crate::schema::SchemaRef;
use crate::{
    CancellationTokenRef, DeltaResult, DeltaResultIteratorStatic, EngineData,
    FileDataReadResultIterator, FileMeta, ParquetFooter, ParquetHandler, PredicateRef,
};

/// Decorator over an engine-provided `Arc<dyn ParquetHandler>` that emits a
/// `ParquetReadCompleted` span on every `read_parquet_files` call.
/// `read_parquet_footer` and `write_parquet_file` are pass-through and emit nothing.
pub struct MeteredParquetHandler {
    inner: Arc<dyn ParquetHandler>,
}

impl MeteredParquetHandler {
    /// Wrap `inner`. Debug-asserts that `inner` is not already a [`MeteredParquetHandler`]
    /// so spans are emitted exactly once.
    pub fn new(inner: Arc<dyn ParquetHandler>) -> Self {
        debug_assert!(
            !inner.any_ref().is::<MeteredParquetHandler>(),
            "MeteredParquetHandler wraps another MeteredParquetHandler; \
             remove the outer wrap to avoid double-counting metrics",
        );
        Self { inner }
    }
}

impl std::fmt::Debug for MeteredParquetHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredParquetHandler")
            .finish_non_exhaustive()
    }
}

impl MeteredParquetHandler {
    /// Wraps `inner` so it emits a `ParquetReadCompleted` span carrying `num_files` and the summed
    /// `bytes_read` of `files` when the iterator is exhausted or dropped.
    fn meter(files: &[FileMeta], inner: FileDataReadResultIterator) -> FileDataReadResultIterator {
        let num_files = files.len() as u64;
        let bytes_read = files.iter().map(|f| f.size).sum();
        Box::new(PrecountedMetricsIterator::new(
            inner,
            num_files,
            bytes_read,
            emit_parquet_read_completed,
        ))
    }
}

impl ParquetHandler for MeteredParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let inner = self
            .inner
            .read_parquet_files(files, physical_schema, predicate)?;
        Ok(Self::meter(files, inner))
    }

    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let inner = self.inner.read_parquet_files_with_cancellation(
            files,
            physical_schema,
            predicate,
            cancellation_token,
        )?;
        Ok(Self::meter(files, inner))
    }

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }

    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<ParquetFooter> {
        self.inner
            .read_parquet_footer_with_cancellation(file, cancellation_token)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use url::Url;

    use super::*;
    use crate::metrics::MetricEvent;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

    #[derive(Debug, Default)]
    struct StubParquetHandler {
        /// Set when the cancellation-aware read variant is invoked, so a test can assert the
        /// metered wrapper forwards to it (rather than the plain read).
        cancellation_read_called: std::sync::atomic::AtomicBool,
    }

    impl ParquetHandler for StubParquetHandler {
        fn read_parquet_files(
            &self,
            _files: &[FileMeta],
            _physical_schema: SchemaRef,
            _predicate: Option<PredicateRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            Ok(Box::new(std::iter::empty()))
        }

        fn read_parquet_files_with_cancellation(
            &self,
            _files: &[FileMeta],
            _physical_schema: SchemaRef,
            _predicate: Option<PredicateRef>,
            _cancellation_token: Option<CancellationTokenRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            self.cancellation_read_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::new(std::iter::empty()))
        }

        fn write_parquet_file(
            &self,
            _location: Url,
            _data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
        ) -> DeltaResult<()> {
            Ok(())
        }

        fn read_parquet_footer(&self, _file: &FileMeta) -> DeltaResult<ParquetFooter> {
            unreachable!("not exercised in these tests")
        }
    }

    fn fake_file(name: &str, size: u64) -> FileMeta {
        FileMeta {
            location: Url::parse(&format!("memory:///_delta_log/{name}")).unwrap(),
            last_modified: 0,
            size,
        }
    }

    fn install_capture() -> (Arc<CapturingReporter>, tracing::subscriber::DefaultGuard) {
        let reporter = Arc::new(CapturingReporter::default());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        (reporter, guard)
    }

    fn delta_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::nullable("x", DataType::INTEGER)]).unwrap())
    }

    #[test]
    fn read_parquet_files_emits_parquet_read_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn ParquetHandler> = Arc::new(StubParquetHandler::default());
        let handler = MeteredParquetHandler::new(inner);

        let files = vec![fake_file("a.parquet", 256), fake_file("b.parquet", 1024)];
        let iter = handler
            .read_parquet_files(&files, delta_schema(), None)
            .unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::ParquetReadCompleted(_)))
            .expect("expected ParquetReadCompleted event");
        let MetricEvent::ParquetReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
        assert_eq!(e.bytes_read, 1280);
    }

    #[test]
    fn read_parquet_files_emits_on_drop_without_consumption() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn ParquetHandler> = Arc::new(StubParquetHandler::default());
        let handler = MeteredParquetHandler::new(inner);

        let files = vec![fake_file("a.parquet", 256), fake_file("b.parquet", 1024)];
        {
            let _iter = handler
                .read_parquet_files(&files, delta_schema(), None)
                .unwrap();
        }

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::ParquetReadCompleted(_)))
            .expect("expected ParquetReadCompleted event on drop");
        let MetricEvent::ParquetReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
        assert_eq!(e.bytes_read, 1280);
    }

    // The metered wrapper's cancellation-aware read must forward to the inner handler's
    // cancellation-aware read (not the plain one) AND still emit the metrics span.
    #[test]
    fn metered_wrapper_forwards_cancellation_read_and_emits_metrics() {
        let (reporter, _guard) = install_capture();
        let stub = Arc::new(StubParquetHandler::default());
        let handler = MeteredParquetHandler::new(stub.clone() as Arc<dyn ParquetHandler>);

        let files = vec![fake_file("a.parquet", 256), fake_file("b.parquet", 1024)];
        let iter = handler
            .read_parquet_files_with_cancellation(&files, delta_schema(), None, None)
            .unwrap();
        let _: Vec<_> = iter.collect();

        assert!(
            stub.cancellation_read_called
                .load(std::sync::atomic::Ordering::SeqCst),
            "metered wrapper must forward to the inner cancellation-aware read"
        );
        let events = reporter.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MetricEvent::ParquetReadCompleted(_))),
            "cancellation-aware read must still emit ParquetReadCompleted"
        );
    }

    #[test]
    fn read_parquet_files_emits_zero_event_for_empty_input() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn ParquetHandler> = Arc::new(StubParquetHandler::default());
        let handler = MeteredParquetHandler::new(inner);

        let iter = handler
            .read_parquet_files(&[], delta_schema(), None)
            .unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::ParquetReadCompleted(_)))
            .expect("expected zero-valued ParquetReadCompleted");
        let MetricEvent::ParquetReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 0);
        assert_eq!(e.bytes_read, 0);
    }

    #[test]
    #[should_panic(expected = "wraps another MeteredParquetHandler")]
    fn new_panics_on_double_wrap() {
        let inner: Arc<dyn ParquetHandler> = Arc::new(StubParquetHandler::default());
        let once: Arc<dyn ParquetHandler> = Arc::new(MeteredParquetHandler::new(inner));
        let _twice = MeteredParquetHandler::new(once);
    }
}
