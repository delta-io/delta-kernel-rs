//! [`MeteredJsonHandler`] wraps any [`JsonHandler`] so its `read_json_files` emits the
//! kernel's standard `JsonReadCompleted` span, carrying `(num_files, bytes_read)` exactly
//! once when the returned iterator is exhausted or dropped. `parse_json` and
//! `write_json_file` pass through.
//!
//! DEBUG events named `json_read_finished` identify each requested path and distinguish successful
//! exhaustion (`success`), read errors (`failure`), and early iterator drops (`dropped`).

use std::sync::Arc;

use tracing::{Dispatch, Level};
use url::Url;

use crate::metrics::PrecountedMetricsIterator;
use crate::metrics::events::emit_json_read_completed;
use crate::schema::SchemaRef;
use crate::{
    CancellationTokenRef, EngineData, FileDataReadResultIterator, FileMeta, FileSize,
    FilteredEngineData, JsonHandler, KernelResult, KernelResultIterator, PredicateRef,
};

/// Decorator over an engine-provided `Arc<dyn JsonHandler>` that emits a
/// `JsonReadCompleted` span on every `read_json_files` call. `parse_json` and
/// `write_json_file` are pass-through and emit nothing.
pub struct MeteredJsonHandler {
    inner: Arc<dyn JsonHandler>,
}

impl MeteredJsonHandler {
    /// Wrap `inner`. Debug-asserts that `inner` is not already a [`MeteredJsonHandler`]
    /// so spans are emitted exactly once.
    pub fn new(inner: Arc<dyn JsonHandler>) -> Self {
        debug_assert!(
            !inner.any_ref().is::<MeteredJsonHandler>(),
            "MeteredJsonHandler wraps another MeteredJsonHandler; \
             remove the outer wrap to avoid double-counting metrics",
        );
        Self { inner }
    }
}

impl std::fmt::Debug for MeteredJsonHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredJsonHandler").finish_non_exhaustive()
    }
}

impl MeteredJsonHandler {
    /// Wraps `inner` so it emits a `JsonReadCompleted` span carrying `num_files` and the summed
    /// `bytes_read` of `files` when the iterator is exhausted or dropped.
    fn meter(
        files: &[FileMeta],
        result: KernelResult<FileDataReadResultIterator>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let mut trace = JsonReadTrace::new(files);
        let inner = result.inspect_err(|_| trace.finish("failure"))?;
        let num_files = files.len() as u64;
        let bytes_read = files.iter().map(|f| f.size).sum();
        let inner =
            PrecountedMetricsIterator::new(inner, num_files, bytes_read, emit_json_read_completed);
        Ok(Box::new(TracedJsonRead { inner, trace }))
    }
}

impl JsonHandler for MeteredJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let result = self
            .inner
            .read_json_files(files, physical_schema, predicate);
        Self::meter(files, result)
    }

    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let result = self.inner.read_json_files_with_cancellation(
            files,
            physical_schema,
            predicate,
            cancellation_token,
        );
        Self::meter(files, result)
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

struct JsonReadTrace {
    paths: Vec<Url>,
    dispatch: Dispatch,
}

impl JsonReadTrace {
    fn new(files: &[FileMeta]) -> Self {
        let paths = if tracing::level_enabled!(Level::DEBUG) {
            files.iter().map(|file| file.location.clone()).collect()
        } else {
            Vec::new()
        };
        Self {
            paths,
            dispatch: tracing::dispatcher::get_default(Clone::clone),
        }
    }

    fn finish(&mut self, outcome: &'static str) {
        // Keep the initiating subscriber when a different thread consumes or drops the iterator.
        tracing::dispatcher::with_default(&self.dispatch, || {
            for path in self.paths.drain(..) {
                tracing::debug!(name: "json_read_finished", path = path.as_str(), outcome);
            }
        });
    }
}

impl Drop for JsonReadTrace {
    fn drop(&mut self) {
        self.finish("dropped");
    }
}

struct TracedJsonRead<I> {
    inner: I,
    trace: JsonReadTrace,
}

impl<I: Iterator<Item = KernelResult<Box<dyn EngineData>>>> Iterator for TracedJsonRead<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        match &item {
            None => self.trace.finish("success"),
            Some(Err(_)) => self.trace.finish("failure"),
            Some(Ok(_)) => {}
        }
        item
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::JsonReadCapture;
    use url::Url;

    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::arrow::datatypes::Schema;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::metrics::MetricEvent;
    use crate::schema::schema_ref;
    use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};

    #[derive(Debug, Default)]
    struct StubJsonHandler {
        /// Set when the cancellation-aware read variant is invoked, so a test can assert the
        /// metered wrapper forwards to it (rather than the plain read).
        cancellation_read_called: std::sync::atomic::AtomicBool,
        write_size: Option<FileSize>,
        read_error: bool,
        batch_error: bool,
    }

    fn empty_batch() -> Box<dyn EngineData> {
        Box::new(ArrowEngineData::new(RecordBatch::new_empty(Arc::new(
            Schema::empty(),
        ))))
    }

    impl JsonHandler for StubJsonHandler {
        fn parse_json(
            &self,
            _json_strings: Box<dyn EngineData>,
            _output_schema: SchemaRef,
        ) -> KernelResult<Box<dyn EngineData>> {
            Ok(empty_batch())
        }

        fn read_json_files(
            &self,
            _files: &[FileMeta],
            _physical_schema: SchemaRef,
            _predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator> {
            if self.read_error {
                return Err(crate::KernelError::generic("read failed"));
            }
            if self.batch_error {
                return Ok(Box::new(std::iter::once(Err(crate::KernelError::generic(
                    "batch failed",
                )))));
            }
            Ok(Box::new(std::iter::empty()))
        }

        fn read_json_files_with_cancellation(
            &self,
            _files: &[FileMeta],
            _physical_schema: SchemaRef,
            _predicate: Option<PredicateRef>,
            _cancellation_token: Option<CancellationTokenRef>,
        ) -> KernelResult<FileDataReadResultIterator> {
            self.cancellation_read_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.read_json_files(_files, _physical_schema, _predicate)
        }

        fn write_json_file(
            &self,
            _path: &Url,
            _data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
            _overwrite: bool,
        ) -> KernelResult<FileSize> {
            self.write_size.ok_or_else(|| {
                crate::KernelError::generic("StubJsonHandler does not support writes")
            })
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
        schema_ref! { nullable "x": INTEGER }
    }

    #[rstest]
    #[case::success(false, false, false)]
    #[case::construction_error(true, false, false)]
    #[case::batch_error(false, true, false)]
    #[case::dropped(false, false, true)]
    fn json_read_trace_reports_one_terminal_outcome(
        #[case] read_error: bool,
        #[case] batch_error: bool,
        #[case] drop_early: bool,
        #[values(false, true)] cancellation: bool,
        #[values(false, true)] other_thread: bool,
    ) {
        let capture = JsonReadCapture::new();
        let handler = MeteredJsonHandler::new(Arc::new(StubJsonHandler {
            read_error,
            batch_error,
            ..Default::default()
        }));
        let files = vec![fake_file("0.json", 100), fake_file("1.json", 50)];
        let result = if cancellation {
            handler.read_json_files_with_cancellation(&files, delta_schema(), None, None)
        } else {
            handler.read_json_files(&files, delta_schema(), None)
        };
        if read_error {
            assert!(result.is_err());
        } else {
            let iterator = result.unwrap();
            let consume = move || {
                if drop_early {
                    drop(iterator);
                } else {
                    let batches = iterator.collect::<Vec<_>>();
                    assert_eq!(batches.iter().any(Result::is_err), batch_error);
                }
            };
            if other_thread {
                std::thread::spawn(consume).join().unwrap();
            } else {
                consume();
            }
        }
        let paths = files
            .iter()
            .map(|file| file.location.clone())
            .collect::<Vec<_>>();
        assert_eq!(capture.requested_paths(), paths);
        assert_eq!(
            capture.successful_paths().len(),
            if !read_error && !batch_error && !drop_early {
                2
            } else {
                0
            }
        );
        assert_eq!(
            capture.failed_paths().len(),
            if read_error || batch_error { 2 } else { 0 }
        );
        assert_eq!(
            capture.dropped_paths().len(),
            if drop_early { 2 } else { 0 }
        );
    }

    #[test]
    fn read_json_files_emits_json_read_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler::default());
        let handler = MeteredJsonHandler::new(inner);

        let files = vec![fake_file("0.json", 100), fake_file("1.json", 50)];
        let iter = handler
            .read_json_files(&files, delta_schema(), None)
            .unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::JsonReadCompleted(_)))
            .expect("expected JsonReadCompleted event");
        let MetricEvent::JsonReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
        assert_eq!(e.bytes_read, 150);
    }

    #[test]
    fn read_json_files_emits_on_drop_without_consumption() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler::default());
        let handler = MeteredJsonHandler::new(inner);

        let files = vec![fake_file("0.json", 100), fake_file("1.json", 50)];
        {
            let _iter = handler
                .read_json_files(&files, delta_schema(), None)
                .unwrap();
        }

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::JsonReadCompleted(_)))
            .expect("expected JsonReadCompleted event on drop");
        let MetricEvent::JsonReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 2);
        assert_eq!(e.bytes_read, 150);
    }

    // The metered wrapper's cancellation-aware read must forward to the inner handler's
    // cancellation-aware read (not the plain one) AND still emit the metrics span.
    #[test]
    fn metered_wrapper_forwards_cancellation_read_and_emits_metrics() {
        let (reporter, _guard) = install_capture();
        let stub = Arc::new(StubJsonHandler::default());
        let handler = MeteredJsonHandler::new(stub.clone() as Arc<dyn JsonHandler>);

        let files = vec![fake_file("0.json", 100), fake_file("1.json", 50)];
        let iter = handler
            .read_json_files_with_cancellation(&files, delta_schema(), None, None)
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
                .any(|e| matches!(e, MetricEvent::JsonReadCompleted(_))),
            "cancellation-aware read must still emit JsonReadCompleted"
        );
    }

    #[test]
    fn read_json_files_emits_zero_event_for_empty_input() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler::default());
        let handler = MeteredJsonHandler::new(inner);

        let iter = handler.read_json_files(&[], delta_schema(), None).unwrap();
        let _: Vec<_> = iter.collect();

        let events = reporter.events();
        let read = events
            .iter()
            .find(|e| matches!(e, MetricEvent::JsonReadCompleted(_)))
            .expect("expected zero-valued JsonReadCompleted");
        let MetricEvent::JsonReadCompleted(e) = read else {
            unreachable!();
        };
        assert_eq!(e.num_files, 0);
        assert_eq!(e.bytes_read, 0);
    }

    #[test]
    fn write_json_file_forwards_size_without_emitting_metrics() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler {
            write_size: Some(42),
            ..Default::default()
        });
        let handler = MeteredJsonHandler::new(inner);
        let path = Url::parse("memory:///_delta_log/0.json").unwrap();

        let written_size = handler
            .write_json_file(&path, Box::new(std::iter::empty()), false)
            .unwrap();

        assert_eq!(written_size, 42);
        assert!(reporter.events().is_empty());
    }

    #[test]
    fn write_json_file_forwards_errors() {
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler::default());
        let handler = MeteredJsonHandler::new(inner);
        let path = Url::parse("memory:///_delta_log/0.json").unwrap();

        let error = handler
            .write_json_file(&path, Box::new(std::iter::empty()), false)
            .unwrap_err();

        assert!(error.to_string().contains("does not support writes"));
    }

    #[test]
    #[should_panic(expected = "wraps another MeteredJsonHandler")]
    fn new_panics_on_double_wrap() {
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler::default());
        let once: Arc<dyn JsonHandler> = Arc::new(MeteredJsonHandler::new(inner));
        let _twice = MeteredJsonHandler::new(once);
    }
}
