//! [`MeteredJsonHandler`] wraps any [`JsonHandler`] so its `read_json_files` emits the
//! kernel's standard `JsonReadCompleted` span, carrying `(num_files, bytes_read)` exactly
//! once when the returned iterator is exhausted or dropped. `parse_json` and
//! `write_json_file` pass through.

use std::sync::Arc;

use crate::metrics::events::emit_json_read_completed;
use crate::metrics::PrecountedMetricsIterator;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, DeltaResultIterator, EngineData, FileDataReadResultIterator, FileMeta,
    FilteredEngineData, JsonHandler, PredicateRef,
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

impl JsonHandler for MeteredJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let num_files = files.len() as u64;
        let bytes_read = files.iter().map(|f| f.size).sum();
        let inner = self
            .inner
            .read_json_files(files, physical_schema, predicate)?;
        Ok(Box::new(PrecountedMetricsIterator::new(
            inner,
            num_files,
            bytes_read,
            emit_json_read_completed,
        )))
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use url::Url;

    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::arrow::datatypes::Schema;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::metrics::MetricEvent;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

    #[derive(Debug)]
    struct StubJsonHandler;

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
        ) -> DeltaResult<Box<dyn EngineData>> {
            Ok(empty_batch())
        }

        fn read_json_files(
            &self,
            _files: &[FileMeta],
            _physical_schema: SchemaRef,
            _predicate: Option<PredicateRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            Ok(Box::new(std::iter::empty()))
        }

        fn write_json_file(
            &self,
            _path: &Url,
            _data: DeltaResultIterator<'_, FilteredEngineData>,
            _overwrite: bool,
        ) -> DeltaResult<()> {
            Ok(())
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
    fn read_json_files_emits_json_read_completed() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler);
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
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler);
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

    #[test]
    fn read_json_files_emits_zero_event_for_empty_input() {
        let (reporter, _guard) = install_capture();
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler);
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
    #[should_panic(expected = "wraps another MeteredJsonHandler")]
    fn new_panics_on_double_wrap() {
        let inner: Arc<dyn JsonHandler> = Arc::new(StubJsonHandler);
        let once: Arc<dyn JsonHandler> = Arc::new(MeteredJsonHandler::new(inner));
        let _twice = MeteredJsonHandler::new(once);
    }
}
