//! Metrics reporter trait and implementations.

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{event, warn, Level, Span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;
use uuid::Uuid;

use crate::metrics::MetricId;

use super::MetricEvent;

/// Trait for reporting metrics events from Delta operations.
///
/// Implementations of this trait receive metric events as they occur during operations
/// and can forward them to monitoring systems like Prometheus, DataDog, etc.
///
/// Events are emitted throughout an operation's lifecycle, allowing real-time monitoring.
pub trait MetricsReporter: Send + Sync + std::fmt::Debug {
    /// Report a metric event.
    fn report(&self, event: MetricEvent);
}

/// A [`MetricsReporter`] that logs each event as a tracing event at the configured level.
#[derive(Debug)]
pub struct LoggingMetricsReporter {
    level: Level,
}

impl LoggingMetricsReporter {
    /// Create a new reporter that logs each [`MetricEvent`] at the given tracing level.
    pub fn new(level: Level) -> Self {
        LoggingMetricsReporter { level }
    }
}

impl MetricsReporter for LoggingMetricsReporter {
    fn report(&self, event: MetricEvent) {
        // event! wants a constant, so we have to do this silliness we also set the parent span to
        // none so this just logs the report and not a bunch of context from the span that generated
        // the report
        match self.level {
            Level::ERROR => event!(parent: Span::none(), Level::ERROR, "{}", event),
            Level::WARN => event!(parent: Span::none(), Level::WARN, "{}", event),
            Level::INFO => event!(parent: Span::none(), Level::INFO, "{}", event),
            Level::DEBUG => event!(parent: Span::none(), Level::DEBUG, "{}", event),
            Level::TRACE => event!(parent: Span::none(), Level::TRACE, "{}", event),
        }
    }
}

/// A [`tracing_subscriber::Layer`] that converts tracing spans into [`MetricEvent`]s and
/// forwards them to a registered [`MetricsReporter`].
///
/// Typically added to a subscriber via [`WithMetricsReporterLayer::with_metrics_reporter_layer`]
/// rather than constructed directly.
#[derive(Debug)]
pub struct ReportGeneratorLayer {
    reporter: Arc<dyn MetricsReporter>,
}

impl ReportGeneratorLayer {
    /// Create a new layer that forwards metric events to the given reporter.
    pub fn new(reporter: Arc<dyn MetricsReporter>) -> Self {
        ReportGeneratorLayer { reporter }
    }
}

struct EventVisitor {
    event: Option<MetricEvent>,
}

impl EventVisitor {
    fn new(event: Option<MetricEvent>) -> Self {
        Self { event }
    }

    fn set_duration(&mut self, target_duration: std::time::Duration) {
        match &mut self.event {
            Some(MetricEvent::LogSegmentLoaded { duration, .. }) => *duration = target_duration,
            Some(MetricEvent::ProtocolMetadataLoaded { duration, .. }) => {
                *duration = target_duration
            }
            Some(MetricEvent::SnapshotCompleted { total_duration, .. }) => {
                *total_duration = target_duration
            }
            Some(MetricEvent::SnapshotFailed { duration, .. }) => *duration = target_duration,
            _ => {}
        }
    }
}

impl Visit for EventVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if let Some(MetricEvent::LogSegmentLoaded {
            ref mut num_commit_files,
            ref mut num_checkpoint_files,
            ref mut num_compaction_files,
            ..
        }) = self.event
        {
            match field.name() {
                "num_commit_files" => *num_commit_files = value,
                "num_checkpoint_files" => *num_checkpoint_files = value,
                "num_compaction_files" => *num_compaction_files = value,
                _ => warn!("Invalid field recorded on {SEGMENT_FOR_SNAPSHOT_SPAN} span"),
            }
        }

        if let Some(MetricEvent::SnapshotCompleted {
            ref mut version, ..
        }) = self.event
        {
            match field.name() {
                "version" => *version = value,
                _ => warn!("Invalid field recorded on {SNAP_BUILD_SPAN} span"),
            }
        }
    }

    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        match field.name() {
            "return" => {} // we default to the success case
            "error" => {
                if let Some(MetricEvent::SnapshotCompleted { operation_id, .. }) = self.event {
                    self.event = Some(MetricEvent::SnapshotFailed {
                        operation_id,
                        duration: std::time::Duration::default(),
                    });
                }
            }
            _ => {}
        }
    }
}

enum StorageEventType {
    None,
    Copy,
    List,
    Read,
}

struct StorageEventTypeVisitor {
    typ: StorageEventType,
    num_files: u64,
    bytes_read: u64,
    duration: u64,
}

pub(crate) const COPY_COMPLETED_NAME: &str = "copy_completed";
pub(crate) const LIST_COMPLETED_NAME: &str = "list_completed";
pub(crate) const READ_COMPLETED_NAME: &str = "read_completed";

// Span names for metric-bearing spans. Each constant is used both at the span creation site
// and in the `on_new_span` match below. `#[instrument(name = "...")]` requires a string literal,
// so those sites carry a comment referencing the constant instead; `tracing::span!()` sites use
// the constant directly.
pub(crate) const SEGMENT_FOR_SNAPSHOT_SPAN: &str = "segment.for_snapshot";
pub(crate) const SEGMENT_READ_METADATA_SPAN: &str = "segment.read_metadata";
pub(crate) const SNAP_BUILD_SPAN: &str = "snap.build";
pub(crate) const STORAGE_SPAN: &str = "storage";
const JSON_READ_COMPLETED_SPAN: &str = "json_read_completed";
const PARQUET_READ_COMPLETED_SPAN: &str = "parquet_read_completed";
const SCAN_METADATA_COMPLETED_SPAN: &str = "scan.metadata_completed";

/// Emit a `JsonReadCompleted` metric event via a tracing span.
///
/// Creates and immediately drops a span whose `on_close` hook will fire
/// [`MetricEvent::JsonReadCompleted`] to any registered [`MetricsReporter`].
///
/// Call this once per [`crate::JsonHandler::read_json_files`] invocation, after the file list
/// is known but before the iterator is consumed (i.e. at iterator exhaustion or drop).
pub fn emit_json_read_completed(num_files: u64, bytes_read: u64) {
    // Span name must match JSON_READ_COMPLETED_SPAN used in ReportGeneratorLayer::on_new_span.
    let _span = tracing::span!(
        tracing::Level::INFO,
        "json_read_completed",
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a `ParquetReadCompleted` metric event via a tracing span.
///
/// Creates and immediately drops a span whose `on_close` hook will fire
/// [`MetricEvent::ParquetReadCompleted`] to any registered [`MetricsReporter`].
///
/// Call this once per [`crate::ParquetHandler::read_parquet_files`] invocation, after the file
/// list is known but before the iterator is consumed (i.e. at iterator exhaustion or drop).
pub fn emit_parquet_read_completed(num_files: u64, bytes_read: u64) {
    // Span name must match PARQUET_READ_COMPLETED_SPAN used in ReportGeneratorLayer::on_new_span.
    let _span = tracing::span!(
        tracing::Level::INFO,
        "parquet_read_completed",
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a `ScanMetadataCompleted` metric event via a tracing span.
///
/// Creates and immediately drops a span whose `on_close` hook will fire
/// [`MetricEvent::ScanMetadataCompleted`] to any registered [`MetricsReporter`].
///
/// The `event` must be a [`MetricEvent::ScanMetadataCompleted`] variant; other variants are
/// ignored. Call this when the scan metadata iterator is exhausted or dropped.
pub(crate) fn emit_scan_metadata_completed(event: &MetricEvent) {
    // Span name must match SCAN_METADATA_COMPLETED_SPAN used in ReportGeneratorLayer::on_new_span.
    let MetricEvent::ScanMetadataCompleted {
        operation_id,
        scan_type,
        total_duration,
        num_add_files_seen,
        num_active_add_files,
        num_remove_files_seen,
        num_non_file_actions,
        num_predicate_filtered,
        peak_hash_set_size,
        dedup_visitor_time_ms,
        predicate_eval_time_ms,
    } = event
    else {
        return;
    };
    let _span = tracing::span!(
        tracing::Level::INFO,
        "scan.metadata_completed",
        report = tracing::field::Empty,
        operation_id = %operation_id,
        scan_type = %scan_type,
        total_duration_ns = total_duration.as_nanos() as u64,
        num_add_files_seen = *num_add_files_seen,
        num_active_add_files = *num_active_add_files,
        num_remove_files_seen = *num_remove_files_seen,
        num_non_file_actions = *num_non_file_actions,
        num_predicate_filtered = *num_predicate_filtered,
        peak_hash_set_size = *peak_hash_set_size as u64,
        dedup_visitor_time_ms = *dedup_visitor_time_ms,
        predicate_eval_time_ms = *predicate_eval_time_ms,
    );
}

#[derive(Default)]
struct ScanMetadataVisitor {
    operation_id: Uuid,
    scan_type_str: String,
    total_duration_ns: u64,
    num_add_files_seen: u64,
    num_active_add_files: u64,
    num_remove_files_seen: u64,
    num_non_file_actions: u64,
    num_predicate_filtered: u64,
    peak_hash_set_size: u64,
    dedup_visitor_time_ms: u64,
    predicate_eval_time_ms: u64,
}

impl ScanMetadataVisitor {
    fn into_event(self) -> MetricEvent {
        use crate::metrics::ScanType;
        let scan_type = match self.scan_type_str.as_str() {
            "sequential" => ScanType::SequentialPhase,
            "parallel" => ScanType::ParallelPhase,
            _ => ScanType::Full,
        };
        MetricEvent::ScanMetadataCompleted {
            operation_id: MetricId(self.operation_id),
            scan_type,
            total_duration: std::time::Duration::from_nanos(self.total_duration_ns),
            num_add_files_seen: self.num_add_files_seen,
            num_active_add_files: self.num_active_add_files,
            num_remove_files_seen: self.num_remove_files_seen,
            num_non_file_actions: self.num_non_file_actions,
            num_predicate_filtered: self.num_predicate_filtered,
            peak_hash_set_size: self.peak_hash_set_size as usize,
            dedup_visitor_time_ms: self.dedup_visitor_time_ms,
            predicate_eval_time_ms: self.predicate_eval_time_ms,
        }
    }
}

impl Visit for ScanMetadataVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "total_duration_ns" => self.total_duration_ns = value,
            "num_add_files_seen" => self.num_add_files_seen = value,
            "num_active_add_files" => self.num_active_add_files = value,
            "num_remove_files_seen" => self.num_remove_files_seen = value,
            "num_non_file_actions" => self.num_non_file_actions = value,
            "num_predicate_filtered" => self.num_predicate_filtered = value,
            "peak_hash_set_size" => self.peak_hash_set_size = value,
            "dedup_visitor_time_ms" => self.dedup_visitor_time_ms = value,
            "predicate_eval_time_ms" => self.predicate_eval_time_ms = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let s = format!("{:?}", value);
        match field.name() {
            "operation_id" => match Uuid::from_str(&s) {
                Ok(u) => self.operation_id = u,
                Err(e) => warn!("Invalid uuid recorded to scan.metadata_completed span: {s}. {e}"),
            },
            "scan_type" => self.scan_type_str = s,
            _ => {}
        }
    }
}

impl Visit for StorageEventTypeVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "name" {
            match value {
                COPY_COMPLETED_NAME => self.typ = StorageEventType::Copy,
                LIST_COMPLETED_NAME => self.typ = StorageEventType::List,
                READ_COMPLETED_NAME => self.typ = StorageEventType::Read,
                _ => warn!("Storage with unknown type: {value}"),
            }
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_files" => self.num_files = value,
            "bytes_read" => self.bytes_read = value,
            "duration" => self.duration = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

/// Visitor for extracting `num_files` and `bytes_read` from file-read spans.
#[derive(Default)]
struct FileReadVisitor {
    num_files: u64,
    bytes_read: u64,
}

impl Visit for FileReadVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_files" => self.num_files = value,
            "bytes_read" => self.bytes_read = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

#[derive(Default)]
struct NewSpanVisitor {
    uuid: Uuid,
}

impl Visit for NewSpanVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "operation_id" {
            let s = format!("{:?}", value);
            match Uuid::from_str(&s) {
                Ok(u) => self.uuid = u,
                Err(e) => {
                    warn!("Invalid uuid recorded to span: {value:?}. {e}. Using a default")
                }
            }
        }
    }
}

impl<S> Layer<S> for ReportGeneratorLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(metadata) = ctx.metadata(id) else {
            return;
        };
        let mut new_span_visitor = NewSpanVisitor::default();
        attrs.record(&mut new_span_visitor);
        let name = metadata.name();
        let event = match name {
            SEGMENT_FOR_SNAPSHOT_SPAN => Some(MetricEvent::LogSegmentLoaded {
                operation_id: MetricId(new_span_visitor.uuid),
                duration: std::time::Duration::default(),
                num_commit_files: 0,
                num_checkpoint_files: 0,
                num_compaction_files: 0,
            }),
            SEGMENT_READ_METADATA_SPAN => Some(MetricEvent::ProtocolMetadataLoaded {
                operation_id: MetricId(new_span_visitor.uuid),
                duration: std::time::Duration::default(),
            }),
            SNAP_BUILD_SPAN => Some(MetricEvent::SnapshotCompleted {
                operation_id: MetricId(new_span_visitor.uuid),
                version: 0,
                total_duration: std::time::Duration::default(),
            }),
            STORAGE_SPAN => {
                let mut storage_visitor = StorageEventTypeVisitor {
                    typ: StorageEventType::None,
                    num_files: 0,
                    bytes_read: 0,
                    duration: 0,
                };
                attrs.record(&mut storage_visitor);
                match storage_visitor.typ {
                    StorageEventType::None => None,
                    StorageEventType::Copy => Some(MetricEvent::StorageCopyCompleted {
                        duration: std::time::Duration::from_nanos(storage_visitor.duration),
                    }),
                    StorageEventType::List => Some(MetricEvent::StorageListCompleted {
                        duration: std::time::Duration::from_nanos(storage_visitor.duration),
                        num_files: storage_visitor.num_files,
                    }),
                    StorageEventType::Read => Some(MetricEvent::StorageReadCompleted {
                        duration: std::time::Duration::from_nanos(storage_visitor.duration),
                        num_files: storage_visitor.num_files,
                        bytes_read: storage_visitor.bytes_read,
                    }),
                }
            }
            JSON_READ_COMPLETED_SPAN => {
                let mut v = FileReadVisitor::default();
                attrs.record(&mut v);
                Some(MetricEvent::JsonReadCompleted {
                    num_files: v.num_files,
                    bytes_read: v.bytes_read,
                })
            }
            PARQUET_READ_COMPLETED_SPAN => {
                let mut v = FileReadVisitor::default();
                attrs.record(&mut v);
                Some(MetricEvent::ParquetReadCompleted {
                    num_files: v.num_files,
                    bytes_read: v.bytes_read,
                })
            }
            SCAN_METADATA_COMPLETED_SPAN => {
                let mut v = ScanMetadataVisitor::default();
                attrs.record(&mut v);
                Some(v.into_event())
            }
            _ => None,
        };

        let mut visitor = EventVisitor::new(event);
        attrs.record(&mut visitor);
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            extensions.insert(visitor);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.event_span(event) {
            let mut extensions = span.extensions_mut();
            if let Some(visitor) = extensions.get_mut::<EventVisitor>() {
                event.record(visitor);
            }
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(visitor) = extensions.get_mut::<EventVisitor>() {
                values.record(visitor);
            }
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // Record the start time on first entry only. Spans that are entered and exited
        // multiple times (e.g. iterator adapters) keep the timestamp from the first entry
        // so that on_close measures elapsed wall time from the span's beginning.
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if extensions.get_mut::<Instant>().is_none() {
                extensions.insert(Instant::now());
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(metadata) = ctx.metadata(&id) else {
            return;
        };
        if metadata.fields().field("report").is_some() {
            let Some(span) = ctx.span(&id) else { return };
            let mut extensions = span.extensions_mut();
            let duration = extensions.get_mut::<Instant>().map(|start| start.elapsed());
            if let Some(event_visitor) = extensions.get_mut::<EventVisitor>() {
                if let Some(duration) = duration {
                    event_visitor.set_duration(duration);
                }
                if let Some(event) = event_visitor.event.take() {
                    self.reporter.report(event);
                }
            }
        }
    }
}
