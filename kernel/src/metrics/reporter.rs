//! Metrics reporter trait and tracing-layer integration.

use std::sync::Arc;
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{event, warn, Level, Span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use super::events::{
    storage_metric_from_attrs, CrcReadCompleted, JsonReadCompleted, LogSegmentLoaded, MetricEvent,
    ParquetReadCompleted, ProtocolMetadataLoaded, ScanMetadataCompleted, SnapshotCompleted,
    STORAGE_SPAN,
};

// ====================================================================
// MetricsReporter trait + LoggingMetricsReporter
// ====================================================================

/// Receives [`MetricEvent`]s as they occur during Delta operations and forwards them to a
/// monitoring system. Reporter implementations must be cheap to call on any thread.
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
        Self { level }
    }
}

impl MetricsReporter for LoggingMetricsReporter {
    fn report(&self, event: MetricEvent) {
        // event! needs a constant level. Detach from the parent span so the log line carries the
        // event payload alone, not the span context that produced it.
        match self.level {
            Level::ERROR => event!(parent: Span::none(), Level::ERROR, "{}", event),
            Level::WARN => event!(parent: Span::none(), Level::WARN, "{}", event),
            Level::INFO => event!(parent: Span::none(), Level::INFO, "{}", event),
            Level::DEBUG => event!(parent: Span::none(), Level::DEBUG, "{}", event),
            Level::TRACE => event!(parent: Span::none(), Level::TRACE, "{}", event),
        }
    }
}

// ====================================================================
// ReportGeneratorLayer
// ====================================================================

/// A [`tracing_subscriber::Layer`] that converts kernel tracing spans into [`MetricEvent`]s and
/// forwards them to a registered [`MetricsReporter`].
///
/// Typically added to a subscriber via
/// [`super::WithMetricsReporterLayer::with_metrics_reporter_layer`].
#[derive(Debug)]
pub struct ReportGeneratorLayer {
    reporter: Arc<dyn MetricsReporter>,
}

impl ReportGeneratorLayer {
    /// Create a new layer that forwards metric events to the given reporter.
    pub fn new(reporter: Arc<dyn MetricsReporter>) -> Self {
        Self { reporter }
    }

    /// Apply `record` to the visitor stashed on `span`, then drain its pending warnings. Warnings
    /// must be emitted *after* the `extensions_mut` lock is released; calling `warn!()` while
    /// holding it would re-enter the layer and deadlock.
    fn drain_into_visitor<S>(
        span: Option<tracing_subscriber::registry::SpanRef<'_, S>>,
        record: impl FnOnce(&mut EventVisitor),
    ) where
        S: Subscriber + for<'l> tracing_subscriber::registry::LookupSpan<'l>,
    {
        let warnings = span.and_then(|span| {
            let mut extensions = span.extensions_mut();
            let visitor = extensions.get_mut::<EventVisitor>()?;
            record(visitor);
            Some(std::mem::take(&mut visitor.pending_warnings))
        });
        for w in warnings.unwrap_or_default() {
            warn!("{w}");
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
        let event = match metadata.name() {
            LogSegmentLoaded::SPAN_NAME => Some(MetricEvent::LogSegmentLoaded(
                LogSegmentLoaded::from_attrs(attrs),
            )),
            ProtocolMetadataLoaded::SPAN_NAME => Some(MetricEvent::ProtocolMetadataLoaded(
                ProtocolMetadataLoaded::from_attrs(attrs),
            )),
            SnapshotCompleted::SPAN_NAME => Some(MetricEvent::SnapshotCompleted(
                SnapshotCompleted::from_attrs(attrs),
            )),
            CrcReadCompleted::SPAN_NAME => Some(MetricEvent::CrcReadCompleted(
                CrcReadCompleted::from_attrs(attrs),
            )),
            JsonReadCompleted::SPAN_NAME => Some(MetricEvent::JsonReadCompleted(
                JsonReadCompleted::from_attrs(attrs),
            )),
            ParquetReadCompleted::SPAN_NAME => Some(MetricEvent::ParquetReadCompleted(
                ParquetReadCompleted::from_attrs(attrs),
            )),
            ScanMetadataCompleted::SPAN_NAME => Some(MetricEvent::ScanMetadataCompleted(
                ScanMetadataCompleted::from_attrs(attrs),
            )),
            STORAGE_SPAN => storage_metric_from_attrs(attrs),
            _ => None,
        };

        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            extensions.insert(EventVisitor::new(event));
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        Self::drain_into_visitor(ctx.event_span(event), |v| event.record(v));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        Self::drain_into_visitor(ctx.span(id), |v| values.record(v));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // Stash the start time on first entry. Spans entered and exited multiple times (e.g.
        // iterator adapters) keep the original timestamp so on_close measures wall time from the
        // span's first entry.
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
        if metadata.fields().field("report").is_none() {
            return;
        }
        let Some(span) = ctx.span(&id) else { return };
        let event = {
            let mut extensions = span.extensions_mut();
            let duration = extensions.get_mut::<Instant>().map(|s| s.elapsed());
            let Some(visitor) = extensions.get_mut::<EventVisitor>() else {
                return;
            };
            if let (Some(d), Some(event)) = (duration, visitor.event.as_mut()) {
                event.set_duration_if_applicable(d);
            }
            visitor.event.take()
        }; // unlock the extensions before reporting; the reporter is free to warn!() etc.
        if let Some(event) = event {
            self.reporter.report(event);
        }
    }
}

// ====================================================================
// EventVisitor: dispatches field updates to the inner event struct
// ====================================================================

/// Per-span visitor stashed in the span's extensions. Holds the partially-constructed event
/// while the span runs, plus any deferred warnings to emit after locks are released.
struct EventVisitor {
    event: Option<MetricEvent>,
    pending_warnings: Vec<String>,
}

impl EventVisitor {
    fn new(event: Option<MetricEvent>) -> Self {
        Self {
            event,
            pending_warnings: vec![],
        }
    }

    fn warn_invalid(&mut self, field: &Field, span_name: &str) {
        self.pending_warnings.push(format!(
            "Invalid field '{}' recorded on {span_name} span",
            field.name()
        ));
    }
}

impl Visit for EventVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        let Some(event) = self.event.as_mut() else {
            return;
        };
        let result = match event {
            MetricEvent::LogSegmentLoaded(e) => e.record_u64(field.name(), value),
            MetricEvent::SnapshotCompleted(e) => e.record_u64(field.name(), value),
            MetricEvent::CrcReadCompleted(e) => e.record_u64(field.name(), value),
            _ => Ok(()),
        };
        if let Err(span_name) = result {
            self.warn_invalid(field, span_name);
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        let Some(event) = self.event.as_mut() else {
            return;
        };
        let result = match event {
            MetricEvent::LogSegmentLoaded(e) => e.record_bool(field.name(), value),
            _ => Ok(()),
        };
        if let Err(span_name) = result {
            self.warn_invalid(field, span_name);
        }
    }

    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        match field.name() {
            "return" => {} // default to the success case
            "error" => {
                // `#[instrument(err)]` records `error` when the wrapped function returns Err.
                // Flip SnapshotCompleted into SnapshotFailed.
                if let Some(MetricEvent::SnapshotCompleted(snap)) = self.event.take() {
                    self.event = Some(MetricEvent::SnapshotFailed(snap.into_failed()));
                }
            }
            _ => {}
        }
    }
}

// ====================================================================
// emit_* helpers
// ====================================================================
//
// Emission sites that build a fully-formed event up front (scan metadata) and connector-side
// `JsonHandler`/`ParquetHandler` implementations that want to report read metrics.

/// Emit a [`MetricEvent::JsonReadCompleted`] via a tracing span.
///
/// Call once per [`crate::JsonHandler::read_json_files`] invocation, after the file list is known
/// but before the iterator is consumed (i.e. at iterator exhaustion or drop).
pub fn emit_json_read_completed(num_files: u64, bytes_read: u64) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        JsonReadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a [`MetricEvent::ParquetReadCompleted`] via a tracing span.
///
/// Call once per [`crate::ParquetHandler::read_parquet_files`] invocation, after the file list is
/// known but before the iterator is consumed (i.e. at iterator exhaustion or drop).
pub fn emit_parquet_read_completed(num_files: u64, bytes_read: u64) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ParquetReadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a [`MetricEvent::ScanMetadataCompleted`] via a tracing span. Call when the scan metadata
/// iterator is exhausted or dropped.
pub(crate) fn emit_scan_metadata_completed(e: &ScanMetadataCompleted) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ScanMetadataCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %e.operation_id,
        scan_type = %e.scan_type,
        duration_ns = e.duration.as_nanos() as u64,
        num_add_files_seen = e.num_add_files_seen,
        num_active_add_files = e.num_active_add_files,
        active_add_files_bytes = e.active_add_files_bytes,
        num_remove_files_seen = e.num_remove_files_seen,
        num_non_file_actions = e.num_non_file_actions,
        num_predicate_filtered = e.num_predicate_filtered,
        peak_hash_set_size = e.peak_hash_set_size as u64,
        dedup_visitor_time_ms = e.dedup_visitor_time_ms,
        predicate_eval_time_ms = e.predicate_eval_time_ms,
    );
}
