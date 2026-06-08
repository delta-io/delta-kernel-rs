//! Metrics reporter trait and tracing-layer integration.

use std::sync::Arc;
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{event, warn, Level, Span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use super::events::{
    storage_metric_from_attrs, CrcReadSuccess, DomainMetadataLoadSuccess, JsonReadCompleted,
    LogSegmentLoadSuccess, MetricEvent, ParquetReadCompleted, ProtocolMetadataLoadSuccess,
    ScanMetadataCompleted, SetTransactionLoadSuccess, SnapshotBuildSuccess, STORAGE_SPAN,
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
    // CONSTRUCTION-TIME CHANNEL. Each Type::from_attrs(attrs) extracts fields bound at span
    // creation.
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(metadata) = ctx.metadata(id) else {
            return;
        };
        let event = match metadata.name() {
            LogSegmentLoadSuccess::SPAN_NAME => Some(MetricEvent::LogSegmentLoadSuccess(
                LogSegmentLoadSuccess::from_attrs(attrs),
            )),
            ProtocolMetadataLoadSuccess::SPAN_NAME => {
                Some(MetricEvent::ProtocolMetadataLoadSuccess(
                    ProtocolMetadataLoadSuccess::from_attrs(attrs),
                ))
            }
            SnapshotBuildSuccess::SPAN_NAME => Some(MetricEvent::SnapshotBuildSuccess(
                SnapshotBuildSuccess::from_attrs(attrs),
            )),
            DomainMetadataLoadSuccess::SPAN_NAME => Some(MetricEvent::DomainMetadataLoadSuccess(
                DomainMetadataLoadSuccess::from_attrs(attrs),
            )),
            SetTransactionLoadSuccess::SPAN_NAME => Some(MetricEvent::SetTransactionLoadSuccess(
                SetTransactionLoadSuccess::from_attrs(attrs),
            )),
            CrcReadSuccess::SPAN_NAME => Some(MetricEvent::CrcReadSuccess(
                CrcReadSuccess::from_attrs(attrs),
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

    // RUNTIME CHANNEL. Both on_event and on_record route Span::current().record(...) updates
    // (and info!() events within a span) through EventVisitor -> MetricEvent::record_u64 /
    // record_bool. The visitor's record_debug also handles `#[instrument(err)]`'s `error` field,
    // flipping a success event into its failure counterpart.
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
// EventVisitor: dispatches field updates to the inner MetricEvent struct
// ====================================================================

/// Per-span visitor stashed in the span's extensions. Responsible for:
///
/// 1. Holding the partially-constructed `MetricEvent` while the span runs.
/// 2. Implementing [`Visit`] so the layer can route `record_*` callbacks to the underlying
///    `MetricEvent` state.
/// 3. Accumulating any deferred warnings to emit after locks are released.
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
        if let Err(span_name) = event.record_u64(field.name(), value) {
            self.warn_invalid(field, span_name);
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        let Some(event) = self.event.as_mut() else {
            return;
        };
        if let Err(span_name) = event.record_bool(field.name(), value) {
            self.warn_invalid(field, span_name);
        }
    }

    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        match field.name() {
            "return" => {} // default to the success case
            "error" => {
                // `#[instrument(err)]` records `error` when the wrapped function returns Err.
                self.event = self.event.take().map(MetricEvent::into_failure);
            }
            _ => {}
        }
    }
}
