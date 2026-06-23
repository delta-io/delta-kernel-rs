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
    ScanMetadataCompleted, SetTransactionLoadSuccess, SnapshotBuildSuccess,
    TransactionCommitSuccess, STORAGE_SPAN,
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
            TransactionCommitSuccess::SPAN_NAME => Some(MetricEvent::TransactionCommitSuccess(
                TransactionCommitSuccess::from_attrs(attrs),
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

    // RUNTIME CHANNEL. on_record is the metric-write channel: it routes Span::record(...) updates
    // through EventVisitor -> MetricEvent::record_u64 / record_bool / record_str. on_event carries
    // log lines, not metric writes; its only metric effect is the `#[instrument(err)]` failure
    // signal, which FailureFlipVisitor turns into the event's failure counterpart. Every other
    // event field is ignored.
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.event_span(event) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        if let Some(visitor) = extensions.get_mut::<EventVisitor>() {
            event.record(&mut FailureFlipVisitor {
                event: &mut visitor.event,
            });
        }
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

    fn record_str(&mut self, field: &Field, value: &str) {
        let Some(event) = self.event.as_mut() else {
            return;
        };
        if let Err(span_name) = event.record_str(field.name(), value) {
            self.warn_invalid(field, span_name);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
        // Debug-typed fields carry no metric values; `.record()` only delivers u64/bool/str.
    }
}

// ====================================================================
// FailureFlipVisitor: the event channel's only metric effect
// ====================================================================

/// Event-path visitor for the synthetic event `#[instrument(err)]` emits on `Err` return.
///
/// Flips the span's in-flight success event to its failure counterpart and ignores every other
/// field, so arbitrary log-line fields never reach the metric allowlist.
struct FailureFlipVisitor<'a> {
    event: &'a mut Option<MetricEvent>,
}

impl Visit for FailureFlipVisitor<'_> {
    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        if field.name() == "error" {
            *self.event = self.event.take().map(MetricEvent::into_failure);
        }
    }
}

// ====================================================================
// Tests
// ====================================================================

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::sync::{Arc, Mutex};

    use test_utils::{ensure_metrics_compatible_global_subscriber, LogWriter};
    use tracing::field::Empty;
    use tracing::subscriber::with_default;
    use tracing::{info, info_span, instrument};
    use tracing_subscriber::fmt::layer;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::registry;
    use uuid::Uuid;

    use crate::metrics::events::SNAPSHOT_COMPLETED_SPAN;
    use crate::metrics::{MetricEvent, WithMetricsReporterLayer as _};
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

    /// Run `f` under a subscriber that layers the metrics layer over an fmt layer capturing to a
    /// buffer, and return the captured log text -- so a test can assert on the warnings the metrics
    /// layer emits.
    fn capture_logs(f: impl FnOnce()) -> String {
        ensure_metrics_compatible_global_subscriber();
        let logs = Arc::new(Mutex::new(Vec::<u8>::new()));
        let logs_writer = Arc::clone(&logs);
        let subscriber = registry()
            .with(
                layer()
                    .with_writer(move || LogWriter(Arc::clone(&logs_writer)))
                    .with_ansi(false),
            )
            .with_metrics_reporter_layer(Arc::new(CapturingReporter::default()));
        with_default(subscriber, f);
        let bytes = logs.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    // info_span! requires a string-literal name; assert it still matches the span const.
    #[test]
    fn log_event_field_inside_reporting_span_does_not_warn() {
        assert_eq!(SNAPSHOT_COMPLETED_SPAN, "snap.build");
        let logs = capture_logs(|| {
            let span = info_span!(
                "snap.build",
                report = Empty,
                operation_id = %Uuid::new_v4(),
            );
            let _enter = span.enter();
            info!(log_tail_len = 5u64, "building snapshot");
        });
        assert!(
            !logs.contains("Invalid field"),
            "a log line's fields must not be judged as metric writes; got: {logs}"
        );
    }

    #[test]
    fn recorded_unknown_field_still_warns() {
        assert_eq!(SNAPSHOT_COMPLETED_SPAN, "snap.build");
        let logs = capture_logs(|| {
            let span = info_span!(
                "snap.build",
                report = Empty,
                operation_id = %Uuid::new_v4(),
                bogus = Empty,
            );
            span.record("bogus", 7u64);
        });
        assert!(
            logs.contains("Invalid field 'bogus' recorded on snap.build span"),
            "an unrecognized .record() field is a declared-field typo and must still warn; got: {logs}"
        );
    }

    #[instrument(name = SNAPSHOT_COMPLETED_SPAN, fields(report, operation_id = %Uuid::new_v4()), err)]
    fn failing_build() -> Result<(), Error> {
        Err(Error::other("boom"))
    }

    #[test]
    fn instrument_err_event_still_flips_to_failure() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let _ = failing_build();
        assert!(
            reporter
                .events()
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildFailure(_))),
            "#[instrument(err)] must flip the success event to its failure form via on_event"
        );
    }
}
