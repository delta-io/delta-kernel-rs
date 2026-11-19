//! Metrics reporter trait and tracing-layer integration.

use std::sync::Arc;
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Level, Span, Subscriber, event, warn};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

use super::events::{
    CheckpointWriteCompleted, ChecksumWriteCompleted, CrcReadCompleted,
    DomainMetadataLoadCompleted, JsonReadCompleted, LastCheckpointReadCompleted,
    LogSegmentLoadCompleted, MetricEvent, ParquetReadCompleted, ProtocolMetadataLoadCompleted,
    STORAGE_SPAN, ScanMetadataCompleted, SetTransactionLoadCompleted, SnapshotBuildCompleted,
    TransactionCommitCompleted, storage_metric_from_attrs,
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
            CheckpointWriteCompleted::SPAN_NAME => Some(MetricEvent::CheckpointWriteCompleted(
                CheckpointWriteCompleted::from_attrs(attrs),
            )),
            LogSegmentLoadCompleted::SPAN_NAME => Some(MetricEvent::LogSegmentLoadCompleted(
                LogSegmentLoadCompleted::from_attrs(attrs),
            )),
            ProtocolMetadataLoadCompleted::SPAN_NAME => {
                Some(MetricEvent::ProtocolMetadataLoadCompleted(
                    ProtocolMetadataLoadCompleted::from_attrs(attrs),
                ))
            }
            SnapshotBuildCompleted::SPAN_NAME => Some(MetricEvent::SnapshotBuildCompleted(
                SnapshotBuildCompleted::from_attrs(attrs),
            )),
            TransactionCommitCompleted::SPAN_NAME => Some(MetricEvent::TransactionCommitCompleted(
                TransactionCommitCompleted::from_attrs(attrs),
            )),
            DomainMetadataLoadCompleted::SPAN_NAME => {
                Some(MetricEvent::DomainMetadataLoadCompleted(
                    DomainMetadataLoadCompleted::from_attrs(attrs),
                ))
            }
            SetTransactionLoadCompleted::SPAN_NAME => {
                Some(MetricEvent::SetTransactionLoadCompleted(
                    SetTransactionLoadCompleted::from_attrs(attrs),
                ))
            }
            CrcReadCompleted::SPAN_NAME => Some(MetricEvent::CrcReadCompleted(
                CrcReadCompleted::from_attrs(attrs),
            )),
            LastCheckpointReadCompleted::SPAN_NAME => {
                Some(MetricEvent::LastCheckpointReadCompleted(
                    LastCheckpointReadCompleted::from_attrs(attrs),
                ))
            }
            ChecksumWriteCompleted::SPAN_NAME => Some(MetricEvent::ChecksumWriteCompleted(
                ChecksumWriteCompleted::from_attrs(attrs),
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

    // RUNTIME CHANNEL. on_event carries log lines, not metric writes; its only metric effect is the
    // `#[instrument(err)]` failure signal, handled by ErrorVisitor.
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.event_span(event) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        if let Some(visitor) = extensions.get_mut::<EventVisitor>() {
            event.record(&mut ErrorVisitor {
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
            if !visitor.duration_recorded
                && let (Some(d), Some(event)) = (duration, visitor.event.as_mut())
            {
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
    // Checkpoint data can finish before the reporting span closes. Preserve its recorded duration.
    duration_recorded: bool,
}

impl EventVisitor {
    fn new(event: Option<MetricEvent>) -> Self {
        Self {
            event,
            pending_warnings: vec![],
            duration_recorded: false,
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
        match event.record_u64(field.name(), value) {
            Ok(()) if field.name() == "duration_ns" => {
                self.duration_recorded = true;
            }
            Ok(()) => {}
            Err(span_name) => self.warn_invalid(field, span_name),
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
        // Kernel records every metric field as u64/bool/str (handled above). Anything arriving here
        // is a debug-formatted log field (or a numeric type tracing forwards to record_debug), not
        // a metric write.
    }
}

// ====================================================================
// ErrorVisitor: the event channel's only metric effect
// ====================================================================

/// Event-path visitor for the synthetic event `#[instrument(err)]` emits on `Err` return.
///
/// Marks the span's in-flight metric as failed and ignores every other field, so arbitrary
/// log-line fields never reach the metric allowlist.
struct ErrorVisitor<'a> {
    event: &'a mut Option<MetricEvent>,
}

impl Visit for ErrorVisitor<'_> {
    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        // `#[instrument(err)]` emits a synthetic event with an `error` field (via Display, hence
        // record_debug) when the wrapped fn returns Err. That field name is a tracing convention,
        // not kernel's.
        if field.name() == "error"
            && let Some(event) = self.event.as_mut()
        {
            event.mark_error();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use test_utils::{LogWriter, ensure_metrics_compatible_global_subscriber};
    use tracing::field::Empty;
    use tracing::subscriber::with_default;
    use tracing::{info, info_span, instrument};
    use tracing_subscriber::fmt::layer;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::registry;
    use uuid::Uuid;

    use crate::metrics::events::{
        SNAPSHOT_COMPLETED_SPAN, SnapshotLoadMetricContext, emit_log_segment_load,
        emit_protocol_metadata_load,
    };
    use crate::metrics::{
        CheckpointOutputStats, CheckpointProcessingStats, CheckpointWriteCompleted,
        CheckpointWriteMode, CheckpointWriteOutcome, CheckpointWriteStats, ChecksumWriteOutcome,
        CrcReadOutcome, DomainMetadataLoadOutcome, LastCheckpointReadOutcome,
        LogSegmentLoadCompleted, LogSegmentLoadOutcome, MetricEvent, MetricId,
        ProtocolMetadataLoadCompleted, ProtocolMetadataLoadOutcome, SetTransactionLoadOutcome,
        SnapshotBuildCompleted, SnapshotBuildOutcome, TransactionCommitOutcome,
        WithMetricsReporterLayer as _,
    };
    use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};

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

    #[rstest::rstest]
    #[case::missing(None, LastCheckpointReadOutcome::Unknown)]
    #[case::empty(Some(""), LastCheckpointReadOutcome::Unknown)]
    #[case::invalid(Some("not_an_outcome"), LastCheckpointReadOutcome::Unknown)]
    #[case::success(Some("success"), LastCheckpointReadOutcome::Success)]
    #[case::not_found(Some("not_found"), LastCheckpointReadOutcome::NotFound)]
    #[case::invalid_hint(Some("invalid"), LastCheckpointReadOutcome::Invalid)]
    #[case::error(Some("error"), LastCheckpointReadOutcome::Error)]
    #[case::unknown(Some("unknown"), LastCheckpointReadOutcome::Unknown)]
    fn last_checkpoint_read_decodes_outcome_and_records_duration(
        #[case] value: Option<&str>,
        #[case] expected: LastCheckpointReadOutcome,
        #[values(false, true)] error_event: bool,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        {
            let span = info_span!("last_checkpoint.read", report = Empty, outcome = Empty);
            let _entered = span.enter();
            if let Some(value) = value {
                span.record("outcome", value);
            }
            info!(
                outcome = "invalid",
                "log fields must not overwrite metric fields"
            );
            if error_event {
                info!(error = %"injected error");
            }
        }
        let events = reporter.events();
        let [MetricEvent::LastCheckpointReadCompleted(event)] = events.as_slice() else {
            panic!("expected one last checkpoint metric: {events:?}");
        };
        let expected = if error_event && expected == LastCheckpointReadOutcome::Unknown {
            LastCheckpointReadOutcome::Error
        } else {
            expected
        };
        assert_eq!(event.outcome, expected);
        assert!(event.duration > Duration::ZERO);
    }

    #[test]
    fn last_checkpoint_invalid_outcome_warns_after_releasing_extension_lock() {
        let logs = capture_logs(|| {
            let span = info_span!("last_checkpoint.read", report = Empty, outcome = Empty);
            let _entered = span.enter();
            span.record("outcome", "not_an_outcome");
        });
        assert!(logs.contains("Invalid field 'outcome' recorded on last_checkpoint.read span"));
    }

    #[test]
    fn log_event_field_inside_reporting_span_does_not_warn() {
        // info_span! requires a string-literal name; assert it still matches the span const.
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

    #[rstest::rstest]
    fn checkpoint_recorded_duration_survives_span_close_and_errors(
        #[values(0u64, 17)] duration_ns: u64,
        #[values(false, true)] fail: bool,
    ) {
        let operation_id = MetricId::new();
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        {
            let span = info_span!(
                "snap.checkpoint",
                report = Empty,
                operation_id = %operation_id,
                version = 42u64,
                outcome = Empty,
                mode = Empty,
                duration_ns = Empty,
                bytes_written = Empty,
                size_in_bytes = Empty,
                num_add_files = Empty,
                num_remove_files = Empty,
                num_actions = Empty,
                num_sidecars = Empty,
                log_tail_input_duration_ns = Empty,
                checkpoint_input_duration_ns = Empty,
                reconciliation_duration_ns = Empty,
                stats_transform_duration_ns = Empty,
                output_preparation_duration_ns = Empty,
                num_actions_from_log_tail = Empty,
                num_actions_from_checkpoint = Empty,
                parquet_write_duration_ns = Empty,
                verification_duration_ns = Empty,
                num_sidecars_written = Empty,
            );
            let _entered = span.enter();
            span.record("mode", "v2_with_sidecars");
            span.record("duration_ns", duration_ns);
            span.record("size_in_bytes", 50u64);
            span.record("num_add_files", 1u64);
            span.record("num_remove_files", 0u64);
            span.record("num_actions", 5u64);
            span.record("num_sidecars", 1u64);
            for (name, value) in [
                ("log_tail_input_duration_ns", 1u64),
                ("reconciliation_duration_ns", 2),
                ("stats_transform_duration_ns", 3),
                ("output_preparation_duration_ns", 4),
                ("num_actions_from_log_tail", 5),
                ("num_actions_from_checkpoint", 6),
                ("checkpoint_input_duration_ns", 7),
                ("parquet_write_duration_ns", 8),
                ("verification_duration_ns", 9),
                ("bytes_written", 10),
                ("num_sidecars_written", 11),
            ] {
                span.record(name, value);
            }
            if fail {
                info!(error = %"injected error");
            }
        }
        let events = reporter.events();
        assert_eq!(events.len(), 1);
        let (mode, duration, id, version, processing, output) = match (&events[0], fail) {
            (
                MetricEvent::CheckpointWriteCompleted(
                    event @ CheckpointWriteCompleted {
                        outcome: CheckpointWriteOutcome::Error,
                        ..
                    },
                ),
                true,
            ) => (
                event.mode,
                event.duration,
                event.operation_id,
                event.version,
                event.processing_stats,
                event.output_stats,
            ),
            (
                MetricEvent::CheckpointWriteCompleted(
                    event @ CheckpointWriteCompleted {
                        outcome:
                            CheckpointWriteOutcome::Written { .. }
                            | CheckpointWriteOutcome::AlreadyExists,
                        ..
                    },
                ),
                false,
            ) => {
                assert_eq!(
                    event.outcome,
                    CheckpointWriteOutcome::Written {
                        stats: CheckpointWriteStats {
                            size_in_bytes: 50,
                            num_add_files: 1,
                            num_remove_files: 0,
                            num_actions: 5,
                            num_sidecars: 1,
                        }
                    }
                );
                (
                    event.mode,
                    event.duration,
                    event.operation_id,
                    event.version,
                    event.processing_stats,
                    event.output_stats,
                )
            }
            (event, _) => panic!("unexpected checkpoint metric: {event:?}"),
        };
        assert_eq!(duration, Duration::from_nanos(duration_ns));
        assert_eq!(mode, CheckpointWriteMode::V2WithSidecars);
        assert_eq!(id, operation_id);
        assert_eq!(version, 42);
        assert_eq!(
            processing,
            CheckpointProcessingStats {
                log_tail_input_duration: Duration::from_nanos(1),
                checkpoint_input_duration: Duration::from_nanos(7),
                reconciliation_duration: Duration::from_nanos(2),
                stats_transform_duration: Duration::from_nanos(3),
                output_preparation_duration: Duration::from_nanos(4),
                num_actions_from_log_tail: 5,
                num_actions_from_checkpoint: 6,
            }
        );
        assert_eq!(
            output,
            CheckpointOutputStats {
                parquet_write_duration: Duration::from_nanos(8),
                verification_duration: Duration::from_nanos(9),
                bytes_written: 10,
                num_sidecars_written: 11,
            }
        );
    }

    #[test]
    fn checkpoint_invalid_enum_fields_warn_without_reentering_the_extension_lock() {
        let logs = capture_logs(|| {
            let span = info_span!(
                "snap.checkpoint",
                report = Empty,
                outcome = Empty,
                mode = Empty
            );
            span.record("outcome", "invalid");
            span.record("mode", "invalid");
        });
        assert!(logs.contains("Invalid field 'outcome' recorded on snap.checkpoint span"));
        assert!(logs.contains("Invalid field 'mode' recorded on snap.checkpoint span"));
    }

    // A log line whose field name collides with a real metric field must NOT overwrite the value a
    // genuine `span.record` wrote -- the core guarantee of narrowing on_event to
    // ErrorVisitor. capture_logs discards the reporter, so this test installs a
    // CapturingReporter to inspect the reported event.
    #[test]
    fn log_event_reusing_a_metric_field_name_does_not_overwrite_recorded_metric() {
        assert_eq!(SNAPSHOT_COMPLETED_SPAN, "snap.build");
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        {
            let span = info_span!(
                "snap.build",
                report = Empty,
                operation_id = %Uuid::new_v4(),
                version = Empty,
            );
            let _enter = span.enter();
            span.record("version", 7u64); // real metric write via the on_record channel
            info!(version = 999u64, "noise"); // log line reusing the metric field name (on_event)
        } // span close -> report
        let events = reporter.events();
        let Some(MetricEvent::SnapshotBuildCompleted(
            success @ SnapshotBuildCompleted {
                outcome: SnapshotBuildOutcome::Success { .. },
                ..
            },
        )) = events.iter().find(|e| {
            matches!(
                e,
                MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                    outcome: SnapshotBuildOutcome::Success { .. },
                    ..
                })
            )
        })
        else {
            panic!("expected a snapshot build success event; got: {events:?}");
        };

        let SnapshotBuildOutcome::Success {
            version: metric_success_version,
            ..
        } = &success.outcome
        else {
            panic!("unexpected metric outcome");
        };
        assert_eq!(
            (*metric_success_version),
            7,
            "an on_event log field must not overwrite a span.record metric"
        );
    }

    #[instrument(
        name = SNAPSHOT_COMPLETED_SPAN,
        skip_all,
        fields(report, operation_id = %Uuid::new_v4()),
        err
    )]
    fn failing_build(delay: Duration) -> Result<(), Error> {
        std::thread::sleep(delay);
        Err(Error::other("boom"))
    }

    #[test]
    fn instrument_err_event_reports_elapsed_failure_duration() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let delay = Duration::from_millis(1);
        let _ = failing_build(delay);
        let events = reporter.events();
        assert!(
            matches!(events.as_slice(), [MetricEvent::SnapshotBuildCompleted(event)]
                if event.outcome == SnapshotBuildOutcome::Error && event.duration >= delay),
            "expected one snapshot build failure with duration >= {delay:?}; got: {events:?}"
        );
    }

    #[rstest::rstest]
    fn failure_emitters_preserve_operation_duration(#[values(0u64, 73)] duration_ns: u64) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let context = SnapshotLoadMetricContext::for_test();
        let duration = Duration::from_nanos(duration_ns);

        emit_log_segment_load(&context, LogSegmentLoadOutcome::Error, duration);
        emit_protocol_metadata_load(&context, ProtocolMetadataLoadOutcome::Error, duration);

        let events = reporter.events();
        let [
            MetricEvent::LogSegmentLoadCompleted(
                segment @ LogSegmentLoadCompleted {
                    outcome: LogSegmentLoadOutcome::Error,
                    ..
                },
            ),
            MetricEvent::ProtocolMetadataLoadCompleted(
                pm @ ProtocolMetadataLoadCompleted {
                    outcome: ProtocolMetadataLoadOutcome::Error,
                    ..
                },
            ),
        ] = events.as_slice()
        else {
            panic!("expected log-segment and protocol/metadata failures; got: {events:?}");
        };
        assert_eq!(segment.duration, duration);
        assert_eq!(pm.duration, duration);
    }

    #[instrument(name = SNAPSHOT_COMPLETED_SPAN, fields(report, operation_id = %Uuid::new_v4()), ret)]
    fn succeeding_build() -> Result<(), Error> {
        Ok(())
    }

    #[test]
    fn instrument_ret_event_does_not_flip_to_failure() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let _ = succeeding_build();
        let events = reporter.events();
        assert!(
            events.iter().any(|e| matches!(
                e,
                MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                    outcome: SnapshotBuildOutcome::Success { .. },
                    ..
                })
            )),
            "#[instrument(ret)]'s `return` field must NOT flip success to failure; got: {events:?}"
        );
        assert!(
            !events.iter().any(|e| matches!(
                e,
                MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                    outcome: SnapshotBuildOutcome::Error,
                    ..
                })
            )),
            "a successful #[instrument(ret)] must not emit a failure event; got: {events:?}"
        );
    }
    #[rstest::rstest]
    #[case::snapshot(|| info_span!("snap.build", report = Empty))]
    #[case::commit(|| info_span!("txn.commit", report = Empty))]
    #[case::domain(|| info_span!("snap.get_domain_metadata", report = Empty))]
    #[case::set_transaction(|| info_span!("snap.get_app_id_version", report = Empty))]
    #[case::crc(|| info_span!("crc_read_completed", report = Empty))]
    #[case::checkpoint(|| info_span!("snap.checkpoint", report = Empty))]
    #[case::checksum(|| info_span!("snap.write_checksum", report = Empty))]
    fn lifecycle_errors_report_elapsed_time(#[case] make_span: fn() -> tracing::Span) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        {
            let span = make_span();
            let _entered = span.enter();
            info!(error = tracing::field::display("injected failure"));
        }
        let events = reporter.events();
        assert_eq!(events.len(), 1);
        let (duration, failed) = match &events[0] {
            MetricEvent::SnapshotBuildCompleted(e) => {
                (e.duration, matches!(e.outcome, SnapshotBuildOutcome::Error))
            }
            MetricEvent::TransactionCommitCompleted(e) => (
                e.duration,
                matches!(
                    e.outcome,
                    TransactionCommitOutcome::Failure {
                        reason: crate::metrics::CommitFailureReason::Error
                    }
                ),
            ),
            MetricEvent::DomainMetadataLoadCompleted(e) => (
                e.duration,
                matches!(e.outcome, DomainMetadataLoadOutcome::Error),
            ),
            MetricEvent::SetTransactionLoadCompleted(e) => (
                e.duration,
                matches!(e.outcome, SetTransactionLoadOutcome::Error),
            ),
            MetricEvent::CrcReadCompleted(e) => {
                (e.duration, matches!(e.outcome, CrcReadOutcome::Error))
            }
            MetricEvent::CheckpointWriteCompleted(e) => {
                (e.duration, e.outcome == CheckpointWriteOutcome::Error)
            }
            MetricEvent::ChecksumWriteCompleted(e) => {
                (e.duration, e.outcome == ChecksumWriteOutcome::Error)
            }
            other => panic!("unexpected metric: {other:?}"),
        };
        assert!(failed);
        assert!(duration > Duration::ZERO);
    }

    #[test]
    fn invalid_commit_failure_reason_warns_after_releasing_span_lock() {
        let logs = capture_logs(|| {
            let span = info_span!("txn.commit", report = Empty, failure_reason = Empty);
            let _entered = span.enter();
            span.record("failure_reason", "invalid_reason");
        });
        assert!(logs.contains("Invalid field 'failure_reason' recorded on txn.commit span"));
    }
}
