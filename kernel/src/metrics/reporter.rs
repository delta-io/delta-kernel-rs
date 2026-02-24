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

/// A metrics reporter that will just log things at the specified level
#[derive(Debug)]
pub struct LoggingMetricsReporter {
    level: Level,
}

impl LoggingMetricsReporter {
    pub fn new(level: Level) -> Self {
        LoggingMetricsReporter { level }
    }
}

impl MetricsReporter for LoggingMetricsReporter {
    fn report(&self, event: MetricEvent) {
        // event! wants a constant, so we have to do this sillyness we also set the parent span to
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

pub struct ReportGeneratorLayer {
    reporter: Arc<dyn MetricsReporter>,
}

impl ReportGeneratorLayer {
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
        if let Some(event) = &mut self.event {
            if let MetricEvent::LogSegmentLoaded { duration, .. } = event {
                *duration = target_duration;
            }
            if let MetricEvent::ProtocolMetadataLoaded { duration, .. } = event {
                *duration = target_duration;
            }
            if let MetricEvent::SnapshotCompleted { total_duration, .. } = event {
                *total_duration = target_duration;
            }
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
                _ => warn!("Invalid field recorded on segment.read_metadata span"),
            }
        }

        if let Some(MetricEvent::SnapshotCompleted {
            ref mut version, ..
        }) = self.event
        {
            match field.name() {
                "version" => *version = value,
                _ => warn!("Invalid field recorded on snap.completed span"),
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
            "segment.for_snapshot" => Some(MetricEvent::LogSegmentLoaded {
                operation_id: MetricId(new_span_visitor.uuid),
                duration: std::time::Duration::default(),
                num_commit_files: 0,
                num_checkpoint_files: 0,
                num_compaction_files: 0,
            }),
            "segment.read_metadata" => Some(MetricEvent::ProtocolMetadataLoaded {
                operation_id: MetricId(new_span_visitor.uuid),
                duration: std::time::Duration::default(),
            }),
            "snap.completed" => Some(MetricEvent::SnapshotCompleted {
                operation_id: MetricId(new_span_visitor.uuid),
                version: 0,
                total_duration: std::time::Duration::default(),
            }),
            "storage" => {
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
        // Store the start time when the span is entered
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            extensions.insert(Instant::now());
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
