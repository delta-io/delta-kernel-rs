//! Metrics reporter trait and implementations.

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

/// A no-op metrics reporter that discards all metrics.
///
/// This is used as the default reporter when no metrics collection is configured.
#[derive(Debug, Clone, Copy)]
pub struct NullReporter;

impl MetricsReporter for NullReporter {
    fn report(&self, _event: MetricEvent) {
        // No-op: discard the metric
    }
}
