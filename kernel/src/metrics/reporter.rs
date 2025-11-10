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
