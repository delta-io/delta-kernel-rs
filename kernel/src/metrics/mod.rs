//! Metrics collection for Delta Kernel operations.
//!
//! This module provides metrics tracking for various Delta operations including
//! snapshot creation, scans, and transactions. Metrics are collected during operations
//! and reported as events via the `MetricsReporter` trait.
//!
//! Each operation (Snapshot, Transaction, Scan) is assigned a unique operation ID ([`MetricId`])
//! when it starts, and all subsequent events for that operation reference this ID.
//! This allows reporters to correlate events and track operation lifecycles.
//!
//! # Example: Implementing a Custom MetricsReporter
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::metrics::{MetricsReporter, MetricEvent};
//!
//! #[derive(Debug)]
//! struct LoggingReporter;
//!
//! impl MetricsReporter for LoggingReporter {
//!     fn report(&self, event: MetricEvent) {
//!         match event {
//!             MetricEvent::LogSegmentLoaded { operation_id, duration, num_commit_files, .. } => {
//!                 println!("Log segment loaded in {:?}: {} commits", duration, num_commit_files);
//!             }
//!             MetricEvent::SnapshotCompleted { operation_id, version, total_duration } => {
//!                 println!("Snapshot completed: v{} in {:?}", version, total_duration);
//!             }
//!             MetricEvent::SnapshotFailed { operation_id, duration } => {
//!                 println!("Snapshot failed: {} after {:?}", operation_id, duration);
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! # Example: Implementing a Composite Reporter
//!
//! If you need to send metrics to multiple destinations, you can create a composite reporter:
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::metrics::{MetricsReporter, MetricEvent};
//!
//! #[derive(Debug)]
//! struct CompositeReporter {
//!     reporters: Vec<Arc<dyn MetricsReporter>>,
//! }
//!
//! impl MetricsReporter for CompositeReporter {
//!     fn report(&self, event: MetricEvent) {
//!         for reporter in &self.reporters {
//!             reporter.report(event.clone());
//!         }
//!     }
//! }
//! ```
//!
//! # Storage Metrics
//!
//! Storage operations (list, read, copy) are automatically instrumented when using
//! `DefaultEngine` with a metrics reporter. The default storage handler implementation
//! emits `StorageListCompleted`, `StorageReadCompleted`, and `StorageCopyCompleted`
//! events that track latencies at the storage layer.
//!
//! These metrics are standalone and track aggregate storage performance without
//! correlating to specific Snapshot/Transaction operations.

mod events;
pub(crate) mod reporter;

use std::sync::Arc;

pub use events::{MetricEvent, MetricId, ScanType};
pub use reporter::{
    emit_json_read_completed, emit_parquet_read_completed, LoggingMetricsReporter, MetricsReporter,
    ReportGeneratorLayer,
};
use tracing::Subscriber;
use tracing_subscriber::layer::{Layered, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;

/// Extension trait that adds [`with_metrics_reporter_layer`] to any compatible tracing subscriber.
///
/// Only implemented for subscribers that also implement [`LookupSpan`], which is required by
/// [`ReportGeneratorLayer`] to store and retrieve per-span state.
///
/// [`with_metrics_reporter_layer`]: WithMetricsReporterLayer::with_metrics_reporter_layer
pub trait WithMetricsReporterLayer: Subscriber + for<'lookup> LookupSpan<'lookup> {
    /// Wrap this subscriber with a [`ReportGeneratorLayer`] that converts tracing spans into
    /// [`MetricEvent`]s and forwards them to `reporter`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use delta_kernel::metrics::{WithMetricsReporterLayer, LoggingMetricsReporter};
    /// use tracing_subscriber::prelude::*;
    ///
    /// tracing_subscriber::registry()
    ///     .with_metrics_reporter_layer(
    ///         Arc::new(LoggingMetricsReporter::new(tracing::Level::INFO))
    ///     );
    /// ```
    ///
    /// # Important: thread-local `set_default` and the callsite interest cache
    ///
    /// If you install the resulting subscriber via thread-local
    /// [`tracing::dispatcher::set_default`] (or
    /// [`tracing_subscriber::util::SubscriberInitExt::set_default`]), call
    /// [`tracing::callsite::rebuild_interest_cache`] immediately afterwards. Several
    /// kernel metrics are emitted from `Drop` impls (notably storage list/read completion
    /// in the default engine), and those `Drop` sites can run on threads that have no
    /// subscriber installed -- for example tokio worker threads owned by the
    /// `DefaultEngine`'s background runtime. If a no-subscriber thread is the first to
    /// hit such a callsite, tracing caches its `Interest` as `never` process-globally,
    /// silently disabling that metric for the rest of the process. The rebuild call
    /// re-evaluates every callsite against all currently installed dispatchers and
    /// unsticks the cache. [`tracing::dispatcher::set_global_default`] does this rebuild
    /// automatically and so does not need the manual call.
    ///
    /// [`tracing_subscriber::util::SubscriberInitExt::set_default`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/util/trait.SubscriberInitExt.html#method.set_default
    fn with_metrics_reporter_layer(
        self,
        reporter: Arc<dyn MetricsReporter>,
    ) -> Layered<ReportGeneratorLayer, Self>
    where
        Self: Sized,
    {
        self.with(ReportGeneratorLayer::new(reporter))
    }
}

impl<S> WithMetricsReporterLayer for S
where
    S: Subscriber,
    for<'lookup> S: LookupSpan<'lookup>,
{
}
