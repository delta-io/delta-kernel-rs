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

pub use events::{MetricEvent, MetricId};
pub use reporter::{LoggingMetricsReporter, MetricsReporter, ReportGeneratorLayer};
use tracing::Subscriber;
use tracing_subscriber::{
    layer::{Layered, SubscriberExt as _},
    registry::LookupSpan,
};

/// An extension trait to add a convenience method for adding a layer that will send reports to a
/// `MetricsReporter`
pub trait WithMetricsReporterLayer: Subscriber {
    /// Add a layer that will generate [`MetricEvent`]s. This is how you can register to get metric
    /// reports if you prefer to consume things that way. For example, to write reports out as logs:
    /// ```
    /// tracing_subscriber::registry()
    ///    .with_metrics_reporter_layer(
    ///        Arc::new(LoggingMetricsReporter::new(tracing::Level::INFO))
    ///    )
    /// ```
    fn with_metrics_reporter_layer(
        self,
        reporter: Arc<dyn MetricsReporter>,
    ) -> Layered<ReportGeneratorLayer, Self>
    where
        Self: Sized,
        for<'lookup> Self: LookupSpan<'lookup>,
    {
        self.with(ReportGeneratorLayer::new(reporter))
    }
}

impl<S> WithMetricsReporterLayer for S where S: Subscriber {}
