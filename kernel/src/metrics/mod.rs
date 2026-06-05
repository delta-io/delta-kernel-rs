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
//! use delta_kernel::metrics::{MetricsReporter, MetricEvent};
//!
//! #[derive(Debug)]
//! struct LoggingReporter;
//!
//! impl MetricsReporter for LoggingReporter {
//!     fn report(&self, event: MetricEvent) {
//!         match event {
//!             MetricEvent::LogSegmentLoaded(e) => {
//!                 println!("Log segment loaded in {:?}: {} commits", e.duration, e.num_commit_files);
//!             }
//!             MetricEvent::SnapshotCompleted(e) => {
//!                 println!("Snapshot completed: v{} in {:?}", e.version, e.duration);
//!             }
//!             MetricEvent::SnapshotFailed(e) => {
//!                 println!("Snapshot failed: {} after {:?}", e.operation_id, e.duration);
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
//! # Handler Metrics
//!
//! Storage, JSON, and Parquet handler operations emit one event per read or copy
//! operation, fired when the returned iterator is exhausted or dropped:
//! `StorageListCompleted` / `StorageReadCompleted` / `StorageCopyCompleted` for storage,
//! `JsonReadCompleted` for `JsonHandler::read_json_files`, and `ParquetReadCompleted`
//! for `ParquetHandler::read_parquet_files`. `DefaultEngine` wraps its three handlers
//! in [`MeteredStorageHandler`], [`MeteredJsonHandler`], and [`MeteredParquetHandler`]
//! at construction; any other engine gets the same coverage by wrapping its [`Engine`]
//! in [`MeteredDeltaEngine`].
//!
//! These metrics are standalone and track aggregate handler performance without
//! correlating to specific Snapshot/Transaction operations.
//!
//! [`Engine`]: crate::Engine

pub(crate) mod events;
mod metered_engine;
mod metered_json;
mod metered_parquet;
mod metered_storage;
mod precounted_metrics_iterator;
pub(crate) mod reporter;
mod streaming_metrics_iterator;

use std::sync::Arc;

pub use events::{
    emit_json_read_completed, emit_parquet_read_completed, CrcReadCompleted, DomainMetadataLoaded,
    JsonReadCompleted, LogSegmentLoaded, MetricEvent, MetricId, ParquetReadCompleted,
    ProtocolMetadataLoaded, ScanMetadataCompleted, ScanType, SetTransactionLoaded,
    SnapshotCompleted, SnapshotFailed, StorageCopyCompleted, StorageListCompleted,
    StorageReadCompleted,
};
pub use metered_engine::MeteredDeltaEngine;
pub use metered_json::MeteredJsonHandler;
pub use metered_parquet::MeteredParquetHandler;
pub use metered_storage::MeteredStorageHandler;
pub(crate) use precounted_metrics_iterator::PrecountedMetricsIterator;
pub use reporter::{LoggingMetricsReporter, MetricsReporter, ReportGeneratorLayer};
pub(crate) use streaming_metrics_iterator::{emit_storage_span, MetricsIterator};
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
    /// # Important: keep a real global default subscriber installed
    ///
    /// Prefer installing this subscriber as the *global* default via
    /// [`tracing::dispatcher::set_global_default`] (or
    /// [`tracing_subscriber::util::SubscriberInitExt::init`]). The global path rebuilds
    /// the callsite interest cache internally and guarantees that every thread has a
    /// real subscriber to consult the first time it hits a callsite.
    ///
    /// If you need thread-local isolation -- e.g. per-test
    /// [`tracing_subscriber::util::SubscriberInitExt::set_default`] guards in a
    /// multi-threaded test binary -- also install a bare global default subscriber
    /// (e.g. `tracing_subscriber::registry().init()`) up front. Test code should
    /// prefer `test_utils::install_thread_local_metrics_reporter`, which bundles
    /// the global-subscriber install with the thread-local `set_default` so callers
    /// cannot accidentally skip the first step. Several kernel metrics are emitted
    /// from `Drop` impls (notably storage list/read completion in the default
    /// engine), and those `Drop` sites can run on threads that have no subscriber
    /// installed -- for example tokio worker threads owned by the `DefaultEngine`'s
    /// background runtime. If a no-subscriber thread is the first to hit such a
    /// callsite, tracing caches its `Interest` as `never` process-globally,
    /// silently disabling that metric for the rest of the process. Keeping a real
    /// global default active means every thread's "current dispatcher" is a real
    /// subscriber, so the cached interest stays in the `always`/`sometimes` regime.
    ///
    /// [`tracing::callsite::rebuild_interest_cache`] is *not* a reliable substitute on
    /// its own: it only re-evaluates callsites that are already registered, but
    /// callsites can be registered for the first time at any point on no-subscriber
    /// threads, re-poisoning the cache after the rebuild.
    ///
    /// [`tracing_subscriber::util::SubscriberInitExt::init`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/util/trait.SubscriberInitExt.html#method.init
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
