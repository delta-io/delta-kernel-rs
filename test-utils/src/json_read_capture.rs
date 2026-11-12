//! Captures the shared JSON handler's terminal read tracing events for I/O assertions.

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::util::SubscriberInitExt as _;
use url::Url;

use crate::ensure_metrics_compatible_global_subscriber;

/// Thread-local JSON read capture. Keep it alive until the observed iterators finish or drop.
/// Iterators retain their initiating subscriber, so reads consumed on other threads stay isolated.
pub struct JsonReadCapture {
    reads: Arc<Mutex<Vec<ReadEvent>>>,
    _guard: DefaultGuard,
}

impl JsonReadCapture {
    /// Installs a capture for JSON reads initiated on this thread. Returns an empty capture.
    pub fn new() -> Self {
        ensure_metrics_compatible_global_subscriber();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing_subscriber::registry()
            .with(ReadLayer(reads.clone()))
            .set_default();
        Self {
            reads,
            _guard: guard,
        }
    }

    /// Returns requested paths whose entire read iterator exhausted without errors.
    pub fn successful_paths(&self) -> Vec<Url> {
        self.paths_with_outcome("success")
    }

    /// Returns requested paths from reads that failed before or during iteration.
    pub fn failed_paths(&self) -> Vec<Url> {
        self.paths_with_outcome("failure")
    }

    /// Returns requested paths whose iterators dropped before success or failure was observed.
    pub fn dropped_paths(&self) -> Vec<Url> {
        self.paths_with_outcome("dropped")
    }

    /// Returns requested paths from all finished reads, including failures and early drops.
    pub fn requested_paths(&self) -> Vec<Url> {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .map(|read| read.path.clone())
            .collect()
    }

    /// Removes captured events. Call only after reads from the previous observation finish or drop.
    pub fn clear(&self) {
        self.reads.lock().unwrap().clear();
    }

    fn paths_with_outcome(&self, outcome: &str) -> Vec<Url> {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|read| read.outcome == outcome)
            .map(|read| read.path.clone())
            .collect()
    }
}

impl Default for JsonReadCapture {
    fn default() -> Self {
        Self::new()
    }
}

struct ReadEvent {
    path: Url,
    outcome: String,
}

struct ReadLayer(Arc<Mutex<Vec<ReadEvent>>>);

impl<S: Subscriber> Layer<S> for ReadLayer {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.name() == "json_read_finished"
            && metadata.target() == "delta_kernel::metrics::metered_json"
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut visitor = ReadVisitor::default();
        event.record(&mut visitor);
        if let (Some(path), Some(outcome)) = (visitor.path, visitor.outcome) {
            self.0.lock().unwrap().push(ReadEvent { path, outcome });
        }
    }
}

#[derive(Default)]
struct ReadVisitor {
    path: Option<Url>,
    outcome: Option<String>,
}

impl Visit for ReadVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "path" => self.path = Some(Url::parse(value).expect("JSON read event has a URL")),
            "outcome" => self.outcome = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
