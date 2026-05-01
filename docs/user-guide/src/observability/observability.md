# Metrics and monitoring

To observe what Kernel is doing at runtime, install a `tracing-subscriber` layer
that converts Kernel's spans and events into `MetricEvent` values. Kernel
instruments snapshot loading, scanning, and storage I/O with the
[`tracing`](https://docs.rs/tracing) crate, so you can also receive raw spans
and events in any subscriber you already use for logging.

## How metrics flow

Kernel emits `tracing` spans at key milestones (snapshot build, log segment
load, scan metadata replay, storage I/O). A `ReportGeneratorLayer`, attached
to your subscriber, watches those spans and forwards `MetricEvent` values to a
`MetricsReporter` you provide. Your reporter can do whatever you want with
each event: print it, push it to Prometheus, increment a counter, or fan it
out to several destinations.

```text
Kernel code
    | tracing span / event
    v
tracing-subscriber Registry
    +-- ReportGeneratorLayer  --->  MetricsReporter (your impl)
    +-- fmt::layer (logs)
    +-- EnvFilter, etc.
```

This means metrics aren't tied to your `Engine`. You wire them up once, at
process startup, alongside any other tracing layers you want.

## Enabling metrics

To enable metrics, build a tracing subscriber, add the metrics layer with
`WithMetricsReporterLayer::with_metrics_reporter_layer`, and call `init()`.
Kernel ships `LoggingMetricsReporter` as a built-in reporter that logs each
event at a tracing level you choose.

Filename: src/main.rs

```rust,no_run
# extern crate delta_kernel;
# extern crate tracing;
# extern crate tracing_subscriber;
use std::sync::Arc;
use delta_kernel::metrics::{LoggingMetricsReporter, WithMetricsReporterLayer};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with_metrics_reporter_layer(
            Arc::new(LoggingMetricsReporter::new(tracing::Level::INFO)),
        )
        .init();

    // ... build engine and read tables as usual
}
```

If you don't install the layer, no `MetricEvent` values are produced. The
underlying spans still exist, so other tracing layers (logging, distributed
tracing) keep working.

> [!NOTE]
> The `MetricsReporter` trait, `WithMetricsReporterLayer` extension, and
> `LoggingMetricsReporter` live under `delta_kernel::metrics`. You also need
> the `tracing` and `tracing-subscriber` crates as direct dependencies of
> your connector.

## Implementing a custom MetricsReporter

The `MetricsReporter` trait has a single method:

```rust,ignore
pub trait MetricsReporter: Send + Sync + std::fmt::Debug {
    fn report(&self, event: MetricEvent);
}
```

Your reported must be `Send + Sync` because the layer can call `report` from any thread
that produces a span. Keep `report` cheap. If your destination is slow, push
the event onto a channel and drain it from a worker.

Filename: src/reporter.rs

```rust,ignore
use delta_kernel::metrics::{MetricEvent, MetricsReporter};

#[derive(Debug)]
struct StdoutReporter;

impl MetricsReporter for StdoutReporter {
    fn report(&self, event: MetricEvent) {
        println!("[kernel-metrics] {event}");
    }
}
```

Wire it in the same way as `LoggingMetricsReporter`:

```rust,ignore
use std::sync::Arc;
use delta_kernel::metrics::WithMetricsReporterLayer;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with_metrics_reporter_layer(Arc::new(StdoutReporter))
    .init();
```

When a snapshot loads, you'll see output like:

```text
[kernel-metrics] LogSegmentLoaded(id=a1b2c3d4-..., duration=12.34ms, commits=5, checkpoints=1, compactions=0)
[kernel-metrics] ProtocolMetadataLoaded(id=a1b2c3d4-..., duration=3.21ms)
[kernel-metrics] SnapshotCompleted(id=a1b2c3d4-..., version=5, duration=15.55ms)
```

## Metric events

Every callback receives a `MetricEvent` enum value. Events fall into three
categories: snapshot lifecycle, scan metadata, and storage/file I/O.

### Snapshot lifecycle events

These events track the process of loading a [Snapshot](../concepts/architecture.md)
from the Delta log. Each carries an `operation_id` (`MetricId`) that ties all
events from the same snapshot load together.

| Event | Fields | What it measures |
|-------|--------|------------------|
| `LogSegmentLoaded` | `operation_id`, `duration`, `num_commit_files`, `num_checkpoint_files`, `num_compaction_files` | Time to list and organize log files into a log segment. |
| `ProtocolMetadataLoaded` | `operation_id`, `duration` | Time to read protocol and metadata actions from the log. |
| `SnapshotCompleted` | `operation_id`, `version`, `total_duration` | End-to-end snapshot creation, including the table version that was loaded. |
| `SnapshotFailed` | `operation_id`, `duration` | Snapshot creation failed. Use this to track error rates. |

### Scan metadata events

`ScanMetadataCompleted` is emitted when a scan metadata iterator is fully
consumed. It provides detailed statistics about the log replay process:

| Field | Meaning |
|-------|---------|
| `operation_id` | Unique ID for this scan, useful for correlation. |
| `scan_type` | Which scan path produced the event (see below). |
| `total_duration` | Wall-clock time from scan start to iterator exhaustion. |
| `num_add_files_seen` | Add actions that entered deduplication. Excludes files already eliminated by data skipping. |
| `num_active_add_files` | Add files that survived log replay. These are the files your connector reads. |
| `num_remove_files_seen` | Remove actions encountered in commit files. |
| `num_non_file_actions` | Non-file actions (protocol, metadata, etc.) seen during replay. |
| `num_predicate_filtered` | Files eliminated by predicate evaluation (data skipping and partition pruning). |
| `peak_hash_set_size` | Peak size of the internal deduplication set. Indicates memory pressure during log replay. |
| `dedup_visitor_time_ms` | Milliseconds spent in the deduplication visitor. |
| `predicate_eval_time_ms` | Milliseconds spent evaluating predicates. |

#### The ScanType enum

The `scan_type` field tells you which scan execution path produced the event:

| Variant | Source |
|---------|--------|
| `ScanType::Full` | Produced by `Scan::scan_metadata()`. The entire log replay happened in one pass. |
| `ScanType::SequentialPhase` | The sequential phase of `Scan::parallel_scan_metadata()`. |
| `ScanType::ParallelPhase` | The parallel phase of `Scan::parallel_scan_metadata()`. |

If you use `parallel_scan_metadata`, you'll receive two `ScanMetadataCompleted`
events per scan: one for each phase.

### Storage and file I/O events

These events track low-level I/O operations. The default storage, JSON, and
Parquet handlers emit them automatically when the metrics layer is installed.
Unlike snapshot events, these don't carry an `operation_id` because a single
storage call may serve multiple higher-level operations.

| Event | Fields | What it measures |
|-------|--------|------------------|
| `StorageListCompleted` | `duration`, `num_files` | A storage list call (e.g., listing the `_delta_log` directory). |
| `StorageReadCompleted` | `duration`, `num_files`, `bytes_read` | A storage read call. `bytes_read` is the total on-disk size. |
| `StorageCopyCompleted` | `duration` | A storage copy/rename call. |
| `JsonReadCompleted` | `num_files`, `bytes_read` | One `JsonHandler::read_json_files` call completed. `bytes_read` is the sum of on-disk file sizes. |
| `ParquetReadCompleted` | `num_files`, `bytes_read` | One `ParquetHandler::read_parquet_files` call completed. `bytes_read` is the sum of on-disk file sizes. |

> [!NOTE]
> If you implement a custom `JsonHandler` or `ParquetHandler`, call
> `delta_kernel::metrics::emit_json_read_completed` or
> `emit_parquet_read_completed` once per read call so connectors that install
> the metrics layer still see those events from your handler.

## Correlating events with MetricId

Several events include an `operation_id` field of type `MetricId`. This is a
UUID that uniquely identifies an operation instance. All events from the same
snapshot load share the same `MetricId`, so you can group them to reconstruct
a timeline:

1. `LogSegmentLoaded` (how long listing took, how many files)
2. `ProtocolMetadataLoaded` (how long protocol/metadata parsing took)
3. `SnapshotCompleted` or `SnapshotFailed` (final outcome and total duration)

You can store the `MetricId` in your monitoring system as a trace ID or
correlation key. Because everything flows through `tracing`, you can also
correlate Kernel's events with your own application spans by attaching them
to the same parent span.

## Example: correlating snapshot events by operation ID

To aggregate timing data per operation, store intermediate events keyed by
`MetricId` and compute totals when the terminal event arrives:

Filename: src/correlating_reporter.rs

```rust,ignore
use std::collections::HashMap;
use std::sync::Mutex;
use delta_kernel::metrics::{MetricEvent, MetricId, MetricsReporter};

#[derive(Debug)]
struct CorrelatingReporter {
    pending: Mutex<HashMap<MetricId, Vec<MetricEvent>>>,
}

impl CorrelatingReporter {
    fn new() -> Self {
        Self { pending: Mutex::new(HashMap::new()) }
    }
}

impl MetricsReporter for CorrelatingReporter {
    fn report(&self, event: MetricEvent) {
        match &event {
            // 1. Buffer intermediate events, grouped by their shared operation_id.
            MetricEvent::LogSegmentLoaded { operation_id, .. }
            | MetricEvent::ProtocolMetadataLoaded { operation_id, .. } => {
                let mut map = self.pending.lock().unwrap();
                map.entry(*operation_id).or_default().push(event);
            }
            // 2. When the terminal event arrives, drain the group and
            //    compute aggregates (here, a count of sub-events).
            MetricEvent::SnapshotCompleted {
                operation_id,
                version,
                total_duration,
            } => {
                let mut map = self.pending.lock().unwrap();
                if let Some(events) = map.remove(operation_id) {
                    println!(
                        "Snapshot v{version} completed in {total_duration:?} ({} sub-events)",
                        events.len()
                    );
                }
            }
            // On failure, discard the buffered group for this operation.
            MetricEvent::SnapshotFailed { operation_id, .. } => {
                let mut map = self.pending.lock().unwrap();
                map.remove(operation_id);
                println!("Snapshot failed for operation {operation_id}");
            }
            // Storage and scan events don't participate in snapshot correlation.
            _ => {}
        }
    }
}
```

## Sending metrics to multiple destinations

To report to more than one system (for example, a logger and Prometheus),
create a composite reporter that fans out to multiple inner reporters:

Filename: src/composite_reporter.rs

```rust,ignore
use std::sync::Arc;
use delta_kernel::metrics::{MetricEvent, MetricsReporter};

#[derive(Debug)]
struct CompositeReporter {
    reporters: Vec<Arc<dyn MetricsReporter>>,
}

impl MetricsReporter for CompositeReporter {
    fn report(&self, event: MetricEvent) {
        for reporter in &self.reporters {
            reporter.report(event.clone());
        }
    }
}
```

`MetricEvent` implements `Clone`, so each inner reporter receives its own copy.
You only install one `ReportGeneratorLayer` on your subscriber. The composite
fans out from there.

## See also

- [`tracing`](https://docs.rs/tracing) and
  [`tracing-subscriber`](https://docs.rs/tracing-subscriber) crate
  documentation, for layer composition, filters, and other subscribers.
