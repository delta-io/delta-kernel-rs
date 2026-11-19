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

```rust
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
[kernel-metrics] LogSegmentLoadCompleted(id=a1b2c3d4-..., table_type=path_based, correlation_id=None, load_type=full, duration=12.34ms, outcome=Success { stats: LogSegmentLoadStats { num_commit_files: 5, num_checkpoint_files: 1, num_compaction_files: 0, crc_versions_behind: None } })
[kernel-metrics] ProtocolMetadataLoadCompleted(id=a1b2c3d4-..., table_type=path_based, correlation_id=None, load_type=full, duration=3.21ms, outcome=Success { source: FullReplay })
[kernel-metrics] SnapshotBuildCompleted(id=a1b2c3d4-..., table_type=path_based, correlation_id=None, load_type=full, duration=15.55ms, outcome=Success { version: 5 })
```

## Metric events

Every callback receives a `MetricEvent` enum value. Events fall into three
categories: snapshot lifecycle, scan metadata, and storage/file I/O.

### Snapshot lifecycle events

These events track the process of loading a [Snapshot](../concepts/architecture.md)
from the Delta log. Their `context.operation_id` (`MetricId`) ties all events from the same
snapshot load together.

| Event | What it measures |
|-------|------------------|
| `LogSegmentLoadCompleted` | Listing and organizing log files, including failed attempts. |
| `ProtocolMetadataLoadCompleted` | Resolving protocol and metadata from the log or a CRC. |
| `SnapshotBuildCompleted` | The complete snapshot build, including its final outcome. |

Each event carries `context` and `duration`. Match its operation-specific `outcome` to access
result fields, such as `SnapshotBuildOutcome::Success { version }`. Error outcomes carry no
successful-result fields. See the [metrics API](https://docs.rs/delta_kernel/latest/delta_kernel/metrics/)
for the event and outcome contracts.

### Scan metadata events

`ScanMetadataCompleted` reports completion of a full scan or one phase of a
parallel scan. It describes the corresponding log replay work:

| Field | Meaning |
|-------|---------|
| `context.operation_id` | Unique ID for this scan, useful for correlation. |
| `scan_type` | Which scan path produced the event (see below). |
| `duration` | Wall-clock time from scan start to iterator exhaustion. |
| `num_add_files_seen` | Add actions in replay input before predicate filtering and deduplication. Includes checkpoint and delta files. |
| `num_add_files_seen_from_delta_files` | Add actions in delta-file replay input before predicate filtering and deduplication. |
| `num_selected_add_files` | Add files that survived log replay. These are the files your connector reads. |
| `selected_add_files_bytes` | Total size in bytes of the selected Add files. |
| `num_remove_files_seen_from_delta_files` | Remove actions in delta-file replay input before deduplication. |
| `num_non_file_actions` | Non-file actions (protocol, metadata, etc.) seen during replay. |
| `num_predicate_filtered` | Files eliminated by predicate evaluation (data skipping and partition pruning). |
| `peak_hash_set_size` | Whole-scan high-water mark for the internal deduplication set. This value is retained across sequential and parallel phase events. |
| `dedup_visitor_time_ns` | Nanoseconds spent in the deduplication visitor. |
| `action_transform_time_ns` | Total nanoseconds spent transforming log actions into scan rows, including parsing JSON statistics into typed columns and retry attempts. Concurrent transforms contribute separately, so their total can exceed the wall-clock `duration`. |
| `predicate_eval_time_ns` | Nanoseconds spent evaluating predicates. |

#### The ScanType enum

The `scan_type` field tells you which scan execution path produced the event:

| Variant | Source |
|---------|--------|
| `ScanType::Full` | Produced by `Scan::scan_metadata()`. The entire log replay happened in one pass. |
| `ScanType::SequentialPhase` | The sequential phase of `Scan::parallel_scan_metadata()`. |
| `ScanType::ParallelPhase` | The parallel phase of `Scan::parallel_scan_metadata()`. |

For `parallel_scan_metadata`, `finish()` emits the sequential-phase event. If it
returns parallel work, call `state.log_metrics()` exactly once after all workers
complete to emit the parallel-phase event. Action counts and timings are phase-local;
`peak_hash_set_size` is the whole-scan high-water mark and should not be combined.
Join the two events by `context.operation_id` to obtain whole-scan metrics.

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
| `CrcReadCompleted` | `duration`, `outcome` | A CRC read and parse attempt. `CrcReadOutcome::Success` carries the raw `bytes_read`; errors normally allow log replay. |

> [!NOTE]
> If you implement a custom `JsonHandler` or `ParquetHandler`, call
> `delta_kernel::metrics::emit_json_read_completed` or
> `emit_parquet_read_completed` once per read call so connectors that install
> the metrics layer still see those events from your handler.

### Checkpoint hint reads

Use [LastCheckpointReadCompleted](https://docs.rs/delta_kernel/latest/delta_kernel/metrics/struct.LastCheckpointReadCompleted.html)
to count `_last_checkpoint` read attempts and record their duration by outcome. A missing or invalid
hint normally leads to log listing. A file-not-found error returned directly by storage propagates
instead of falling back, but still reports `NotFound`. Use snapshot events to measure snapshot
failures separately from hint availability. The event has no `operation_id`; see its rustdoc for
outcome and timing semantics.

### Checksum write events

To track checksum write latency and errors, use
[ChecksumWriteCompleted](https://docs.rs/delta_kernel/latest/delta_kernel/metrics/struct.ChecksumWriteCompleted.html)
and [ChecksumWriteOutcome](https://docs.rs/delta_kernel/latest/delta_kernel/metrics/enum.ChecksumWriteOutcome.html).
A checksum that already exists produces a success event. See
[Snapshot::write_checksum](https://docs.rs/delta_kernel/latest/delta_kernel/struct.Snapshot.html#method.write_checksum)
for the call's behavior.

## Correlating events with MetricId

Several events include a `context.operation_id` field of type `MetricId`. This is a
UUID that uniquely identifies an operation instance. All events from the same
snapshot load share the same `MetricId`, so you can group them to reconstruct
a timeline:

1. `LogSegmentLoadCompleted` (listing the log segment)
2. `ProtocolMetadataLoadCompleted` (reading protocol and metadata)
3. `SnapshotBuildCompleted` (final outcome and total duration)

Each step emits a completed event carrying the shared `context.operation_id`. When a
step fails, the snapshot build fails too. The terminal `SnapshotBuildCompleted` has an error
outcome and carries the same ID, so the buffered group shows which step failed.

You can store the `MetricId` in your monitoring system as a trace ID or
correlation key. Because everything flows through `tracing`, you can also
correlate Kernel's events with your own application spans by attaching them
to the same parent span.

## Example: correlating snapshot events by operation ID

To aggregate timing data per operation, store intermediate events keyed by
`MetricId` and compute totals when the terminal event arrives:

Filename: src/correlating_reporter.rs

```rust
use std::collections::HashMap;
use std::sync::Mutex;
use delta_kernel::metrics::{MetricEvent, MetricId, MetricsReporter, SnapshotBuildOutcome};

#[derive(Debug)]
struct CorrelatingReporter {
    pending: Mutex<HashMap<MetricId, Vec<MetricEvent>>>,
}

impl CorrelatingReporter {
    fn new() -> Self {
        Self { pending: Mutex::new(HashMap::new()) }
    }

    // Buffer an intermediate step under its operation_id.
    fn buffer(&self, operation_id: MetricId, event: MetricEvent) {
        self.pending.lock().unwrap().entry(operation_id).or_default().push(event);
    }

    // Drain the buffered steps for a finished operation.
    fn drain(&self, operation_id: MetricId) -> Vec<MetricEvent> {
        self.pending.lock().unwrap().remove(&operation_id).unwrap_or_default()
    }
}

impl MetricsReporter for CorrelatingReporter {
    fn report(&self, event: MetricEvent) {
        match event {
            // 1. Buffer each intermediate step, success OR failure, under its operation_id.
            //    `ref e` borrows the id so `event` can still move into the buffer.
            MetricEvent::LogSegmentLoadCompleted(ref e) => self.buffer(e.context.operation_id, event),
            MetricEvent::ProtocolMetadataLoadCompleted(ref e) => {
                self.buffer(e.context.operation_id, event)
            }

            // 2. The terminal event drains the group. The buffered steps reconstruct the
            //    timeline; on failure, the last step shows where the build broke.
            MetricEvent::SnapshotBuildCompleted(e) => {
                let steps = self.drain(e.context.operation_id);
                match e.outcome {
                    SnapshotBuildOutcome::Success { version } => {
                        println!(
                            "Snapshot v{} completed in {:?} ({} sub-events)",
                            version,
                            e.duration,
                            steps.len()
                        );
                    }
                    SnapshotBuildOutcome::Error => {
                        println!(
                            "Snapshot {} failed after {:?} ({} sub-events); last step: {:?}",
                            e.context.operation_id,
                            e.duration,
                            steps.len(),
                            steps.last()
                        );
                    }
                }
            }
            // Storage, scan, and post-build loads don't participate in snapshot correlation.
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
