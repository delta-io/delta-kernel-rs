# Kernel metrics

Kernel emits metrics as `tracing` spans. A connector installs `ReportGeneratorLayer`
(`reporter.rs`), which turns each span carrying the `report` field into a `MetricEvent`
(`events.rs`) and hands it to the connector's `MetricsReporter`. Kernel does no aggregation.

## Two mechanisms

The difference is *when a metric's values are known*:

- **Span-based** (`#[instrument]` on a function): values arrive over the function's life. Some at
  the start, some mid-call (`Span::current().record(...)`), the duration at the end.
- **Emit-based** (an `emit_*` helper): all values are known at once, so the helper fires a single
  `span!` carrying every field (including `duration_ns`) and drops it. Nothing arrives later.

`from_attrs` builds the event from a span's fields. It reads them with a `Visit` (the trait whose
`record_*` methods receive one field at a time). Emit events run one `Visit` over the whole struct;
span-based events reuse small per-field helpers like `MetricId::from_attrs`, each a `Visit` inside.

## Failure events

Operations with separate success and failure variants emit the success span, then fire `error`
on it (`#[instrument(err)]` does this on `Err`; an emit helper calls
`tracing::info!(error = ...)`). The layer sees `error` and flips the event via
`MetricEvent::into_failure`.

Snapshot-build failures receive elapsed time when the lifecycle span closes. Log-segment and
protocol/metadata failures preserve the operation duration passed to their emit helper; timing the
helper span itself would omit the failed work.

`CheckpointWriteSuccess` reports `Written` with statistics or `AlreadyExists` without statistics.
`CheckpointWriteFailure` retains the selected mode and duration without write statistics. Both durations
stop after checkpoint data writes and verification, before hint publication. Recording `duration_ns`
preserves that boundary when the reporting span closes after the final API result is known. Calls
that return before data completion use span-close timing.

Checkpoint discovery and inspection use completion reports with an explicit outcome. A
successful operation may find no checkpoint or sidecars. Their emit helpers supply the duration,
including failed work, without a success-to-failure conversion.

## Adding an event

1. Define the struct + `SPAN_NAME` in `events.rs`; add a `MetricEvent` variant and its
   `into_failure` arm.
2. For emit, add an `emit_*` helper and a `from_attrs`; register the span name in `on_new_span`
   (`reporter.rs`).
3. Make each label a strum enum (`serialize_all = "snake_case"`).

The FFI mirror (`ffi/src/ffi_metrics.rs`) and connectors destructure these structs, so adding a
field or variant is a breaking change.
