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

Success and failure are two variants of one event. Emit the success span, then fire `error` on it
(`#[instrument(err)]` does this on `Err`; an emit helper calls `tracing::info!(error = ...)`). The
layer sees `error` and flips the event via `MetricEvent::into_failure`.

## Adding an event

1. Define the struct + `SPAN_NAME` in `events.rs`; add a `MetricEvent` variant and its
   `into_failure` arm.
2. For emit, add an `emit_*` helper and a `from_attrs`; register the span name in `on_new_span`
   (`reporter.rs`).
3. Make each label a strum enum (`serialize_all = "snake_case"`).

The FFI mirror (`ffi/src/ffi_metrics.rs`) and connectors destructure these structs, so adding a
field or variant is a breaking change.
