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
span-based events decode shared labels through `MetricContext::from_attrs`.

## Completion outcomes

Each operation emits one `*Completed` event. Attempt-wide fields stay on the event, while its
operation-specific outcome carries only the fields applicable to that result. `MetricContext` owns
operation ID, caller correlation ID, and table type wherever those labels travel together.

Lifecycle spans start with a success payload. `#[instrument(err)]` marks the outcome as an error
without reconstructing the event or copying shared fields. An explicit commit failure reason
survives the generic error signal. Fields recorded after an error must not restore success.

Emit-based log-segment and protocol/metadata reports carry their outcome and duration in creation
attributes. Decode the outcome after other creation attributes so attribute order cannot discard
shared fields. The helper span's lifetime does not measure the operation's work.

`LastCheckpointReadCompleted` records its outcome explicitly. The error channel fills an unrecorded
outcome with `Error` without overwriting an explicit classification such as `NotFound`.

Checkpoint processing and output statistics describe attempted work across outcomes. Completed
checkpoint contents belong to `Written`. Preserve an explicitly recorded checkpoint duration:
its boundary excludes hint publication and returned snapshot construction. Other lifecycle events
receive elapsed time when their span closes, including errors.

## Warnings inside layer callbacks

Tracing layer callbacks must not emit tracing events directly. Calling `warn!()` or
any tracing macro inside a `tracing_subscriber::Layer` callback (`on_event`, `on_record`,
`on_close`) while holding a span's `extensions_mut()` write lock will re-enter the layer
and deadlock on the same lock. `on_new_span` builds its event with `from_attrs` before taking
the extension lock, so direct `warn!()` is safe there. In `on_record`, store warnings in a
`pending_warnings: Vec<String>` field on the visitor, take them out after the extensions
block closes, and emit via `warn!()` only then. (`on_event`'s visitor does no
warning-eligible work, so it may run under the lock directly.) See
`reporter.rs` for the canonical pattern.

## Adding an event

1. Define the struct + `SPAN_NAME` in `events.rs`; add a `MetricEvent` variant and its
   error-outcome arm.
2. For emit, add an `emit_*` helper and a `from_attrs`; register the span name in `on_new_span`
   (`reporter.rs`).
3. Make each label a strum enum (`serialize_all = "snake_case"`).

The FFI mirror (`ffi/src/ffi_metrics.rs`) and connectors destructure these structs, so adding a
field or variant is a breaking change.
