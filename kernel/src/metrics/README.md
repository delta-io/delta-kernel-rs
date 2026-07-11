# Kernel metrics

Kernel emits metrics as `tracing` spans. A connector installs [`ReportGeneratorLayer`]
(`reporter.rs`), which turns each span into a [`MetricEvent`] (`events.rs`) and hands it to the
connector's [`MetricsReporter`]. Kernel does no aggregation; the connector does.

## Terms

- **Span-based (instrument-based)**: the span *is* a function. `#[instrument]` opens it on entry
  and closes it on return. The event fires because the function ran; its duration is the function's
  wall-clock. Data fields arrive via the runtime-record channel during the call.
- **Emit-based**: the span is a one-shot marker fired by an `emit_*` helper at an arbitrary point,
  then dropped (never entered). The event fires because you called `emit_*`; all fields, including
  `duration_ns`, are passed as creation attrs. There is no function boundary and no runtime-record.
- An event type is **fully emit-based** when every one of its instances is produced by `emit_*` (so
  it needs no `record_*` / `set_duration` methods), and **span-based** when produced by
  `#[instrument]`. An event filled by *both* mechanisms (some call sites instrument, others emit) is
  **hybrid**: it must support both channels, which is more machinery; avoid unless a call site
  genuinely can't use the other.

## How a span becomes an event

Every metric span carries a `report` field (the sentinel the layer keys on) plus its data fields.
In `on_new_span` the layer matches the span name to its event and builds it via that event's
`from_attrs` (storage events share one span name, dispatched by `storage_metric_from_attrs`), then
reports it in `on_close`. Two channels fill an event's fields:

- **Creation attrs**: `span!(NAME, field = value, ...)`. Read once, at construction, by a
  `Visit`or in `from_attrs`. All fields must be known up front.
- **Runtime record**: `Span::current().record("field", value)` while the span is open. Routed to
  the event's `record_*` methods. For fields not known until mid-span.

Duration: the layer stamps it in `on_close` from the time the span was **entered**, but only if it
was entered. A span that is created and dropped without `.enter()` has no duration unless you pass
one as a `duration_ns` creation attr.

## Which mechanism to use

| You want to... | Use | Why |
|---|---|---|
| Time one function, event fires when it runs | `#[instrument(name = SPAN, fields(report, ...))]` | The macro enters/exits the fn; duration = fn wall-clock. Fields via `record()` during the call. |
| Fire an event from a point that is **not** a whole function (a match arm, a convergence of several branches) | an `emit_*` helper that fires a one-shot `span!` with all fields as creation attrs | No function boundary to annotate; the branches are the point. See `emit_protocol_metadata_load`. |
| Fire the **same** event from **several** call sites | one `emit_*` helper, called from each | One event definition, many callers. Instrument can't span two functions. |
| Record a diagnostic log line, not a metric | `info!` / `warn!` / `debug!` (no `report` field) | The layer ignores spans/events without `report`; these stay pure logging. |
| Add a field to an open instrumented span | `Span::current().record("field", v)` + a `record_*` arm on the event | Runtime channel; only meaningful on an entered (`#[instrument]`) span. |

## Failure events

Success and failure are two variants of one event. Emit the **success**-named span, then signal
failure by firing `error` on it: `#[instrument(err)]` does this automatically on `Err`; an emit
helper does it by entering the span and calling `tracing::info!(error = ...)`. The layer's
`on_event` sees `error` and flips the built event via `MetricEvent::into_failure`. A bare creation
attr does **not** flip it; the `error` event is what triggers the flip (see
`emit_protocol_metadata_load_failure`).

## Adding an event

1. Define the struct + `SPAN_NAME` in `events.rs`; add a `MetricEvent` variant and its
   `into_failure` mapping (the match is exhaustive, so a missing arm fails to compile).
2. Pick a mechanism from the table. For emit, add an `emit_*` helper and a `from_attrs` `Visit`or.
3. Add the span-name arm to `on_new_span` in `reporter.rs`.
4. Keep every label a typed enum with the strum derives (`EnumString`, `Display`, `AsRefStr`,
   `IntoStaticStr`, `serialize_all = "snake_case"`) so the wire string and the parse stay in sync.

The FFI mirror (`ffi/src/ffi_metrics.rs`) and connector consumers destructure these structs, so
adding a field or variant is a breaking change for them.

[`ReportGeneratorLayer`]: reporter.rs
[`MetricEvent`]: events.rs
[`MetricsReporter`]: reporter.rs
