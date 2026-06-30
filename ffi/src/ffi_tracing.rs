//! FFI functions to allow engines to receive log, tracing, and metrics events from kernel.
//!
//! We use a single global tracing subscriber, registered the first time any of
//! [`enable_event_tracing`], [`enable_log_line_tracing`], [`enable_formatted_log_line_tracing`], or
//! [`enable_metrics_reporting`] is called. The subscriber has two layers, one for log events, and
//! one for metric report events:
//!
//! - The logging layer is a type-erased [`Layer`] that an `enable_*_tracing` call swaps in (either
//!   event-based or formatted log-line)
//! - The metrics slot is a [`ReportGeneratorLayer`] that is `OFF` (zero overhead) until
//!   [`enable_metrics_reporting`] turns it on.
//!
//! Both are reloadable so they can be swapped.

use std::sync::{Arc, LazyLock, Mutex};
use std::{fmt, io};

use delta_kernel::metrics::{
    MetricEvent as KernelMetricEvent, MetricsReporter, ReportGeneratorLayer,
};
use delta_kernel::{DeltaResult, Error};
use tracing::field::{Field as TracingField, Visit};
use tracing::{error, Event as TracingEvent, Subscriber};
use tracing_core::Dispatch;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Identity, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{reload, Layer, Registry};

use crate::ffi_metrics::{with_ffi_event, MetricEvent};
use crate::{kernel_string_slice, KernelStringSlice};

/// A type-erased logging layer that lives in the reloadable logging slot of the global subscriber.
/// cbindgen:ignore
type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync>;
/// Reload handle for swapping the boxed logging layer.
/// cbindgen:ignore
type LayerHandle = reload::Handle<BoxedLayer, Registry>;
/// Reload handle for changing a slot's level filter.
/// cbindgen:ignore
type FilterHandle = reload::Handle<LevelFilter, Registry>;

/// Definitions of level verbosity. Verbose Levels are "greater than" less verbose ones. So
/// Level::ERROR is the lowest, and Level::TRACE the highest.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Level {
    ERROR = 0,
    WARN = 1,
    INFO = 2,
    DEBUG = 3,
    TRACE = 4,
}

impl Level {
    fn is_valid(self) -> bool {
        static VALID_VALUES: &[u32] = &[0, 1, 2, 3, 4];
        VALID_VALUES.contains(&(self as u32))
    }
}

impl From<&tracing::Level> for Level {
    fn from(value: &tracing::Level) -> Self {
        match *value {
            tracing::Level::TRACE => Level::TRACE,
            tracing::Level::DEBUG => Level::DEBUG,
            tracing::Level::INFO => Level::INFO,
            tracing::Level::WARN => Level::WARN,
            tracing::Level::ERROR => Level::ERROR,
        }
    }
}

impl From<Level> for LevelFilter {
    fn from(value: Level) -> Self {
        match value {
            Level::TRACE => LevelFilter::TRACE,
            Level::DEBUG => LevelFilter::DEBUG,
            Level::INFO => LevelFilter::INFO,
            Level::WARN => LevelFilter::WARN,
            Level::ERROR => LevelFilter::ERROR,
        }
    }
}

/// An `Event` can generally be thought of a "log message". It contains all the relevant bits such
/// that an engine can generate a log message in its format
#[repr(C)]
pub struct Event {
    /// The log message associated with the event
    message: KernelStringSlice,
    /// Level that the event was emitted at
    level: Level,
    /// A string that specifies in what part of the system the event occurred
    target: KernelStringSlice,
    /// source file line number where the event occurred, or 0 (zero) if unknown
    line: u32,
    /// file where the event occurred. If unknown the slice `ptr` will be null and the len will be
    /// 0
    file: KernelStringSlice,
}

pub type TracingEventFn = extern "C" fn(event: Event);

/// Enable getting called back for tracing (logging) events in the kernel. `max_level` specifies
/// that only events `<=` to the specified level should be reported.  More verbose Levels are
/// "greater than" less verbose ones. So Level::ERROR is the lowest, and Level::TRACE the highest.
///
/// Calling `enable_event_tracing`, `enable_log_line_tracing`, or
/// `enable_formatted_log_line_tracing` again replaces the active logging layer and its level,
/// including switching between event-based and log-line formats.
///
/// Returns `true` if the callback was setup successfully, `false` on failure.
///
/// Event-based tracing gives an engine maximal flexibility in formatting event log
/// lines. Kernel can also format events for the engine. If this is desired call
/// [`enable_log_line_tracing`] instead of this method.
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_event_tracing(callback: TracingEventFn, max_level: Level) -> bool {
    setup_event_subscriber(callback, max_level).is_ok()
}

pub type TracingLogLineFn = extern "C" fn(line: KernelStringSlice);

/// Format to use for log lines. These correspond to the formats from [`tracing_subscriber`
/// formats](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/format/index.html).
#[repr(C)]
pub enum LogLineFormat {
    /// The default formatter. This emits human-readable, single-line logs for each event that
    /// occurs, with the context displayed before the formatted representation of the event.
    /// Example:
    /// `2022-02-15T18:40:14.289898Z  INFO fmt: preparing to shave yaks number_of_yaks=3`
    FULL,
    /// A variant of the FULL formatter, optimized for short line lengths. Fields from the context
    /// are appended to the fields of the formatted event, and targets are not shown.
    /// Example:
    /// `2022-02-17T19:51:05.809287Z  INFO fmt_compact: preparing to shave yaks number_of_yaks=3`
    COMPACT,
    /// Emits excessively pretty, multi-line logs, optimized for human readability. This is
    /// primarily intended to be used in local development and debugging, or for command-line
    /// applications, where automated analysis and compact storage of logs is less of a priority
    /// than readability and visual appeal.
    /// Example:
    /// ```ignore
    ///   2022-02-15T18:44:24.535324Z  INFO fmt_pretty: preparing to shave yaks, number_of_yaks: 3
    ///   at examples/examples/fmt-pretty.rs:16 on main
    /// ```
    PRETTY,
    /// Outputs newline-delimited JSON logs. This is intended for production use with systems where
    /// structured logs are consumed as JSON by analysis and viewing tools. The JSON output is not
    /// optimized for human readability.
    /// Example:
    /// `{"timestamp":"2022-02-15T18:47:10.821315Z","level":"INFO","fields":{"message":"preparing
    /// to shave yaks","number_of_yaks":3},"target":"fmt_json"}`
    JSON,
}

/// Enable getting called back with log lines in the kernel using default settings:
/// - FULL format
/// - include ansi color
/// - include timestamps
/// - include level
/// - include target
///
/// `max_level` specifies that only logs `<=` to the specified level should be reported.  More
/// verbose Levels are "greater than" less verbose ones. So Level::ERROR is the lowest, and
/// Level::TRACE the highest.
///
/// Log lines passed to the callback will already have a newline at the end.
///
/// Calling `enable_event_tracing`, `enable_log_line_tracing`, or
/// `enable_formatted_log_line_tracing` again replaces the active logging layer and its level,
/// including switching between event-based and log-line formats.
///
/// Returns `true` if the callback was setup successfully, `false` on failure.
///
/// Log line based tracing is simple for an engine as it can just log the passed string, but does
/// not provide flexibility for an engine to format events. If the engine wants to use a specific
/// format for events it should call [`enable_event_tracing`] instead of this function.
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_log_line_tracing(
    callback: TracingLogLineFn,
    max_level: Level,
) -> bool {
    setup_log_line_subscriber(
        callback,
        max_level,
        LogLineFormat::FULL,
        true, /* ansi color on */
        true, /* time included */
        true, /* level included */
        true, /* target included */
    )
    .is_ok()
}

/// Enable getting called back with log lines in the kernel. This variant allows specifying
/// formatting options for the log lines. See [`enable_log_line_tracing`] for general info on
/// getting called back for log lines.
///
/// Calling `enable_event_tracing`, `enable_log_line_tracing`, or
/// `enable_formatted_log_line_tracing` again replaces the active logging layer and its level,
/// including switching between event-based and log-line formats.
///
/// Returns `true` if the callback was setup successfully, `false` on failure.
///
/// Options that can be set:
/// - `format`: see [`LogLineFormat`]
/// - `ansi`: should the formatter use ansi escapes for color
/// - `with_time`: should the formatter include a timestamp in the log message
/// - `with_level`: should the formatter include the level in the log message
/// - `with_target`: should the formatter include what part of the system the event occurred
///
/// # Safety
/// Caller must pass a valid function pointer for the callback
#[no_mangle]
pub unsafe extern "C" fn enable_formatted_log_line_tracing(
    callback: TracingLogLineFn,
    max_level: Level,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) -> bool {
    setup_log_line_subscriber(
        callback,
        max_level,
        format,
        ansi,
        with_time,
        with_level,
        with_target,
    )
    .is_ok()
}

// utility code below for setting up the tracing subscriber for events

fn set_global_default(dispatch: tracing_core::Dispatch) -> DeltaResult<()> {
    tracing_core::dispatcher::set_global_default(dispatch).map_err(|_| {
        Error::generic("Unable to set global default subscriber. Trying to set more than once?")
    })
}

struct MessageFieldVisitor {
    message: Option<String>,
}

impl Visit for MessageFieldVisitor {
    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &TracingField, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }
}

struct EventLayer {
    callback: TracingEventFn,
}

impl<S> Layer<S> for EventLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &TracingEvent<'_>, _context: Context<'_, S>) {
        // it would be tempting to `impl TryFrom` to convert the `TracingEvent` into an `Event`, but
        // we want to use a KernelStringSlice, so we need the extracted string to live long enough
        // for the callback which won't happen if we convert inside a `try_from` call
        let metadata = event.metadata();
        let target = metadata.target();
        let mut message_visitor = MessageFieldVisitor { message: None };
        event.record(&mut message_visitor);
        if let Some(message) = message_visitor.message {
            // we ignore events without a message
            let file = metadata.file().unwrap_or("");
            let event = Event {
                message: kernel_string_slice!(message),
                level: metadata.level().into(),
                target: kernel_string_slice!(target),
                line: metadata.line().unwrap_or(0),
                file: kernel_string_slice!(file),
            };
            (self.callback)(event);
        }
    }
}

type SharedBuffer = Arc<Mutex<Vec<u8>>>;

struct TriggerLayer {
    buf: SharedBuffer,
    callback: TracingLogLineFn,
}

impl<S> Layer<S> for TriggerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, _: &TracingEvent<'_>, _context: Context<'_, S>) {
        match self.buf.lock() {
            Ok(mut buf) => {
                let message = String::from_utf8_lossy(&buf);
                let message = kernel_string_slice!(message);
                (self.callback)(message);
                buf.clear();
            }
            Err(_) => {
                let message = "INTERNAL KERNEL ERROR: Could not lock message buffer.";
                let message = kernel_string_slice!(message);
                (self.callback)(message);
            }
        }
    }
}

#[derive(Default)]
struct BufferedMessageWriter {
    current_buffer: SharedBuffer,
}

impl io::Write for BufferedMessageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.current_buffer
            .lock()
            .map_err(|_| io::Error::other("Could not lock buffer"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufferedMessageWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        BufferedMessageWriter {
            current_buffer: self.current_buffer.clone(),
        }
    }
}

/// Callback an engine registers via [`enable_metrics_reporting`] to receive kernel
/// [`MetricEvent`]s. This is invoked synchronously on the thread that emitted the event. Note that
/// the `event`, and any [`KernelStringSlice`] it carries, are only valid for the duration of the
/// call.
pub type MetricsEventFn = extern "C" fn(event: MetricEvent);

/// A [`MetricsReporter`] that forwards each kernel [`MetricEvent`] to a registered FFI callback
#[derive(Debug)]
struct FfiMetricsReporter {
    callback: Arc<Mutex<Option<MetricsEventFn>>>,
}

impl MetricsReporter for FfiMetricsReporter {
    fn report(&self, event: KernelMetricEvent) {
        let callback = match self.callback.lock() {
            Ok(guard) => *guard,
            Err(_) => {
                error!("Failed to lock metrics callback (mutex poisoned).");
                return;
            }
        };
        if let Some(callback) = callback {
            with_ffi_event(&event, |ffi_event| callback(ffi_event));
        }
    }
}

fn build_event_layer(callback: TracingEventFn) -> BoxedLayer {
    Box::new(EventLayer { callback })
}

fn build_log_line_layer(
    callback: TracingLogLineFn,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) -> BoxedLayer {
    let buffer: SharedBuffer = Arc::new(Mutex::new(vec![]));
    let writer = BufferedMessageWriter {
        current_buffer: buffer.clone(),
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(ansi)
        .with_level(with_level)
        .with_target(with_target);
    let trigger = TriggerLayer {
        buf: buffer,
        callback,
    };

    // The fmt layer formats the line into the shared buffer; the trigger layer then flushes it to
    // the callback. fmt must run before trigger, so it must be the first item in the vec.
    macro_rules! boxed_log_line {
        ($($transform:ident()).*) => {{
            let fmt: BoxedLayer = Box::new(fmt_layer $(.$transform())*);
            let trigger: BoxedLayer = Box::new(trigger);
            Box::new(vec![fmt, trigger]) as BoxedLayer
        }};
    }

    use LogLineFormat::*;
    match (format, with_time) {
        (FULL, true) => boxed_log_line!(),
        (FULL, false) => boxed_log_line!(without_time()),
        (COMPACT, true) => boxed_log_line!(compact()),
        (COMPACT, false) => boxed_log_line!(compact().without_time()),
        (PRETTY, true) => boxed_log_line!(pretty()),
        (PRETTY, false) => boxed_log_line!(pretty().without_time()),
        (JSON, true) => boxed_log_line!(json()),
        (JSON, false) => boxed_log_line!(json().without_time()),
    }
}

struct GlobalTracingState {
    installed: bool,
    /// Swaps the active logging layer (event-based vs log-line).
    logging_layer: Option<LayerHandle>,
    /// Sets the level filter gating the logging layer.
    logging_filter: Option<FilterHandle>,
    /// Toggles the metrics layer on (`TRACE`) or off (`OFF`).
    metrics_filter: Option<FilterHandle>,
    /// Shared callback the metrics layer's reporter forwards events to.
    metrics_callback: Arc<Mutex<Option<MetricsEventFn>>>,
}

impl GlobalTracingState {
    fn uninitialized() -> Self {
        GlobalTracingState {
            installed: false,
            logging_layer: None,
            logging_filter: None,
            metrics_filter: None,
            metrics_callback: Arc::new(Mutex::new(None)),
        }
    }

    /// If the global subscriber hasn't been installed yet, this installs it and sets up all the
    /// logging layers. When called again, this is a no-op.
    fn ensure_installed(&mut self) -> DeltaResult<()> {
        if self.installed {
            return Ok(());
        }

        let noop: BoxedLayer = Box::new(Identity::new());
        let (logging_slot, logging_layer) = reload::Layer::new(noop);
        let (logging_filter_layer, logging_filter) = reload::Layer::new(LevelFilter::OFF);
        let logging: BoxedLayer = Box::new(logging_slot.with_filter(logging_filter_layer));

        let (metrics_filter_layer, metrics_filter) = reload::Layer::new(LevelFilter::OFF);
        let reporter = Arc::new(FfiMetricsReporter {
            callback: self.metrics_callback.clone(),
        });
        let metrics: BoxedLayer =
            Box::new(ReportGeneratorLayer::new(reporter).with_filter(metrics_filter_layer));

        let subscriber = Registry::default().with(vec![logging, metrics]);
        set_global_default(Dispatch::new(subscriber))?;

        self.logging_layer = Some(logging_layer);
        self.logging_filter = Some(logging_filter);
        self.metrics_filter = Some(metrics_filter);
        self.installed = true;
        Ok(())
    }

    /// Make `layer` the active logging layer and set its level filter.
    fn reload_logging(&self, layer: BoxedLayer, max_level: Level) -> DeltaResult<()> {
        let reload_err = |e| Error::generic(format!("Unable to reload logging subscriber: {e}"));
        self.logging_layer
            .as_ref()
            .ok_or_else(|| Error::generic("logging slot not installed"))?
            .reload(layer)
            .map_err(reload_err)?;
        self.logging_filter
            .as_ref()
            .ok_or_else(|| Error::generic("logging filter not installed"))?
            .reload(LevelFilter::from(max_level))
            .map_err(reload_err)
    }

    fn register_event_callback(
        &mut self,
        callback: TracingEventFn,
        max_level: Level,
    ) -> DeltaResult<()> {
        if !max_level.is_valid() {
            return Err(Error::generic("max_level out of range"));
        }
        self.ensure_installed()?;
        self.reload_logging(build_event_layer(callback), max_level)
    }

    #[allow(clippy::too_many_arguments)]
    fn register_log_line_callback(
        &mut self,
        callback: TracingLogLineFn,
        max_level: Level,
        format: LogLineFormat,
        ansi: bool,
        with_time: bool,
        with_level: bool,
        with_target: bool,
    ) -> DeltaResult<()> {
        if !max_level.is_valid() {
            return Err(Error::generic("max_level out of range"));
        }
        self.ensure_installed()?;
        let layer =
            build_log_line_layer(callback, format, ansi, with_time, with_level, with_target);
        self.reload_logging(layer, max_level)
    }

    /// Set the metrics callback and turn the metrics slot's filter on. Metric spans are emitted at
    /// `INFO`.
    fn register_metrics_callback(&mut self, callback: MetricsEventFn) -> DeltaResult<()> {
        self.ensure_installed()?;
        *self
            .metrics_callback
            .lock()
            .map_err(|_| Error::generic("Failed to lock metrics callback (mutex poisoned)."))? =
            Some(callback);
        self.metrics_filter
            .as_ref()
            .ok_or_else(|| Error::generic("metrics filter not installed"))?
            .reload(LevelFilter::INFO)
            .map_err(|e| Error::generic(format!("Unable to reload metrics subscriber: {e}")))
    }
}

static TRACING_STATE: LazyLock<Mutex<GlobalTracingState>> =
    LazyLock::new(|| Mutex::new(GlobalTracingState::uninitialized()));

fn setup_event_subscriber(callback: TracingEventFn, max_level: Level) -> DeltaResult<()> {
    let mut state = TRACING_STATE
        .lock()
        .map_err(|_e| Error::generic("Poisoned mutex while setting up event subscriber"))?;
    state.register_event_callback(callback, max_level)
}

fn setup_log_line_subscriber(
    callback: TracingLogLineFn,
    max_level: Level,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) -> DeltaResult<()> {
    let mut state = TRACING_STATE
        .lock()
        .map_err(|_e| Error::generic("Poisoned mutex while setting up log_line_subscriber"))?;
    state.register_log_line_callback(
        callback,
        max_level,
        format,
        ansi,
        with_time,
        with_level,
        with_target,
    )
}

/// Enable getting called back with structured kernel metric events. `callback` receives a
/// [`MetricEvent`] each time kernel emits a report. (See the [`delta_kernel::metrics`] module).
///
/// Calling this replaces any previously set callback.
///
/// Returns `true` if reporting was enabled successfully, `false` on failure.
///
/// # Safety
/// Caller must pass a valid function pointer for the callback.
#[no_mangle]
pub unsafe extern "C" fn enable_metrics_reporting(callback: MetricsEventFn) -> bool {
    setup_metrics_reporter(callback).is_ok()
}

fn setup_metrics_reporter(callback: MetricsEventFn) -> DeltaResult<()> {
    let mut state = TRACING_STATE
        .lock()
        .map_err(|_e| Error::generic("Poisoned mutex while setting up metrics reporter"))?;
    state.register_metrics_callback(callback)
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use tracing::{debug, info, trace};
    use tracing_subscriber::fmt::time::FormatTime;

    use super::*;
    use crate::TryFromStringSlice;

    // Because we have to access a global messages buffer, we have to force tests to run one at a
    // time
    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    static MESSAGES: Mutex<Option<Vec<String>>> = Mutex::new(None);

    // Local dispatch builders used with `with_default` so tests can exercise each layer in
    // isolation without consuming the once-only global default subscriber.
    fn create_event_dispatch(callback: TracingEventFn, max_level: Level) -> Dispatch {
        let layer = build_event_layer(callback).with_filter(LevelFilter::from(max_level));
        Dispatch::new(Registry::default().with(layer))
    }

    #[allow(clippy::too_many_arguments)]
    fn create_log_line_dispatch(
        callback: TracingLogLineFn,
        max_level: Level,
        format: LogLineFormat,
        ansi: bool,
        with_time: bool,
        with_level: bool,
        with_target: bool,
    ) -> Dispatch {
        let layer =
            build_log_line_layer(callback, format, ansi, with_time, with_level, with_target)
                .with_filter(LevelFilter::from(max_level));
        Dispatch::new(Registry::default().with(layer))
    }

    fn create_metrics_dispatch(callback: MetricsEventFn) -> Dispatch {
        let reporter = Arc::new(FfiMetricsReporter {
            callback: Arc::new(Mutex::new(Some(callback))),
        });
        let layer = ReportGeneratorLayer::new(reporter).with_filter(LevelFilter::TRACE);
        Dispatch::new(Registry::default().with(layer))
    }

    fn record_callback_with_filter(line: KernelStringSlice, expected_log_lines: Vec<&str>) {
        let line_str: &str = unsafe { TryFromStringSlice::try_from_slice(&line).unwrap() };
        let line_str = line_str.to_string();
        let ok = expected_log_lines.is_empty()
            || expected_log_lines
                .iter()
                .any(|expected_log_line| line_str.ends_with(expected_log_line));
        if ok {
            let mut lock = MESSAGES.lock().unwrap();
            if let Some(ref mut msgs) = *lock {
                msgs.push(line_str);
            }
        }
    }

    // Note: record callbacks must be extern "C". Thus we cannot construct test callback closures in
    // runtime.
    extern "C" fn record_callback_with_filter_1(line: KernelStringSlice) {
        record_callback_with_filter(line, vec!["Testing 1\n", "Another line\n"])
    }

    extern "C" fn record_callback_with_filter_2(line: KernelStringSlice) {
        record_callback_with_filter(line, vec!["Testing 2\n", "Yet another line\n"])
    }

    fn setup_messages() {
        *MESSAGES.lock().unwrap() = Some(vec![]);
    }

    /// Format the current time as a string using the same formatter that tracing uses, trimmed
    /// to hours, minutes, and seconds (e.g. `"2026-03-10T14:32:45"`). This is used by tests to
    /// verify that log output contains a reasonable timestamp.
    ///
    /// Must be called both before and after logging to bracket the log's timestamp -- a context
    /// switch between capturing and logging can cross a second boundary, so the test asserts
    /// that at least one of the two captured timestamps appears in the output.
    fn get_time_test_str() -> String {
        #[derive(Default)]
        struct W {
            s: String,
        }
        impl fmt::Write for W {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.s.push_str(s);
                Ok(())
            }
        }

        let mut w = W::default();
        let mut writer = tracing_subscriber::fmt::format::Writer::new(&mut w);
        let now = tracing_subscriber::fmt::time::SystemTime;
        now.format_time(&mut writer).unwrap();
        let tstr = w.s;
        assert!(tstr.len() >= 19, "Unexpected time format: {tstr:?}");
        // Trim to hours, minutes, and seconds
        tstr[..19].to_string()
    }

    /// Check that logged messages match the expected lines, level, and timestamp.
    ///
    /// Timestamps are captured before and after logging to handle second-boundary races: a
    /// context switch between capturing the time and emitting the log line can cause the
    /// seconds to differ. By bracketing with a range check (`time_before <= log_time <=
    /// time_after`), we tolerate any amount of delay between capture and log emission. This
    /// works because ISO 8601 timestamps sort lexicographically.
    fn check_messages(
        expected_lines: Vec<&str>,
        time_before: &str,
        time_after: &str,
        expected_level_str: &str,
    ) {
        let lock = MESSAGES.lock().unwrap();
        let Some(ref msgs) = *lock else {
            panic!("Messages wasn't Some");
        };
        assert_eq!(msgs.len(), expected_lines.len());
        for (got, expect) in msgs.iter().zip(expected_lines) {
            assert!(got.ends_with(expect));
            assert!(got.contains(expected_level_str));
            assert!(got.contains("delta_kernel_ffi::ffi_tracing::tests"));
            assert_timestamp_in_range(got, time_before, time_after);
        }
    }

    /// Assert that the log line contains a timestamp within [time_before, time_after].
    ///
    /// Log lines may contain ANSI escape codes before the timestamp, so we locate the
    /// timestamp by searching for the `time_before` prefix (up to the seconds). Once found,
    /// we extract the full timestamp and do a lexicographic range check. ISO 8601 timestamps
    /// are fixed-width and sort lexicographically.
    fn assert_timestamp_in_range(log_line: &str, time_before: &str, time_after: &str) {
        let len = time_before.len();
        // Search for the date+hour+minute prefix (first 16 chars, e.g. "2026-03-10T14:32")
        // to locate the timestamp start. We use 16 chars (excluding seconds) because the
        // seconds may differ between time_before and the log line.
        let prefix = &time_before[..16];
        let ts_start = log_line
            .find(prefix)
            .unwrap_or_else(|| panic!("No timestamp found in log line: {log_line:?}"));
        let log_time = &log_line[ts_start..ts_start + len];
        assert!(
            log_time >= time_before && log_time <= time_after,
            "Timestamp {log_time:?} not in expected range [{time_before:?}, {time_after:?}], \
             full log line: {log_line:?}"
        );
    }

    // IMPORTANT: This is the only test that should call the actual `extern "C"` function, as we can
    // only call it once to set the global subscriber. Other tests ALL need to use
    // `get_X_dispatcher` and set it locally using `with_default`
    #[test]
    fn test_enable_log_line_tracing() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_messages();
        unsafe {
            // record_callback_with_filter_1 filters only "Testing 1\n", "Another line\n"
            enable_log_line_tracing(record_callback_with_filter_1, Level::INFO);
        }
        let lines = [
            "Testing 1\n",
            "Another line\n",
            "Testing 2\n",
            "Yet another line\n",
        ];
        // We registered record_callback_with_filter_1, which filters only the first two lines.
        let expected_lines = vec!["Testing 1\n", "Another line\n"];
        let time_before = get_time_test_str();
        for line in &lines {
            // Remove final newline which will be added back by logging
            info!("{}", &line[..(line.len() - 1)]);
        }
        let time_after = get_time_test_str();

        check_messages(expected_lines, &time_before, &time_after, "INFO");
        setup_messages();

        // Ensure we can setup again with a new callback and a new tracing level
        let ok = unsafe { enable_log_line_tracing(record_callback_with_filter_2, Level::DEBUG) };
        assert!(ok, "Failed to set up second time");

        // Ensure both callback and tracing level are reloaded.
        // We registered record_callback_with_filter_2, which filters the other logging lines.
        let expected_lines = vec!["Testing 2\n", "Yet another line\n"];
        let time_before = get_time_test_str();
        for line in &lines {
            debug!("{}", &line[..(line.len() - 1)]);
            // Trace must not be visible in messages, because we changed level to debug
            trace!("{}", &line[..(line.len() - 1)]);
        }
        let time_after = get_time_test_str();
        check_messages(expected_lines, &time_before, &time_after, "DEBUG");
    }

    #[test]
    fn info_logs_with_formatted_log_line_tracing() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_messages();
        let dispatch = create_log_line_dispatch(
            record_callback_with_filter_1,
            Level::INFO,
            LogLineFormat::COMPACT,
            false,
            true,
            false,
            false,
        );
        tracing_core::dispatcher::with_default(&dispatch, || {
            let lines = ["Testing 1\n", "Another line\n"];
            let time_before = get_time_test_str();
            for line in lines {
                // remove final newline which will be added back by logging
                info!("{}", &line[..(line.len() - 1)]);
            }
            let time_after = get_time_test_str();
            let lock = MESSAGES.lock().unwrap();
            if let Some(ref msgs) = *lock {
                assert_eq!(msgs.len(), lines.len());
                for (got, expect) in msgs.iter().zip(lines) {
                    assert!(got.ends_with(expect));
                    assert!(!got.contains("INFO"));
                    assert!(!got.contains("delta_kernel_ffi::ffi_tracing::tests"));
                    assert_timestamp_in_range(got, &time_before, &time_after);
                }
            } else {
                panic!("Messages wasn't Some");
            }
        })
    }

    static EVENTS_OK: Mutex<Option<Vec<(String, tracing::Level)>>> = Mutex::new(None);
    fn setup_events() {
        *EVENTS_OK.lock().unwrap() = Some(vec![]);
    }

    fn events_to_string(events: Vec<(String, tracing::Level)>) -> String {
        let events_str = events
            .iter()
            .map(|(s, lvl)| format!("{s}:{lvl}"))
            .collect::<Vec<_>>()
            .join(", ");
        events_str
    }

    fn convert_level(level: Level) -> tracing::Level {
        match level {
            Level::ERROR => tracing::Level::ERROR,
            Level::WARN => tracing::Level::WARN,
            Level::INFO => tracing::Level::INFO,
            Level::DEBUG => tracing::Level::DEBUG,
            Level::TRACE => tracing::Level::TRACE,
        }
    }

    fn event_callback_with_filter(event: Event, expected_log_lines: Vec<&str>) {
        let msg: &str = unsafe { TryFromStringSlice::try_from_slice(&event.message).unwrap() };
        let target: &str = unsafe { TryFromStringSlice::try_from_slice(&event.target).unwrap() };
        let file: &str = unsafe { TryFromStringSlice::try_from_slice(&event.file).unwrap() };

        // file path will use \ on windows
        use std::path::MAIN_SEPARATOR;
        let expected_file = format!("ffi{MAIN_SEPARATOR}src{MAIN_SEPARATOR}ffi_tracing.rs");

        let ok = target == "delta_kernel_ffi::ffi_tracing::tests"
            && file == expected_file
            && expected_log_lines.contains(&msg);
        if ok {
            let mut lock = EVENTS_OK.lock().unwrap();
            if let Some(ref mut events) = *lock {
                events.push((msg.to_string(), convert_level(event.level)));
            }
        }
    }

    extern "C" fn event_callback_with_filter_1(event: Event) {
        event_callback_with_filter(event, vec!["Testing 1", "Another line"])
    }

    extern "C" fn event_callback_with_filter_2(event: Event) {
        event_callback_with_filter(event, vec!["Testing 2", "Yet another line"])
    }

    fn check_events(expected_level: tracing::Level, expected_messages: Vec<&str>) {
        let lock = EVENTS_OK.lock().unwrap();
        if let Some(ref results) = *lock {
            assert!(!results.is_empty(), "No events were captured");

            assert!(
                results.iter().all(|(_msg, lvl)| *lvl == expected_level),
                "Not all events were {expected_level}"
            );
            let events_str = events_to_string(results.to_vec());
            assert!(
                results
                    .iter()
                    .all(|(msg, _lvl)| expected_messages.contains(&msg.as_str())),
                "Not all messages have expected format: {events_str}"
            )
        } else {
            panic!("Events wasn't Some");
        }
    }

    #[test]
    fn trace_event_tracking() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_events();
        let dispatch = create_event_dispatch(event_callback_with_filter_1, Level::TRACE);
        tracing_core::dispatcher::with_default(&dispatch, || {
            let lines = ["Testing 1", "Another line"];
            for line in lines {
                info!("{line}");
            }
        });
        check_events(tracing::Level::INFO, vec!["Testing 1", "Another line"]);
    }

    #[test]
    #[ignore] // We cannot run this test if test_enable_log_line_tracing was run before - see comment there,
              // however this test works if run individually.
    fn test_enable_event_tracing() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_events();
        unsafe {
            // Filters only "Testing 1", "Another line"
            enable_event_tracing(event_callback_with_filter_1, Level::INFO);
        }
        let lines = ["Testing 1", "Another line", "Testing 2", "Yet another line"];
        // We registered record_callback_with_filter_1, which filters the first two logging lines
        let expected_lines = vec!["Testing 1", "Another line"];
        for line in &lines {
            info!("{}", &line);
        }

        check_events(tracing::Level::INFO, expected_lines);
        setup_events();
        assert!(EVENTS_OK
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|v| v.is_empty()));

        // Ensure we can setup again with a new callback and a new tracing level
        unsafe {
            enable_event_tracing(event_callback_with_filter_2, Level::DEBUG);
        };

        // Ensure both callback and tracing level are reloaded.
        // We registered record_callback_with_filter_2, which filters the other logging lines
        let expected_lines = vec!["Testing 2", "Yet another line"];
        for line in &lines {
            debug!("{}", &line);
            // trace must not be visible in messages, because we changed level to debug
            trace!("{}", &line);
        }
        check_events(tracing::Level::DEBUG, expected_lines);
    }

    #[test]
    fn level_from_impl() {
        let trace: Level = (&tracing::Level::TRACE).into();
        assert_eq!(trace, Level::TRACE);
        let debug: Level = (&tracing::Level::DEBUG).into();
        assert_eq!(debug, Level::DEBUG);
        let info: Level = (&tracing::Level::INFO).into();
        assert_eq!(info, Level::INFO);
        let warn: Level = (&tracing::Level::WARN).into();
        assert_eq!(warn, Level::WARN);
        let error: Level = (&tracing::Level::ERROR).into();
        assert_eq!(error, Level::ERROR);
    }

    static METRIC_EVENTS: Mutex<Option<Vec<String>>> = Mutex::new(None);

    extern "C" fn capture_metric_event(event: MetricEvent) {
        let desc = match event {
            MetricEvent::JsonReadCompleted(e) => format!("json:{}:{}", e.num_files, e.bytes_read),
            MetricEvent::ParquetReadCompleted(e) => {
                format!("parquet:{}:{}", e.num_files, e.bytes_read)
            }
            _ => "other".to_string(),
        };
        if let Ok(mut lock) = METRIC_EVENTS.lock() {
            if let Some(events) = lock.as_mut() {
                events.push(desc);
            }
        }
    }

    #[test]
    fn metrics_reporting_delivers_structured_events() {
        let _lock = TEST_LOCK.lock().unwrap();
        *METRIC_EVENTS.lock().unwrap() = Some(vec![]);
        let dispatch = create_metrics_dispatch(capture_metric_event);
        tracing_core::dispatcher::with_default(&dispatch, || {
            delta_kernel::metrics::emit_json_read_completed(3, 100);
            delta_kernel::metrics::emit_parquet_read_completed(2, 50);
        });
        let lock = METRIC_EVENTS.lock().unwrap();
        assert_eq!(
            lock.as_deref(),
            Some(["json:3:100".to_string(), "parquet:2:50".to_string()].as_slice())
        );
    }
}
