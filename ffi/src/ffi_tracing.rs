//! FFI functions to allow engines to receive log and tracing events from kernel

use std::sync::{Arc, Mutex};
use std::{fmt, io};

use delta_kernel::{DeltaResult, Error};
use tracing::{
    field::{Field as TracingField, Visit},
    Event as TracingEvent, Subscriber,
};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{filter::LevelFilter, layer::Context, registry::LookupSpan, Layer};

use crate::{kernel_string_slice, KernelStringSlice};

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
    /// file where the event occurred. If unknown the slice `ptr` will be null and the len will be 0
    file: KernelStringSlice,
}

pub type TracingEventFn = extern "C" fn(event: Event);

/// Enable getting called back for tracing (logging) events in the kernel. `max_level` specifies
/// that only events `<=` to the specified level should be reported.  More verbose Levels are "greater
/// than" less verbose ones. So Level::ERROR is the lowest, and Level::TRACE the highest.
///
/// Note that setting up such a call back can only be done ONCE. Calling any of
/// `enable_event_tracing`, `enable_log_line_tracing`, or `enable_formatted_log_line_tracing` more
/// than once is a no-op.
///
/// Returns `true` if the callback was setup successfully, false on failure (i.e. if called a second
/// time)
///
/// [`event`] based tracing gives an engine maximal flexibility in formatting event log
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
    /// `{"timestamp":"2022-02-15T18:47:10.821315Z","level":"INFO","fields":{"message":"preparing to shave yaks","number_of_yaks":3},"target":"fmt_json"}`
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
/// Note that setting up such a call back can only be done ONCE. Calling any of
/// `enable_event_tracing`, `enable_log_line_tracing`, or `enable_formatted_log_line_tracing` more
/// than once is a no-op.
///
/// Returns `true` if the callback was setup successfully, false on failure (i.e. if called a second
/// time)
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
/// Note that setting up such a call back can only be done ONCE. Calling any of
/// `enable_event_tracing`, `enable_log_line_tracing`, or `enable_formatted_log_line_tracing` more
/// than once is a no-op.
///
/// Returns `true` if the callback was setup successfully, false on failure (i.e. if called a second
/// time)
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
            self.message = Some(format!("{:?}", value));
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

fn get_event_dispatcher(callback: TracingEventFn, max_level: Level) -> tracing_core::Dispatch {
    use tracing_subscriber::{layer::SubscriberExt, registry::Registry};
    let filter: LevelFilter = max_level.into();
    let event_layer = EventLayer { callback }.with_filter(filter);
    let subscriber = Registry::default().with(event_layer);
    tracing_core::Dispatch::new(subscriber)
}

fn setup_event_subscriber(callback: TracingEventFn, max_level: Level) -> DeltaResult<()> {
    if !max_level.is_valid() {
        return Err(Error::generic("max_level out of range"));
    }
    let dispatch = get_event_dispatcher(callback, max_level);
    set_global_default(dispatch)
}

// utility code below for setting up the tracing subscriber for log lines

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
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Could not lock buffer"))?
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

fn get_log_line_dispatch(
    callback: TracingLogLineFn,
    max_level: Level,
    format: LogLineFormat,
    ansi: bool,
    with_time: bool,
    with_level: bool,
    with_target: bool,
) -> tracing_core::Dispatch {
    use tracing_subscriber::{layer::SubscriberExt, registry::Registry};
    let buffer = Arc::new(Mutex::new(vec![]));
    let writer = BufferedMessageWriter {
        current_buffer: buffer.clone(),
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(ansi)
        .with_level(with_level)
        .with_target(with_target);
    let filter: LevelFilter = max_level.into();
    let tracking_layer = TriggerLayer {
        buf: buffer.clone(),
        callback,
    };

    // This repeats some code, but avoids some insane generic wrangling if we try to abstract the
    // type of `fmt_layer` over the formatter
    macro_rules! setup_subscriber {
        ($($transform:ident()).*) => {{
            let fmt_layer = fmt_layer$(.$transform())*.with_filter(filter);
            let subscriber = Registry::default()
                .with(fmt_layer)
                .with(tracking_layer.with_filter(filter));
            tracing_core::Dispatch::new(subscriber)
        }};
    }
    use LogLineFormat::*;
    match (format, with_time) {
        (FULL, true) => setup_subscriber!(),
        (FULL, false) => setup_subscriber!(without_time()),
        (COMPACT, true) => setup_subscriber!(compact()),
        (COMPACT, false) => setup_subscriber!(compact().without_time()),
        (PRETTY, true) => setup_subscriber!(pretty()),
        (PRETTY, false) => setup_subscriber!(pretty().without_time()),
        (JSON, true) => setup_subscriber!(json()),
        (JSON, false) => setup_subscriber!(json().without_time()),
    }
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
    if !max_level.is_valid() {
        return Err(Error::generic("max_level out of range"));
    }
    let dispatch = get_log_line_dispatch(
        callback,
        max_level,
        format,
        ansi,
        with_time,
        with_level,
        with_target,
    );
    set_global_default(dispatch)
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use tracing::info;
    use tracing_subscriber::fmt::time::FormatTime;

    use crate::TryFromStringSlice;

    use super::*;

    // Because we have to access a global messages buffer, we have to force tests to run one at a
    // time
    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    static MESSAGES: Mutex<Option<Vec<String>>> = Mutex::new(None);

    extern "C" fn record_callback(line: KernelStringSlice) {
        let s: &str = unsafe { TryFromStringSlice::try_from_slice(&line).unwrap() };
        let s = s.to_string();
        let mut lock = MESSAGES.lock().unwrap();
        if let Some(ref mut msgs) = *lock {
            msgs.push(s);
        }
    }

    fn setup_messages() {
        *MESSAGES.lock().unwrap() = Some(vec![]);
    }

    // get the string that we should ensure is in log messages for the time. If current time seconds
    // is >= 50, return None because the minute might roll over before we actually log which would
    // invalidate this check
    fn get_time_test_str() -> Option<String> {
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
        if tstr.len() < 19 {
            return None;
        }
        let secs: u32 = tstr[17..19].parse().expect("Failed to parse secs");
        if secs >= 50 {
            // risk of roll-over, don't check
            return None;
        }
        // Trim to just hours and minutes
        Some(tstr[..19].to_string())
    }

    // IMPORTANT: This is the only test that should call the actual `extern "C"` function, as we can
    // only call it once to set the global subscriber. Other tests ALL need to use
    // `get_X_dispatcher` and set it locally using `with_default`
    #[test]
    fn info_logs_with_log_line_tracing() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_messages();
        unsafe {
            enable_log_line_tracing(record_callback, Level::INFO);
        }
        let lines = ["Testing 1\n", "Another line\n"];
        let test_time_str = get_time_test_str();
        for line in lines {
            // remove final newline which will be added back by logging
            info!("{}", &line[..(line.len() - 1)]);
        }
        let lock = MESSAGES.lock().unwrap();
        if let Some(ref msgs) = *lock {
            assert_eq!(msgs.len(), lines.len());
            for (got, expect) in msgs.iter().zip(lines) {
                assert!(got.ends_with(expect));
                assert!(got.contains("INFO"));
                assert!(got.contains("delta_kernel_ffi::ffi_tracing::tests"));
                if let Some(ref tstr) = test_time_str {
                    assert!(got.contains(tstr));
                }
            }
        } else {
            panic!("Messages wasn't Some");
        }

        // ensure we can't setup again
        // do in the same test to ensure ordering
        let ok = unsafe {
            enable_formatted_log_line_tracing(
                record_callback,
                Level::TRACE,
                LogLineFormat::FULL,
                true, // ansi
                true, // with_time
                true, // with_level
                true, // with_target
            )
        };
        assert!(!ok, "Should have not set up a second time")
    }

    #[test]
    fn info_logs_with_formatted_log_line_tracing() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_messages();
        let dispatch = get_log_line_dispatch(
            record_callback,
            Level::INFO,
            LogLineFormat::COMPACT,
            false,
            true,
            false,
            false,
        );
        tracing_core::dispatcher::with_default(&dispatch, || {
            let lines = ["Testing 1\n", "Another line\n"];
            let test_time_str = get_time_test_str();
            for line in lines {
                // remove final newline which will be added back by logging
                info!("{}", &line[..(line.len() - 1)]);
            }
            let lock = MESSAGES.lock().unwrap();
            if let Some(ref msgs) = *lock {
                assert_eq!(msgs.len(), lines.len());
                for (got, expect) in msgs.iter().zip(lines) {
                    assert!(got.ends_with(expect));
                    assert!(!got.contains("INFO"));
                    assert!(!got.contains("delta_kernel_ffi::ffi_tracing::tests"));
                    if let Some(ref tstr) = test_time_str {
                        assert!(got.contains(tstr));
                    }
                }
            } else {
                panic!("Messages wasn't Some");
            }
        })
    }

    static EVENTS_OK: Mutex<Option<Vec<bool>>> = Mutex::new(None);
    fn setup_events() {
        *EVENTS_OK.lock().unwrap() = Some(vec![]);
    }

    extern "C" fn event_callback(event: Event) {
        let msg: &str = unsafe { TryFromStringSlice::try_from_slice(&event.message).unwrap() };
        let target: &str = unsafe { TryFromStringSlice::try_from_slice(&event.target).unwrap() };
        let file: &str = unsafe { TryFromStringSlice::try_from_slice(&event.file).unwrap() };

        // file path will use \ on windows
        use std::path::MAIN_SEPARATOR;
        let expected_file = format!("ffi{}src{}ffi_tracing.rs", MAIN_SEPARATOR, MAIN_SEPARATOR);

        let ok = event.level == Level::INFO
            && target == "delta_kernel_ffi::ffi_tracing::tests"
            && file == expected_file
            && (msg == "Testing 1" || msg == "Another line");
        let mut lock = EVENTS_OK.lock().unwrap();
        if let Some(ref mut events) = *lock {
            events.push(ok);
        }
    }

    #[test]
    fn trace_event_tracking() {
        let _lock = TEST_LOCK.lock().unwrap();
        setup_events();
        let dispatch = get_event_dispatcher(event_callback, Level::TRACE);
        tracing_core::dispatcher::with_default(&dispatch, || {
            let lines = ["Testing 1", "Another line"];
            for line in lines {
                info!("{line}");
            }
        });
        let lock = EVENTS_OK.lock().unwrap();
        if let Some(ref results) = *lock {
            assert!(results.iter().all(|x| *x));
        } else {
            panic!("Events wasn't Some");
        }
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
}
