//! Reports for checkpoint hint reads, discovery, inspection, and sidecar reads.
//!
//! These are standalone operation reports, like handler metrics. They describe work that was
//! requested by the read path; enabling them does not trigger checkpoint I/O. A successful
//! discovery can find no checkpoint, and a successful inspection can find no sidecar references.
//! Operation and caller-supplied correlation IDs are present only when the calling path has
//! that context. Standalone operations leave them absent.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use strum::{AsRefStr, Display, EnumString, IntoStaticStr};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;

use super::events::{correlation_id_from_attrs, record_operation_id};
use super::{MetricId, SnapshotLoadMetricContext};
use crate::{FileDataReadResultIterator, KernelResult};

#[derive(Clone, Default)]
pub(crate) struct CheckpointReadContext {
    pub(crate) operation_id: Option<MetricId>,
    pub(crate) correlation_id: Option<Arc<str>>,
}

impl From<&SnapshotLoadMetricContext> for CheckpointReadContext {
    fn from(context: &SnapshotLoadMetricContext) -> Self {
        Self {
            operation_id: Some(context.operation_id),
            correlation_id: context.correlation_id.clone(),
        }
    }
}

/// Direction used to discover a checkpoint in the transaction log.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointDiscoveryMode {
    /// A forward listing from the supplied lower bound.
    Forward,
    /// Backward listing windows ending at the supplied upper bound.
    Backward,
    /// The report did not provide a recognized mode.
    #[default]
    Unknown,
}

/// Whether checkpoint discovery or inspection returned successfully.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointReadOutcome {
    /// The operation returned successfully, including an empty result.
    Success,
    /// The operation returned an error; counters describe work completed before the error.
    #[default]
    Error,
}

/// How a top-level checkpoint's sidecar references were inspected.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointInspectionMode {
    /// Read until the first batch containing a sidecar, or until the input ended.
    SidecarProbe,
    /// Read and convert all sidecar references.
    SidecarEnumeration,
    /// The report did not provide a recognized mode.
    #[default]
    Unknown,
}

/// Physical format of the top-level checkpoint inspected by an operation.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointReadFormat {
    /// JSON checkpoint.
    Json,
    /// Parquet checkpoint.
    Parquet,
    /// The report did not provide a recognized format.
    #[default]
    Unknown,
}

/// One forward listing or backward checkpoint search completed, before snapshot-segment validation.
///
/// Emitted only when listing is allowed to adopt checkpoints. The result is the latest complete
/// checkpoint selected in the searched range, not the `_last_checkpoint` hint. Failure counters
/// include partially consumed listings. Catalog-provided log-tail entries are not counted.
#[derive(Debug, Clone, Default)]
pub struct CheckpointDiscoveryCompleted {
    /// Existing enclosing operation, absent for standalone work.
    pub operation_id: Option<MetricId>,
    /// Caller-supplied correlation value, when available.
    pub correlation_id: Option<Arc<str>>,
    /// Direction of the search.
    pub mode: CheckpointDiscoveryMode,
    /// Whether listing and checkpoint selection completed successfully.
    pub outcome: CheckpointReadOutcome,
    /// Inclusive lowest version searched, including the last attempted backward window on failure.
    pub start_version: u64,
    /// Inclusive upper bound, or no upper bound for a latest-version forward listing.
    pub end_version: Option<u64>,
    /// Number of listing requests attempted, including a request that returned an error.
    pub num_list_calls: u64,
    /// Parsed, listable log entries consumed inside the searched bounds. This excludes paths
    /// discarded by log-path parsing and does not count the out-of-range lookahead entry.
    pub num_files_examined: u64,
    /// Selected checkpoint version on success; absent when none was found or the search failed.
    pub checkpoint_version: Option<u64>,
    /// Elapsed wall-clock time, including failed listing work.
    pub duration: Duration,
}

impl CheckpointDiscoveryCompleted {
    pub(crate) const SPAN_NAME: &'static str = "checkpoint_discovery_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut fields = CheckpointReadAttrs::default();
        attrs.record(&mut fields);
        Self {
            operation_id: fields.operation_id,
            correlation_id: correlation_id_from_attrs(attrs),
            mode: parse_label(&fields.mode),
            outcome: parse_label(&fields.outcome),
            start_version: fields.start_version,
            end_version: fields.has_end_version.then_some(fields.end_version),
            num_list_calls: fields.num_list_calls,
            num_files_examined: fields.num_files_examined,
            checkpoint_version: fields
                .has_checkpoint_version
                .then_some(fields.checkpoint_version),
            duration: Duration::from_nanos(fields.duration_ns),
        }
    }

    pub(crate) fn emit(&self) {
        tracing::span!(
            tracing::Level::INFO,
            "checkpoint_discovery_completed",
            report = tracing::field::Empty,
            operation_id = self.operation_id.as_ref().map(tracing::field::display),
            correlation_id = self.correlation_id.as_deref().unwrap_or(""),
            mode = self.mode.as_ref(),
            outcome = self.outcome.as_ref(),
            start_version = self.start_version,
            has_end_version = self.end_version.is_some(),
            end_version = self.end_version.unwrap_or(0),
            num_list_calls = self.num_list_calls,
            num_files_examined = self.num_files_examined,
            has_checkpoint_version = self.checkpoint_version.is_some(),
            checkpoint_version = self.checkpoint_version.unwrap_or(0),
            duration_ns = self.duration.as_nanos() as u64,
        );
    }
}

impl fmt::Display for CheckpointDiscoveryCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Inspection of sidecar references in one top-level checkpoint completed.
///
/// A probe stops after its first batch containing a sidecar. Its observed count can exceed one
/// because a batch can contain several references; it is not the checkpoint's total sidecar count.
/// Enumeration succeeds only after consuming and converting every reference. Neither mode reads
/// sidecar contents. Cached hints and footer-only classification do not emit this report.
#[derive(Debug, Clone, Default)]
pub struct CheckpointInspectionCompleted {
    /// Existing enclosing operation, absent for standalone work.
    pub operation_id: Option<MetricId>,
    /// Caller-supplied correlation value, when available.
    pub correlation_id: Option<Arc<str>>,
    /// Probe or complete enumeration of sidecar references.
    pub mode: CheckpointInspectionMode,
    /// Physical format of the top-level checkpoint.
    pub format: CheckpointReadFormat,
    /// Whether the inspection returned successfully.
    pub outcome: CheckpointReadOutcome,
    /// Version of the selected checkpoint being inspected.
    pub checkpoint_version: u64,
    /// Top-level checkpoint's size from file metadata, not measured I/O bytes.
    pub checkpoint_size_bytes: u64,
    /// References decoded before returning, including partial progress on failure.
    pub num_sidecars_observed: u64,
    /// Elapsed wall-clock time through the returned result, including failed work.
    pub duration: Duration,
}

impl CheckpointInspectionCompleted {
    pub(crate) const SPAN_NAME: &'static str = "checkpoint_inspection_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut fields = CheckpointReadAttrs::default();
        attrs.record(&mut fields);
        Self {
            operation_id: fields.operation_id,
            correlation_id: correlation_id_from_attrs(attrs),
            mode: parse_label(&fields.mode),
            format: parse_label(&fields.format),
            outcome: parse_label(&fields.outcome),
            checkpoint_version: fields.checkpoint_version,
            checkpoint_size_bytes: fields.checkpoint_size_bytes,
            num_sidecars_observed: fields.num_sidecars_observed,
            duration: Duration::from_nanos(fields.duration_ns),
        }
    }

    pub(crate) fn emit(&self) {
        tracing::span!(
            tracing::Level::INFO,
            "checkpoint_inspection_completed",
            report = tracing::field::Empty,
            operation_id = self.operation_id.as_ref().map(tracing::field::display),
            correlation_id = self.correlation_id.as_deref().unwrap_or(""),
            mode = self.mode.as_ref(),
            format = self.format.as_ref(),
            outcome = self.outcome.as_ref(),
            checkpoint_version = self.checkpoint_version,
            checkpoint_size_bytes = self.checkpoint_size_bytes,
            num_sidecars_observed = self.num_sidecars_observed,
            duration_ns = self.duration.as_nanos() as u64,
        );
    }
}

impl fmt::Display for CheckpointInspectionCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Result of reading and decoding one `_last_checkpoint` hint.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointHintReadOutcome {
    /// A valid hint was returned; later checkpoint selection can still ignore it.
    Parsed,
    /// The storage iterator reported that the hint file does not exist; fallback is allowed.
    Missing,
    /// The storage iterator returned no item; fallback is allowed.
    Empty,
    /// Decoding or hint-field validation failed; the hint was ignored and fallback is allowed.
    Invalid,
    /// The attempt returned an error to its caller, rather than allowing fallback.
    #[default]
    Error,
}

/// One attempt to read and decode `_last_checkpoint`, including recoverable hint rejection.
///
/// Emitted once per requested hint read, without additional I/O. Missing, empty, and invalid hints
/// permit log discovery to continue; they do not describe that discovery's result. A parsed hint
/// may still be ignored when it is newer than the requested version or describes another
/// checkpoint. Snapshot and log-segment reports describe the enclosing operation's result.
#[derive(Debug, Clone, Default)]
pub struct CheckpointHintReadCompleted {
    /// Existing enclosing operation, absent for standalone work.
    pub operation_id: Option<MetricId>,
    /// Caller-supplied correlation value, when available.
    pub correlation_id: Option<Arc<str>>,
    /// Whether a hint was returned, ignored, or caused the attempt to fail.
    pub outcome: CheckpointHintReadOutcome,
    /// Hint version only when a valid hint was returned.
    pub checkpoint_version: Option<u64>,
    /// Wall-clock time through the read and decoding result, including failed work.
    pub duration: Duration,
}

impl CheckpointHintReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = "checkpoint_hint_read_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut fields = CheckpointReadAttrs::default();
        attrs.record(&mut fields);
        Self {
            operation_id: fields.operation_id,
            correlation_id: correlation_id_from_attrs(attrs),
            outcome: parse_label(&fields.outcome),
            checkpoint_version: fields
                .has_checkpoint_version
                .then_some(fields.checkpoint_version),
            duration: Duration::from_nanos(fields.duration_ns),
        }
    }

    pub(crate) fn emit(&self) {
        tracing::span!(
            tracing::Level::INFO,
            "checkpoint_hint_read_completed",
            report = tracing::field::Empty,
            operation_id = self.operation_id.as_ref().map(tracing::field::display),
            correlation_id = self.correlation_id.as_deref().unwrap_or(""),
            outcome = self.outcome.as_ref(),
            has_checkpoint_version = self.checkpoint_version.is_some(),
            checkpoint_version = self.checkpoint_version.unwrap_or(0),
            duration_ns = self.duration.as_nanos() as u64,
        );
    }
}

impl fmt::Display for CheckpointHintReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// The sidecar read boundary measured by a report.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointSidecarReadMode {
    /// Read one sidecar's footer to obtain its schema.
    Footer,
    /// Request and consume sidecar action batches through a Parquet handler.
    Data,
    /// The report did not provide a recognized mode.
    #[default]
    Unknown,
}

/// Result observed at a sidecar read boundary.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointSidecarReadOutcome {
    /// A footer was returned or a data iterator reached its end without an observed error.
    Success,
    /// The handler call or data iterator returned an error.
    #[default]
    Error,
    /// The data iterator was dropped before reaching its end or observing an error.
    Incomplete,
}

/// One requested read of sidecar contents, separate from inspecting sidecar references.
///
/// Footer reads emit once on return. Data reads emit once on handler failure, the first iterator
/// error, exhaustion, or early drop. Early drop does not imply successful completion. Reports do
/// not drain iterators, retry reads, count bytes, or replace generic handler read reports. Reads
/// executed by a connector's declarative plan executor are outside this handler boundary.
#[derive(Debug, Clone, Default)]
pub struct CheckpointSidecarReadCompleted {
    /// Existing enclosing operation, absent for standalone work.
    pub operation_id: Option<MetricId>,
    /// Caller-supplied correlation value, when available.
    pub correlation_id: Option<Arc<str>>,
    /// Footer or action-batch read.
    pub mode: CheckpointSidecarReadMode,
    /// Result actually observed by the caller-facing reader.
    pub outcome: CheckpointSidecarReadOutcome,
    /// Version of the checkpoint whose sidecar was requested.
    pub checkpoint_version: u64,
    /// Files passed to this request, not files successfully read or measured I/O.
    pub num_files_requested: u64,
    /// Footer wall-clock time, or accumulated data-handler creation and iterator `next` time.
    /// Data duration excludes time spent waiting between calls by the consumer.
    pub duration: Duration,
}

impl CheckpointSidecarReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = "checkpoint_sidecar_read_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut fields = CheckpointReadAttrs::default();
        attrs.record(&mut fields);
        Self {
            operation_id: fields.operation_id,
            correlation_id: correlation_id_from_attrs(attrs),
            mode: parse_label(&fields.mode),
            outcome: parse_label(&fields.outcome),
            checkpoint_version: fields.checkpoint_version,
            num_files_requested: fields.num_files_requested,
            duration: Duration::from_nanos(fields.duration_ns),
        }
    }

    pub(crate) fn emit(&self) {
        tracing::span!(
            tracing::Level::INFO,
            "checkpoint_sidecar_read_completed",
            report = tracing::field::Empty,
            operation_id = self.operation_id.as_ref().map(tracing::field::display),
            correlation_id = self.correlation_id.as_deref().unwrap_or(""),
            mode = self.mode.as_ref(),
            outcome = self.outcome.as_ref(),
            checkpoint_version = self.checkpoint_version,
            num_files_requested = self.num_files_requested,
            duration_ns = self.duration.as_nanos() as u64,
        );
    }

    pub(crate) fn meter_data_read(
        mut self,
        read: impl FnOnce() -> KernelResult<FileDataReadResultIterator>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let start = Instant::now();
        let result = read();
        self.duration += start.elapsed();
        match result {
            Ok(inner) => Ok(Box::new(SidecarReadIterator {
                inner,
                report: Some(self),
            })),
            Err(error) => {
                self.outcome = CheckpointSidecarReadOutcome::Error;
                self.emit();
                Err(error)
            }
        }
    }
}

impl fmt::Display for CheckpointSidecarReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

struct SidecarReadIterator {
    inner: FileDataReadResultIterator,
    report: Option<CheckpointSidecarReadCompleted>,
}

impl Iterator for SidecarReadIterator {
    type Item = <FileDataReadResultIterator as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Instant::now();
        let item = self.inner.next();
        if let Some(report) = &mut self.report {
            report.duration += start.elapsed();
        }
        let outcome = match &item {
            Some(Ok(_)) => return item,
            Some(Err(_)) => CheckpointSidecarReadOutcome::Error,
            None => CheckpointSidecarReadOutcome::Success,
        };
        if let Some(mut report) = self.report.take() {
            report.outcome = outcome;
            report.emit();
        }
        item
    }
}

impl Drop for SidecarReadIterator {
    fn drop(&mut self) {
        if let Some(mut report) = self.report.take() {
            report.outcome = CheckpointSidecarReadOutcome::Incomplete;
            report.emit();
        }
    }
}

#[derive(Default)]
struct CheckpointReadAttrs {
    operation_id: Option<MetricId>,
    mode: String,
    format: String,
    outcome: String,
    start_version: u64,
    end_version: u64,
    has_end_version: bool,
    num_files_requested: u64,
    num_list_calls: u64,
    num_files_examined: u64,
    checkpoint_version: u64,
    has_checkpoint_version: bool,
    checkpoint_size_bytes: u64,
    num_sidecars_observed: u64,
    duration_ns: u64,
}

impl Visit for CheckpointReadAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "start_version" => self.start_version = value,
            "end_version" => self.end_version = value,
            "num_files_requested" => self.num_files_requested = value,
            "num_list_calls" => self.num_list_calls = value,
            "num_files_examined" => self.num_files_examined = value,
            "checkpoint_version" => self.checkpoint_version = value,
            "checkpoint_size_bytes" => self.checkpoint_size_bytes = value,
            "num_sidecars_observed" => self.num_sidecars_observed = value,
            "duration_ns" => self.duration_ns = value,
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "has_end_version" => self.has_end_version = value,
            "has_checkpoint_version" => self.has_checkpoint_version = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "mode" => self.mode = value.to_owned(),
            "format" => self.format = value.to_owned(),
            "outcome" => self.outcome = value.to_owned(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if let Some(id) = record_operation_id(field, value, "checkpoint_read") {
            self.operation_id = Some(id);
        }
    }
}

fn parse_label<T: Default + std::str::FromStr>(value: &str) -> T {
    value.parse().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::KernelError;
    use crate::metrics::MetricEvent;
    use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};

    #[rstest::rstest]
    fn completion_spans_preserve_optional_versions_and_recorded_duration(
        #[values(None, Some(0), Some(u64::MAX))] version: Option<u64>,
        #[values(0, 17)] duration_ns: u64,
        #[values(false, true)] has_context: bool,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let operation_id = has_context.then(MetricId::new);
        let correlation_id = has_context.then(|| Arc::<str>::from("caller-operation"));
        CheckpointDiscoveryCompleted {
            operation_id,
            correlation_id: correlation_id.clone(),
            mode: CheckpointDiscoveryMode::Backward,
            outcome: CheckpointReadOutcome::Success,
            end_version: version,
            checkpoint_version: version,
            duration: Duration::from_nanos(duration_ns),
            ..Default::default()
        }
        .emit();
        CheckpointInspectionCompleted {
            operation_id,
            correlation_id: correlation_id.clone(),
            mode: CheckpointInspectionMode::SidecarProbe,
            format: CheckpointReadFormat::Parquet,
            outcome: CheckpointReadOutcome::Error,
            checkpoint_version: version.unwrap_or(0),
            duration: Duration::from_nanos(duration_ns),
            ..Default::default()
        }
        .emit();
        let events = reporter.events();
        let [
            MetricEvent::CheckpointDiscoveryCompleted(discovery),
            MetricEvent::CheckpointInspectionCompleted(inspection),
        ] = events.as_slice()
        else {
            panic!("expected exactly one of each checkpoint report");
        };
        assert_eq!(discovery.operation_id, operation_id);
        assert_eq!(discovery.correlation_id, correlation_id);
        assert_eq!(inspection.operation_id, operation_id);
        assert_eq!(inspection.correlation_id, correlation_id);
        assert_eq!(discovery.mode, CheckpointDiscoveryMode::Backward);
        assert_eq!(discovery.outcome, CheckpointReadOutcome::Success);
        assert_eq!(discovery.end_version, version);
        assert_eq!(discovery.checkpoint_version, version);
        assert_eq!(discovery.duration, Duration::from_nanos(duration_ns));
        assert_eq!(inspection.mode, CheckpointInspectionMode::SidecarProbe);
        assert_eq!(inspection.format, CheckpointReadFormat::Parquet);
        assert_eq!(inspection.outcome, CheckpointReadOutcome::Error);
        assert_eq!(inspection.checkpoint_version, version.unwrap_or(0));
        assert_eq!(inspection.duration, Duration::from_nanos(duration_ns));
    }
    #[rstest::rstest]
    #[case::handler_error(true, false, true, CheckpointSidecarReadOutcome::Error, 0)]
    #[case::iterator_error(false, false, true, CheckpointSidecarReadOutcome::Error, 1)]
    #[case::exhausted(false, false, false, CheckpointSidecarReadOutcome::Success, 1)]
    #[case::dropped(false, true, true, CheckpointSidecarReadOutcome::Incomplete, 0)]
    fn sidecar_read_emits_once_without_draining(
        #[case] call_error: bool,
        #[case] drop_early: bool,
        #[case] iterator_error: bool,
        #[case] expected: CheckpointSidecarReadOutcome,
        #[case] expected_pulls: usize,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let pulls = Arc::new(AtomicUsize::new(0));
        let operation_id = MetricId::new();
        let report = CheckpointSidecarReadCompleted {
            operation_id: Some(operation_id),
            correlation_id: Some("caller-read".into()),
            mode: CheckpointSidecarReadMode::Data,
            checkpoint_version: 0,
            num_files_requested: 3,
            ..Default::default()
        };
        let result = report.meter_data_read(|| {
            if call_error {
                return Err(KernelError::Cancelled);
            }
            let pulls = pulls.clone();
            Ok(Box::new(std::iter::from_fn(move || {
                pulls.fetch_add(1, Ordering::Relaxed);
                iterator_error.then_some(Err(KernelError::Cancelled))
            })))
        });
        if call_error {
            assert!(matches!(result, Err(KernelError::Cancelled)));
        } else {
            let mut iter = result.unwrap();
            if !drop_early {
                match iter.next() {
                    Some(result) => assert!(matches!(result, Err(KernelError::Cancelled))),
                    None => assert!(!iterator_error),
                }
            }
            drop(iter);
        }
        assert_eq!(pulls.load(Ordering::Relaxed), expected_pulls);
        let events = reporter.events();
        let [MetricEvent::CheckpointSidecarReadCompleted(event)] = events.as_slice() else {
            panic!("expected one sidecar report");
        };
        assert_eq!(event.outcome, expected);
        assert_eq!(event.mode, CheckpointSidecarReadMode::Data);
        assert_eq!(event.checkpoint_version, 0);
        assert_eq!(event.num_files_requested, 3);
        assert_eq!(event.operation_id, Some(operation_id));
        assert_eq!(event.correlation_id.as_deref(), Some("caller-read"));
    }
}
