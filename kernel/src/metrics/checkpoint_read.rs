//! Completion reports for checkpoint discovery and inspection.
//!
//! These are standalone operation reports, like handler metrics. They describe work that was
//! requested by the read path; enabling them does not trigger checkpoint I/O. A successful
//! discovery can find no checkpoint, and a successful inspection can find no sidecar references.
//! These reports do not carry operation or correlation IDs; consumers need external context for
//! attribution.

use std::fmt;
use std::time::Duration;

use strum::{AsRefStr, Display, EnumString, IntoStaticStr};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;

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

#[derive(Default)]
struct CheckpointReadAttrs {
    mode: String,
    format: String,
    outcome: String,
    start_version: u64,
    end_version: u64,
    has_end_version: bool,
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

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

fn parse_label<T: Default + std::str::FromStr>(value: &str) -> T {
    value.parse().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::metrics::MetricEvent;
    use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};

    #[rstest::rstest]
    fn completion_spans_preserve_optional_versions_and_recorded_duration(
        #[values(None, Some(0), Some(u64::MAX))] version: Option<u64>,
        #[values(0, 17)] duration_ns: u64,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        CheckpointDiscoveryCompleted {
            mode: CheckpointDiscoveryMode::Backward,
            outcome: CheckpointReadOutcome::Success,
            end_version: version,
            checkpoint_version: version,
            duration: Duration::from_nanos(duration_ns),
            ..Default::default()
        }
        .emit();
        CheckpointInspectionCompleted {
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
}
