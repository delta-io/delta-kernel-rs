//! Metric event types emitted during Delta Kernel operations.
//!
//! Each [`MetricEvent`] variant wraps a per-event struct that owns its fields, `Display` impl,
//! span name, and the small set of methods the `tracing` layer in [`crate::metrics::reporter`]
//! calls to construct and finalize the event. Per-event code is colocated in a single block
//! per type below.
//!
//! Enums carried in event payloads derive strum helpers (`Display`, `AsRefStr`, `IntoStaticStr`)
//! with stable serialized names. Fieldless labels also derive `EnumString`. `IntoStaticStr`
//! lets connectors convert a value to its `&'static str` metric-label string via `.into()`
//! instead of maintaining their own variant-to-string `match`.
//!
//! Event construction is infallible: a malformed span field warns and falls back to a
//! default rather than failing the operation being observed.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use delta_kernel_derive::internal_api;
use strum::{AsRefStr, Display as StrumDisplay, EnumString, IntoStaticStr};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::warn;
use uuid::Uuid;

use crate::log_segment::LogSegment;

// ====================================================================
// MetricId
// ====================================================================

/// Unique identifier for a metrics operation.
///
/// Each operation (Snapshot, Transaction, Scan) gets a unique `MetricId` that correlates all
/// events emitted from that operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricId(pub(crate) Uuid);

impl MetricId {
    /// Generate a new unique `MetricId`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// The nil id (all-zero UUID): the decode seed when a span omits `operation_id`, and the
    /// sentinel for a malformed one.
    pub(crate) fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Return the 16 raw bytes of the underlying UUID. Useful for FFI consumers that want to
    /// carry the id without allocating or parsing its string form.
    pub fn as_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl fmt::Display for MetricId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ====================================================================
// MetricEvent
// ====================================================================

/// Identity and table labels shared by metrics from the same operation.
#[derive(Debug, Clone)]
pub struct MetricContext {
    /// Unique identifier assigned by Kernel to this operation.
    pub operation_id: MetricId,
    /// Opaque caller-supplied identifier for joining metrics to the caller's request.
    pub correlation_id: Option<Arc<str>>,
    /// Table mode used by the operation. Snapshot loading uses the requested mode.
    pub table_type: TableType,
}

impl MetricContext {
    fn empty() -> Self {
        Self {
            operation_id: MetricId::nil(),
            correlation_id: None,
            table_type: TableType::PathBased,
        }
    }

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut context = Self::empty();
        attrs.record(&mut context);
        context
    }
}

impl Visit for MetricContext {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == IS_CATALOG_MANAGED_FIELD {
            self.table_type = TableType::from_catalog_managed(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            CORRELATION_ID_FIELD if !value.is_empty() => self.correlation_id = Some(value.into()),
            "operation_id" => self.record_debug(field, &tracing::field::display(value)),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if let Some(id) = record_operation_id(field, value, "metric context") {
            self.operation_id = id;
        }
    }
}

impl fmt::Display for MetricContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id={}, table_type={}, correlation_id={:?}",
            self.operation_id, self.table_type, self.correlation_id
        )
    }
}

/// Metric events emitted during Delta Kernel operations.
#[derive(Debug, Clone)]
pub enum MetricEvent {
    LogSegmentLoadCompleted(LogSegmentLoadCompleted),
    ProtocolMetadataLoadCompleted(ProtocolMetadataLoadCompleted),
    SnapshotBuildCompleted(SnapshotBuildCompleted),
    TransactionCommitCompleted(TransactionCommitCompleted),
    DomainMetadataLoadCompleted(DomainMetadataLoadCompleted),
    SetTransactionLoadCompleted(SetTransactionLoadCompleted),
    CrcReadCompleted(CrcReadCompleted),
    LastCheckpointReadCompleted(LastCheckpointReadCompleted),
    CheckpointWriteCompleted(CheckpointWriteCompleted),
    ChecksumWriteCompleted(ChecksumWriteCompleted),
    JsonReadCompleted(JsonReadCompleted),
    ParquetReadCompleted(ParquetReadCompleted),
    ScanMetadataCompleted(ScanMetadataCompleted),
    StorageListCompleted(StorageListCompleted),
    StorageReadCompleted(StorageReadCompleted),
    StorageCopyCompleted(StorageCopyCompleted),
}

impl MetricEvent {
    /// Set elapsed time on operations timed by the lifecycle span.
    pub(crate) fn set_duration_if_applicable(&mut self, d: Duration) {
        match self {
            Self::SnapshotBuildCompleted(e) => e.duration = d,
            Self::TransactionCommitCompleted(e) => e.duration = d,
            Self::DomainMetadataLoadCompleted(e) => e.duration = d,
            Self::SetTransactionLoadCompleted(e) => e.duration = d,
            Self::CrcReadCompleted(e) => e.duration = d,
            Self::LastCheckpointReadCompleted(e) => e.duration = d,
            Self::CheckpointWriteCompleted(e) => e.duration = d,
            Self::ChecksumWriteCompleted(e) => e.duration = d,
            Self::LogSegmentLoadCompleted(_)
            | Self::ProtocolMetadataLoadCompleted(_)
            | Self::JsonReadCompleted(_)
            | Self::ParquetReadCompleted(_)
            | Self::ScanMetadataCompleted(_)
            | Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => {}
        }
    }
    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match self {
            Self::SnapshotBuildCompleted(e) => e.record_u64(name, value),
            Self::TransactionCommitCompleted(e) => e.record_u64(name, value),
            Self::DomainMetadataLoadCompleted(e) => e.record_u64(name, value),
            Self::CrcReadCompleted(e) => e.record_u64(name, value),
            Self::CheckpointWriteCompleted(e) => e.record_u64(name, value),
            Self::ChecksumWriteCompleted(e) => e.record_u64(name, value),
            _ => Err(self.span_name()),
        }
    }
    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match self {
            Self::TransactionCommitCompleted(e) => e.record_bool(name, value),
            Self::DomainMetadataLoadCompleted(e) => e.record_bool(name, value),
            Self::SetTransactionLoadCompleted(e) => e.record_bool(name, value),
            _ => Err(self.span_name()),
        }
    }
    pub(crate) fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        match self {
            Self::TransactionCommitCompleted(e) => e.record_str(name, value),
            Self::CheckpointWriteCompleted(e) => e.record_str(name, value),
            Self::ChecksumWriteCompleted(e) => e.record_str(name, value),
            Self::LastCheckpointReadCompleted(e) => e.record_str(name, value),
            _ => Err(self.span_name()),
        }
    }
    /// Change only the outcome, preserving the attempt's context and measurements.
    pub(crate) fn mark_error(&mut self) {
        match self {
            Self::LogSegmentLoadCompleted(e) => e.outcome = LogSegmentLoadOutcome::Error,
            Self::ProtocolMetadataLoadCompleted(e) => {
                e.outcome = ProtocolMetadataLoadOutcome::Error
            }
            Self::SnapshotBuildCompleted(e) => e.outcome = SnapshotBuildOutcome::Error,
            Self::TransactionCommitCompleted(e) => {
                if let TransactionCommitOutcome::Success { .. } = e.outcome {
                    e.outcome = TransactionCommitOutcome::Failure {
                        reason: CommitFailureReason::Error,
                    };
                }
            }
            Self::DomainMetadataLoadCompleted(e) => e.outcome = DomainMetadataLoadOutcome::Error,
            Self::SetTransactionLoadCompleted(e) => e.outcome = SetTransactionLoadOutcome::Error,
            Self::CrcReadCompleted(e) => e.outcome = CrcReadOutcome::Error,
            Self::CheckpointWriteCompleted(e) => e.outcome = CheckpointWriteOutcome::Error,
            Self::ChecksumWriteCompleted(e) => e.outcome = ChecksumWriteOutcome::Error,
            Self::LastCheckpointReadCompleted(e) => {
                if e.outcome == LastCheckpointReadOutcome::Unknown {
                    e.outcome = LastCheckpointReadOutcome::Error;
                }
            }
            Self::JsonReadCompleted(_)
            | Self::ParquetReadCompleted(_)
            | Self::ScanMetadataCompleted(_)
            | Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => {}
        }
    }

    fn span_name(&self) -> &'static str {
        match self {
            Self::LogSegmentLoadCompleted(_) => LogSegmentLoadCompleted::SPAN_NAME,
            Self::ProtocolMetadataLoadCompleted(_) => ProtocolMetadataLoadCompleted::SPAN_NAME,
            Self::SnapshotBuildCompleted(_) => SnapshotBuildCompleted::SPAN_NAME,
            Self::TransactionCommitCompleted(_) => TransactionCommitCompleted::SPAN_NAME,
            Self::DomainMetadataLoadCompleted(_) => DomainMetadataLoadCompleted::SPAN_NAME,
            Self::SetTransactionLoadCompleted(_) => SetTransactionLoadCompleted::SPAN_NAME,
            Self::CrcReadCompleted(_) => CrcReadCompleted::SPAN_NAME,
            Self::LastCheckpointReadCompleted(_) => LastCheckpointReadCompleted::SPAN_NAME,
            Self::CheckpointWriteCompleted(_) => CheckpointWriteCompleted::SPAN_NAME,
            Self::ChecksumWriteCompleted(_) => ChecksumWriteCompleted::SPAN_NAME,
            Self::JsonReadCompleted(_) => JsonReadCompleted::SPAN_NAME,
            Self::ParquetReadCompleted(_) => ParquetReadCompleted::SPAN_NAME,
            Self::ScanMetadataCompleted(_) => ScanMetadataCompleted::SPAN_NAME,
            Self::StorageListCompleted(_) => STORAGE_SPAN,
            Self::StorageReadCompleted(_) => STORAGE_SPAN,
            Self::StorageCopyCompleted(_) => STORAGE_SPAN,
        }
    }
}

impl fmt::Display for MetricEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogSegmentLoadCompleted(e) => e.fmt(f),
            Self::ProtocolMetadataLoadCompleted(e) => e.fmt(f),
            Self::SnapshotBuildCompleted(e) => e.fmt(f),
            Self::TransactionCommitCompleted(e) => e.fmt(f),
            Self::DomainMetadataLoadCompleted(e) => e.fmt(f),
            Self::SetTransactionLoadCompleted(e) => e.fmt(f),
            Self::CrcReadCompleted(e) => e.fmt(f),
            Self::LastCheckpointReadCompleted(e) => e.fmt(f),
            Self::CheckpointWriteCompleted(e) => e.fmt(f),
            Self::ChecksumWriteCompleted(e) => e.fmt(f),
            Self::JsonReadCompleted(e) => e.fmt(f),
            Self::ParquetReadCompleted(e) => e.fmt(f),
            Self::ScanMetadataCompleted(e) => e.fmt(f),
            Self::StorageListCompleted(e) => e.fmt(f),
            Self::StorageReadCompleted(e) => e.fmt(f),
            Self::StorageCopyCompleted(e) => e.fmt(f),
        }
    }
}

/// Result of a [`crate::Snapshot::checkpoint`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointWriteOutcome {
    /// The checkpoint was written and the API returned its updated snapshot.
    Written {
        /// Size and action counts from the completed checkpoint write.
        stats: CheckpointWriteStats,
    },
    /// The API returned [`crate::snapshot::CheckpointWriteResult::AlreadyExists`].
    AlreadyExists,
    /// The operation returned an error.
    Error,
}

/// Checkpoint format and sidecar-writing mode selected during a checkpoint call.
///
/// The mode is retained if preparation or writing fails. It describes the selected work;
/// [`CheckpointOutputStats::num_sidecars_written`] reports how many sidecar writes succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CheckpointWriteMode {
    /// The call returned before selecting a mode, such as when a checkpoint already exists
    /// or the requested format fails preflight validation.
    NotAttempted,
    /// A V1 checkpoint without sidecars.
    V1,
    /// A V2 checkpoint with file actions in the main checkpoint file.
    V2WithoutSidecars,
    /// A V2 checkpoint using the sidecar-writing path, which may produce zero sidecars.
    V2WithSidecars,
}

/// A checkpoint write attempt completed.
///
/// Processing and output statistics retain work performed before any outcome, including an error
/// or `AlreadyExists`. Only [`CheckpointWriteOutcome::Written`] carries completed-checkpoint
/// contents. An error can occur after data writes, during hint publication or snapshot
/// construction.
#[derive(Debug, Clone)]
pub struct CheckpointWriteCompleted {
    /// Unique identifier for this checkpoint attempt.
    pub operation_id: MetricId,
    /// Snapshot version being checkpointed.
    pub version: u64,
    /// Whether the call wrote a checkpoint, found one already present, or returned an error.
    pub outcome: CheckpointWriteOutcome,
    /// Selected write mode, or [`CheckpointWriteMode::NotAttempted`] for an early return.
    pub mode: CheckpointWriteMode,
    /// Elapsed time from API entry through checkpoint data writes and size verification.
    ///
    /// Hint publication and returned snapshot construction are excluded. Calls that return
    /// before checkpoint data completion report elapsed time through that return.
    pub duration: Duration,
    /// Work performed while producing checkpoint batches, including on an AlreadyExists return.
    pub processing_stats: CheckpointProcessingStats,
    /// Output work performed, including on an AlreadyExists return after writing starts.
    pub output_stats: CheckpointOutputStats,
}

/// Processing times and input action counts for one [`crate::Snapshot::checkpoint`] attempt.
///
/// Timers follow batch processing order: input, reconciliation, statistics transforms, then output
/// preparation. Each measures elapsed time, including engine work and waits, but excludes pulls
/// from earlier stages. Background prefetch may overlap these stages.
///
/// These timings exclude Parquet encoding, output writes, and `_last_checkpoint` publication.
/// Their sum does not represent the total checkpoint duration. Unreached stages report zero;
/// failures retain measurements accumulated before the error.
///
/// Action counts are taken before reconciliation and include discarded actions. If processing
/// fails, counts include the entire received batch. Counts exclude actions generated for the new
/// checkpoint, such as checkpoint metadata and sidecar references.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointProcessingStats {
    /// Time setting up and consuming input from original commit files.
    /// Includes reads, decoding, waits, and final or failing iterator calls.
    pub log_tail_input_duration: Duration,

    /// Time setting up and consuming the previous checkpoint and its sidecars.
    /// Includes schema/footer reads, sidecar-reference extraction, decoding, waits, and final
    /// or failing iterator calls. Includes JSON main checkpoints as well as Parquet input.
    pub checkpoint_input_duration: Duration,

    /// Time reconciling input actions and building selection vectors.
    pub reconciliation_duration: Duration,

    /// Time evaluating statistics and partition-value transforms for checkpoint output.
    /// Reuses or converts existing statistics.
    pub stats_transform_duration: Duration,

    /// Time applying selection vectors to transformed batches.
    ///
    /// With sidecars, also includes projecting and filtering file and non-file actions into
    /// separate batches, and buffering non-file batches for the main checkpoint.
    pub output_preparation_duration: Duration,

    /// Input Delta log actions from original commit files after the previous checkpoint,
    /// or from version zero when there is no previous checkpoint.
    pub num_actions_from_log_tail: u64,

    /// Input Delta log actions from the previous checkpoint and its sidecars.
    /// Zero when there is no previous checkpoint.
    pub num_actions_from_checkpoint: u64,
}

/// Output work performed by one checkpoint attempt, retained on failures and AlreadyExists returns.
///
/// Timings measure elapsed time, including waits. They exclude `_last_checkpoint` publication.
/// Byte and sidecar counts include only successful Parquet write calls, even if size verification
/// subsequently fails. Bytes uploaded by a failing call are not available from the engine API.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointOutputStats {
    /// Parquet writer time, excluding pulls from its input iterator. Includes failing write calls.
    pub parquet_write_duration: Duration,
    /// Time querying storage metadata and verifying written file sizes, including failures.
    pub verification_duration: Duration,
    /// Sum of sizes returned by successful Parquet writes of the main checkpoint and new sidecars.
    pub bytes_written: u64,
    /// Sidecars whose Parquet write calls returned successfully.
    pub num_sidecars_written: u64,
}

/// Contents of a completed checkpoint, including referenced sidecars and excluding
/// `_last_checkpoint`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointWriteStats {
    /// Main checkpoint size plus every referenced sidecar's size.
    pub size_in_bytes: u64,
    /// Active Add actions across the completed checkpoint.
    pub num_add_files: u64,
    /// Retained Remove actions across the completed checkpoint.
    pub num_remove_files: u64,
    /// Action rows across the main checkpoint and referenced sidecars, including metadata and
    /// sidecar references. This is not the sum of Add and Remove counts.
    pub num_actions: u64,
    /// Total sidecars referenced by the completed checkpoint.
    pub num_sidecars: u64,
}

impl CheckpointWriteCompleted {
    pub(crate) const SPAN_NAME: &'static str = "snap.checkpoint";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut event = Self {
            operation_id: MetricId::nil(),
            version: 0,
            outcome: CheckpointWriteOutcome::Written {
                stats: CheckpointWriteStats::default(),
            },
            mode: CheckpointWriteMode::NotAttempted,
            duration: Duration::ZERO,
            processing_stats: CheckpointProcessingStats::default(),
            output_stats: CheckpointOutputStats::default(),
        };
        attrs.record(&mut event);
        event
    }

    fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "version" => self.version = value,
            "duration_ns" => self.duration = Duration::from_nanos(value),
            "log_tail_input_duration_ns" => {
                self.processing_stats.log_tail_input_duration = Duration::from_nanos(value)
            }
            "checkpoint_input_duration_ns" => {
                self.processing_stats.checkpoint_input_duration = Duration::from_nanos(value)
            }
            "reconciliation_duration_ns" => {
                self.processing_stats.reconciliation_duration = Duration::from_nanos(value)
            }
            "stats_transform_duration_ns" => {
                self.processing_stats.stats_transform_duration = Duration::from_nanos(value)
            }
            "output_preparation_duration_ns" => {
                self.processing_stats.output_preparation_duration = Duration::from_nanos(value)
            }
            "num_actions_from_log_tail" => self.processing_stats.num_actions_from_log_tail = value,
            "num_actions_from_checkpoint" => {
                self.processing_stats.num_actions_from_checkpoint = value
            }
            "parquet_write_duration_ns" => {
                self.output_stats.parquet_write_duration = Duration::from_nanos(value)
            }
            "verification_duration_ns" => {
                self.output_stats.verification_duration = Duration::from_nanos(value)
            }
            "bytes_written" => self.output_stats.bytes_written = value,
            "num_sidecars_written" => self.output_stats.num_sidecars_written = value,
            _ => return self.record_write_stats(name, value),
        }
        Ok(())
    }

    fn record_write_stats(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        let CheckpointWriteOutcome::Written { stats } = &mut self.outcome else {
            return Ok(());
        };
        match name {
            "size_in_bytes" => stats.size_in_bytes = value,
            "num_add_files" => stats.num_add_files = value,
            "num_remove_files" => stats.num_remove_files = value,
            "num_actions" => stats.num_actions = value,
            "num_sidecars" => stats.num_sidecars = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        match name {
            "outcome" if value == CheckpointWriteOutcome::AlreadyExists.as_ref() => {
                if self.outcome != CheckpointWriteOutcome::Error {
                    self.outcome = CheckpointWriteOutcome::AlreadyExists;
                }
            }
            "mode" => self.mode = value.parse().map_err(|_| Self::SPAN_NAME)?,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl Visit for CheckpointWriteCompleted {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "version" {
            self.version = value;
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if let Some(id) = record_operation_id(field, value, Self::SPAN_NAME) {
            self.operation_id = id;
        }
    }
}

impl fmt::Display for CheckpointWriteCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CheckpointWriteCompleted(id={}, version={}, outcome={}, mode={}, \
             duration={:?}, processing_stats={:?}, output_stats={:?}",
            self.operation_id,
            self.version,
            self.outcome,
            self.mode,
            self.duration,
            self.processing_stats,
            self.output_stats,
        )?;
        if let CheckpointWriteOutcome::Written { stats } = self.outcome {
            write!(f, ", stats={stats:?}")?;
        }
        f.write_str(")")
    }
}

// ====================================================================
// LogSegmentLoad
// ====================================================================
//
// Canonical example for the per-event block pattern. Other events below follow the same
// shape; detailed `///` docs on `from_attrs` and `record_*` live here only.

pub(crate) const LOG_SEGMENT_LOADED_SPAN: &str = "segment.for_snapshot";

/// How the snapshot was constructed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    EnumString,
    StrumDisplay,
    AsRefStr,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum SnapshotLoadType {
    /// The segment was listed from its base (a checkpoint, else version 0) up to the target. A
    /// fresh snapshot build (`LogSegment::for_snapshot`) reads this way.
    Full,
    /// The build started from an existing snapshot. It may return that snapshot unchanged, replay
    /// newer commits, or rebuild from a newly discovered checkpoint.
    Incremental,
    /// The snapshot was constructed from complete caller-supplied state without asking the engine
    /// to list or read Delta log files.
    SnapshotHint,
    /// Decode fell back here because the span's `load_type` field was unset or unrecognized.
    /// Kernel never emits this deliberately.
    #[default]
    Unknown,
}

impl SnapshotLoadType {
    fn parse_or_unknown(s: &str) -> Self {
        parse_load_type_or_unknown(s)
    }
}

/// How the log segment and protocol/metadata state were loaded.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, EnumString, StrumDisplay, AsRefStr, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum LogSegmentLoadType {
    /// The segment was listed from its base through the target version.
    Full,
    /// The segment load started from an existing snapshot. It may reuse that segment, extend it,
    /// or replace it after discovering a newer checkpoint.
    Incremental,
    /// Decode fell back here because the span field was unset or unrecognized.
    #[default]
    Unknown,
}

impl LogSegmentLoadType {
    fn parse_or_unknown(s: &str) -> Self {
        parse_load_type_or_unknown(s)
    }
}

fn parse_load_type_or_unknown<T>(s: &str) -> T
where
    T: Default + FromStr,
    T::Err: fmt::Display,
{
    if s.is_empty() {
        return T::default();
    }
    s.parse().unwrap_or_else(|error| {
        warn!("Invalid load_type '{s}': {error}. Using Unknown.");
        T::default()
    })
}

/// A log-segment load attempt completed.
#[derive(Debug, Clone)]
pub struct LogSegmentLoadCompleted {
    /// Identity and table labels shared by this operation's events.
    pub context: MetricContext,
    /// How this operation loaded its state.
    pub load_type: LogSegmentLoadType,
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: LogSegmentLoadOutcome,
}

/// Result of a log segment load operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum LogSegmentLoadOutcome {
    /// The operation succeeded.
    Success {
        /// Measurements from the successful operation.
        stats: LogSegmentLoadStats,
    },
    /// The operation returned an error.
    Error,
}

/// Measurements from a successful log segment load.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogSegmentLoadStats {
    /// Commit files in the assembled log segment.
    pub num_commit_files: u64,
    /// Parts of the segment's checkpoint.
    pub num_checkpoint_files: u64,
    /// Log compaction files in the assembled segment.
    pub num_compaction_files: u64,
    /// How many versions behind the segment's end version the latest on-disk CRC file is, or
    /// `None` if there is no CRC file.
    pub crc_versions_behind: Option<u64>,
}

impl LogSegmentLoadOutcome {
    pub(crate) fn success(segment: &LogSegment) -> Self {
        Self::Success {
            stats: LogSegmentLoadStats {
                num_commit_files: segment.listed.ascending_commit_files.len() as u64,
                num_checkpoint_files: segment.listed.checkpoint_parts.len() as u64,
                num_compaction_files: segment.listed.ascending_compaction_files.len() as u64,
                crc_versions_behind: segment.crc_versions_behind(),
            },
        }
    }
}

impl LogSegmentLoadCompleted {
    pub(crate) const SPAN_NAME: &'static str = LOG_SEGMENT_LOADED_SPAN;

    /// Span field for CRC staleness. `None` (no CRC file) encodes as `-1`.
    const CRC_VERSIONS_BEHIND_FIELD: &'static str = "crc_versions_behind";

    fn encode_crc_versions_behind(v: Option<u64>) -> i64 {
        v.map_or(-1, |n| n as i64)
    }

    fn decode_crc_versions_behind(v: i64) -> Option<u64> {
        u64::try_from(v).ok()
    }

    /// The default seed [`Self::from_attrs`] populates from span fields.
    fn empty() -> Self {
        Self {
            context: MetricContext::empty(),
            load_type: LogSegmentLoadType::default(),
            duration: Duration::ZERO,
            outcome: LogSegmentLoadOutcome::Success {
                stats: LogSegmentLoadStats {
                    num_commit_files: 0,
                    num_checkpoint_files: 0,
                    num_compaction_files: 0,
                    crc_versions_behind: None,
                },
            },
        }
    }

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut event = Self::empty();
        attrs.record(&mut event);
        if has_error_outcome(attrs) {
            event.outcome = LogSegmentLoadOutcome::Error;
        }
        event
    }
}

impl Visit for LogSegmentLoadCompleted {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.context.record_bool(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.context.record_str(field, value);
        if field.name() == "load_type" {
            self.load_type = LogSegmentLoadType::parse_or_unknown(value);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "duration_ns" {
            self.duration = Duration::from_nanos(value);
            return;
        }

        let LogSegmentLoadOutcome::Success { stats } = &mut self.outcome else {
            return;
        };

        match field.name() {
            "num_commit_files" => stats.num_commit_files = value,
            "num_checkpoint_files" => stats.num_checkpoint_files = value,
            "num_compaction_files" => stats.num_compaction_files = value,
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let LogSegmentLoadOutcome::Success { stats } = &mut self.outcome else {
            return;
        };

        if field.name() == Self::CRC_VERSIONS_BEHIND_FIELD {
            stats.crc_versions_behind = Self::decode_crc_versions_behind(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.context.record_debug(field, value);
    }
}

impl fmt::Display for LogSegmentLoadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogSegmentLoadCompleted({}, load_type={}, duration={:?}, \
             outcome={:?})",
            self.context, self.load_type, self.duration, self.outcome
        )
    }
}

// ====================================================================
// ProtocolMetadataLoad
// ====================================================================

pub(crate) const PROTOCOL_METADATA_LOADED_SPAN: &str = "segment.read_metadata";

/// How a snapshot load resolved Protocol and Metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ProtocolMetadataSource {
    /// A CRC already sat at the target version; used as-is with zero replay.
    CrcAtTarget,
    /// A stale CRC was advanced to the target version by reverse replay.
    CrcAdvancedByReplay,
    /// A stale CRC seeded a pruned P&M replay of the commits above it (falling back to the
    /// CRC's own P&M when the pruned replay found no newer Protocol or Metadata).
    CrcSeededPmOnlyReplay,
    /// No CRC baseline; P&M came from log replay (full history on a fresh load, or the new
    /// commits on an incremental load).
    FullReplay,
    /// Decode fell back here because the span's `pm_source` field was unset or unrecognized.
    /// Kernel never emits this deliberately.
    Unknown,
}

impl ProtocolMetadataSource {
    fn parse_or_unknown(s: &str) -> Self {
        if s.is_empty() {
            return Self::Unknown;
        }
        Self::from_str(s).unwrap_or_else(|e| {
            warn!("Invalid pm_source '{s}': {e}. Using Unknown.");
            Self::Unknown
        })
    }
}

/// A protocol and metadata load attempt completed.
#[derive(Debug, Clone)]
pub struct ProtocolMetadataLoadCompleted {
    /// Identity and table labels shared by this operation's events.
    pub context: MetricContext,
    /// How this operation loaded its state.
    pub load_type: LogSegmentLoadType,
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: ProtocolMetadataLoadOutcome,
}

/// Result of a protocol and metadata load operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ProtocolMetadataLoadOutcome {
    /// The operation succeeded.
    Success { source: ProtocolMetadataSource },
    /// The operation returned an error.
    Error,
}

impl ProtocolMetadataLoadCompleted {
    pub(crate) const SPAN_NAME: &'static str = PROTOCOL_METADATA_LOADED_SPAN;

    /// The all-unset seed that [`Self::from_attrs`] fills in as span fields arrive. Each field
    /// here is the value used when its span field is absent.
    fn empty() -> Self {
        Self {
            context: MetricContext::empty(),
            load_type: LogSegmentLoadType::default(),
            duration: Duration::ZERO,
            outcome: ProtocolMetadataLoadOutcome::Success {
                source: ProtocolMetadataSource::Unknown,
            },
        }
    }

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut event = Self::empty();
        attrs.record(&mut event);
        if has_error_outcome(attrs) {
            event.outcome = ProtocolMetadataLoadOutcome::Error;
        }
        event
    }
}

impl Visit for ProtocolMetadataLoadCompleted {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.context.record_bool(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.context.record_str(field, value);
        match field.name() {
            "load_type" => self.load_type = LogSegmentLoadType::parse_or_unknown(value),
            "pm_source" => {
                if let ProtocolMetadataLoadOutcome::Success { source } = &mut self.outcome {
                    *source = ProtocolMetadataSource::parse_or_unknown(value);
                }
            }
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "duration_ns" {
            self.duration = Duration::from_nanos(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.context.record_debug(field, value);
    }
}

impl fmt::Display for ProtocolMetadataLoadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProtocolMetadataLoadCompleted({}, load_type={}, duration={:?}, \
             outcome={:?})",
            self.context, self.load_type, self.duration, self.outcome
        )
    }
}

// ====================================================================
// SnapshotBuild
// ====================================================================

pub(crate) const SNAPSHOT_COMPLETED_SPAN: &str = "snap.build";

/// A snapshot build attempt completed.
#[derive(Debug, Clone)]
pub struct SnapshotBuildCompleted {
    /// Identity and table labels shared by this operation's events.
    pub context: MetricContext,
    /// How this operation loaded its state.
    pub load_type: SnapshotLoadType,
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: SnapshotBuildOutcome,
}

/// Result of a snapshot build operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SnapshotBuildOutcome {
    /// The operation succeeded.
    Success { version: u64 },
    /// The operation returned an error.
    Error,
}

impl SnapshotBuildCompleted {
    pub(crate) const SPAN_NAME: &'static str = SNAPSHOT_COMPLETED_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        Self {
            context: MetricContext::from_attrs(attrs),
            load_type: load_type_from_attrs(attrs),
            duration: Duration::default(),
            outcome: SnapshotBuildOutcome::Success { version: 0 },
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        let SnapshotBuildOutcome::Success { version, .. } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "version" => (*version) = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for SnapshotBuildCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SnapshotBuildCompleted({}, load_type={}, duration={:?}, \
             outcome={:?})",
            self.context, self.load_type, self.duration, self.outcome
        )
    }
}

// ====================================================================
// TransactionCommit
// ====================================================================

pub(crate) const TRANSACTION_COMMIT_SPAN: &str = "txn.commit";

/// A transaction commit attempt completed.
#[derive(Debug, Clone)]
pub struct TransactionCommitCompleted {
    /// Identity and table labels shared by this operation's events.
    pub context: MetricContext,
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: TransactionCommitOutcome,
}

/// Result of a transaction commit operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum TransactionCommitOutcome {
    /// The operation succeeded.
    Success {
        /// Measurements from the successful operation.
        stats: TransactionCommitStats,
    },
    /// The commit conflicted, encountered retryable I/O, or returned an error.
    Failure {
        /// Why the commit did not succeed.
        reason: CommitFailureReason,
    },
}

/// Measurements from a successful transaction commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionCommitStats {
    /// Version committed by this transaction.
    pub commit_version: u64,
    /// Add-file actions written by the commit.
    pub num_add_files: u64,
    /// Remove-file actions written by the commit.
    pub num_remove_files: u64,
    /// Files whose deletion vectors were updated.
    pub num_dv_updates: u64,
    /// Total bytes represented by the Add-file actions.
    pub add_files_bytes: u64,
    /// Total bytes represented by the Remove-file actions.
    pub remove_files_bytes: u64,
    /// Whether the transaction used blind-append semantics.
    pub is_blind_append: bool,
    /// Whether the commit changes table data.
    pub data_change: bool,
    /// Operation recorded in the commit information, when available.
    pub operation: Option<String>,
    /// Time assembling and validating the commit, before the committer call.
    pub prepare_duration: Duration,
    /// Time in the committer's `commit()` call.
    pub committer_duration: Duration,
}

impl TransactionCommitCompleted {
    pub(crate) const SPAN_NAME: &'static str = TRANSACTION_COMMIT_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = TransactionCommitAttrs::default();
        attrs.record(&mut v);
        Self {
            context: MetricContext::from_attrs(attrs),
            duration: Duration::default(),
            outcome: TransactionCommitOutcome::Success {
                stats: TransactionCommitStats {
                    commit_version: v.commit_version,
                    num_add_files: 0,
                    num_remove_files: 0,
                    num_dv_updates: 0,
                    add_files_bytes: 0,
                    remove_files_bytes: 0,
                    is_blind_append: false,
                    data_change: false,
                    operation: None,
                    prepare_duration: Duration::default(),
                    committer_duration: Duration::default(),
                },
            },
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        let TransactionCommitOutcome::Success { stats } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "num_add_files" => stats.num_add_files = value,
            "num_remove_files" => stats.num_remove_files = value,
            "num_dv_updates" => stats.num_dv_updates = value,
            "add_files_bytes" => stats.add_files_bytes = value,
            "remove_files_bytes" => stats.remove_files_bytes = value,
            "prepare_duration_ns" => stats.prepare_duration = Duration::from_nanos(value),
            "committer_duration_ns" => stats.committer_duration = Duration::from_nanos(value),
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        let TransactionCommitOutcome::Success { stats } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "is_blind_append" => stats.is_blind_append = value,
            "data_change" => stats.data_change = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        if name == "failure_reason" {
            let parsed = value.parse();
            self.outcome = TransactionCommitOutcome::Failure {
                reason: parsed
                    .as_ref()
                    .copied()
                    .unwrap_or(CommitFailureReason::Error),
            };
            return parsed
                .map(|_: CommitFailureReason| ())
                .map_err(|_| Self::SPAN_NAME);
        }

        let TransactionCommitOutcome::Success { stats } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "operation" => stats.operation = Some(value.to_string()),
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for TransactionCommitCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TransactionCommitCompleted({}, duration={:?}, outcome={:?})",
            self.context, self.duration, self.outcome
        )
    }
}

/// Why a transaction commit did not succeed.
///
/// Serializes to its `snake_case` name for the `failure_reason` span field (e.g.
/// `RetryableIo` -> `"retryable_io"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CommitFailureReason {
    /// The commit conflicted with a concurrently committed version.
    Conflict,
    /// A retryable IO error occurred during the commit.
    RetryableIo,
    /// A terminal (non-retryable) error occurred.
    Error,
}

#[derive(Default)]
struct TransactionCommitAttrs {
    commit_version: u64,
}

impl Visit for TransactionCommitAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "commit_version" {
            self.commit_version = value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

// ====================================================================
// DomainMetadataLoad
// ====================================================================

pub(crate) const DOMAIN_METADATA_LOADED_SPAN: &str = "snap.get_domain_metadata";

/// Emitted once per domain metadata load, whether served from the CRC cache (`from_cache`) or
/// from a log replay. Covers connector-issued loads of user domains and kernel-internal loads of
/// system (`delta.*`) domains such as clustering or row tracking.
#[derive(Debug, Clone)]
pub struct DomainMetadataLoadCompleted {
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: DomainMetadataLoadOutcome,
}

/// Result of a domain metadata load operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum DomainMetadataLoadOutcome {
    /// The operation succeeded.
    Success {
        from_cache: bool,
        num_domains_returned: u64,
    },
    /// The operation returned an error.
    Error,
}

impl DomainMetadataLoadCompleted {
    pub(crate) const SPAN_NAME: &'static str = DOMAIN_METADATA_LOADED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            duration: Duration::default(),
            outcome: DomainMetadataLoadOutcome::Success {
                from_cache: false,
                num_domains_returned: 0,
            },
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        let DomainMetadataLoadOutcome::Success {
            num_domains_returned,
            ..
        } = &mut self.outcome
        else {
            return Ok(());
        };

        match name {
            "num_domains_returned" => (*num_domains_returned) = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        let DomainMetadataLoadOutcome::Success { from_cache, .. } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "from_cache" => (*from_cache) = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for DomainMetadataLoadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DomainMetadataLoadCompleted(duration={:?}, outcome={:?})",
            self.duration, self.outcome
        )
    }
}

// ====================================================================
// SetTransactionLoad
// ====================================================================

pub(crate) const SET_TRANSACTION_LOADED_SPAN: &str = "snap.get_app_id_version";

/// Emitted once per `SetTransaction` (app id) load, whether served from the CRC cache
/// (`from_cache`) or from a log replay. `found` is true when the app id has a committed
/// transaction version, false when none exists or the existing one is expired.
#[derive(Debug, Clone)]
pub struct SetTransactionLoadCompleted {
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: SetTransactionLoadOutcome,
}

/// Result of a set transaction load operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SetTransactionLoadOutcome {
    /// The operation succeeded.
    Success { from_cache: bool, found: bool },
    /// The operation returned an error.
    Error,
}

impl SetTransactionLoadCompleted {
    pub(crate) const SPAN_NAME: &'static str = SET_TRANSACTION_LOADED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            duration: Duration::default(),
            outcome: SetTransactionLoadOutcome::Success {
                from_cache: false,
                found: false,
            },
        }
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        let SetTransactionLoadOutcome::Success {
            from_cache, found, ..
        } = &mut self.outcome
        else {
            return Ok(());
        };

        match name {
            "from_cache" => (*from_cache) = value,
            "found" => (*found) = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for SetTransactionLoadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SetTransactionLoadCompleted(duration={:?}, outcome={:?})",
            self.duration, self.outcome
        )
    }
}

// ====================================================================
// CrcRead
// ====================================================================

pub(crate) const CRC_READ_COMPLETED_SPAN: &str = "crc_read_completed";

/// A CRC read and parse attempt completed.
#[derive(Debug, Clone)]
pub struct CrcReadCompleted {
    /// Elapsed time through the operation's return.
    pub duration: Duration,
    /// The operation result and its applicable measurements.
    pub outcome: CrcReadOutcome,
}

/// Result of a CRC read and parse operation.
#[derive(Debug, Clone, PartialEq, Eq, StrumDisplay, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CrcReadOutcome {
    /// The operation succeeded.
    Success { bytes_read: u64 },
    /// The operation returned an error.
    Error,
}

impl CrcReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = CRC_READ_COMPLETED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            duration: Duration::default(),
            outcome: CrcReadOutcome::Success { bytes_read: 0 },
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        let CrcReadOutcome::Success { bytes_read, .. } = &mut self.outcome else {
            return Ok(());
        };

        match name {
            "bytes_read" => (*bytes_read) = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for CrcReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CrcReadCompleted(duration={:?}, outcome={:?})",
            self.duration, self.outcome
        )
    }
}

// ====================================================================
// LastCheckpointRead
// ====================================================================

pub(crate) const LAST_CHECKPOINT_READ_SPAN: &str = "last_checkpoint.read";

/// Result of an attempt to read and validate a `_last_checkpoint` hint.
///
/// This describes the hint read, not whether the enclosing snapshot operation succeeded.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, EnumString, StrumDisplay, AsRefStr, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum LastCheckpointReadOutcome {
    /// The hint was parsed and retained. It may still name a checkpoint the caller cannot use.
    Success,
    /// Storage reported that the hint file was absent.
    ///
    /// An iterator-level file-not-found returns no hint; a top-level storage error propagates.
    NotFound,
    /// The hint was empty, malformed, or discarded by hint validation.
    Invalid,
    /// The attempt returned another error, including cancellation.
    Error,
    /// The outcome was not recorded or its metric label was unrecognized.
    #[default]
    Unknown,
}

/// A `_last_checkpoint` read attempt completed, including attempts returning no hint or an error.
///
/// Reporters can count attempts and record latency using [`Self::outcome`] as a label. Missing
/// and invalid hints normally allow the caller to fall back to listing the log.
#[derive(Debug, Clone)]
pub struct LastCheckpointReadCompleted {
    /// Result of reading and validating the hint.
    pub outcome: LastCheckpointReadOutcome,
    /// Wall-clock time from entry through return, including storage access, parsing, and
    /// validation.
    pub duration: Duration,
}

impl LastCheckpointReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = LAST_CHECKPOINT_READ_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            outcome: LastCheckpointReadOutcome::Unknown,
            duration: Duration::ZERO,
        }
    }

    fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        if name != "outcome" {
            return Err(Self::SPAN_NAME);
        }
        match value.parse() {
            Ok(outcome) => {
                self.outcome = outcome;
                Ok(())
            }
            Err(_) => {
                self.outcome = LastCheckpointReadOutcome::Unknown;
                Err(Self::SPAN_NAME)
            }
        }
    }
}

impl fmt::Display for LastCheckpointReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { outcome, duration } = self;
        write!(
            f,
            "LastCheckpointReadCompleted(outcome={outcome}, duration={duration:?})"
        )
    }
}

// ====================================================================
// ChecksumWrite
// ====================================================================

pub(crate) const CHECKSUM_WRITE_SPAN: &str = "snap.write_checksum";

/// Result of a [`crate::Snapshot::write_checksum`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum ChecksumWriteOutcome {
    /// The CRC was written and the updated snapshot was constructed successfully.
    Written {
        /// Exact serialized UTF-8 byte count written by this call.
        bytes_written: u64,
    },
    /// The CRC already existed, including a concurrent writer winning the storage race.
    AlreadyExists,
    /// The operation returned an error.
    Error,
}

/// CRC construction path selected during [`crate::Snapshot::write_checksum`].
///
/// Reusing a CRC already at the snapshot version is [`Self::Cached`], regardless of how it was
/// constructed. The selected mode is retained on failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, StrumDisplay, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ChecksumWriteResolutionMode {
    /// Attempted construction from a checkpoint or version zero, replaying commits as needed.
    Full,
    /// Attempted to advance an older CRC by replaying newer commits.
    Incremental,
    /// Selected the in-memory CRC already at the snapshot version.
    Cached,
    /// Returned before selecting a CRC construction path.
    NotAttempted,
}

/// A checksum write attempt completed.
#[derive(Debug, Clone)]
pub struct ChecksumWriteCompleted {
    /// Final result returned by the API.
    pub outcome: ChecksumWriteOutcome,
    /// CRC construction path selected during this call.
    pub resolution_mode: ChecksumWriteResolutionMode,
    /// Wall-clock time from API entry to return.
    pub duration: Duration,
}

impl ChecksumWriteCompleted {
    pub(crate) const SPAN_NAME: &'static str = CHECKSUM_WRITE_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            outcome: ChecksumWriteOutcome::Written { bytes_written: 0 },
            resolution_mode: ChecksumWriteResolutionMode::NotAttempted,
            duration: Duration::default(),
        }
    }

    fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "bytes_written" => {
                if let ChecksumWriteOutcome::Written { bytes_written } = &mut self.outcome {
                    *bytes_written = value;
                }
            }
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        match name {
            "outcome" if value == ChecksumWriteOutcome::AlreadyExists.as_ref() => {
                if self.outcome != ChecksumWriteOutcome::Error {
                    self.outcome = ChecksumWriteOutcome::AlreadyExists;
                }
            }
            "resolution_mode" => {
                self.resolution_mode = value.parse().map_err(|_| Self::SPAN_NAME)?;
            }
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }
}

impl fmt::Display for ChecksumWriteCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            outcome,
            resolution_mode,
            duration,
        } = self;
        write!(
            f,
            "ChecksumWriteCompleted(outcome={outcome}, resolution_mode={resolution_mode}, \
             duration={duration:?}"
        )?;
        if let ChecksumWriteOutcome::Written { bytes_written } = outcome {
            write!(f, ", bytes_written={bytes_written}")?;
        }
        f.write_str(")")
    }
}

// ====================================================================
// JsonReadCompleted
// ====================================================================

/// Emitted once per `JsonHandler::read_json_files` call when the returned iterator is fully
/// consumed or dropped. `bytes_read` is the sum of on-disk `FileMeta::size`, not the
/// deserialized payload size.
#[derive(Debug, Clone)]
pub struct JsonReadCompleted {
    // === Set on span creation ===
    pub num_files: u64,
    pub bytes_read: u64,
}

impl JsonReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = "json_read_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = FileReadAttrs::default();
        attrs.record(&mut v);
        Self {
            num_files: v.num_files,
            bytes_read: v.bytes_read,
        }
    }
}

impl fmt::Display for JsonReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            num_files,
            bytes_read,
        } = self;
        write!(
            f,
            "JsonReadCompleted(files={num_files}, bytes={bytes_read})"
        )
    }
}

// ====================================================================
// ParquetReadCompleted
// ====================================================================

/// Emitted once per `ParquetHandler::read_parquet_files` call when the returned iterator is
/// fully consumed or dropped. `bytes_read` is the sum of on-disk `FileMeta::size`, not the
/// deserialized payload size.
#[derive(Debug, Clone)]
pub struct ParquetReadCompleted {
    // === Set on span creation ===
    pub num_files: u64,
    pub bytes_read: u64,
}

impl ParquetReadCompleted {
    pub(crate) const SPAN_NAME: &'static str = "parquet_read_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = FileReadAttrs::default();
        attrs.record(&mut v);
        Self {
            num_files: v.num_files,
            bytes_read: v.bytes_read,
        }
    }
}

impl fmt::Display for ParquetReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            num_files,
            bytes_read,
        } = self;
        write!(
            f,
            "ParquetReadCompleted(files={num_files}, bytes={bytes_read})"
        )
    }
}

/// Shared attribute decoder for `JsonReadCompleted` and `ParquetReadCompleted`.
#[derive(Default)]
struct FileReadAttrs {
    num_files: u64,
    bytes_read: u64,
}

impl Visit for FileReadAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_files" => self.num_files = value,
            "bytes_read" => self.bytes_read = value,
            _ => {}
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

// ====================================================================
// ScanMetadataCompleted
// ====================================================================

/// Identifies which scan execution path produced a scan metadata metrics event.
///
/// Serializes to the explicit `serialize` name on each variant for the `scan_type` span field
/// (e.g. `SequentialPhase` -> `"sequential"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, StrumDisplay, AsRefStr, IntoStaticStr)]
pub enum ScanType {
    /// Sequential phase of [`crate::scan::Scan::parallel_scan_metadata`].
    #[strum(serialize = "sequential")]
    SequentialPhase,
    /// Parallel phase of [`crate::scan::Scan::parallel_scan_metadata`].
    #[strum(serialize = "parallel")]
    ParallelPhase,
    /// Scan metadata from [`crate::scan::Scan::scan_metadata`].
    #[strum(serialize = "full")]
    Full,
}

impl ScanType {
    fn parse_lenient(s: &str) -> Self {
        Self::from_str(s).unwrap_or_else(|e| {
            warn!("Invalid scan_type '{s}' on span: {e}. Using Full.");
            Self::Full
        })
    }
}

// ====================================================================
// SnapshotLoadMetricContext and shared span fields
// ====================================================================

/// The `is_catalog_managed` bool span field carried by every event that records it. It is the
/// confirmed protocol value, except on snapshot-load events, which use the requested mode because
/// the on-disk protocol is not known that early (see `SnapshotBuilder::build`).
pub(crate) const IS_CATALOG_MANAGED_FIELD: &str = "is_catalog_managed";

/// Whether a table is path-based or catalog-managed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TableType {
    /// Loaded without catalog involvement; commits live directly in the Delta log.
    #[default]
    PathBased,
    /// Backed by a managing catalog.
    CatalogManaged,
}

impl TableType {
    #[internal_api]
    pub(crate) fn from_catalog_managed(is_catalog_managed: bool) -> Self {
        if is_catalog_managed {
            Self::CatalogManaged
        } else {
            Self::PathBased
        }
    }

    pub(crate) fn is_catalog_managed(self) -> bool {
        self == Self::CatalogManaged
    }
}

impl fmt::Display for TableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PathBased => "path_based",
            Self::CatalogManaged => "catalog_managed",
        })
    }
}

/// Operation-scoped values threaded through the snapshot-load chain to label its metric events.
#[derive(Debug, Clone)]
pub struct SnapshotLoadMetricContext {
    pub(crate) context: MetricContext,
    pub(crate) load_type: SnapshotLoadType,
}

#[cfg(test)]
impl SnapshotLoadMetricContext {
    /// A context seeded with a nil `operation_id` for tests that exercise the load chain without
    /// asserting on the id.
    pub(crate) fn for_test() -> Self {
        Self {
            context: MetricContext::empty(),
            load_type: SnapshotLoadType::default(),
        }
    }
}

/// Decode the `operation_id` debug-string span field into a [`MetricId`]. Returns `None` when the
/// field is not `operation_id`; returns the nil id (with a warning) when the value is malformed.
/// Shared by the self-visiting event decoders.
pub(crate) fn record_operation_id(
    field: &Field,
    value: &dyn fmt::Debug,
    span_name: &str,
) -> Option<MetricId> {
    (field.name() == "operation_id").then(|| {
        let s = format!("{value:?}");
        Uuid::from_str(&s).map(MetricId).unwrap_or_else(|e| {
            warn!("Invalid uuid '{s}' on {span_name}: {e}. Using nil.");
            MetricId::nil()
        })
    })
}

/// The `correlation_id` string span field carried by every event that records it. Empty means the
/// caller did not supply one.
pub(crate) const CORRELATION_ID_FIELD: &str = "correlation_id";

pub(crate) fn load_type_from_attrs(attrs: &Attributes<'_>) -> SnapshotLoadType {
    #[derive(Default)]
    struct V(SnapshotLoadType);
    impl Visit for V {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "load_type" {
                self.0 = SnapshotLoadType::parse_or_unknown(value);
            }
        }
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
    let mut v = V::default();
    attrs.record(&mut v);
    v.0
}

/// A `scan_metadata` scan emits one event with [`ScanType::Full`] after iterator exhaustion.
/// Calling [`SequentialScanMetadata::finish`](crate::scan::SequentialScanMetadata::finish) emits
/// a [`ScanType::SequentialPhase`] event. If it returns parallel work, the caller emits a
/// [`ScanType::ParallelPhase`] event by calling
/// [`ParallelState::log_metrics`](crate::scan::ParallelState::log_metrics) after all workers
/// complete. The two phase events share an `operation_id`. Action counts and timings are
/// phase-local, while `peak_hash_set_size` is the whole-scan high-water mark.
#[derive(Debug, Clone)]
pub struct ScanMetadataCompleted {
    /// Identity and table labels shared by this scan's metric events.
    pub context: MetricContext,
    /// Which scan execution path produced this event.
    pub scan_type: ScanType,
    /// Wall-clock time from scan start to iterator exhaustion.
    pub duration: Duration,
    /// Add actions in replay input before predicate filtering and deduplication. Includes
    /// checkpoint and delta files.
    pub num_add_files_seen: u64,
    /// Add actions in delta-file replay input before predicate filtering and deduplication.
    pub num_add_files_seen_from_delta_files: u64,
    /// Add files that survived log replay (the files the connector reads).
    pub num_selected_add_files: u64,
    /// Size in bytes of the files that survived log replay (files to read).
    pub selected_add_files_bytes: u64,
    /// Remove actions in delta-file replay input before deduplication.
    pub num_remove_files_seen_from_delta_files: u64,
    /// Non-file actions seen (protocol, metadata, etc.).
    pub num_non_file_actions: u64,
    /// Files filtered by predicates (data skipping + partition pruning).
    pub num_predicate_filtered: u64,
    /// Peak size of the deduplication hash set.
    pub peak_hash_set_size: usize,
    /// Time spent in the deduplication visitor.
    pub dedup_visitor_time: Duration,
    /// Total time spent transforming log actions into scan rows, including retry attempts.
    ///
    /// This includes extracting Add fields, parsing JSON statistics into typed columns, and
    /// converting partition-value strings to their schema types. Concurrent transforms contribute
    /// separately, so their total can exceed the wall-clock `duration`.
    pub action_transform_time: Duration,
    /// Time spent evaluating predicates.
    pub predicate_eval_time: Duration,
}

impl ScanMetadataCompleted {
    pub(crate) const SPAN_NAME: &'static str = "scan.metadata_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = ScanMetadataCompletedAttrs::default();
        attrs.record(&mut v);
        Self {
            context: MetricContext::from_attrs(attrs),
            scan_type: ScanType::parse_lenient(&v.scan_type),
            duration: Duration::from_nanos(v.duration_ns),
            num_add_files_seen: v.num_add_files_seen,
            num_add_files_seen_from_delta_files: v.num_add_files_seen_from_delta_files,
            num_selected_add_files: v.num_selected_add_files,
            selected_add_files_bytes: v.selected_add_files_bytes,
            num_remove_files_seen_from_delta_files: v.num_remove_files_seen_from_delta_files,
            num_non_file_actions: v.num_non_file_actions,
            num_predicate_filtered: v.num_predicate_filtered,
            peak_hash_set_size: v.peak_hash_set_size as usize,
            dedup_visitor_time: Duration::from_nanos(v.dedup_visitor_time_ns),
            action_transform_time: Duration::from_nanos(v.action_transform_time_ns),
            predicate_eval_time: Duration::from_nanos(v.predicate_eval_time_ns),
        }
    }
}

impl fmt::Display for ScanMetadataCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            context:
                MetricContext {
                    operation_id,
                    table_type,
                    correlation_id,
                },
            scan_type,
            duration,
            num_add_files_seen,
            num_add_files_seen_from_delta_files,
            num_selected_add_files,
            selected_add_files_bytes,
            num_remove_files_seen_from_delta_files,
            num_non_file_actions,
            num_predicate_filtered,
            peak_hash_set_size,
            dedup_visitor_time,
            action_transform_time,
            predicate_eval_time,
        } = self;
        write!(
            f,
            "ScanMetadataCompleted(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, scan_type={scan_type}, duration={duration:?}, \
             add_files_seen={num_add_files_seen}, \
             add_files_seen_from_delta_files={num_add_files_seen_from_delta_files}, \
             selected_add_files={num_selected_add_files}, \
             selected_add_files_bytes={selected_add_files_bytes}, \
             remove_files_seen_from_delta_files={num_remove_files_seen_from_delta_files}, \
             non_file_actions={num_non_file_actions}, \
             predicate_filtered={num_predicate_filtered}, peak_hash_set_size={peak_hash_set_size}, \
             dedup_visitor_time={dedup_visitor_time:?}, \
             action_transform_time={action_transform_time:?}, \
             predicate_eval_time={predicate_eval_time:?})"
        )
    }
}

#[derive(Default)]
struct ScanMetadataCompletedAttrs {
    scan_type: String,
    duration_ns: u64,
    num_add_files_seen: u64,
    num_add_files_seen_from_delta_files: u64,
    num_selected_add_files: u64,
    selected_add_files_bytes: u64,
    num_remove_files_seen_from_delta_files: u64,
    num_non_file_actions: u64,
    num_predicate_filtered: u64,
    peak_hash_set_size: u64,
    dedup_visitor_time_ns: u64,
    action_transform_time_ns: u64,
    predicate_eval_time_ns: u64,
}

impl Visit for ScanMetadataCompletedAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "duration_ns" => self.duration_ns = value,
            "num_add_files_seen" => self.num_add_files_seen = value,
            "num_add_files_seen_from_delta_files" => {
                self.num_add_files_seen_from_delta_files = value
            }
            "num_selected_add_files" => self.num_selected_add_files = value,
            "selected_add_files_bytes" => self.selected_add_files_bytes = value,
            "num_remove_files_seen_from_delta_files" => {
                self.num_remove_files_seen_from_delta_files = value
            }
            "num_non_file_actions" => self.num_non_file_actions = value,
            "num_predicate_filtered" => self.num_predicate_filtered = value,
            "peak_hash_set_size" => self.peak_hash_set_size = value,
            "dedup_visitor_time_ns" => self.dedup_visitor_time_ns = value,
            "action_transform_time_ns" => self.action_transform_time_ns = value,
            "predicate_eval_time_ns" => self.predicate_eval_time_ns = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "scan_type" {
            self.scan_type = format!("{value:?}");
        }
    }
}

// ====================================================================
// Storage events (List / Read / Copy)
// ====================================================================
//
// All three storage events share the `STORAGE_SPAN` span name and distinguish themselves via
// a `name=` field carrying one of `<event>::NAME`. The shared [`storage_metric_from_attrs`]
// helper inspects the `name` value and constructs the matching variant.

pub(crate) const STORAGE_SPAN: &str = "storage";

/// Build the appropriate storage `MetricEvent` from the span attributes. Returns `None` if the
/// `name` field is missing or unknown.
pub(crate) fn storage_metric_from_attrs(attrs: &Attributes<'_>) -> Option<MetricEvent> {
    let mut v = StorageAttrs::default();
    attrs.record(&mut v);
    let duration = Duration::from_nanos(v.duration_ns);
    match v.kind {
        StorageKind::List => Some(MetricEvent::StorageListCompleted(StorageListCompleted {
            duration,
            num_files: v.num_files,
        })),
        StorageKind::Read => Some(MetricEvent::StorageReadCompleted(StorageReadCompleted {
            duration,
            num_files: v.num_files,
            bytes_read: v.bytes_read,
        })),
        StorageKind::Copy => Some(MetricEvent::StorageCopyCompleted(StorageCopyCompleted {
            duration,
        })),
        StorageKind::Unknown => None,
    }
}

#[derive(Default)]
enum StorageKind {
    #[default]
    Unknown,
    List,
    Read,
    Copy,
}

#[derive(Default)]
struct StorageAttrs {
    kind: StorageKind,
    num_files: u64,
    bytes_read: u64,
    duration_ns: u64,
}

impl Visit for StorageAttrs {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "name" {
            self.kind = match value {
                StorageListCompleted::NAME => StorageKind::List,
                StorageReadCompleted::NAME => StorageKind::Read,
                StorageCopyCompleted::NAME => StorageKind::Copy,
                _ => {
                    warn!("Storage span with unknown name: {value}");
                    StorageKind::Unknown
                }
            };
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_files" => self.num_files = value,
            "bytes_read" => self.bytes_read = value,
            "duration_ns" => self.duration_ns = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

// ============================
// StorageListCompleted
// ============================

/// A storage list operation completed.
#[derive(Debug, Clone)]
pub struct StorageListCompleted {
    // === Set on span creation ===
    pub duration: Duration,
    pub num_files: u64,
}

impl StorageListCompleted {
    pub(crate) const NAME: &'static str = "list_completed";
}

impl fmt::Display for StorageListCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            duration,
            num_files,
        } = self;
        write!(
            f,
            "StorageListCompleted(duration={duration:?}, files={num_files})"
        )
    }
}

// ============================
// StorageReadCompleted
// ============================

/// A storage read operation completed.
#[derive(Debug, Clone)]
pub struct StorageReadCompleted {
    // === Set on span creation ===
    pub duration: Duration,
    pub num_files: u64,
    pub bytes_read: u64,
}

impl StorageReadCompleted {
    pub(crate) const NAME: &'static str = "read_completed";
}

impl fmt::Display for StorageReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            duration,
            num_files,
            bytes_read,
        } = self;
        write!(
            f,
            "StorageReadCompleted(duration={duration:?}, files={num_files}, bytes={bytes_read})"
        )
    }
}

// ============================
// StorageCopyCompleted
// ============================

/// A storage copy or rename operation completed.
#[derive(Debug, Clone)]
pub struct StorageCopyCompleted {
    // === Set on span creation ===
    pub duration: Duration,
}

impl StorageCopyCompleted {
    pub(crate) const NAME: &'static str = "copy_completed";
}

impl fmt::Display for StorageCopyCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { duration } = self;
        write!(f, "StorageCopyCompleted(duration={duration:?})")
    }
}

// ====================================================================
// emit_* helpers
// ====================================================================
//
// Fire a tracing span carrying a pre-built event payload. Used by the scan metadata emission
// site (which constructs the event up front) and by connector-side `JsonHandler` /
// `ParquetHandler` implementations that want to report read metrics.

/// Emit a [`MetricEvent::JsonReadCompleted`] via a tracing span.
///
/// Call once per [`crate::JsonHandler::read_json_files`] invocation, at iterator exhaustion or
/// drop.
pub fn emit_json_read_completed(num_files: u64, bytes_read: u64) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        JsonReadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a [`MetricEvent::ParquetReadCompleted`] via a tracing span.
///
/// Call once per [`crate::ParquetHandler::read_parquet_files`] invocation, at iterator exhaustion
/// or drop.
pub fn emit_parquet_read_completed(num_files: u64, bytes_read: u64) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ParquetReadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        num_files,
        bytes_read,
    );
}

/// Emit a [`MetricEvent::ScanMetadataCompleted`] via a tracing span. Call when the scan metadata
/// iterator is exhausted or dropped.
pub(crate) fn emit_scan_metadata_completed(e: &ScanMetadataCompleted) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ScanMetadataCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %e.context.operation_id,
        is_catalog_managed = e.context.table_type.is_catalog_managed(),
        correlation_id = e.context.correlation_id.as_deref().unwrap_or(""),
        scan_type = %e.scan_type,
        duration_ns = e.duration.as_nanos() as u64,
        num_add_files_seen = e.num_add_files_seen,
        num_add_files_seen_from_delta_files = e.num_add_files_seen_from_delta_files,
        num_selected_add_files = e.num_selected_add_files,
        selected_add_files_bytes = e.selected_add_files_bytes,
        num_remove_files_seen_from_delta_files = e.num_remove_files_seen_from_delta_files,
        num_non_file_actions = e.num_non_file_actions,
        num_predicate_filtered = e.num_predicate_filtered,
        peak_hash_set_size = e.peak_hash_set_size as u64,
        dedup_visitor_time_ns = e.dedup_visitor_time.as_nanos() as u64,
        action_transform_time_ns = e.action_transform_time.as_nanos() as u64,
        predicate_eval_time_ns = e.predicate_eval_time.as_nanos() as u64,
    );
}

/// Emit the result and elapsed time of one log-segment load.
pub(crate) fn emit_log_segment_load(
    ctx: &SnapshotLoadMetricContext,
    outcome: LogSegmentLoadOutcome,
    duration: Duration,
) {
    let (commits, checkpoints, compactions, crc_versions_behind) = match &outcome {
        LogSegmentLoadOutcome::Success {
            stats:
                LogSegmentLoadStats {
                    num_commit_files,
                    num_checkpoint_files,
                    num_compaction_files,
                    crc_versions_behind,
                },
        } => (
            *num_commit_files,
            *num_checkpoint_files,
            *num_compaction_files,
            *crc_versions_behind,
        ),
        LogSegmentLoadOutcome::Error => (0, 0, 0, None),
    };
    let _span = tracing::span!(
        tracing::Level::INFO,
        LogSegmentLoadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %ctx.context.operation_id,
        is_catalog_managed = ctx.context.table_type.is_catalog_managed(),
        correlation_id = ctx.context.correlation_id.as_deref().unwrap_or(""),
        load_type = ctx.load_type.as_ref(),
        outcome = outcome.as_ref(),
        num_commit_files = commits,
        num_checkpoint_files = checkpoints,
        num_compaction_files = compactions,
        crc_versions_behind = LogSegmentLoadCompleted::encode_crc_versions_behind(crc_versions_behind),
        duration_ns = duration.as_nanos() as u64,
    );
}

/// Emit the result and elapsed time of one protocol/metadata resolution.
pub(crate) fn emit_protocol_metadata_load(
    ctx: &SnapshotLoadMetricContext,
    outcome: ProtocolMetadataLoadOutcome,
    duration: Duration,
) {
    let source = match &outcome {
        ProtocolMetadataLoadOutcome::Success { source } => *source,
        ProtocolMetadataLoadOutcome::Error => ProtocolMetadataSource::Unknown,
    };
    let _span = tracing::span!(
        tracing::Level::INFO,
        ProtocolMetadataLoadCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %ctx.context.operation_id,
        is_catalog_managed = ctx.context.table_type.is_catalog_managed(),
        correlation_id = ctx.context.correlation_id.as_deref().unwrap_or(""),
        load_type = ctx.load_type.as_ref(),
        outcome = outcome.as_ref(),
        pm_source = source.as_ref(),
        duration_ns = duration.as_nanos() as u64,
    );
}

/// Read the final classification after all creation fields have been decoded.
fn has_error_outcome(attrs: &Attributes<'_>) -> bool {
    struct Outcome(bool);
    impl Visit for Outcome {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "outcome" {
                match value {
                    "error" => self.0 = true,
                    "success" => {}
                    _ => warn!("Invalid outcome '{value}' on metric span. Using success."),
                }
            }
        }
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
    let mut outcome = Outcome(false);
    attrs.record(&mut outcome);
    outcome.0
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tracing::Callsite;

    use super::*;

    #[test]
    fn load_visitors_record_shared_fields_after_error() {
        let callsite = tracing::callsite! {
            name: "load_fields",
            kind: tracing::metadata::Kind::SPAN,
            fields: operation_id, correlation_id, is_catalog_managed, load_type, duration_ns,
                num_commit_files, num_checkpoint_files, num_compaction_files, crc_versions_behind,
                pm_source,
        };
        let fields = callsite.metadata().fields();
        let field = |name| fields.field(name).unwrap();
        let id = MetricId::new();
        let mut log_segment = LogSegmentLoadCompleted {
            outcome: LogSegmentLoadOutcome::Error,
            ..LogSegmentLoadCompleted::empty()
        };
        let mut protocol_metadata = ProtocolMetadataLoadCompleted {
            outcome: ProtocolMetadataLoadOutcome::Error,
            ..ProtocolMetadataLoadCompleted::empty()
        };

        for visitor in [
            &mut log_segment as &mut dyn Visit,
            &mut protocol_metadata as &mut dyn Visit,
        ] {
            visitor.record_str(&field("operation_id"), &id.to_string());
            visitor.record_str(&field("correlation_id"), "request-1");
            visitor.record_bool(&field("is_catalog_managed"), true);
            visitor.record_str(&field("load_type"), "incremental");
            visitor.record_u64(&field("duration_ns"), 53);
        }
        log_segment.record_u64(&field("num_commit_files"), 3);
        log_segment.record_u64(&field("num_checkpoint_files"), 1);
        log_segment.record_u64(&field("num_compaction_files"), 2);
        log_segment.record_i64(&field("crc_versions_behind"), 4);
        protocol_metadata.record_str(&field("pm_source"), "full_replay");

        for (context, load_type, duration) in [
            (
                &log_segment.context,
                log_segment.load_type,
                log_segment.duration,
            ),
            (
                &protocol_metadata.context,
                protocol_metadata.load_type,
                protocol_metadata.duration,
            ),
        ] {
            assert_eq!(context.operation_id, id);
            assert_eq!(context.correlation_id.as_deref(), Some("request-1"));
            assert_eq!(context.table_type, TableType::CatalogManaged);
            assert_eq!(load_type, LogSegmentLoadType::Incremental);
            assert_eq!(duration, Duration::from_nanos(53));
        }
        assert_eq!(log_segment.outcome, LogSegmentLoadOutcome::Error);
        assert_eq!(
            protocol_metadata.outcome,
            ProtocolMetadataLoadOutcome::Error
        );
    }

    fn commit_success(operation_id: MetricId) -> TransactionCommitCompleted {
        TransactionCommitCompleted {
            context: MetricContext {
                operation_id,
                table_type: TableType::PathBased,
                correlation_id: None,
            },
            duration: Duration::default(),
            outcome: TransactionCommitOutcome::Success {
                stats: TransactionCommitStats {
                    commit_version: 1,
                    num_add_files: 0,
                    num_remove_files: 0,
                    num_dv_updates: 0,
                    add_files_bytes: 0,
                    remove_files_bytes: 0,
                    is_blind_append: false,
                    data_change: false,
                    operation: None,
                    prepare_duration: Duration::default(),
                    committer_duration: Duration::default(),
                },
            },
        }
    }

    #[rstest]
    #[case::conflict("conflict", CommitFailureReason::Conflict)]
    #[case::retryable_io("retryable_io", CommitFailureReason::RetryableIo)]
    #[case::error("error", CommitFailureReason::Error)]
    #[case::unknown_defaults_to_error("totally_unknown", CommitFailureReason::Error)]
    fn record_str_failure_reason_flips_to_expected_reason(
        #[case] value: &str,
        #[case] expected: CommitFailureReason,
    ) {
        let id = MetricId::new();
        let mut event = MetricEvent::TransactionCommitCompleted(commit_success(id));
        let recorded = event.record_str("failure_reason", value);
        assert_eq!(recorded.is_err(), value == "totally_unknown");
        let MetricEvent::TransactionCommitCompleted(
            failure @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Failure { .. },
                ..
            },
        ) = event
        else {
            panic!("expected TransactionCommitCompleted");
        };
        assert_eq!(failure.context.operation_id, id);

        let TransactionCommitOutcome::Failure {
            reason: metric_failure_reason,
            ..
        } = &failure.outcome
        else {
            panic!("unexpected metric outcome");
        };
        assert_eq!((*metric_failure_reason), expected);
    }

    #[rstest]
    #[case("success", LastCheckpointReadOutcome::Success)]
    #[case("not_found", LastCheckpointReadOutcome::NotFound)]
    #[case("invalid", LastCheckpointReadOutcome::Invalid)]
    #[case("error", LastCheckpointReadOutcome::Error)]
    #[case("unknown", LastCheckpointReadOutcome::Unknown)]
    fn last_checkpoint_outcome_has_stable_label(
        #[case] label: &str,
        #[case] outcome: LastCheckpointReadOutcome,
    ) {
        let static_label: &'static str = outcome.into();
        assert_eq!(static_label, label);
        assert_eq!(label.parse::<LastCheckpointReadOutcome>().unwrap(), outcome);
        let mut event = MetricEvent::LastCheckpointReadCompleted(LastCheckpointReadCompleted {
            outcome,
            duration: Duration::ZERO,
        });
        event.set_duration_if_applicable(Duration::from_nanos(17));
        assert_eq!(
            event.to_string(),
            format!("LastCheckpointReadCompleted(outcome={label}, duration=17ns)")
        );
        assert_eq!(
            event.record_u64("outcome", 1),
            Err(LastCheckpointReadCompleted::SPAN_NAME)
        );
        assert_eq!(
            event.record_bool("outcome", false),
            Err(LastCheckpointReadCompleted::SPAN_NAME)
        );
        assert_eq!(
            event.record_str("failure_reason", "error"),
            Err(LastCheckpointReadCompleted::SPAN_NAME)
        );
    }

    #[test]
    fn record_str_operation_sets_field_without_flipping() {
        let mut event = MetricEvent::TransactionCommitCompleted(commit_success(MetricId::new()));
        event.record_str("operation", "WRITE").unwrap();
        let MetricEvent::TransactionCommitCompleted(
            success @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Success { .. },
                ..
            },
        ) = event
        else {
            panic!("expected TransactionCommitCompleted");
        };

        let TransactionCommitOutcome::Success { stats } = &success.outcome else {
            panic!("unexpected metric outcome");
        };
        assert_eq!(stats.operation.as_deref(), Some("WRITE"));
    }

    #[test]
    fn record_u64_num_dv_updates_sets_field() {
        let mut event = MetricEvent::TransactionCommitCompleted(commit_success(MetricId::new()));
        event.record_u64("num_dv_updates", 7).unwrap();
        let MetricEvent::TransactionCommitCompleted(
            success @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Success { .. },
                ..
            },
        ) = event
        else {
            panic!("expected TransactionCommitCompleted");
        };

        let TransactionCommitOutcome::Success { stats } = &success.outcome else {
            panic!("unexpected metric outcome");
        };
        assert_eq!(stats.num_dv_updates, 7);
    }

    #[rstest]
    #[case::conflict(CommitFailureReason::Conflict, "conflict")]
    #[case::retryable_io(CommitFailureReason::RetryableIo, "retryable_io")]
    #[case::error(CommitFailureReason::Error, "error")]
    fn commit_failure_reason_serializes_to_wire_name_and_parses_back(
        #[case] reason: CommitFailureReason,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = reason.into();
        assert_eq!(serialized, wire);
        assert_eq!(CommitFailureReason::from_str(wire).unwrap(), reason);
    }

    #[rstest]
    #[case::written_zero(
        CheckpointWriteOutcome::Written { stats: CheckpointWriteStats::default() },
        "written"
    )]
    #[case::written_nonzero(
        CheckpointWriteOutcome::Written {
            stats: CheckpointWriteStats {
                size_in_bytes: 50,
                num_add_files: 1,
                num_remove_files: 2,
                num_actions: 7,
                num_sidecars: 1,
            }
        },
        "written"
    )]
    #[case::already_exists(CheckpointWriteOutcome::AlreadyExists, "already_exists")]
    #[case::error(CheckpointWriteOutcome::Error, "error")]
    fn checkpoint_write_outcome_serializes_to_wire_name(
        #[case] outcome: CheckpointWriteOutcome,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = outcome.into();
        assert_eq!(serialized, wire);
        assert_eq!(outcome.as_ref(), wire);
        assert_eq!(outcome.to_string(), wire);
    }

    #[rstest]
    #[case::not_attempted(CheckpointWriteMode::NotAttempted, "not_attempted")]
    #[case::v1(CheckpointWriteMode::V1, "v1")]
    #[case::v2_without_sidecars(CheckpointWriteMode::V2WithoutSidecars, "v2_without_sidecars")]
    #[case::v2_with_sidecars(CheckpointWriteMode::V2WithSidecars, "v2_with_sidecars")]
    fn checkpoint_write_mode_serializes_to_wire_name_and_parses_back(
        #[case] mode: CheckpointWriteMode,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = mode.into();
        assert_eq!(serialized, wire);
        assert_eq!(mode.as_ref(), wire);
        assert_eq!(mode.to_string(), wire);
        assert_eq!(CheckpointWriteMode::from_str(wire).unwrap(), mode);
    }

    #[rstest]
    #[case::sequential(ScanType::SequentialPhase, "sequential")]
    #[case::parallel(ScanType::ParallelPhase, "parallel")]
    #[case::full(ScanType::Full, "full")]
    fn scan_type_serializes_to_wire_name_and_parses_back(
        #[case] scan_type: ScanType,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = scan_type.into();
        assert_eq!(serialized, wire);
        assert_eq!(ScanType::from_str(wire).unwrap(), scan_type);
    }

    #[rstest]
    #[case::known("parallel", ScanType::ParallelPhase)]
    #[case::unknown_defaults_to_full("totally_unknown", ScanType::Full)]
    fn scan_type_parse_lenient_maps_unknown_to_full(
        #[case] value: &str,
        #[case] expected: ScanType,
    ) {
        assert_eq!(ScanType::parse_lenient(value), expected);
    }

    #[rstest]
    #[case::full(SnapshotLoadType::Full, "full")]
    #[case::incremental(SnapshotLoadType::Incremental, "incremental")]
    #[case::snapshot_hint(SnapshotLoadType::SnapshotHint, "snapshot_hint")]
    #[case::unknown(SnapshotLoadType::Unknown, "unknown")]
    fn snapshot_load_type_serializes_to_wire_name_and_parses_back(
        #[case] load_type: SnapshotLoadType,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = load_type.into();
        assert_eq!(serialized, wire);
        assert_eq!(SnapshotLoadType::from_str(wire).unwrap(), load_type);
    }

    #[rstest]
    #[case::known("incremental", SnapshotLoadType::Incremental)]
    #[case::empty_maps_to_unknown("", SnapshotLoadType::Unknown)]
    #[case::unrecognized_maps_to_unknown("totally_unknown", SnapshotLoadType::Unknown)]
    fn snapshot_load_type_parse_or_unknown(
        #[case] value: &str,
        #[case] expected: SnapshotLoadType,
    ) {
        assert_eq!(SnapshotLoadType::parse_or_unknown(value), expected);
    }

    #[rstest]
    #[case::full(LogSegmentLoadType::Full, "full")]
    #[case::incremental(LogSegmentLoadType::Incremental, "incremental")]
    #[case::unknown(LogSegmentLoadType::Unknown, "unknown")]
    fn log_segment_load_type_serializes_to_wire_name_and_parses_back(
        #[case] load_type: LogSegmentLoadType,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = load_type.into();
        assert_eq!(serialized, wire);
        assert_eq!(LogSegmentLoadType::from_str(wire).unwrap(), load_type);
    }

    #[rstest]
    #[case::known("incremental", LogSegmentLoadType::Incremental)]
    #[case::empty_maps_to_unknown("", LogSegmentLoadType::Unknown)]
    #[case::unrecognized_maps_to_unknown("totally_unknown", LogSegmentLoadType::Unknown)]
    fn log_segment_load_type_parse_or_unknown(
        #[case] value: &str,
        #[case] expected: LogSegmentLoadType,
    ) {
        assert_eq!(LogSegmentLoadType::parse_or_unknown(value), expected);
    }

    #[rstest]
    #[case::none(None)]
    #[case::zero(Some(0))]
    #[case::some(Some(7))]
    fn crc_versions_behind_sentinel_round_trips(#[case] v: Option<u64>) {
        let encoded = LogSegmentLoadCompleted::encode_crc_versions_behind(v);
        assert_eq!(
            LogSegmentLoadCompleted::decode_crc_versions_behind(encoded),
            v
        );
    }

    #[rstest]
    #[case::crc_at_target(ProtocolMetadataSource::CrcAtTarget, "crc_at_target")]
    #[case::crc_advanced(ProtocolMetadataSource::CrcAdvancedByReplay, "crc_advanced_by_replay")]
    #[case::crc_seeded(
        ProtocolMetadataSource::CrcSeededPmOnlyReplay,
        "crc_seeded_pm_only_replay"
    )]
    #[case::full_replay(ProtocolMetadataSource::FullReplay, "full_replay")]
    #[case::unknown(ProtocolMetadataSource::Unknown, "unknown")]
    fn protocol_metadata_source_serializes_to_wire_name_and_parses_back(
        #[case] source: ProtocolMetadataSource,
        #[case] wire: &str,
    ) {
        let serialized: &'static str = source.into();
        assert_eq!(serialized, wire);
        assert_eq!(ProtocolMetadataSource::from_str(wire).unwrap(), source);
    }

    #[rstest]
    #[case::known("crc_at_target", ProtocolMetadataSource::CrcAtTarget)]
    #[case::empty_maps_to_unknown("", ProtocolMetadataSource::Unknown)]
    #[case::unrecognized_maps_to_unknown("totally_unknown", ProtocolMetadataSource::Unknown)]
    fn protocol_metadata_source_parse_or_unknown(
        #[case] value: &str,
        #[case] expected: ProtocolMetadataSource,
    ) {
        assert_eq!(ProtocolMetadataSource::parse_or_unknown(value), expected);
    }

    #[rstest]
    fn snapshot_load_failure_display_includes_context_and_duration(
        #[values(Duration::ZERO, Duration::from_nanos(73))] duration: Duration,
        #[values(None, Some("snapshot-request"))] correlation_id: Option<&str>,
    ) {
        let operation_id = MetricId::new();
        let correlation_id = correlation_id.map(Arc::<str>::from);
        let table_type = TableType::CatalogManaged;
        let events = [
            (
                "LogSegmentLoadCompleted",
                MetricEvent::LogSegmentLoadCompleted(LogSegmentLoadCompleted {
                    context: MetricContext {
                        operation_id,
                        correlation_id: correlation_id.clone(),
                        table_type,
                    },
                    load_type: LogSegmentLoadType::Incremental,
                    duration,
                    outcome: LogSegmentLoadOutcome::Error,
                }),
            ),
            (
                "ProtocolMetadataLoadCompleted",
                MetricEvent::ProtocolMetadataLoadCompleted(ProtocolMetadataLoadCompleted {
                    context: MetricContext {
                        operation_id,
                        correlation_id: correlation_id.clone(),
                        table_type,
                    },
                    load_type: LogSegmentLoadType::Incremental,
                    duration,
                    outcome: ProtocolMetadataLoadOutcome::Error,
                }),
            ),
            (
                "SnapshotBuildCompleted",
                MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                    context: MetricContext {
                        operation_id,
                        correlation_id: correlation_id.clone(),
                        table_type,
                    },
                    load_type: SnapshotLoadType::Incremental,
                    duration,
                    outcome: SnapshotBuildOutcome::Error,
                }),
            ),
        ];
        for (name, event) in events {
            assert_eq!(
                event.to_string(),
                format!(
                    "{name}(id={operation_id}, table_type=catalog_managed, \
                     correlation_id={correlation_id:?}, load_type=incremental, duration={duration:?}, outcome=Error)"
                )
            );
        }
    }

    #[test]
    fn mark_error_maps_commit_success_to_error_reason() {
        let id = MetricId::new();
        let mut failure = MetricEvent::TransactionCommitCompleted(commit_success(id));
        failure.mark_error();
        let MetricEvent::TransactionCommitCompleted(
            failure @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Failure { .. },
                ..
            },
        ) = failure
        else {
            panic!("expected TransactionCommitCompleted");
        };
        assert_eq!(failure.context.operation_id, id);

        let TransactionCommitOutcome::Failure {
            reason: metric_failure_reason,
            ..
        } = &failure.outcome
        else {
            panic!("unexpected metric outcome");
        };
        assert_eq!((*metric_failure_reason), CommitFailureReason::Error);
    }

    #[test]
    fn record_str_failure_reason_flip_preserves_correlation_id() {
        let mut success = commit_success(MetricId::new());
        success.context.correlation_id = Some("commit-req-1".into());
        let mut event = MetricEvent::TransactionCommitCompleted(success);
        event.record_str("failure_reason", "conflict").unwrap();
        let MetricEvent::TransactionCommitCompleted(
            failure @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Failure { .. },
                ..
            },
        ) = event
        else {
            panic!("expected TransactionCommitCompleted");
        };
        assert_eq!(
            failure.context.correlation_id.as_deref(),
            Some("commit-req-1")
        );
    }

    #[test]
    fn mark_error_preserves_correlation_id() {
        let mut success = commit_success(MetricId::new());
        success.context.correlation_id = Some("commit-req-2".into());
        let mut event = MetricEvent::TransactionCommitCompleted(success);
        event.mark_error();
        let MetricEvent::TransactionCommitCompleted(
            failure @ TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Failure { .. },
                ..
            },
        ) = event
        else {
            panic!("expected TransactionCommitCompleted");
        };
        assert_eq!(
            failure.context.correlation_id.as_deref(),
            Some("commit-req-2")
        );

        let snapshot = SnapshotBuildCompleted {
            context: MetricContext {
                operation_id: MetricId::new(),
                table_type: TableType::PathBased,
                correlation_id: Some("snap-req-3".into()),
            },
            load_type: SnapshotLoadType::Incremental,
            duration: Duration::from_nanos(37),
            outcome: SnapshotBuildOutcome::Success { version: 0 },
        };
        let mut event = MetricEvent::SnapshotBuildCompleted(snapshot);
        event.mark_error();
        let MetricEvent::SnapshotBuildCompleted(
            failure @ SnapshotBuildCompleted {
                outcome: SnapshotBuildOutcome::Error,
                ..
            },
        ) = event
        else {
            panic!("expected SnapshotBuildCompleted");
        };
        assert_eq!(
            failure.context.correlation_id.as_deref(),
            Some("snap-req-3")
        );
        assert_eq!(failure.load_type, SnapshotLoadType::Incremental);
        assert_eq!(failure.duration, Duration::from_nanos(37));
    }

    #[rstest]
    #[case(ChecksumWriteOutcome::Written { bytes_written: 0 }, "written")]
    #[case(ChecksumWriteOutcome::Written { bytes_written: 100 }, "written")]
    #[case(ChecksumWriteOutcome::AlreadyExists, "already_exists")]
    #[case(ChecksumWriteOutcome::Error, "error")]
    fn crc_write_outcome_has_stable_label(
        #[case] outcome: ChecksumWriteOutcome,
        #[case] label: &str,
    ) {
        assert_eq!(outcome.as_ref(), label);
        assert_eq!(outcome.to_string(), label);
    }

    #[rstest]
    #[case(ChecksumWriteResolutionMode::Full, "full")]
    #[case(ChecksumWriteResolutionMode::Incremental, "incremental")]
    #[case(ChecksumWriteResolutionMode::Cached, "cached")]
    #[case(ChecksumWriteResolutionMode::NotAttempted, "not_attempted")]
    fn crc_write_resolution_mode_has_stable_label(
        #[case] mode: ChecksumWriteResolutionMode,
        #[case] label: &str,
    ) {
        assert_eq!(mode.as_ref(), label);
        assert_eq!(mode.to_string(), label);
        assert_eq!(label.parse::<ChecksumWriteResolutionMode>().unwrap(), mode);
    }

    #[rstest]
    fn crc_write_failure_preserves_mode_and_duration_but_discards_written_payload(
        #[values(
            ChecksumWriteResolutionMode::Full,
            ChecksumWriteResolutionMode::Incremental,
            ChecksumWriteResolutionMode::Cached,
            ChecksumWriteResolutionMode::NotAttempted
        )]
        mode: ChecksumWriteResolutionMode,
    ) {
        let duration = Duration::from_millis(42);
        let mut event = MetricEvent::ChecksumWriteCompleted(ChecksumWriteCompleted {
            outcome: ChecksumWriteOutcome::Written { bytes_written: 100 },
            resolution_mode: mode,
            duration,
        });
        event.mark_error();
        let MetricEvent::ChecksumWriteCompleted(
            event @ ChecksumWriteCompleted {
                outcome: ChecksumWriteOutcome::Error,
                ..
            },
        ) = event
        else {
            panic!("expected ChecksumWriteCompleted");
        };
        assert_eq!(event.resolution_mode, mode);
        assert_eq!(event.duration, duration);
    }
    #[rstest]
    #[case(CommitFailureReason::Conflict)]
    #[case(CommitFailureReason::RetryableIo)]
    #[case(CommitFailureReason::Error)]
    fn commit_failure_reason_survives_generic_error_and_late_success_fields(
        #[case] reason: CommitFailureReason,
    ) {
        let mut event = MetricEvent::TransactionCommitCompleted(commit_success(MetricId::new()));
        event.record_str("failure_reason", reason.as_ref()).unwrap();
        event.mark_error();
        event.mark_error();
        event.record_u64("num_add_files", 7).unwrap();
        event.record_bool("data_change", true).unwrap();
        event.record_str("operation", "WRITE").unwrap();
        assert!(matches!(
            event,
            MetricEvent::TransactionCommitCompleted(TransactionCommitCompleted {
                outcome: TransactionCommitOutcome::Failure {
                    reason: actual
                },
                ..
            }) if actual == reason
        ));
    }

    #[rstest]
    #[case::snapshot(
        MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
            context: MetricContext::empty(), load_type: SnapshotLoadType::Full,
            duration: Duration::ZERO, outcome: SnapshotBuildOutcome::Success { version: 0 },
        }),
        Some("version"), &[],
    )]
    #[case::domain(
        MetricEvent::DomainMetadataLoadCompleted(DomainMetadataLoadCompleted {
            duration: Duration::ZERO,
            outcome: DomainMetadataLoadOutcome::Success { from_cache: false, num_domains_returned: 0 },
        }),
        Some("num_domains_returned"), &["from_cache"],
    )]
    #[case::set_transaction(
        MetricEvent::SetTransactionLoadCompleted(SetTransactionLoadCompleted {
            duration: Duration::ZERO,
            outcome: SetTransactionLoadOutcome::Success { from_cache: false, found: false },
        }),
        None, &["from_cache", "found"],
    )]
    #[case::crc(
        MetricEvent::CrcReadCompleted(CrcReadCompleted {
            duration: Duration::ZERO, outcome: CrcReadOutcome::Success { bytes_read: 0 },
        }),
        Some("bytes_read"), &[],
    )]
    fn late_read_measurements_preserve_error_and_receive_elapsed_time(
        #[case] mut event: MetricEvent,
        #[case] count_field: Option<&str>,
        #[case] bool_fields: &[&str],
    ) {
        event.mark_error();
        if let Some(field) = count_field {
            event.record_u64(field, 37).unwrap();
        }
        for field in bool_fields {
            event.record_bool(field, true).unwrap();
        }
        event.mark_error();
        let elapsed = Duration::from_nanos(53);
        event.set_duration_if_applicable(elapsed);
        assert!(matches!(
            event,
            MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                duration, outcome: SnapshotBuildOutcome::Error, ..
            })
            | MetricEvent::DomainMetadataLoadCompleted(DomainMetadataLoadCompleted {
                duration, outcome: DomainMetadataLoadOutcome::Error,
            })
            | MetricEvent::SetTransactionLoadCompleted(SetTransactionLoadCompleted {
                duration, outcome: SetTransactionLoadOutcome::Error,
            })
            | MetricEvent::CrcReadCompleted(CrcReadCompleted {
                duration, outcome: CrcReadOutcome::Error,
            }) if duration == elapsed
        ));
    }

    #[rstest]
    #[case::checkpoint(MetricEvent::CheckpointWriteCompleted(CheckpointWriteCompleted {
        operation_id: MetricId::new(), version: 3,
        mode: CheckpointWriteMode::V1, duration: Duration::from_nanos(19),
        processing_stats: CheckpointProcessingStats::default(),
        output_stats: CheckpointOutputStats::default(),
        outcome: CheckpointWriteOutcome::Written { stats: CheckpointWriteStats::default() },
    }))]
    #[case::checksum(MetricEvent::ChecksumWriteCompleted(ChecksumWriteCompleted {
        resolution_mode: ChecksumWriteResolutionMode::Full,
        duration: Duration::from_nanos(19),
        outcome: ChecksumWriteOutcome::Written { bytes_written: 0 },
    }))]
    fn late_write_measurements_preserve_terminal_outcome(
        #[case] mut event: MetricEvent,
        #[values(false, true)] failed: bool,
    ) {
        if failed {
            event.mark_error();
        } else {
            event.record_str("outcome", "already_exists").unwrap();
        }
        event.record_u64("bytes_written", 31).unwrap();
        event.record_str("outcome", "already_exists").unwrap();
        match event {
            MetricEvent::CheckpointWriteCompleted(e) => {
                assert_eq!(
                    e.outcome,
                    if failed {
                        CheckpointWriteOutcome::Error
                    } else {
                        CheckpointWriteOutcome::AlreadyExists
                    }
                );
                assert_eq!(e.output_stats.bytes_written, 31);
                assert_eq!(e.duration, Duration::from_nanos(19));
            }
            MetricEvent::ChecksumWriteCompleted(e) => {
                assert_eq!(
                    e.outcome,
                    if failed {
                        ChecksumWriteOutcome::Error
                    } else {
                        ChecksumWriteOutcome::AlreadyExists
                    }
                );
                assert_eq!(e.duration, Duration::from_nanos(19));
            }
            _ => panic!("unexpected metric"),
        }
    }
}
