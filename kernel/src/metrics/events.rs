//! Metric event types emitted during Delta Kernel operations.
//!
//! Each [`MetricEvent`] variant wraps a per-event struct that owns its fields, `Display` impl,
//! span name, and the small set of methods the `tracing` layer in [`crate::metrics::reporter`]
//! calls to construct and finalize the event. Per-event code is colocated in a single block
//! per type below.
//!
//! Enums carried in event payloads derive the full strum set (`EnumString`, `Display`,
//! `AsRefStr`, `IntoStaticStr`) with stable serialized names. `IntoStaticStr` in particular
//! lets connectors convert a value to its `&'static str` metric-label string via `.into()`
//! instead of maintaining their own variant-to-string `match`.
//!
//! Event construction is infallible: a malformed span field warns and falls back to a
//! default rather than failing the operation being observed.

use std::fmt;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use delta_kernel_derive::internal_api;
use strum::{AsRefStr, Display as StrumDisplay, EnumString, IntoStaticStr};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::warn;
use uuid::Uuid;

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
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Return the 16 raw bytes of the underlying UUID. Useful for FFI consumers that want to
    /// carry the id without allocating or parsing its string form.
    pub fn as_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Extract the `operation_id` field from span attributes. Returns a nil id if the field is
    /// absent, or warns and returns nil if the value is present but malformed.
    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        #[derive(Default)]
        struct V(Uuid);
        impl Visit for V {
            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                if field.name() == "operation_id" {
                    let s = format!("{value:?}");
                    match Uuid::from_str(&s) {
                        Ok(u) => self.0 = u,
                        Err(e) => warn!("Invalid uuid '{s}' on span: {e}. Using default."),
                    }
                }
            }
        }
        let mut v = V::default();
        attrs.record(&mut v);
        Self(v.0)
    }
}

impl Default for MetricId {
    fn default() -> Self {
        Self::new()
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

/// Metric events emitted during Delta Kernel operations.
#[derive(Debug, Clone)]
pub enum MetricEvent {
    LogSegmentLoadSuccess(LogSegmentLoadSuccess),
    LogSegmentLoadFailure(LogSegmentLoadFailure),
    ProtocolMetadataLoadSuccess(ProtocolMetadataLoadSuccess),
    ProtocolMetadataLoadFailure(ProtocolMetadataLoadFailure),
    SnapshotBuildSuccess(SnapshotBuildSuccess),
    SnapshotBuildFailure(SnapshotBuildFailure),
    TransactionCommitSuccess(TransactionCommitSuccess),
    TransactionCommitFailure(TransactionCommitFailure),
    DomainMetadataLoadSuccess(DomainMetadataLoadSuccess),
    DomainMetadataLoadFailure,
    SetTransactionLoadSuccess(SetTransactionLoadSuccess),
    SetTransactionLoadFailure,
    CrcReadSuccess(CrcReadSuccess),
    CrcReadFailure,
    JsonReadCompleted(JsonReadCompleted),
    ParquetReadCompleted(ParquetReadCompleted),
    ScanMetadataCompleted(ScanMetadataCompleted),
    StorageListCompleted(StorageListCompleted),
    StorageReadCompleted(StorageReadCompleted),
    StorageCopyCompleted(StorageCopyCompleted),
}

impl MetricEvent {
    /// Set the wall-clock duration on lifecycle events.
    pub(crate) fn set_duration_if_applicable(&mut self, d: Duration) {
        match self {
            // Lifecycle success: duration must be set by the tracing layer on span close.
            Self::LogSegmentLoadSuccess(e) => e.set_duration(d),
            Self::SnapshotBuildSuccess(e) => e.set_duration(d),
            Self::TransactionCommitSuccess(e) => e.set_duration(d),
            Self::DomainMetadataLoadSuccess(e) => e.set_duration(d),
            Self::SetTransactionLoadSuccess(e) => e.set_duration(d),
            Self::CrcReadSuccess(e) => e.set_duration(d),

            // For now, failure events carry no duration; storage/scan/protocol-metadata events
            // set it at construction (emit-based); read events have no duration field.
            Self::ProtocolMetadataLoadSuccess(_)
            | Self::LogSegmentLoadFailure(_)
            | Self::ProtocolMetadataLoadFailure(_)
            | Self::SnapshotBuildFailure(_)
            | Self::TransactionCommitFailure(_)
            | Self::DomainMetadataLoadFailure
            | Self::SetTransactionLoadFailure
            | Self::CrcReadFailure
            | Self::ScanMetadataCompleted(_)
            | Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_)
            | Self::JsonReadCompleted(_)
            | Self::ParquetReadCompleted(_) => {}
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match self {
            // Variants with u64 fields set during span lifetime.
            Self::LogSegmentLoadSuccess(e) => e.record_u64(name, value),
            Self::SnapshotBuildSuccess(e) => e.record_u64(name, value),
            Self::TransactionCommitSuccess(e) => e.record_u64(name, value),
            Self::DomainMetadataLoadSuccess(e) => e.record_u64(name, value),
            Self::CrcReadSuccess(e) => e.record_u64(name, value),

            // No u64 fields set during span lifetime — a runtime record() on these is a bug.
            Self::ProtocolMetadataLoadSuccess(_) => Err(ProtocolMetadataLoadSuccess::SPAN_NAME),
            Self::SetTransactionLoadSuccess(_) => Err(SetTransactionLoadSuccess::SPAN_NAME),
            Self::ScanMetadataCompleted(_) => Err(ScanMetadataCompleted::SPAN_NAME),
            Self::JsonReadCompleted(_) => Err(JsonReadCompleted::SPAN_NAME),
            Self::ParquetReadCompleted(_) => Err(ParquetReadCompleted::SPAN_NAME),
            Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => Err(STORAGE_SPAN),

            // Failure events are built at span close, after any runtime records.
            Self::LogSegmentLoadFailure(_) => Err(LogSegmentLoadSuccess::SPAN_NAME),
            Self::ProtocolMetadataLoadFailure(_) => Err(ProtocolMetadataLoadSuccess::SPAN_NAME),
            Self::SnapshotBuildFailure(_) => Err(SnapshotBuildSuccess::SPAN_NAME),
            Self::TransactionCommitFailure(_) => Err(TransactionCommitSuccess::SPAN_NAME),
            Self::DomainMetadataLoadFailure => Err(DomainMetadataLoadSuccess::SPAN_NAME),
            Self::SetTransactionLoadFailure => Err(SetTransactionLoadSuccess::SPAN_NAME),
            Self::CrcReadFailure => Err(CrcReadSuccess::SPAN_NAME),
        }
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match self {
            // Variants with bool fields set during span lifetime.
            Self::LogSegmentLoadSuccess(e) => e.record_bool(name, value),
            Self::TransactionCommitSuccess(e) => e.record_bool(name, value),
            Self::DomainMetadataLoadSuccess(e) => e.record_bool(name, value),
            Self::SetTransactionLoadSuccess(e) => e.record_bool(name, value),

            // No bool fields set during span lifetime — a runtime record() on these is a bug.
            Self::ProtocolMetadataLoadSuccess(_) => Err(ProtocolMetadataLoadSuccess::SPAN_NAME),
            Self::SnapshotBuildSuccess(_) => Err(SnapshotBuildSuccess::SPAN_NAME),
            Self::CrcReadSuccess(_) => Err(CrcReadSuccess::SPAN_NAME),
            Self::ScanMetadataCompleted(_) => Err(ScanMetadataCompleted::SPAN_NAME),
            Self::JsonReadCompleted(_) => Err(JsonReadCompleted::SPAN_NAME),
            Self::ParquetReadCompleted(_) => Err(ParquetReadCompleted::SPAN_NAME),
            Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => Err(STORAGE_SPAN),

            // Failure events are built at span close, after any runtime records.
            Self::LogSegmentLoadFailure(_) => Err(LogSegmentLoadSuccess::SPAN_NAME),
            Self::ProtocolMetadataLoadFailure(_) => Err(ProtocolMetadataLoadSuccess::SPAN_NAME),
            Self::SnapshotBuildFailure(_) => Err(SnapshotBuildSuccess::SPAN_NAME),
            Self::TransactionCommitFailure(_) => Err(TransactionCommitSuccess::SPAN_NAME),
            Self::DomainMetadataLoadFailure => Err(DomainMetadataLoadSuccess::SPAN_NAME),
            Self::SetTransactionLoadFailure => Err(SetTransactionLoadSuccess::SPAN_NAME),
            Self::CrcReadFailure => Err(CrcReadSuccess::SPAN_NAME),
        }
    }

    pub(crate) fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        if name == "failure_reason" {
            if let Self::TransactionCommitSuccess(e) = self {
                let operation_id = e.operation_id;
                let table_type = e.table_type;
                let correlation_id = e.correlation_id.take();
                *self = Self::TransactionCommitFailure(TransactionCommitFailure {
                    operation_id,
                    table_type,
                    correlation_id,
                    reason: value.parse().unwrap_or_else(|e| {
                        warn!("Invalid failure_reason '{value}' on span: {e}. Using Error.");
                        CommitFailureReason::Error
                    }),
                });
            }
            return Ok(());
        }
        // Only TransactionCommitSuccess records string fields today. If other events need them,
        // dispatch per-variant like `record_u64`/`record_bool` above instead of this catch-all.
        match self {
            Self::TransactionCommitSuccess(e) => e.record_str(name, value),
            _ => Ok(()),
        }
    }

    /// Maps a lifecycle success event to its failure counterpart when the span errors.
    pub(crate) fn into_failure(self) -> Self {
        match self {
            Self::LogSegmentLoadSuccess(e) => Self::LogSegmentLoadFailure(LogSegmentLoadFailure {
                operation_id: e.operation_id,
                table_type: e.table_type,
                correlation_id: e.correlation_id,
                load_purpose: e.load_purpose,
            }),
            Self::ProtocolMetadataLoadSuccess(e) => {
                Self::ProtocolMetadataLoadFailure(ProtocolMetadataLoadFailure {
                    operation_id: e.operation_id,
                    table_type: e.table_type,
                    correlation_id: e.correlation_id,
                })
            }
            Self::SnapshotBuildSuccess(e) => Self::SnapshotBuildFailure(SnapshotBuildFailure {
                operation_id: e.operation_id,
                table_type: e.table_type,
                correlation_id: e.correlation_id,
                load_type: e.load_type,
            }),
            Self::TransactionCommitSuccess(e) => {
                Self::TransactionCommitFailure(TransactionCommitFailure {
                    operation_id: e.operation_id,
                    table_type: e.table_type,
                    correlation_id: e.correlation_id,
                    reason: CommitFailureReason::Error,
                })
            }
            Self::DomainMetadataLoadSuccess(_) => Self::DomainMetadataLoadFailure,
            Self::SetTransactionLoadSuccess(_) => Self::SetTransactionLoadFailure,
            Self::CrcReadSuccess(_) => Self::CrcReadFailure,
            // Events with no failure form pass through unchanged.
            // Note: here we list explicitly so adding a lifecycle event without a failure mapping
            //       fails to compile.
            e @ (Self::LogSegmentLoadFailure(_)
            | Self::ProtocolMetadataLoadFailure(_)
            | Self::SnapshotBuildFailure(_)
            | Self::TransactionCommitFailure(_)
            | Self::DomainMetadataLoadFailure
            | Self::SetTransactionLoadFailure
            | Self::CrcReadFailure
            | Self::JsonReadCompleted(_)
            | Self::ParquetReadCompleted(_)
            | Self::ScanMetadataCompleted(_)
            | Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_)) => e,
        }
    }
}

impl fmt::Display for MetricEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogSegmentLoadSuccess(e) => e.fmt(f),
            Self::LogSegmentLoadFailure(e) => e.fmt(f),
            Self::ProtocolMetadataLoadSuccess(e) => e.fmt(f),
            Self::ProtocolMetadataLoadFailure(e) => e.fmt(f),
            Self::SnapshotBuildSuccess(e) => e.fmt(f),
            Self::SnapshotBuildFailure(e) => e.fmt(f),
            Self::TransactionCommitSuccess(e) => e.fmt(f),
            Self::TransactionCommitFailure(e) => e.fmt(f),
            Self::DomainMetadataLoadSuccess(e) => e.fmt(f),
            Self::DomainMetadataLoadFailure => f.write_str("DomainMetadataLoadFailure"),
            Self::SetTransactionLoadSuccess(e) => e.fmt(f),
            Self::SetTransactionLoadFailure => f.write_str("SetTransactionLoadFailure"),
            Self::CrcReadSuccess(e) => e.fmt(f),
            Self::CrcReadFailure => f.write_str("CrcReadFailure"),
            Self::JsonReadCompleted(e) => e.fmt(f),
            Self::ParquetReadCompleted(e) => e.fmt(f),
            Self::ScanMetadataCompleted(e) => e.fmt(f),
            Self::StorageListCompleted(e) => e.fmt(f),
            Self::StorageReadCompleted(e) => e.fmt(f),
            Self::StorageCopyCompleted(e) => e.fmt(f),
        }
    }
}

// ====================================================================
// LogSegmentLoad
// ====================================================================
//
// Canonical example for the per-event block pattern. Other events below follow the same
// shape; detailed `///` docs on `from_attrs` and `record_*` live here only.

// Module-scope span name. `#[instrument(name = ...)]` only accepts a bare identifier here,
// not a multi-segment path like `Type::SPAN_NAME`.
pub(crate) const LOG_SEGMENT_LOADED_SPAN: &str = "segment.for_snapshot";

/// Which caller loaded the log segment. A single `LogSegmentLoad` metric slices by this label so
/// fresh and incremental snapshot loads (and, in the future, other segment loaders) share one
/// metric definition instead of re-declaring file-count denominators per caller.
///
/// Serializes to its `snake_case` name for the `load_purpose` span field.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, EnumString, StrumDisplay, AsRefStr, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum LogSegmentLoadPurpose {
    /// A fresh snapshot build listed the segment from scratch (`Snapshot::builder_for`).
    #[default]
    Snapshot,
    /// An incremental snapshot update re-listed the tail on top of an existing snapshot
    /// (`Snapshot::builder_from`).
    IncrementalSnapshot,
}

impl LogSegmentLoadPurpose {
    /// Empty means the field was not set (fresh `#[instrument]` path, which defaults to
    /// `Snapshot`); a non-empty unknown value warns.
    fn parse_lenient(s: &str) -> Self {
        if s.is_empty() {
            return Self::Snapshot;
        }
        Self::from_str(s).unwrap_or_else(|e| {
            warn!("Invalid load_purpose '{s}' on span: {e}. Using Snapshot.");
            Self::Snapshot
        })
    }
}

/// A log segment was listed and assembled for a snapshot.
#[derive(Debug, Clone)]
pub struct LogSegmentLoadSuccess {
    // === Set on span creation ===
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    /// Which caller loaded the segment. On the fresh path this is set on span creation; on the
    /// emit-based incremental path it is carried as a creation attr.
    pub load_purpose: LogSegmentLoadPurpose,

    // === Set during span lifetime (fresh path) or on creation (emit-based incremental path) ===
    pub num_commit_files: u64,
    pub num_checkpoint_files: u64,
    pub num_compaction_files: u64,
    pub has_latest_crc_file: bool,

    // === Set on span close (fresh path) or on creation (emit-based incremental path) ===
    pub duration: Duration,
}

impl LogSegmentLoadSuccess {
    pub(crate) const SPAN_NAME: &'static str = LOG_SEGMENT_LOADED_SPAN;

    /// Construction-time channel. Extracts fields bound at span creation via
    /// `#[instrument(fields(X = expr))]` or `tracing::span!(..., X = expr)`.
    ///
    /// The count/duration fields read here so the emit-based incremental path can pass them as
    /// creation attrs; on the fresh `#[instrument]` path they are absent at creation (default 0)
    /// and filled later via `record_u64` / span-close duration.
    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = LogSegmentLoadAttrs::default();
        attrs.record(&mut v);
        Self {
            operation_id: MetricId::from_attrs(attrs),
            table_type: TableType::from_catalog_managed(read_is_catalog_managed(attrs)),
            correlation_id: correlation_id_from_attrs(attrs),
            load_purpose: LogSegmentLoadPurpose::parse_lenient(&v.load_purpose),
            num_commit_files: v.num_commit_files,
            num_checkpoint_files: v.num_checkpoint_files,
            num_compaction_files: v.num_compaction_files,
            has_latest_crc_file: v.has_latest_crc_file,
            duration: Duration::from_nanos(v.duration_ns),
        }
    }

    /// Runtime channel. Dispatches a u64 field update from `Span::current().record(name, value)`
    /// to the matching field.
    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "num_commit_files" => self.num_commit_files = value,
            "num_checkpoint_files" => self.num_checkpoint_files = value,
            "num_compaction_files" => self.num_compaction_files = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match name {
            "has_latest_crc_file" => self.has_latest_crc_file = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    /// Set the wall-clock duration from the span's `on_close`. Only fills a still-zero duration:
    /// the emit-based incremental path provides its own duration as a creation attr, and the
    /// near-zero `on_close` elapsed of an immediately-dropped emit span must not clobber it. The
    /// fresh `#[instrument]` path has no creation-attr duration (starts at zero) and is filled
    /// here.
    pub(crate) fn set_duration(&mut self, d: Duration) {
        if self.duration.is_zero() {
            self.duration = d;
        }
    }
}

impl fmt::Display for LogSegmentLoadSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            load_purpose,
            duration,
            num_commit_files,
            num_checkpoint_files,
            num_compaction_files,
            has_latest_crc_file,
        } = self;
        write!(
            f,
            "LogSegmentLoadSuccess(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, load_purpose={load_purpose}, \
             duration={duration:?}, commits={num_commit_files}, \
             checkpoints={num_checkpoint_files}, compactions={num_compaction_files}, \
             has_latest_crc={has_latest_crc_file})"
        )
    }
}

/// Shared attribute decoder for `LogSegmentLoadSuccess`. The fresh `#[instrument]` path leaves the
/// count/duration fields as `Empty` at creation (they arrive via `record_u64` / span close), so
/// they default to zero here; the emit-based incremental path binds them as real creation attrs.
#[derive(Default)]
struct LogSegmentLoadAttrs {
    load_purpose: String,
    num_commit_files: u64,
    num_checkpoint_files: u64,
    num_compaction_files: u64,
    has_latest_crc_file: bool,
    duration_ns: u64,
}

impl Visit for LogSegmentLoadAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_commit_files" => self.num_commit_files = value,
            "num_checkpoint_files" => self.num_checkpoint_files = value,
            "num_compaction_files" => self.num_compaction_files = value,
            "duration_ns" => self.duration_ns = value,
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "has_latest_crc_file" {
            self.has_latest_crc_file = value;
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "load_purpose" {
            self.load_purpose = value.to_string();
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// Listing the log segment for a snapshot failed.
#[derive(Debug, Clone)]
pub struct LogSegmentLoadFailure {
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    /// Which caller attempted the load.
    pub load_purpose: LogSegmentLoadPurpose,
}

impl fmt::Display for LogSegmentLoadFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogSegmentLoadFailure(id={}, table_type={}, correlation_id={:?}, load_purpose={})",
            self.operation_id, self.table_type, self.correlation_id, self.load_purpose
        )
    }
}

// ====================================================================
// ProtocolMetadataLoad
// ====================================================================

pub(crate) const PROTOCOL_METADATA_LOADED_SPAN: &str = "segment.read_metadata";

/// Where a snapshot's Protocol and Metadata came from during load.
///
/// Note: on the CRC branches the load resolves more than P&M (file stats, domain metadata, set
/// transactions, ICT are carried by the CRC); the event is named for the always-present P&M part.
///
/// The `Crc*` split reflects that a stale base CRC produces two different outcomes depending on the
/// [`IncrementalReplay`] budget: `CrcAdvancedByReplay` reverse-replays commits to advance the CRC
/// (yielding a warm, full-state snapshot), while `CrcSeededPmOnlyReplay` forward-replays only P&M
/// columns (a cold snapshot with no cached stats/DM/txn). We record the outcome, not the budget.
///
/// Serializes to its `snake_case` name for the `pm_source` span field.
///
/// [`IncrementalReplay`]: crate::snapshot::IncrementalReplay
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, EnumString, StrumDisplay, AsRefStr, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum ProtocolMetadataSource {
    /// A CRC already at the target version supplied P&M with zero replay.
    CrcAtTarget,
    /// A stale CRC was advanced to the target version by reverse-replaying commits, yielding a
    /// full-state snapshot. P&M is a byproduct of the CRC advance.
    CrcAdvancedByReplay,
    /// A stale CRC seeded a forward, P&M-columns-only pruned replay of the commits above it (the
    /// budget did not permit a full CRC advance). Cold snapshot: no cached stats/DM/txn.
    CrcSeededPmOnlyReplay,
    /// No usable CRC; full P&M log replay over the segment (commits + checkpoint).
    #[default]
    FullReplay,
    /// Incremental update where the existing snapshot's P&M carried forward and the new commits
    /// contributed no P&M change.
    InheritedFromExisting,
}

impl ProtocolMetadataSource {
    fn parse_lenient(s: &str) -> Self {
        Self::from_str(s).unwrap_or_else(|e| {
            warn!("Invalid pm_source '{s}' on span: {e}. Using FullReplay.");
            Self::FullReplay
        })
    }
}

/// Protocol and metadata actions were resolved for a snapshot, from a CRC and/or log replay.
///
/// Emit-based (like [`ScanMetadataCompleted`]): the snapshot layer classifies the `source`,
/// measures the `duration`, and sums the replay denominators, then fires the event via
/// [`emit_protocol_metadata_load`]. `num_commits_replayed_for_pm` and `bytes_read_for_pm` are the
/// work denominators; both are zero when P&M is served from a cache (`CrcAtTarget`,
/// `InheritedFromExisting`).
///
/// `bytes_read_for_pm` is the on-disk size of the files the replay covered (commits, plus
/// checkpoint parts on `FullReplay`); it is an upper bound on bytes actually decoded, since
/// row-group skipping and early termination can skip data. Sidecars are excluded (they are not
/// listed in the segment). Bytes is the size-invariant normalization denominator.
#[derive(Debug, Clone)]
pub struct ProtocolMetadataLoadSuccess {
    // === Set on span creation (emit-based: all fields are creation attrs) ===
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    /// Where P&M came from.
    pub source: ProtocolMetadataSource,
    /// Commits read during the P&M replay. Zero on the cache-served sources.
    pub num_commits_replayed_for_pm: u64,
    /// On-disk bytes the P&M replay covered (normalization denominator). Zero on cache hits.
    pub bytes_read_for_pm: u64,
    pub duration: Duration,
}

impl ProtocolMetadataLoadSuccess {
    pub(crate) const SPAN_NAME: &'static str = PROTOCOL_METADATA_LOADED_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = ProtocolMetadataLoadAttrs::default();
        attrs.record(&mut v);
        Self {
            operation_id: MetricId::from_attrs(attrs),
            table_type: TableType::from_catalog_managed(read_is_catalog_managed(attrs)),
            correlation_id: correlation_id_from_attrs(attrs),
            source: ProtocolMetadataSource::parse_lenient(&v.source),
            num_commits_replayed_for_pm: v.num_commits_replayed_for_pm,
            bytes_read_for_pm: v.bytes_read_for_pm,
            duration: Duration::from_nanos(v.duration_ns),
        }
    }
}

impl fmt::Display for ProtocolMetadataLoadSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            source,
            num_commits_replayed_for_pm,
            bytes_read_for_pm,
            duration,
        } = self;
        write!(
            f,
            "ProtocolMetadataLoadSuccess(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, source={source}, \
             commits_replayed={num_commits_replayed_for_pm}, bytes={bytes_read_for_pm}, \
             duration={duration:?})"
        )
    }
}

#[derive(Default)]
struct ProtocolMetadataLoadAttrs {
    source: String,
    num_commits_replayed_for_pm: u64,
    bytes_read_for_pm: u64,
    duration_ns: u64,
}

impl Visit for ProtocolMetadataLoadAttrs {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "num_commits_replayed_for_pm" => self.num_commits_replayed_for_pm = value,
            "bytes_read_for_pm" => self.bytes_read_for_pm = value,
            "duration_ns" => self.duration_ns = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "pm_source" {
            self.source = value.to_string();
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// Reading protocol and metadata for a snapshot failed.
#[derive(Debug, Clone)]
pub struct ProtocolMetadataLoadFailure {
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
}

impl fmt::Display for ProtocolMetadataLoadFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProtocolMetadataLoadFailure(id={}, table_type={}, correlation_id={:?})",
            self.operation_id, self.table_type, self.correlation_id
        )
    }
}

// ====================================================================
// SnapshotBuild
// ====================================================================

pub(crate) const SNAPSHOT_COMPLETED_SPAN: &str = "snap.build";

/// Whether a snapshot was built from scratch or as an incremental update of an existing snapshot.
///
/// Serializes to its `snake_case` name for the `load_type` span field.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, EnumString, StrumDisplay, AsRefStr, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum SnapshotLoadType {
    /// Built from scratch by listing the log (`Snapshot::builder_for`).
    #[default]
    Fresh,
    /// Built by updating an existing snapshot (`Snapshot::builder_from`).
    Incremental,
}

impl SnapshotLoadType {
    fn parse_lenient(s: &str) -> Self {
        if s.is_empty() {
            return Self::Fresh;
        }
        Self::from_str(s).unwrap_or_else(|e| {
            warn!("Invalid load_type '{s}' on span: {e}. Using Fresh.");
            Self::Fresh
        })
    }
}

/// A snapshot was built successfully.
#[derive(Debug, Clone)]
pub struct SnapshotBuildSuccess {
    // === Set on span creation ===
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    /// Whether this was a fresh or incremental build.
    pub load_type: SnapshotLoadType,

    // === Set during span lifetime ===
    pub version: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl SnapshotBuildSuccess {
    pub(crate) const SPAN_NAME: &'static str = SNAPSHOT_COMPLETED_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        Self {
            operation_id: MetricId::from_attrs(attrs),
            table_type: TableType::from_catalog_managed(read_is_catalog_managed(attrs)),
            correlation_id: correlation_id_from_attrs(attrs),
            load_type: SnapshotLoadType::parse_lenient(&read_load_type(attrs)),
            version: 0,
            duration: Duration::default(),
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "version" => self.version = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for SnapshotBuildSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            load_type,
            version,
            duration,
        } = self;
        write!(
            f,
            "SnapshotBuildSuccess(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, load_type={load_type}, version={version}, \
             duration={duration:?})"
        )
    }
}

/// The `load_type` string span field on the snapshot-build span. Empty means unset (defaults to
/// `Fresh`).
pub(crate) const LOAD_TYPE_FIELD: &str = "load_type";

fn read_load_type(attrs: &Attributes<'_>) -> String {
    #[derive(Default)]
    struct V(String);
    impl Visit for V {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == LOAD_TYPE_FIELD {
                self.0 = value.to_string();
            }
        }
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
    let mut v = V::default();
    attrs.record(&mut v);
    v.0
}

// ====================================================================
// SnapshotBuildFailure
// ====================================================================

/// Building a snapshot failed.
#[derive(Debug, Clone)]
pub struct SnapshotBuildFailure {
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    /// Whether this was a fresh or incremental build attempt.
    pub load_type: SnapshotLoadType,
}

impl fmt::Display for SnapshotBuildFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SnapshotBuildFailure(id={}, table_type={}, correlation_id={:?}, load_type={})",
            self.operation_id, self.table_type, self.correlation_id, self.load_type
        )
    }
}

// ====================================================================
// TransactionCommit
// ====================================================================

pub(crate) const TRANSACTION_COMMIT_SPAN: &str = "txn.commit";

/// A transaction was committed successfully.
#[derive(Debug, Clone)]
pub struct TransactionCommitSuccess {
    // === Set on span creation ===
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    pub commit_version: u64,

    // === Set during span lifetime ===
    pub num_add_files: u64,
    pub num_remove_files: u64,
    pub num_dv_updates: u64,
    pub add_files_bytes: u64,
    pub remove_files_bytes: u64,
    pub is_blind_append: bool,
    pub data_change: bool,
    pub operation: Option<String>,
    /// Time assembling and validating the commit, before the committer call.
    pub prepare_duration: Duration,
    /// Time in the committer's `commit()` call.
    pub committer_duration: Duration,

    // === Set on span close ===
    pub total_duration: Duration,
}

impl TransactionCommitSuccess {
    pub(crate) const SPAN_NAME: &'static str = TRANSACTION_COMMIT_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = TransactionCommitAttrs::default();
        attrs.record(&mut v);
        Self {
            operation_id: MetricId::from_attrs(attrs),
            table_type: TableType::from_catalog_managed(read_is_catalog_managed(attrs)),
            correlation_id: correlation_id_from_attrs(attrs),
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
            total_duration: Duration::default(),
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "num_add_files" => self.num_add_files = value,
            "num_remove_files" => self.num_remove_files = value,
            "num_dv_updates" => self.num_dv_updates = value,
            "add_files_bytes" => self.add_files_bytes = value,
            "remove_files_bytes" => self.remove_files_bytes = value,
            "prepare_duration_ns" => self.prepare_duration = Duration::from_nanos(value),
            "committer_duration_ns" => self.committer_duration = Duration::from_nanos(value),
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match name {
            "is_blind_append" => self.is_blind_append = value,
            "data_change" => self.data_change = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_str(&mut self, name: &str, value: &str) -> Result<(), &'static str> {
        match name {
            "operation" => self.operation = Some(value.to_string()),
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.total_duration = d;
    }
}

impl fmt::Display for TransactionCommitSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            commit_version,
            num_add_files,
            num_remove_files,
            num_dv_updates,
            add_files_bytes,
            remove_files_bytes,
            is_blind_append,
            data_change,
            operation,
            prepare_duration,
            committer_duration,
            total_duration,
        } = self;
        write!(
            f,
            "TransactionCommitSuccess(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, version={commit_version}, \
             total_duration={total_duration:?}, prepare={prepare_duration:?}, committer={committer_duration:?}, \
             add_files={num_add_files}, remove_files={num_remove_files}, dv_updates={num_dv_updates}, \
             add_bytes={add_files_bytes}, remove_bytes={remove_files_bytes}, \
             is_blind_append={is_blind_append}, data_change={data_change}, operation={operation:?})"
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

/// A transaction commit did not succeed; `reason` distinguishes conflict, retryable IO, and
/// terminal errors.
#[derive(Debug, Clone)]
pub struct TransactionCommitFailure {
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    pub table_type: TableType,
    pub reason: CommitFailureReason,
}

impl fmt::Display for TransactionCommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            reason,
        } = self;
        write!(
            f,
            "TransactionCommitFailure(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, reason={reason})"
        )
    }
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
pub struct DomainMetadataLoadSuccess {
    // === Set during span lifetime ===
    pub from_cache: bool,
    pub num_domains_returned: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl DomainMetadataLoadSuccess {
    pub(crate) const SPAN_NAME: &'static str = DOMAIN_METADATA_LOADED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            from_cache: false,
            num_domains_returned: 0,
            duration: Duration::default(),
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "num_domains_returned" => self.num_domains_returned = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match name {
            "from_cache" => self.from_cache = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for DomainMetadataLoadSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            from_cache,
            num_domains_returned,
            duration,
        } = self;
        write!(
            f,
            "DomainMetadataLoadSuccess(duration={duration:?}, from_cache={from_cache}, \
             num_domains_returned={num_domains_returned})"
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
pub struct SetTransactionLoadSuccess {
    // === Set during span lifetime ===
    pub from_cache: bool,
    pub found: bool,

    // === Set on span close ===
    pub duration: Duration,
}

impl SetTransactionLoadSuccess {
    pub(crate) const SPAN_NAME: &'static str = SET_TRANSACTION_LOADED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            from_cache: false,
            found: false,
            duration: Duration::default(),
        }
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match name {
            "from_cache" => self.from_cache = value,
            "found" => self.found = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for SetTransactionLoadSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            from_cache,
            found,
            duration,
        } = self;
        write!(
            f,
            "SetTransactionLoadSuccess(duration={duration:?}, from_cache={from_cache}, found={found})"
        )
    }
}

// ====================================================================
// CrcRead
// ====================================================================

pub(crate) const CRC_READ_COMPLETED_SPAN: &str = "crc_read_completed";

/// A CRC file was read and parsed successfully. `bytes_read` is the raw byte count from storage.
#[derive(Debug, Clone)]
pub struct CrcReadSuccess {
    // === Set during span lifetime ===
    pub bytes_read: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl CrcReadSuccess {
    pub(crate) const SPAN_NAME: &'static str = CRC_READ_COMPLETED_SPAN;

    pub(crate) fn from_attrs(_attrs: &Attributes<'_>) -> Self {
        Self {
            bytes_read: 0,
            duration: Duration::default(),
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match name {
            "bytes_read" => self.bytes_read = value,
            _ => return Err(Self::SPAN_NAME),
        }
        Ok(())
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for CrcReadSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            duration,
            bytes_read,
        } = self;
        write!(
            f,
            "CrcReadSuccess(duration={duration:?}, bytes={bytes_read})"
        )
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
#[derive(Debug, Clone, Default)]
pub struct SnapshotLoadMetricContext {
    pub(crate) operation_id: MetricId,
    pub(crate) correlation_id: Option<Arc<str>>,
    pub(crate) is_catalog_managed: bool,
}

pub(crate) fn read_is_catalog_managed(attrs: &Attributes<'_>) -> bool {
    #[derive(Default)]
    struct V(bool);
    impl Visit for V {
        fn record_bool(&mut self, field: &Field, value: bool) {
            if field.name() == IS_CATALOG_MANAGED_FIELD {
                self.0 = value;
            }
        }
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
    let mut v = V::default();
    attrs.record(&mut v);
    v.0
}

/// The `correlation_id` string span field carried by every event that records it. Empty means the
/// caller did not supply one.
pub(crate) const CORRELATION_ID_FIELD: &str = "correlation_id";

/// Extract the optional caller-supplied correlation id from span attributes; empty or absent
/// yields `None`.
pub(crate) fn correlation_id_from_attrs(attrs: &Attributes<'_>) -> Option<Arc<str>> {
    #[derive(Default)]
    struct CorrelationIdVisitor(Option<Arc<str>>);
    impl Visit for CorrelationIdVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == CORRELATION_ID_FIELD && !value.is_empty() {
                self.0 = Some(value.into());
            }
        }
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
    let mut v = CorrelationIdVisitor::default();
    attrs.record(&mut v);
    v.0
}

/// A `parallel_scan_metadata` scan emits **two** events (one per phase) sharing the same
/// `operation_id`; `scan_metadata` emits one event with [`ScanType::Full`].
#[derive(Debug, Clone)]
pub struct ScanMetadataCompleted {
    // === Set on span creation ===
    /// Unique ID to correlate this scan with other events.
    pub operation_id: MetricId,
    /// Opaque, caller-supplied id for joining this operation's metric events to the caller's
    /// own request or operation id.
    pub correlation_id: Option<Arc<str>>,
    /// Whether the scanned table is path-based or catalog-managed.
    pub table_type: TableType,
    /// Which scan execution path produced this event.
    pub scan_type: ScanType,
    /// Wall-clock time from scan start to iterator exhaustion.
    pub duration: Duration,
    /// Add files that entered deduplication (excludes files filtered by data skipping).
    pub num_add_files_seen: u64,
    /// Add files that survived log replay (the files the connector reads).
    pub num_active_add_files: u64,
    /// Size in bytes of the files that survived log replay (files to read).
    pub active_add_files_bytes: u64,
    /// Remove files seen (from delta/commit files only).
    pub num_remove_files_seen: u64,
    /// Non-file actions seen (protocol, metadata, etc.).
    pub num_non_file_actions: u64,
    /// Files filtered by predicates (data skipping + partition pruning).
    pub num_predicate_filtered: u64,
    /// Peak size of the deduplication hash set.
    pub peak_hash_set_size: usize,
    /// Time spent in the deduplication visitor.
    pub dedup_visitor_time: Duration,
    /// Time spent evaluating predicates.
    pub predicate_eval_time: Duration,
}

impl ScanMetadataCompleted {
    pub(crate) const SPAN_NAME: &'static str = "scan.metadata_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = ScanMetadataCompletedAttrs::default();
        attrs.record(&mut v);
        Self {
            operation_id: MetricId(v.operation_id),
            table_type: TableType::from_catalog_managed(v.is_catalog_managed),
            correlation_id: v.correlation_id,
            scan_type: ScanType::parse_lenient(&v.scan_type),
            duration: Duration::from_nanos(v.duration_ns),
            num_add_files_seen: v.num_add_files_seen,
            num_active_add_files: v.num_active_add_files,
            active_add_files_bytes: v.active_add_files_bytes,
            num_remove_files_seen: v.num_remove_files_seen,
            num_non_file_actions: v.num_non_file_actions,
            num_predicate_filtered: v.num_predicate_filtered,
            peak_hash_set_size: v.peak_hash_set_size as usize,
            dedup_visitor_time: Duration::from_nanos(v.dedup_visitor_time_ns),
            predicate_eval_time: Duration::from_nanos(v.predicate_eval_time_ns),
        }
    }
}

impl fmt::Display for ScanMetadataCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            table_type,
            correlation_id,
            scan_type,
            duration,
            num_add_files_seen,
            num_active_add_files,
            active_add_files_bytes,
            num_remove_files_seen,
            num_non_file_actions,
            num_predicate_filtered,
            peak_hash_set_size,
            dedup_visitor_time,
            predicate_eval_time,
        } = self;
        write!(
            f,
            "ScanMetadataCompleted(id={operation_id}, table_type={table_type}, \
             correlation_id={correlation_id:?}, scan_type={scan_type}, duration={duration:?}, \
             add_files_seen={num_add_files_seen}, active_add_files={num_active_add_files}, \
             active_add_files_bytes={active_add_files_bytes}, \
             remove_files_seen={num_remove_files_seen}, non_file_actions={num_non_file_actions}, \
             predicate_filtered={num_predicate_filtered}, peak_hash_set_size={peak_hash_set_size}, \
             dedup_visitor_time={dedup_visitor_time:?}, predicate_eval_time={predicate_eval_time:?})"
        )
    }
}

#[derive(Default)]
struct ScanMetadataCompletedAttrs {
    operation_id: Uuid,
    is_catalog_managed: bool,
    correlation_id: Option<Arc<str>>,
    scan_type: String,
    duration_ns: u64,
    num_add_files_seen: u64,
    num_active_add_files: u64,
    active_add_files_bytes: u64,
    num_remove_files_seen: u64,
    num_non_file_actions: u64,
    num_predicate_filtered: u64,
    peak_hash_set_size: u64,
    dedup_visitor_time_ns: u64,
    predicate_eval_time_ns: u64,
}

impl Visit for ScanMetadataCompletedAttrs {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == IS_CATALOG_MANAGED_FIELD {
            self.is_catalog_managed = value;
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == CORRELATION_ID_FIELD && !value.is_empty() {
            self.correlation_id = Some(value.into());
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "duration_ns" => self.duration_ns = value,
            "num_add_files_seen" => self.num_add_files_seen = value,
            "num_active_add_files" => self.num_active_add_files = value,
            "active_add_files_bytes" => self.active_add_files_bytes = value,
            "num_remove_files_seen" => self.num_remove_files_seen = value,
            "num_non_file_actions" => self.num_non_file_actions = value,
            "num_predicate_filtered" => self.num_predicate_filtered = value,
            "peak_hash_set_size" => self.peak_hash_set_size = value,
            "dedup_visitor_time_ns" => self.dedup_visitor_time_ns = value,
            "predicate_eval_time_ns" => self.predicate_eval_time_ns = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let s = format!("{value:?}");
        match field.name() {
            "operation_id" => match Uuid::from_str(&s) {
                Ok(u) => self.operation_id = u,
                Err(e) => warn!(
                    "Invalid uuid '{s}' on {}: {e}",
                    ScanMetadataCompleted::SPAN_NAME
                ),
            },
            "scan_type" => self.scan_type = s,
            _ => {}
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

/// Emit a successful [`MetricEvent::ProtocolMetadataLoadSuccess`] via a tracing span.
///
/// Emit-based: the snapshot layer classifies the source, measures the duration, and sums the
/// replay denominators, then calls this once per snapshot load (all source branches, fresh and
/// incremental).
pub(crate) fn emit_protocol_metadata_load(
    metric_context: &SnapshotLoadMetricContext,
    source: ProtocolMetadataSource,
    num_commits_replayed_for_pm: u64,
    bytes_read_for_pm: u64,
    duration: Duration,
) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ProtocolMetadataLoadSuccess::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %metric_context.operation_id,
        is_catalog_managed = metric_context.is_catalog_managed,
        correlation_id = metric_context.correlation_id.as_deref().unwrap_or(""),
        pm_source = source.as_ref(),
        num_commits_replayed_for_pm,
        bytes_read_for_pm,
        duration_ns = duration.as_nanos() as u64,
    );
}

/// Emit a [`MetricEvent::ProtocolMetadataLoadFailure`] via a tracing span.
///
/// The `error` field flips the span-close event to its failure counterpart, matching how
/// `#[instrument(err)]` records failures on the instrument-based paths.
pub(crate) fn emit_protocol_metadata_load_failure(metric_context: &SnapshotLoadMetricContext) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ProtocolMetadataLoadSuccess::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %metric_context.operation_id,
        is_catalog_managed = metric_context.is_catalog_managed,
        correlation_id = metric_context.correlation_id.as_deref().unwrap_or(""),
        pm_source = tracing::field::Empty,
        num_commits_replayed_for_pm = tracing::field::Empty,
        bytes_read_for_pm = tracing::field::Empty,
        duration_ns = tracing::field::Empty,
        error = "protocol/metadata load failed",
    );
}

/// Emit a [`MetricEvent::LogSegmentLoadSuccess`] via a tracing span, for callers that assemble a
/// segment outside the instrumented `for_snapshot` path (e.g. the incremental snapshot update).
/// The fresh path uses the `#[instrument]` on `LogSegment::for_snapshot` instead.
pub(crate) fn emit_log_segment_load(
    metric_context: &SnapshotLoadMetricContext,
    load_purpose: LogSegmentLoadPurpose,
    num_commit_files: u64,
    num_checkpoint_files: u64,
    num_compaction_files: u64,
    has_latest_crc_file: bool,
    duration: Duration,
) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        LogSegmentLoadSuccess::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %metric_context.operation_id,
        is_catalog_managed = metric_context.is_catalog_managed,
        correlation_id = metric_context.correlation_id.as_deref().unwrap_or(""),
        load_purpose = load_purpose.as_ref(),
        num_commit_files,
        num_checkpoint_files,
        num_compaction_files,
        has_latest_crc_file,
        duration_ns = duration.as_nanos() as u64,
    );
}

/// Emit a [`MetricEvent::ScanMetadataCompleted`] via a tracing span. Call when the scan metadata
/// iterator is exhausted or dropped.
pub(crate) fn emit_scan_metadata_completed(e: &ScanMetadataCompleted) {
    let _span = tracing::span!(
        tracing::Level::INFO,
        ScanMetadataCompleted::SPAN_NAME,
        report = tracing::field::Empty,
        operation_id = %e.operation_id,
        is_catalog_managed = e.table_type.is_catalog_managed(),
        correlation_id = e.correlation_id.as_deref().unwrap_or(""),
        scan_type = %e.scan_type,
        duration_ns = e.duration.as_nanos() as u64,
        num_add_files_seen = e.num_add_files_seen,
        num_active_add_files = e.num_active_add_files,
        active_add_files_bytes = e.active_add_files_bytes,
        num_remove_files_seen = e.num_remove_files_seen,
        num_non_file_actions = e.num_non_file_actions,
        num_predicate_filtered = e.num_predicate_filtered,
        peak_hash_set_size = e.peak_hash_set_size as u64,
        dedup_visitor_time_ns = e.dedup_visitor_time.as_nanos() as u64,
        predicate_eval_time_ns = e.predicate_eval_time.as_nanos() as u64,
    );
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn commit_success(operation_id: MetricId) -> TransactionCommitSuccess {
        TransactionCommitSuccess {
            operation_id,
            table_type: TableType::PathBased,
            correlation_id: None,
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
            total_duration: Duration::default(),
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
        let mut event = MetricEvent::TransactionCommitSuccess(commit_success(id));
        event.record_str("failure_reason", value).unwrap();
        let MetricEvent::TransactionCommitFailure(failure) = event else {
            panic!("expected TransactionCommitFailure");
        };
        assert_eq!(failure.operation_id, id);
        assert_eq!(failure.reason, expected);
    }

    #[test]
    fn record_str_operation_sets_field_without_flipping() {
        let mut event = MetricEvent::TransactionCommitSuccess(commit_success(MetricId::new()));
        event.record_str("operation", "WRITE").unwrap();
        let MetricEvent::TransactionCommitSuccess(success) = event else {
            panic!("expected TransactionCommitSuccess");
        };
        assert_eq!(success.operation.as_deref(), Some("WRITE"));
    }

    #[test]
    fn record_u64_num_dv_updates_sets_field() {
        let mut event = MetricEvent::TransactionCommitSuccess(commit_success(MetricId::new()));
        event.record_u64("num_dv_updates", 7).unwrap();
        let MetricEvent::TransactionCommitSuccess(success) = event else {
            panic!("expected TransactionCommitSuccess");
        };
        assert_eq!(success.num_dv_updates, 7);
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

    #[test]
    fn into_failure_maps_commit_success_to_error_reason() {
        let id = MetricId::new();
        let failure = MetricEvent::TransactionCommitSuccess(commit_success(id)).into_failure();
        let MetricEvent::TransactionCommitFailure(failure) = failure else {
            panic!("expected TransactionCommitFailure");
        };
        assert_eq!(failure.operation_id, id);
        assert_eq!(failure.reason, CommitFailureReason::Error);
    }

    #[test]
    fn record_str_failure_reason_flip_preserves_correlation_id() {
        let mut success = commit_success(MetricId::new());
        success.correlation_id = Some("commit-req-1".into());
        let mut event = MetricEvent::TransactionCommitSuccess(success);
        event.record_str("failure_reason", "conflict").unwrap();
        let MetricEvent::TransactionCommitFailure(failure) = event else {
            panic!("expected TransactionCommitFailure");
        };
        assert_eq!(failure.correlation_id.as_deref(), Some("commit-req-1"));
    }

    #[test]
    fn into_failure_preserves_correlation_id() {
        let mut success = commit_success(MetricId::new());
        success.correlation_id = Some("commit-req-2".into());
        let MetricEvent::TransactionCommitFailure(failure) =
            MetricEvent::TransactionCommitSuccess(success).into_failure()
        else {
            panic!("expected TransactionCommitFailure");
        };
        assert_eq!(failure.correlation_id.as_deref(), Some("commit-req-2"));

        let snapshot = SnapshotBuildSuccess {
            operation_id: MetricId::new(),
            table_type: TableType::PathBased,
            correlation_id: Some("snap-req-3".into()),
            version: 0,
            duration: Duration::default(),
        };
        let MetricEvent::SnapshotBuildFailure(failure) =
            MetricEvent::SnapshotBuildSuccess(snapshot).into_failure()
        else {
            panic!("expected SnapshotBuildFailure");
        };
        assert_eq!(failure.correlation_id.as_deref(), Some("snap-req-3"));
    }
}
