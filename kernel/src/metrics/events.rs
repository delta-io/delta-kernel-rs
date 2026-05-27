//! Metric event types emitted during Delta Kernel operations.
//!
//! Each [`MetricEvent`] variant wraps a per-event struct that owns its fields, `Display` impl,
//! span name, and the small set of methods the `tracing` layer in [`crate::metrics::reporter`]
//! calls to construct and finalize the event. Per-event code is colocated in a single block
//! per type below.

use std::fmt;
use std::str::FromStr as _;
use std::time::Duration;

use delta_kernel_derive::internal_api;
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
    LogSegmentLoaded(LogSegmentLoaded),
    ProtocolMetadataLoaded(ProtocolMetadataLoaded),
    SnapshotCompleted(SnapshotCompleted),
    SnapshotFailed(SnapshotFailed),
    DomainMetadataLoaded(DomainMetadataLoaded),
    SetTransactionLoaded(SetTransactionLoaded),
    CrcReadCompleted(CrcReadCompleted),
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
            // Lifecycle: duration must be set by the tracing layer on span close.
            Self::LogSegmentLoaded(e) => e.set_duration(d),
            Self::ProtocolMetadataLoaded(e) => e.set_duration(d),
            Self::SnapshotCompleted(e) => e.set_duration(d),
            Self::SnapshotFailed(e) => e.set_duration(d),
            Self::DomainMetadataLoaded(e) => e.set_duration(d),
            Self::SetTransactionLoaded(e) => e.set_duration(d),
            Self::CrcReadCompleted(e) => e.set_duration(d),

            // Duration already set at construction.
            Self::ScanMetadataCompleted(_)
            | Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_)
            // No duration field.
            | Self::JsonReadCompleted(_)
            | Self::ParquetReadCompleted(_) => {}
        }
    }

    pub(crate) fn record_u64(&mut self, name: &str, value: u64) -> Result<(), &'static str> {
        match self {
            // Variants with u64 fields set during span lifetime.
            Self::LogSegmentLoaded(e) => e.record_u64(name, value),
            Self::SnapshotCompleted(e) => e.record_u64(name, value),
            Self::DomainMetadataLoaded(e) => e.record_u64(name, value),
            Self::CrcReadCompleted(e) => e.record_u64(name, value),

            // No u64 fields set during span lifetime — a runtime record() on these is a bug.
            Self::ProtocolMetadataLoaded(_) => Err(ProtocolMetadataLoaded::SPAN_NAME),
            Self::SnapshotFailed(_) => Err(SnapshotCompleted::SPAN_NAME),
            Self::SetTransactionLoaded(_) => Err(SetTransactionLoaded::SPAN_NAME),
            Self::ScanMetadataCompleted(_) => Err(ScanMetadataCompleted::SPAN_NAME),
            Self::JsonReadCompleted(_) => Err(JsonReadCompleted::SPAN_NAME),
            Self::ParquetReadCompleted(_) => Err(ParquetReadCompleted::SPAN_NAME),
            Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => Err(STORAGE_SPAN),
        }
    }

    pub(crate) fn record_bool(&mut self, name: &str, value: bool) -> Result<(), &'static str> {
        match self {
            // Variants with bool fields set during span lifetime.
            Self::LogSegmentLoaded(e) => e.record_bool(name, value),
            Self::DomainMetadataLoaded(e) => e.record_bool(name, value),
            Self::SetTransactionLoaded(e) => e.record_bool(name, value),

            // No bool fields set during span lifetime — a runtime record() on these is a bug.
            Self::ProtocolMetadataLoaded(_) => Err(ProtocolMetadataLoaded::SPAN_NAME),
            Self::SnapshotCompleted(_) => Err(SnapshotCompleted::SPAN_NAME),
            Self::SnapshotFailed(_) => Err(SnapshotCompleted::SPAN_NAME),
            Self::CrcReadCompleted(_) => Err(CrcReadCompleted::SPAN_NAME),
            Self::ScanMetadataCompleted(_) => Err(ScanMetadataCompleted::SPAN_NAME),
            Self::JsonReadCompleted(_) => Err(JsonReadCompleted::SPAN_NAME),
            Self::ParquetReadCompleted(_) => Err(ParquetReadCompleted::SPAN_NAME),
            Self::StorageListCompleted(_)
            | Self::StorageReadCompleted(_)
            | Self::StorageCopyCompleted(_) => Err(STORAGE_SPAN),
        }
    }
}

impl fmt::Display for MetricEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogSegmentLoaded(e) => e.fmt(f),
            Self::ProtocolMetadataLoaded(e) => e.fmt(f),
            Self::SnapshotCompleted(e) => e.fmt(f),
            Self::SnapshotFailed(e) => e.fmt(f),
            Self::DomainMetadataLoaded(e) => e.fmt(f),
            Self::SetTransactionLoaded(e) => e.fmt(f),
            Self::CrcReadCompleted(e) => e.fmt(f),
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
// LogSegmentLoaded
// ====================================================================
//
// Canonical example for the per-event block pattern. Other events below follow the same
// shape; detailed `///` docs on `from_attrs` and `record_*` live here only.

// Module-scope span name. `#[instrument(name = ...)]` only accepts a bare identifier here,
// not a multi-segment path like `Type::SPAN_NAME`.
pub(crate) const LOG_SEGMENT_LOADED_SPAN: &str = "segment.for_snapshot";

#[derive(Debug, Clone)]
pub struct LogSegmentLoaded {
    // === Set on span creation ===
    pub operation_id: MetricId,

    // === Set during span lifetime ===
    pub num_commit_files: u64,
    pub num_checkpoint_files: u64,
    pub num_compaction_files: u64,
    pub has_latest_crc_file: bool,

    // === Set on span close ===
    pub duration: Duration,
}

impl LogSegmentLoaded {
    pub(crate) const SPAN_NAME: &'static str = LOG_SEGMENT_LOADED_SPAN;

    /// Construction-time channel. Extracts fields bound at span creation via
    /// `#[instrument(fields(X = expr))]` or `tracing::span!(..., X = expr)`.
    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        Self {
            operation_id: MetricId::from_attrs(attrs),
            num_commit_files: 0,
            num_checkpoint_files: 0,
            num_compaction_files: 0,
            has_latest_crc_file: false,
            duration: Duration::default(),
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

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for LogSegmentLoaded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            duration,
            num_commit_files,
            num_checkpoint_files,
            num_compaction_files,
            has_latest_crc_file,
        } = self;
        write!(
            f,
            "LogSegmentLoaded(id={operation_id}, duration={duration:?}, \
             commits={num_commit_files}, checkpoints={num_checkpoint_files}, \
             compactions={num_compaction_files}, has_latest_crc={has_latest_crc_file})"
        )
    }
}

// ====================================================================
// ProtocolMetadataLoaded
// ====================================================================

pub(crate) const PROTOCOL_METADATA_LOADED_SPAN: &str = "segment.read_metadata";

#[derive(Debug, Clone)]
pub struct ProtocolMetadataLoaded {
    // === Set on span creation ===
    pub operation_id: MetricId,

    // === Set on span close ===
    pub duration: Duration,
}

impl ProtocolMetadataLoaded {
    pub(crate) const SPAN_NAME: &'static str = PROTOCOL_METADATA_LOADED_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        Self {
            operation_id: MetricId::from_attrs(attrs),
            duration: Duration::default(),
        }
    }

    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for ProtocolMetadataLoaded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            duration,
        } = self;
        write!(
            f,
            "ProtocolMetadataLoaded(id={operation_id}, duration={duration:?})"
        )
    }
}

// ====================================================================
// SnapshotCompleted
// ====================================================================

pub(crate) const SNAPSHOT_COMPLETED_SPAN: &str = "snap.build";

#[derive(Debug, Clone)]
pub struct SnapshotCompleted {
    // === Set on span creation ===
    pub operation_id: MetricId,

    // === Set during span lifetime ===
    pub version: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl SnapshotCompleted {
    pub(crate) const SPAN_NAME: &'static str = SNAPSHOT_COMPLETED_SPAN;

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        Self {
            operation_id: MetricId::from_attrs(attrs),
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

    /// Convert to a [`SnapshotFailed`] when the span fires an `error` field.
    pub(crate) fn into_failed(self) -> SnapshotFailed {
        SnapshotFailed {
            operation_id: self.operation_id,
            duration: self.duration,
        }
    }
}

impl fmt::Display for SnapshotCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            version,
            duration,
        } = self;
        write!(
            f,
            "SnapshotCompleted(id={operation_id}, version={version}, duration={duration:?})"
        )
    }
}

// ====================================================================
// SnapshotFailed
// ====================================================================

/// Snapshot creation failed. Emitted in place of [`SnapshotCompleted`] when the `snap.build`
/// span returns `Err`; carries the same `operation_id`.
#[derive(Debug, Clone)]
pub struct SnapshotFailed {
    // === Carried over from `SnapshotCompleted` when the span errors ===
    pub operation_id: MetricId,

    // === Set on span close ===
    pub duration: Duration,
}

impl SnapshotFailed {
    pub(crate) fn set_duration(&mut self, d: Duration) {
        self.duration = d;
    }
}

impl fmt::Display for SnapshotFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            duration,
        } = self;
        write!(
            f,
            "SnapshotFailed(id={operation_id}, duration={duration:?})"
        )
    }
}

// ====================================================================
// DomainMetadataLoaded
// ====================================================================

pub(crate) const DOMAIN_METADATA_LOADED_SPAN: &str = "snap.get_domain_metadata";

/// Emitted once per domain metadata load, whether served from the CRC cache (`from_cache`) or
/// from a log replay. Covers connector-issued loads of user domains and kernel-internal loads of
/// system (`delta.*`) domains such as clustering or row tracking.
#[derive(Debug, Clone)]
pub struct DomainMetadataLoaded {
    // === Set during span lifetime ===
    pub from_cache: bool,
    pub num_domains_returned: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl DomainMetadataLoaded {
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

impl fmt::Display for DomainMetadataLoaded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            from_cache,
            num_domains_returned,
            duration,
        } = self;
        write!(
            f,
            "DomainMetadataLoaded(duration={duration:?}, from_cache={from_cache}, \
             num_domains_returned={num_domains_returned})"
        )
    }
}

// ====================================================================
// SetTransactionLoaded
// ====================================================================

pub(crate) const SET_TRANSACTION_LOADED_SPAN: &str = "snap.get_app_id_version";

/// Emitted once per `SetTransaction` (app id) load, whether served from the CRC cache
/// (`from_cache`) or from a log replay. `found` is true when the app id has a committed
/// transaction version, false when none exists or the existing one is expired.
#[derive(Debug, Clone)]
pub struct SetTransactionLoaded {
    // === Set during span lifetime ===
    pub from_cache: bool,
    pub found: bool,

    // === Set on span close ===
    pub duration: Duration,
}

impl SetTransactionLoaded {
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

impl fmt::Display for SetTransactionLoaded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            from_cache,
            found,
            duration,
        } = self;
        write!(
            f,
            "SetTransactionLoaded(duration={duration:?}, from_cache={from_cache}, found={found})"
        )
    }
}

// ====================================================================
// CrcReadCompleted
// ====================================================================

pub(crate) const CRC_READ_COMPLETED_SPAN: &str = "crc_read_completed";

/// Emitted even when JSON parsing fails after a successful storage read.
/// `bytes_read` is the raw byte count from storage (zero if the storage read itself failed).
#[derive(Debug, Clone)]
pub struct CrcReadCompleted {
    // === Set during span lifetime ===
    pub bytes_read: u64,

    // === Set on span close ===
    pub duration: Duration,
}

impl CrcReadCompleted {
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

impl fmt::Display for CrcReadCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            duration,
            bytes_read,
        } = self;
        write!(
            f,
            "CrcReadCompleted(duration={duration:?}, bytes={bytes_read})"
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanType {
    /// Sequential phase of [`crate::scan::Scan::parallel_scan_metadata`].
    SequentialPhase,
    /// Parallel phase of [`crate::scan::Scan::parallel_scan_metadata`].
    ParallelPhase,
    /// Scan metadata from [`crate::scan::Scan::scan_metadata`].
    Full,
}

impl ScanType {
    /// Parse the value of the `scan_type` span field. Unknown values map to `Full`.
    fn parse(s: &str) -> Self {
        match s {
            "sequential" => Self::SequentialPhase,
            "parallel" => Self::ParallelPhase,
            _ => Self::Full,
        }
    }
}

impl fmt::Display for ScanType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SequentialPhase => "sequential",
            Self::ParallelPhase => "parallel",
            Self::Full => "full",
        })
    }
}

/// A `parallel_scan_metadata` scan emits **two** events (one per phase) sharing the same
/// `operation_id`; `scan_metadata` emits one event with [`ScanType::Full`].
#[derive(Debug, Clone)]
pub struct ScanMetadataCompleted {
    // === Set on span creation ===
    /// Unique ID to correlate this scan with other events.
    pub operation_id: MetricId,
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
    /// Time spent in the deduplication visitor (milliseconds).
    pub dedup_visitor_time_ms: u64,
    /// Time spent evaluating predicates (milliseconds).
    pub predicate_eval_time_ms: u64,
}

impl ScanMetadataCompleted {
    pub(crate) const SPAN_NAME: &'static str = "scan.metadata_completed";

    pub(crate) fn from_attrs(attrs: &Attributes<'_>) -> Self {
        let mut v = ScanMetadataCompletedAttrs::default();
        attrs.record(&mut v);
        Self {
            operation_id: MetricId(v.operation_id),
            scan_type: ScanType::parse(&v.scan_type),
            duration: Duration::from_nanos(v.duration_ns),
            num_add_files_seen: v.num_add_files_seen,
            num_active_add_files: v.num_active_add_files,
            active_add_files_bytes: v.active_add_files_bytes,
            num_remove_files_seen: v.num_remove_files_seen,
            num_non_file_actions: v.num_non_file_actions,
            num_predicate_filtered: v.num_predicate_filtered,
            peak_hash_set_size: v.peak_hash_set_size as usize,
            dedup_visitor_time_ms: v.dedup_visitor_time_ms,
            predicate_eval_time_ms: v.predicate_eval_time_ms,
        }
    }
}

impl fmt::Display for ScanMetadataCompleted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation_id,
            scan_type,
            duration,
            num_add_files_seen,
            num_active_add_files,
            active_add_files_bytes,
            num_remove_files_seen,
            num_non_file_actions,
            num_predicate_filtered,
            peak_hash_set_size,
            dedup_visitor_time_ms,
            predicate_eval_time_ms,
        } = self;
        write!(
            f,
            "ScanMetadataCompleted(id={operation_id}, scan_type={scan_type}, duration={duration:?}, \
             add_files_seen={num_add_files_seen}, active_add_files={num_active_add_files}, \
             active_add_files_bytes={active_add_files_bytes}, \
             remove_files_seen={num_remove_files_seen}, non_file_actions={num_non_file_actions}, \
             predicate_filtered={num_predicate_filtered}, peak_hash_set_size={peak_hash_set_size}, \
             dedup_visitor_time_ms={dedup_visitor_time_ms}, predicate_eval_time_ms={predicate_eval_time_ms})"
        )
    }
}

#[derive(Default)]
struct ScanMetadataCompletedAttrs {
    operation_id: Uuid,
    scan_type: String,
    duration_ns: u64,
    num_add_files_seen: u64,
    num_active_add_files: u64,
    active_add_files_bytes: u64,
    num_remove_files_seen: u64,
    num_non_file_actions: u64,
    num_predicate_filtered: u64,
    peak_hash_set_size: u64,
    dedup_visitor_time_ms: u64,
    predicate_eval_time_ms: u64,
}

impl Visit for ScanMetadataCompletedAttrs {
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
            "dedup_visitor_time_ms" => self.dedup_visitor_time_ms = value,
            "predicate_eval_time_ms" => self.predicate_eval_time_ms = value,
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

#[internal_api]
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

#[derive(Debug, Clone)]
pub struct StorageListCompleted {
    // === Set on span creation ===
    pub duration: Duration,
    pub num_files: u64,
}

impl StorageListCompleted {
    #[internal_api]
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

#[derive(Debug, Clone)]
pub struct StorageReadCompleted {
    // === Set on span creation ===
    pub duration: Duration,
    pub num_files: u64,
    pub bytes_read: u64,
}

impl StorageReadCompleted {
    #[internal_api]
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

#[derive(Debug, Clone)]
pub struct StorageCopyCompleted {
    // === Set on span creation ===
    pub duration: Duration,
}

impl StorageCopyCompleted {
    #[internal_api]
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
        operation_id = %e.operation_id,
        scan_type = %e.scan_type,
        duration_ns = e.duration.as_nanos() as u64,
        num_add_files_seen = e.num_add_files_seen,
        num_active_add_files = e.num_active_add_files,
        active_add_files_bytes = e.active_add_files_bytes,
        num_remove_files_seen = e.num_remove_files_seen,
        num_non_file_actions = e.num_non_file_actions,
        num_predicate_filtered = e.num_predicate_filtered,
        peak_hash_set_size = e.peak_hash_set_size as u64,
        dedup_visitor_time_ms = e.dedup_visitor_time_ms,
        predicate_eval_time_ms = e.predicate_eval_time_ms,
    );
}
