//! FFI-safe representations of kernel [`MetricEvent`]s.
//!
//! [`MetricEvent`] is a `repr(C)` version of [`delta_kernel::metrics::MetricEvent`], to allow
//! engines to receive structured metrics across the FFI boundary. To enable metrics reporting call
//! [`crate::ffi_tracing::enable_metrics_reporting`].
//!
//! - Durations are reported as `u64`, either nanoseconds or milliseconds, indicated by an `ns` or
//!   `ms` suffix
//! - Operation ids are UUIDs in kernel (see [`MetricId`]). Here we report the raw 16 bytes of the
//!   UUID.

use delta_kernel::metrics as kernel;

use crate::KernelStringSlice;

/// The 16 raw bytes of an operation's UUID, used to correlate events from the same operation.
#[repr(C)]
pub struct MetricId {
    pub bytes: [u8; 16],
}

impl From<kernel::MetricId> for MetricId {
    fn from(id: kernel::MetricId) -> Self {
        Self {
            bytes: id.as_bytes(),
        }
    }
}

/// Identity and table labels borrowed from a kernel metric event.
///
/// `correlation_id` remains valid for the duration of the metrics callback.
#[repr(C)]
pub struct MetricContext {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
}

impl From<&kernel::MetricContext> for MetricContext {
    fn from(context: &kernel::MetricContext) -> Self {
        Self {
            operation_id: context.operation_id.into(),
            correlation_id: correlation_id_slice(context.correlation_id.as_deref()),
            table_type: context.table_type.into(),
        }
    }
}

// we add cbindgen:prefix-with-name=true below otherwise we get name conflicts. For example
// CommitFailureReason::Error would become just `Error`, which conflicts with the type in
// error.rs. Not all variants cause conflicts, but since they refer to common kernel concepts we
// prefix all enums to avoid future issues.

/// Which scan execution path produced a [`ScanMetadataCompleted`] event.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum ScanType {
    SequentialPhase,
    ParallelPhase,
    Full,
}

impl From<kernel::ScanType> for ScanType {
    fn from(t: kernel::ScanType) -> Self {
        match t {
            kernel::ScanType::SequentialPhase => Self::SequentialPhase,
            kernel::ScanType::ParallelPhase => Self::ParallelPhase,
            kernel::ScanType::Full => Self::Full,
        }
    }
}

/// Why a transaction commit did not succeed.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum CommitFailureReason {
    Conflict,
    RetryableIo,
    Error,
}

impl From<kernel::CommitFailureReason> for CommitFailureReason {
    fn from(r: kernel::CommitFailureReason) -> Self {
        match r {
            kernel::CommitFailureReason::Conflict => Self::Conflict,
            kernel::CommitFailureReason::RetryableIo => Self::RetryableIo,
            kernel::CommitFailureReason::Error => Self::Error,
        }
    }
}

/// The result of a [`delta_kernel::Snapshot::checkpoint`] call.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum CheckpointWriteOutcome {
    /// The checkpoint was written and the API returned its updated snapshot.
    Written {
        /// Size and action counts from the successful write, including all-zero measurements.
        stats: CheckpointWriteStats,
    },
    /// The API returned that a checkpoint already exists at this version.
    AlreadyExists,
    /// The call returned an error.
    Error,
}

impl From<kernel::CheckpointWriteOutcome> for CheckpointWriteOutcome {
    fn from(outcome: kernel::CheckpointWriteOutcome) -> Self {
        match outcome {
            kernel::CheckpointWriteOutcome::Written { stats } => Self::Written {
                stats: stats.into(),
            },
            kernel::CheckpointWriteOutcome::AlreadyExists => Self::AlreadyExists,
            kernel::CheckpointWriteOutcome::Error => Self::Error,
        }
    }
}

/// The checkpoint format and sidecar-writing path selected during the call.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum CheckpointWriteMode {
    /// The call returned before selecting a checkpoint write mode.
    NotAttempted,
    /// V1 checkpoint without sidecars.
    V1,
    /// V2 checkpoint with file actions in the main checkpoint file.
    V2WithoutSidecars,
    /// V2 sidecar-writing path, which can produce zero sidecars if there are no file actions.
    V2WithSidecars,
}

impl From<kernel::CheckpointWriteMode> for CheckpointWriteMode {
    fn from(mode: kernel::CheckpointWriteMode) -> Self {
        match mode {
            kernel::CheckpointWriteMode::NotAttempted => Self::NotAttempted,
            kernel::CheckpointWriteMode::V1 => Self::V1,
            kernel::CheckpointWriteMode::V2WithoutSidecars => Self::V2WithoutSidecars,
            kernel::CheckpointWriteMode::V2WithSidecars => Self::V2WithSidecars,
        }
    }
}

/// Outcome of a [`delta_kernel::Snapshot::write_checksum`] call.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ChecksumWriteOutcome {
    /// The CRC was written and the returned snapshot was constructed successfully.
    Written {
        /// Exact serialized UTF-8 byte count written by this call.
        bytes_written: u64,
    },
    /// The CRC already existed, including a concurrent writer winning the storage race.
    AlreadyExists,
    /// The call returned an error.
    Error,
}

impl From<kernel::ChecksumWriteOutcome> for ChecksumWriteOutcome {
    fn from(outcome: kernel::ChecksumWriteOutcome) -> Self {
        match outcome {
            kernel::ChecksumWriteOutcome::Written { bytes_written } => {
                Self::Written { bytes_written }
            }
            kernel::ChecksumWriteOutcome::AlreadyExists => Self::AlreadyExists,
            kernel::ChecksumWriteOutcome::Error => Self::Error,
        }
    }
}

/// CRC construction path selected during a [`delta_kernel::Snapshot::write_checksum`] call.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
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

impl From<kernel::ChecksumWriteResolutionMode> for ChecksumWriteResolutionMode {
    fn from(mode: kernel::ChecksumWriteResolutionMode) -> Self {
        match mode {
            kernel::ChecksumWriteResolutionMode::Full => Self::Full,
            kernel::ChecksumWriteResolutionMode::Incremental => Self::Incremental,
            kernel::ChecksumWriteResolutionMode::Cached => Self::Cached,
            kernel::ChecksumWriteResolutionMode::NotAttempted => Self::NotAttempted,
        }
    }
}

/// Whether a table is path-based or catalog-managed.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum TableType {
    PathBased,
    CatalogManaged,
}

impl From<kernel::TableType> for TableType {
    fn from(t: kernel::TableType) -> Self {
        match t {
            kernel::TableType::PathBased => Self::PathBased,
            kernel::TableType::CatalogManaged => Self::CatalogManaged,
        }
    }
}

/// How the snapshot was constructed.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum SnapshotLoadType {
    Full = 0,
    Incremental = 1,
    /// Snapshot construction used complete caller-supplied state without engine log I/O.
    SnapshotHint = 2,
    Unknown = 3,
}

/// How the log segment and protocol/metadata state were loaded.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum LogSegmentLoadType {
    Full,
    Incremental,
    Unknown,
}

impl From<kernel::LogSegmentLoadType> for LogSegmentLoadType {
    fn from(t: kernel::LogSegmentLoadType) -> Self {
        match t {
            kernel::LogSegmentLoadType::Full => Self::Full,
            kernel::LogSegmentLoadType::Incremental => Self::Incremental,
            _ => Self::Unknown,
        }
    }
}

impl From<kernel::SnapshotLoadType> for SnapshotLoadType {
    fn from(t: kernel::SnapshotLoadType) -> Self {
        match t {
            kernel::SnapshotLoadType::Full => Self::Full,
            kernel::SnapshotLoadType::Incremental => Self::Incremental,
            kernel::SnapshotLoadType::SnapshotHint => Self::SnapshotHint,
            _ => Self::Unknown,
        }
    }
}

/// A log segment load attempt completed.
#[repr(C)]
pub struct LogSegmentLoadCompleted {
    pub context: MetricContext,
    pub load_type: LogSegmentLoadType,
    pub duration_ns: u64,
    pub outcome: LogSegmentLoadOutcome,
}

/// C representation of [`kernel::LogSegmentLoadOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum LogSegmentLoadOutcome {
    Success {
        /// Measurements from the successful operation.
        stats: LogSegmentLoadStats,
    },
    Error,
}

/// Measurements from a successful log segment load.
#[repr(C)]
pub struct LogSegmentLoadStats {
    pub num_commit_files: u64,
    pub num_checkpoint_files: u64,
    pub num_compaction_files: u64,
    /// Whether the segment has a CRC. When false, `crc_versions_behind` is meaningless.
    pub has_crc: bool,
    /// CRC staleness in versions; zero when at the target version or `has_crc` is false.
    pub crc_versions_behind: u64,
}

impl From<&kernel::LogSegmentLoadStats> for LogSegmentLoadStats {
    fn from(stats: &kernel::LogSegmentLoadStats) -> Self {
        Self {
            num_commit_files: stats.num_commit_files,
            num_checkpoint_files: stats.num_checkpoint_files,
            num_compaction_files: stats.num_compaction_files,
            has_crc: stats.crc_versions_behind.is_some(),
            crc_versions_behind: stats.crc_versions_behind.unwrap_or(0),
        }
    }
}

impl From<&kernel::LogSegmentLoadOutcome> for LogSegmentLoadOutcome {
    fn from(outcome: &kernel::LogSegmentLoadOutcome) -> Self {
        match outcome {
            kernel::LogSegmentLoadOutcome::Success { stats } => Self::Success {
                stats: stats.into(),
            },
            kernel::LogSegmentLoadOutcome::Error => Self::Error,
        }
    }
}

/// How a snapshot load resolved Protocol and Metadata.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum ProtocolMetadataSource {
    CrcAtTarget,
    CrcAdvancedByReplay,
    CrcSeededPmOnlyReplay,
    FullReplay,
    Unknown,
}

impl From<kernel::ProtocolMetadataSource> for ProtocolMetadataSource {
    fn from(s: kernel::ProtocolMetadataSource) -> Self {
        match s {
            kernel::ProtocolMetadataSource::CrcAtTarget => Self::CrcAtTarget,
            kernel::ProtocolMetadataSource::CrcAdvancedByReplay => Self::CrcAdvancedByReplay,
            kernel::ProtocolMetadataSource::CrcSeededPmOnlyReplay => Self::CrcSeededPmOnlyReplay,
            kernel::ProtocolMetadataSource::FullReplay => Self::FullReplay,
            _ => Self::Unknown,
        }
    }
}

/// A protocol and metadata resolution attempt completed.
#[repr(C)]
pub struct ProtocolMetadataLoadCompleted {
    pub context: MetricContext,
    pub load_type: LogSegmentLoadType,
    pub duration_ns: u64,
    pub outcome: ProtocolMetadataLoadOutcome,
}

/// C representation of [`kernel::ProtocolMetadataLoadOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum ProtocolMetadataLoadOutcome {
    Success { source: ProtocolMetadataSource },
    Error,
}

impl From<&kernel::ProtocolMetadataLoadOutcome> for ProtocolMetadataLoadOutcome {
    fn from(outcome: &kernel::ProtocolMetadataLoadOutcome) -> Self {
        match outcome {
            kernel::ProtocolMetadataLoadOutcome::Success { source } => Self::Success {
                source: (*source).into(),
            },
            kernel::ProtocolMetadataLoadOutcome::Error => Self::Error,
        }
    }
}

/// A snapshot build attempt completed.
#[repr(C)]
pub struct SnapshotBuildCompleted {
    pub context: MetricContext,
    pub load_type: SnapshotLoadType,
    pub duration_ns: u64,
    pub outcome: SnapshotBuildOutcome,
}

/// C representation of [`kernel::SnapshotBuildOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum SnapshotBuildOutcome {
    Success { version: u64 },
    Error,
}

impl From<&kernel::SnapshotBuildOutcome> for SnapshotBuildOutcome {
    fn from(outcome: &kernel::SnapshotBuildOutcome) -> Self {
        match outcome {
            kernel::SnapshotBuildOutcome::Success { version } => {
                Self::Success { version: *version }
            }
            kernel::SnapshotBuildOutcome::Error => Self::Error,
        }
    }
}

/// A transaction commit attempt completed.
///
/// Strings in the context and successful outcome are borrowed for the metrics callback.
#[repr(C)]
pub struct TransactionCommitCompleted {
    pub context: MetricContext,
    pub duration_ns: u64,
    pub outcome: TransactionCommitOutcome,
}

/// C representation of [`kernel::TransactionCommitOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum TransactionCommitOutcome {
    Success {
        /// Measurements from the successful operation.
        stats: TransactionCommitStats,
    },
    Failure {
        reason: CommitFailureReason,
    },
}

/// Measurements from a successful transaction commit.
#[repr(C)]
pub struct TransactionCommitStats {
    pub commit_version: u64,
    pub num_add_files: u64,
    pub num_remove_files: u64,
    pub num_dv_updates: u64,
    pub add_files_bytes: u64,
    pub remove_files_bytes: u64,
    pub is_blind_append: bool,
    pub data_change: bool,
    pub operation: KernelStringSlice,
    pub prepare_duration_ns: u64,
    pub committer_duration_ns: u64,
}

impl From<&kernel::TransactionCommitStats> for TransactionCommitStats {
    fn from(stats: &kernel::TransactionCommitStats) -> Self {
        Self {
            commit_version: stats.commit_version,
            num_add_files: stats.num_add_files,
            num_remove_files: stats.num_remove_files,
            num_dv_updates: stats.num_dv_updates,
            add_files_bytes: stats.add_files_bytes,
            remove_files_bytes: stats.remove_files_bytes,
            is_blind_append: stats.is_blind_append,
            data_change: stats.data_change,
            operation: correlation_id_slice(stats.operation.as_deref()),
            prepare_duration_ns: duration_ns(stats.prepare_duration),
            committer_duration_ns: duration_ns(stats.committer_duration),
        }
    }
}

impl From<&kernel::TransactionCommitOutcome> for TransactionCommitOutcome {
    fn from(outcome: &kernel::TransactionCommitOutcome) -> Self {
        match outcome {
            kernel::TransactionCommitOutcome::Success { stats } => Self::Success {
                stats: stats.into(),
            },
            kernel::TransactionCommitOutcome::Failure { reason } => Self::Failure {
                reason: (*reason).into(),
            },
        }
    }
}

/// A [`delta_kernel::Snapshot::checkpoint`] attempt completed.
///
/// Processing and output statistics retain work performed before any outcome, including an error
/// or `AlreadyExists`. Unreached stages report zero, including preflight failures. Only
/// [`CheckpointWriteOutcome::Written`] carries completed-checkpoint contents. An error can occur
/// after data writes, during hint publication or snapshot construction.
#[repr(C)]
pub struct CheckpointWriteCompleted {
    /// Unique identifier for this checkpoint attempt.
    pub operation_id: MetricId,
    /// Snapshot version being checkpointed.
    pub version: u64,
    /// Whether the call wrote a checkpoint, found one already present, or returned an error.
    pub outcome: CheckpointWriteOutcome,
    /// Selected checkpoint write mode.
    pub mode: CheckpointWriteMode,
    /// API entry through checkpoint data completion, or an earlier return, in nanoseconds.
    /// Excludes `_last_checkpoint` publication and returned snapshot construction.
    pub duration_ns: u64,
    /// Work performed while producing checkpoint batches, including on an AlreadyExists return.
    pub processing_stats: CheckpointProcessingStats,
    /// Output work performed, including on an AlreadyExists return after writing starts.
    pub output_stats: CheckpointOutputStats,
}

/// Checkpoint processing times and input action counts. Timings measure elapsed time, including
/// waits, and exclude pulls from preceding stages. They cover only the instrumented pipeline.
/// Action counts include discarded actions and partial work on errors. See
/// [`delta_kernel::metrics::CheckpointProcessingStats`] for the timing and counting boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct CheckpointProcessingStats {
    /// Time setting up and consuming original commit input, in nanoseconds.
    /// Includes reads, decoding, waits, and final or failing iterator calls.
    pub log_tail_input_duration_ns: u64,
    /// Time setting up and consuming the previous checkpoint and its sidecars, in nanoseconds.
    /// Includes schema/footer reads, sidecar-reference extraction, decoding, waits, and final
    /// or failing iterator calls, including for JSON main checkpoints.
    pub checkpoint_input_duration_ns: u64,
    /// Time reconciling actions and constructing selection vectors, in nanoseconds.
    pub reconciliation_duration_ns: u64,
    /// Time evaluating statistics and partition-value transforms for checkpoint output,
    /// in nanoseconds. Reuses or converts existing statistics.
    pub stats_transform_duration_ns: u64,
    /// Time applying selection vectors to transformed batches, in nanoseconds.
    /// With sidecars, also includes projecting and filtering file and non-file actions into
    /// separate batches, and buffering non-file batches for the main checkpoint.
    pub output_preparation_duration_ns: u64,
    /// Input Delta log actions from original commit files after the previous checkpoint,
    /// or from version zero when there is no previous checkpoint.
    pub num_actions_from_log_tail: u64,
    /// Input Delta log actions from the previous checkpoint and its sidecars.
    /// Zero when there is no previous checkpoint.
    pub num_actions_from_checkpoint: u64,
}

impl From<kernel::CheckpointProcessingStats> for CheckpointProcessingStats {
    fn from(stats: kernel::CheckpointProcessingStats) -> Self {
        Self {
            log_tail_input_duration_ns: duration_ns(stats.log_tail_input_duration),
            checkpoint_input_duration_ns: duration_ns(stats.checkpoint_input_duration),
            reconciliation_duration_ns: duration_ns(stats.reconciliation_duration),
            stats_transform_duration_ns: duration_ns(stats.stats_transform_duration),
            output_preparation_duration_ns: duration_ns(stats.output_preparation_duration),
            num_actions_from_log_tail: stats.num_actions_from_log_tail,
            num_actions_from_checkpoint: stats.num_actions_from_checkpoint,
        }
    }
}

/// Output work retained on failures and AlreadyExists returns. Byte and sidecar counts include
/// successful Parquet calls even if verification fails; they exclude bytes from failing calls.
/// See [`delta_kernel::metrics::CheckpointOutputStats`] for the accounting boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct CheckpointOutputStats {
    /// Parquet writer time excluding input iterator pulls, in nanoseconds. Includes failing calls.
    pub parquet_write_duration_ns: u64,
    /// Time querying metadata and verifying written file sizes, in nanoseconds. Includes failures.
    pub verification_duration_ns: u64,
    /// Sizes returned by successful Parquet writes of the main checkpoint and new sidecars.
    pub bytes_written: u64,
    /// Sidecars whose Parquet writes returned successfully.
    pub num_sidecars_written: u64,
}

impl From<kernel::CheckpointOutputStats> for CheckpointOutputStats {
    fn from(stats: kernel::CheckpointOutputStats) -> Self {
        Self {
            parquet_write_duration_ns: duration_ns(stats.parquet_write_duration),
            verification_duration_ns: duration_ns(stats.verification_duration),
            bytes_written: stats.bytes_written,
            num_sidecars_written: stats.num_sidecars_written,
        }
    }
}

/// Contents of a completed checkpoint, including referenced sidecars.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct CheckpointWriteStats {
    /// Main checkpoint size plus every referenced sidecar's size, excluding `_last_checkpoint`.
    pub size_in_bytes: u64,
    /// Active Add actions across the completed checkpoint.
    pub num_add_files: u64,
    /// Retained Remove actions across the completed checkpoint.
    pub num_remove_files: u64,
    /// Action rows across the main checkpoint and referenced sidecars, including sidecar
    /// references.
    pub num_actions: u64,
    /// Total sidecars referenced by the completed checkpoint.
    pub num_sidecars: u64,
}

impl From<kernel::CheckpointWriteStats> for CheckpointWriteStats {
    fn from(stats: kernel::CheckpointWriteStats) -> Self {
        Self {
            size_in_bytes: stats.size_in_bytes,
            num_add_files: stats.num_add_files,
            num_remove_files: stats.num_remove_files,
            num_actions: stats.num_actions,
            num_sidecars: stats.num_sidecars,
        }
    }
}

/// A domain metadata load completed.
#[repr(C)]
pub struct DomainMetadataLoadCompleted {
    pub duration_ns: u64,
    pub outcome: DomainMetadataLoadOutcome,
}

/// C representation of [`kernel::DomainMetadataLoadOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum DomainMetadataLoadOutcome {
    Success {
        from_cache: bool,
        num_domains_returned: u64,
    },
    Error,
}

impl From<&kernel::DomainMetadataLoadOutcome> for DomainMetadataLoadOutcome {
    fn from(outcome: &kernel::DomainMetadataLoadOutcome) -> Self {
        match outcome {
            kernel::DomainMetadataLoadOutcome::Success {
                from_cache,
                num_domains_returned,
            } => Self::Success {
                from_cache: *from_cache,
                num_domains_returned: *num_domains_returned,
            },
            kernel::DomainMetadataLoadOutcome::Error => Self::Error,
        }
    }
}

/// A `SetTransaction` (app id) load completed.
#[repr(C)]
pub struct SetTransactionLoadCompleted {
    pub duration_ns: u64,
    pub outcome: SetTransactionLoadOutcome,
}

/// C representation of [`kernel::SetTransactionLoadOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum SetTransactionLoadOutcome {
    Success { from_cache: bool, found: bool },
    Error,
}

impl From<&kernel::SetTransactionLoadOutcome> for SetTransactionLoadOutcome {
    fn from(outcome: &kernel::SetTransactionLoadOutcome) -> Self {
        match outcome {
            kernel::SetTransactionLoadOutcome::Success { from_cache, found } => Self::Success {
                from_cache: *from_cache,
                found: *found,
            },
            kernel::SetTransactionLoadOutcome::Error => Self::Error,
        }
    }
}

/// A CRC read and parse attempt completed.
#[repr(C)]
pub struct CrcReadCompleted {
    pub duration_ns: u64,
    pub outcome: CrcReadOutcome,
}

/// C representation of [`kernel::CrcReadOutcome`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum CrcReadOutcome {
    Success { bytes_read: u64 },
    Error,
}

impl From<&kernel::CrcReadOutcome> for CrcReadOutcome {
    fn from(outcome: &kernel::CrcReadOutcome) -> Self {
        match outcome {
            kernel::CrcReadOutcome::Success { bytes_read } => Self::Success {
                bytes_read: *bytes_read,
            },
            kernel::CrcReadOutcome::Error => Self::Error,
        }
    }
}

/// A [`delta_kernel::Snapshot::write_checksum`] attempt completed.
///
/// Storage may already contain the CRC if constructing the returned snapshot failed.
#[repr(C)]
pub struct ChecksumWriteCompleted {
    /// Whether the call wrote a new CRC, found one already present, or returned an error.
    pub outcome: ChecksumWriteOutcome,
    /// CRC construction path selected during this call.
    pub resolution_mode: ChecksumWriteResolutionMode,
    /// Wall-clock time from API entry to return.
    pub duration_ns: u64,
}

/// Result of reading and validating a `_last_checkpoint` hint.
///
/// See [`kernel::LastCheckpointReadOutcome`] for return and fallback semantics.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum LastCheckpointReadOutcome {
    /// The hint was parsed and retained.
    Success,
    /// Storage reported that the hint file was absent.
    ///
    /// When the file iterator reports the missing hint, Kernel lists the log to find checkpoints
    /// and commits. If the storage call itself returns the error, it propagates instead.
    NotFound,
    /// The hint was empty, malformed, or discarded by hint validation.
    Invalid,
    /// The attempt returned another error, including cancellation.
    Error,
    /// The outcome was not recorded or its metric label was unrecognized.
    Unknown,
}

impl From<kernel::LastCheckpointReadOutcome> for LastCheckpointReadOutcome {
    fn from(outcome: kernel::LastCheckpointReadOutcome) -> Self {
        match outcome {
            kernel::LastCheckpointReadOutcome::Success => Self::Success,
            kernel::LastCheckpointReadOutcome::NotFound => Self::NotFound,
            kernel::LastCheckpointReadOutcome::Invalid => Self::Invalid,
            kernel::LastCheckpointReadOutcome::Error => Self::Error,
            kernel::LastCheckpointReadOutcome::Unknown => Self::Unknown,
        }
    }
}

/// A `_last_checkpoint` read attempt completed.
#[repr(C)]
pub struct LastCheckpointReadCompleted {
    /// Result of reading and validating the hint.
    pub outcome: LastCheckpointReadOutcome,
    /// Wall-clock time from entry through return, including parsing and validation, in
    /// nanoseconds.
    pub duration_ns: u64,
}

/// A `JsonHandler::read_json_files` call completed.
#[repr(C)]
pub struct JsonReadCompleted {
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A `ParquetHandler::read_parquet_files` call completed.
#[repr(C)]
pub struct ParquetReadCompleted {
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A scan metadata operation completed. See [`delta_kernel::metrics::ScanMetadataCompleted`] for
/// field semantics.
#[repr(C)]
pub struct ScanMetadataCompleted {
    pub context: MetricContext,
    pub scan_type: ScanType,
    pub duration_ns: u64,
    pub num_add_files_seen: u64,
    pub num_add_files_seen_from_delta_files: u64,
    pub num_selected_add_files: u64,
    pub selected_add_files_bytes: u64,
    pub num_remove_files_seen_from_delta_files: u64,
    pub num_non_file_actions: u64,
    pub num_predicate_filtered: u64,
    pub peak_hash_set_size: u64,
    pub dedup_visitor_time_ns: u64,
    pub action_transform_time_ns: u64,
    pub predicate_eval_time_ns: u64,
}

/// A storage list operation completed.
#[repr(C)]
pub struct StorageListCompleted {
    pub duration_ns: u64,
    pub num_files: u64,
}

/// A storage read operation completed.
#[repr(C)]
pub struct StorageReadCompleted {
    pub duration_ns: u64,
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A storage copy or rename operation completed.
#[repr(C)]
pub struct StorageCopyCompleted {
    pub duration_ns: u64,
}

/// C version of [`delta_kernel::metrics::MetricEvent`]. Passed by value to a callback registered
/// via [`crate::ffi_tracing::enable_metrics_reporting`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
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

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Borrow the caller-supplied correlation id as a [`KernelStringSlice`], or an empty slice when
/// unset. The slice is valid only as long as the source kernel event is alive.
fn correlation_id_slice(correlation_id: Option<&str>) -> KernelStringSlice {
    match correlation_id {
        // Safety: the borrowed str lives as long as the source event, which outlives both the FFI
        // event and the callback it is passed to.
        Some(id) => unsafe { KernelStringSlice::new_unsafe(id) },
        None => KernelStringSlice::empty(),
    }
}

impl MetricEvent {
    /// Borrow a kernel event for the duration of the metrics callback.
    pub(crate) fn from_kernel(event: &kernel::MetricEvent) -> Self {
        use kernel::MetricEvent as K;
        match event {
            K::LogSegmentLoadCompleted(e) => {
                Self::LogSegmentLoadCompleted(LogSegmentLoadCompleted {
                    context: (&e.context).into(),
                    load_type: e.load_type.into(),
                    duration_ns: duration_ns(e.duration),
                    outcome: (&e.outcome).into(),
                })
            }
            K::ProtocolMetadataLoadCompleted(e) => {
                Self::ProtocolMetadataLoadCompleted(ProtocolMetadataLoadCompleted {
                    context: (&e.context).into(),
                    load_type: e.load_type.into(),
                    duration_ns: duration_ns(e.duration),
                    outcome: (&e.outcome).into(),
                })
            }
            K::SnapshotBuildCompleted(e) => Self::SnapshotBuildCompleted(SnapshotBuildCompleted {
                context: (&e.context).into(),
                load_type: e.load_type.into(),
                duration_ns: duration_ns(e.duration),
                outcome: (&e.outcome).into(),
            }),
            K::TransactionCommitCompleted(e) => {
                Self::TransactionCommitCompleted(TransactionCommitCompleted {
                    context: (&e.context).into(),
                    duration_ns: duration_ns(e.duration),
                    outcome: (&e.outcome).into(),
                })
            }
            K::DomainMetadataLoadCompleted(e) => {
                Self::DomainMetadataLoadCompleted(DomainMetadataLoadCompleted {
                    duration_ns: duration_ns(e.duration),
                    outcome: (&e.outcome).into(),
                })
            }
            K::SetTransactionLoadCompleted(e) => {
                Self::SetTransactionLoadCompleted(SetTransactionLoadCompleted {
                    duration_ns: duration_ns(e.duration),
                    outcome: (&e.outcome).into(),
                })
            }
            K::CrcReadCompleted(e) => Self::CrcReadCompleted(CrcReadCompleted {
                duration_ns: duration_ns(e.duration),
                outcome: (&e.outcome).into(),
            }),
            K::CheckpointWriteCompleted(e) => {
                Self::CheckpointWriteCompleted(CheckpointWriteCompleted {
                    operation_id: e.operation_id.into(),
                    version: e.version,
                    outcome: e.outcome.into(),
                    mode: e.mode.into(),
                    duration_ns: duration_ns(e.duration),
                    processing_stats: e.processing_stats.into(),
                    output_stats: e.output_stats.into(),
                })
            }
            K::ChecksumWriteCompleted(e) => Self::ChecksumWriteCompleted(ChecksumWriteCompleted {
                outcome: e.outcome.into(),
                resolution_mode: e.resolution_mode.into(),
                duration_ns: duration_ns(e.duration),
            }),
            K::LastCheckpointReadCompleted(e) => {
                Self::LastCheckpointReadCompleted(LastCheckpointReadCompleted {
                    duration_ns: duration_ns(e.duration),
                    outcome: e.outcome.into(),
                })
            }
            K::JsonReadCompleted(kernel::JsonReadCompleted {
                num_files,
                bytes_read,
            }) => Self::JsonReadCompleted(JsonReadCompleted {
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::ParquetReadCompleted(kernel::ParquetReadCompleted {
                num_files,
                bytes_read,
            }) => Self::ParquetReadCompleted(ParquetReadCompleted {
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::ScanMetadataCompleted(kernel::ScanMetadataCompleted {
                context,
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
            }) => Self::ScanMetadataCompleted(ScanMetadataCompleted {
                context: context.into(),
                scan_type: (*scan_type).into(),
                duration_ns: duration_ns(*duration),
                num_add_files_seen: *num_add_files_seen,
                num_add_files_seen_from_delta_files: *num_add_files_seen_from_delta_files,
                num_selected_add_files: *num_selected_add_files,
                selected_add_files_bytes: *selected_add_files_bytes,
                num_remove_files_seen_from_delta_files: *num_remove_files_seen_from_delta_files,
                num_non_file_actions: *num_non_file_actions,
                num_predicate_filtered: *num_predicate_filtered,
                peak_hash_set_size: *peak_hash_set_size as u64,
                dedup_visitor_time_ns: duration_ns(*dedup_visitor_time),
                action_transform_time_ns: duration_ns(*action_transform_time),
                predicate_eval_time_ns: duration_ns(*predicate_eval_time),
            }),
            K::StorageListCompleted(kernel::StorageListCompleted {
                duration,
                num_files,
            }) => Self::StorageListCompleted(StorageListCompleted {
                duration_ns: duration_ns(*duration),
                num_files: *num_files,
            }),
            K::StorageReadCompleted(kernel::StorageReadCompleted {
                duration,
                num_files,
                bytes_read,
            }) => Self::StorageReadCompleted(StorageReadCompleted {
                duration_ns: duration_ns(*duration),
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::StorageCopyCompleted(kernel::StorageCopyCompleted { duration }) => {
                Self::StorageCopyCompleted(StorageCopyCompleted {
                    duration_ns: duration_ns(*duration),
                })
            }
        }
    }
}

/// Invoke `f` with the FFI [`MetricEvent`] built from `event`.
pub(crate) fn with_ffi_event<R>(
    event: &kernel::MetricEvent,
    f: impl FnOnce(MetricEvent) -> R,
) -> R {
    f(MetricEvent::from_kernel(event))
}

#[cfg(test)]
mod tests {
    use std::mem::discriminant;
    use std::time::Duration;

    use rstest::rstest;

    use super::*;
    use crate::TryFromStringSlice;

    #[rstest]
    #[case(
        kernel::LastCheckpointReadOutcome::Success,
        LastCheckpointReadOutcome::Success
    )]
    #[case(
        kernel::LastCheckpointReadOutcome::NotFound,
        LastCheckpointReadOutcome::NotFound
    )]
    #[case(
        kernel::LastCheckpointReadOutcome::Invalid,
        LastCheckpointReadOutcome::Invalid
    )]
    #[case(
        kernel::LastCheckpointReadOutcome::Error,
        LastCheckpointReadOutcome::Error
    )]
    #[case(
        kernel::LastCheckpointReadOutcome::Unknown,
        LastCheckpointReadOutcome::Unknown
    )]
    fn last_checkpoint_read_conversion_preserves_outcome_and_duration(
        #[case] outcome: kernel::LastCheckpointReadOutcome,
        #[case] expected: LastCheckpointReadOutcome,
    ) {
        let event =
            kernel::MetricEvent::LastCheckpointReadCompleted(kernel::LastCheckpointReadCompleted {
                outcome,
                duration: Duration::from_nanos(17),
            });
        with_ffi_event(&event, |event| {
            let MetricEvent::LastCheckpointReadCompleted(event) = event else {
                panic!("expected LastCheckpointReadCompleted");
            };
            assert_eq!(event.outcome, expected);
            assert_eq!(event.duration_ns, 17);
        });
    }

    #[rstest]
    #[case::v1(kernel::CheckpointWriteMode::V1, CheckpointWriteMode::V1)]
    #[case::v2_without_sidecars(
        kernel::CheckpointWriteMode::V2WithoutSidecars,
        CheckpointWriteMode::V2WithoutSidecars
    )]
    #[case::v2_with_sidecars(
        kernel::CheckpointWriteMode::V2WithSidecars,
        CheckpointWriteMode::V2WithSidecars
    )]
    fn from_kernel_checkpoint_written_preserves_stats(
        #[case] mode: kernel::CheckpointWriteMode,
        #[case] expected_mode: CheckpointWriteMode,
        #[values(false, true)] zero_stats: bool,
    ) {
        let operation_id = kernel::MetricId::new();
        let (processing_stats, expected_processing) = checkpoint_processing_stats();
        let output_stats = kernel::CheckpointOutputStats {
            parquet_write_duration: Duration::from_nanos(7),
            verification_duration: Duration::from_nanos(8),
            bytes_written: 9,
            num_sidecars_written: 10,
        };
        let (bytes, adds, removes, actions, sidecars) = if zero_stats {
            (0, 0, 0, 0, 0)
        } else {
            let sidecars = u64::from(mode == kernel::CheckpointWriteMode::V2WithSidecars) * 2;
            (101, 3, 5, 11 + sidecars, sidecars)
        };
        let event =
            kernel::MetricEvent::CheckpointWriteCompleted(kernel::CheckpointWriteCompleted {
                operation_id,
                version: 42,
                outcome: kernel::CheckpointWriteOutcome::Written {
                    stats: kernel::CheckpointWriteStats {
                        size_in_bytes: bytes,
                        num_add_files: adds,
                        num_remove_files: removes,
                        num_actions: actions,
                        num_sidecars: sidecars,
                    },
                },
                mode,
                duration: Duration::from_nanos(123),
                processing_stats,
                output_stats,
            });

        with_ffi_event(&event, |ffi| {
            let MetricEvent::CheckpointWriteCompleted(e) = ffi else {
                panic!("expected CheckpointWriteCompleted");
            };
            assert_eq!(e.mode, expected_mode);
            assert_eq!(e.operation_id.bytes, operation_id.as_bytes());
            assert_eq!(e.version, 42);
            assert_eq!(e.processing_stats, expected_processing);
            assert_eq!(
                e.output_stats,
                CheckpointOutputStats {
                    parquet_write_duration_ns: 7,
                    verification_duration_ns: 8,
                    bytes_written: 9,
                    num_sidecars_written: 10,
                }
            );
            assert_eq!(e.duration_ns, 123);
            assert_eq!(
                e.outcome,
                CheckpointWriteOutcome::Written {
                    stats: CheckpointWriteStats {
                        size_in_bytes: bytes,
                        num_add_files: adds,
                        num_remove_files: removes,
                        num_actions: actions,
                        num_sidecars: sidecars,
                    },
                }
            );
        });
    }

    #[rstest]
    #[case::not_attempted(
        kernel::CheckpointWriteMode::NotAttempted,
        CheckpointWriteMode::NotAttempted
    )]
    #[case::with_sidecars(
        kernel::CheckpointWriteMode::V2WithSidecars,
        CheckpointWriteMode::V2WithSidecars
    )]
    fn from_kernel_checkpoint_nonwritten_preserves_mode_and_duration(
        #[case] mode: kernel::CheckpointWriteMode,
        #[case] expected_mode: CheckpointWriteMode,
        #[values(false, true)] failed: bool,
        #[values(
            (Duration::ZERO, 0),
            (Duration::from_nanos(456), 456),
            (Duration::from_nanos(u64::MAX), u64::MAX),
            (Duration::from_nanos(u64::MAX) + Duration::from_nanos(1), u64::MAX)
        )]
        durations: (Duration, u64),
    ) {
        let operation_id = kernel::MetricId::new();
        let (processing_stats, expected_processing) = checkpoint_processing_stats();
        let (duration, duration_ns) = durations;
        let output_stats = kernel::CheckpointOutputStats {
            parquet_write_duration: duration,
            verification_duration: duration.saturating_add(Duration::from_nanos(1)),
            bytes_written: 9,
            num_sidecars_written: 10,
        };
        let event = if failed {
            kernel::MetricEvent::CheckpointWriteCompleted(kernel::CheckpointWriteCompleted {
                operation_id,
                version: 42,
                mode,
                duration,
                processing_stats,
                output_stats,
                outcome: kernel::CheckpointWriteOutcome::Error,
            })
        } else {
            kernel::MetricEvent::CheckpointWriteCompleted(kernel::CheckpointWriteCompleted {
                operation_id,
                version: 42,
                outcome: kernel::CheckpointWriteOutcome::AlreadyExists,
                mode,
                duration,
                processing_stats,
                output_stats,
            })
        };

        with_ffi_event(&event, |ffi| {
            let (mode, duration, id, version, processing_stats, output_stats) = match ffi {
                MetricEvent::CheckpointWriteCompleted(
                    e @ CheckpointWriteCompleted {
                        outcome: CheckpointWriteOutcome::Error,
                        ..
                    },
                ) if failed => (
                    e.mode,
                    e.duration_ns,
                    e.operation_id,
                    e.version,
                    e.processing_stats,
                    e.output_stats,
                ),
                MetricEvent::CheckpointWriteCompleted(e) if !failed => {
                    assert_eq!(e.outcome, CheckpointWriteOutcome::AlreadyExists);
                    (
                        e.mode,
                        e.duration_ns,
                        e.operation_id,
                        e.version,
                        e.processing_stats,
                        e.output_stats,
                    )
                }
                _ => panic!("expected checkpoint success or failure matching the kernel event"),
            };
            assert_eq!(mode, expected_mode);
            assert_eq!(duration, duration_ns);
            assert_eq!(id.bytes, operation_id.as_bytes());
            assert_eq!(version, 42);
            assert_eq!(processing_stats, expected_processing);
            assert_eq!(
                output_stats,
                CheckpointOutputStats {
                    parquet_write_duration_ns: duration_ns,
                    verification_duration_ns: duration_ns.saturating_add(1),
                    bytes_written: 9,
                    num_sidecars_written: 10,
                }
            );
        });
    }

    fn checkpoint_processing_stats()
    -> (kernel::CheckpointProcessingStats, CheckpointProcessingStats) {
        (
            kernel::CheckpointProcessingStats {
                log_tail_input_duration: Duration::from_nanos(1),
                checkpoint_input_duration: Duration::from_nanos(7),
                reconciliation_duration: Duration::from_nanos(2),
                stats_transform_duration: Duration::from_nanos(3),
                output_preparation_duration: Duration::from_nanos(4),
                num_actions_from_log_tail: 5,
                num_actions_from_checkpoint: 6,
            },
            CheckpointProcessingStats {
                log_tail_input_duration_ns: 1,
                checkpoint_input_duration_ns: 7,
                reconciliation_duration_ns: 2,
                stats_transform_duration_ns: 3,
                output_preparation_duration_ns: 4,
                num_actions_from_log_tail: 5,
                num_actions_from_checkpoint: 6,
            },
        )
    }

    #[test]
    fn from_kernel_transaction_commit_success_carries_operation_and_id() {
        let id = kernel::MetricId::new();
        let event =
            kernel::MetricEvent::TransactionCommitCompleted(kernel::TransactionCommitCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: Some("req-7".into()),
                    table_type: kernel::TableType::CatalogManaged,
                },
                duration: Duration::from_nanos(30),
                outcome: kernel::TransactionCommitOutcome::Success {
                    stats: kernel::TransactionCommitStats {
                        commit_version: 7,
                        num_add_files: 2,
                        num_remove_files: 1,
                        num_dv_updates: 4,
                        add_files_bytes: 100,
                        remove_files_bytes: 50,
                        is_blind_append: true,
                        data_change: false,
                        operation: Some("WRITE".to_string()),
                        prepare_duration: Duration::from_nanos(10),
                        committer_duration: Duration::from_nanos(20),
                    },
                },
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::TransactionCommitCompleted(
                e @ TransactionCommitCompleted {
                    outcome: TransactionCommitOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected TransactionCommitCompleted");
            };
            let TransactionCommitOutcome::Success {
                stats:
                    TransactionCommitStats {
                        operation: e_operation,
                        ..
                    },
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };

            assert_eq!(e.context.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.context.table_type, TableType::CatalogManaged));

            let TransactionCommitOutcome::Success { stats } = &e.outcome else {
                panic!("unexpected metric outcome");
            };
            assert_eq!(stats.commit_version, 7);
            assert_eq!(stats.num_dv_updates, 4);
            assert_eq!(stats.add_files_bytes, 100);
            assert!(stats.is_blind_append);
            assert_eq!(stats.prepare_duration_ns, 10);
            assert_eq!(e.duration_ns, 30);
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(e_operation).unwrap() };
            assert_eq!(op, "WRITE");
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "req-7");
        });
    }

    #[test]
    fn from_kernel_transaction_commit_success_unset_operation_is_empty_slice() {
        let event =
            kernel::MetricEvent::TransactionCommitCompleted(kernel::TransactionCommitCompleted {
                context: kernel::MetricContext {
                    operation_id: kernel::MetricId::new(),
                    correlation_id: None,
                    table_type: kernel::TableType::PathBased,
                },
                duration: Duration::from_nanos(0),
                outcome: kernel::TransactionCommitOutcome::Success {
                    stats: kernel::TransactionCommitStats {
                        commit_version: 1,
                        num_add_files: 0,
                        num_remove_files: 0,
                        num_dv_updates: 0,
                        add_files_bytes: 0,
                        remove_files_bytes: 0,
                        is_blind_append: false,
                        data_change: true,
                        operation: None,
                        prepare_duration: Duration::from_nanos(0),
                        committer_duration: Duration::from_nanos(0),
                    },
                },
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::TransactionCommitCompleted(
                e @ TransactionCommitCompleted {
                    outcome: TransactionCommitOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected TransactionCommitCompleted");
            };
            let TransactionCommitOutcome::Success {
                stats:
                    TransactionCommitStats {
                        operation: e_operation,
                        ..
                    },
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };

            assert!(matches!(e.context.table_type, TableType::PathBased));
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(e_operation).unwrap() };
            assert_eq!(op, "");
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "");
        });
    }

    #[test]
    fn from_kernel_maps_failure_reason() {
        let id = kernel::MetricId::new();
        let failure =
            kernel::MetricEvent::TransactionCommitCompleted(kernel::TransactionCommitCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: Some("req-fail".into()),
                    table_type: kernel::TableType::CatalogManaged,
                },
                duration: std::time::Duration::ZERO,
                outcome: kernel::TransactionCommitOutcome::Failure {
                    reason: kernel::CommitFailureReason::Conflict,
                },
            });
        with_ffi_event(&failure, |ffi| {
            let MetricEvent::TransactionCommitCompleted(
                e @ TransactionCommitCompleted {
                    outcome: TransactionCommitOutcome::Failure { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected TransactionCommitCompleted");
            };

            let TransactionCommitOutcome::Failure {
                reason: metric_e_reason,
                ..
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };
            assert!(matches!(*metric_e_reason, CommitFailureReason::Conflict));
            assert!(matches!(e.context.table_type, TableType::CatalogManaged));
            assert_eq!(e.context.operation_id.bytes, id.as_bytes());
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "req-fail");
        });
    }

    #[rstest]
    #[case::written(
        kernel::ChecksumWriteOutcome::Written { bytes_written: u64::MAX },
        ChecksumWriteOutcome::Written { bytes_written: u64::MAX }
    )]
    #[case::written_zero(
        kernel::ChecksumWriteOutcome::Written { bytes_written: 0 },
        ChecksumWriteOutcome::Written { bytes_written: 0 }
    )]
    #[case::already_exists(
        kernel::ChecksumWriteOutcome::AlreadyExists,
        ChecksumWriteOutcome::AlreadyExists
    )]
    fn from_kernel_checksum_write_success_preserves_payload_mode_and_duration(
        #[case] outcome: kernel::ChecksumWriteOutcome,
        #[case] expected_outcome: ChecksumWriteOutcome,
        #[values(
            (kernel::ChecksumWriteResolutionMode::Full, ChecksumWriteResolutionMode::Full),
            (
                kernel::ChecksumWriteResolutionMode::Incremental,
                ChecksumWriteResolutionMode::Incremental
            ),
            (kernel::ChecksumWriteResolutionMode::Cached, ChecksumWriteResolutionMode::Cached),
            (
                kernel::ChecksumWriteResolutionMode::NotAttempted,
                ChecksumWriteResolutionMode::NotAttempted
            )
        )]
        modes: (
            kernel::ChecksumWriteResolutionMode,
            ChecksumWriteResolutionMode,
        ),
        #[values(
            (Duration::ZERO, 0),
            (Duration::from_nanos(123_456_789), 123_456_789),
            (Duration::from_nanos(u64::MAX), u64::MAX)
        )]
        durations: (Duration, u64),
    ) {
        let (mode, expected_mode) = modes;
        let (duration, expected_duration_ns) = durations;
        let event = kernel::MetricEvent::ChecksumWriteCompleted(kernel::ChecksumWriteCompleted {
            outcome,
            resolution_mode: mode,
            duration,
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ChecksumWriteCompleted(event) = ffi else {
                panic!("expected ChecksumWriteCompleted");
            };
            assert_eq!(event.outcome, expected_outcome);
            assert_eq!(event.resolution_mode, expected_mode);
            assert_eq!(event.duration_ns, expected_duration_ns);
        });
    }

    #[rstest]
    fn from_kernel_checksum_write_failure_preserves_mode_and_duration(
        #[values(
            (kernel::ChecksumWriteResolutionMode::Full, ChecksumWriteResolutionMode::Full),
            (
                kernel::ChecksumWriteResolutionMode::Incremental,
                ChecksumWriteResolutionMode::Incremental
            ),
            (kernel::ChecksumWriteResolutionMode::Cached, ChecksumWriteResolutionMode::Cached),
            (
                kernel::ChecksumWriteResolutionMode::NotAttempted,
                ChecksumWriteResolutionMode::NotAttempted
            )
        )]
        modes: (
            kernel::ChecksumWriteResolutionMode,
            ChecksumWriteResolutionMode,
        ),
        #[values(
            (Duration::ZERO, 0),
            (Duration::from_nanos(123_456_789), 123_456_789),
            (Duration::from_nanos(u64::MAX), u64::MAX)
        )]
        durations: (Duration, u64),
    ) {
        let (mode, expected_mode) = modes;
        let (duration, expected_duration_ns) = durations;
        let event = kernel::MetricEvent::ChecksumWriteCompleted(kernel::ChecksumWriteCompleted {
            resolution_mode: mode,
            duration,
            outcome: kernel::ChecksumWriteOutcome::Error,
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ChecksumWriteCompleted(
                event @ ChecksumWriteCompleted {
                    outcome: ChecksumWriteOutcome::Error,
                    ..
                },
            ) = ffi
            else {
                panic!("expected ChecksumWriteCompleted");
            };
            assert_eq!(event.resolution_mode, expected_mode);
            assert_eq!(event.duration_ns, expected_duration_ns);
        });
    }

    #[test]
    fn from_kernel_scan_metadata_completed_maps_all_fields() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::ScanMetadataCompleted(kernel::ScanMetadataCompleted {
            context: kernel::MetricContext {
                operation_id: id,
                correlation_id: Some("scan-req".into()),
                table_type: kernel::TableType::CatalogManaged,
            },
            scan_type: kernel::ScanType::ParallelPhase,
            duration: Duration::from_nanos(13),
            num_add_files_seen: 17,
            num_add_files_seen_from_delta_files: 18,
            num_selected_add_files: 19,
            selected_add_files_bytes: 23,
            num_remove_files_seen_from_delta_files: 29,
            num_non_file_actions: 31,
            num_predicate_filtered: 37,
            peak_hash_set_size: 41,
            dedup_visitor_time: Duration::from_nanos(43),
            action_transform_time: Duration::from_nanos(47),
            predicate_eval_time: Duration::from_nanos(53),
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ScanMetadataCompleted(e) = ffi else {
                panic!("expected ScanMetadataCompleted");
            };
            assert_eq!(e.context.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.context.table_type, TableType::CatalogManaged));
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "scan-req");
            assert_eq!(
                discriminant(&e.scan_type),
                discriminant(&ScanType::ParallelPhase)
            );
            assert_eq!(e.duration_ns, 13);
            assert_eq!(e.num_add_files_seen, 17);
            assert_eq!(e.num_add_files_seen_from_delta_files, 18);
            assert_eq!(e.num_selected_add_files, 19);
            assert_eq!(e.selected_add_files_bytes, 23);
            assert_eq!(e.num_remove_files_seen_from_delta_files, 29);
            assert_eq!(e.num_non_file_actions, 31);
            assert_eq!(e.num_predicate_filtered, 37);
            assert_eq!(e.peak_hash_set_size, 41);
            assert_eq!(e.dedup_visitor_time_ns, 43);
            assert_eq!(e.action_transform_time_ns, 47);
            assert_eq!(e.predicate_eval_time_ns, 53);
        });
    }

    #[test]
    fn from_kernel_protocol_metadata_load_success_maps_source() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::ProtocolMetadataLoadCompleted(
            kernel::ProtocolMetadataLoadCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: Some("pm-req".into()),
                    table_type: kernel::TableType::CatalogManaged,
                },
                load_type: kernel::LogSegmentLoadType::Incremental,
                duration: Duration::from_nanos(53),
                outcome: kernel::ProtocolMetadataLoadOutcome::Success {
                    source: kernel::ProtocolMetadataSource::CrcAdvancedByReplay,
                },
            },
        );
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ProtocolMetadataLoadCompleted(
                e @ ProtocolMetadataLoadCompleted {
                    outcome: ProtocolMetadataLoadOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected ProtocolMetadataLoadCompleted");
            };
            assert_eq!(e.context.operation_id.bytes, id.as_bytes());
            assert_eq!(e.context.table_type, TableType::CatalogManaged);
            assert_eq!(e.load_type, LogSegmentLoadType::Incremental);
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "pm-req");

            let ProtocolMetadataLoadOutcome::Success {
                source: metric_e_source,
                ..
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };
            assert_eq!(
                discriminant(metric_e_source),
                discriminant(&ProtocolMetadataSource::CrcAdvancedByReplay)
            );
            assert_eq!(e.duration_ns, 53);
        });
    }

    #[test]
    fn from_kernel_log_segment_load_success_carries_correlation_id_and_table_type() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::LogSegmentLoadCompleted(kernel::LogSegmentLoadCompleted {
            context: kernel::MetricContext {
                operation_id: id,
                correlation_id: Some("req-42".into()),
                table_type: kernel::TableType::CatalogManaged,
            },
            load_type: kernel::LogSegmentLoadType::Full,
            duration: Duration::from_nanos(61),
            outcome: kernel::LogSegmentLoadOutcome::Success {
                stats: kernel::LogSegmentLoadStats {
                    num_commit_files: 3,
                    num_checkpoint_files: 1,
                    num_compaction_files: 2,
                    crc_versions_behind: Some(5),
                },
            },
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::LogSegmentLoadCompleted(
                e @ LogSegmentLoadCompleted {
                    outcome: LogSegmentLoadOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected LogSegmentLoadCompleted");
            };
            assert_eq!(e.context.operation_id.bytes, id.as_bytes());
            assert_eq!(e.context.table_type, TableType::CatalogManaged);
            assert_eq!(e.duration_ns, 61);
            assert_eq!(e.load_type, LogSegmentLoadType::Full);

            let LogSegmentLoadOutcome::Success { stats } = &e.outcome else {
                panic!("unexpected metric outcome");
            };
            assert_eq!(stats.num_commit_files, 3);
            assert_eq!(stats.num_checkpoint_files, 1);
            assert_eq!(stats.num_compaction_files, 2);
            assert!(stats.has_crc);
            assert_eq!(stats.crc_versions_behind, 5);
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "req-42");
        });
    }

    #[test]
    fn from_kernel_snapshot_hint_load_type_maps_to_appended_ffi_variant() {
        let event = kernel::MetricEvent::SnapshotBuildCompleted(kernel::SnapshotBuildCompleted {
            context: kernel::MetricContext {
                operation_id: kernel::MetricId::new(),
                correlation_id: None,
                table_type: kernel::TableType::PathBased,
            },
            load_type: kernel::SnapshotLoadType::SnapshotHint,
            duration: Duration::from_nanos(11),
            outcome: kernel::SnapshotBuildOutcome::Success { version: 7 },
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::SnapshotBuildCompleted(
                e @ SnapshotBuildCompleted {
                    outcome: SnapshotBuildOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected SnapshotBuildCompleted");
            };
            assert_eq!(e.load_type, SnapshotLoadType::SnapshotHint);

            let SnapshotBuildOutcome::Success {
                version: metric_e_version,
                ..
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };
            assert_eq!((*metric_e_version), 7);
        });
    }

    #[rstest]
    fn from_kernel_snapshot_lifecycle_failures_preserve_duration(
        #[values(0u64, 53, u64::MAX)] duration_ns: u64,
    ) {
        let id = kernel::MetricId::new();
        let duration = Duration::from_nanos(duration_ns);
        let snapshot =
            kernel::MetricEvent::SnapshotBuildCompleted(kernel::SnapshotBuildCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: None,
                    table_type: kernel::TableType::PathBased,
                },
                load_type: kernel::SnapshotLoadType::SnapshotHint,
                duration,
                outcome: kernel::SnapshotBuildOutcome::Error,
            });
        with_ffi_event(&snapshot, |ffi| {
            assert!(matches!(
                ffi,
                MetricEvent::SnapshotBuildCompleted(SnapshotBuildCompleted {
                    load_type: SnapshotLoadType::SnapshotHint,
                    outcome: SnapshotBuildOutcome::Error,
                    duration_ns: actual_duration,
                    ..
                }) if actual_duration == duration_ns
            ));
        });

        let log_segment =
            kernel::MetricEvent::LogSegmentLoadCompleted(kernel::LogSegmentLoadCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: None,
                    table_type: kernel::TableType::PathBased,
                },
                load_type: kernel::LogSegmentLoadType::Full,
                duration,
                outcome: kernel::LogSegmentLoadOutcome::Error,
            });
        with_ffi_event(&log_segment, |ffi| {
            assert!(matches!(
                ffi,
                MetricEvent::LogSegmentLoadCompleted(LogSegmentLoadCompleted {
                    load_type: LogSegmentLoadType::Full,
                    outcome: LogSegmentLoadOutcome::Error,
                    duration_ns: actual_duration,
                    ..
                }) if actual_duration == duration_ns
            ));
        });

        let protocol_metadata = kernel::MetricEvent::ProtocolMetadataLoadCompleted(
            kernel::ProtocolMetadataLoadCompleted {
                context: kernel::MetricContext {
                    operation_id: id,
                    correlation_id: None,
                    table_type: kernel::TableType::PathBased,
                },
                load_type: kernel::LogSegmentLoadType::Full,
                duration,
                outcome: kernel::ProtocolMetadataLoadOutcome::Error,
            },
        );
        with_ffi_event(&protocol_metadata, |ffi| {
            assert!(matches!(
                ffi,
                MetricEvent::ProtocolMetadataLoadCompleted(ProtocolMetadataLoadCompleted {
                    load_type: LogSegmentLoadType::Full,
                    outcome: ProtocolMetadataLoadOutcome::Error,
                    duration_ns: actual_duration,
                    ..
                }) if actual_duration == duration_ns
            ));
        });
    }

    #[rstest]
    #[case::full(kernel::SnapshotLoadType::Full, SnapshotLoadType::Full)]
    #[case::incremental(kernel::SnapshotLoadType::Incremental, SnapshotLoadType::Incremental)]
    #[case::snapshot_hint(kernel::SnapshotLoadType::SnapshotHint, SnapshotLoadType::SnapshotHint)]
    #[case::unknown(kernel::SnapshotLoadType::Unknown, SnapshotLoadType::Unknown)]
    fn kernel_snapshot_load_type_maps_to_ffi_variant(
        #[case] source: kernel::SnapshotLoadType,
        #[case] expected: SnapshotLoadType,
    ) {
        assert_eq!(SnapshotLoadType::from(source), expected);
    }

    #[test]
    fn snapshot_load_type_discriminants_are_stable() {
        assert_eq!(SnapshotLoadType::Full as i32, 0);
        assert_eq!(SnapshotLoadType::Incremental as i32, 1);
        assert_eq!(SnapshotLoadType::SnapshotHint as i32, 2);
        assert_eq!(SnapshotLoadType::Unknown as i32, 3);
    }

    #[test]
    fn from_kernel_log_segment_load_success_unset_correlation_id_is_empty_slice() {
        let event = kernel::MetricEvent::LogSegmentLoadCompleted(kernel::LogSegmentLoadCompleted {
            context: kernel::MetricContext {
                operation_id: kernel::MetricId::new(),
                correlation_id: None,
                table_type: kernel::TableType::PathBased,
            },
            load_type: kernel::LogSegmentLoadType::Full,
            duration: Duration::from_nanos(0),
            outcome: kernel::LogSegmentLoadOutcome::Success {
                stats: kernel::LogSegmentLoadStats {
                    num_commit_files: 0,
                    num_checkpoint_files: 0,
                    num_compaction_files: 0,
                    crc_versions_behind: None,
                },
            },
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::LogSegmentLoadCompleted(
                e @ LogSegmentLoadCompleted {
                    outcome: LogSegmentLoadOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected LogSegmentLoadCompleted");
            };
            assert_eq!(e.context.table_type, TableType::PathBased);

            let LogSegmentLoadOutcome::Success { stats } = &e.outcome else {
                panic!("unexpected metric outcome");
            };
            assert!(!stats.has_crc);
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.context.correlation_id).unwrap() };
            assert_eq!(cid, "");
        });
    }

    #[test]
    fn from_kernel_set_transaction_load_success_distinguishes_bools() {
        // from_cache and found are adjacent bools that are trivial to swap; assert distinct values.
        let event =
            kernel::MetricEvent::SetTransactionLoadCompleted(kernel::SetTransactionLoadCompleted {
                duration: Duration::from_nanos(53),
                outcome: kernel::SetTransactionLoadOutcome::Success {
                    from_cache: true,
                    found: false,
                },
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::SetTransactionLoadCompleted(
                e @ SetTransactionLoadCompleted {
                    outcome: SetTransactionLoadOutcome::Success { .. },
                    ..
                },
            ) = ffi
            else {
                panic!("expected SetTransactionLoadCompleted");
            };

            let SetTransactionLoadOutcome::Success {
                from_cache: metric_e_from_cache,
                found: metric_e_found,
                ..
            } = &e.outcome
            else {
                panic!("unexpected metric outcome");
            };
            assert!((*metric_e_from_cache));
            assert!(!(*metric_e_found));
            assert_eq!(e.duration_ns, 53);
        });
    }
    #[rstest]
    fn domain_metadata_success_preserves_cache_status_and_count(
        #[values(false, true)] from_cache: bool,
        #[values(0, 37, u64::MAX)] num_domains_returned: u64,
    ) {
        let event =
            kernel::MetricEvent::DomainMetadataLoadCompleted(kernel::DomainMetadataLoadCompleted {
                duration: Duration::from_nanos(53),
                outcome: kernel::DomainMetadataLoadOutcome::Success {
                    from_cache,
                    num_domains_returned,
                },
            });
        with_ffi_event(&event, |ffi| {
            assert!(matches!(
                ffi,
                MetricEvent::DomainMetadataLoadCompleted(DomainMetadataLoadCompleted {
                    duration_ns: 53,
                    outcome: DomainMetadataLoadOutcome::Success {
                        from_cache: actual_cache,
                        num_domains_returned: actual_count,
                    },
                }) if actual_cache == from_cache && actual_count == num_domains_returned
            ));
        });
    }

    #[rstest]
    fn crc_success_preserves_byte_count(#[values(0, 37, u64::MAX)] bytes_read: u64) {
        let event = kernel::MetricEvent::CrcReadCompleted(kernel::CrcReadCompleted {
            duration: Duration::from_nanos(71),
            outcome: kernel::CrcReadOutcome::Success { bytes_read },
        });
        with_ffi_event(&event, |ffi| {
            assert!(matches!(
                ffi,
                MetricEvent::CrcReadCompleted(CrcReadCompleted {
                    duration_ns: 71,
                    outcome: CrcReadOutcome::Success { bytes_read: actual },
                }) if actual == bytes_read
            ));
        });
    }

    #[rstest]
    #[case::crc(kernel::MetricEvent::CrcReadCompleted(kernel::CrcReadCompleted {
        duration: Duration::from_nanos(29), outcome: kernel::CrcReadOutcome::Error,
    }))]
    #[case::domain(kernel::MetricEvent::DomainMetadataLoadCompleted(kernel::DomainMetadataLoadCompleted {
        duration: Duration::from_nanos(29), outcome: kernel::DomainMetadataLoadOutcome::Error,
    }))]
    #[case::set_transaction(kernel::MetricEvent::SetTransactionLoadCompleted(kernel::SetTransactionLoadCompleted {
        duration: Duration::from_nanos(29), outcome: kernel::SetTransactionLoadOutcome::Error,
    }))]
    fn error_outcomes_preserve_duration_in_ffi(#[case] event: kernel::MetricEvent) {
        with_ffi_event(&event, |ffi| match ffi {
            MetricEvent::CrcReadCompleted(CrcReadCompleted {
                duration_ns,
                outcome: CrcReadOutcome::Error,
            })
            | MetricEvent::DomainMetadataLoadCompleted(DomainMetadataLoadCompleted {
                duration_ns,
                outcome: DomainMetadataLoadOutcome::Error,
            })
            | MetricEvent::SetTransactionLoadCompleted(SetTransactionLoadCompleted {
                duration_ns,
                outcome: SetTransactionLoadOutcome::Error,
            }) => assert_eq!(duration_ns, 29),
            _ => panic!("unexpected metric outcome"),
        });
    }
}
