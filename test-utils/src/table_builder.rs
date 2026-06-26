// rstest_reuse templates generate macros that appear unused when building the lib target
// (they're only consumed by tests in this and other crates).
#![allow(unused_macros)]

//! A composable test table builder for delta-kernel-rs.
//!
//! The builder writes data by default (1 parquet file with 10 rows per commit). Override
//! with [`TestTableBuilder::with_data`] when a test needs specific file/row counts.
//!
//! Provides five orthogonal axes for parameterized testing:
//! - [`LogState`]: what log files exist on disk (commits, checkpoints, CRC)
//! - [`FeatureSet`]: which Delta table features are enabled
//! - [`DataLayoutConfig`]: data layout (unpartitioned, partitioned, clustered)
//! - [`TableConfig`]: runtime knobs (e.g. checkpoint stats format)
//! - [`VersionTarget`]: how the snapshot is loaded (latest, time travel, incremental)
//!
//! # Quick start
//!
//! The `test_context!` macro builds a table, engine, and snapshot in one call.
//! Pair it with rstest `#[values]` for cross-product testing:
//!
//! ```ignore
//! use rstest::rstest;
//! use test_utils::table_builder::*;
//! use test_utils::test_context;
//!
//! #[rstest]
//! fn test_scan(
//!     #[values(LogState::with_latest_version(2))]
//!     log_state: LogState,
//!     #[values(FeatureSet::empty())]
//!     feature_set: FeatureSet,
//!     #[values(unpartitioned())]
//!     data_layout: DataLayoutConfig,
//!     #[values(checkpoint_json_stats())]
//!     table_config: TableConfig,
//!     #[values(VersionTarget::Latest, VersionTarget::IncrementalToLatest { from: 0 })]
//!     version_target: VersionTarget,
//! ) {
//!     let (engine, snap, _table) = test_context!(
//!         log_state, feature_set, data_layout, table_config, version_target,
//!     );
//!     let scan = snap.scan_builder().build().unwrap();
//!     // ...
//! }
//! ```
//!
//! Requires `Snapshot` and `DefaultEngineBuilder` to be in scope at the call site.
//! The macros expand there, so types resolve to the caller's kernel crate -- avoiding
//! the type mismatch between `test_utils`'s kernel and `kernel/src/` unit tests.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, Error as ObjectStoreError, ObjectStoreExt as _};
use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef, StructField, StructType};
use delta_kernel::snapshot::ChecksumWriteResult;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel_default_engine::executor::TaskExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};

// ===========================================================================
// Sweep constants
// ===========================================================================

/// Latest commit version used by every log state in the
/// [`default_sweep!`](crate::default_sweep) template.
pub const DEFAULT_SWEEP_LATEST_VERSION: u64 = 10;

/// Mid version used by the [`default_sweep!`](crate::default_sweep) template for
/// `AtVersion` and `IncrementalToLatest` targets. Must satisfy
/// `mid <= DEFAULT_SWEEP_LATEST_VERSION`.
pub const DEFAULT_SWEEP_MID_VERSION: u64 = 5;

// ===========================================================================
// Sync/async bridge
// ===========================================================================

/// Run `make_fut` to completion on a dedicated multi-threaded tokio runtime in a
/// scoped background thread. Safe to call from both sync tests and `#[tokio::test]`
/// bodies -- the scoped thread avoids the nested-runtime panic that occurs when
/// calling `Runtime::block_on` from a thread that already owns a tokio runtime.
///
/// A multi-threaded runtime is required so kernel operations that call
/// `block_in_place` (e.g. `Snapshot::checkpoint`) do not deadlock, which is why
/// the sync wrappers in this module all route through this helper.
fn block_on_sync<F, Fut, T>(make_fut: F) -> DeltaResult<T>
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = DeltaResult<T>>,
    T: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
            runtime.block_on(make_fut())
        })
        .join()
        .expect("block_on_sync thread panicked")
    })
}

// ===========================================================================
// LogState
// ===========================================================================

/// Format kernel uses when writing each checkpoint in a [`LogState`]. Mirrors
/// kernel's [`CheckpointSpec`], but with a soft-coupling fallback for
/// cross-product tests: requesting V2 sidecars on a table that doesn't have the
/// `v2Checkpoint` feature falls back to kernel's default rather than erroring.
#[derive(Clone, Debug, Default)]
pub enum CheckpointFormat {
    /// Kernel picks V1 or V2-NoSidecar based on the `v2Checkpoint` feature flag.
    #[default]
    Default,
    /// V2 with sidecar files. Honored only when `v2Checkpoint` is enabled;
    /// silently falls back to [`CheckpointFormat::Default`] otherwise.
    V2WithSidecarsIfEnabled {
        /// Suggested file actions per sidecar; `None` uses the kernel default.
        file_actions_per_sidecar_hint: Option<usize>,
    },
}

/// State of the `_delta_log/_last_checkpoint` hint file on disk.
///
/// Three states a kernel reader must handle:
///
/// - [`Present`](Self::Present): hint file exists and points at the highest checkpoint on disk.
///   Default; what `Snapshot::checkpoint` writes.
/// - [`Missing`](Self::Missing): no hint file. Forces the reader's listing fallback to discover the
///   latest checkpoint.
/// - [`Stale`](Self::Stale): hint file exists but points at an OLDER real checkpoint, not the
///   latest. The reader follows the hint, lists from there, and picks up the actual latest
///   checkpoint by listing forward (logging an info on the version mismatch).
///
/// `Stale` is only meaningful with at least two checkpoints in the [`LogState`] -- the hint
/// rewrites to point at the lowest checkpoint, leaving the highest checkpoint as the actual
/// "latest" the reader should discover. Pairing `Stale` with fewer than two checkpoints
/// panics at build time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LastCheckpointHintState {
    #[default]
    Present,
    Missing,
    Stale,
}

impl fmt::Display for LastCheckpointHintState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Present => write!(f, "present"),
            Self::Missing => write!(f, "missing"),
            Self::Stale => write!(f, "stale"),
        }
    }
}

/// Shape of a Delta table's `_delta_log/` directory: commits, checkpoints, CRC
/// files, the `_last_checkpoint` hint, and log cleanup.
#[derive(Clone, Debug)]
pub struct LogState {
    /// Latest version on the table. Versions `0..=latest_version` exist as
    /// commits on disk (v=0 is the create-table commit); `latest_version + 1`
    /// total commits.
    latest_version: u64,
    /// Sorted ascending, distinct, all `<= latest_version`.
    checkpoints_at: Vec<u64>,
    /// Format applied to every checkpoint in `checkpoints_at`.
    checkpoint_format: CheckpointFormat,
    /// Sorted ascending, distinct, all `<= latest_version`. A `.crc` file is
    /// written at each listed version.
    crcs_at: Vec<u64>,
    /// If `Some(n)`, log files at versions `< n` are deleted after the table is built.
    cleanup_before: Option<u64>,
    /// State of the `_last_checkpoint` hint file. Defaults to `Present`.
    last_checkpoint_hint: LastCheckpointHintState,
}

impl LogState {
    /// Build a table whose latest version is `n`. Produces `n + 1` commits
    /// (v=0 create + v=1..=n data). No checkpoints by default.
    pub fn with_latest_version(n: u64) -> Self {
        Self {
            latest_version: n,
            checkpoints_at: Vec::new(),
            checkpoint_format: CheckpointFormat::Default,
            crcs_at: Vec::new(),
            cleanup_before: None,
            last_checkpoint_hint: LastCheckpointHintState::Present,
        }
    }

    /// Add checkpoints at the given versions. Pass `[v]` for a single
    /// checkpoint or `[v1, v2, ...]` for multiple. Each `v` must be
    /// `<= latest_version` and not already present.
    ///
    /// # Panics
    ///
    /// Panics if any `v > latest_version` or is already present.
    pub fn with_checkpoint_at(mut self, vs: impl IntoIterator<Item = u64>) -> Self {
        for v in vs {
            assert!(
                v <= self.latest_version,
                "checkpoint_at ({v}) must be <= latest_version ({})",
                self.latest_version,
            );
            assert!(
                !self.checkpoints_at.contains(&v),
                "checkpoint_at ({v}) already present in {:?}",
                self.checkpoints_at,
            );
            self.checkpoints_at.push(v);
        }
        self.checkpoints_at.sort_unstable();
        self
    }

    /// Optionally add a checkpoint at the given version. No-op when `None`.
    /// Useful for cartesian-product rstest axes parameterized as
    /// `#[values(None, Some(v))]`.
    pub fn maybe_with_checkpoint_at(self, v: Option<u64>) -> Self {
        match v {
            Some(v) => self.with_checkpoint_at([v]),
            None => self,
        }
    }

    /// Add CRC files at the given versions. Pass `[v]` for a single CRC or
    /// `[v1, v2, ...]` for multiple. Each `v` must be `<= latest_version` and
    /// not already present.
    ///
    /// # Panics
    ///
    /// Panics if any `v > latest_version` or is already present.
    pub fn with_crc_at(mut self, vs: impl IntoIterator<Item = u64>) -> Self {
        for v in vs {
            assert!(
                v <= self.latest_version,
                "crc_at ({v}) must be <= latest_version ({})",
                self.latest_version,
            );
            assert!(
                !self.crcs_at.contains(&v),
                "crc_at ({v}) already present in {:?}",
                self.crcs_at,
            );
            self.crcs_at.push(v);
        }
        self.crcs_at.sort_unstable();
        self
    }

    /// Switch every checkpoint to V2 with sidecars. Pass `None` for the kernel
    /// default `file_actions_per_sidecar_hint`, or `Some(n)` to override.
    /// Honored only when the paired [`FeatureSet`] enables `v2_checkpoint()`;
    /// silently falls back to the default format otherwise so that
    /// `#[values]` sweeps mixing both axes don't fail on invalid combinations.
    pub fn with_sidecars_if_enabled(
        mut self,
        file_actions_per_sidecar_hint: Option<usize>,
    ) -> Self {
        self.checkpoint_format = CheckpointFormat::V2WithSidecarsIfEnabled {
            file_actions_per_sidecar_hint,
        };
        self
    }

    /// Simulate log cleanup by deleting files at versions `< n`. Files at
    /// `n..=latest_version` (including the JSON and checkpoint at `v=n`) survive.
    /// Requires a checkpoint at `v=n` and `n` in `1..=latest_version`.
    pub fn with_cleanup_commits_before(mut self, n: u64) -> Self {
        self.cleanup_before = Some(n);
        self
    }

    /// Set the `_delta_log/_last_checkpoint` hint state.
    ///
    /// `Stale` requires at least two checkpoints in the LogState; the builder
    /// asserts on the precondition at `build()` time.
    pub fn with_last_checkpoint_hint(mut self, state: LastCheckpointHintState) -> Self {
        self.last_checkpoint_hint = state;
        self
    }

    /// Latest version on the table. The total number of commits on disk is
    /// `latest_version + 1` (or fewer if
    /// [`with_cleanup_commits_before`](Self::with_cleanup_commits_before) removed earlier
    /// versions).
    pub fn latest_version(&self) -> u64 {
        self.latest_version
    }

    /// Versions at which checkpoints are written, in ascending order.
    pub(crate) fn checkpoints_at(&self) -> &[u64] {
        &self.checkpoints_at
    }

    /// Format applied to every checkpoint.
    pub(crate) fn checkpoint_format(&self) -> &CheckpointFormat {
        &self.checkpoint_format
    }

    /// Versions at which CRC files are written, in ascending order.
    pub(crate) fn crcs_at(&self) -> &[u64] {
        &self.crcs_at
    }

    /// Version below which commits and checkpoints have been cleaned up, if any.
    pub(crate) fn cleanup_before(&self) -> Option<u64> {
        self.cleanup_before
    }

    /// State of the `_last_checkpoint` hint file on the built table.
    pub(crate) fn last_checkpoint_hint(&self) -> LastCheckpointHintState {
        self.last_checkpoint_hint
    }
}

// Canonical sweep rows for the LogState axis. Test case names derive from these
// function names (e.g. `log_state_1_commits_only__`).

pub fn commits_only() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
}

pub fn checkpoint_at_end() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_checkpoint_at([DEFAULT_SWEEP_LATEST_VERSION])
}

pub fn checkpoint_at_end_no_hint() -> LogState {
    checkpoint_at_end().with_last_checkpoint_hint(LastCheckpointHintState::Missing)
}

pub fn checkpoint_mid() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_checkpoint_at([DEFAULT_SWEEP_MID_VERSION])
}

pub fn checkpoint_mid_no_hint() -> LogState {
    checkpoint_mid().with_last_checkpoint_hint(LastCheckpointHintState::Missing)
}

pub fn two_checkpoints_stale_hint() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_checkpoint_at([DEFAULT_SWEEP_MID_VERSION, DEFAULT_SWEEP_LATEST_VERSION])
        .with_last_checkpoint_hint(LastCheckpointHintState::Stale)
}

pub fn crc_at_end() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_crc_at([DEFAULT_SWEEP_LATEST_VERSION])
}

pub fn crc_at_mid() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_crc_at([DEFAULT_SWEEP_MID_VERSION])
}

pub fn checkpoint_at_end_crc_at_end() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_checkpoint_at([DEFAULT_SWEEP_LATEST_VERSION])
        .with_crc_at([DEFAULT_SWEEP_LATEST_VERSION])
}

// Post-cleanup variants: same shapes as above but with log cleanup applied at MID.
// Cleanup at MID (not at LATEST) keeps commits MID..=LATEST reachable, so the canonical
// `at_version(MID)` and `incremental_to_latest { from: MID }` targets still resolve.

pub fn checkpoint_at_end_post_cleanup() -> LogState {
    LogState::with_latest_version(DEFAULT_SWEEP_LATEST_VERSION)
        .with_checkpoint_at([DEFAULT_SWEEP_MID_VERSION, DEFAULT_SWEEP_LATEST_VERSION])
        .with_cleanup_commits_before(DEFAULT_SWEEP_MID_VERSION)
}

pub fn checkpoint_at_end_no_hint_post_cleanup() -> LogState {
    checkpoint_at_end_post_cleanup().with_last_checkpoint_hint(LastCheckpointHintState::Missing)
}

pub fn checkpoint_mid_post_cleanup() -> LogState {
    checkpoint_mid().with_cleanup_commits_before(DEFAULT_SWEEP_MID_VERSION)
}

pub fn checkpoint_mid_no_hint_post_cleanup() -> LogState {
    checkpoint_mid_no_hint().with_cleanup_commits_before(DEFAULT_SWEEP_MID_VERSION)
}

pub fn two_checkpoints_stale_hint_post_cleanup() -> LogState {
    two_checkpoints_stale_hint().with_cleanup_commits_before(DEFAULT_SWEEP_MID_VERSION)
}

pub fn checkpoint_mid_crc_at_mid_post_cleanup() -> LogState {
    checkpoint_mid_post_cleanup().with_crc_at([DEFAULT_SWEEP_MID_VERSION])
}

pub fn checkpoint_mid_crc_above_mid_post_cleanup() -> LogState {
    // Version 8 lies strictly between MID=5 and LATEST=10.
    checkpoint_mid_post_cleanup().with_crc_at([8])
}

pub fn checkpoint_mid_crc_at_end_post_cleanup() -> LogState {
    checkpoint_mid_post_cleanup().with_crc_at([DEFAULT_SWEEP_LATEST_VERSION])
}

/// Extract the version from a versioned log file. Returns `None` for unversioned
/// files like `_last_checkpoint`.
fn log_file_version(location: &Path) -> Option<u64> {
    let filename = location.filename()?;
    let (prefix, _) = filename.split_once('.')?;
    prefix.parse::<u64>().ok()
}

async fn read_hint_bytes(store: &Arc<DynObjectStore>, path: &Path) -> DeltaResult<Vec<u8>> {
    let result = store.get(path).await.map_err(delta_kernel::Error::from)?;
    let bytes = result.bytes().await.map_err(delta_kernel::Error::from)?;
    Ok(bytes.to_vec())
}

/// Up-front validation so panics surface on the caller's thread, not through
/// `block_on_sync`'s worker-thread join boundary.
fn validate_log_state(log_state: &LogState) {
    if let Some(n) = log_state.cleanup_before() {
        assert!(
            n >= 1,
            "with_cleanup_commits_before(n) requires n >= 1 (cleanup_before(0) is a no-op)",
        );
        assert!(
            n <= log_state.latest_version(),
            "with_cleanup_commits_before({n}) exceeds latest_version ({})",
            log_state.latest_version(),
        );
        assert!(
            log_state.checkpoints_at().contains(&n),
            "with_cleanup_commits_before({n}) requires a checkpoint at v={n}; \
             pair with `with_checkpoint_at([{n}, ...])`",
        );
    }
    if log_state.last_checkpoint_hint() == LastCheckpointHintState::Stale {
        assert!(
            log_state.checkpoints_at().len() >= 2,
            "Stale hint requires at least 2 checkpoints (one to be stale relative to); \
             pair with `with_checkpoint_at` at two distinct versions",
        );
    }
}

impl fmt::Display for LogState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v={}", self.latest_version)?;
        for v in &self.checkpoints_at {
            write!(f, "+checkpoint_at({v})")?;
        }
        for v in &self.crcs_at {
            write!(f, "+crc_at({v})")?;
        }
        if let Some(n) = self.cleanup_before {
            write!(f, "+cleanup_before({n})")?;
        }
        if self.last_checkpoint_hint != LastCheckpointHintState::Present {
            write!(f, "+hint({})", self.last_checkpoint_hint)?;
        }
        match self.checkpoint_format {
            CheckpointFormat::V2WithSidecarsIfEnabled {
                file_actions_per_sidecar_hint: Some(n),
            } => write!(f, "+sidecars({n})")?,
            CheckpointFormat::V2WithSidecarsIfEnabled {
                file_actions_per_sidecar_hint: None,
            } => write!(f, "+sidecars")?,
            CheckpointFormat::Default => {}
        }
        Ok(())
    }
}

// ===========================================================================
// FeatureSet
// ===========================================================================

/// Which Delta table features to enable. Methods chain for composability.
///
/// Stores table properties passed to the `CreateTable` API, which handles protocol
/// derivation, schema annotations (column mapping), and feature auto-enablement.
#[derive(Clone, Debug, Default)]
pub struct FeatureSet {
    pub(crate) table_properties: Vec<(String, String)>,
}

impl FeatureSet {
    /// No features enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Alias for `new()`. Reads better when no features are intended.
    pub fn empty() -> Self {
        Self::new()
    }

    // === Feature methods ===
    //
    // Each method enables one table feature via its natural property:
    //   - Enablement property (e.g. delta.enableInCommitTimestamps=true) for features that have one
    //   - Feature signal (delta.feature.X=supported) for protocol-only features
    //
    // TimestampNtz and clustering are NOT FeatureSet methods -- they are driven by
    // the schema (TimestampNtz columns auto-enable the feature) and DataLayoutConfig
    // (clustering via with_data_layout) respectively.

    /// Set column mapping mode ("name" or "id"). Passing "none" is a no-op.
    pub fn column_mapping(mut self, mode: &str) -> Self {
        if mode != "none" {
            self.table_properties
                .push(("delta.columnMapping.mode".into(), mode.into()));
        }
        self
    }

    pub fn ict(mut self) -> Self {
        self.table_properties
            .push(("delta.enableInCommitTimestamps".into(), "true".into()));
        self
    }

    pub fn v2_checkpoint(mut self) -> Self {
        self.table_properties
            .push(("delta.feature.v2Checkpoint".into(), "supported".into()));
        self
    }

    pub fn deletion_vectors(mut self) -> Self {
        self.table_properties
            .push(("delta.enableDeletionVectors".into(), "true".into()));
        self
    }

    pub fn append_only(mut self) -> Self {
        self.table_properties
            .push(("delta.appendOnly".into(), "true".into()));
        self
    }

    pub fn change_data_feed(mut self) -> Self {
        self.table_properties
            .push(("delta.enableChangeDataFeed".into(), "true".into()));
        self
    }

    pub fn type_widening(mut self) -> Self {
        self.table_properties
            .push(("delta.enableTypeWidening".into(), "true".into()));
        self
    }

    pub fn domain_metadata(mut self) -> Self {
        self.table_properties
            .push(("delta.feature.domainMetadata".into(), "supported".into()));
        self
    }

    pub fn vacuum_protocol_check(mut self) -> Self {
        self.table_properties.push((
            "delta.feature.vacuumProtocolCheck".into(),
            "supported".into(),
        ));
        self
    }

    pub fn row_tracking(mut self) -> Self {
        self.table_properties
            .push(("delta.enableRowTracking".into(), "true".into()));
        self
    }

    /// Set an arbitrary table property. Useful for properties that don't have a
    /// dedicated method.
    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.table_properties.push((key.into(), value.into()));
        self
    }

    /// Returns the table features implied by the properties in this set. Used by tests
    /// to check that each builder method actually enables the right feature.
    pub fn expected_features(&self) -> Vec<TableFeature> {
        let mut out = Vec::new();
        for (k, _) in &self.table_properties {
            match k.as_str() {
                "delta.columnMapping.mode" => out.push(TableFeature::ColumnMapping),
                "delta.enableInCommitTimestamps" => out.push(TableFeature::InCommitTimestamp),
                "delta.feature.v2Checkpoint" => out.push(TableFeature::V2Checkpoint),
                "delta.enableDeletionVectors" => out.push(TableFeature::DeletionVectors),
                "delta.appendOnly" => out.push(TableFeature::AppendOnly),
                "delta.enableChangeDataFeed" => out.push(TableFeature::ChangeDataFeed),
                "delta.enableTypeWidening" => out.push(TableFeature::TypeWidening),
                "delta.feature.domainMetadata" => out.push(TableFeature::DomainMetadata),
                "delta.feature.vacuumProtocolCheck" => out.push(TableFeature::VacuumProtocolCheck),
                "delta.enableRowTracking" => {
                    out.push(TableFeature::RowTracking);
                    out.push(TableFeature::DomainMetadata); // row tracking depends on DM
                }
                _ => {}
            }
        }
        out
    }
}

impl fmt::Display for FeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.table_properties.is_empty() {
            return write!(f, "empty");
        }
        let props: Vec<_> = self
            .table_properties
            .iter()
            .map(|(k, v)| {
                // Strip common prefixes for readability
                let short_key = k
                    .strip_prefix("delta.feature.")
                    .or_else(|| k.strip_prefix("delta."))
                    .unwrap_or(k);
                format!("{short_key}={v}")
            })
            .collect();
        write!(f, "{}", props.join(", "))
    }
}

// Canonical sweep rows for the FeatureSet axis.

pub fn no_features() -> FeatureSet {
    FeatureSet::empty()
}

pub fn all_features_cm_id() -> FeatureSet {
    all_features_base().column_mapping("id")
}

pub fn all_features_cm_name() -> FeatureSet {
    all_features_base().column_mapping("name")
}

fn all_features_base() -> FeatureSet {
    FeatureSet::new()
        .deletion_vectors()
        .row_tracking()
        .domain_metadata()
        .ict()
        .v2_checkpoint()
        .vacuum_protocol_check()
        .change_data_feed()
        .append_only()
}

// ===========================================================================
// TableConfig
// ===========================================================================

/// Table configuration properties that are orthogonal to table features.
///
/// These are pure runtime knobs (stats format, checkpoint interval, file sizing,
/// etc.) that don't affect the protocol or enable features. Crossed with
/// [`DataLayoutConfig`] in the sweep so layout shape and write-time properties
/// vary independently.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableConfig {
    pub(crate) table_properties: Vec<(String, String)>,
}

impl TableConfig {
    /// Default table configuration (no properties set).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set `delta.checkpoint.writeStatsAsJson`.
    pub fn write_stats_as_json(mut self, enabled: bool) -> Self {
        self.table_properties.push((
            "delta.checkpoint.writeStatsAsJson".into(),
            enabled.to_string(),
        ));
        self
    }

    /// Set `delta.checkpoint.writeStatsAsStruct`.
    pub fn write_stats_as_struct(mut self, enabled: bool) -> Self {
        self.table_properties.push((
            "delta.checkpoint.writeStatsAsStruct".into(),
            enabled.to_string(),
        ));
        self
    }

    /// Set `delta.dataSkippingNumIndexedCols` to `n`: stats are capped to the first `n` leaf
    /// columns. `-1` means all columns; the protocol default when unset is 32.
    pub fn num_data_skipping_indexed_cols(mut self, n: i64) -> Self {
        self.table_properties
            .push(("delta.dataSkippingNumIndexedCols".into(), n.to_string()));
        self
    }

    /// Set `delta.dataSkippingStatsColumns` to a comma-separated list of column names.
    /// Per the Delta spec this takes precedence over `dataSkippingNumIndexedCols`. The
    /// builder joins names verbatim and does not validate them; entries that don't
    /// resolve in the active schema are silently skipped by kernel (with a warning).
    pub fn data_skipping_stats_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let joined = columns
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>()
            .join(",");
        self.table_properties
            .push(("delta.dataSkippingStatsColumns".into(), joined));
        self
    }
}

impl fmt::Display for TableConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.table_properties.is_empty() {
            return write!(f, "default");
        }
        let props: Vec<_> = self
            .table_properties
            .iter()
            .map(|(k, v)| {
                let short_key = k
                    .strip_prefix("delta.checkpoint.")
                    .or_else(|| k.strip_prefix("delta."))
                    .unwrap_or(k);
                format!("{short_key}={v}")
            })
            .collect();
        write!(f, "{}", props.join(", "))
    }
}

// Canonical sweep rows for the `TableConfig` axis. The sweep covers three checkpoint
// stats encodings plus six data-skipping rows: three count-based (`numIndexedCols`)
// values and three named-columns (`dataSkippingStatsColumns`) shapes, so each knob
// varies across the full LogState x FeatureSet x DataLayout x VersionTarget cross
// product. Per-commit add-file stats are always written and unaffected by the
// checkpoint format flags.

/// Stats-column names that span the schemas used by [`DataLayoutConfig`]: `int_col`
/// exists in [`default_schema`], `value` in [`partitioned_schema`] and
/// [`clustered_schema`]. Kernel silently skips entries that don't exist in the active
/// schema, so each layout collects stats on whichever entry matches.
pub(crate) const STATS_COLUMNS_BY_NAME: &[&str] = &["int_col", "value"];

// === Checkpoint stats encoding rows ===

pub fn checkpoint_json_stats() -> TableConfig {
    TableConfig::new()
        .write_stats_as_json(true)
        .write_stats_as_struct(false)
}

pub fn checkpoint_struct_stats() -> TableConfig {
    TableConfig::new()
        .write_stats_as_json(false)
        .write_stats_as_struct(true)
}

/// Disables both checkpoint stats encodings; per-commit add-file stats are still written.
pub fn no_checkpoint_stats() -> TableConfig {
    TableConfig::new()
        .write_stats_as_json(false)
        .write_stats_as_struct(false)
}

// === numIndexedCols rows (count-based data skipping) ===

/// `numIndexedCols = 0`: zero leaf columns get stats.
pub fn num_indexed_cols_zero() -> TableConfig {
    TableConfig::new().num_data_skipping_indexed_cols(0)
}

/// `numIndexedCols = 2`: cap stats to the first 2 leaf columns.
pub fn num_indexed_cols_narrow() -> TableConfig {
    TableConfig::new().num_data_skipping_indexed_cols(2)
}

/// `numIndexedCols = -1`: stats on every leaf column.
pub fn num_indexed_cols_all() -> TableConfig {
    TableConfig::new().num_data_skipping_indexed_cols(-1)
}

// === dataSkippingStatsColumns rows (named-cols data skipping) ===

/// `dataSkippingStatsColumns = []`: named-cols knob set, no columns listed.
pub fn stats_columns_empty() -> TableConfig {
    TableConfig::new().data_skipping_stats_columns(std::iter::empty::<&str>())
}

/// `dataSkippingStatsColumns` listing names that match at least one column in every
/// `DataLayoutConfig` schema (kernel silently skips entries that don't match), in reverse
/// schema order to exercise list-order preservation through write + read.
pub fn stats_columns_reordered() -> TableConfig {
    TableConfig::new().data_skipping_stats_columns(STATS_COLUMNS_BY_NAME.iter().rev().copied())
}

// === Composers: enable a checkpoint stats encoding on a data-skipping config ===
// Each enables only its own encoding, so composing both writes stats in both encodings.

pub fn with_json_stats(cfg: TableConfig) -> TableConfig {
    cfg.write_stats_as_json(true)
}

pub fn with_struct_stats(cfg: TableConfig) -> TableConfig {
    cfg.write_stats_as_struct(true)
}

// ===========================================================================
// rstest_reuse templates
// ===========================================================================

// Standard `#[values]` lists for cross-product testing. Apply with `#[apply(template_name)]`
// on an rstest test function. Each template injects one parameter; combine multiple templates
// to build a cross-product. Tests that only need a subset should use inline `#[values]` instead.
//
// Example:
// ```ignore
// use rstest::rstest;
// use rstest_reuse::apply;
// use test_utils::table_builder::*;
//
// #[apply(feature_sets)]
// #[apply(table_configs)]
// fn test_scan(feature_set: FeatureSet, table_config: TableConfig) { ... }
// ```

/// Empty + one per write-compatible feature + all combined. `type_widening` is
/// excluded because kernel errors when writing tables with that feature enabled.
#[rstest_reuse::template]
#[rstest::rstest]
pub fn feature_sets(
    #[values(
        FeatureSet::empty(),
        FeatureSet::new().column_mapping("name"),
        FeatureSet::new().ict(),
        FeatureSet::new().v2_checkpoint(),
        FeatureSet::new().deletion_vectors(),
        FeatureSet::new().append_only(),
        FeatureSet::new().change_data_feed(),
        FeatureSet::new().domain_metadata(),
        FeatureSet::new().vacuum_protocol_check(),
        FeatureSet::new().row_tracking(),
        FeatureSet::new()
            .column_mapping("name")
            .ict()
            .v2_checkpoint()
            .deletion_vectors()
            .append_only()
            .change_data_feed()
            .domain_metadata()
            .vacuum_protocol_check()
            .row_tracking()
    )]
    feature_set: FeatureSet,
) {
}

/// All common table configs: default plus four stats combos.
#[rstest_reuse::template]
#[rstest::rstest]
pub fn table_configs(
    #[values(
        TableConfig::new(),
        TableConfig::new().write_stats_as_json(true).write_stats_as_struct(false),
        TableConfig::new().write_stats_as_json(false).write_stats_as_struct(true),
        TableConfig::new().write_stats_as_json(true).write_stats_as_struct(true),
        TableConfig::new().write_stats_as_json(false).write_stats_as_struct(false)
    )]
    table_config: TableConfig,
) {
}

// ===========================================================================
// DataLayoutConfig
// ===========================================================================

/// Data layout configuration for cross-product testing.
///
/// Describes only the partitioning/clustering shape of the table. Write-time table
/// properties (stats format, etc.) live in [`TableConfig`], which is crossed with
/// this axis independently.
///
/// Designed for rstest `#[values]` parameterization alongside [`LogState`],
/// [`FeatureSet`], and [`TableConfig`].
#[derive(Clone, Debug, PartialEq)]
pub enum DataLayoutConfig {
    /// No partitioning or clustering. Uses an all-primitives schema including a
    /// nested-struct column.
    Unpartitioned,
    /// Partition by every valid primitive type. Uses [`partitioned_schema`] with all columns
    /// as partition columns.
    PartitionedAllTypes,
    /// Cluster by every stats-eligible primitive type. Uses [`clustered_schema`] with all
    /// clustering-eligible columns. Boolean and Binary are excluded (not stats-eligible).
    ClusteredAllTypes,
}

/// Period for sparse null injection. Data generation emits a null every
/// `NULL_RATE_EVERY_NTH` rows for each nullable, non-partition column, regardless of
/// [`DataLayoutConfig`]. Clustering columns are nulled like any other data column (kernel
/// permits null clustering keys). Partition columns are left fully populated: the protocol
/// permits null partition values, but the generator does not model them, so partition data
/// stays well-defined and matches the declared partition value.
pub const NULL_RATE_EVERY_NTH: usize = 3;

impl DataLayoutConfig {
    /// The layout column names (partition or clustering) for this config. Returns all
    /// schema columns except the `"value"` data column.
    pub fn columns(&self) -> Vec<String> {
        let schema = match self {
            DataLayoutConfig::Unpartitioned => return vec![],
            DataLayoutConfig::PartitionedAllTypes => partitioned_schema(),
            DataLayoutConfig::ClusteredAllTypes => clustered_schema(),
        };
        schema
            .fields()
            .filter(|f| f.name() != "value")
            .map(|f| f.name().to_string())
            .collect()
    }

    /// The schema for this config.
    pub fn schema(&self) -> SchemaRef {
        match self {
            DataLayoutConfig::Unpartitioned => default_schema(),
            DataLayoutConfig::PartitionedAllTypes => partitioned_schema(),
            DataLayoutConfig::ClusteredAllTypes => clustered_schema(),
        }
    }

    /// Whether this config uses partitioning.
    pub fn is_partitioned(&self) -> bool {
        self == &DataLayoutConfig::PartitionedAllTypes
    }

    /// Whether this config uses clustering.
    pub fn is_clustered(&self) -> bool {
        self == &DataLayoutConfig::ClusteredAllTypes
    }

    /// A nullable `INTEGER` data column for data-skipping predicate tests. Its values follow
    /// the generator's `version * 1000 + row` formula, so a `>= n * 1000` predicate prunes
    /// files from versions below `n`. (`int_col` in the default schema; `value` in
    /// [`partitioned_schema`] and [`clustered_schema`].)
    pub fn skipping_column(&self) -> &'static str {
        match self {
            DataLayoutConfig::Unpartitioned => "int_col",
            DataLayoutConfig::PartitionedAllTypes | DataLayoutConfig::ClusteredAllTypes => "value",
        }
    }
}

impl fmt::Display for DataLayoutConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataLayoutConfig::Unpartitioned => write!(f, "unpartitioned"),
            DataLayoutConfig::PartitionedAllTypes => write!(f, "partitioned(all_types)"),
            DataLayoutConfig::ClusteredAllTypes => write!(f, "clustered(all_types)"),
        }
    }
}

/// Schema with all partition-valid primitive types. Use with [`DataLayoutConfig`] to select
/// which columns are partition columns. Includes `TimestampNtz` which auto-enables the
/// `timestampNtz` table feature.
///
/// All columns are nullable. Follow-up: add non-nullable variants to test NOT NULL handling.
pub fn partitioned_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked(vec![
        // Partition-candidate columns (all valid partition types, matches write::partitioned)
        StructField::new("part_bool", DataType::BOOLEAN, true),
        StructField::new("part_byte", DataType::BYTE, true),
        StructField::new("part_short", DataType::SHORT, true),
        StructField::new("part_int", DataType::INTEGER, true),
        StructField::new("part_long", DataType::LONG, true),
        StructField::new("part_float", DataType::FLOAT, true),
        StructField::new("part_double", DataType::DOUBLE, true),
        StructField::new("part_string", DataType::STRING, true),
        StructField::new("part_binary", DataType::BINARY, true),
        StructField::new("part_date", DataType::DATE, true),
        StructField::new("part_ts", DataType::TIMESTAMP, true),
        StructField::new("part_ts_ntz", DataType::TIMESTAMP_NTZ, true),
        StructField::new("part_decimal", DataType::decimal(10, 2).unwrap(), true),
        // Non-partition data column (required: at least one non-partition column)
        StructField::new("value", DataType::INTEGER, true),
    ]))
}

/// Schema with all stats-eligible primitive types for clustering. Boolean and Binary are
/// excluded (not stats-eligible). Includes `TimestampNtz` which auto-enables the
/// `timestampNtz` table feature.
///
/// All columns are nullable. Follow-up: add non-nullable variants to test NOT NULL handling.
pub fn clustered_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked(vec![
        // Clustering-eligible columns (stats-eligible primitive types)
        StructField::new("clust_byte", DataType::BYTE, true),
        StructField::new("clust_short", DataType::SHORT, true),
        StructField::new("clust_int", DataType::INTEGER, true),
        StructField::new("clust_long", DataType::LONG, true),
        StructField::new("clust_float", DataType::FLOAT, true),
        StructField::new("clust_double", DataType::DOUBLE, true),
        StructField::new("clust_string", DataType::STRING, true),
        StructField::new("clust_date", DataType::DATE, true),
        StructField::new("clust_ts", DataType::TIMESTAMP, true),
        StructField::new("clust_ts_ntz", DataType::TIMESTAMP_NTZ, true),
        StructField::new("clust_decimal", DataType::decimal(10, 2).unwrap(), true),
        // Non-clustering data column
        StructField::new("value", DataType::INTEGER, true),
    ]))
}

// Canonical sweep rows for the DataLayoutConfig axis.

pub fn unpartitioned() -> DataLayoutConfig {
    DataLayoutConfig::Unpartitioned
}

pub fn partitioned() -> DataLayoutConfig {
    DataLayoutConfig::PartitionedAllTypes
}

pub fn clustered() -> DataLayoutConfig {
    DataLayoutConfig::ClusteredAllTypes
}

// ===========================================================================
// VersionTarget
// ===========================================================================

/// How the snapshot should be loaded from the built table.
///
/// Designed for use as an rstest `#[values]` parameter. Use the `build_snapshot!` macro
/// or `test_context!` macro to load a snapshot according to this target.
#[derive(Clone, Debug)]
pub enum VersionTarget {
    /// Load the latest version.
    Latest,
    /// Time travel to a specific version.
    AtVersion(u64),
    /// Load at `from`, then incrementally update to latest.
    IncrementalToLatest { from: u64 },
    /// Load at `from`, then incrementally update to a specific target `to`. Exercises the
    /// `Snapshot::builder_from(base).at_version(to)` path with `to <= latest`. Requires
    /// `from <= to`.
    IncrementalFrom { from: u64, to: u64 },
    /// Time travel by timestamp: resolves `timestamp` (milliseconds since Unix epoch) to a
    /// version via [`history_manager::latest_version_as_of`], then loads that version.
    ///
    /// [`history_manager::latest_version_as_of`]: delta_kernel::history_manager::latest_version_as_of
    AtTimestamp(i64),
}

impl fmt::Display for VersionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionTarget::Latest => write!(f, "latest"),
            VersionTarget::AtVersion(v) => write!(f, "at_version({v})"),
            VersionTarget::IncrementalToLatest { from } => {
                write!(f, "incremental({from}->latest)")
            }
            VersionTarget::IncrementalFrom { from, to } => {
                write!(f, "incremental({from}->{to})")
            }
            VersionTarget::AtTimestamp(ts) => write!(f, "at_timestamp({ts})"),
        }
    }
}

// Canonical sweep rows for the VersionTarget axis.

pub fn version_latest() -> VersionTarget {
    VersionTarget::Latest
}
pub fn version_at_mid() -> VersionTarget {
    VersionTarget::AtVersion(DEFAULT_SWEEP_MID_VERSION)
}
pub fn version_incremental_from_mid_to_latest() -> VersionTarget {
    VersionTarget::IncrementalToLatest {
        from: DEFAULT_SWEEP_MID_VERSION,
    }
}
/// Incremental update from `mid` to `latest - 1`. The non-latest `to` exercises the
/// partial-replay path that `version_incremental_from_mid_to_latest()` cannot reach, since
/// updating to latest is indistinguishable from a non-incremental load.
pub fn version_incremental_from_mid_to_pre_latest() -> VersionTarget {
    VersionTarget::IncrementalFrom {
        from: DEFAULT_SWEEP_MID_VERSION,
        to: DEFAULT_SWEEP_LATEST_VERSION - 1,
    }
}
/// Timestamp travel using `i64::MAX`, which always resolves to the latest version (every
/// commit's timestamp is below `i64::MAX`). This is a smoke test that the timestamp
/// conversion path runs without error across the sweep, not a resolution-correctness
/// check: `InMemory` collapses successive `put` timestamps to a single millisecond, so the
/// sweep can't reach an intermediate version. Resolution to an intermediate version is
/// covered by `test_at_timestamp_resolves_to_intermediate_version`, which sets commit
/// modification times explicitly on the local filesystem.
pub fn version_at_timestamp_max() -> VersionTarget {
    VersionTarget::AtTimestamp(i64::MAX)
}

// ===========================================================================
// TestTableBuilder
// ===========================================================================

/// Builds an in-memory Delta table with the requested configuration.
///
/// Uses kernel's full write path (CreateTable, Transaction) to produce correct tables
/// with proper protocol handling. The builder does NOT expose a snapshot -- the test
/// loads one using its own kernel types.
pub struct TestTableBuilder {
    log_state: LogState,
    features: FeatureSet,
    table_config: TableConfig,
    schema: SchemaRef,
    partition_columns: Vec<String>,
    clustering_columns: Vec<String>,
    num_data_files: usize,
    rows_per_file: usize,
}

impl Default for TestTableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestTableBuilder {
    /// Create a builder with sensible defaults: 1 commit, no features, 1 data file
    /// with 10 rows per commit.
    pub fn new() -> Self {
        Self {
            log_state: LogState::with_latest_version(0),
            features: FeatureSet::empty(),
            table_config: TableConfig::new(),
            schema: default_schema(),
            partition_columns: Vec::new(),
            clustering_columns: Vec::new(),
            num_data_files: 1,
            rows_per_file: 10,
        }
    }

    /// Set the log state (what files exist on disk).
    pub fn with_log_state(mut self, s: LogState) -> Self {
        self.log_state = s;
        self
    }

    /// Set the table features.
    pub fn with_features(mut self, f: FeatureSet) -> Self {
        self.features = f;
        self
    }

    /// Set table configuration properties (orthogonal to features).
    pub fn with_table_config(mut self, c: TableConfig) -> Self {
        self.table_config = c;
        self
    }

    /// Override the default schema.
    pub fn with_schema(mut self, s: SchemaRef) -> Self {
        self.schema = s;
        self
    }

    /// Override the number of parquet data files per commit and rows per file. Defaults
    /// are 1 file with 10 rows -- most tests don't need to call this.
    pub fn with_data(mut self, files_per_commit: usize, rows_per_file: usize) -> Self {
        self.num_data_files = files_per_commit;
        self.rows_per_file = rows_per_file;
        self
    }

    /// Set partition columns by logical name. For standard layouts, prefer
    /// [`with_data_layout`](Self::with_data_layout) which sets schema and columns together.
    /// Use this directly only when you need a custom schema with specific partition columns.
    ///
    /// Each data file gets deterministic partition values derived from version and file index.
    /// Clears any previously set clustering columns (partitioning and clustering are mutually
    /// exclusive).
    pub fn with_partition_columns(
        mut self,
        cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = cols.into_iter().map(Into::into).collect();
        self.clustering_columns.clear();
        self
    }

    /// Set clustering columns by logical name. For standard layouts, prefer
    /// [`with_data_layout`](Self::with_data_layout) which sets schema and columns together.
    /// Use this directly only when you need a custom schema with specific clustering columns.
    ///
    /// The columns must exist in the schema and have stats-eligible types. Clears any
    /// previously set partition columns (partitioning and clustering are mutually exclusive).
    pub fn with_clustering_columns(
        mut self,
        cols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.clustering_columns = cols.into_iter().map(Into::into).collect();
        self.partition_columns.clear();
        self
    }

    /// Apply a [`DataLayoutConfig`], setting the schema and layout columns. Pair with
    /// [`with_table_config`](Self::with_table_config) to also set write-time properties
    /// (e.g. stats format); the two axes are independent.
    ///
    /// For `Unpartitioned`, leaves the schema and columns unchanged.
    pub fn with_data_layout(self, config: DataLayoutConfig) -> Self {
        let cols = config.columns();
        if cols.is_empty() {
            return self;
        }
        let builder = self.with_schema(config.schema());
        if config.is_clustered() {
            builder.with_clustering_columns(cols)
        } else {
            builder.with_partition_columns(cols)
        }
    }

    /// Build the table and return a [`TestTable`] handle to the store.
    ///
    /// Safe to call from both sync tests and `#[tokio::test]` -- uses a dedicated
    /// runtime on a background thread to avoid panicking on nested runtimes.
    /// Propagates I/O, create-table, commit, checkpoint, and CRC errors from the
    /// underlying write path.
    ///
    /// # Panics
    /// Panics if [`LastCheckpointHintState::Stale`] is paired with fewer than two
    /// checkpoints.
    pub fn build(self) -> DeltaResult<TestTable> {
        validate_log_state(&self.log_state);
        block_on_sync(|| self.build_async())
    }

    async fn build_async(self) -> DeltaResult<TestTable> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let engine = Arc::new(
            DefaultEngineBuilder::new(store.clone())
                .with_task_executor(executor)
                .build(),
        );
        let schema = self.schema;

        // Resolve the checkpoint spec once. `V2WithSidecarsIfEnabled` falls back to
        // `None` (kernel default: V1 or V2-NoSidecar based on the feature flag) when
        // the table doesn't enable `v2Checkpoint`.
        let v2_supported = self
            .features
            .expected_features()
            .contains(&TableFeature::V2Checkpoint);
        let spec = match self.log_state.checkpoint_format() {
            CheckpointFormat::V2WithSidecarsIfEnabled {
                file_actions_per_sidecar_hint,
            } if v2_supported => Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: *file_actions_per_sidecar_hint,
            })),
            _ => None,
        };
        let checkpoints_at: HashSet<u64> =
            self.log_state.checkpoints_at().iter().copied().collect();

        // Version 0: CreateTable
        let mut builder = create_table(table_root, schema, "TestTableBuilder/1.0");
        let all_properties: Vec<_> = self
            .features
            .table_properties
            .iter()
            .chain(self.table_config.table_properties.iter())
            .collect();
        if !all_properties.is_empty() {
            builder = builder.with_table_properties(
                all_properties.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            );
        }
        if !self.partition_columns.is_empty() {
            builder = builder.with_data_layout(DataLayout::partitioned(
                self.partition_columns.iter().map(|s| s.as_str()),
            ));
        } else if !self.clustering_columns.is_empty() {
            builder = builder.with_data_layout(DataLayout::clustered(
                self.clustering_columns.iter().map(|s| s.as_str()),
            ));
        }
        let hint_state = self.log_state.last_checkpoint_hint();
        let hint_path = Path::from("_delta_log/_last_checkpoint");
        let resolved_hint_path = crate::resolve_table_path(table_root, &hint_path)?;
        // For Stale, capture the hint bytes kernel writes after the lowest checkpoint
        // and restore them at the end.
        let mut stale_hint_bytes: Option<Vec<u8>> = None;

        let mut snapshot = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();

        let crcs_at = self.log_state.crcs_at();
        if crcs_at.contains(&0) {
            write_crc(&snapshot, engine.as_ref())?;
        }
        if checkpoints_at.contains(&0) {
            snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;
            if hint_state == LastCheckpointHintState::Stale && stale_hint_bytes.is_none() {
                stale_hint_bytes = Some(read_hint_bytes(&store, &resolved_hint_path).await?);
            }
        }

        // Data commits (versions 1..=latest). Checkpoint inline using the post-commit snapshot.
        let latest = self.log_state.latest_version();
        for v in 1..=latest {
            snapshot = write_data_commit(
                snapshot.clone(),
                &engine,
                self.num_data_files,
                self.rows_per_file,
                &self.partition_columns,
                v,
            )
            .await?
            .unwrap_post_commit_snapshot();
            if crcs_at.contains(&v) {
                write_crc(&snapshot, engine.as_ref())?;
            }
            if checkpoints_at.contains(&v) {
                snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;
                if hint_state == LastCheckpointHintState::Stale && stale_hint_bytes.is_none() {
                    stale_hint_bytes = Some(read_hint_bytes(&store, &resolved_hint_path).await?);
                }
            }
        }

        match hint_state {
            LastCheckpointHintState::Present => {}
            LastCheckpointHintState::Missing => match store.delete(&resolved_hint_path).await {
                Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {}
                Err(e) => return Err(delta_kernel::Error::from(e)),
            },
            LastCheckpointHintState::Stale => {
                // Restore the hint bytes captured after the lowest checkpoint write.
                let bytes = stale_hint_bytes
                    .expect("validate_log_state should have caught Stale + 0 checkpoints");
                store
                    .put(&resolved_hint_path, bytes.into())
                    .await
                    .map_err(delta_kernel::Error::from)?;
            }
        }

        // Simulate log cleanup by deleting versioned log files at v < n. Unversioned
        // files like `_last_checkpoint` are skipped.
        if let Some(n) = self.log_state.cleanup_before() {
            let log_dir = Path::from("_delta_log");
            let resolved_log_dir = crate::resolve_table_path(table_root, &log_dir)?;
            let listing = store
                .list_with_delimiter(Some(&resolved_log_dir))
                .await
                .map_err(delta_kernel::Error::from)?;
            for object in listing.objects {
                if matches!(log_file_version(&object.location), Some(v) if v < n) {
                    store
                        .delete(&object.location)
                        .await
                        .map_err(delta_kernel::Error::from)?;
                }
            }
        }

        Ok(TestTable {
            store,
            table_root: table_root.to_string(),
            description: if self.table_config.table_properties.is_empty() {
                format!("{} + {}", self.log_state, self.features)
            } else {
                format!(
                    "{} + {} + config({})",
                    self.log_state, self.features, self.table_config
                )
            },
        })
    }
}

// ===========================================================================
// Data commit via kernel write path
// ===========================================================================

/// Write a CRC file via kernel's checksum writer. Builder invariant: the snapshot
/// comes from a post-commit handoff on a fresh in-memory table, so the CRC for
/// that version cannot already exist on disk.
fn write_crc(snapshot: &Arc<Snapshot>, engine: &dyn Engine) -> DeltaResult<()> {
    let (result, _) = snapshot.write_checksum(engine)?;
    assert_eq!(
        result,
        ChecksumWriteResult::Written,
        "fresh in-memory table should never have a CRC on disk at this version",
    );
    Ok(())
}

/// Write a data commit using kernel's transaction + write_parquet path.
/// Produces `num_files` parquet files with `rows_per_file` rows each. For partitioned
/// tables, all rows in a file share the same partition values; for unpartitioned or
/// clustered tables, uses `unpartitioned_write_context`. Non-partition columns get
/// varying data derived from version and file index. Partition columns are never nulled
/// so their data matches the declared partition value; all other nullable columns
/// (including clustering columns, which kernel permits to be null) get sparse nulls.
async fn write_data_commit<E: TaskExecutor>(
    snapshot: Arc<Snapshot>,
    engine: &DefaultEngine<E>,
    num_files: usize,
    rows_per_file: usize,
    partition_columns: &[String],
    version: u64,
) -> DeltaResult<delta_kernel::transaction::CommitResult> {
    let logical_schema = snapshot.schema().clone();
    let arrow_schema: ArrowSchema = TryFromKernel::try_from_kernel(logical_schema.as_ref())
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let partition_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();

    for file_idx in 0..num_files {
        let base = (version as i32 * 1000) + (file_idx as i32 * 100);
        let partition_seed = (version as usize) * 1000 + file_idx * 100;
        // Data batches must not contain any partition columns.
        let mut data_fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (arrow_field, kernel_field) in arrow_schema.fields().iter().zip(logical_schema.fields())
        {
            if partition_set.contains(kernel_field.name().as_str()) {
                continue;
            }
            let data_type = arrow_field.data_type();
            let values = generate_column(data_type, rows_per_file, base);
            let values = if arrow_field.is_nullable() {
                with_sparse_nulls(values, NULL_RATE_EVERY_NTH)
            } else {
                values
            };
            columns.push(values);
            data_fields.push(arrow_field.clone());
        }
        let data_arrow_schema = ArrowSchema::new(data_fields);
        let batch = RecordBatch::try_new(Arc::new(data_arrow_schema), columns)
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

        let write_context = if partition_columns.is_empty() {
            txn.unpartitioned_write_context()?
        } else {
            let partition_values = generate_partition_values(
                logical_schema.as_ref(),
                partition_columns,
                partition_seed,
            );
            txn.partitioned_write_context(partition_values)?
        };

        let add_files = engine
            .write_parquet(&ArrowEngineData::new(batch), &write_context)
            .await?;
        txn.add_files(add_files);
    }

    txn.commit(engine)
}

/// Generate a single column of data based on its Arrow type.
/// Data is deterministic: values are derived from `base` (version * 1000 + file_idx * 100).
fn generate_column(arrow_type: &ArrowDataType, rows: usize, base: i32) -> ArrayRef {
    match arrow_type {
        #[allow(unknown_lints, clippy::manual_is_multiple_of)]
        ArrowDataType::Boolean => {
            let values: Vec<bool> = (0..rows).map(|i| (base as usize + i) % 2 == 0).collect();
            Arc::new(BooleanArray::from(values))
        }
        ArrowDataType::Int8 => {
            let values: Vec<i8> = (0..rows).map(|i| ((base + i as i32) % 120) as i8).collect();
            Arc::new(Int8Array::from(values))
        }
        ArrowDataType::Int16 => {
            let values: Vec<i16> = (0..rows)
                .map(|i| ((base + i as i32) % 30000) as i16)
                .collect();
            Arc::new(Int16Array::from(values))
        }
        ArrowDataType::Int32 => {
            let values: Vec<i32> = (0..rows).map(|i| base + i as i32).collect();
            Arc::new(Int32Array::from(values))
        }
        ArrowDataType::Int64 => {
            let values: Vec<i64> = (0..rows).map(|i| (base + i as i32) as i64 * 1000).collect();
            Arc::new(Int64Array::from(values))
        }
        ArrowDataType::Float32 => {
            let values: Vec<f32> = (0..rows).map(|i| base as f32 + i as f32 * 0.5).collect();
            Arc::new(Float32Array::from(values))
        }
        ArrowDataType::Float64 => {
            let values: Vec<f64> = (0..rows).map(|i| base as f64 + i as f64 * 0.25).collect();
            Arc::new(Float64Array::from(values))
        }
        ArrowDataType::Utf8 => {
            let values: Vec<String> = (0..rows)
                .map(|i| format!("val_{}", base + i as i32))
                .collect();
            Arc::new(StringArray::from(values))
        }
        ArrowDataType::Binary => {
            let values: Vec<Vec<u8>> = (0..rows)
                .map(|i| format!("bin_{}", base + i as i32).into_bytes())
                .collect();
            Arc::new(BinaryArray::from(
                values.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
            ))
        }
        ArrowDataType::Date32 => {
            let values: Vec<i32> = (0..rows).map(|i| 18000 + base + i as i32).collect();
            Arc::new(Date32Array::from(values))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let values: Vec<i64> = (0..rows)
                .map(|i| (18000 + base + i as i32) as i64 * 86_400_000_000)
                .collect();
            let array = TimestampMicrosecondArray::from(values);
            match tz {
                Some(tz) => Arc::new(array.with_timezone(tz.as_ref())),
                None => Arc::new(array),
            }
        }
        ArrowDataType::Decimal128(precision, scale) => {
            let scale_factor = 10i128.pow(*scale as u32);
            let values: Vec<i128> = (0..rows)
                .map(|i| (base + i as i32) as i128 * scale_factor)
                .collect();
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .expect("valid decimal"),
            )
        }
        ArrowDataType::Struct(fields) => {
            let child_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|f| {
                    let child = generate_column(f.data_type(), rows, base);
                    if f.is_nullable() {
                        with_sparse_nulls(child, NULL_RATE_EVERY_NTH)
                    } else {
                        child
                    }
                })
                .collect();
            Arc::new(StructArray::new(fields.clone(), child_arrays, None))
        }
        other => panic!("unsupported Arrow type in test data generation: {other:?}"),
    }
}

/// Wrap `array` so that every `every_nth`-th row reads as null. Used by
/// [`write_data_commit`] to make the default sweep exercise null-aware code paths
/// (stats null counts, predicate evaluation under null, etc.) uniformly.
fn with_sparse_nulls(array: ArrayRef, every_nth: usize) -> ArrayRef {
    assert!(every_nth >= 1, "every_nth must be >= 1");
    let n = array.len();
    let valid: Vec<bool> = (0..n).map(|i| !i.is_multiple_of(every_nth)).collect();
    let null_buffer = NullBuffer::from(valid);
    let data = array
        .to_data()
        .into_builder()
        .nulls(Some(null_buffer))
        .build()
        .expect("rebuilding array with null buffer");
    delta_kernel::arrow::array::make_array(data)
}

// ===========================================================================
// TestTable
// ===========================================================================

/// A built test table backed by an in-memory object store.
///
/// Exposes only the store and table root URL. Does NOT expose kernel types like
/// `Snapshot` or `Engine` -- the test creates those from its own kernel imports.
/// This avoids type mismatches between the test crate's kernel and test_utils's kernel.
pub struct TestTable {
    store: Arc<DynObjectStore>,
    table_root: String,
    description: String,
}

impl TestTable {
    /// The object store containing all table files.
    pub fn store(&self) -> &Arc<DynObjectStore> {
        &self.store
    }

    /// The table root URL string (e.g. `"memory:///"`).
    pub fn table_root(&self) -> &str {
        &self.table_root
    }

    /// Human-readable description of this table's configuration (e.g.
    /// `"v=3+checkpoint_at(2) + columnMapping.mode=name, enableInCommitTimestamps=true"`).
    /// Useful in assert messages to identify which config failed.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Create a `DefaultEngine` backed by this table's store.
    ///
    /// Returns the engine from `test_utils`'s `delta_kernel`. For unit tests inside
    /// `kernel/src/`, use `DefaultEngineBuilder::new(table.store().clone()).build()`
    /// instead to get the correct crate-local engine type.
    pub fn engine(&self) -> DefaultEngine<TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(self.store.clone()).build()
    }
}

impl fmt::Display for TestTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

// ===========================================================================
// rstest fixtures
// ===========================================================================

/// Convenience wrapper: build a [`TestTable`] from a `log_state`, `feature_set`,
/// `data_layout`, and `table_config`. Used by the `test_context!` macro and available
/// for direct use in tests.
pub fn test_table(
    log_state: LogState,
    feature_set: FeatureSet,
    data_layout: DataLayoutConfig,
    table_config: TableConfig,
) -> TestTable {
    TestTableBuilder::new()
        .with_log_state(log_state)
        .with_features(feature_set)
        .with_data_layout(data_layout)
        .with_table_config(table_config)
        .build()
        .expect("failed to build test table")
}

// ===========================================================================
// Macros
// ===========================================================================

/// Load a snapshot from a [`TestTable`] according to a [`VersionTarget`].
///
/// Expands at the call site so `Snapshot` resolves to the caller's crate. This avoids
/// the type mismatch between `test_utils`'s kernel and `kernel/src/` unit tests' kernel.
/// Requires `Snapshot` to be in scope at the call site.
#[macro_export]
macro_rules! build_snapshot {
    ($version_target:expr, $table_root:expr, $engine:expr) => {
        match &$version_target {
            $crate::table_builder::VersionTarget::Latest => {
                Snapshot::builder_for($table_root).build($engine).unwrap()
            }
            $crate::table_builder::VersionTarget::AtVersion(v) => {
                Snapshot::builder_for($table_root)
                    .at_version(*v)
                    .build($engine)
                    .unwrap()
            }
            $crate::table_builder::VersionTarget::IncrementalToLatest { from } => {
                let base = Snapshot::builder_for($table_root)
                    .at_version(*from)
                    .build($engine)
                    .unwrap();
                Snapshot::builder_from(base).build($engine).unwrap()
            }
            $crate::table_builder::VersionTarget::IncrementalFrom { from, to } => {
                let base = Snapshot::builder_for($table_root)
                    .at_version(*from)
                    .build($engine)
                    .unwrap();
                Snapshot::builder_from(base)
                    .at_version(*to)
                    .build($engine)
                    .unwrap()
            }
            $crate::table_builder::VersionTarget::AtTimestamp(ts) => {
                let latest = Snapshot::builder_for($table_root).build($engine).unwrap();
                let commit = ::delta_kernel::history_manager::latest_version_as_of(
                    &latest,
                    $engine,
                    *ts,
                    ::delta_kernel::history_manager::HistoryCommitType::Recreatable,
                )
                .unwrap();
                Snapshot::builder_for($table_root)
                    .at_version(commit.version)
                    .build($engine)
                    .unwrap()
            }
        }
    };
}

/// Build a table, engine, and snapshot from rstest parameters in one call.
///
/// Expands at the call site so `Snapshot` and the engine type resolve to the caller's crate
/// types. Returns `(engine, snapshot, table)`.
///
/// The 5-argument form defaults to `DefaultEngine` and requires `DefaultEngineBuilder` to be in
/// scope at the call site. The 6-argument form takes an explicit engine factory closure
/// `Fn(Arc<DynObjectStore>) -> Engine`, useful when the caller cannot construct a
/// `DefaultEngine` (e.g. kernel-internal unit tests that depend only on `SyncEngine`).
///
/// ```ignore
/// let (engine, snap, table) = test_context!(
///     log_state, feature_set, data_layout, table_config, version_target,
/// );
/// let (engine, snap, table) = test_context!(
///     log_state, feature_set, data_layout, table_config, version_target,
///     |store| SyncEngine::new_with_store(store),
/// );
/// ```
#[macro_export]
macro_rules! test_context {
    ($log_state:expr, $feature_set:expr, $data_layout:expr, $table_config:expr, $version_target:expr $(,)?) => {
        $crate::test_context!(
            $log_state,
            $feature_set,
            $data_layout,
            $table_config,
            $version_target,
            |store| { DefaultEngineBuilder::new(store).build() }
        )
    };
    ($log_state:expr, $feature_set:expr, $data_layout:expr, $table_config:expr, $version_target:expr, $engine_factory:expr $(,)?) => {{
        let table = $crate::table_builder::test_table(
            $log_state,
            $feature_set,
            $data_layout,
            $table_config,
        );
        let engine = ($engine_factory)(table.store().clone());
        let snap = $crate::build_snapshot!($version_target, table.table_root(), &engine);
        (engine, snap, table)
    }};
}

// ===========================================================================
// Partition value generation
// ===========================================================================

/// Generate deterministic partition values for a given version and file index.
/// Follows the Delta protocol partition value serialization format.
fn generate_partition_values(
    schema: &StructType,
    partition_columns: &[String],
    seed: usize,
) -> HashMap<String, Scalar> {
    partition_columns
        .iter()
        .map(|col_name| {
            let field = schema
                .field(col_name)
                .unwrap_or_else(|| panic!("partition column '{col_name}' not in schema"));
            let value = scalar_for_type(field.data_type(), seed);
            (col_name.clone(), value)
        })
        .collect()
}

/// Generate a deterministic [`Scalar`] partition value for the given data type.
fn scalar_for_type(data_type: &DataType, seed: usize) -> Scalar {
    match data_type {
        DataType::Primitive(p) => match p {
            PrimitiveType::Boolean => Scalar::Boolean(seed.is_multiple_of(2)),
            PrimitiveType::Byte => Scalar::Byte((seed % 100) as i8),
            PrimitiveType::Short => Scalar::Short((seed % 100) as i16),
            PrimitiveType::Integer => Scalar::Integer((seed % 100) as i32),
            PrimitiveType::Long => Scalar::Long((seed * 1000) as i64),
            PrimitiveType::Float => Scalar::Float(seed as f32 * 0.5),
            PrimitiveType::Double => Scalar::Double(seed as f64 * 0.25),
            PrimitiveType::String => Scalar::String(format!("part_{seed}")),
            PrimitiveType::Binary => Scalar::Binary(format!("bin_{seed}").into_bytes()),
            PrimitiveType::Date => {
                // Days since epoch (1970-01-01)
                Scalar::Date(18000 + seed as i32)
            }
            PrimitiveType::Timestamp => {
                // Microseconds since epoch (UTC)
                Scalar::Timestamp((18000 + seed as i64) * 86_400_000_000)
            }
            PrimitiveType::TimestampNtz => {
                // Microseconds since epoch (no timezone)
                Scalar::TimestampNtz((18000 + seed as i64) * 86_400_000_000)
            }
            PrimitiveType::Decimal(dt) => {
                let scale_factor = 10i128.pow(dt.scale() as u32);
                let bits = seed as i128 * scale_factor;
                Scalar::decimal(bits, dt.precision(), dt.scale())
                    .expect("test seed produced invalid decimal")
            }
            PrimitiveType::Void => panic!("void type is not a valid partition column"),
            PrimitiveType::IntervalYearMonth | PrimitiveType::IntervalDayTime => {
                panic!("interval types are not supported as partition values")
            }
        },
        other => panic!("partition columns must be primitive types, got: {other:?}"),
    }
}

// ===========================================================================
// Helpers: schema
// ===========================================================================

/// Default schema with all Delta primitive types including TimestampNtz
/// and a nested column type.
pub(crate) fn default_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("bool_col", DataType::BOOLEAN, true),
        StructField::new("byte_col", DataType::BYTE, true),
        StructField::new("short_col", DataType::SHORT, true),
        StructField::new("int_col", DataType::INTEGER, true),
        StructField::new("long_col", DataType::LONG, true),
        StructField::new("float_col", DataType::FLOAT, true),
        StructField::new("double_col", DataType::DOUBLE, true),
        StructField::new("string_col", DataType::STRING, true),
        StructField::new("binary_col", DataType::BINARY, true),
        StructField::new("date_col", DataType::DATE, true),
        StructField::new("ts_col", DataType::TIMESTAMP, true),
        StructField::new("ts_ntz_col", DataType::TIMESTAMP_NTZ, true),
        StructField::new("decimal_col", DataType::decimal(10, 2).unwrap(), true),
        StructField::new(
            "nested_col",
            DataType::try_struct_type([
                StructField::nullable("a", DataType::LONG),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
            true,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStore;
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_basic_build() -> DeltaResult<()> {
        let table = TestTableBuilder::new().build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 0);
        Ok(())
    }

    #[test]
    fn test_commits_only() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(2))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 2);
        Ok(())
    }

    /// Demonstrates the `test_context!` macro with rstest `#[values]`.
    #[rstest]
    fn test_version_targets(
        #[values(LogState::with_latest_version(4))] log_state: LogState,
        #[values(FeatureSet::empty())] feature_set: FeatureSet,
        #[values(unpartitioned())] data_layout: DataLayoutConfig,
        #[values(checkpoint_json_stats())] table_config: TableConfig,
        #[values(
            VersionTarget::Latest,
            VersionTarget::AtVersion(2),
            VersionTarget::IncrementalToLatest { from: 1 },
            VersionTarget::IncrementalFrom { from: 1, to: 3 },
            VersionTarget::AtTimestamp(i64::MAX),
        )]
        version_target: VersionTarget,
    ) {
        let (_engine, snap, _table) = test_context!(
            log_state,
            feature_set,
            data_layout,
            table_config,
            version_target
        );
        let expected = match &version_target {
            VersionTarget::Latest | VersionTarget::IncrementalToLatest { .. } => 4,
            VersionTarget::AtTimestamp(ts) if *ts == i64::MAX => 4,
            VersionTarget::AtVersion(v) => *v,
            VersionTarget::IncrementalFrom { to, .. } => *to,
            VersionTarget::AtTimestamp(ts) => {
                panic!("test only uses AtTimestamp(i64::MAX), got {ts}")
            }
        };
        assert_eq!(snap.version(), expected);
    }

    #[rstest_reuse::apply(feature_sets)]
    fn test_feature_sets_enable_table_features(feature_set: FeatureSet) -> DeltaResult<()> {
        let expected_features = feature_set.expected_features();
        let table = TestTableBuilder::new().with_features(feature_set).build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let protocol = snap.table_configuration().protocol();
        let writer_features = protocol
            .writer_features()
            .expect("should have writer features");
        for feature in &expected_features {
            assert!(
                writer_features.contains(feature),
                "expected {feature:?} in writer_features: {writer_features:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_column_mapping_none_is_noop() {
        let fs = FeatureSet::new().column_mapping("none");
        assert!(fs.table_properties.is_empty());
    }

    #[test]
    fn test_table_config_properties_applied() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_table_config(
                TableConfig::new()
                    .write_stats_as_json(false)
                    .write_stats_as_struct(true),
            )
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 0);
        Ok(())
    }

    #[test]
    fn test_data_skipping_table_properties_in_metadata() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_table_config(
                TableConfig::new()
                    .num_data_skipping_indexed_cols(2)
                    .data_skipping_stats_columns(["int_col", "long_col"]),
            )
            .build()?;
        let config = read_metadata_configuration(table.store(), 0)?;
        assert_eq!(
            config
                .get("delta.dataSkippingNumIndexedCols")
                .map(|s| s.as_str()),
            Some("2")
        );
        assert_eq!(
            config
                .get("delta.dataSkippingStatsColumns")
                .map(|s| s.as_str()),
            Some("int_col,long_col")
        );
        Ok(())
    }

    fn read_metadata_configuration(
        store: &Arc<DynObjectStore>,
        version: u64,
    ) -> DeltaResult<std::collections::HashMap<String, String>> {
        let store = store.clone();
        block_on_sync(move || async move {
            crate::read_metadata_configuration_from_store(store.as_ref(), version)
                .await
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))
        })
    }

    /// Verifies every common table config builds successfully.
    #[rstest_reuse::apply(table_configs)]
    fn test_all_table_configs_build(table_config: TableConfig) -> DeltaResult<()> {
        TestTableBuilder::new()
            .with_table_config(table_config)
            .build()?;
        Ok(())
    }

    /// V2 checkpoints with sidecars require both the `v2Checkpoint` feature
    /// and `with_sidecars_if_enabled(...)` on the `LogState`. The checkpoint writer
    /// emits sidecar parquet files into `_delta_log/_sidecars/`.
    #[rstest::rstest]
    #[case::default_hint(None)]
    #[case::explicit_hint(Some(1))]
    fn test_v2_sidecars_emit_sidecar_files(#[case] hint: Option<usize>) -> DeltaResult<()> {
        let log_state = LogState::with_latest_version(2)
            .with_checkpoint_at([1])
            .with_sidecars_if_enabled(hint);
        let table = TestTableBuilder::new()
            .with_log_state(log_state)
            .with_features(FeatureSet::new().v2_checkpoint())
            .with_data(2, 5)
            .build()?;
        let sidecars = list_dir_filenames(table.store(), "_delta_log/_sidecars")?;
        assert!(
            sidecars.iter().any(|n| n.ends_with(".parquet")),
            "expected sidecar files in _delta_log/_sidecars/, got {sidecars:?}",
        );
        Ok(())
    }

    /// `with_sidecars_if_enabled` without the `v2Checkpoint` feature is a no-op:
    /// the table builds (no error) and no sidecar directory is produced.
    #[test]
    fn test_v2_sidecars_silently_ignored_without_v2_feature() -> DeltaResult<()> {
        let log_state = LogState::with_latest_version(2)
            .with_checkpoint_at([1])
            .with_sidecars_if_enabled(None);
        let table = TestTableBuilder::new()
            .with_log_state(log_state)
            .with_data(2, 5)
            .build()?;
        let sidecars = list_dir_filenames(table.store(), "_delta_log/_sidecars")?;
        assert!(
            sidecars.is_empty(),
            "expected no sidecar files without v2_checkpoint, got {sidecars:?}",
        );
        Ok(())
    }

    #[test]
    fn test_checkpoint_and_commits() -> DeltaResult<()> {
        let log_state = LogState::with_latest_version(4).with_checkpoint_at([2]);
        let table = TestTableBuilder::new()
            .with_log_state(log_state.clone())
            .build()?;
        assert_log_state_files_on_disk(&table, &log_state)?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 4);
        Ok(())
    }

    /// Cartesian-product check that every checkpoint configuration the builder
    /// permits actually lands on disk and rebuilds correctly. Covers single
    /// checkpoints at every legal version (None, 0, mid, latest) plus a
    /// multi-checkpoint case (mid + latest).
    #[rstest::rstest]
    #[case::no_checkpoint(LogState::with_latest_version(2))]
    #[case::checkpoint_at_v0(LogState::with_latest_version(2).with_checkpoint_at([0]))]
    #[case::checkpoint_at_v1(LogState::with_latest_version(2).with_checkpoint_at([1]))]
    #[case::checkpoint_at_latest(LogState::with_latest_version(2).with_checkpoint_at([2]))]
    #[case::two_checkpoints(
        LogState::with_latest_version(2).with_checkpoint_at([1, 2]),
    )]
    #[case::hint_missing_no_checkpoint(
        LogState::with_latest_version(2).with_last_checkpoint_hint(LastCheckpointHintState::Missing),
    )]
    #[case::hint_missing_with_checkpoint(
        LogState::with_latest_version(2)
            .with_checkpoint_at([1])
            .with_last_checkpoint_hint(LastCheckpointHintState::Missing),
    )]
    #[case::hint_stale_two_checkpoints(
        LogState::with_latest_version(2)
            .with_checkpoint_at([1, 2])
            .with_last_checkpoint_hint(LastCheckpointHintState::Stale),
    )]
    #[case::cleanup_with_hint_missing(
        LogState::with_latest_version(2)
            .with_checkpoint_at([1])
            .with_cleanup_commits_before(1)
            .with_last_checkpoint_hint(LastCheckpointHintState::Missing),
    )]
    #[case::cleanup_with_hint_stale(
        LogState::with_latest_version(2)
            .with_checkpoint_at([1, 2])
            .with_cleanup_commits_before(1)
            .with_last_checkpoint_hint(LastCheckpointHintState::Stale),
    )]
    #[case::stale_hint_points_at_deleted_checkpoint(
        LogState::with_latest_version(10)
            .with_checkpoint_at([5, 8])
            .with_cleanup_commits_before(8)
            .with_last_checkpoint_hint(LastCheckpointHintState::Stale),
    )]
    #[case::crc_at_v0(LogState::with_latest_version(2).with_crc_at([0]))]
    #[case::crc_at_v1(LogState::with_latest_version(2).with_crc_at([1]))]
    #[case::crc_at_latest(LogState::with_latest_version(2).with_crc_at([2]))]
    #[case::checkpoint_with_later_crc(
        LogState::with_latest_version(2).with_checkpoint_at([1]).with_crc_at([2]),
    )]
    #[case::crc_below_cleanup_is_deleted(
        LogState::with_latest_version(3)
            .with_checkpoint_at([2])
            .with_crc_at([1])
            .with_cleanup_commits_before(2),
    )]
    #[case::crc_at_cleanup_boundary_survives(
        LogState::with_latest_version(3)
            .with_checkpoint_at([2])
            .with_crc_at([2])
            .with_cleanup_commits_before(2),
    )]
    fn test_log_state_checkpoint_shapes_land_on_disk(
        #[case] log_state: LogState,
    ) -> DeltaResult<()> {
        let expected_version = log_state.latest_version();
        let table = TestTableBuilder::new()
            .with_log_state(log_state.clone())
            .build()?;
        assert_log_state_files_on_disk(&table, &log_state)?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(
            snap.version(),
            expected_version,
            "rebuild lost commits for {}",
            table.description(),
        );
        Ok(())
    }

    /// Stale hint must point at a real older checkpoint (not the latest); pairing
    /// `Stale` with fewer than two checkpoints is rejected at build time.
    #[rstest::rstest]
    #[case::no_checkpoint(LogState::with_latest_version(3))]
    #[case::single_checkpoint(LogState::with_latest_version(3).with_checkpoint_at([1]))]
    #[should_panic(expected = "Stale hint requires at least 2 checkpoints")]
    fn test_stale_hint_requires_two_checkpoints(#[case] base: LogState) {
        let log_state = base.with_last_checkpoint_hint(LastCheckpointHintState::Stale);
        let _ = TestTableBuilder::new().with_log_state(log_state).build();
    }

    /// Stale hint behavioral check: kernel must follow the stale (older) hint, list
    /// forward, and pick up the actual latest checkpoint at the higher version --
    /// not the stale-hinted older one.
    #[test]
    fn test_stale_hint_recovery_resolves_to_actual_latest_checkpoint() -> DeltaResult<()> {
        let log_state = LogState::with_latest_version(5)
            .with_checkpoint_at([2, 4])
            .with_last_checkpoint_hint(LastCheckpointHintState::Stale);
        let table = TestTableBuilder::new()
            .with_log_state(log_state.clone())
            .build()?;
        assert_log_state_files_on_disk(&table, &log_state)?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 5);
        Ok(())
    }

    #[test]
    fn test_scan_with_column_mapping() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_features(FeatureSet::new().column_mapping("name"))
            .with_data(1, 5)
            .build()?;
        let engine: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
        let scan = snap.scan_builder().build()?;
        let batches = crate::read_scan(&scan, engine)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
        Ok(())
    }

    #[test]
    fn test_scan_with_data() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data(2, 5)
            .build()?;
        let engine: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
        let scan = snap.scan_builder().build()?;
        let batches = crate::read_scan(&scan, engine)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
        Ok(())
    }

    /// Verifies the data generator injects sparse nulls into every nullable data column.
    /// Uses `rows_per_file = NULL_RATE_EVERY_NTH * 3` so each file has exactly three null
    /// rows per column.
    #[test]
    fn test_sparse_null_injection_in_generated_data() -> DeltaResult<()> {
        let rows_per_file = NULL_RATE_EVERY_NTH * 3;
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data(1, rows_per_file)
            .build()?;
        let engine: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
        let scan = snap.scan_builder().build()?;
        let batches = crate::read_scan(&scan, engine)?;
        assert!(!batches.is_empty(), "expected at least one batch");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, rows_per_file);
        let expected_nulls_per_col = rows_per_file / NULL_RATE_EVERY_NTH;
        for batch in &batches {
            for col_idx in 0..batch.num_columns() {
                let null_count = batch.column(col_idx).null_count();
                assert_eq!(
                    null_count,
                    expected_nulls_per_col,
                    "column '{}' should have {expected_nulls_per_col} nulls, got {null_count}",
                    batch.schema().field(col_idx).name(),
                );
            }
        }
        Ok(())
    }

    /// Partition columns are never nulled so their data matches the declared partition
    /// value. Clustering columns are ordinary data columns (kernel permits null clustering
    /// keys), so they receive the same sparse nulls as any other nullable column -- the
    /// clustered case guards against accidentally protecting them.
    #[rstest::rstest]
    #[case::partitioned(partitioned())]
    #[case::clustered(clustered())]
    fn test_layout_column_null_injection(#[case] config: DataLayoutConfig) -> DeltaResult<()> {
        let rows_per_file = NULL_RATE_EVERY_NTH * 3;
        // Only partition columns are protected from nulling; clustering columns are not.
        let protected_columns = if config.is_partitioned() {
            config.columns()
        } else {
            Vec::new()
        };
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data_layout(config)
            .with_data(1, rows_per_file)
            .build()?;
        let engine: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
        let scan = snap.scan_builder().build()?;
        let batches = crate::read_scan(&scan, engine)?;
        assert!(!batches.is_empty(), "expected at least one batch");
        let expected_nulls_per_col = rows_per_file / NULL_RATE_EVERY_NTH;
        for batch in &batches {
            for col_idx in 0..batch.num_columns() {
                let name = batch.schema().field(col_idx).name().to_string();
                let null_count = batch.column(col_idx).null_count();
                if protected_columns.contains(&name) {
                    assert_eq!(
                        null_count, 0,
                        "partition column '{name}' must never be nulled, got {null_count}",
                    );
                } else {
                    assert_eq!(
                        null_count, expected_nulls_per_col,
                        "data column '{name}' should have {expected_nulls_per_col} nulls, \
                         got {null_count}",
                    );
                }
            }
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::partitioned(partitioned(), partitioned_schema())]
    #[case::clustered(clustered(), clustered_schema())]
    fn test_data_layout_table(
        #[case] config: DataLayoutConfig,
        #[case] expected_schema: SchemaRef,
    ) -> DeltaResult<()> {
        // 2 versions means v0 (create_table) + v1 (1 data commit with 10 rows)
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data_layout(config)
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 1);
        assert_eq!(snap.schema(), expected_schema);
        let scan = snap.scan_builder().build()?;
        let engine_arc: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let batches = crate::read_scan(&scan, engine_arc)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
        Ok(())
    }

    #[test]
    fn test_clustered_table_multiple_versions() -> DeltaResult<()> {
        // v0=create, v1-v3=data commits, 10 rows each
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(3))
            .with_data_layout(clustered())
            .with_table_config(checkpoint_struct_stats())
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        let scan = snap.scan_builder().build()?;
        let engine_arc: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let batches = crate::read_scan(&scan, engine_arc)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 30);
        Ok(())
    }

    #[test]
    fn test_with_clustering_columns_directly() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_schema(clustered_schema())
            .with_clustering_columns(["clust_int", "clust_string"])
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 1);
        assert_eq!(snap.schema(), clustered_schema());
        Ok(())
    }

    #[test]
    fn test_nested_struct_schema_round_trip() -> DeltaResult<()> {
        let inner = DataType::try_struct_type([StructField::nullable("a", DataType::LONG)])?;
        let schema: SchemaRef = Arc::new(StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("inner", inner),
        ])?);
        let table = TestTableBuilder::new()
            .with_schema(schema.clone())
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.schema(), schema);
        Ok(())
    }

    #[test]
    fn test_layout_columns_are_mutually_exclusive() {
        let builder = TestTableBuilder::new()
            .with_partition_columns(["col_a"])
            .with_clustering_columns(["col_b"]);
        assert!(builder.partition_columns.is_empty());
        assert_eq!(builder.clustering_columns, vec!["col_b"]);

        let builder = TestTableBuilder::new()
            .with_clustering_columns(["col_a"])
            .with_partition_columns(["col_b"]);
        assert!(builder.clustering_columns.is_empty());
        assert_eq!(builder.partition_columns, vec!["col_b"]);
    }

    #[test]
    #[should_panic(expected = "must be <=")]
    fn test_with_checkpoint_at_rejects_above_latest_version() {
        LogState::with_latest_version(2).with_checkpoint_at([3]);
    }

    #[rstest::rstest]
    #[case::no_features(FeatureSet::empty(), false)]
    #[case::v2_checkpoint_feature(FeatureSet::new().v2_checkpoint(), false)]
    #[case::ict(FeatureSet::new().ict(), true)]
    fn test_builder_writes_crc_with_correct_content(
        #[case] features: FeatureSet,
        #[case] expect_ict: bool,
    ) -> DeltaResult<()> {
        let log_state = LogState::with_latest_version(2)
            .with_checkpoint_at([2])
            .with_crc_at([2]);
        let table = TestTableBuilder::new()
            .with_log_state(log_state.clone())
            .with_features(features)
            .build()?;
        assert_log_state_files_on_disk(&table, &log_state)?;

        let crc = read_crc_json(table.store(), 2)?;
        assert_eq!(
            crc["numFiles"].as_u64().unwrap(),
            2,
            "numFiles should match the 2 data adds emitted at v=1 and v=2",
        );
        assert!(
            crc["tableSizeBytes"].as_u64().unwrap() > 0,
            "tableSizeBytes should be positive for a table with 2 data files",
        );
        assert_eq!(
            crc["inCommitTimestampOpt"].is_i64(),
            expect_ict,
            "inCommitTimestampOpt presence must match the ICT feature flag",
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "must be <= latest_version")]
    fn test_with_crc_at_rejects_above_latest_version() {
        LogState::with_latest_version(2).with_crc_at([3]);
    }

    #[test]
    #[should_panic(expected = "already present")]
    fn test_with_crc_at_rejects_duplicate_version() {
        LogState::with_latest_version(2).with_crc_at([1, 1]);
    }

    #[test]
    fn test_cleanup_commits_before_deletes_old_files_and_rebuilds() -> DeltaResult<()> {
        // Checkpoints at v=1 (below cutoff), v=3 (at cutoff), v=5 (above cutoff)
        // exercise both the deletion and preservation paths for checkpoint files.
        let log_state = LogState::with_latest_version(5)
            .with_checkpoint_at([1, 3, 5])
            .with_cleanup_commits_before(3);
        let table = TestTableBuilder::new()
            .with_log_state(log_state.clone())
            .build()?;
        assert_log_state_files_on_disk(&table, &log_state)?;
        let entries = list_log_dir_filenames(table.store())?;
        for v in 0..3 {
            let json = format!("{v:020}.json");
            assert!(
                !entries.iter().any(|name| name == &json),
                "expected v={v} JSON to be cleaned up, found in {entries:?}",
            );
        }
        for v in 3..=5 {
            let json = format!("{v:020}.json");
            assert!(
                entries.iter().any(|name| name == &json),
                "expected v={v} JSON to survive cleanup, missing from {entries:?}",
            );
        }
        // Checkpoint files: v=1 deleted, v=3 and v=5 preserved.
        let v1_prefix = format!("{:020}.checkpoint", 1u64);
        assert!(
            !entries.iter().any(|name| name.starts_with(&v1_prefix)),
            "expected v=1 checkpoint to be cleaned up, found in {entries:?}",
        );
        for v in [3u64, 5] {
            let prefix = format!("{v:020}.checkpoint");
            assert!(
                entries.iter().any(|name| name.starts_with(&prefix)),
                "expected v={v} checkpoint to survive cleanup, missing from {entries:?}",
            );
        }
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 5);
        Ok(())
    }

    #[rstest::rstest]
    #[case::no_checkpoint(LogState::with_latest_version(5))]
    #[case::checkpoint_at_early_version(
        LogState::with_latest_version(5).with_checkpoint_at([2]),
    )]
    #[should_panic(expected = "requires a checkpoint at v=3")]
    fn test_cleanup_commits_before_requires_checkpoint_at_n(#[case] base: LogState) {
        let log_state = base.with_cleanup_commits_before(3);
        let _ = TestTableBuilder::new().with_log_state(log_state).build();
    }

    #[test]
    #[should_panic(expected = "exceeds latest_version")]
    fn test_cleanup_commits_before_rejects_above_latest_version() {
        let log_state = LogState::with_latest_version(2)
            .with_checkpoint_at([2])
            .with_cleanup_commits_before(3);
        let _ = TestTableBuilder::new().with_log_state(log_state).build();
    }

    #[rstest::rstest]
    #[case(LogState::with_latest_version(3).with_checkpoint_at([2]), 3)]
    #[case(LogState::with_latest_version(3).with_checkpoint_at([1]), 3)]
    #[case(LogState::with_latest_version(4), 4)]
    #[case(LogState::with_latest_version(2).with_checkpoint_at([1, 2]), 2)]
    fn test_log_state_latest_version(#[case] log_state: LogState, #[case] expected: u64) {
        assert_eq!(log_state.latest_version(), expected);
    }

    /// Asserts the on-disk log directory matches `log_state`: declared checkpoints
    /// survive iff their version is >= `cleanup_before`, no versioned log file
    /// remains below `cleanup_before`, and the `_last_checkpoint` hint reflects
    /// the declared hint state. Snapshot rebuilds can silently succeed via JSON
    /// replay even when these files are wrong.
    fn assert_log_state_files_on_disk(table: &TestTable, log_state: &LogState) -> DeltaResult<()> {
        let entries = list_log_dir_filenames(table.store())?;
        let cleanup = log_state.cleanup_before().unwrap_or(0);
        for &v in log_state.checkpoints_at() {
            let prefix = format!("{v:020}.checkpoint");
            let surviving = v >= cleanup;
            let found = entries
                .iter()
                .any(|name| name.starts_with(&prefix) && name.ends_with(".parquet"));
            assert_eq!(
                found,
                surviving,
                "checkpoint at v={v} should be {} for {log_state}: {entries:?}",
                if surviving { "present" } else { "cleaned up" },
            );
        }
        for entry in &entries {
            if let Some(v) = log_file_version(&Path::from(entry.as_str())) {
                assert!(
                    v >= cleanup,
                    "expected v={v} ({entry}) to be cleaned up (cleanup_before={cleanup}): {entries:?}",
                );
            }
        }

        for &v in log_state.crcs_at() {
            let crc_name = format!("{v:020}.crc");
            let surviving = v >= cleanup;
            let found = entries.iter().any(|name| name == &crc_name);
            assert_eq!(
                found,
                surviving,
                "CRC at v={v} should be {} for {log_state}: {entries:?}",
                if surviving { "present" } else { "cleaned up" },
            );
            if surviving {
                let crc = read_crc_json(table.store(), v)?;
                assert!(
                    crc["tableSizeBytes"].is_u64(),
                    "CRC at v={v} missing tableSizeBytes",
                );
                assert!(crc["numFiles"].is_u64(), "CRC at v={v} missing numFiles");
                assert_eq!(
                    crc["numMetadata"].as_u64(),
                    Some(1),
                    "CRC at v={v} must have numMetadata == 1 per spec",
                );
                assert_eq!(
                    crc["numProtocol"].as_u64(),
                    Some(1),
                    "CRC at v={v} must have numProtocol == 1 per spec",
                );
                assert!(crc["protocol"].is_object(), "CRC at v={v} missing protocol");
                assert!(crc["metadata"].is_object(), "CRC at v={v} missing metadata");
            }
        }

        let hint = read_last_checkpoint_hint(table.store())?;
        match log_state.last_checkpoint_hint() {
            LastCheckpointHintState::Present => {
                let want = log_state.checkpoints_at().last().copied();
                assert_eq!(
                    hint.map(|h| h.version),
                    want,
                    "Present hint should point at the highest checkpoint for {log_state}",
                );
            }
            LastCheckpointHintState::Missing => {
                assert!(
                    hint.is_none(),
                    "Missing hint should leave no _last_checkpoint file for {log_state}, got {hint:?}",
                );
            }
            LastCheckpointHintState::Stale => {
                let want = log_state.checkpoints_at().first().copied();
                assert_eq!(
                    hint.map(|h| h.version),
                    want,
                    "Stale hint should point at the lowest checkpoint for {log_state}",
                );
            }
        }
        Ok(())
    }

    #[derive(Debug)]
    struct HintFile {
        version: u64,
    }

    /// Read and JSON-parse a file from `store`. Returns `Ok(None)` if absent.
    async fn try_read_json(
        store: &DynObjectStore,
        path: &Path,
    ) -> DeltaResult<Option<serde_json::Value>> {
        let bytes = match store.get(path).await {
            Ok(r) => r.bytes().await.map_err(delta_kernel::Error::from)?,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(delta_kernel::Error::from(e)),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))
    }

    /// Read and parse the `_last_checkpoint` hint, if present.
    fn read_last_checkpoint_hint(store: &Arc<DynObjectStore>) -> DeltaResult<Option<HintFile>> {
        let store = store.clone();
        block_on_sync(move || async move {
            let path = Path::from("_delta_log/_last_checkpoint");
            match try_read_json(&store, &path).await? {
                Some(parsed) => {
                    let version = parsed["version"].as_u64().ok_or_else(|| {
                        delta_kernel::Error::generic("hint missing `version` field")
                    })?;
                    Ok(Some(HintFile { version }))
                }
                None => Ok(None),
            }
        })
    }

    /// Read and parse a CRC file at `version`. Errors if the file is absent.
    fn read_crc_json(store: &Arc<DynObjectStore>, version: u64) -> DeltaResult<serde_json::Value> {
        let store = store.clone();
        block_on_sync(move || async move {
            let path = Path::from(format!("_delta_log/{version:020}.crc"));
            try_read_json(&store, &path)
                .await?
                .ok_or_else(|| delta_kernel::Error::generic(format!("CRC at v={version} missing")))
        })
    }

    /// Helper: list filenames (basenames only) directly under `_delta_log/` in
    /// `store`. Returns just the leaf name so callers can match by suffix without
    /// dealing with `memory:///`-style path quirks.
    fn list_log_dir_filenames(store: &Arc<DynObjectStore>) -> DeltaResult<Vec<String>> {
        list_dir_filenames(store, "_delta_log")
    }

    /// Helper: list filenames at an arbitrary prefix.
    fn list_dir_filenames(store: &Arc<DynObjectStore>, prefix: &str) -> DeltaResult<Vec<String>> {
        let store = store.clone();
        let prefix = Path::from(prefix);
        block_on_sync(move || async move {
            let result = store
                .list_with_delimiter(Some(&prefix))
                .await
                .map_err(delta_kernel::Error::from)?;
            Ok(result
                .objects
                .into_iter()
                .filter_map(|m| m.location.filename().map(|s| s.to_string()))
                .collect())
        })
    }
}
