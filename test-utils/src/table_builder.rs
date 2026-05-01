// rstest_reuse templates generate macros that appear unused when building the lib target
// (they're only consumed by tests in this and other crates).
#![allow(unused_macros)]

//! A composable test table builder for delta-kernel-rs.
//!
//! The builder writes data by default (1 parquet file with 10 rows per commit). Override
//! with [`TestTableBuilder::with_data`] when a test needs specific file/row counts.
//!
//! Provides five orthogonal axes for parameterized testing:
//! - [`LogState`] -- what log files exist on disk (commits, checkpoints, CRC)
//! - [`FeatureSet`] -- which Delta table features are enabled
//! - [`TableConfig`] -- runtime knobs (e.g. stats format) orthogonal to features
//! - [`VersionTarget`] -- how the snapshot is loaded (latest, time travel, incremental)
//! - [`DataLayoutConfig`] -- data layout (unpartitioned, partitioned, clustered)
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
//!     #[values(LogState::commits(3))]
//!     log_state: LogState,
//!     #[values(FeatureSet::empty())]
//!     feature_set: FeatureSet,
//!     #[values(VersionTarget::Latest, VersionTarget::IncrementalToLatest { from: 0 })]
//!     version_target: VersionTarget,
//! ) {
//!     let (engine, snap, _table) =
//!         test_context!(log_state, feature_set, version_target);
//!     let scan = snap.scan_builder().build().unwrap();
//!     // ...
//! }
//! ```
//!
//! Requires `Snapshot` and `DefaultEngineBuilder` to be in scope at the call site.
//! The macros expand there, so types resolve to the caller's kernel crate -- avoiding
//! the type mismatch between `test_utils`'s kernel and `kernel/src/` unit tests.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, Error as ObjectStoreError, ObjectStoreExt as _};
use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef, StructField, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Snapshot};
use serde_json::json;

use crate::delta_path_for_version;

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

/// State of the `_delta_log/_last_checkpoint` hint file on disk.
///
/// Mirrors the three states a kernel reader must handle:
///
/// - [`Present`](Self::Present): hint file exists and points to the latest checkpoint that is
///   actually on disk. Default; what `Snapshot::checkpoint` writes.
/// - [`Missing`](Self::Missing): no hint file. Forces the reader's listing fallback to discover the
///   latest checkpoint.
/// - [`Stale`](Self::Stale): hint file exists but points to a version that has no checkpoint files.
///   Exercises the reader's recovery from a stale pointer (read hint, attempt load, fail, fall back
///   to listing).
///
/// `Stale` is only meaningful when there is a real checkpoint at version
/// `>= 1` for the stale pointer to be stale relative to: the fixture writes a
/// hint pointing to v=0, and kernel only recovers from hints pointing OLDER
/// than the actual checkpoint. The builder asserts on `Stale` paired with
/// `checkpoint_at = None` (no real checkpoint at all) or
/// `checkpoint_at = Some(0)` (no version older than 0 to point to).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastCheckpointHintState {
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

/// Describes the structure of a Delta table's log files on disk along four
/// orthogonal axes:
///
/// 1. `total_versions` -- how many JSON commit files exist (versions 0..total).
/// 2. `checkpoint_at` -- an optional version at which a checkpoint exists. Whether the checkpoint
///    is written in V1 or V2 format is determined by the `v2Checkpoint` feature on the paired
///    [`FeatureSet`], not by [`LogState`].
/// 3. `crc_at` -- an optional version at which a `.crc` file exists.
/// 4. `last_checkpoint_hint` -- the state of the `_delta_log/_last_checkpoint` hint file (present,
///    missing, or stale). See [`LastCheckpointHintState`].
///
/// # Out of scope (deferred)
///
/// Additional sub-axes that this struct does NOT yet model:
///
/// - `log_compaction_at` -- log compaction is currently disabled in kernel.
/// - `schema_history` -- schema evolution (add/drop/rename/alter) requires metadata-only commits
///   that the data-write builder path does not emit.
/// - `catalog_tail` -- catalog-managed staged commits / `log_tail` (deferred to the catalog-managed
///   PR).
///
/// Compose via chained builder methods. Each bounded `with_*_at` method validates
/// bounds at call time so invalid configurations panic in the test's own setup rather
/// than deep in the builder. Designed for ergonomic use with rstest `#[values]` /
/// `#[case]`:
///
/// ```ignore
/// #[rstest::rstest]
/// fn test(
///     #[values(
///         LogState::commits(3),
///         LogState::commits(3).with_checkpoint_at(2),
///         LogState::commits(3).with_crc_at(0).keep_last_checkpoint_hint(false),
///         LogState::commits(3).with_checkpoint_at(1).with_crc_at(2),
///     )]
///     log_state: LogState,
/// ) { /* ... */ }
/// ```
///
/// For orthogonal cross-products, pair with [`Self::maybe_with_checkpoint_at`] and
/// [`Self::maybe_with_crc_at`]:
///
/// ```ignore
/// #[rstest::rstest]
/// fn test(
///     #[values(None, Some(2))] checkpoint_at: Option<u64>,
///     #[values(None, Some(0))] crc_at: Option<u64>,
///     #[values(true, false)] keep_hint: bool,
/// ) {
///     let log_state = LogState::commits(3)
///         .maybe_with_checkpoint_at(checkpoint_at)
///         .maybe_with_crc_at(crc_at)
///         .keep_last_checkpoint_hint(keep_hint);
/// }
/// ```
#[derive(Clone, Debug)]
pub struct LogState {
    total_versions: u64,
    checkpoint_at: Option<u64>,
    crc_at: Option<u64>,
    last_checkpoint_hint: LastCheckpointHintState,
}

impl LogState {
    /// Commits-only table with `n` total versions (0..n).
    ///
    /// `n` must be >= 1. Version 0 is the create-table commit; versions 1..n contain
    /// data. By default: no checkpoint, no CRC, hint kept (a no-op when no checkpoint
    /// is written).
    pub fn commits(n: u64) -> Self {
        assert!(
            n >= 1,
            "commits() requires at least 1 version (the create-table commit)"
        );
        Self {
            total_versions: n,
            checkpoint_at: None,
            crc_at: None,
            last_checkpoint_hint: LastCheckpointHintState::Present,
        }
    }

    /// Write a checkpoint at version `v` (V1 or V2 format per the paired
    /// [`FeatureSet`]'s `v2Checkpoint`). `v` must be < `total_versions`.
    pub fn with_checkpoint_at(mut self, v: u64) -> Self {
        assert!(
            v < self.total_versions,
            "checkpoint_at ({v}) must be < total_versions ({})",
            self.total_versions,
        );
        self.checkpoint_at = Some(v);
        self
    }

    /// Optionally write a checkpoint at the given version. No-op when `None`. Useful
    /// for orthogonal rstest axes where the checkpoint axis is parameterized as
    /// `#[values(None, Some(v))]`.
    pub fn maybe_with_checkpoint_at(self, v: Option<u64>) -> Self {
        match v {
            Some(v) => self.with_checkpoint_at(v),
            None => self,
        }
    }

    /// Write a CRC file at version `v`. `v` must be < `total_versions`.
    pub fn with_crc_at(mut self, v: u64) -> Self {
        assert!(
            v < self.total_versions,
            "crc_at ({v}) must be < total_versions ({})",
            self.total_versions,
        );
        self.crc_at = Some(v);
        self
    }

    /// Optionally write a CRC file at the given version. No-op when `None`. Useful
    /// for orthogonal rstest axes where the CRC axis is parameterized as
    /// `#[values(None, Some(v))]`.
    pub fn maybe_with_crc_at(self, v: Option<u64>) -> Self {
        match v {
            Some(v) => self.with_crc_at(v),
            None => self,
        }
    }

    /// Set the state of the `_delta_log/_last_checkpoint` hint file directly.
    /// Default is [`LastCheckpointHintState::Present`].
    pub fn with_last_checkpoint_hint(mut self, state: LastCheckpointHintState) -> Self {
        self.last_checkpoint_hint = state;
        self
    }

    /// Sugar over [`Self::with_last_checkpoint_hint`] for the binary
    /// present/missing case. `true` -> [`LastCheckpointHintState::Present`],
    /// `false` -> [`LastCheckpointHintState::Missing`]. Use
    /// [`Self::with_last_checkpoint_hint`] directly to set
    /// [`LastCheckpointHintState::Stale`].
    pub fn keep_last_checkpoint_hint(self, keep: bool) -> Self {
        let state = if keep {
            LastCheckpointHintState::Present
        } else {
            LastCheckpointHintState::Missing
        };
        self.with_last_checkpoint_hint(state)
    }

    /// The canonical set of log shapes a snapshot reader path test should iterate
    /// over. Each entry exercises a different reader code path or boundary against
    /// a 5-version table (the create-table commit at v=0 plus 4 data commits at
    /// v=1..=4) so the mid-stream shapes have 2 data commits before and 2 after a
    /// checkpoint at v=2. Each shape exercises a distinct snapshot reader path:
    ///
    /// 1. commits-only: pure JSON replay
    /// 2. CRC at v=4 (latest): CRC found at target version, no checkpoint
    /// 3. CRC at v=0 (stale): CRC at an old version, trailing commits make it stale
    /// 4. checkpoint at v=4 (latest), hint present: checkpoint, no tail
    /// 5. checkpoint at v=4 (latest), hint missing: latest checkpoint via listing
    /// 6. checkpoint at v=2 (mid-stream), hint missing: mid checkpoint via listing
    /// 7. checkpoint at v=2 (mid-stream), hint stale: stale-pointer recovery
    /// 8. checkpoint at v=2 (mid-stream), hint present: standard mid + JSON tail
    ///
    /// Two reader paths are NOT covered: log compaction (kernel currently
    /// disabled) and schema-history (requires metadata-only commits not produced
    /// by the data-write builder path).
    ///
    /// Two shapes are intentionally NOT in `common()` -- checkpoint at v=0 and
    /// checkpoint+CRC overlap at v=2. Callers that want to exercise those
    /// should parameterize via [`Self::maybe_with_checkpoint_at`] /
    /// [`Self::maybe_with_crc_at`] or call [`TestTableBuilder::all_tables`]
    /// for an exhaustive cross-product.
    pub fn common() -> Vec<Self> {
        const N: u64 = 5;
        vec![
            Self::commits(N),
            Self::commits(N).with_crc_at(N - 1),
            Self::commits(N).with_crc_at(0),
            Self::commits(N).with_checkpoint_at(N - 1),
            Self::commits(N)
                .with_checkpoint_at(N - 1)
                .with_last_checkpoint_hint(LastCheckpointHintState::Missing),
            Self::commits(N)
                .with_checkpoint_at(2)
                .with_last_checkpoint_hint(LastCheckpointHintState::Missing),
            Self::commits(N)
                .with_checkpoint_at(2)
                .with_last_checkpoint_hint(LastCheckpointHintState::Stale),
            Self::commits(N).with_checkpoint_at(2),
        ]
    }

    /// Number of commit files on disk. A return value of `N` means versions 0
    /// through `N - 1` exist.
    pub(crate) fn num_versions(&self) -> u64 {
        self.total_versions
    }

    /// Version at which a checkpoint is written, if any.
    pub(crate) fn checkpoint_at(&self) -> Option<u64> {
        self.checkpoint_at
    }

    /// Version at which a CRC file is written, if any.
    pub(crate) fn crc_at(&self) -> Option<u64> {
        self.crc_at
    }

    /// State of the `_last_checkpoint` hint file on the built table.
    pub(crate) fn last_checkpoint_hint(&self) -> LastCheckpointHintState {
        self.last_checkpoint_hint
    }
}

impl fmt::Display for LogState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "commits({})", self.total_versions)?;
        if let Some(v) = self.checkpoint_at {
            write!(f, "+checkpoint_at({v})")?;
        }
        if let Some(v) = self.crc_at {
            write!(f, "+crc_at({v})")?;
        }
        if self.last_checkpoint_hint != LastCheckpointHintState::Present {
            write!(f, "+hint({})", self.last_checkpoint_hint)?;
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

    /// Common feature sets for cross-product testing: empty, one per write-compatible
    /// feature, and one with all write-compatible features combined. Not the full power
    /// set -- add specific combos as needed.
    ///
    /// `type_widening` is intentionally excluded because kernel errors when writing
    /// tables with that feature enabled (see `TableFeature::TypeWidening`).
    pub fn common() -> Vec<Self> {
        vec![
            Self::empty(),
            Self::new().column_mapping("name"),
            Self::new().ict(),
            Self::new().v2_checkpoint(),
            Self::new().deletion_vectors(),
            Self::new().append_only(),
            Self::new().change_data_feed(),
            Self::new().domain_metadata(),
            Self::new().vacuum_protocol_check(),
            Self::new().row_tracking(),
            Self::new()
                .column_mapping("name")
                .ict()
                .v2_checkpoint()
                .deletion_vectors()
                .append_only()
                .change_data_feed()
                .domain_metadata()
                .vacuum_protocol_check()
                .row_tracking(),
        ]
    }

    /// Returns a new `FeatureSet` containing this set's properties plus those from `other`.
    /// On key conflict, `other` wins (the rightmost value for each key is kept). Property
    /// order is preserved: entries from `self` come first (with any key overrides applied
    /// in place), followed by `other`'s novel keys in their original order.
    pub fn merge(&self, other: &FeatureSet) -> Self {
        let mut merged = self.clone();
        for (k, v) in &other.table_properties {
            if let Some(existing) = merged.table_properties.iter_mut().find(|(ek, _)| ek == k) {
                existing.1 = v.clone();
            } else {
                merged.table_properties.push((k.clone(), v.clone()));
            }
        }
        merged
    }

    /// Whether v2_checkpoint is enabled.
    pub fn has_v2_checkpoint(&self) -> bool {
        self.table_properties
            .iter()
            .any(|(k, v)| k == "delta.feature.v2Checkpoint" && v == "supported")
    }

    /// Whether in-commit timestamps are enabled.
    pub fn has_ict(&self) -> bool {
        self.table_properties
            .iter()
            .any(|(k, v)| k == "delta.enableInCommitTimestamps" && v == "true")
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

// ===========================================================================
// TableConfig
// ===========================================================================

/// Table configuration properties that are orthogonal to table features.
///
/// These are pure runtime knobs (e.g. stats format, checkpoint interval) that don't
/// affect the protocol or enable features. Use as a separate cross-product axis from
/// [`FeatureSet`] since the two are independent.
#[derive(Clone, Debug, Default)]
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

    /// Common table configs for cross-product testing: default plus all four stats
    /// property combos.
    pub fn common() -> Vec<Self> {
        vec![
            Self::new(),
            Self::new()
                .write_stats_as_json(true)
                .write_stats_as_struct(false),
            Self::new()
                .write_stats_as_json(false)
                .write_stats_as_struct(true),
            Self::new()
                .write_stats_as_json(true)
                .write_stats_as_struct(true),
            Self::new()
                .write_stats_as_json(false)
                .write_stats_as_struct(false),
        ]
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

/// All common feature sets: empty, one per write-compatible feature, and all combined.
///
/// `type_widening` is intentionally excluded because kernel errors when writing tables
/// with that feature enabled (see [`FeatureSet::common`]).
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
/// Designed for rstest `#[values]` parameterization alongside [`LogState`] and
/// [`FeatureSet`].
#[derive(Clone, Debug, PartialEq)]
pub enum DataLayoutConfig {
    /// No special data layout (default schema).
    Unpartitioned,
    /// Partition by every valid primitive type. Uses [`partitioned_schema`] with all columns
    /// as partition columns.
    PartitionedAllTypes,
    /// Cluster by every stats-eligible primitive type. Uses [`clustered_schema`] with all
    /// clustering-eligible columns. Boolean and Binary are excluded (not stats-eligible).
    ClusteredAllTypes,
}

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
        *self == DataLayoutConfig::PartitionedAllTypes
    }

    /// Whether this config uses clustering.
    pub fn is_clustered(&self) -> bool {
        *self == DataLayoutConfig::ClusteredAllTypes
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
        // Partition-candidate columns (all valid partition types, matches write_partitioned.rs)
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
}

impl fmt::Display for VersionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionTarget::Latest => write!(f, "latest"),
            VersionTarget::AtVersion(v) => write!(f, "at_version({v})"),
            VersionTarget::IncrementalToLatest { from } => {
                write!(f, "incremental({from}->latest)")
            }
        }
    }
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
            log_state: LogState::commits(1),
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

    /// Apply a [`DataLayoutConfig`], setting the schema and layout columns accordingly.
    /// This is the recommended way to configure partitioning or clustering -- it sets both
    /// the schema and columns in one call. Use
    /// [`with_partition_columns`](Self::with_partition_columns) or
    /// [`with_clustering_columns`](Self::with_clustering_columns) directly only when you
    /// need a custom schema with specific columns.
    ///
    /// For [`DataLayoutConfig::Unpartitioned`], leaves the schema and columns unchanged.
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

    /// Build every valid ([`LogState`], [`FeatureSet`]) combination with the given
    /// required features forced on every table.
    ///
    /// Cross-products [`LogState::common`] with [`FeatureSet::common`], merging
    /// `required` into each base feature set. Pass [`FeatureSet::empty`] for no
    /// required features. All combinations are valid: whether the resulting
    /// checkpoint is V1 or V2 is determined by the feature set alone.
    pub fn all_tables(required: FeatureSet) -> DeltaResult<Vec<TestTable>> {
        let mut tables = Vec::new();
        for log_state in LogState::common() {
            for base_features in FeatureSet::common() {
                let features = base_features.merge(&required);
                tables.push(
                    Self::new()
                        .with_log_state(log_state.clone())
                        .with_features(features)
                        .build()?,
                );
            }
        }
        Ok(tables)
    }

    /// Build the table and return a [`TestTable`] handle to the store.
    ///
    /// Safe to call from both sync tests and `#[tokio::test]` -- uses a dedicated
    /// runtime on a background thread to avoid panicking on nested runtimes.
    /// Propagates I/O, create-table, commit, checkpoint, and CRC errors from the
    /// underlying write path.
    ///
    /// # Panics
    /// Panics if the [`LogState`] requests a `Stale` hint without a real
    /// checkpoint at version >= 1 to be stale relative to (kernel only recovers
    /// from hints pointing OLDER than the actual checkpoint). Validated up-front
    /// so the panic message surfaces directly rather than through the build
    /// runtime's worker-thread join boundary.
    pub fn build(self) -> DeltaResult<TestTable> {
        validate_stale_hint(&self.log_state);
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
        let committed = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_committed();
        let mut snapshot = committed.post_commit_snapshot().unwrap().clone();

        // Each post-commit snapshot has an in-memory CRC populated; we need that to
        // call `write_checksum`. Capture the snapshot at `crc_at` if it falls on the
        // create-table commit (v=0) or any data commit (1..total) below.
        let crc_at = self.log_state.crc_at();
        let mut crc_snapshot = (crc_at == Some(0)).then(|| snapshot.clone());

        // Data commits (versions 1..N)
        let total = self.log_state.num_versions();
        for v in 1..total {
            let result = write_data_commit(
                snapshot.clone(),
                &engine,
                self.num_data_files,
                self.rows_per_file,
                &self.partition_columns,
                v,
            )
            .await?;
            snapshot = result
                .unwrap_committed()
                .post_commit_snapshot()
                .unwrap()
                .clone();
            if crc_at == Some(v) {
                crc_snapshot = Some(snapshot.clone());
            }
        }

        if let Some(checkpoint_at) = self.log_state.checkpoint_at() {
            let snap = Snapshot::builder_for(table_root)
                .at_version(checkpoint_at)
                .build(engine.as_ref())?;
            snap.checkpoint(engine.as_ref())?;
        }

        // Write the CRC via kernel's `write_checksum`, which produces a spec-complete
        // CRC (with `inCommitTimestampOpt`, `domainMetadata`, `setTransactions`,
        // `fileSizeHistogram`). Requires the snapshot to be a post-commit snapshot
        // (its in-memory `lazy_crc` is populated by `compute_post_commit_crc`).
        if let Some(snap) = crc_snapshot {
            snap.write_checksum(engine.as_ref())?;
        }

        // Override the `_last_checkpoint` hint if the caller asked for a non-default
        // state. `Snapshot::checkpoint` always writes a fresh hint file pointing at
        // the latest checkpoint, so producing Missing or Stale requires post-fact
        // mutation of the hint.
        match self.log_state.last_checkpoint_hint() {
            LastCheckpointHintState::Present => {}
            LastCheckpointHintState::Missing => {
                let hint_path = Path::from("_delta_log/_last_checkpoint");
                let resolved = crate::resolve_table_path(table_root, &hint_path)?;
                match store.delete(&resolved).await {
                    Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {}
                    Err(e) => return Err(delta_kernel::Error::from(e)),
                }
            }
            LastCheckpointHintState::Stale => {
                // Stale: overwrite the hint with a JSON pointing to a version that
                // has no checkpoint files. Kernel will read the hint, attempt to
                // load that version's checkpoint, miss, then fall back to listing.
                // Picking a version distinct from the actual checkpoint is required
                // to make the pointer genuinely stale.
                let stale_version = stale_hint_version(self.log_state.checkpoint_at());
                let hint_path = Path::from("_delta_log/_last_checkpoint");
                let resolved = crate::resolve_table_path(table_root, &hint_path)?;
                let body = format!(r#"{{"version":{stale_version},"size":1}}"#);
                store
                    .put(&resolved, body.into_bytes().into())
                    .await
                    .map_err(delta_kernel::Error::from)?;
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

/// Validate up-front in the synchronous `build()` entry that a `Stale` hint is
/// paired with a checkpoint at version `>= 1`. Calling this before
/// [`block_on_sync`] ensures the panic message reaches `#[should_panic]`
/// matching directly rather than through the worker-thread join boundary,
/// which propagates the panic but loses the original message in the outer
/// `expect` wrapper.
fn validate_stale_hint(log_state: &LogState) {
    if log_state.last_checkpoint_hint() == LastCheckpointHintState::Stale {
        let _ = stale_hint_version(log_state.checkpoint_at());
    }
}

/// Pick a version OLDER than `checkpoint_at` for a stale `_last_checkpoint`
/// hint to point to. Kernel handles stale-older-than-actual hints by listing
/// from the hinted version and using the actual checkpoint discovered ahead of
/// it (logging an info about the version mismatch); a stale hint pointing
/// AHEAD of the actual checkpoint would error out as "didn't find any
/// checkpoints" since the listing skips over the real checkpoint.
///
/// # Panics
/// Panics on `checkpoint_at = None` (no real checkpoint to be stale relative
/// to) or `checkpoint_at = Some(0)` (no version older than 0).
fn stale_hint_version(checkpoint_at: Option<u64>) -> u64 {
    let actual = checkpoint_at.expect(
        "Stale hint requires a checkpoint to be stale relative to; pair with with_checkpoint_at",
    );
    assert!(
        actual >= 1,
        "Stale hint requires checkpoint_at >= 1 so the hint can point to v=0 (older than the \
         actual checkpoint); kernel does not recover from hints pointing AHEAD of the actual \
         checkpoint",
    );
    0
}

// ===========================================================================
// Data commit via kernel write path
// ===========================================================================

/// Write a data commit using kernel's transaction + write_parquet path.
/// Produces `num_files` parquet files with `rows_per_file` rows each. For partitioned
/// tables, all rows in a file share the same partition values; for unpartitioned or
/// clustered tables, uses `unpartitioned_write_context`. Non-partition columns get
/// varying data derived from version and file index.
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

    for file_idx in 0..num_files {
        let base = (version as i32 * 1000) + (file_idx as i32 * 100);
        let partition_seed = (version as usize) * 1000 + file_idx * 100;
        let columns: Vec<ArrayRef> = arrow_schema
            .fields()
            .iter()
            .zip(logical_schema.fields())
            .map(|(arrow_field, kernel_field)| {
                if partition_columns.contains(&kernel_field.name().to_string()) {
                    generate_constant_column(arrow_field.data_type(), rows_per_file, partition_seed)
                } else {
                    generate_column(arrow_field.data_type(), rows_per_file, base)
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)
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

/// Generate a constant column where all rows have the same value derived from `seed`.
/// Used for partition columns so the data matches the declared partition values.
fn generate_constant_column(arrow_type: &ArrowDataType, rows: usize, seed: usize) -> ArrayRef {
    match arrow_type {
        ArrowDataType::Boolean => {
            let v = seed.is_multiple_of(2);
            Arc::new(BooleanArray::from(vec![v; rows]))
        }
        ArrowDataType::Int8 => {
            let v = (seed % 100) as i8;
            Arc::new(Int8Array::from(vec![v; rows]))
        }
        ArrowDataType::Int16 => {
            let v = (seed % 100) as i16;
            Arc::new(Int16Array::from(vec![v; rows]))
        }
        ArrowDataType::Int32 => {
            let v = (seed % 100) as i32;
            Arc::new(Int32Array::from(vec![v; rows]))
        }
        ArrowDataType::Int64 => {
            let v = (seed * 1000) as i64;
            Arc::new(Int64Array::from(vec![v; rows]))
        }
        ArrowDataType::Float32 => {
            let v = seed as f32 * 0.5;
            Arc::new(Float32Array::from(vec![v; rows]))
        }
        ArrowDataType::Float64 => {
            let v = seed as f64 * 0.25;
            Arc::new(Float64Array::from(vec![v; rows]))
        }
        ArrowDataType::Utf8 => {
            let v = format!("part_{seed}");
            Arc::new(StringArray::from(vec![v.as_str(); rows]))
        }
        ArrowDataType::Binary => {
            let v = format!("bin_{seed}").into_bytes();
            Arc::new(BinaryArray::from(vec![v.as_slice(); rows]))
        }
        ArrowDataType::Date32 => {
            let v = 18000 + seed as i32;
            Arc::new(Date32Array::from(vec![v; rows]))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let v = (18000 + seed as i64) * 86_400_000_000;
            let array = TimestampMicrosecondArray::from(vec![v; rows]);
            match tz {
                Some(tz) => Arc::new(array.with_timezone(tz.as_ref())),
                None => Arc::new(array),
            }
        }
        ArrowDataType::Decimal128(precision, scale) => {
            let scale_factor = 10i128.pow(*scale as u32);
            let v = seed as i128 * scale_factor;
            Arc::new(
                Decimal128Array::from(vec![v; rows])
                    .with_precision_and_scale(*precision, *scale)
                    .expect("valid decimal"),
            )
        }
        other => panic!("unsupported Arrow type for partition column: {other:?}"),
    }
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
        other => panic!("unsupported Arrow type in test data generation: {other:?}"),
    }
}

// ===========================================================================
// CRC file generation
// ===========================================================================

/// Extract the spec's deletion-vector `uniqueId` from an Add/Remove action's
/// `deletionVector` sub-object. Returns `None` when no DV is attached (the common
/// case for tables without the `deletionVectors` feature).
///
/// Per spec, `uniqueId` is `storageType || pathOrInlineDv` plus an optional
/// `@offset` suffix when `offset` is present. Note: an `offset` field present with
/// value `0` yields an `@0` suffix; absence of the field is what suppresses the
/// suffix. Adding a `!= 0` guard would conflate the two and break the
/// `(path, uniqueId)` reconciliation key.
fn dv_unique_id(action: &serde_json::Value) -> Option<String> {
    let dv = action.get("deletionVector")?;
    let storage_type = dv.get("storageType")?.as_str()?;
    let path_or_inline = dv.get("pathOrInlineDv")?.as_str()?;
    let mut id = format!("{storage_type}{path_or_inline}");
    if let Some(offset) = dv.get("offset").and_then(|o| o.as_i64()) {
        id.push_str(&format!("@{offset}"));
    }
    Some(id)
}

/// Write a CRC file at `version` for a table rooted at `table_root` by scanning commit
/// JSONs and reconciling Add/Remove actions on `(path, deletionVector.uniqueId)`.
///
/// **Prefer the builder path** ([`LogState::with_crc_at`]) for any table built through
/// [`TestTableBuilder`] -- the builder calls kernel's `Snapshot::write_checksum`, which
/// produces a spec-complete CRC (with `inCommitTimestampOpt`, `domainMetadata`,
/// `setTransactions`, `fileSizeHistogram`, etc.).
///
/// This standalone function exists for tests that hand-craft commits outside kernel's
/// write path (e.g. the deletion-vector reconciliation tests, which inject custom
/// `add`/`remove` actions to verify the CRC's reconciliation rule). For those tests,
/// kernel's writer cannot help: `write_checksum` requires a post-commit snapshot whose
/// `lazy_crc` is populated by the kernel-managed write path, and that path doesn't
/// exist for hand-crafted commits.
///
/// # Fixture-only caveats
/// Callers must not rely on the CRC produced by this function being byte-equivalent
/// to what kernel's write path would emit. Known limitations:
///
/// - Only scans commit JSON files -- checkpoint parquet files are not read, so the latest
///   `protocol`/`metaData` must appear in some commit `<= version`.
/// - The following optional spec fields are intentionally omitted:
///   - `inCommitTimestampOpt` (spec requires this when the table has ICT enabled)
///   - `domainMetadata`
///   - `setTransactions`
///   - `allFiles`
///   - `fileSizeHistogram`
/// - O(N) in commit count -- not suitable for fuzz-style tests with hundreds of versions.
///
/// # Errors
/// Returns an error if any commit file `0..=version` cannot be fetched, UTF-8
/// decoded, or JSON parsed; if writing the CRC file to the store fails; if the
/// scanned commits contain no `protocol` or `metaData` action (which would violate
/// protocol invariants for a well-formed table); or if an `add` action is missing a
/// required `path` or `size` field.
pub async fn write_crc(
    version: u64,
    store: &Arc<DynObjectStore>,
    table_root: &str,
) -> DeltaResult<()> {
    let mut protocol_json: Option<serde_json::Value> = None;
    let mut metadata_json: Option<serde_json::Value> = None;
    // Key is (path, deletionVector.uniqueId) per the spec's logical-file primary key.
    // `uniqueId` is `None` when no DV is attached.
    let mut live_adds: HashMap<(String, Option<String>), i64> = HashMap::new();

    for v in 0..=version {
        let commit_path = delta_path_for_version(v, "json");
        let resolved = crate::resolve_table_path(table_root, &commit_path)?;
        let bytes = store.get(&resolved).await?.bytes().await?;
        let text = std::str::from_utf8(bytes.as_ref())?;
        for line in text.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            if let Some(p) = action.get("protocol") {
                protocol_json = Some(p.clone());
            }
            if let Some(m) = action.get("metaData") {
                metadata_json = Some(m.clone());
            }
            if let Some(add) = action.get("add") {
                let path = add
                    .get("path")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| {
                        delta_kernel::Error::generic(format!(
                            "add action missing path field: {add}"
                        ))
                    })?
                    .to_string();
                let size = add.get("size").and_then(|s| s.as_i64()).ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "add action missing required size field: {add}"
                    ))
                })?;
                let dv_id = dv_unique_id(add);
                live_adds.insert((path, dv_id), size);
            }
            if let Some(remove) = action.get("remove") {
                // A remove without `path` is malformed per spec and cannot identify a
                // logical file to cancel; we silently skip it (rather than erroring)
                // because add's required-field check already guards the symmetric case
                // on the producer side.
                if let Some(path) = remove.get("path").and_then(|p| p.as_str()) {
                    let dv_id = dv_unique_id(remove);
                    live_adds.remove(&(path.to_string(), dv_id));
                }
            }
        }
    }

    let protocol_json = protocol_json.ok_or_else(|| {
        delta_kernel::Error::generic(format!("no protocol action found in commits 0..={version}"))
    })?;
    let metadata_json = metadata_json.ok_or_else(|| {
        delta_kernel::Error::generic(format!("no metaData action found in commits 0..={version}"))
    })?;
    let num_files = live_adds.len() as i64;
    let table_size_bytes: i64 = live_adds.values().sum();

    let crc = json!({
        "tableSizeBytes": table_size_bytes,
        "numFiles": num_files,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": metadata_json,
        "protocol": protocol_json,
    });

    let crc_path = delta_path_for_version(version, "crc");
    let resolved = crate::resolve_table_path(table_root, &crc_path)?;
    store
        .put(&resolved, serde_json::to_string(&crc)?.into())
        .await?;
    Ok(())
}

/// Sync wrapper around [`write_crc`]. Safe to call from both sync tests and
/// `#[tokio::test]`; spawns a dedicated multi-threaded tokio runtime on a background
/// thread to avoid the nested-runtime panic that occurs when calling
/// `Runtime::block_on` from a thread that already owns a tokio runtime.
pub fn write_crc_sync(
    version: u64,
    store: &Arc<DynObjectStore>,
    table_root: &str,
) -> DeltaResult<()> {
    block_on_sync(|| write_crc(version, store, table_root))
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
    /// `"commits(3)+checkpoint_at(2)+hint(missing) + columnMapping.mode=name,
    /// enableInCommitTimestamps=true"`). Useful in assert messages to identify which config
    /// failed.
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

    /// Delete the table's `_last_checkpoint` hint file if present. No-op if the file
    /// does not exist (the returned `Ok(())` on NotFound lets tests call this
    /// unconditionally regardless of log state).
    ///
    /// Most callers should configure
    /// [`LogState::with_last_checkpoint_hint(LastCheckpointHintState::Missing)`](
    /// LogState::with_last_checkpoint_hint) at build time instead. This method
    /// is a test-only post-build escape hatch for cases that need to mutate
    /// hint state across multiple snapshot loads on a single table.
    #[cfg(test)]
    pub(crate) fn delete_last_checkpoint(&self) -> DeltaResult<()> {
        let path = Path::from("_delta_log/_last_checkpoint");
        let resolved = crate::resolve_table_path(&self.table_root, &path)?;
        let store = Arc::clone(&self.store);
        block_on_sync(move || async move {
            match store.delete(&resolved).await {
                Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
                Err(e) => Err(delta_kernel::Error::from(e)),
            }
        })
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

/// Convenience wrapper: build a [`TestTable`] from a `log_state` and `feature_set`.
/// Used by the `test_context!` macro and available for direct use in tests.
pub fn test_table(log_state: LogState, feature_set: FeatureSet) -> TestTable {
    TestTableBuilder::new()
        .with_log_state(log_state)
        .with_features(feature_set)
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
        }
    };
}

/// Build a table, engine, and snapshot from rstest parameters in one call.
///
/// Expands at the call site so `Snapshot` and `DefaultEngineBuilder` resolve to the
/// caller's crate types. Returns `(engine, snapshot, table)`.
///
/// Requires `Snapshot` and `DefaultEngineBuilder` to be in scope at the call site.
///
/// ```ignore
/// let (engine, snap, table) = test_context!(log_state, feature_set, version_target);
/// ```
#[macro_export]
macro_rules! test_context {
    ($log_state:expr, $feature_set:expr, $version_target:expr) => {{
        let table = $crate::table_builder::test_table($log_state, $feature_set);
        let engine = DefaultEngineBuilder::new(table.store().clone()).build();
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
        },
        other => panic!("partition columns must be primitive types, got: {other:?}"),
    }
}

// ===========================================================================
// Helpers: schema
// ===========================================================================

/// Default schema with all Delta primitive types including TimestampNtz
/// (nested types are a separate concern).
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
    ]))
}

#[cfg(test)]
mod tests {
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
            .with_log_state(LogState::commits(3))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 2);
        Ok(())
    }

    /// Demonstrates the `test_context!` macro with rstest `#[values]`.
    #[rstest]
    fn test_version_targets(
        #[values(LogState::commits(5))] log_state: LogState,
        #[values(FeatureSet::empty())] feature_set: FeatureSet,
        #[values(
            VersionTarget::Latest,
            VersionTarget::AtVersion(2),
            VersionTarget::IncrementalToLatest { from: 1 }
        )]
        version_target: VersionTarget,
    ) {
        let (_engine, snap, _table) = test_context!(log_state, feature_set, version_target);
        let expected = match &version_target {
            VersionTarget::Latest | VersionTarget::IncrementalToLatest { .. } => 4,
            VersionTarget::AtVersion(v) => *v,
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
    fn test_has_v2_checkpoint() {
        assert!(!FeatureSet::empty().has_v2_checkpoint());
        assert!(FeatureSet::new().v2_checkpoint().has_v2_checkpoint());
        assert!(!FeatureSet::new().ict().has_v2_checkpoint());
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

    /// Verifies every common table config builds successfully.
    #[rstest_reuse::apply(table_configs)]
    fn test_all_table_configs_build(table_config: TableConfig) -> DeltaResult<()> {
        TestTableBuilder::new()
            .with_table_config(table_config)
            .build()?;
        Ok(())
    }

    #[test]
    fn test_checkpoint_v1() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(4).with_checkpoint_at(2))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }

    #[test]
    fn test_checkpoint_v2() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(4).with_checkpoint_at(2))
            .with_features(FeatureSet::new().v2_checkpoint())
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }

    #[test]
    fn test_checkpoint_and_commits() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(5).with_checkpoint_at(2))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 4);
        Ok(())
    }

    #[test]
    fn test_with_crc() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(4).with_crc_at(2))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }

    /// Exhaustive cardinality check: every combination of `checkpoint_at`,
    /// `crc_at`, and the hint state (for `total_versions = 3`) that the builder
    /// permits must be constructible and produce a readable table at the latest
    /// version. Pins the flat-LogState invariant that the cartesian product is
    /// closed (no hidden compatibility constraints) for shapes that meet the
    /// stale-hint precondition (Stale requires checkpoint_at = Some(_)).
    #[rstest::rstest]
    fn test_log_state_cardinality_is_fully_constructible(
        #[values(None, Some(0), Some(1), Some(2))] checkpoint_at: Option<u64>,
        #[values(None, Some(0), Some(1), Some(2))] crc_at: Option<u64>,
        #[values(
            LastCheckpointHintState::Present,
            LastCheckpointHintState::Missing,
            LastCheckpointHintState::Stale
        )]
        hint: LastCheckpointHintState,
    ) -> DeltaResult<()> {
        // Stale hint only makes sense when there is a real checkpoint OLDER than
        // the hint can target -- i.e. `checkpoint_at >= 1`. Skip the impossible
        // corners; the builder asserts on this paired condition independently
        // (covered by a dedicated panic test).
        if hint == LastCheckpointHintState::Stale && !matches!(checkpoint_at, Some(v) if v >= 1) {
            return Ok(());
        }
        let log_state = LogState::commits(3)
            .maybe_with_checkpoint_at(checkpoint_at)
            .maybe_with_crc_at(crc_at)
            .with_last_checkpoint_hint(hint);
        let table = TestTableBuilder::new().with_log_state(log_state).build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(
            snap.version(),
            2,
            "rebuild lost commits for {}",
            table.description(),
        );
        Ok(())
    }

    /// Stale hint paired with `checkpoint_at = None` is rejected at build time
    /// because Stale's "stale relative to what?" semantics need a real checkpoint.
    /// Stale hint paired with `checkpoint_at = Some(0)` is also rejected because
    /// kernel does not recover from hints pointing AHEAD of the actual checkpoint
    /// (only OLDER hints recover via re-listing).
    #[rstest::rstest]
    #[case::no_checkpoint(LogState::commits(3))]
    #[case::checkpoint_at_zero(LogState::commits(3).with_checkpoint_at(0))]
    #[should_panic(expected = "Stale hint requires")]
    fn test_stale_hint_invalid_pairings_panic(#[case] base: LogState) {
        let log_state = base.with_last_checkpoint_hint(LastCheckpointHintState::Stale);
        let _ = TestTableBuilder::new().with_log_state(log_state).build();
    }

    /// Locks in that the builder actually writes checkpoint and CRC files to disk
    /// (rather than the snapshot rebuild silently passing via JSON-replay fallback).
    /// Lists `_delta_log/` directly to verify checkpoint presence by filename prefix
    /// (V2 checkpoints have a UUID suffix), and parses the on-disk CRC content to
    /// verify field-name compatibility with kernel's CRC reader.
    #[rstest::rstest]
    fn test_builder_writes_checkpoint_and_crc_files_on_disk(
        #[values(FeatureSet::empty(), FeatureSet::new().v2_checkpoint())] features: FeatureSet,
    ) -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(3).with_checkpoint_at(2).with_crc_at(2))
            .with_features(features)
            .build()?;

        // Checkpoint file at v=2 is on disk: filename starts with version digits and
        // contains the `.checkpoint` segment regardless of V1/V2 format.
        let entries = list_log_dir_filenames(table.store())?;
        assert!(
            entries
                .iter()
                .any(|name| name.starts_with("00000000000000000002.checkpoint")
                    && name.ends_with(".parquet")),
            "no checkpoint file at v=2 in: {entries:?}",
        );

        // CRC content at v=2 parses, exposes the camelCase field names kernel's
        // CRC reader expects, and reports counts/sizes consistent with the
        // builder's actual on-disk file layout (default 1 data file per data
        // commit -> 2 adds at v=1 and v=2).
        let crc = read_crc_json(&table, 2)?;
        assert_eq!(
            crc["numFiles"].as_u64().unwrap(),
            2,
            "numFiles should match the 2 data adds the builder emits at v=1 and v=2",
        );
        assert!(
            crc["tableSizeBytes"].as_u64().unwrap() > 0,
            "tableSizeBytes should be positive for a table with 2 data files",
        );
        assert!(crc["protocol"].is_object(), "missing protocol object");
        assert!(crc["metadata"].is_object(), "missing metadata object");
        Ok(())
    }

    #[test]
    fn test_all_tables() -> DeltaResult<()> {
        let tables = TestTableBuilder::all_tables(FeatureSet::empty())?;
        // `all_tables` cross-products LogState::common() with FeatureSet::common();
        // every combination is valid, so the expected count is just the product.
        let expected = LogState::common().len() * FeatureSet::common().len();
        assert_eq!(tables.len(), expected);
        for table in &tables {
            let engine = table.engine();
            let builder = Snapshot::builder_for(table.table_root());
            let snap = builder.build(&engine)?;
            // Every shape in `LogState::common` uses 5 versions (v0..=v4).
            assert_eq!(snap.version(), 4, "{}", table.description());
        }
        Ok(())
    }

    #[test]
    fn test_all_tables_propagates_required_features() -> DeltaResult<()> {
        let tables = TestTableBuilder::all_tables(FeatureSet::new().deletion_vectors())?;
        assert!(!tables.is_empty());
        for table in &tables {
            let engine = table.engine();
            let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
            let writer_features = snap
                .table_configuration()
                .protocol()
                .writer_features()
                .expect("writer features present");
            assert!(
                writer_features.contains(&TableFeature::DeletionVectors),
                "missing DeletionVectors in table: {}",
                table.description()
            );
        }
        Ok(())
    }

    // Whether a checkpoint at `checkpoint_at` is written in V1 or V2 format is
    // determined by the paired [`FeatureSet`]'s `v2Checkpoint`, not by [`LogState`].
    // Both combinations are valid; kernel's checkpoint writer dispatches on the
    // feature alone. This pair of tests locks in that invariant.
    #[test]
    fn test_checkpoint_builds_without_v2_feature() -> DeltaResult<()> {
        TestTableBuilder::new()
            .with_log_state(LogState::commits(3).with_checkpoint_at(1))
            .build()?;
        Ok(())
    }

    #[test]
    fn test_checkpoint_builds_with_v2_feature() -> DeltaResult<()> {
        TestTableBuilder::new()
            .with_log_state(LogState::commits(3).with_checkpoint_at(1))
            .with_features(FeatureSet::new().v2_checkpoint())
            .build()?;
        Ok(())
    }

    #[rstest::rstest]
    #[case::empty_into_empty(FeatureSet::empty(), FeatureSet::empty(), vec![])]
    #[case::empty_into_nonempty(
        FeatureSet::new().ict(),
        FeatureSet::empty(),
        vec![("delta.enableInCommitTimestamps".into(), "true".into())]
    )]
    #[case::nonempty_into_empty(
        FeatureSet::empty(),
        FeatureSet::new().ict(),
        vec![("delta.enableInCommitTimestamps".into(), "true".into())]
    )]
    #[case::disjoint_preserves_order(
        FeatureSet::new().ict(),
        FeatureSet::new().v2_checkpoint(),
        vec![
            ("delta.enableInCommitTimestamps".into(), "true".into()),
            ("delta.feature.v2Checkpoint".into(), "supported".into()),
        ]
    )]
    #[case::duplicate_key_other_wins(
        FeatureSet::new().column_mapping("name"),
        FeatureSet::new().column_mapping("id"),
        vec![("delta.columnMapping.mode".into(), "id".into())]
    )]
    #[case::same_key_same_value_deduped(
        FeatureSet::new().v2_checkpoint(),
        FeatureSet::new().v2_checkpoint(),
        vec![("delta.feature.v2Checkpoint".into(), "supported".into())]
    )]
    fn test_feature_set_merge(
        #[case] base: FeatureSet,
        #[case] other: FeatureSet,
        #[case] expected: Vec<(String, String)>,
    ) {
        let merged = base.merge(&other);
        assert_eq!(merged.table_properties, expected);
    }

    #[test]
    fn test_write_crc_content() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(2))
            .with_features(FeatureSet::new().column_mapping("name"))
            .build()?;
        write_crc_sync(1, table.store(), table.table_root())?;

        let crc = read_crc_json(&table, 1)?;
        assert_eq!(crc["numMetadata"], 1);
        assert_eq!(crc["numProtocol"], 1);
        // Default builder writes 1 file per data commit; v1 has one Add.
        assert_eq!(crc["numFiles"], 1);
        assert!(crc["tableSizeBytes"].as_i64().unwrap() > 0);
        assert!(crc["metadata"].is_object());
        assert!(crc["protocol"].is_object());
        assert_eq!(
            crc["metadata"]["configuration"]["delta.columnMapping.mode"],
            "name"
        );
        Ok(())
    }

    #[test]
    fn test_scan_with_column_mapping() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(2))
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
            .with_log_state(LogState::commits(2))
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

    #[rstest::rstest]
    #[case::partitioned(DataLayoutConfig::PartitionedAllTypes, partitioned_schema())]
    #[case::clustered(DataLayoutConfig::ClusteredAllTypes, clustered_schema())]
    fn test_data_layout_table(
        #[case] config: DataLayoutConfig,
        #[case] expected_schema: SchemaRef,
    ) -> DeltaResult<()> {
        // 2 versions means v0 (create_table) + v1 (1 data commit with 10 rows)
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(2))
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
            .with_log_state(LogState::commits(4))
            .with_data_layout(DataLayoutConfig::ClusteredAllTypes)
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
            .with_log_state(LogState::commits(2))
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
    #[should_panic(expected = "at least 1 version")]
    fn test_commits_zero_panics() {
        LogState::commits(0);
    }

    #[test]
    #[should_panic(expected = "must be <")]
    fn test_with_checkpoint_at_rejects_at_equal_total() {
        LogState::commits(3).with_checkpoint_at(3);
    }

    #[test]
    #[should_panic(expected = "must be <")]
    fn test_with_checkpoint_at_rejects_at_greater_than_total() {
        LogState::commits(3).with_checkpoint_at(4);
    }

    #[test]
    #[should_panic(expected = "must be <")]
    fn test_with_crc_at_rejects_at_equal_total() {
        LogState::commits(3).with_crc_at(3);
    }

    #[test]
    #[should_panic(expected = "must be <")]
    fn test_with_crc_at_rejects_at_greater_than_total() {
        LogState::commits(3).with_crc_at(4);
    }

    #[rstest::rstest]
    #[case(LogState::commits(4).with_checkpoint_at(2), 4)]
    #[case(LogState::commits(4).with_checkpoint_at(1), 4)]
    #[case(LogState::commits(3).with_crc_at(0), 3)]
    #[case(LogState::commits(5), 5)]
    #[case(LogState::commits(3).with_checkpoint_at(1).with_crc_at(2), 3)]
    fn test_log_state_num_versions(#[case] log_state: LogState, #[case] expected: u64) {
        assert_eq!(log_state.num_versions(), expected);
    }

    #[rstest::rstest]
    #[case::hint_present_checkpoint(LogState::commits(3).with_checkpoint_at(1), true)]
    #[case::hint_absent_commits_only(LogState::commits(3), false)]
    #[case::hint_absent_crc(LogState::commits(3).with_crc_at(0), false)]
    #[case::hint_suppressed_via_builder(
        LogState::commits(3).with_checkpoint_at(1).keep_last_checkpoint_hint(false),
        false,
    )]
    fn test_last_checkpoint_hint_presence(
        #[case] log_state: LogState,
        #[case] hint_should_exist: bool,
    ) -> DeltaResult<()> {
        let table = TestTableBuilder::new().with_log_state(log_state).build()?;
        let hint_path = crate::resolve_table_path(
            table.table_root(),
            &Path::from("_delta_log/_last_checkpoint"),
        )?;

        assert_eq!(
            object_exists(table.store(), &hint_path)?,
            hint_should_exist,
            "unexpected hint existence for {}",
            table.description()
        );

        // `delete_last_checkpoint` is a no-op when the hint is already absent and
        // is idempotent when present. Calling it twice exercises the NotFound-swallow
        // branch either way.
        table.delete_last_checkpoint()?;
        table.delete_last_checkpoint()?;
        assert!(
            !object_exists(table.store(), &hint_path)?,
            "hint still present after delete",
        );
        Ok(())
    }

    #[test]
    fn test_write_crc_reconciles_removes() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(2))
            .build()?;
        // Extract v1's add path/size so we can hand-write a matching remove.
        let text = read_commit_json(&table, 1)?;
        let (add_path, add_size) = text
            .lines()
            .find_map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).ok()?;
                let add = v.get("add")?;
                let p = add.get("path")?.as_str()?.to_string();
                let s = add.get("size")?.as_i64()?;
                Some((p, s))
            })
            .expect("commit v1 should contain an add action");

        let commit_v2 = serde_json::json!({
            "remove": {
                "path": add_path,
                "dataChange": true,
                "deletionTimestamp": 0i64,
            }
        })
        .to_string();
        add_commit_sync(table.store(), table.table_root(), 2, commit_v2)?;

        write_crc_sync(2, table.store(), table.table_root())?;

        let crc = read_crc_json(&table, 2)?;
        assert_eq!(crc["numFiles"], 0, "remove should cancel the earlier add");
        assert_eq!(
            crc["tableSizeBytes"].as_i64().unwrap(),
            0,
            "tableSizeBytes should drop the removed file's size (was {add_size})",
        );
        Ok(())
    }

    #[test]
    fn test_write_crc_errors_when_commit_missing() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(1))
            .build()?;
        // Ask for a CRC at a version that has no commit file on disk.
        let err = write_crc_sync(5, table.store(), table.table_root())
            .expect_err("expected error for missing commit");
        let msg = err.to_string();
        assert!(
            msg.contains("Not found") || msg.contains("NotFound") || msg.contains("not found"),
            "expected object-store not-found, got: {msg}",
        );
        Ok(())
    }

    #[test]
    fn test_write_crc_errors_when_protocol_missing() -> DeltaResult<()> {
        // Hand-build a table root with a single commit that has neither protocol nor
        // metaData (only a commitInfo action). write_crc should surface a descriptive
        // error rather than producing a broken CRC.
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let commit_v0 = serde_json::json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "TEST",
            }
        })
        .to_string();
        add_commit_sync(&store, table_root, 0, commit_v0)?;
        let err =
            write_crc_sync(0, &store, table_root).expect_err("expected error for missing protocol");
        let msg = err.to_string();
        assert!(
            msg.contains("no protocol action found"),
            "expected protocol-missing error, got: {msg}",
        );
        Ok(())
    }

    #[test]
    fn test_write_crc_picks_up_latest_protocol() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::commits(2))
            .build()?;

        // Hand-craft a v2 commit that bumps the protocol's minReaderVersion.
        let protocol_bump = serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": [],
            }
        })
        .to_string();
        add_commit_sync(table.store(), table.table_root(), 2, protocol_bump)?;

        write_crc_sync(2, table.store(), table.table_root())?;
        let crc = read_crc_json(&table, 2)?;
        assert_eq!(
            crc["protocol"]["minReaderVersion"], 3,
            "CRC should reflect the latest-wins protocol from commit 2",
        );
        Ok(())
    }

    /// Helper: read and parse a CRC file at `version` from `table`'s store.
    fn read_crc_json(table: &TestTable, version: u64) -> DeltaResult<serde_json::Value> {
        let crc_path =
            crate::resolve_table_path(table.table_root(), &delta_path_for_version(version, "crc"))?;
        let store = table.store().clone();
        let bytes = block_on_sync(move || async move {
            let data = store
                .get(&crc_path)
                .await
                .map_err(delta_kernel::Error::from)?;
            data.bytes().await.map_err(delta_kernel::Error::from)
        })?;
        serde_json::from_slice(&bytes).map_err(|e| delta_kernel::Error::generic(e.to_string()))
    }

    /// Helper: read and parse a commit JSON file at `version` from `table`'s store.
    fn read_commit_json(table: &TestTable, version: u64) -> DeltaResult<String> {
        let commit_path = crate::resolve_table_path(
            table.table_root(),
            &delta_path_for_version(version, "json"),
        )?;
        let store = table.store().clone();
        block_on_sync(move || async move {
            let data = store
                .get(&commit_path)
                .await
                .map_err(delta_kernel::Error::from)?
                .bytes()
                .await
                .map_err(delta_kernel::Error::from)?;
            String::from_utf8(data.to_vec())
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))
        })
    }

    /// Helper: synchronously append a commit JSON at `version`.
    fn add_commit_sync(
        store: &Arc<DynObjectStore>,
        table_root: &str,
        version: u64,
        data: String,
    ) -> DeltaResult<()> {
        let store = store.clone();
        let table_root = table_root.to_string();
        block_on_sync(move || async move {
            crate::add_commit(&table_root, store.as_ref(), version, data)
                .await
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))
        })
    }

    /// Helper: list filenames (basenames only) directly under `_delta_log/` in
    /// `store`. Returns just the leaf name so callers can match by suffix without
    /// dealing with `memory:///`-style path quirks.
    fn list_log_dir_filenames(store: &Arc<DynObjectStore>) -> DeltaResult<Vec<String>> {
        use delta_kernel::object_store::ObjectStore;
        let store = store.clone();
        block_on_sync(move || async move {
            let prefix = Path::from("_delta_log");
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

    /// Helper: synchronously check whether an object exists at `path` in `store`.
    fn object_exists(store: &Arc<DynObjectStore>, path: &Path) -> DeltaResult<bool> {
        let store = store.clone();
        let path = path.clone();
        block_on_sync(move || async move {
            match store.head(&path).await {
                Ok(_) => Ok(true),
                Err(ObjectStoreError::NotFound { .. }) => Ok(false),
                Err(e) => Err(delta_kernel::Error::from(e)),
            }
        })
    }

    /// Build a DV sub-object JSON for use in Add/Remove test fixtures.
    fn dv(storage_type: &str, path_or_inline: &str, offset: Option<i64>) -> serde_json::Value {
        let mut v = serde_json::json!({
            "storageType": storage_type,
            "pathOrInlineDv": path_or_inline,
        });
        if let Some(o) = offset {
            v.as_object_mut().unwrap().insert("offset".into(), o.into());
        }
        v
    }

    /// Covers the `(path, deletionVector.uniqueId)` reconciliation rule: removes only
    /// cancel adds whose logical file primary key matches, not merely whose `path`
    /// matches. Each case hand-writes a commit that adds one file (with optional DV)
    /// and a commit that removes a file (with optional DV) sharing the same path, then
    /// asserts whether the resulting CRC shows the file as live.
    #[rstest::rstest]
    #[case::no_dv_on_both_sides_cancels(None, None, 0)]
    #[case::matching_dv_cancels(
        Some(("u", "abc", None)),
        Some(("u", "abc", None)),
        0,
    )]
    #[case::different_dv_path_preserves_add(
        Some(("u", "abc", None)),
        Some(("u", "def", None)),
        1,
    )]
    #[case::different_storage_type_preserves_add(
        Some(("u", "abc", None)),
        Some(("p", "abc", None)),
        1,
    )]
    #[case::matching_offset_cancels(
        Some(("u", "abc", Some(5))),
        Some(("u", "abc", Some(5))),
        0,
    )]
    #[case::different_offset_preserves_add(
        Some(("u", "abc", Some(5))),
        Some(("u", "abc", Some(9))),
        1,
    )]
    #[case::remove_without_dv_preserves_dv_tagged_add(
        Some(("u", "abc", None)),
        None,
        1,
    )]
    #[case::remove_with_dv_preserves_plain_add(
        None,
        Some(("u", "abc", None)),
        1,
    )]
    #[case::zero_offset_is_present_and_cancels(
        Some(("u", "abc", Some(0))),
        Some(("u", "abc", Some(0))),
        0,
    )]
    // `dv_unique_id` derives uniqueId purely from JSON presence (per spec's
    // "Derived Fields" table), so absent-offset and offset=0 produce different
    // keys even when the spec's "semantic" reading would treat them as the same
    // file. This case pins the fixture to the literal JSON-presence rule.
    #[case::absent_offset_and_zero_offset_are_distinct_keys(
        Some(("u", "abc", None)),
        Some(("u", "abc", Some(0))),
        1,
    )]
    fn test_write_crc_dv_keyed_reconciliation(
        #[case] add_dv: Option<(&str, &str, Option<i64>)>,
        #[case] remove_dv: Option<(&str, &str, Option<i64>)>,
        #[case] expected_num_files: i64,
    ) -> DeltaResult<()> {
        // Build a minimal table (v0 only) and hand-write v1 (add) and v2 (remove) so
        // the DV fields are under test-control rather than derived from the kernel
        // writer's default output.
        let table = TestTableBuilder::new().build()?;
        let path = "hand/written.parquet".to_string();
        let size: i64 = 100;

        let mut add = serde_json::json!({
            "path": path,
            "partitionValues": {},
            "size": size,
            "modificationTime": 0i64,
            "dataChange": true,
        });
        if let Some((s, p, o)) = add_dv {
            add.as_object_mut()
                .unwrap()
                .insert("deletionVector".into(), dv(s, p, o));
        }
        let commit_v1 = serde_json::json!({ "add": add }).to_string();

        let mut remove = serde_json::json!({
            "path": path,
            "dataChange": true,
        });
        if let Some((s, p, o)) = remove_dv {
            remove
                .as_object_mut()
                .unwrap()
                .insert("deletionVector".into(), dv(s, p, o));
        }
        let commit_v2 = serde_json::json!({ "remove": remove }).to_string();

        add_commit_sync(table.store(), table.table_root(), 1, commit_v1)?;
        add_commit_sync(table.store(), table.table_root(), 2, commit_v2)?;

        write_crc_sync(2, table.store(), table.table_root())?;

        let crc = read_crc_json(&table, 2)?;
        assert_eq!(
            crc["numFiles"].as_i64().unwrap(),
            expected_num_files,
            "add_dv={add_dv:?}, remove_dv={remove_dv:?}",
        );
        let expected_size = expected_num_files * size;
        assert_eq!(
            crc["tableSizeBytes"].as_i64().unwrap(),
            expected_size,
            "tableSizeBytes mismatch for add_dv={add_dv:?}, remove_dv={remove_dv:?}",
        );
        Ok(())
    }
}
