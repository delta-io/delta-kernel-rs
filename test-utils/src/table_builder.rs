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
//!     #[values(LogState::with_commits(3))]
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
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef, StructField, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Snapshot};

// ===========================================================================
// LogState
// ===========================================================================

/// Describes the structure of a Delta table's log files on disk.
#[derive(Clone, Debug)]
pub enum LogState {
    /// Only JSON commit files: `num_commits` total versions (0 through `num_commits - 1`).
    /// Version 0 is always the create-table commit; versions 1+ contain data.
    CommitsOnly { num_commits: u64 },
}

impl LogState {
    /// Table with `n` total versions as JSON commit files.
    ///
    /// `n` must be >= 1. Version 0 is a metadata-only create-table commit (not CTAS).
    /// For example, `with_commits(3)` produces versions 0, 1, 2 where version 0 has only
    /// metadata and versions 1-2 contain data.
    pub fn with_commits(n: u64) -> Self {
        assert!(
            n >= 1,
            "with_commits() requires at least 1 version (the create-table commit)"
        );
        LogState::CommitsOnly { num_commits: n }
    }

    /// Number of commit files on disk (versions 0 through `num_versions - 1`).
    pub(crate) fn num_versions(&self) -> u64 {
        match self {
            LogState::CommitsOnly { num_commits } => *num_commits,
        }
    }
}

impl fmt::Display for LogState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogState::CommitsOnly { num_commits } => write!(f, "commits({num_commits})"),
        }
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

    /// Common feature sets for cross-product testing: empty, one per feature, and one
    /// with all features combined. Not the full power set -- add specific combos as needed.
    pub fn common() -> Vec<Self> {
        vec![
            Self::empty(),
            Self::new().column_mapping("name"),
            Self::new().ict(),
            Self::new().v2_checkpoint(),
            Self::new().deletion_vectors(),
            Self::new().append_only(),
            Self::new().change_data_feed(),
            Self::new().type_widening(),
            Self::new().domain_metadata(),
            Self::new().vacuum_protocol_check(),
            Self::new().row_tracking(),
            // All features combined
            Self::new()
                .column_mapping("name")
                .ict()
                .v2_checkpoint()
                .deletion_vectors()
                .append_only()
                .change_data_feed()
                .type_widening()
                .domain_metadata()
                .vacuum_protocol_check()
                .row_tracking(),
        ]
    }

    /// Whether v2_checkpoint is enabled.
    pub fn has_v2_checkpoint(&self) -> bool {
        self.table_properties
            .iter()
            .any(|(k, v)| k == "delta.feature.v2Checkpoint" && v == "supported")
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

/// All common feature sets: empty, one per feature, and all combined.
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
        FeatureSet::new().type_widening(),
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
            .type_widening()
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
            log_state: LogState::with_commits(1),
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

    /// Build the table and return a [`TestTable`] handle to the store.
    ///
    /// Safe to call from both sync tests and `#[tokio::test]` -- uses a dedicated runtime
    /// on a background thread to avoid panicking on nested runtimes.
    pub fn build(self) -> DeltaResult<TestTable> {
        std::thread::scope(|s| {
            s.spawn(|| {
                tokio::runtime::Runtime::new()
                    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?
                    .block_on(self.build_async())
            })
            .join()
            .expect("builder thread panicked")
        })
    }

    async fn build_async(self) -> DeltaResult<TestTable> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
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

/// Write a data commit using kernel's transaction + write_parquet path.
/// Produces `num_files` parquet files with `rows_per_file` rows each. For partitioned
/// tables, all rows in a file share the same partition values; for unpartitioned or
/// clustered tables, uses `unpartitioned_write_context`. Non-partition columns get
/// varying data derived from version and file index.
async fn write_data_commit(
    snapshot: Arc<Snapshot>,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
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
    /// `"commits(3) + columnMapping.mode=name, enableInCommitTimestamps=true"`).
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
            PrimitiveType::Void => panic!("void type is not a valid partition column"),
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
            .with_log_state(LogState::with_commits(3))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 2);
        Ok(())
    }

    /// Demonstrates the `test_context!` macro with rstest `#[values]`.
    #[rstest]
    fn test_version_targets(
        #[values(LogState::with_commits(5))] log_state: LogState,
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
    fn test_scan_with_column_mapping() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_commits(2))
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
            .with_log_state(LogState::with_commits(2))
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
            .with_log_state(LogState::with_commits(2))
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
            .with_log_state(LogState::with_commits(4))
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
            .with_log_state(LogState::with_commits(2))
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
        LogState::with_commits(0);
    }
}
