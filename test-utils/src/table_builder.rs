//! A composable test table builder for delta-kernel-rs.
//!
//! Provides four orthogonal axes for parameterized testing:
//! - [`LogState`] -- what log files exist on disk (commits, checkpoints, CRC)
//! - [`FeatureSet`] -- which Delta table features are enabled
//! - [`VersionTarget`] -- how the snapshot is loaded (latest, time travel, incremental)
//! - [`PartitionConfig`] -- partition column configuration
//!
//! # Quick start
//!
//! The [`test_context!`] macro builds a table, engine, and snapshot in one call.
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
//!     #[values(VersionTarget::Latest, VersionTarget::Incremental { from: 0, to: 2 })]
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

use std::fmt;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
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
    /// `n` must be >= 1. Version 0 is the create-table commit. For example, `commits(3)`
    /// produces versions 0, 1, 2 where version 0 has only metadata and versions 1-2 have
    /// data files.
    pub fn commits(n: u64) -> Self {
        assert!(n >= 1, "commits() requires at least 1 version (the create-table commit)");
        LogState::CommitsOnly { num_commits: n }
    }

    /// Total number of versions in this log state.
    pub(crate) fn total_versions(&self) -> u64 {
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
// VersionTarget
// ===========================================================================

/// How the snapshot should be loaded from the built table.
///
/// Designed for use as an rstest `#[values]` parameter. Use [`build_snapshot`] to load a
/// snapshot according to this target, or define a local fixture in `kernel/src/` tests
/// (see module-level docs for an example).
///
/// [`build_snapshot`]: VersionTarget::build_snapshot
#[derive(Clone, Debug)]
pub enum VersionTarget {
    /// Load the latest version.
    Latest,
    /// Time travel to a specific version.
    AtVersion(u64),
    /// Load at `from`, then incrementally update to `to`.
    Incremental { from: u64, to: u64 },
}

impl VersionTarget {
    /// Load a snapshot from the given table root according to this target.
    ///
    /// Returns test_utils's `Snapshot` type. For unit tests inside `kernel/src/`, define
    /// a local fixture instead (see module-level docs).
    pub fn build_snapshot(
        &self,
        table_root: &str,
        engine: &impl delta_kernel::Engine,
    ) -> Arc<Snapshot> {
        match self {
            VersionTarget::Latest => Snapshot::builder_for(table_root)
                .build(engine)
                .expect("failed to load latest snapshot"),
            VersionTarget::AtVersion(v) => Snapshot::builder_for(table_root)
                .at_version(*v)
                .build(engine)
                .expect("failed to load snapshot at version"),
            VersionTarget::Incremental { from, to } => {
                let base = Snapshot::builder_for(table_root)
                    .at_version(*from)
                    .build(engine)
                    .expect("failed to load base snapshot");
                Snapshot::builder_from(base)
                    .at_version(*to)
                    .build(engine)
                    .expect("failed to load incremental snapshot")
            }
        }
    }
}

impl fmt::Display for VersionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionTarget::Latest => write!(f, "latest"),
            VersionTarget::AtVersion(v) => write!(f, "at_version({v})"),
            VersionTarget::Incremental { from, to } => {
                write!(f, "incremental({from}->{to})")
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
    schema: Option<SchemaRef>,
}

impl Default for TestTableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestTableBuilder {
    /// Create a builder with sensible defaults: 1 commit, no features.
    pub fn new() -> Self {
        Self {
            log_state: LogState::commits(1),
            features: FeatureSet::empty(),
            schema: None,
        }
    }

    /// Set the log state (what files exist on disk).
    pub fn log_state(mut self, s: LogState) -> Self {
        self.log_state = s;
        self
    }

    /// Set the table features.
    pub fn features(mut self, f: FeatureSet) -> Self {
        self.features = f;
        self
    }

    /// Override the default schema.
    pub fn schema(mut self, s: SchemaRef) -> Self {
        self.schema = Some(s);
        self
    }

    /// Build the table and return a [`TestTable`] handle to the store.
    pub fn build(self) -> DeltaResult<TestTable> {
        tokio::runtime::Runtime::new()
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))?
            .block_on(self.build_async())
    }

    async fn build_async(self) -> DeltaResult<TestTable> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = Arc::new(
            DefaultEngineBuilder::new(store.clone())
                .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                    tokio::runtime::Handle::current(),
                )))
                .build(),
        );
        let schema = self.schema.clone().unwrap_or_else(default_schema);

        // Version 0: CreateTable
        let mut builder = create_table(table_root, schema, "TestTableBuilder/1.0");
        if !self.features.table_properties.is_empty() {
            builder = builder.with_table_properties(
                self.features
                    .table_properties
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str())),
            );
        }
        let committed = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_committed();
        let mut snapshot = committed.post_commit_snapshot().unwrap().clone();

        // Data commits (versions 1..N)
        let total = self.log_state.total_versions();
        for _v in 1..total {
            let result = write_empty_commit(snapshot.clone(), &engine)?;
            snapshot = result
                .unwrap_committed()
                .post_commit_snapshot()
                .unwrap()
                .clone();
        }

        Ok(TestTable {
            store,
            table_root: table_root.to_string(),
            description: format!("{} + {}", self.log_state, self.features),
        })
    }
}

// ===========================================================================
// Empty commit
// ===========================================================================

/// Write an empty commit (no data files) using kernel's transaction path.
fn write_empty_commit(
    snapshot: Arc<Snapshot>,
    engine: &Arc<DefaultEngine<TokioMultiThreadExecutor>>,
) -> DeltaResult<delta_kernel::transaction::CommitResult> {
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());
    txn.commit(engine.as_ref())
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

/// Build a [`TestTable`] from a `log_state` and `feature_set`.
///
/// Designed for use as an rstest fixture. In an rstest test, declare `log_state: LogState`
/// and `feature_set: FeatureSet` with `#[values(...)]`, then add `table: TestTable` as a
/// parameter -- rstest will auto-inject it using this function.
pub fn table(log_state: LogState, feature_set: FeatureSet) -> TestTable {
    TestTableBuilder::new()
        .log_state(log_state)
        .features(feature_set)
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
            $crate::table_builder::VersionTarget::Incremental { from, to } => {
                let base = Snapshot::builder_for($table_root)
                    .at_version(*from)
                    .build($engine)
                    .unwrap();
                Snapshot::builder_from(base)
                    .at_version(*to)
                    .build($engine)
                    .unwrap()
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
        let table = $crate::table_builder::table($log_state, $feature_set);
        let engine = DefaultEngineBuilder::new(table.store().clone()).build();
        let snap = $crate::build_snapshot!($version_target, table.table_root(), &engine);
        (engine, snap, table)
    }};
}

// ===========================================================================
// Helpers: schema
// ===========================================================================

/// Default schema with all Delta primitive types (except TimestampNtz which auto-enables
/// a table feature, and nested types which are a separate concern).
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
        StructField::new("decimal_col", DataType::decimal(10, 2).unwrap(), true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

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
            .log_state(LogState::commits(3))
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
            VersionTarget::Incremental { from: 1, to: 3 }
        )]
        version_target: VersionTarget,
    ) {
        let (_engine, snap, _table) =
            test_context!(log_state, feature_set, version_target);
        let expected = match &version_target {
            VersionTarget::Latest => 4,
            VersionTarget::AtVersion(v) => *v,
            VersionTarget::Incremental { to, .. } => *to,
        };
        assert_eq!(snap.version(), expected);
    }

    #[test]
    #[should_panic(expected = "at least 1 version")]
    fn test_commits_zero_panics() {
        LogState::commits(0);
    }
}
