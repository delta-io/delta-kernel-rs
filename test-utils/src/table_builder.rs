//! A composable test table builder for delta-kernel-rs.
//!
//! Provides three orthogonal axes for parameterized testing:
//! - [`LogState`] -- what log files exist on disk (commits, checkpoints, CRC, compaction)
//! - [`FeatureSet`] -- which Delta table features are enabled
//! - [`VersionTarget`] -- how the snapshot is loaded (latest, time travel, incremental)
//!
//! These are designed to work with rstest `#[values]` for cartesian-product test generation:
//!
//! ```ignore
//! #[rstest]
//! fn test_snapshot(
//!     #[values(LogState::commits(3), LogState::checkpoint_v1(2, 3))]
//!     log_state: LogState,
//!     #[values(FeatureSet::empty(), FeatureSet::new().column_mapping("name"))]
//!     features: FeatureSet,
//! ) -> DeltaResult<()> {
//!     let table = TestTableBuilder::new()
//!         .log_state(log_state)
//!         .features(features)
//!         .build()?;
//!     let engine = table.engine();
//!     let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
//!     // ...
//! }
//! ```
//!
//! `TestTable` exposes only the store and table root -- no kernel types like `Snapshot`.
//! This avoids type mismatches when the builder is used from unit tests inside kernel/src/,
//! where the test's `Snapshot` and `test_utils`'s `Snapshot` would be different crate instances.

use std::collections::HashMap;
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
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use serde_json::json;

use crate::delta_path_for_version;

// ---- LogState ---------------------------------------------------------------

/// Describes the structure of a Delta table's log files on disk.
#[derive(Clone, Debug)]
pub enum LogState {
    /// Only JSON commit files: versions 0 through `num_commits - 1`.
    CommitsOnly { num_commits: u64 },
    /// Commits 0..total_versions, with a V1 parquet checkpoint at `checkpoint_at`.
    CheckpointV1 {
        checkpoint_at: u64,
        total_versions: u64,
    },
    /// Commits 0..total_versions, with a V2 checkpoint at `checkpoint_at`.
    /// Requires `FeatureSet::new().v2_checkpoint()`.
    CheckpointV2 {
        checkpoint_at: u64,
        total_versions: u64,
    },
    /// A V1 checkpoint followed by additional commits.
    CheckpointAndCommits {
        checkpoint_at: u64,
        commits_after: u64,
    },
    /// Commits with a CRC file at `crc_at`.
    WithCrc { crc_at: u64, total_versions: u64 },
}

impl LogState {
    /// Table with only JSON commit files.
    pub fn commits(n: u64) -> Self {
        LogState::CommitsOnly { num_commits: n }
    }

    /// Table with a V1 checkpoint at the given version.
    pub fn checkpoint_v1(at: u64, total: u64) -> Self {
        LogState::CheckpointV1 {
            checkpoint_at: at,
            total_versions: total,
        }
    }

    /// Table with a V2 checkpoint at the given version.
    pub fn checkpoint_v2(at: u64, total: u64) -> Self {
        LogState::CheckpointV2 {
            checkpoint_at: at,
            total_versions: total,
        }
    }

    /// Table with a V1 checkpoint followed by additional commits.
    pub fn checkpoint_and_commits(checkpoint_at: u64, commits_after: u64) -> Self {
        LogState::CheckpointAndCommits {
            checkpoint_at,
            commits_after,
        }
    }

    /// Table with a CRC file at the given version.
    pub fn with_crc(at: u64, total: u64) -> Self {
        LogState::WithCrc {
            crc_at: at,
            total_versions: total,
        }
    }

    /// Total number of versions in this log state.
    fn total_versions(&self) -> u64 {
        match self {
            LogState::CommitsOnly { num_commits } => *num_commits,
            LogState::CheckpointV1 { total_versions, .. } => *total_versions,
            LogState::CheckpointV2 { total_versions, .. } => *total_versions,
            LogState::CheckpointAndCommits {
                checkpoint_at,
                commits_after,
            } => checkpoint_at + commits_after + 1,
            LogState::WithCrc { total_versions, .. } => *total_versions,
        }
    }
}

// ---- FeatureSet -------------------------------------------------------------

/// Which Delta table features to enable. Methods chain for composability.
///
/// Stores table properties passed to the `CreateTable` API, which handles protocol
/// derivation, schema annotations (column mapping), and feature auto-enablement.
#[derive(Clone, Debug, Default)]
pub struct FeatureSet {
    table_properties: Vec<(String, String)>,
}

impl FeatureSet {
    /// No features enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Alias for `new()`.
    pub fn empty() -> Self {
        Self::new()
    }

    /// Enable column mapping with the given mode ("none", "name", or "id").
    pub fn column_mapping(mut self, mode: &str) -> Self {
        if mode != "none" {
            self.table_properties
                .push(("delta.columnMapping.mode".into(), mode.into()));
        }
        self
    }

    /// Enable V2 checkpoints.
    pub fn v2_checkpoint(mut self) -> Self {
        self.table_properties
            .push(("delta.feature.v2Checkpoint".into(), "supported".into()));
        self
    }

    /// Enable in-commit timestamps.
    pub fn ict(mut self) -> Self {
        self.table_properties
            .push(("delta.enableInCommitTimestamps".into(), "true".into()));
        self
    }

    // -- Features below will be enabled as CreateTable support lands --
    // pub fn dvs(mut self) -> Self { ... }
    // pub fn cdf(mut self) -> Self { ... }
    // pub fn row_tracking(mut self) -> Self { ... }
    // pub fn timestamp_ntz(mut self) -> Self { ... }
}

// ---- VersionTarget ----------------------------------------------------------

/// How the snapshot should be loaded from the built table.
/// Passed as an rstest `#[values]` parameter but applied by the test, not the builder.
#[derive(Clone, Debug)]
pub enum VersionTarget {
    /// Load the latest version.
    Latest,
    /// Time travel to a specific version.
    AtVersion(u64),
    /// Load at `from`, then incrementally update to `to`.
    Incremental { from: u64, to: u64 },
}

// ---- TestTableBuilder -------------------------------------------------------

/// Builds an in-memory Delta table with the requested configuration.
///
/// Uses kernel's full write path (CreateTable, Transaction, write_parquet) to produce
/// correct tables with proper column mapping, stats, and protocol handling. The builder
/// does NOT expose a snapshot -- the test loads one using its own kernel types.
pub struct TestTableBuilder {
    log_state: LogState,
    features: FeatureSet,
    schema: Option<SchemaRef>,
    num_data_files: usize,
    rows_per_file: usize,
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
            num_data_files: 1,
            rows_per_file: 10,
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

    /// Set number of data files per commit and rows per file.
    pub fn data(mut self, files_per_commit: usize, rows_per_file: usize) -> Self {
        self.num_data_files = files_per_commit;
        self.rows_per_file = rows_per_file;
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

        // -- Version 0: CreateTable --
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

        // -- Data commits (versions 1..N) --
        let total = self.log_state.total_versions();
        for v in 1..total {
            let result = write_data_commit(
                snapshot.clone(),
                &engine,
                self.num_data_files,
                self.rows_per_file,
                v,
            )
            .await?;
            snapshot = result.unwrap_committed().post_commit_snapshot().unwrap().clone();
        }

        // -- Checkpoints --
        match &self.log_state {
            LogState::CheckpointV1 { checkpoint_at, .. }
            | LogState::CheckpointV2 { checkpoint_at, .. }
            | LogState::CheckpointAndCommits { checkpoint_at, .. } => {
                let snap = Snapshot::builder_for(table_root)
                    .at_version(*checkpoint_at)
                    .build(engine.as_ref())?;
                snap.checkpoint(engine.as_ref())?;
            }
            _ => {}
        }

        // -- CRC files --
        if let LogState::WithCrc { crc_at, .. } = &self.log_state {
            write_crc(*crc_at, &store, table_root).await?;
        }

        Ok(TestTable {
            store,
            table_root: table_root.to_string(),
        })
    }
}

// ---- Data commit via kernel write path --------------------------------------

/// Write a data commit using kernel's transaction + write_parquet path.
/// Produces `num_files` parquet files with `rows_per_file` rows each.
async fn write_data_commit(
    snapshot: Arc<Snapshot>,
    engine: &Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    num_files: usize,
    rows_per_file: usize,
    version: u64,
) -> DeltaResult<delta_kernel::transaction::CommitResult> {
    let arrow_schema: ArrowSchema = TryFromKernel::try_from_kernel(snapshot.schema().as_ref())
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let write_context = txn.get_write_context();

    for file_idx in 0..num_files {
        let base = (version as i32 * 1000) + (file_idx as i32 * 100);
        let columns: Vec<ArrayRef> = arrow_schema
            .fields()
            .iter()
            .map(|f| generate_column(f.data_type(), rows_per_file, base))
            .collect();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
        let add_files = engine
            .write_parquet(
                &ArrowEngineData::new(batch),
                &write_context,
                HashMap::new(),
            )
            .await?;
        txn.add_files(add_files);
    }

    txn.commit(engine.as_ref())
}

// ---- CRC file generation ----------------------------------------------------

/// Write a CRC file at the given version, extracting P&M from the version 0 commit.
pub async fn write_crc(
    version: u64,
    store: &Arc<DynObjectStore>,
    table_root: &str,
) -> DeltaResult<()> {
    let commit_path = delta_path_for_version(0, "json");
    let resolved = crate::resolve_table_path(table_root, &commit_path)
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let data = store
        .get(&resolved)
        .await
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let bytes = data
        .bytes()
        .await
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    let mut protocol_json = json!(null);
    let mut metadata_json = json!(null);
    for line in text.lines() {
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
        if let Some(p) = v.get("protocol") {
            protocol_json = p.clone();
        }
        if let Some(m) = v.get("metaData") {
            metadata_json = m.clone();
        }
    }

    let crc = json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": metadata_json,
        "protocol": protocol_json,
    });

    let crc_path = delta_path_for_version(version, "crc");
    let resolved = crate::resolve_table_path(table_root, &crc_path)
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    store
        .put(&resolved, serde_json::to_string(&crc).expect("json").into())
        .await
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    Ok(())
}

// ---- TestTable --------------------------------------------------------------

/// A built test table backed by an in-memory object store.
///
/// Exposes only the store and table root URL. Does NOT expose kernel types like
/// `Snapshot` or `Engine` -- the test creates those from its own kernel imports.
/// This avoids type mismatches between the test crate's kernel and test_utils's kernel.
pub struct TestTable {
    store: Arc<DynObjectStore>,
    table_root: String,
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

    /// Create a `DefaultEngine` backed by this table's store.
    ///
    /// Returns the engine from `test_utils`'s `delta_kernel`. For unit tests inside
    /// `kernel/src/`, use `DefaultEngineBuilder::new(table.store().clone()).build()`
    /// instead to get the correct crate-local engine type.
    pub fn engine(&self) -> DefaultEngine<TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(self.store.clone()).build()
    }
}

// ---- Helpers: schema and data generation ------------------------------------

/// Default schema with all Delta primitive types (except TimestampNtz which auto-enables
/// a table feature, and nested types which are a separate concern).
fn default_schema() -> SchemaRef {
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

/// Generate a single column of data based on its Arrow type.
/// Data is deterministic: values are derived from `base` (version * 1000 + file_idx * 100).
fn generate_column(arrow_type: &ArrowDataType, rows: usize, base: i32) -> ArrayRef {
    match arrow_type {
        ArrowDataType::Boolean => {
            let values: Vec<bool> = (0..rows).map(|i| (base as usize + i) % 2 == 0).collect();
            Arc::new(BooleanArray::from(values))
        }
        ArrowDataType::Int8 => {
            let values: Vec<i8> = (0..rows)
                .map(|i| ((base + i as i32) % 120) as i8)
                .collect();
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
            let values: Vec<i64> = (0..rows)
                .map(|i| (base + i as i32) as i64 * 1000)
                .collect();
            Arc::new(Int64Array::from(values))
        }
        ArrowDataType::Float32 => {
            let values: Vec<f32> = (0..rows)
                .map(|i| base as f32 + i as f32 * 0.5)
                .collect();
            Arc::new(Float32Array::from(values))
        }
        ArrowDataType::Float64 => {
            let values: Vec<f64> = (0..rows)
                .map(|i| base as f64 + i as f64 * 0.25)
                .collect();
            Arc::new(Float64Array::from(values))
        }
        ArrowDataType::Utf8 => {
            let values: Vec<String> = (0..rows)
                .map(|i| format!("val_{}", base + i as i32))
                .collect();
            Arc::new(StringArray::from(
                values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::Snapshot;

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

    #[test]
    fn test_with_column_mapping() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .features(FeatureSet::new().column_mapping("name"))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let protocol = snap.table_configuration().protocol();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        Ok(())
    }

    #[test]
    fn test_with_ict() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .features(FeatureSet::new().ict())
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let protocol = snap.table_configuration().protocol();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        Ok(())
    }

    #[test]
    fn test_combined_features() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .features(
                FeatureSet::new()
                    .column_mapping("name")
                    .ict()
                    .v2_checkpoint(),
            )
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let protocol = snap.table_configuration().protocol();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        Ok(())
    }

    #[test]
    fn test_time_travel() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::commits(5))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root())
            .at_version(2)
            .build(&engine)?;
        assert_eq!(snap.version(), 2);
        Ok(())
    }

    #[test]
    fn test_incremental() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::commits(5))
            .build()?;
        let engine = table.engine();
        let base = Snapshot::builder_for(table.table_root())
            .at_version(1)
            .build(&engine)?;
        let updated = Snapshot::builder_from(base).at_version(3).build(&engine)?;
        assert_eq!(updated.version(), 3);
        Ok(())
    }

    #[test]
    fn test_checkpoint_v1() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::checkpoint_v1(2, 4))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }

    #[test]
    fn test_checkpoint_v2() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::checkpoint_v2(2, 4))
            .features(FeatureSet::new().v2_checkpoint())
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }

    #[test]
    fn test_checkpoint_and_commits() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::checkpoint_and_commits(2, 2))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 4);
        Ok(())
    }

    #[test]
    fn test_scan_with_data() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::commits(2))
            .data(2, 5)
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let scan = snap.scan_builder().build()?;
        let engine_arc: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let batches = crate::read_scan(&scan, engine_arc)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
        Ok(())
    }

    #[test]
    fn test_scan_with_column_mapping() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::commits(2))
            .features(FeatureSet::new().column_mapping("name"))
            .data(1, 5)
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let scan = snap.scan_builder().build()?;
        let engine_arc: Arc<dyn delta_kernel::Engine> =
            Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
        let batches = crate::read_scan(&scan, engine_arc)?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
        Ok(())
    }

    #[test]
    fn test_with_crc() -> DeltaResult<()> {
        let table = TestTableBuilder::new()
            .log_state(LogState::with_crc(2, 4))
            .build()?;
        let engine = table.engine();
        let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;
        assert_eq!(snap.version(), 3);
        Ok(())
    }
}
