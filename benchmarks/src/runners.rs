//! Benchmark runners for executing Delta table operations.
//!
//! Each runner holds all the state required for its workload (e.g. read metadata needs
//! pre-built snapshots and a config) so that `execute` measures only the operation itself.
//! Results are discarded for benchmarking purposes.
//!
//! Engine and snapshot construction is handled based on `TableInfo`:
//! - S3 tables (`table_path` with s3://): credentials from `AWS_*` env vars
//! - Local tables: local filesystem engine

use std::hint::black_box;
use std::sync::Arc;

use delta_kernel::expressions::PredicateRef;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata, StatsOptions};
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_unity_catalog::snapshot_builder_from_load_table;
use delta_kernel_workloads::models::{
    ReadOperation, ReadSpec, SnapshotConstructionSpec, TableInfo, TimeTravel,
};
use delta_kernel_workloads::predicate_parser::parse_predicate;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};
use url::Url;

use crate::registry::{ParallelScan, ReadConfig};

pub trait WorkloadRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn name(&self) -> &str;
}

/// Builds `{table}/{case}` from `table_info.name` and `case_name`.
pub fn benchmark_name(table_info: &TableInfo, case_name: &str) -> String {
    format!("{}/{case_name}", table_info.name)
}

/// Builds `{table}/{case}/{config}` from `table_info.name`, `case_name`, and `config_name`.
pub fn configured_benchmark_name(
    table_info: &TableInfo,
    case_name: &str,
    config_name: &str,
) -> String {
    format!("{}/{case_name}/{config_name}", table_info.name)
}

fn build_engine(
    store: Arc<delta_kernel::object_store::DynObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Arc<dyn Engine> {
    let executor = TokioMultiThreadExecutor::new(runtime.handle().clone());
    Arc::new(
        DefaultEngine::builder(store)
            .with_task_executor(Arc::new(executor))
            .build(),
    )
}

/// Determines how a snapshot is loaded. Built once at setup via [`resolve_snapshot_strategy`],
/// then used by runners to construct snapshots.
enum SnapshotStrategy {
    /// Standard snapshot builder (local, S3, or UC-managed non-catalog-managed tables).
    Standard { url: Url },
    /// Catalog-managed table: snapshot loaded via the UC Delta-Tables API. The three-part table
    /// name is resolved via `UCClient::load_table`, whose response is turned into a snapshot
    /// builder by `snapshot_builder_from_load_table`.
    CatalogManaged {
        table_name: String,
        client: Box<UCClient>,
    },
}

impl SnapshotStrategy {
    /// Builds a snapshot using this strategy.
    fn load_snapshot(
        &self,
        engine: &dyn Engine,
        runtime: &tokio::runtime::Runtime,
        time_travel: Option<&TimeTravel>,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        match self {
            SnapshotStrategy::Standard { url } => {
                let mut builder = Snapshot::builder_for(url.clone());
                if let Some(tt) = time_travel {
                    builder = builder.at_version(tt.as_version()?);
                }
                Ok(builder.build(engine)?)
            }
            SnapshotStrategy::CatalogManaged { table_name, client } => {
                let (catalog, schema, table) = parse_three_part_name(table_name)?;
                let resp = runtime
                    .block_on(client.load_table(catalog, schema, table))
                    .map_err(|e| format!("Catalog load_table failed: {e}"))?;
                let mut builder = snapshot_builder_from_load_table(&resp)?;
                if let Some(tt) = time_travel {
                    builder = builder.at_version(tt.as_version()?);
                }
                Ok(builder.build(engine)?)
            }
        }
    }
}

/// Splits a `"catalog.schema.table"` name into its three parts.
fn parse_three_part_name(name: &str) -> Result<(&str, &str, &str), Box<dyn std::error::Error>> {
    match name.split('.').collect::<Vec<_>>()[..] {
        [catalog, schema, table] => Ok((catalog, schema, table)),
        _ => Err(
            format!("expected a three-part table name 'catalog.schema.table', got: {name}").into(),
        ),
    }
}

/// Resolves the engine and snapshot strategy from a [`TableInfo`].
///
/// For catalog-managed tables (`catalog_info` is present), credentials are vended via `UCClient`
/// and the snapshot is loaded through the UC Delta-Tables API (`load_table`). For non-UC tables,
/// the engine is built from env vars (`AWS_*` for S3, local filesystem otherwise).
fn resolve_snapshot_strategy(
    table_info: &TableInfo,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<(Arc<dyn Engine>, SnapshotStrategy), Box<dyn std::error::Error>> {
    let Some(cm) = &table_info.catalog_info else {
        let url = table_info.resolved_table_root();
        let engine = resolve_engine_for_url(&url, runtime)?;
        return Ok((engine, SnapshotStrategy::Standard { url }));
    };

    let endpoint = std::env::var("UC_WORKSPACE").map_err(|_| "UC_WORKSPACE required")?;
    let token = std::env::var("UC_TOKEN").map_err(|_| "UC_TOKEN required")?;
    let config = ClientConfig::build(&endpoint, &token).build()?;
    let client = Box::new(UCClient::new(config)?);

    let (catalog, schema, table) = parse_three_part_name(&cm.table_name)?;
    let creds = runtime
        .block_on(client.get_table_credentials(catalog, schema, table, Operation::Read))
        .map_err(|e| format!("Credential vending failed: {e}"))?;

    let resp = runtime
        .block_on(client.load_table(catalog, schema, table))
        .map_err(|e| format!("load_table failed: {e}"))?;
    let table_url = Url::parse(&resp.metadata.location)?;

    let opts = object_store_options(&creds.storage_credentials);
    let (store, _) = delta_kernel::object_store::parse_url_opts(&table_url, opts)?;
    let engine = build_engine(store.into(), runtime);

    Ok((
        engine,
        SnapshotStrategy::CatalogManaged {
            table_name: cm.table_name.clone(),
            client,
        },
    ))
}

/// Maps vended UC storage credentials into `object_store` option keys.
fn object_store_options(
    creds: &[unity_catalog_delta_client_api::StorageCredential],
) -> Vec<(String, String)> {
    let mut opts = Vec::new();
    for cred in creds {
        for (key, value) in &cred.config {
            let mapped = match key.as_str() {
                "s3.access-key-id" => "access_key_id",
                "s3.secret-access-key" => "secret_access_key",
                "s3.session-token" => "session_token",
                other => other,
            };
            opts.push((mapped.to_string(), value.clone()));
        }
    }
    if let Ok(region) = std::env::var("UC_AWS_REGION") {
        opts.push(("region".to_string(), region));
    }
    opts
}

/// Builds an engine from the table URL scheme and env vars (S3 or local).
fn resolve_engine_for_url(
    url: &Url,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<Arc<dyn Engine>, Box<dyn std::error::Error>> {
    match url.scheme() {
        "s3" | "s3a" => {
            let region =
                std::env::var("AWS_REGION").map_err(|_| "AWS_REGION required for S3 tables")?;
            let mut opts: Vec<(&str, String)> = vec![("region", region)];
            for (env_key, opt_key) in [
                ("AWS_ACCESS_KEY_ID", "access_key_id"),
                ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
                ("AWS_SESSION_TOKEN", "session_token"),
            ] {
                if let Ok(v) = std::env::var(env_key) {
                    opts.push((opt_key, v));
                }
            }
            let (store, _) = delta_kernel::object_store::parse_url_opts(url, opts)?;
            Ok(build_engine(store.into(), runtime))
        }
        "file" => Ok(build_engine(Arc::new(LocalFileSystem::new()), runtime)),
        scheme => Err(format!(
            "Unsupported scheme '{scheme}': only s3://, s3a://, and file:// are supported"
        )
        .into()),
    }
}

pub struct ReadMetadataRunner {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    name: String,
    config: ReadConfig,
    thread_pool: Option<rayon::ThreadPool>, /* None for serial configuration, Some for parallel
                                             * configuration */
    predicate: Option<PredicateRef>,
}

impl ReadMetadataRunner {
    pub fn setup(
        name: String,
        read_spec: &ReadSpec,
        config: ReadConfig,
        table_info: &TableInfo,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (engine, strategy) = resolve_snapshot_strategy(table_info, runtime.clone())?;
        let snapshot =
            strategy.load_snapshot(engine.as_ref(), &runtime, read_spec.time_travel.as_ref())?;

        let predicate = read_spec
            .predicate
            .as_deref()
            .map(|sql| parse_predicate(sql, &snapshot.schema()))
            .transpose()?
            .map(Arc::new);

        let thread_pool = match &config.parallel_scan {
            ParallelScan::Enabled { num_threads } => {
                if *num_threads == 0 {
                    return Err("num_threads in ReadConfig must be greater than 0".into());
                }
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(*num_threads)
                    .build()?;
                Some(thread_pool)
            }
            ParallelScan::Disabled => None,
        };

        Ok(Self {
            snapshot,
            engine,
            name,
            config,
            thread_pool,
            predicate,
        })
    }

    fn execute_serial(&self) -> Result<(), Box<dyn std::error::Error>> {
        let scan = self
            .snapshot
            .clone()
            .scan_builder()
            .with_predicate(self.predicate.clone())
            .with_stats(StatsOptions::all_struct())
            .build()?;
        let metadata_iter = scan.scan_metadata(self.engine.as_ref())?;
        for result in metadata_iter {
            black_box(result?);
        }
        Ok(())
    }

    fn execute_parallel(&self) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self
            .thread_pool
            .as_ref()
            .ok_or("thread_pool must be Some for parallel execution")?;

        let scan = self
            .snapshot
            .clone()
            .scan_builder()
            .with_predicate(self.predicate.clone())
            .with_stats(StatsOptions::all_struct())
            .build()?;

        let mut phase1 = scan.parallel_scan_metadata(self.engine.clone())?;
        for result in phase1.by_ref() {
            black_box(result?);
        }

        match phase1.finish()? {
            AfterSequentialScanMetadata::Done => {}
            AfterSequentialScanMetadata::Parallel { state, files } => {
                let num_threads = pool.current_num_threads();
                let files_per_worker = files.len().div_ceil(num_threads);

                let partitions: Vec<_> = files
                    .chunks(files_per_worker)
                    .map(|chunk| chunk.to_vec())
                    .collect();

                let state = Arc::new(*state);

                pool.scope(|s| {
                    for partition_files in partitions {
                        let engine = self.engine.clone();
                        let state = state.clone();

                        s.spawn(move |_| {
                            if partition_files.is_empty() {
                                return;
                            }

                            let parallel =
                                ParallelScanMetadata::try_new(engine, state, partition_files)
                                    .expect("Failed to create ParallelScanMetadata");
                            for result in parallel {
                                black_box(result.expect("Parallel scan error"));
                            }
                        });
                    }
                });
            }
        }
        Ok(())
    }
}

impl WorkloadRunner for ReadMetadataRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.config.parallel_scan {
            ParallelScan::Disabled => self.execute_serial(),
            ParallelScan::Enabled { .. } => self.execute_parallel(),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Factory function that creates the appropriate read runner for a given operation and config.
/// `name` is the full benchmark identifier (`{table_name}/{case}/{config}`).
pub fn create_read_runner(
    name: String,
    read_spec: &ReadSpec,
    operation: ReadOperation,
    config: ReadConfig,
    table_info: &TableInfo,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<Box<dyn WorkloadRunner>, Box<dyn std::error::Error>> {
    match operation {
        ReadOperation::ReadMetadata => Ok(Box::new(ReadMetadataRunner::setup(
            name, read_spec, config, table_info, runtime,
        )?)),
        ReadOperation::ReadData => Err("ReadDataRunner not yet implemented".into()),
    }
}

pub struct SnapshotConstructionRunner {
    engine: Arc<dyn Engine>,
    runtime: Arc<tokio::runtime::Runtime>,
    snapshot_strategy: SnapshotStrategy,
    time_travel: Option<TimeTravel>,
    name: String,
}

impl SnapshotConstructionRunner {
    pub fn setup(
        name: String,
        snapshot_spec: &SnapshotConstructionSpec,
        table_info: &TableInfo,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (engine, snapshot_strategy) = resolve_snapshot_strategy(table_info, runtime.clone())?;

        Ok(Self {
            engine,
            runtime,
            snapshot_strategy,
            time_travel: snapshot_spec.time_travel.clone(),
            name,
        })
    }
}

impl WorkloadRunner for SnapshotConstructionRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = self.snapshot_strategy.load_snapshot(
            self.engine.as_ref(),
            &self.runtime,
            self.time_travel.as_ref(),
        )?;
        black_box(snapshot);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::slice;
    use std::sync::LazyLock;

    use delta_kernel_workloads::models::{ReadSpec, Spec, TableInfo, TimeTravel, Workload};

    use super::*;
    use crate::registry::BenchRegistry;
    use crate::utils::load_all_workloads;

    fn test_runtime() -> Arc<tokio::runtime::Runtime> {
        static RT: LazyLock<Arc<tokio::runtime::Runtime>> = LazyLock::new(|| {
            Arc::new(tokio::runtime::Runtime::new().expect("failed to create runtime"))
        });
        RT.clone()
    }

    fn test_table_info() -> TableInfo {
        let path = format!(
            "{}/../kernel/tests/data/basic_partitioned",
            env!("CARGO_MANIFEST_DIR")
        );
        let json = format!(
            r#"{{
                "name": "basic_partitioned",
                "description": "basic partitioned table for testing",
                "tablePath": "{}",
                "schema": {{
                    "type": "struct",
                    "fields": [
                        {{"name": "letter",  "type": "string", "nullable": true, "metadata": {{}}}},
                        {{"name": "number",  "type": "long",   "nullable": true, "metadata": {{}}}},
                        {{"name": "a_float", "type": "double", "nullable": true, "metadata": {{}}}}
                    ]
                }},
                "protocol": {{"minReaderVersion": 1, "minWriterVersion": 2}},
                "logInfo": {{
                    "numAddFiles": 6,
                    "numRemoveFiles": 0,
                    "sizeInBytes": 4505,
                    "numCommits": 2,
                    "numActions": 10
                }},
                "properties": {{}},
                "dataLayout": {{"numPartitionColumns": 1, "numDistinctPartitions": 5}},
                "tags": []
            }}"#,
            Url::from_file_path(path).unwrap()
        );
        serde_json::from_str(&json).expect("failed to build test TableInfo")
    }

    fn test_read_spec() -> ReadSpec {
        ReadSpec {
            time_travel: None,
            predicate: None,
            columns: None,
            expected: None,
        }
    }

    fn serial_config() -> ReadConfig {
        ReadConfig {
            name: "serial".to_string(),
            parallel_scan: ParallelScan::Disabled,
        }
    }

    fn parallel_config() -> ReadConfig {
        ReadConfig {
            name: "parallel2".to_string(),
            parallel_scan: ParallelScan::Enabled { num_threads: 2 },
        }
    }

    #[test]
    fn configured_benchmark_name_uses_human_readable_table_name() {
        let mut table_info = test_table_info();
        table_info.name = "Human readable table".to_string();

        assert_eq!(
            configured_benchmark_name(&table_info, "testCase", "serial"),
            "Human readable table/testCase/serial"
        );
    }

    #[test]
    fn test_read_metadata_runner_serial() {
        let runner = ReadMetadataRunner::setup(
            configured_benchmark_name(&test_table_info(), "testCase", "serial"),
            &test_read_spec(),
            serial_config(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(runner.name(), "basic_partitioned/testCase/serial");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_read_metadata_runner_parallel() {
        let runner = ReadMetadataRunner::setup(
            configured_benchmark_name(&test_table_info(), "testCase", "parallel2"),
            &test_read_spec(),
            parallel_config(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(runner.name(), "basic_partitioned/testCase/parallel2");
        assert!(runner.execute().is_ok());
    }

    fn test_snapshot_spec() -> SnapshotConstructionSpec {
        SnapshotConstructionSpec {
            time_travel: None,
            expected: None,
        }
    }

    #[test]
    fn test_snapshot_construction_runner_setup() {
        let runner = SnapshotConstructionRunner::setup(
            benchmark_name(&test_table_info(), "testCase"),
            &test_snapshot_spec(),
            &test_table_info(),
            test_runtime(),
        );
        assert!(runner.is_ok());
    }

    #[test]
    fn test_snapshot_construction_runner_name() {
        let runner = SnapshotConstructionRunner::setup(
            benchmark_name(&test_table_info(), "testCase"),
            &test_snapshot_spec(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(runner.name(), "basic_partitioned/testCase");
    }

    #[test]
    fn test_snapshot_construction_runner_execute() {
        let runner = SnapshotConstructionRunner::setup(
            benchmark_name(&test_table_info(), "testCase"),
            &test_snapshot_spec(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }

    /// Guards the checked-in `bench-registry.json` so a malformed edit or a duplicate config name
    /// fails fast in `cargo test` rather than only when the harness runs.
    #[test]
    fn checked_in_registry_loads_and_validates() {
        let path = format!("{}/bench-registry.json", env!("CARGO_MANIFEST_DIR"));
        let registry = BenchRegistry::load_from_path(Path::new(&path))
            .expect("checked-in bench-registry.json must load");
        let mut table_info = test_table_info();
        table_info.table_info_dir = PathBuf::from("10kAdds0CommitsSinceChkpt1V2Chkpt");
        let workload = Workload {
            table_info,
            case_name: "readMetadataLatest".to_string(),
            spec: Spec::Read(test_read_spec()),
        };
        registry
            .validate(slice::from_ref(&workload))
            .expect("checked-in bench-registry.json must validate");
        let read = registry.read_configs(&workload).unwrap();
        assert_eq!(
            read.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["serial", "parallel2"]
        );
    }

    /// Every registry key must name a real read workload. A key miss silently falls back to the
    /// serial default, so a stale/typo'd key would otherwise be invisible until the harness runs.
    /// Skips when the downloaded workload archive is absent (e.g. offline unit runs).
    #[test]
    fn checked_in_registry_keys_match_real_workloads() {
        let Ok(workloads) = load_all_workloads() else {
            return; // workload archive not downloaded -- nothing to cross-check
        };
        let workload_by_key: HashMap<(&str, &str), &Workload> = workloads
            .iter()
            .map(|w| {
                (
                    (
                        w.table_info
                            .registry_table_key()
                            .expect("loaded workloads must have a registry table key"),
                        w.case_name.as_str(),
                    ),
                    w,
                )
            })
            .collect();

        let path = format!("{}/bench-registry.json", env!("CARGO_MANIFEST_DIR"));
        let registry = BenchRegistry::load_from_path(Path::new(&path))
            .expect("checked-in bench-registry.json must load");
        registry
            .validate(&workloads)
            .expect("registry configs must match workload types");

        for (table, case) in registry.keys() {
            workload_by_key.get(&(table, case)).unwrap_or_else(|| {
                panic!("registry key '{table}/{case}' matches no workload (stale or typo'd?)")
            });
        }
    }

    #[test]
    fn test_create_read_runner_read_metadata() {
        let runner = create_read_runner(
            configured_benchmark_name(&test_table_info(), "testCase", "serial"),
            &test_read_spec(),
            ReadOperation::ReadMetadata,
            serial_config(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("create_read_runner should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_read_metadata_runner_with_valid_predicate() {
        let mut spec = test_read_spec();
        spec.predicate = Some("letter = 'a'".to_string());
        let runner = ReadMetadataRunner::setup(
            configured_benchmark_name(&test_table_info(), "test_case", "serial"),
            &spec,
            serial_config(),
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_read_metadata_runner_with_invalid_predicate() {
        let mut spec = test_read_spec();
        spec.predicate = Some("a LIKE '%foo'".to_string());
        let result = ReadMetadataRunner::setup(
            configured_benchmark_name(&test_table_info(), "test_case", "serial"),
            &spec,
            serial_config(),
            &test_table_info(),
            test_runtime(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_read_runner_read_data_unimplemented() {
        let result = create_read_runner(
            configured_benchmark_name(&test_table_info(), "testCase", "serial"),
            &test_read_spec(),
            ReadOperation::ReadData,
            serial_config(),
            &test_table_info(),
            test_runtime(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_engine_unsupported_scheme() {
        let url = Url::parse("gs://bucket/table").unwrap();
        let result = resolve_engine_for_url(&url, test_runtime());
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_construction_with_time_travel() {
        let spec = SnapshotConstructionSpec {
            time_travel: Some(TimeTravel::Version { version: 0 }),
            expected: None,
        };
        let runner = SnapshotConstructionRunner::setup(
            benchmark_name(&test_table_info(), "testCase"),
            &spec,
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }
}
