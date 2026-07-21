//! Benchmark runners for executing Delta table operations.
//!
//! Each runner holds all the state required for its workload (e.g. read metadata needs
//! pre-built snapshots and a config) so that `execute` measures only the operation itself.
//! Results are discarded for benchmarking purposes.
//!
//! Engine and snapshot construction is handled based on `TableInfo`:
//! - UC tables (`catalog_info`): UC-vended credentials; catalog-managed tables use
//!   `UCKernelClient::load_snapshot`, others use the standard snapshot builder
//! - S3 tables (`table_path` with s3://): credentials from `AWS_*` env vars
//! - Local tables: local filesystem engine

use std::hint::black_box;
use std::sync::Arc;

use delta_kernel::expressions::PredicateRef;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata, StatsOptions};
use delta_kernel::snapshot::IncrementalReplay;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_unity_catalog::UCKernelClient;
use delta_kernel_workloads::models::{
    ReadOperation, ReadSpec, SnapshotConstructionSpec, TableInfo, TimeTravel,
};
use delta_kernel_workloads::predicate_parser::parse_predicate;
use unity_catalog_delta_client_api::{Error as UcApiError, Operation};
use unity_catalog_delta_rest_client::{
    ClientConfig, Error as UcRestError, UCClient, UCCommitsRestClient,
};
use url::Url;

use crate::registry::{IncrementalCrcReplay, ParallelScan, ReadConfig, SnapshotBuilderConfig};

/// Delta table property indicating catalog-managed support.
const CATALOG_MANAGED_PROPERTY: &str = "delta.feature.catalogManaged";

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

/// Builds a snapshot-construction benchmark name.
///
/// `table_info` supplies the human-readable table name, `case_name` identifies the workload spec,
/// and `config_name` supplies the optional registered-config suffix. The returned name remains
/// unsuffixed when `config_name` is `None`, preserving legacy names for unregistered workloads.
pub fn snapshot_benchmark_name(
    table_info: &TableInfo,
    case_name: &str,
    config_name: Option<&str>,
) -> String {
    match config_name {
        Some(config_name) => configured_benchmark_name(table_info, case_name, config_name),
        None => benchmark_name(table_info, case_name),
    }
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
    /// Catalog-managed table: snapshot loaded via `UCKernelClient::load_snapshot`.
    CatalogManaged {
        table_id: String,
        table_uri: String,
        commits_client: Box<UCCommitsRestClient>,
    },
}

impl SnapshotStrategy {
    /// Builds a snapshot using this strategy.
    fn load_snapshot(
        &self,
        engine: &dyn Engine,
        runtime: &tokio::runtime::Runtime,
        time_travel: Option<&TimeTravel>,
        incremental_replay: IncrementalReplay,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        match self {
            SnapshotStrategy::Standard { url } => {
                let mut builder = Snapshot::builder_for(url.clone())
                    .with_incremental_crc_replay(incremental_replay);
                if let Some(tt) = time_travel {
                    builder = builder.at_version(tt.as_version()?);
                }
                Ok(builder.build(engine)?)
            }
            SnapshotStrategy::CatalogManaged {
                table_id,
                table_uri,
                commits_client,
            } => {
                if incremental_replay != IncrementalReplay::Disabled {
                    return Err(
                        "Incremental CRC replay is unsupported for catalog-managed snapshots"
                            .into(),
                    );
                }
                let catalog = UCKernelClient::new(commits_client.as_ref());
                let result = match time_travel {
                    Some(tt) => {
                        let version = tt.as_version()?;
                        runtime.block_on(
                            catalog.load_snapshot_at(table_id, table_uri, version, engine),
                        )
                    }
                    None => runtime.block_on(catalog.load_snapshot(table_id, table_uri, engine)),
                };
                result.map_err(|e| format!("Catalog snapshot failed: {e}").into())
            }
        }
    }
}

impl From<IncrementalCrcReplay> for IncrementalReplay {
    fn from(value: IncrementalCrcReplay) -> Self {
        match value {
            IncrementalCrcReplay::Disabled => Self::Disabled,
            IncrementalCrcReplay::UpToCommits { num_commits } => Self::UpToCommits(num_commits),
            IncrementalCrcReplay::Unlimited => Self::Unlimited,
        }
    }
}

fn validate_snapshot_versions(
    snapshot_spec: &SnapshotConstructionSpec,
    snapshot_builder: &SnapshotBuilderConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let SnapshotBuilderConfig::From { version } = snapshot_builder else {
        return Ok(());
    };
    if let Some(time_travel) = snapshot_spec.time_travel.as_ref() {
        validate_base_precedes_target(*version, time_travel.as_version()?)?;
    }
    Ok(())
}

fn validate_snapshot_strategy(
    snapshot_builder: &SnapshotBuilderConfig,
    incremental_crc_replay: IncrementalCrcReplay,
    is_catalog_managed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if is_catalog_managed && incremental_crc_replay != IncrementalCrcReplay::Disabled {
        return Err("Incremental CRC replay is unsupported for catalog-managed snapshots".into());
    }
    if is_catalog_managed && matches!(snapshot_builder, SnapshotBuilderConfig::From { .. }) {
        return Err("Snapshot::builder_from is unsupported for catalog-managed benchmarks".into());
    }
    Ok(())
}

fn validate_base_precedes_target(
    base_version: u64,
    target_version: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if base_version >= target_version {
        return Err(format!(
            "Base snapshot version {base_version} must be below target version {target_version}"
        )
        .into());
    }
    Ok(())
}

/// Resolves engine credentials and snapshot strategy from a [`TableInfo`].
///
/// For UC-managed tables (`catalog_info` is present), credentials are vended via
/// `UCClient`. The `delta.feature.catalogManaged` property then determines whether to use
/// `UCKernelClient` (catalog-managed) or the standard snapshot builder.
///
/// For non-UC tables, the engine is built from env vars (`AWS_*` for S3, local filesystem
/// otherwise).
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
    let client = UCClient::new(config.clone())?;

    let result: Result<_, UcRestError> = runtime.block_on(async {
        let table = client.get_table(&cm.table_name).await?;
        let creds = client
            .get_credentials(&table.table_id, Operation::Read)
            .await?;
        let aws = creds
            .aws_temp_credentials
            .ok_or(UcApiError::UnsupportedOperation(
                // TODO(#2305): support non-AWS credential types
                "Credential vending returned no AWS credentials".into(),
            ))?;
        Ok((
            table.table_id,
            table.storage_location,
            table.properties,
            aws,
        ))
    });
    let (table_id, table_uri, uc_properties, aws) = result?;

    let table_url = Url::parse(&table_uri)?;
    let region = std::env::var("AWS_REGION").map_err(|_| "AWS_REGION required")?;
    let options = [
        ("region", region.as_str()),
        ("access_key_id", aws.access_key_id.as_str()),
        ("secret_access_key", aws.secret_access_key.as_str()),
        ("session_token", aws.session_token.as_str()),
    ];
    let (store, _) = delta_kernel::object_store::parse_url_opts(&table_url, options)?;
    let engine = build_engine(store.into(), runtime);

    let is_catalog_managed = uc_properties
        .get(CATALOG_MANAGED_PROPERTY)
        .is_some_and(|v| v == "supported");

    let strategy = if is_catalog_managed {
        let commits_client = Box::new(UCCommitsRestClient::new(config)?);
        SnapshotStrategy::CatalogManaged {
            table_id,
            table_uri,
            commits_client,
        }
    } else {
        SnapshotStrategy::Standard { url: table_url }
    };

    Ok((engine, strategy))
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
        let snapshot = strategy.load_snapshot(
            engine.as_ref(),
            &runtime,
            read_spec.time_travel.as_ref(),
            IncrementalReplay::Disabled,
        )?;

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

enum SnapshotConstructionSource {
    Fresh(SnapshotStrategy),
    Existing(Arc<Snapshot>),
}

pub struct SnapshotConstructionRunner {
    engine: Arc<dyn Engine>,
    runtime: Arc<tokio::runtime::Runtime>,
    source: SnapshotConstructionSource,
    time_travel: Option<TimeTravel>,
    incremental_replay: IncrementalReplay,
    name: String,
}

impl SnapshotConstructionRunner {
    /// Prepares a snapshot-construction benchmark and state excluded from timing.
    ///
    /// `name` is the Criterion benchmark identifier, `snapshot_spec` selects the target version,
    /// `snapshot_builder` selects fresh or incremental construction, `incremental_crc_replay`
    /// selects the CRC replay policy, `table_info` identifies the table, and `runtime` executes
    /// asynchronous setup work. Incremental benchmarks load their base snapshot during setup.
    ///
    /// Returns a runner whose [`WorkloadRunner::execute`] method performs only the measured
    /// snapshot construction.
    ///
    /// Returns an error when the table cannot be resolved, incremental construction is
    /// unsupported, or the configured base version does not precede the target version.
    pub fn setup(
        name: String,
        snapshot_spec: &SnapshotConstructionSpec,
        snapshot_builder: SnapshotBuilderConfig,
        incremental_crc_replay: IncrementalCrcReplay,
        table_info: &TableInfo,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        validate_snapshot_versions(snapshot_spec, &snapshot_builder)?;
        let (engine, snapshot_strategy) = resolve_snapshot_strategy(table_info, runtime.clone())?;
        let incremental_replay = incremental_crc_replay.into();
        let is_catalog_managed =
            matches!(&snapshot_strategy, SnapshotStrategy::CatalogManaged { .. });
        validate_snapshot_strategy(
            &snapshot_builder,
            incremental_crc_replay,
            is_catalog_managed,
        )?;

        let source = match snapshot_builder {
            SnapshotBuilderConfig::For => SnapshotConstructionSource::Fresh(snapshot_strategy),
            SnapshotBuilderConfig::From { version } => {
                let base_time_travel = TimeTravel::Version {
                    version: version.try_into()?,
                };
                let existing_snapshot = snapshot_strategy.load_snapshot(
                    engine.as_ref(),
                    &runtime,
                    Some(&base_time_travel),
                    incremental_replay,
                )?;
                if snapshot_spec.time_travel.is_none() {
                    let target_version = Snapshot::builder_from(existing_snapshot.clone())
                        .with_incremental_crc_replay(incremental_replay)
                        .build(engine.as_ref())?
                        .version();
                    validate_base_precedes_target(version, target_version)?;
                }
                SnapshotConstructionSource::Existing(existing_snapshot)
            }
        };

        Ok(Self {
            engine,
            runtime,
            source,
            time_travel: snapshot_spec.time_travel.clone(),
            incremental_replay,
            name,
        })
    }
}

impl WorkloadRunner for SnapshotConstructionRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = match &self.source {
            SnapshotConstructionSource::Fresh(snapshot_strategy) => snapshot_strategy
                .load_snapshot(
                    self.engine.as_ref(),
                    &self.runtime,
                    self.time_travel.as_ref(),
                    self.incremental_replay,
                )?,
            SnapshotConstructionSource::Existing(existing_snapshot) => {
                let mut builder = Snapshot::builder_from(existing_snapshot.clone())
                    .with_incremental_crc_replay(self.incremental_replay);
                if let Some(time_travel) = self.time_travel.as_ref() {
                    builder = builder.at_version(time_travel.as_version()?);
                }
                builder.build(self.engine.as_ref())?
            }
        };
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
    use rstest::rstest;
    use test_utils::copy_directory;

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
    fn snapshot_benchmark_names_preserve_legacy_and_suffix_registered_configs() {
        let table_info = test_table_info();
        assert_eq!(
            snapshot_benchmark_name(&table_info, "snapshotLatest", None),
            "basic_partitioned/snapshotLatest"
        );
        assert_eq!(
            snapshot_benchmark_name(&table_info, "snapshotLatest", Some("fresh")),
            "basic_partitioned/snapshotLatest/fresh"
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

    fn registry_workload(table: &str, case: &str, spec: Spec) -> Workload {
        let mut table_info = test_table_info();
        table_info.table_info_dir = PathBuf::from(table);
        Workload {
            table_info,
            case_name: case.to_string(),
            spec,
        }
    }

    fn from_snapshot_config(version: u64) -> SnapshotBuilderConfig {
        SnapshotBuilderConfig::From { version }
    }

    #[test]
    fn test_fresh_snapshot_construction_runner() {
        let runner = SnapshotConstructionRunner::setup(
            benchmark_name(&test_table_info(), "testCase"),
            &test_snapshot_spec(),
            SnapshotBuilderConfig::For,
            IncrementalCrcReplay::Disabled,
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(runner.name(), "basic_partitioned/testCase");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_snapshot_construction_runner_from_existing_snapshot() {
        let runner = SnapshotConstructionRunner::setup(
            configured_benchmark_name(&test_table_info(), "testCase", "from0"),
            &test_snapshot_spec(),
            from_snapshot_config(0),
            IncrementalCrcReplay::Disabled,
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }

    #[rstest]
    #[case(Some(0), 0)]
    #[case(None, 1)]
    fn test_snapshot_construction_rejects_base_at_target(
        #[case] target_version: Option<i64>,
        #[case] base_version: u64,
    ) {
        let spec = SnapshotConstructionSpec {
            time_travel: target_version.map(|version| TimeTravel::Version { version }),
            expected: None,
        };
        let err = SnapshotConstructionRunner::setup(
            configured_benchmark_name(
                &test_table_info(),
                "testCase",
                &format!("from{base_version}"),
            ),
            &spec,
            from_snapshot_config(base_version),
            IncrementalCrcReplay::Disabled,
            &test_table_info(),
            test_runtime(),
        )
        .err()
        .expect("setup should reject a base at the target version")
        .to_string();
        assert!(err.contains("must be below target version"), "got: {err}");
    }

    #[test]
    fn snapshot_from_latest_uses_actual_version_instead_of_commit_count() {
        let mut table_info = test_table_info();
        table_info.log_info.num_commits = 1;
        let runner = SnapshotConstructionRunner::setup(
            configured_benchmark_name(&table_info, "testCase", "from0"),
            &test_snapshot_spec(),
            from_snapshot_config(0),
            IncrementalCrcReplay::Disabled,
            &table_info,
            test_runtime(),
        )
        .expect("setup should use the actual latest snapshot version");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn snapshot_from_rejects_catalog_managed_config() {
        let error = validate_snapshot_strategy(
            &from_snapshot_config(0),
            IncrementalCrcReplay::Disabled,
            true,
        )
        .expect_err("catalog-managed builder_from must be rejected");
        assert!(error.to_string().contains("catalog-managed"));
    }

    #[test]
    fn snapshot_from_honors_explicit_target_before_latest() {
        let temp = tempfile::tempdir().unwrap();
        let source = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../kernel/tests/data/basic_partitioned"
        ));
        copy_directory(source, temp.path()).unwrap();
        std::fs::write(
            temp.path().join("_delta_log/00000000000000000002.json"),
            "not valid json\n",
        )
        .unwrap();

        let mut table_info = test_table_info();
        table_info.table_path = Some(Url::from_directory_path(temp.path()).unwrap());
        table_info.log_info.num_commits = 3;
        let spec = SnapshotConstructionSpec {
            time_travel: Some(TimeTravel::Version { version: 1 }),
            expected: None,
        };
        let runner = SnapshotConstructionRunner::setup(
            configured_benchmark_name(&table_info, "snapshotVersion1", "from0"),
            &spec,
            from_snapshot_config(0),
            IncrementalCrcReplay::Disabled,
            &table_info,
            test_runtime(),
        )
        .expect("setup should succeed");

        runner.execute().expect("version 2 must not be read");
    }

    #[test]
    fn test_snapshot_construction_rejects_catalog_managed_incremental_crc() {
        let err = validate_snapshot_strategy(
            &SnapshotBuilderConfig::For,
            IncrementalCrcReplay::Unlimited,
            true,
        )
        .expect_err("catalog-managed incremental CRC should be rejected")
        .to_string();
        assert!(
            err.contains("unsupported for catalog-managed snapshots"),
            "got: {err}"
        );
    }

    #[test]
    fn test_incremental_crc_replay_config_conversion() {
        assert_eq!(
            IncrementalReplay::from(IncrementalCrcReplay::UpToCommits { num_commits: 5 }),
            IncrementalReplay::UpToCommits(5)
        );
        assert_eq!(
            IncrementalReplay::from(IncrementalCrcReplay::Unlimited),
            IncrementalReplay::Unlimited
        );
    }

    /// Guards the checked-in `bench-registry.json` so a malformed edit or a duplicate config name
    /// fails fast in `cargo test` rather than only when the harness runs.
    #[test]
    fn checked_in_registry_loads_and_validates() {
        let path = format!("{}/bench-registry.json", env!("CARGO_MANIFEST_DIR"));
        let registry = BenchRegistry::load_from_path(Path::new(&path))
            .expect("checked-in bench-registry.json must load");
        let workload = registry_workload(
            "10kAdds0CommitsSinceChkpt1V2Chkpt",
            "readMetadataLatest",
            Spec::Read(test_read_spec()),
        );
        registry
            .validate(slice::from_ref(&workload))
            .expect("checked-in bench-registry.json must validate");
        let read = registry.read_configs(&workload).unwrap();
        assert_eq!(
            read.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["serial", "parallel2"]
        );
        for (table, crc_budget) in [
            ("crc1Col200Commits1ChkptCrc150", Some(50)),
            ("crc1Col200Commits1ChkptCrc195", Some(5)),
            ("crc1Col200Commits1ChkptCrcLatest", None),
            ("crc1Col200Commits1ChkptNoCrc", None),
        ] {
            let workload = registry_workload(
                table,
                "snapshotLatest",
                Spec::SnapshotConstruction(Box::new(test_snapshot_spec())),
            );
            let snapshots = registry
                .snapshot_configs(&workload)
                .expect("snapshot registry entry should have snapshot configs")
                .expect("snapshot registry entry should be present");
            let config = |name| {
                snapshots
                    .iter()
                    .find(|config| config.name == name)
                    .unwrap_or_else(|| panic!("missing config '{name}' for '{table}'"))
            };

            assert_eq!(config("fresh").snapshot_builder, SnapshotBuilderConfig::For);
            assert_eq!(
                config("fresh").incremental_crc_replay,
                IncrementalCrcReplay::Disabled
            );
            assert_eq!(
                config("from199").snapshot_builder,
                SnapshotBuilderConfig::From { version: 199 }
            );
            assert_eq!(
                config("from199").incremental_crc_replay,
                IncrementalCrcReplay::Disabled
            );

            if let Some(num_commits) = crc_budget {
                let expected = IncrementalCrcReplay::UpToCommits { num_commits };
                assert_eq!(
                    config("freshIncrementalCrc").snapshot_builder,
                    SnapshotBuilderConfig::For
                );
                assert_eq!(
                    config("freshIncrementalCrc").incremental_crc_replay,
                    expected
                );
                assert_eq!(
                    config("from199IncrementalCrc").snapshot_builder,
                    SnapshotBuilderConfig::From { version: 199 }
                );
                assert_eq!(
                    config("from199IncrementalCrc").incremental_crc_replay,
                    expected
                );
            } else {
                assert_eq!(snapshots.len(), 2, "unexpected CRC configs for '{table}'");
            }
        }
    }

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
            let workload = workload_by_key.get(&(table, case)).unwrap_or_else(|| {
                panic!("registry key '{table}/{case}' matches no workload (stale or typo'd?)")
            });
            let resolved = match &workload.spec {
                Spec::Read(_) => registry.read_configs(workload).unwrap().len(),
                Spec::SnapshotConstruction(spec) => {
                    let configs = registry
                        .snapshot_configs(workload)
                        .expect("snapshot workload must have snapshot configs")
                        .expect("registered snapshot workload must resolve configs");
                    if let Some(time_travel) = spec.time_travel.as_ref() {
                        let target_version = time_travel
                            .as_version()
                            .expect("snapshot workload target must be a version");
                        for config in &configs {
                            if let SnapshotBuilderConfig::From { version } = config.snapshot_builder
                            {
                                assert!(
                                    version < target_version,
                                    "registry base version {version} must be below target version \
                                     {target_version} for '{table}/{case}'"
                                );
                            }
                        }
                    }
                    configs.len()
                }
            };
            assert_ne!(resolved, 0);
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
            SnapshotBuilderConfig::For,
            IncrementalCrcReplay::Disabled,
            &test_table_info(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }
}
