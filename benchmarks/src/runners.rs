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

use crate::models::{
    ParallelScan, ReadConfig, ReadOperation, ReadSpec, SnapshotConstructionSpec, TableInfo,
    TimeTravel,
};
use crate::predicate_parser::parse_predicate;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::Engine;
use delta_kernel::Snapshot;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_unity_catalog::UCKernelClient;
use unity_catalog_delta_client_api::{Error as UcApiError, Operation};
use unity_catalog_delta_rest_client::{
    ClientConfig, Error as UcRestError, UCClient, UCCommitsRestClient,
};

use std::hint::black_box;
use std::sync::Arc;
use url::Url;

/// Delta table property indicating catalog-managed support.
const CATALOG_MANAGED_PROPERTY: &str = "delta.feature.catalogManaged";

pub trait WorkloadRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn name(&self) -> &str;
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
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        match self {
            SnapshotStrategy::Standard { url } => {
                let mut builder = Snapshot::builder_for(url.clone());
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
    thread_pool: Option<rayon::ThreadPool>, // None for serial configuration, Some for parallel configuration
    predicate: Option<PredicateRef>,
}

impl ReadMetadataRunner {
    pub fn setup(
        table_info: &TableInfo,
        case_name: &str,
        read_spec: &ReadSpec,
        config: ReadConfig,
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

        let name = format!(
            "{}/{}/{}/{}",
            table_info.name,
            case_name,
            ReadOperation::ReadMetadata.as_str(),
            config.name,
        );

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

/// Factory function that creates the appropriate read runner for a given operation and config
pub fn create_read_runner(
    table_info: &TableInfo,
    case_name: &str,
    read_spec: &ReadSpec,
    operation: ReadOperation,
    config: ReadConfig,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<Box<dyn WorkloadRunner>, Box<dyn std::error::Error>> {
    match operation {
        ReadOperation::ReadMetadata => Ok(Box::new(ReadMetadataRunner::setup(
            table_info, case_name, read_spec, config, runtime,
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
        table_info: &TableInfo,
        case_name: &str,
        snapshot_spec: &SnapshotConstructionSpec,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let name = format!(
            "{}/{}/{}",
            table_info.name,
            case_name,
            snapshot_spec.as_str()
        );

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
    use super::*;
    use crate::models::{ParallelScan, ReadConfig, ReadSpec, TableInfo, TimeTravel};
    use std::sync::LazyLock;

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
    fn test_read_metadata_runner_serial() {
        let runner = ReadMetadataRunner::setup(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            serial_config(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/testCase/readMetadata/serial"
        );
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_read_metadata_runner_parallel() {
        let runner = ReadMetadataRunner::setup(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            parallel_config(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/testCase/readMetadata/parallel2"
        );
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
            &test_table_info(),
            "testCase",
            &test_snapshot_spec(),
            test_runtime(),
        );
        assert!(runner.is_ok());
    }

    #[test]
    fn test_snapshot_construction_runner_name() {
        let runner = SnapshotConstructionRunner::setup(
            &test_table_info(),
            "testCase",
            &test_snapshot_spec(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/testCase/snapshotConstruction"
        );
    }

    #[test]
    fn test_snapshot_construction_runner_execute() {
        let runner = SnapshotConstructionRunner::setup(
            &test_table_info(),
            "testCase",
            &test_snapshot_spec(),
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_create_read_runner_read_metadata() {
        let runner = create_read_runner(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            ReadOperation::ReadMetadata,
            serial_config(),
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
            &test_table_info(),
            "test_case",
            &spec,
            serial_config(),
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
            &test_table_info(),
            "test_case",
            &spec,
            serial_config(),
            test_runtime(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_read_runner_read_data_unimplemented() {
        let result = create_read_runner(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            ReadOperation::ReadData,
            serial_config(),
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
            &test_table_info(),
            "testCase",
            &spec,
            test_runtime(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }
}
