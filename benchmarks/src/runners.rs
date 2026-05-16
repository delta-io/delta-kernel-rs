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

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::{Array, AsArray};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{Expression, PredicateRef, UnaryExpressionOp};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use delta_kernel_unity_catalog::UCKernelClient;
use unity_catalog_delta_client_api::{Error as UcApiError, Operation};
use unity_catalog_delta_rest_client::{
    ClientConfig, Error as UcRestError, UCClient, UCCommitsRestClient,
};
use url::Url;

use crate::models::{
    ParallelScan, ReadConfig, ReadEngine, ReadOperation, ReadSpec, SnapshotConstructionSpec,
    TableInfo, TimeTravel,
};
use crate::predicate_parser::parse_predicate;

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
    thread_pool: Option<rayon::ThreadPool>, /* None for serial configuration, Some for parallel
                                             * configuration */
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

pub struct ReadDataStateMachineRunner {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    name: String,
    predicate: Option<PredicateRef>,
    projected_schema: Option<delta_kernel::schema::SchemaRef>,
}

impl ReadDataStateMachineRunner {
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
        let projected_schema = read_spec
            .columns
            .as_ref()
            .map(|cols| snapshot.schema().project(cols))
            .transpose()?;
        let name = format!(
            "{}/{}/{}/{}",
            table_info.name,
            case_name,
            ReadOperation::ReadData.as_str(),
            config.name,
        );
        Ok(Self {
            snapshot,
            engine,
            name,
            predicate,
            projected_schema,
        })
    }
}

impl WorkloadRunner for ReadDataStateMachineRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = self
            .snapshot
            .clone()
            .scan_builder()
            .with_predicate(self.predicate.clone());
        if let Some(schema) = &self.projected_schema {
            builder = builder.with_schema(schema.clone());
        }
        let scan = builder.build()?;
        for result in scan.execute(self.engine.clone())? {
            black_box(result?);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct ReadDataPlansRunner {
    snapshot: Arc<Snapshot>,
    executor: DataFusionExecutor,
    runtime: Arc<tokio::runtime::Runtime>,
    name: String,
    predicate: Option<PredicateRef>,
    projected_schema: Option<delta_kernel::schema::SchemaRef>,
}

fn evaluate_to_json_column(
    batch: &delta_kernel::arrow::array::RecordBatch,
    col: &'static str,
) -> DeltaResult<delta_kernel::arrow::array::StringArray> {
    let arr = evaluate_expression(
        &Expression::unary(UnaryExpressionOp::ToJson, Expression::column([col])),
        batch,
        Some(&KernelDataType::STRING),
    )?;
    Ok(arr.as_string::<i32>().clone())
}

async fn extract_snapshot_protocol_metadata_from_fsr(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
) -> DeltaResult<(Protocol, Metadata)> {
    let validated_protocol = snapshot.protocol().clone();
    let validated_metadata = snapshot.metadata().clone();
    let executor = DataFusionExecutor::try_new_with_engine(engine)
        .map_err(|e| Error::generic(format!("create DataFusionExecutor: {e}")))?;
    let sm = snapshot.full_state()?;
    let rp = executor
        .drive_to_completion(sm)
        .await
        .map_err(|e| Error::generic(format!("execute full_state via DataFusionExecutor: {e}")))?;
    let fsr_batches = executor.collect_result(rp).await.map_err(|e| {
        Error::generic(format!(
            "collect full_state results via DataFusionExecutor: {e}"
        ))
    })?;

    // Exercise FSR action stream extraction, but keep snapshot semantic validation at
    // Snapshot/TableConfiguration boundary.
    for batch in fsr_batches {
        let protocol_col = evaluate_to_json_column(&batch, "protocol")?;
        let metadata_col = evaluate_to_json_column(&batch, "metaData")?;
        for i in 0..batch.num_rows() {
            if protocol_col.is_valid(i) {
                let _ = serde_json::from_str::<Protocol>(protocol_col.value(i));
            }
            if metadata_col.is_valid(i) {
                let _ = serde_json::from_str::<Metadata>(metadata_col.value(i));
            }
        }
    }

    Ok((validated_protocol, validated_metadata))
}

impl ReadDataPlansRunner {
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
        let projected_schema = read_spec
            .columns
            .as_ref()
            .map(|cols| snapshot.schema().project(cols))
            .transpose()?;
        let predicate = read_spec
            .predicate
            .as_deref()
            .map(|sql| parse_predicate(sql, &snapshot.schema()))
            .transpose()?
            .map(Arc::new);
        let executor = DataFusionExecutor::try_new_with_engine(engine.clone())
            .map_err(|e| format!("DataFusion executor setup failed: {e}"))?;
        let name = format!(
            "{}/{}/{}/{}",
            table_info.name,
            case_name,
            ReadOperation::ReadData.as_str(),
            config.name,
        );
        Ok(Self {
            snapshot,
            executor,
            runtime,
            name,
            predicate,
            projected_schema,
        })
    }
}

impl WorkloadRunner for ReadDataPlansRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = self.snapshot.clone().scan_builder();
        if let Some(schema) = &self.projected_schema {
            builder = builder.with_schema(schema.clone());
        }
        if let Some(predicate) = &self.predicate {
            builder = builder.with_predicate(predicate.clone());
        }
        let replay_scan = builder
            .build_replay()
            .map_err(|e| format!("build replay scan failed: {e}"))?;
        let replay_sm = replay_scan
            .scan_state_machine()
            .map_err(|e| format!("build replay scan SM failed: {e}"))?;
        let batches = self
            .runtime
            .block_on(async {
                let rp = self.executor.drive_to_completion(replay_sm).await?;
                self.executor.collect_result(rp).await
            })
            .map_err(|e| format!("DataFusion read execution failed: {e}"))?;
        for batch in batches {
            black_box(batch);
        }
        Ok(())
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
    match (operation, &config.read_engine) {
        (ReadOperation::ReadMetadata, ReadEngine::DefaultEngine) => Ok(Box::new(
            ReadMetadataRunner::setup(table_info, case_name, read_spec, config, runtime)?,
        )),
        (ReadOperation::ReadMetadata, ReadEngine::Datafusion) => {
            Err("ReadMetadata with the Datafusion engine is not supported".into())
        }
        (ReadOperation::ReadData, ReadEngine::DefaultEngine) => Ok(Box::new(
            ReadDataStateMachineRunner::setup(table_info, case_name, read_spec, config, runtime)?,
        )),
        (ReadOperation::ReadData, ReadEngine::Datafusion) => Ok(Box::new(
            ReadDataPlansRunner::setup(table_info, case_name, read_spec, config, runtime)?,
        )),
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
        let (protocol, metadata) =
            self.runtime
                .block_on(extract_snapshot_protocol_metadata_from_fsr(
                    Arc::clone(&snapshot),
                    Arc::clone(&self.engine),
                ))?;
        black_box((snapshot.version(), protocol, metadata));
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::models::{
        ParallelScan, ReadConfig, ReadEngine, ReadSpec, Spec, TableInfo, TimeTravel,
    };

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
            name: "default_engine_serial".to_string(),
            read_engine: ReadEngine::DefaultEngine,
            parallel_scan: ParallelScan::Disabled,
        }
    }

    fn parallel_config() -> ReadConfig {
        ReadConfig {
            name: "default_engine_parallel2".to_string(),
            read_engine: ReadEngine::DefaultEngine,
            parallel_scan: ParallelScan::Enabled { num_threads: 2 },
        }
    }

    fn datafusion_config() -> ReadConfig {
        ReadConfig {
            name: "datafusion".to_string(),
            read_engine: ReadEngine::Datafusion,
            parallel_scan: ParallelScan::Disabled,
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
            "basic_partitioned/testCase/readMetadata/default_engine_serial"
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
            "basic_partitioned/testCase/readMetadata/default_engine_parallel2"
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
        if let Err(e) = runner.execute() {
            panic!("read_data_plans_datafusion execute failed: {e}");
        }
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
    fn test_create_read_runner_read_data_state_machine() {
        let runner = create_read_runner(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            ReadOperation::ReadData,
            serial_config(),
            test_runtime(),
        )
        .expect("create_read_runner should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_create_read_runner_read_data_plans_datafusion() {
        let runner = create_read_runner(
            &test_table_info(),
            "testCase",
            &test_read_spec(),
            ReadOperation::ReadData,
            datafusion_config(),
            test_runtime(),
        )
        .expect("create_read_runner should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/testCase/readData/datafusion"
        );
        assert!(runner.execute().is_ok());
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

    #[test]
    fn print_read_data_plans_datafusion_physical_plan() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("workloads")
            .join("benchmarks")
            .join("101kAdds1kCommitsSinceChkpt1Chkpt");
        let table_info = TableInfo::from_json_path(root.join("tableInfo.json"))
            .expect("load benchmark tableInfo.json");
        let spec = Spec::from_json_path(root.join("specs").join("readV60.json"))
            .expect("load benchmark read spec");
        let read_spec = match spec {
            Spec::Read(s) => s,
            _ => panic!("readV60.json must be a read spec"),
        };

        let runner = ReadDataPlansRunner::setup(
            &table_info,
            "readV60",
            &read_spec,
            datafusion_config(),
            test_runtime(),
        )
        .expect("setup should succeed");

        let schema = runner
            .projected_schema
            .clone()
            .unwrap_or_else(|| runner.snapshot.schema().clone());
        let replay_scan = runner
            .snapshot
            .clone()
            .scan_builder()
            .with_schema(schema)
            .build_replay()
            .expect("build replay scan should succeed");
        let result_plan = replay_scan
            .scan_plans()
            .expect("replay scan plans should succeed");
        // The terminal plan is the one whose Relation sink names the scan-result handle.
        let terminal_id = result_plan.result_relation.id;
        let plan = result_plan
            .plans
            .into_iter()
            .find(|p| {
                matches!(
                    &p.sink.sink_type,
                    delta_kernel::plans::ir::nodes::SinkType::Relation(h) if h.id == terminal_id
                )
            })
            .expect("expected replay plans to include the scan-result Relation sink");
        let logical = runner
            .executor
            .compile_plan_logical_for_inspection(&plan)
            .expect("compile_plan_logical should succeed");

        println!(
            "=== DataFusion Logical Plan ===\n{}",
            logical.display_indent()
        );
    }

    #[test]
    fn print_fsr_metadata_phase_physical_plans() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("workloads")
            .join("benchmarks")
            .join("101kAdds1kCommitsSinceChkpt1Chkpt");
        let table_info = TableInfo::from_json_path(root.join("tableInfo.json"))
            .expect("load benchmark tableInfo.json");
        let spec = Spec::from_json_path(root.join("specs").join("readV60.json"))
            .expect("load benchmark read spec");
        let read_spec = match spec {
            Spec::Read(s) => s,
            _ => panic!("readV60.json must be a read spec"),
        };

        let runner = ReadDataPlansRunner::setup(
            &table_info,
            "readV60",
            &read_spec,
            datafusion_config(),
            test_runtime(),
        )
        .expect("setup should succeed");

        let sm = runner.snapshot.full_state().expect("build full_state SM");
        runner.runtime.block_on(async {
            let result_plan = runner
                .executor
                .drive_to_completion(sm)
                .await
                .expect("drive full_state SM to completion");
            for (idx, plan) in result_plan.plans.iter().enumerate() {
                match runner.executor.compile_plan_logical_for_inspection(plan) {
                    Ok(logical) => println!(
                        "=== FSR Phase Plan {} ({:?}) ===\n{}",
                        idx,
                        plan.sink.sink_type,
                        logical.display_indent()
                    ),
                    Err(e) => println!(
                        "=== FSR Phase Plan {} ({:?}) -- compile skipped: {} ===",
                        idx, plan.sink.sink_type, e
                    ),
                }
            }
            runner
                .executor
                .execute_plans(&result_plan.plans)
                .await
                .expect("execute FSR plans");
        });
    }
}
