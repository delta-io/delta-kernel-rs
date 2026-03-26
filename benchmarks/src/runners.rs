//! Benchmark runners for executing Delta table operations.
//!
//! Each runner holds all the state required for its workload (e.g. read metadata needs
//! pre-built snapshots and a config) so that `execute` measures only the operation itself.
//! Results are discarded for benchmarking purposes.
//!
//! Engine and snapshot construction is handled internally based on `TableInfo`:
//! - UC tables (`catalog_managed_info` set): credentials from `UC_WORKSPACE`/`UC_TOKEN` env vars,
//!   snapshot via `UCCatalog::load_snapshot` (supports both CCv2 and regular UC tables)
//! - S3 tables (`table_path` with s3:// scheme): credentials from `AWS_*` env vars
//! - Local tables: local filesystem engine

use crate::models::{
    ParallelScan, ReadConfig, ReadOperation, ReadSpec, SnapshotConstructionSpec, TableInfo,
};
use crate::predicate_parser::parse_predicate;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::Engine;
use delta_kernel::Snapshot;
use object_store::local::LocalFileSystem;
use uc_catalog::UCCatalog;
use uc_client::prelude::*;
use uc_client::ClientConfig;

use std::hint::black_box;
use std::sync::Arc;
use url::Url;

pub trait WorkloadRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Engine + snapshot setup (resolved from TableInfo + env vars)
// ---------------------------------------------------------------------------

fn build_engine(
    store: Arc<dyn object_store::ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Arc<dyn Engine> {
    let executor = TokioMultiThreadExecutor::new(runtime.handle().clone());
    Arc::new(
        DefaultEngine::builder(store)
            .with_task_executor(Arc::new(executor))
            .build(),
    )
}

/// Pre-resolved UC state for building snapshots via UCCatalog.
struct ResolvedUcInfo {
    table_id: String,
    table_uri: String,
    commits_client: UCCommitsRestClient,
}

impl ResolvedUcInfo {
    fn load_snapshot(
        &self,
        engine: &dyn Engine,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        let catalog = UCCatalog::new(&self.commits_client);
        runtime
            .block_on(catalog.load_snapshot(&self.table_id, &self.table_uri, engine))
            .map_err(|e| format!("UC snapshot failed: {e}").into())
    }
}

/// Resolves a UC table: gets credentials, builds engine, resolves table_id/uri.
/// Does NOT build the snapshot - call `load_snapshot()` for that.
fn resolve_uc(
    table_info: &TableInfo,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<(Arc<dyn Engine>, ResolvedUcInfo), Box<dyn std::error::Error>> {
    let cm = table_info
        .catalog_managed_info
        .as_ref()
        .ok_or("not a UC table")?;
    let endpoint = std::env::var("UC_WORKSPACE").map_err(|_| "UC_WORKSPACE required")?;
    let token = std::env::var("UC_TOKEN").map_err(|_| "UC_TOKEN required")?;

    let config = ClientConfig::build(&endpoint, &token).build()?;
    let client = UCClient::new(config.clone())?;
    let commits_client = UCCommitsRestClient::new(config)?;

    let (table_id, table_uri, aws) = runtime.block_on(async {
        let table = client.get_table(&cm.table_name).await?;
        let creds = client
            .get_credentials(&table.table_id, Operation::Read)
            .await?;
        let aws = creds
            .aws_temp_credentials
            .ok_or(uc_client::Error::UnsupportedOperation(
                "UC credential vending returned no AWS credentials".into(),
            ))?;
        Ok::<_, uc_client::Error>((table.table_id, table.storage_location, aws))
    })?;

    let table_url = Url::parse(&table_uri)?;
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    let options = [
        ("region", region.as_str()),
        ("access_key_id", aws.access_key_id.as_str()),
        ("secret_access_key", aws.secret_access_key.as_str()),
        ("session_token", aws.session_token.as_str()),
    ];
    let (store, _) = object_store::parse_url_opts(&table_url, options)?;

    let engine = build_engine(store.into(), runtime);
    let uc = ResolvedUcInfo {
        table_id,
        table_uri,
        commits_client,
    };
    Ok((engine, uc))
}

/// Builds an engine from table_path scheme + env vars (S3 or local).
fn resolve_engine(
    table_info: &TableInfo,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<Arc<dyn Engine>, Box<dyn std::error::Error>> {
    let url = table_info.resolved_table_root();
    match url.scheme() {
        "s3" | "s3a" => {
            let env = |k: &str| std::env::var(k).ok();
            let mut opts: Vec<(&str, String)> = vec![(
                "region",
                env("AWS_REGION").unwrap_or_else(|| "us-west-2".into()),
            )];
            if let Some(v) = env("AWS_ACCESS_KEY_ID") {
                opts.push(("access_key_id", v));
            }
            if let Some(v) = env("AWS_SECRET_ACCESS_KEY") {
                opts.push(("secret_access_key", v));
            }
            if let Some(v) = env("AWS_SESSION_TOKEN") {
                opts.push(("session_token", v));
            }
            let (store, _) = object_store::parse_url_opts(&url, opts)?;
            Ok(build_engine(store.into(), runtime))
        }
        "file" | "" => Ok(build_engine(Arc::new(LocalFileSystem::new()), runtime)),
        scheme => Err(format!(
            "Unsupported scheme '{scheme}': only s3://, s3a://, and file:// are supported"
        )
        .into()),
    }
}

// ---------------------------------------------------------------------------
// ReadMetadataRunner
// ---------------------------------------------------------------------------

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
        let (engine, snapshot) = if table_info.catalog_managed_info.is_some() {
            let (engine, uc) = resolve_uc(table_info, runtime.clone())?;
            let snapshot = uc.load_snapshot(engine.as_ref(), &runtime)?;
            (engine, snapshot)
        } else {
            let engine = resolve_engine(table_info, runtime.clone())?;
            let mut builder = Snapshot::builder_for(table_info.resolved_table_root());
            if let Some(v) = read_spec.version {
                builder = builder.at_version(v);
            }
            let snapshot = builder.build(engine.as_ref())?;
            (engine, snapshot)
        };

        let predicate = read_spec
            .predicate
            .as_deref()
            .map(parse_predicate)
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

// ---------------------------------------------------------------------------
// SnapshotConstructionRunner
// ---------------------------------------------------------------------------

pub struct SnapshotConstructionRunner {
    engine: Arc<dyn Engine>,
    runtime: Arc<tokio::runtime::Runtime>,
    url: Url,
    version: Option<u64>,
    uc: Option<ResolvedUcInfo>,
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

        let (engine, uc) = if table_info.catalog_managed_info.is_some() {
            let (engine, uc) = resolve_uc(table_info, runtime.clone())?;
            (engine, Some(uc))
        } else {
            (resolve_engine(table_info, runtime.clone())?, None)
        };

        Ok(Self {
            engine,
            runtime,
            url: table_info.resolved_table_root(),
            version: snapshot_spec.version,
            uc,
            name,
        })
    }
}

impl WorkloadRunner for SnapshotConstructionRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = if let Some(uc) = &self.uc {
            uc.load_snapshot(self.engine.as_ref(), &self.runtime)?
        } else {
            let mut builder = Snapshot::builder_for(self.url.clone());
            if let Some(v) = self.version {
                builder = builder.at_version(v);
            }
            builder.build(self.engine.as_ref())?
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
    use super::*;
    use crate::models::{ParallelScan, ReadConfig, ReadSpec, TableInfo};

    fn test_runtime() -> Arc<tokio::runtime::Runtime> {
        use std::sync::LazyLock;
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
            version: None,
            predicate: None,
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
        SnapshotConstructionSpec { version: None }
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

    /// Tests read metadata with a UC-loaded snapshot using an in-memory object store and
    /// in-memory commits client. V0 is a published commit; v1 is a staged (unpublished) commit
    /// resolved through `UCCatalog::load_snapshot`.
    #[test]
    fn test_read_metadata_with_in_memory_uc_snapshot() {
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use object_store::ObjectStore;
        use uc_client::commits_client::{InMemoryCommitsClient, TableData};
        use uc_client::models::commits::Commit;

        let table_root = "memory:///uc_table/";
        let table_id = "test-table-id";
        let staged_filename = "00000000000000000001.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json";

        let store = Arc::new(InMemory::new());
        let runtime = test_runtime();

        // v0: published commit with protocol + metadata
        let v0 = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1000}}"#;
        runtime
            .block_on(store.put(
                &Path::from("uc_table/_delta_log/00000000000000000000.json"),
                v0.into(),
            ))
            .expect("put v0");

        // v1: staged commit with an add action
        let v1 = r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":100,"modificationTime":2000,"dataChange":true}}"#;
        runtime
            .block_on(store.put(
                &Path::from(format!(
                    "uc_table/_delta_log/_staged_commits/{staged_filename}"
                )),
                v1.into(),
            ))
            .expect("put v1 staged");

        // Set up in-memory commits client with v0 published, v1 staged
        let commits_client = InMemoryCommitsClient::new();
        commits_client.insert_table(
            table_id,
            TableData {
                max_ratified_version: 1,
                catalog_commits: vec![Commit::new(1, 2000, staged_filename, 100, 2000)],
            },
        );

        let engine = build_engine(store, runtime.clone());
        let snapshot = runtime
            .block_on(
                UCCatalog::new(&commits_client)
                    .load_snapshot(table_id, table_root, engine.as_ref()),
            )
            .expect("UC snapshot load should succeed");

        assert_eq!(snapshot.version(), 1);

        let runner = ReadMetadataRunner {
            snapshot,
            engine,
            name: "uc_in_memory/readMetadata/serial".to_string(),
            config: serial_config(),
            thread_pool: None,
            predicate: None,
        };
        assert!(runner.execute().is_ok());
    }
}
