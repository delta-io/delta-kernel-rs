//! Benchmark runners for executing Delta table operations.
//!
//! Each runner holds all the state required for its workload (e.g. read metadata needs pre-built snapshots and a config)
//! so that `execute` measures only the operation itself
//! Results are discarded for benchmarking purposes

use crate::models::{
    ParallelScan, ReadConfig, ReadOperation, ReadSpec, SnapshotConstructionSpec, TableInfo,
};
use delta_kernel::parallel::parallel_phase::ParallelPhase;
use delta_kernel::parallel::sequential_phase::AfterSequential;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{try_parse_uri, Engine, Error};

use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use url::Url;

/// Each runner holds all the state required for its workload (e.g. read metadata needs pre-built snapshots and a config)
/// so that `execute` measures only the operation itself
pub trait WorkloadRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn name(&self) -> &str;
}

pub struct ReadMetadataRunner {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    name: String,
    config: ReadConfig,
}

impl ReadMetadataRunner {
    pub fn setup(
        table_info: &TableInfo,
        case_name: &str,
        read_spec: &ReadSpec,
        config: ReadConfig,
        engine: Arc<dyn Engine>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table_root = table_info.resolved_table_root();
        let url = try_parse_uri(table_root)?;

        let mut builder = Snapshot::builder_for(url);
        if let Some(version) = read_spec.version {
            builder = builder.at_version(version);
        }

        let snapshot = builder.build(engine.as_ref())?;

        let name = format!(
            "{}/{}/{}/{}",
            table_info.name,
            case_name,
            ReadOperation::ReadMetadata.as_str(),
            config.name,
        );

        Ok(Self {
            snapshot,
            engine,
            name,
            config,
        })
    }

    fn execute_serial(&self) -> Result<(), Box<dyn std::error::Error>> {
        let scan = self.snapshot.clone().scan_builder().build()?;
        let metadata_iter = scan.scan_metadata(self.engine.as_ref())?;
        for result in metadata_iter {
            black_box(result?);
        }
        Ok(())
    }

    fn execute_parallel(&self, num_threads: usize) -> Result<(), Box<dyn std::error::Error>> {
        let scan = self.snapshot.clone().scan_builder().build()?;

        let mut phase1 = scan.parallel_scan_metadata(self.engine.clone())?;
        for result in phase1.by_ref() {
            black_box(result?);
        }

        match phase1.finish()? {
            AfterSequential::Done(_) => {}
            AfterSequential::Parallel { processor, files } => {
                if num_threads == 0 {
                    return Err("num_threads in ReadConfig must be greater than 0".into());
                }
                let files_per_worker = files.len().div_ceil(num_threads);

                let partitions: Vec<_> = files
                    .chunks(files_per_worker)
                    .map(|chunk| chunk.to_vec())
                    .collect();

                let processor = Arc::new(processor);

                let handles: Vec<_> = partitions
                    .into_iter()
                    .map(|partition_files| {
                        let engine = self.engine.clone();
                        let processor = processor.clone();

                        thread::spawn(move || -> Result<(), Error> {
                            if partition_files.is_empty() {
                                return Ok(());
                            }

                            let parallel =
                                ParallelPhase::try_new(engine, processor, partition_files)?;
                            for result in parallel {
                                black_box(result?);
                            }

                            Ok(())
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().map_err(|_| -> Box<dyn std::error::Error> {
                        "Worker thread panicked".into()
                    })??;
                }
            }
        }
        Ok(())
    }
}

impl WorkloadRunner for ReadMetadataRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.config.parallel_scan {
            ParallelScan::Disabled => {
                self.execute_serial()?;
            }
            ParallelScan::Enabled { num_threads } => {
                self.execute_parallel(*num_threads)?;
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub fn create_read_runner(
    table_info: &TableInfo,
    case_name: &str,
    read_spec: &ReadSpec,
    operation: ReadOperation,
    config: ReadConfig,
    engine: Arc<dyn Engine>,
) -> Result<Box<dyn WorkloadRunner>, Box<dyn std::error::Error>> {
    match operation {
        ReadOperation::ReadMetadata => Ok(Box::new(ReadMetadataRunner::setup(
            table_info, case_name, read_spec, config, engine,
        )?)),
        ReadOperation::ReadData => Err("ReadDataRunner not yet implemented".into()),
    }
}

pub struct SnapshotConstructionRunner {
    url: Url,
    version: Option<u64>,
    engine: Arc<dyn Engine>,
    name: String,
}

impl SnapshotConstructionRunner {
    pub fn setup(
        table_info: &TableInfo,
        case_name: &str,
        snapshot_spec: &SnapshotConstructionSpec,
        engine: Arc<dyn Engine>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table_root = table_info.resolved_table_root();
        let url = try_parse_uri(table_root)?;

        let name = format!(
            "{}/{}/{}",
            table_info.name,
            case_name,
            snapshot_spec.as_str()
        );

        Ok(Self {
            url,
            version: snapshot_spec.version,
            engine,
            name,
        })
    }
}

impl WorkloadRunner for SnapshotConstructionRunner {
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = Snapshot::builder_for(self.url.clone());
        if let Some(version) = self.version {
            builder = builder.at_version(version);
        }
        black_box(builder.build(self.engine.as_ref())?);

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
    use delta_kernel::engine::default::DefaultEngine;
    use object_store::local::LocalFileSystem;
    use std::path::PathBuf;

    fn test_table_info() -> TableInfo {
        TableInfo {
            name: "basic_partitioned".to_string(),
            description: None,
            table_path: Some(format!(
                "{}/../kernel/tests/data/basic_partitioned",
                env!("CARGO_MANIFEST_DIR")
            )),
            table_info_dir: PathBuf::new(),
        }
    }

    fn test_read_spec() -> ReadSpec {
        ReadSpec { version: None }
    }

    fn serial_config() -> ReadConfig {
        ReadConfig {
            name: "serial".to_string(),
            parallel_scan: ParallelScan::Disabled,
        }
    }

    fn parallel_config() -> ReadConfig {
        ReadConfig {
            name: "parallel".to_string(),
            parallel_scan: ParallelScan::Enabled { num_threads: 2 },
        }
    }

    fn test_engine() -> Arc<dyn Engine> {
        let store = Arc::new(LocalFileSystem::new());
        Arc::new(DefaultEngine::builder(store).build())
    }

    #[test]
    fn test_read_metadata_runner_serial() {
        let runner = ReadMetadataRunner::setup(
            &test_table_info(),
            "test_case",
            &test_read_spec(),
            serial_config(),
            test_engine(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/test_case/read_metadata/serial"
        );
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_read_metadata_runner_parallel() {
        let runner = ReadMetadataRunner::setup(
            &test_table_info(),
            "test_case",
            &test_read_spec(),
            parallel_config(),
            test_engine(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/test_case/read_metadata/parallel"
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
            "test_case",
            &test_snapshot_spec(),
            test_engine(),
        );
        assert!(runner.is_ok());
    }

    #[test]
    fn test_snapshot_construction_runner_name() {
        let runner = SnapshotConstructionRunner::setup(
            &test_table_info(),
            "test_case",
            &test_snapshot_spec(),
            test_engine(),
        )
        .expect("setup should succeed");
        assert_eq!(
            runner.name(),
            "basic_partitioned/test_case/snapshot_construction"
        );
    }

    #[test]
    fn test_snapshot_construction_runner_execute() {
        let runner = SnapshotConstructionRunner::setup(
            &test_table_info(),
            "test_case",
            &test_snapshot_spec(),
            test_engine(),
        )
        .expect("setup should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_create_read_runner_read_metadata() {
        let runner = create_read_runner(
            &test_table_info(),
            "test_case",
            &test_read_spec(),
            ReadOperation::ReadMetadata,
            serial_config(),
            test_engine(),
        )
        .expect("create_read_runner should succeed");
        assert!(runner.execute().is_ok());
    }

    #[test]
    fn test_create_read_runner_read_data_unimplemented() {
        let result = create_read_runner(
            &test_table_info(),
            "test_case",
            &test_read_spec(),
            ReadOperation::ReadData,
            serial_config(),
            test_engine(),
        );
        assert!(result.is_err());
    }
}
