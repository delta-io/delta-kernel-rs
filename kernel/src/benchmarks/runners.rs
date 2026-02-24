//! Benchmark runners for executing Delta table operations.
//!
//! Each runner handles setup and execution of a specific operation (e.g., reading metadata)
//! Results are discarded for benchmarking purposes
//! Currently only supports reading metadata

use crate::benchmarks::models::{ParallelScan, ReadConfig, ReadOperation, ReadSpec, TableInfo};
use crate::parallel::parallel_phase::ParallelPhase;
use crate::parallel::sequential_phase::AfterSequential;
use crate::snapshot::Snapshot;
use crate::Engine;

use std::hint::black_box;
use std::sync::Arc;
use std::thread;

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
    /// Sets up a benchmark runner for reading metadata.
    pub fn setup(
        table_info: &TableInfo,
        case_name: &str,
        read_spec: &ReadSpec,
        config: ReadConfig,
        engine: Arc<dyn Engine>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table_root = table_info.resolved_table_root();
        let url = crate::try_parse_uri(table_root)?;

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

                        thread::spawn(move || -> Result<(), crate::Error> {
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
        ReadOperation::ReadData => {
            todo!("ReadDataRunner not yet implemented")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmarks::models::{ParallelScan, ReadConfig, ReadSpec, TableInfo};
    use crate::engine::sync::SyncEngine;
    use std::path::PathBuf;

    fn test_table_info() -> TableInfo {
        TableInfo {
            name: "basic_partitioned".to_string(),
            description: None,
            table_path: Some(format!(
                "{}/tests/data/basic_partitioned",
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
        Arc::new(SyncEngine::new())
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
