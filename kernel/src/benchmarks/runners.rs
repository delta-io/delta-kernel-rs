//! Benchmark runners for executing Delta table operations.
//!
//! Each runner handles setup and execution of a specific operation (e.g., reading metadata)
//! Results are discarded for benchmarking purposes
//! Currently only supports reading metadata

use crate::benchmarks::models::{Config, ParallelScan, Spec, WorkloadVariant};
use crate::parallel::parallel_phase::ParallelPhase;
use crate::parallel::sequential_phase::AfterSequential;
use crate::snapshot::Snapshot;
use crate::Engine;

use std::hint::black_box;
use std::sync::Arc;
use std::thread;

pub struct ReadMetadataRunner {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    workload_variant: WorkloadVariant, //Complete workload specification
}

impl ReadMetadataRunner {
    /// Sets up a benchmark runner for reading metadata.
    pub fn setup(
        workload_variant: WorkloadVariant,
        engine: Arc<dyn Engine>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        workload_variant.validate()?;

        let table_root = workload_variant.table_info.resolved_table_root();

        let url = crate::try_parse_uri(table_root)?;

        let version = match &workload_variant.spec {
            Spec::Read { version } => version,
        };

        let mut builder = Snapshot::builder_for(url);
        if let Some(version) = version {
            builder = builder.at_version(*version);
        }

        let snapshot = builder.build(engine.as_ref())?;

        Ok(Self {
            snapshot,
            engine,
            workload_variant,
        })
    }

    pub fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.workload_variant.config {
            Config::Read(config) => match &config.parallel_scan {
                ParallelScan::Disabled => {
                    self.execute_serial()?;
                }
                ParallelScan::Enabled { num_threads } => {
                    self.execute_parallel(num_threads)?;
                }
            },
        }

        Ok(())
    }

    fn execute_serial(&self) -> Result<(), Box<dyn std::error::Error>> {
        let scan = self.snapshot.clone().scan_builder().build()?;
        let metadata_iter = scan.scan_metadata(self.engine.as_ref())?;
        for result in metadata_iter {
            black_box(result?);
        }
        Ok(())
    }

    fn execute_parallel(&self, num_threads: &usize) -> Result<(), Box<dyn std::error::Error>> {
        let scan = self.snapshot.clone().scan_builder().build()?;

        let mut phase1 = scan.parallel_scan_metadata(self.engine.clone())?;
        for result in phase1.by_ref() {
            black_box(result?);
        }

        match phase1.finish()? {
            AfterSequential::Done(_) => {}
            AfterSequential::Parallel { processor, files } => {
                let num_workers = *num_threads;
                let files_per_worker = files.len().div_ceil(num_workers);

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

    pub fn name(&self) -> Result<String, Box<dyn std::error::Error>> {
        self.workload_variant.name()
    }
}
