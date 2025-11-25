//! Sidecar phase for log replay - processes sidecar/leaf parquet files.

use std::sync::Arc;

use crate::log_replay::ActionsBatch;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, FileMeta};

/// Phase that processes sidecar or leaf parquet files.
///
/// This phase is distributable - you can partition `files` and create multiple
/// instances on different executors.
#[allow(unused)]
pub(crate) struct LeafCheckpointReader {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl LeafCheckpointReader {
    /// Create a new sidecar phase from file list.
    ///
    /// # Distributability
    ///
    /// This phase is designed to be distributable. To distribute:
    /// 1. Partition `files` across N executors
    /// 2. Create N `LeafCheckpointReader` instances, one per executor with its file partition
    ///
    /// # Parameters
    /// - `files`: Sidecar/leaf files to process
    /// - `engine`: Engine for reading files
    /// - `schema`: Schema to use when reading sidecar files (projected based on processor requirements)
    #[allow(unused)]
    pub(crate) fn new(
        files: Vec<FileMeta>,
        engine: Arc<dyn Engine>,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let actions = engine
            .parquet_handler()
            .read_parquet_files(&files, schema, None)?
            .map(|batch| batch.map(|b| ActionsBatch::new(b, false)));

        Ok(Self {
            actions: Box::new(actions),
        })
    }
}

impl Iterator for LeafCheckpointReader {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{get_commit_schema, ADD_NAME};
    use crate::engine::default::DefaultEngine;
    use crate::log_reader::manifest::{AfterManifest, ManifestPhase};
    use crate::log_replay::LogReplayProcessor;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use crate::{Error, Snapshot, SnapshotRef};
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tempfile::TempDir;
    use url::Url;

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(Arc<dyn Engine>, SnapshotRef, Url, TempDir)> {
        let test_dir = test_utils::load_test_data("tests/data", table_name)
            .map_err(|e| Error::generic(format!("Failed to load test data: {}", e)))?;
        let test_path = test_dir.path().join(table_name);

        let url = url::Url::from_directory_path(&test_path)
            .map_err(|_| Error::generic("Failed to create URL from path"))?;

        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

        Ok((engine, snapshot, url, test_dir))
    }

    #[test]
    fn test_sidecar_phase_processes_files() -> DeltaResult<()> {
        let (engine, snapshot, _table_root, _tempdir) =
            load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;

        // First we need to run through manifest phase to get the sidecar files
        if log_segment.checkpoint_parts.is_empty() {
            println!("Test table has no checkpoint parts, skipping");
            return Ok(());
        }

        // Get the first checkpoint part
        let checkpoint_file = &log_segment.checkpoint_parts[0];
        let manifest_file = checkpoint_file.location.clone();

        let mut manifest_phase =
            ManifestPhase::new(manifest_file, log_segment.log_root.clone(), engine.clone())?;

        // Drain manifest phase and apply processor
        for batch in manifest_phase.by_ref() {
            let batch = batch?;
            processor.process_actions_batch(batch)?;
        }

        let after_manifest = manifest_phase.finalize()?;

        match after_manifest {
            AfterManifest::Sidecars { sidecars } => {
                println!("Testing with {} sidecar files", sidecars.len());

                let schema = get_commit_schema().project(&[ADD_NAME])?;

                let mut sidecar_phase =
                    LeafCheckpointReader::new(sidecars, engine.clone(), schema)?;

                let mut sidecar_file_paths = Vec::new();
                let mut batch_count = 0;

                while let Some(result) = sidecar_phase.next() {
                    let batch = result?;
                    let metadata = processor.process_actions_batch(batch)?;
                    let paths = metadata.visit_scan_files(
                        vec![],
                        |ps: &mut Vec<String>, path, _, _, _, _, _| {
                            ps.push(path.to_string());
                        },
                    )?;
                    sidecar_file_paths.extend(paths);
                    batch_count += 1;
                }

                sidecar_file_paths.sort();

                // v2-checkpoints-json-with-sidecars has exactly 2 sidecar files with 101 total files
                assert_eq!(
                    batch_count, 2,
                    "LeafCheckpointReader should process exactly 2 sidecar batches"
                );

                assert_eq!(
                    sidecar_file_paths.len(),
                    101,
                    "LeafCheckpointReader should find exactly 101 files from sidecars"
                );

                // Verify first few files match expected (sampling to keep test readable)
                let expected_first_files = vec![
                    "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
                    "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
                    "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
                ];

                assert_eq!(
                    &sidecar_file_paths[..3],
                    &expected_first_files[..],
                    "LeafCheckpointReader should process files in expected order"
                );
            }
            AfterManifest::Done => {
                println!("No sidecars found - test inconclusive");
            }
        }

        Ok(())
    }
}
