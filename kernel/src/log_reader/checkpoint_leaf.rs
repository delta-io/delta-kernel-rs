//! Sidecar phase for log replay - processes sidecar/leaf parquet files.

use std::sync::Arc;

use itertools::Itertools;

use crate::log_replay::ActionsBatch;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, FileMeta};

/// Phase that processes a leaf-level checkpoint file. A leaf-level checkpoint is any checkpoint
/// file that does not reference another checkpoint file. This includes:
/// - Sidecar files in a table with V2-Checkpoint that contains a manifest file
/// - Multi-part checkpoint files
/// - Single-part checkpoint files
#[allow(unused)]
pub(crate) struct CheckpointLeafReader {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl CheckpointLeafReader {
    /// Create a new sidecar phase from file list.
    ///
    /// # Parameters
    /// - `files`: Sidecar/leaf files to process
    /// - `engine`: Engine for reading files
    /// - `schema`: Schema to use when reading sidecar files (projected based on processor requirements)
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let actions = engine
            .parquet_handler()
            .read_parquet_files(&files, schema, None)?
            .map_ok(|batch| ActionsBatch::new(batch, false));

        Ok(Self {
            actions: Box::new(actions),
        })
    }
}

impl Iterator for CheckpointLeafReader {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{get_commit_schema, ADD_NAME};
    use crate::arrow::array::{Array, StringArray, StructArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::log_reader::checkpoint_manifest::CheckpointManifestReader;
    use crate::utils::test_utils::load_test_table;
    use itertools::Itertools;

    #[test]
    fn test_sidecar_phase_processes_files() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();
        assert_eq!(
            log_segment.checkpoint_parts.len(),
            1,
            "There should be a single manifest checkpoint"
        );

        let mut manifest_phase = CheckpointManifestReader::try_new(
            engine.clone(),
            &log_segment.checkpoint_parts[0],
            log_segment.log_root.clone(),
        )?;

        // Drain manifest phase
        for batch in manifest_phase.by_ref() {
            let _batch = batch?;
        }

        let sidecars = manifest_phase.extract_sidecars()?;
        assert_eq!(sidecars.len(), 2, "There should be two sidecar files");

        let schema = get_commit_schema().project(&[ADD_NAME])?;
        let sidecar_phase = CheckpointLeafReader::try_new(engine.clone(), sidecars, schema)?;

        let mut sidecar_file_paths = Vec::new();
        for result in sidecar_phase {
            let batch = result?;
            let ActionsBatch {
                actions,
                is_log_batch,
            } = batch;
            assert!(!is_log_batch, "Sidecars should not be log batches");

            let record_batch = actions.try_into_record_batch()?;
            let add = record_batch.column_by_name("add").unwrap();
            let add_struct = add.as_any().downcast_ref::<StructArray>().unwrap();
            let path = add_struct
                .column_by_name("path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let batch_paths = path.iter().flatten().map(ToString::to_string).collect_vec();
            sidecar_file_paths.extend(batch_paths);
        }

        sidecar_file_paths.sort();

        assert_eq!(
            sidecar_file_paths.len(),
            101,
            "CheckpointLeafReader should find exactly 101 files from sidecars"
        );

        // Verify first few files match expected (sampling to keep test readable)
        let expected_first_files = [
            "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
        ];

        assert_eq!(
            &sidecar_file_paths[..3],
            &expected_first_files[..],
            "CheckpointLeafReader should process files in expected order"
        );

        Ok(())
    }
}
