//! Manifest phase for log replay - processes single-part checkpoints and manifest checkpoints.

use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{Add, Remove, Sidecar, ADD_NAME};
use crate::actions::{REMOVE_NAME, SIDECAR_NAME};
use crate::log_replay::ActionsBatch;
use crate::path::ParsedLogPath;
use crate::schema::{SchemaRef, StructField, StructType, ToSchema};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta, RowVisitor};

/// Phase that processes single-part checkpoint. This also treats the checkpoint as a manifest file
/// and extracts the sidecar actions during iteration.
#[allow(unused)]
pub(crate) struct ManifestPhase {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
    sidecar_visitor: SidecarVisitor,
    log_root: Url,
    is_complete: bool,
}

/// Possible transitions after ManifestPhase completes.
#[allow(unused)]
pub(crate) enum AfterManifest {
    /// Sidecars extracted from the manifest phase
    Sidecars(Vec<FileMeta>),
    /// No sidecars
    Done,
}

impl ManifestPhase {
    /// Create a new manifest phase for a single-part checkpoint.
    ///
    /// The schema is automatically augmented with the sidecar column since the manifest
    /// phase needs to extract sidecar references for phase transitions.
    ///
    /// # Parameters
    /// - `manifest_file`: The checkpoint manifest file to process
    /// - `log_root`: Root URL for resolving sidecar paths
    /// - `engine`: Engine for reading files
    #[allow(unused)]
    pub(crate) fn try_new(
        manifest: &ParsedLogPath,
        log_root: Url,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        static MANIFEST_READ_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
            Arc::new(StructType::new_unchecked([
                StructField::nullable(ADD_NAME, Add::to_schema()),
                StructField::nullable(REMOVE_NAME, Remove::to_schema()),
                StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
            ]))
        });

        let actions = match manifest.extension.as_str() {
            "json" => engine.json_handler().read_json_files(
                std::slice::from_ref(&manifest.location),
                MANIFEST_READ_SCHMEA.clone(),
                None,
            )?,
            "parquet" => engine.parquet_handler().read_parquet_files(
                std::slice::from_ref(&manifest.location),
                MANIFEST_READ_SCHMEA.clone(),
                None,
            )?,
            extension => {
                return Err(Error::generic(format!(
                    "Unsupported checkpoint extension: {}",
                    extension
                )));
            }
        };

        let actions = Box::new(actions.map(|batch_res| Ok(ActionsBatch::new(batch_res?, false))));
        Ok(Self {
            actions,
            sidecar_visitor: SidecarVisitor::default(),
            log_root,
            is_complete: false,
        })
    }

    /// Transition to the next phase.
    ///
    /// Returns an enum indicating what comes next:
    /// - `Sidecars`: Extracted sidecar files
    /// - `Done`: No sidecars found
    #[allow(unused)]
    pub(crate) fn finalize(self) -> DeltaResult<AfterManifest> {
        require!(
            self.is_complete,
            Error::generic("Finalize called on manifest reader but it was not exausted")
        );

        let sidecars: Vec<_> = self
            .sidecar_visitor
            .sidecars
            .into_iter()
            .map(|s| s.to_filemeta(&self.log_root))
            .try_collect()?;

        if sidecars.is_empty() {
            Ok(AfterManifest::Done)
        } else {
            Ok(AfterManifest::Sidecars(sidecars))
        }
    }
}

impl Iterator for ManifestPhase {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.actions.next().map(|batch_result| {
            batch_result.and_then(|batch| {
                self.sidecar_visitor.visit_rows_of(batch.actions())?;
                Ok(batch)
            })
        });

        if result.is_none() {
            self.is_complete = true;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::{
        assert_result_error_with_message, create_engine_and_snapshot_from_path,
        load_extracted_test_table,
    };

    use crate::SnapshotRef;
    use std::sync::Arc;
    use test_utils::load_test_data;

    /// Core helper function to test manifest phase with expected add paths and sidecars
    fn verify_manifest_phase(
        engine: Arc<dyn Engine>,
        snapshot: SnapshotRef,
        expected_add_paths: &[&str],
        expected_sidecars: &[&str],
    ) -> DeltaResult<()> {
        use crate::arrow::array::{Array, StringArray, StructArray};
        use crate::engine::arrow_data::EngineDataArrowExt as _;
        use itertools::Itertools;

        let log_segment = snapshot.log_segment();
        let log_root = log_segment.log_root.clone();
        assert_eq!(log_segment.checkpoint_parts.len(), 1);
        let checkpoint_file = &log_segment.checkpoint_parts[0];
        let mut manifest_phase = ManifestPhase::try_new(checkpoint_file, log_root, engine.clone())?;

        // Extract add file paths and verify expectations
        let mut file_paths = vec![];
        for result in manifest_phase.by_ref() {
            let batch = result?;
            let ActionsBatch {
                actions,
                is_log_batch,
            } = batch;
            assert!(!is_log_batch, "Manifest should not be a log batch");

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
            file_paths.extend(batch_paths);
        }

        // Verify collected add paths
        file_paths.sort();
        assert_eq!(
            file_paths, expected_add_paths,
            "ManifestPhase should extract expected Add file paths from checkpoint"
        );

        // Check sidecars
        let next = manifest_phase.finalize()?;

        match (next, expected_sidecars) {
            (AfterManifest::Sidecars(sidecars), []) => {
                panic!("Expected to be Done, but found Sidecars: {:?}", sidecars)
            }
            (AfterManifest::Done, []) => { /* Empty expected sidecars is Done */ }
            (AfterManifest::Done, sidecars) => {
                panic!("Expected manifest phase to be Done, but got {:?}", sidecars)
            }
            (AfterManifest::Sidecars(sidecars), expected_sidecars) => {
                assert_eq!(
                    sidecars.len(),
                    expected_sidecars.len(),
                    "Should collect exactly {} sidecars",
                    expected_sidecars.len()
                );

                // Extract and verify the sidecar paths
                let mut collected_paths: Vec<String> = sidecars
                    .iter()
                    .map(|fm| {
                        fm.location
                            .path_segments()
                            .and_then(|mut segments| segments.next_back())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                collected_paths.sort();
                // Verify they're the expected sidecar files
                assert_eq!(collected_paths, expected_sidecars.to_vec());
            }
        }

        Ok(())
    }

    /// Helper function to test manifest phase with expected add paths and sidecars.
    /// Works with both compressed (tar.zst) and already-extracted test tables.
    fn test_manifest_phase(
        table_name: &str,
        expected_add_paths: &[&str],
        expected_sidecars: &[&str],
    ) -> DeltaResult<()> {
        // Try loading as compressed table first, fall back to extracted
        let (engine, snapshot, _tempdir) = match load_test_data("tests/data", table_name) {
            Ok(test_dir) => {
                let test_path = test_dir.path().join(table_name);
                let (engine, snapshot) = create_engine_and_snapshot_from_path(&test_path)?;
                (engine, snapshot, Some(test_dir))
            }
            Err(_) => {
                let (engine, snapshot) = load_extracted_test_table(table_name)?;
                (engine, snapshot, None)
            }
        };

        verify_manifest_phase(engine, snapshot, expected_add_paths, expected_sidecars)
    }

    #[test]
    fn test_manifest_phase_extracts_file_paths() -> DeltaResult<()> {
        test_manifest_phase(
            "with_checkpoint_no_last_checkpoint",
            &["part-00000-a190be9e-e3df-439e-b366-06a863f51e99-c000.snappy.parquet"],
            &[], // No sidecars
        )
    }

    #[test]
    fn test_manifest_phase_early_finalize_error() -> DeltaResult<()> {
        let (engine, snapshot) = load_extracted_test_table("with_checkpoint_no_last_checkpoint")?;

        let manifest_phase = ManifestPhase::try_new(
            &snapshot.log_segment().checkpoint_parts[0],
            snapshot.log_segment().log_root.clone(),
            engine.clone(),
        )?;

        let result = manifest_phase.finalize();
        assert_result_error_with_message(result, "not exausted");
        Ok(())
    }

    #[test]
    fn test_manifest_phase_collects_sidecars() -> DeltaResult<()> {
        test_manifest_phase(
            "v2-checkpoints-json-with-sidecars",
            &[], // No add paths in manifest (they're in sidecars)
            &[
                "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet",
            ],
        )
    }

    #[test]
    fn test_manifest_phase_collects_sidecars_parquet() -> DeltaResult<()> {
        test_manifest_phase(
            "v2-checkpoints-parquet-with-sidecars",
            &[], // No add paths in manifest (they're in sidecars)
            &[
                "00000000000000000006.checkpoint.0000000001.0000000002.76931b15-ead3-480d-b86c-afe55a577fc3.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.4367b29c-0e87-447f-8e81-9814cc01ad1f.parquet",
            ],
        )
    }
}
