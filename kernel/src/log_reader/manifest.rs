//! Manifest phase for log replay - processes single-part checkpoint manifest files.

use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use url::Url;

use crate::actions::{get_all_actions_schema, visitors::SidecarVisitor, SIDECAR_NAME};
use crate::actions::{get_commit_schema, Sidecar, ADD_NAME};
use crate::expressions::Transform;
use crate::log_replay::ActionsBatch;
use crate::schema::{Schema, SchemaRef, StructField, ToSchema};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Expression, ExpressionEvaluator, FileMeta, RowVisitor};

/// Phase that processes single-part checkpoint manifest files.
///
/// Extracts sidecar references while processing the manifest.
pub(crate) struct ManifestPhase {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
    sidecar_visitor: SidecarVisitor,
    manifest_file: FileMeta,
    log_root: Url,
    is_complete: bool,
}

/// Possible transitions after ManifestPhase completes.
pub(crate) enum AfterManifest {
    /// Has sidecars → return sidecar files
    Sidecars { sidecars: Vec<FileMeta> },
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
    pub fn new(
        manifest_file: FileMeta,
        log_root: Url,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        #[allow(clippy::unwrap_used)]
        static MANIFEST_READ_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
            get_commit_schema()
                .project(&[ADD_NAME, SIDECAR_NAME])
                .unwrap()
        });

        let files = vec![manifest_file.clone()];

        // Determine file type from extension
        let extension = manifest_file
            .location
            .path()
            .rsplit('.')
            .next()
            .unwrap_or("");

        let actions = match extension {
            "json" => {
                engine
                    .json_handler()
                    .read_json_files(&files, MANIFEST_READ_SCHMEA.clone(), None)?
            }
            "parquet" => engine.parquet_handler().read_parquet_files(
                &files,
                MANIFEST_READ_SCHMEA.clone(),
                None,
            )?,
            ext => {
                return Err(Error::generic(format!(
                    "Unsupported checkpoint extension: {}",
                    ext
                )))
            }
        };

        let actions = actions.map(|batch| batch.map(|b| ActionsBatch::new(b, false)));

        Ok(Self {
            actions: Box::new(actions),
            sidecar_visitor: SidecarVisitor::default(),
            log_root,
            manifest_file,
            is_complete: false,
        })
    }

    /// Transition to the next phase.
    ///
    /// Returns an enum indicating what comes next:
    /// - `Sidecars`: Extracted sidecar files
    /// - `Done`: No sidecars found
    pub(crate) fn finalize(self) -> DeltaResult<AfterManifest> {
        require!(
            self.is_complete,
            Error::generic(format!(
                "Finalized called on ManifestReader for file {:?}",
                self.manifest_file.location
            ))
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
            Ok(AfterManifest::Sidecars { sidecars })
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
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::log_replay::LogReplayProcessor;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use object_store::local::LocalFileSystem;
    use std::path::PathBuf;
    use std::sync::Arc as StdArc;

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(
        StdArc<DefaultEngine<TokioBackgroundExecutor>>,
        StdArc<crate::Snapshot>,
        url::Url,
    )> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(table_name);

        let path = std::fs::canonicalize(path)
            .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;

        let url = url::Url::from_directory_path(path)
            .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;

        let store = StdArc::new(LocalFileSystem::new());
        let engine = StdArc::new(DefaultEngine::new(store));
        let snapshot = crate::Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

        Ok((engine, snapshot, url))
    }

    #[test]
    fn test_manifest_phase_with_checkpoint() -> DeltaResult<()> {
        // Use a table with v2 checkpoints where adds might be in sidecars
        let (engine, snapshot, log_root) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();

        // Check if there are any checkpoint parts
        if log_segment.checkpoint_parts.is_empty() {
            println!("Test table has no checkpoint parts, skipping");
            return Ok(());
        }

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;

        // Get the first checkpoint part
        let checkpoint_file = &log_segment.checkpoint_parts[0];
        let manifest_file = checkpoint_file.location.clone();

        let mut manifest_phase =
            ManifestPhase::new(manifest_file, log_root.clone(), engine.clone())?;

        // Count batches and collect results
        let mut batch_count = 0;
        let mut file_paths = Vec::new();

        while let Some(result) = manifest_phase.next() {
            let batch = result?;
            let metadata = processor.process_actions_batch(batch)?;
            let paths = metadata.visit_scan_files(
                vec![],
                |ps: &mut Vec<String>, path, _, _, _, _, _| {
                    ps.push(path.to_string());
                },
            )?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // For v2 checkpoints with sidecars, the manifest might not contain adds directly.
        // In this test table, all adds are in sidecars, so manifest should be empty.
        assert_eq!(
            batch_count, 1,
            "Single manifest file should produce exactly 1 batch"
        );

        // Verify the manifest itself contains no add files (they're all in sidecars)
        file_paths.sort();
        assert_eq!(
            file_paths.len(), 0,
            "For this v2 checkpoint with sidecars, manifest should contain 0 add files (all in sidecars)"
        );

        Ok(())
    }

    #[test]
    fn test_manifest_phase_collects_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, log_root) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();

        if log_segment.checkpoint_parts.is_empty() {
            println!("Test table has no checkpoint parts, skipping");
            return Ok(());
        }

        let checkpoint_file = &log_segment.checkpoint_parts[0];
        let manifest_file = checkpoint_file.location.clone();

        let schema = crate::actions::get_commit_schema().project(&[crate::actions::ADD_NAME])?;

        let mut manifest_phase =
            ManifestPhase::new(manifest_file, log_root.clone(), engine.clone())?;

        // Drain the phase
        while manifest_phase.next().is_some() {}

        // Check if sidecars were collected
        let next = manifest_phase.finalize()?;

        match next {
            AfterManifest::Sidecars { sidecars } => {
                // For the v2-checkpoints-json-with-sidecars test table at version 6,
                // there are exactly 2 sidecar files
                assert_eq!(
                    sidecars.len(),
                    2,
                    "Should collect exactly 2 sidecars for checkpoint at version 6"
                );

                // Extract and verify the sidecar paths
                let mut collected_paths: Vec<String> = sidecars
                    .iter()
                    .map(|fm| {
                        // Get the filename from the URL path
                        fm.location
                            .path_segments()
                            .and_then(|segments| segments.last())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                collected_paths.sort();

                // Verify they're the expected sidecar files for version 6
                assert_eq!(collected_paths[0], "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet");
                assert_eq!(collected_paths[1], "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet");
            }
            AfterManifest::Done => {
                panic!("Expected sidecars for v2-checkpoints-json-with-sidecars table");
            }
        }

        Ok(())
    }
}
