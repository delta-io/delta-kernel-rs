//! Commit phase for log replay - processes JSON commit files.

use std::sync::Arc;

use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine};

/// Phase that processes JSON commit files.
pub(crate) struct CommitReader {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl CommitReader {
    /// Create a new commit phase from a log segment.
    ///
    /// # Parameters
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    pub(crate) fn try_new(
        engine: &dyn Engine,
        log_segment: &LogSegment,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let commit_files = log_segment.find_commit_cover();
        let actions = engine
            .json_handler()
            .read_json_files(&commit_files, schema, None)?
            .map(|batch| batch.map(|b| ActionsBatch::new(b, true)));

        Ok(Self {
            actions: Box::new(actions),
        })
    }
}

impl Iterator for CommitReader {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions.next()
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
    fn test_commit_phase_processes_commits() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut commit_phase = CommitPhase::try_new(&log_segment, engine.clone())?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();

        for result in commit_phase {
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

        // table-without-dv-small has exactly 1 commit file
        assert_eq!(
            batch_count, 1,
            "table-without-dv-small should have exactly 1 commit batch"
        );

        // table-without-dv-small has exactly 1 add file
        file_paths.sort();
        let expected_files =
            vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "CommitPhase should find exactly the expected file"
        );

        Ok(())
    }
}
