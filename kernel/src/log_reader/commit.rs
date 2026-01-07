//! Commit phase for log replay - processes JSON commit files.

use itertools::Itertools;

use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine};

/// Phase that processes JSON commit files into [`ActionsBatch`]s
pub(crate) struct CommitReader {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl CommitReader {
    /// Create a new commit phase from a log segment.
    ///
    /// # Parameters
    /// - `engine`: Engine for reading files
    /// - `log_segment`: The log segment to process
    /// - `schema`: The schema to read the json files
    pub(crate) fn try_new(
        engine: &dyn Engine,
        log_segment: &LogSegment,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let commit_files = log_segment.find_commit_cover();
        let actions = engine
            .json_handler()
            .read_json_files(&commit_files, schema, None)?
            .map_ok(|batch| ActionsBatch::new(batch, true));

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
    use crate::arrow::array::{StringArray, StructArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::scan::COMMIT_READ_SCHEMA;
    use crate::utils::test_utils::load_test_table;
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_commit_phase_processes_commits() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot, _tempdir) = load_test_table("app-txn-no-checkpoint")?;
        let log_segment = Arc::new(snapshot.log_segment().clone());

        let schema = COMMIT_READ_SCHEMA.clone();
        let commit_phase = CommitReader::try_new(engine.as_ref(), &log_segment, schema)?;

        let mut file_paths = vec![];
        for result in commit_phase {
            let batch = result?;
            let ActionsBatch {
                actions,
                is_log_batch,
            } = batch;
            assert!(is_log_batch);

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

        file_paths.sort();
        let expected_files = vec![
            "modified=2021-02-01/part-00001-80996595-a345-43b7-b213-e247d6f091f7-c000.snappy.parquet",
            "modified=2021-02-01/part-00001-8ebcaf8b-0f48-4213-98c9-5c2156d20a7e-c000.snappy.parquet",
            "modified=2021-02-02/part-00001-9a16b9f6-c12a-4609-a9c4-828eacb9526a-c000.snappy.parquet",
            "modified=2021-02-02/part-00001-bfac5c74-426e-410f-ab74-21a64e518e9c-c000.snappy.parquet",
        ];
        assert_eq!(
            file_paths, expected_files,
            "CommitReader should find exactly the expected files"
        );

        Ok(())
    }
}
