use core::panic;
use itertools::Itertools;
use log_replay::{checkpoint_file_actions_iter, CheckpointData};
use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, time::SystemTime};
use url::Url;

use crate::{
    actions::{
        get_log_schema, visitors::SidecarVisitor, Sidecar, ADD_NAME, REMOVE_NAME, SIDECAR_NAME,
    },
    path::ParsedLogPath,
    schema::StructType,
    snapshot::Snapshot,
    DeltaResult, Engine, EngineData, Error, FileMeta, RowVisitor,
};

pub mod log_replay;
#[cfg(test)]
pub mod test;
/// Trait representing checkpoint data that can be written.
/// This is the core abstraction for checkpoint data generation.
pub trait CheckpointInfo {
    /// Get information needed to write a checkpoint file.
    /// This provides the data iterator and target path for writing.
    fn get_checkpoint_info(&mut self, engine: &dyn Engine)
        -> DeltaResult<SingleFileCheckpointInfo>;
}

/// Task for an executor to write a sidecar file.
/// Each executor handles a portion of the log segment's
/// checkpoint reconciliation to generate & write its own sidecar file.
pub struct SidecarTask {
    /// Chunk of the log segment for conflict resolution
    log_segment: ChunkedLogSegment,

    /// Path where the sidecar should be written
    sidecar_path: Url,

    /// Return file actions from commit files in chunked log segment
    /// Only one executor should include commit actions to avoid duplication
    pub include_commit_actions: bool,
}

impl CheckpointInfo for SidecarTask {
    fn get_checkpoint_info(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<SingleFileCheckpointInfo> {
        // Return the checkpoint info for the sidecar task by doing action reconciliation
        // This performs executor-side materialization, allowing each executor to
        // independently perform action reconciliation on its assigned checkpoint chunk
        let data = checkpoint_file_actions_iter(
            self.log_segment.replay_chunk(engine)?,
            self.include_commit_actions,
        );

        return Ok(SingleFileCheckpointInfo {
            data: Box::new(data),
            path: self.sidecar_path.clone(),
        });
    }
}

/// Represents a chunk of a log segment for processing.
/// This is the core data structure for distributed checkpoint generation,
/// allowing each executor to process its own portion of the log.
pub struct ChunkedLogSegment {
    /// Commit files to be processed with this chunk
    pub commit_files: Vec<ParsedLogPath>,

    /// For parquet checkpoints: range of row groups to process
    pub checkpoint_row_group_range: Option<(u64, u64)>,

    /// For JSON checkpoints: range of lines to process
    pub checkpoint_json_line_range: Option<(u64, u64)>,

    /// For checkpoints with sidecars: sidecar files to process
    pub sidecar_files: Option<Vec<ParsedLogPath>>,
}

impl ChunkedLogSegment {
    /// Replays this chunk of the log segment to produce an iterator of actions.
    /// This allows for decentralized action reconciliation where each executor
    /// processes its own portion of the log independently.
    fn replay_chunk(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>>
    {
        // Create a schema that only projects the needed columns for efficiency
        let read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;

        // Read commit files in reverse order (newest first)
        let commit_stream = self.get_commit_stream(engine, read_schema.clone())?;

        // Process based on chunk type (sidecar, parquet row groups, or JSON lines)
        if self.sidecar_files.is_some() {
            self.process_sidecar_chunk(engine, read_schema.clone(), commit_stream)
        } else if let Some((start_row, end_row)) = self.checkpoint_row_group_range {
            self.process_parquet_chunk(
                engine,
                read_schema.clone(),
                commit_stream,
                start_row,
                end_row,
            )
        } else if let Some((start_line, end_line)) = self.checkpoint_json_line_range {
            self.process_json_chunk(
                engine,
                read_schema.clone(),
                commit_stream,
                start_line,
                end_line,
            )
        } else {
            // If no specific chunk type is specified, just return the commit stream
            Ok(Box::new(commit_stream))
        }
    }

    /// Gets a stream of data from commit files for reconciliation
    fn get_commit_stream(
        &self,
        engine: &dyn Engine,
        read_schema: Arc<StructType>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>>
    {
        // Read commit files in reverse order (newest first) for correct conflict resolution
        let commit_files: Vec<_> = self
            .commit_files
            .iter()
            .rev()
            .map(|f| f.location.clone())
            .collect();

        // The boolean flag indicates these are commit actions (true)
        let commit_stream = engine
            .get_json_handler()
            .read_json_files(&commit_files, read_schema.clone(), None)?
            .map_ok(|batch| (batch, true));

        Ok(Box::new(commit_stream))
    }

    /// Processes a chunk containing sidecar files alongside commit files
    fn process_sidecar_chunk(
        &self,
        engine: &dyn Engine,
        read_schema: Arc<StructType>,
        commit_stream: Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>>
    {
        let sidecar_files = self.sidecar_files.as_ref().unwrap();

        let sidecar_locations: Vec<_> = sidecar_files.iter().map(|f| f.location.clone()).collect();

        // The boolean flag indicates these are not commit actions (false)
        let sidecar_stream = engine
            .get_parquet_handler()
            .read_parquet_files(&sidecar_locations, read_schema, None)?
            .map_ok(|batch| (batch, false));

        // Merge the commit stream with the sidecar stream
        Ok(Box::new(commit_stream.chain(sidecar_stream)))
    }

    /// Processes a chunk of a Parquet file by row group ranges
    /// This allows for parallel processing of large Parquet checkpoints
    fn process_parquet_chunk(
        &self,
        _engine: &dyn Engine,
        _read_schema: Arc<StructType>,
        _commit_stream: Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>,
        _start_row: u64,
        _end_row: u64,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>>
    {
        let checkpoint = self
            .commit_files
            .first()
            .ok_or_else(|| Error::generic("Cannot replay chunk: no checkpoint file found"))?;

        panic!("TODO: Implement parquet chunk processing");
        // // Here we would use a specialized API to read a specific range of row groups
        // let row_group_stream = engine
        //     .get_parquet_handler()
        //     .read_parquet_row_groups(&checkpoint.location, read_schema, None, start_row, end_row)?
        //     .map_ok(|batch| (batch, false));

        // // Merge the commit stream with the row group stream
        // Ok(Box::new(commit_stream.chain(row_group_stream)))
    }

    /// Processes a chunk of a JSON file by line ranges
    /// This allows for parallel processing of large JSON checkpoints
    fn process_json_chunk(
        &self,
        _engine: &dyn Engine,
        _read_schema: Arc<StructType>,
        _commit_stream: Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>,
        _start_line: u64,
        _end_line: u64,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send>>
    {
        let checkpoint = self
            .commit_files
            .first()
            .ok_or_else(|| Error::generic("Cannot replay chunk: no checkpoint file found"))?;

        panic!("TODO: Implement JSON chunk processing");
        // // Here we would use a specialized API to read a specific range of JSON lines
        // let json_line_stream = engine
        //     .get_json_handler()
        //     .read_json_lines(
        //         &checkpoint.location,
        //         read_schema,
        //         None,
        //         start_line,
        //         end_line,
        //     )?
        //     .map_ok(|batch| (batch, false));

        // // Merge the commit stream with the JSON line stream
        // Ok(Box::new(commit_stream.chain(json_line_stream)))
    }
}

/// Contains information for writing a single checkpoint file
pub struct SingleFileCheckpointInfo {
    /// The target URL where the checkpoint file will be written
    pub path: Url,

    /// Iterator over checkpoint actions to be written to the file
    pub data: Box<dyn Iterator<Item = DeltaResult<CheckpointData>> + Send>,
}

/// Writer for creating the manifest file in multi-file checkpoints.
/// This is responsible for orchestrating the creation of sidecars and the manifest.
pub struct ManifestWriter {
    /// The table snapshot from which to create the checkpoint
    pub(crate) snapshot: Snapshot,

    /// Number of sidecars to generate (specified by the engine)
    pub(crate) num_sidecars: usize,
}

impl ManifestWriter {
    /// Creates a new ManifestWriter
    pub fn new(snapshot: Snapshot, num_sidecars: usize) -> Self {
        Self {
            snapshot,
            num_sidecars,
        }
    }

    /// Returns tasks for executors to generate sidecar files.
    /// Each task contains a portion of the log segment for parallel processing.
    pub fn get_sidecar_tasks(&mut self, engine: &dyn Engine) -> DeltaResult<Vec<SidecarTask>> {
        // Determine the type of checkpoint and apply appropriate chunking strategy
        let chunked_segments = self.create_chunked_segments(engine)?;

        // Generate sidecar tasks from chunked segments
        // Each task will be processed by a separate executor
        let tasks = chunked_segments
            .into_iter()
            .enumerate()
            .map(|(idx, segment)| {
                // Generate unique UUID for sidecar file name
                let uuid = uuid::Uuid::new_v4();
                let sidecar_name = format!("_sidecars/{}.parquet", uuid);

                let sidecar_path = self
                    .snapshot
                    .log_segment()
                    .log_root
                    .join(&sidecar_name)
                    .unwrap();

                // Only include commit actions in the first sidecar to avoid duplication
                Ok(SidecarTask {
                    log_segment: segment,
                    sidecar_path,
                    include_commit_actions: idx == 0,
                })
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        Ok(tasks)
    }

    /// Creates chunked segments based on the checkpoint type.
    /// This implements different chunking strategies depending on the checkpoint format.
    fn create_chunked_segments(&self, engine: &dyn Engine) -> DeltaResult<Vec<ChunkedLogSegment>> {
        // First, check if we have existing sidecars to process
        let read_sidecars = self.replay_for_sidecars(engine)?;

        if read_sidecars.is_empty() {
            // Chunk the single-file checkpoint (no existing sidecars)
            return self.chunk_single_file_checkpoint();
        } else {
            // Chunk the multi-file checkpoint (with existing sidecars)
            return self.chunk_multi_file_checkpoint(read_sidecars);
        }
    }

    /// Chunks a single-file checkpoint for parallel processing
    fn chunk_single_file_checkpoint(&self) -> DeltaResult<Vec<ChunkedLogSegment>> {
        // Chunk the single-file checkpoint
        // This would implement strategies for splitting Parquet by row groups or JSON by lines
        todo!("Implement single-file checkpoint chunking")
    }

    /// Chunks a multi-file checkpoint with existing sidecars
    /// Uses a bin-packing algorithm to distribute sidecars evenly among executors
    fn chunk_multi_file_checkpoint(
        &self,
        sidecars: Vec<Sidecar>,
    ) -> DeltaResult<Vec<ChunkedLogSegment>> {
        // Number of chunks to create (number of executors)
        let num_chunks = self.num_sidecars;

        // If we have fewer sidecars than chunks, adjust the number of chunks
        let actual_chunks = std::cmp::min(num_chunks, sidecars.len());
        if actual_chunks == 0 {
            // No sidecars, but we have a checkpoint; use single-file chunking
            return self.chunk_single_file_checkpoint();
        }

        // Initialize bins for our greedy bin packing algorithm
        // Each bin is a tuple of (total_size, Vec<ParsedLogPath>)
        let mut bin_contents: Vec<Vec<ParsedLogPath>> = vec![Vec::new(); actual_chunks];

        // Sort sidecars by size in descending order for better bin packing
        let mut sorted_sidecars = sidecars;
        sorted_sidecars.sort_by(|a, b| b.size_in_bytes.cmp(&a.size_in_bytes));

        // Create min-heap of (bin_size, bin_index) for efficient bin selection
        // Using Reverse for min-heap behavior (default is max-heap)
        let mut heap = BinaryHeap::with_capacity(actual_chunks);
        for i in 0..actual_chunks {
            heap.push(Reverse((0i64, i)));
        }

        // Distribute sidecars using greedy bin packing with heap
        for sidecar in sorted_sidecars {
            // Get bin with smallest current size
            let Reverse((bin_size, bin_idx)) = heap.pop().unwrap();

            // Convert Sidecar to ParsedLogPath
            let log_path = ParsedLogPath {
                filename: sidecar.path.clone(),
                version: 0,
                file_type: crate::path::LogPathFileType::Sidecar,
                location: FileMeta::new(
                    self.snapshot
                        .log_segment()
                        .log_root
                        .join(&sidecar.path)
                        .unwrap(),
                    0,
                    sidecar.size_in_bytes as usize,
                ),
                extension: "parquet".to_string(),
            };

            // Add sidecar to the bin
            bin_contents[bin_idx].push(log_path);

            // Update heap with new bin size
            heap.push(Reverse((bin_size + sidecar.size_in_bytes, bin_idx)));
        }

        // Convert bins to ChunkedLogSegment for executor processing
        let chunked_segments = bin_contents
            .into_iter()
            .enumerate()
            .filter_map(|(_, sidecar_files)| {
                if !sidecar_files.is_empty() {
                    Some(ChunkedLogSegment {
                        // All chunks need access to the commit files for proper reconciliation
                        commit_files: self.snapshot.log_segment().ascending_commit_files.clone(),
                        checkpoint_row_group_range: None,
                        checkpoint_json_line_range: None,
                        sidecar_files: Some(sidecar_files),
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(chunked_segments)
    }

    /// Reads the checkpoint to find sidecar metadata
    /// This determines if the checkpoint has sidecars that need to be processed
    fn replay_for_sidecars(&self, engine: &dyn Engine) -> DeltaResult<Vec<Sidecar>> {
        let checkpoint = match self.snapshot.log_segment().checkpoint_parts.first() {
            Some(parsed_log_path) => parsed_log_path,
            None => return Ok(Vec::new()), // No checkpoints to process
        };

        // Create a schema that includes the sidecar column
        let checkpoint_read_schema = get_log_schema().project(&[SIDECAR_NAME])?;

        let checkpoint_location = vec![checkpoint.location.clone()];

        // Read the checkpoint file based on its extension
        let actions = match checkpoint.extension.as_str() {
            "json" => engine.get_json_handler().read_json_files(
                &checkpoint_location,
                checkpoint_read_schema.clone(),
                None,
            )?,
            "parquet" => engine.get_parquet_handler().read_parquet_files(
                &checkpoint_location,
                checkpoint_read_schema.clone(),
                None,
            )?,
            _ => {
                return Err(Error::generic(format!(
                    "Unsupported checkpoint file type: {}",
                    checkpoint.extension,
                )));
            }
        };

        // Process the actions to extract sidecar metadata
        let mut sidecar_metadata = Vec::new();
        for action_result in actions {
            // OPTIMIZATION: If we find an add action, we can return immediately
            // since add actions cannot coexist with sidecar actions in V2 checkpoints
            let action_batch = action_result?;

            // Use the sidecar visitor to extract metadata from the checkpoint batch
            let mut visitor = SidecarVisitor::default();
            visitor.visit_rows_of(action_batch.as_ref())?;

            // Add extracted sidecars to our result
            for sidecar in visitor.sidecars {
                sidecar_metadata.push(sidecar);
            }
        }

        Ok(sidecar_metadata)
    }

    /// Finalizes sidecar writing and prepares the manifest file
    /// Called after all sidecar files have been written
    pub fn finalize_sidecars(&mut self, sidecar_metadata: &dyn EngineData) -> DeltaResult<()> {
        // Process sidecar metadata and prepare manifest file
        // This would create sidecar actions to include in the manifest
        todo!("Implement sidecar finalization")
    }

    /// Gets information needed to write the manifest file
    /// This provides the data for the top-level checkpoint file
    pub fn get_checkpoint_info(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<SingleFileCheckpointInfo> {
        // Create iterator over non-file actions and sidecar references
        todo!("Implement manifest file info extraction")
    }

    /// Finalizes the checkpoint by writing the _last_checkpoint file
    /// This is called after all checkpoint data has been written
    pub fn finalize_checkpoint(self, metadata: &dyn EngineData) -> DeltaResult<()> {
        // Write _last_checkpoint file with metadata about the checkpoint
        todo!("Implement checkpoint finalization")
    }
}

/// Builder for configuring and creating checkpoint writers
/// This is the entry point for the checkpoint API
pub struct CheckpointBuilder {
    /// The table snapshot from which to create the checkpoint
    snapshot: Snapshot,

    /// Whether to use classic naming for the checkpoint files
    with_classic_naming: bool,

    /// Number of sidecars to include in the checkpoint
    num_sidecars: usize,
}

impl CheckpointBuilder {
    /// Creates a new CheckpointBuilder with the given snapshot
    pub fn new(snapshot: Snapshot) -> Self {
        Self {
            snapshot,
            with_classic_naming: false,
            num_sidecars: 0,
        }
    }

    /// Configures the builder to use classic naming scheme
    /// Classic naming is used for backward compatibility
    pub fn with_classic_naming(mut self, with_classic_naming: bool) -> Self {
        self.with_classic_naming = with_classic_naming;
        self
    }

    /// Configures the number of sidecars to generate
    /// This determines the level of parallelism for checkpoint generation
    pub fn with_sidecar_support(mut self, num_sidecars: usize) -> Self {
        self.num_sidecars = num_sidecars;
        self
    }

    /// Builds a checkpoint writer based on the configuration
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<ManifestWriter> {
        // Check that the table supports v2Checkpoints if using sidecars
        if self.num_sidecars > 0 {
            Ok(ManifestWriter::new(self.snapshot, self.num_sidecars))
        } else {
            panic!("CheckpointBuilder must be configured with sidecar support")
        }
    }
}
#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::arrow::array::Int64Array;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowExpressionHandler;
    use crate::{ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};
    use arrow_53::json::LineDelimitedWriter;
    use std::sync::{atomic::AtomicI64, Arc};
    use std::time::Duration;
    use url::Url;

    // Helper to serialize and extract the _last_checkpoint JSON for verification
    fn as_json(data: Box<dyn EngineData>) -> serde_json::Value {
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let buf = Vec::new();
        let mut writer = LineDelimitedWriter::new(buf);
        writer.write_batches(&[&record_batch]).unwrap();
        writer.finish().unwrap();
        let buf = writer.into_inner();

        serde_json::from_slice(&buf).unwrap()
    }

    // TODO(seb): Merge with other definitions and move to a common test module
    pub(crate) struct ExprEngine(Arc<dyn ExpressionHandler>);

    impl ExprEngine {
        pub(crate) fn new() -> Self {
            ExprEngine(Arc::new(ArrowExpressionHandler))
        }
    }

    impl Engine for ExprEngine {
        fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
            self.0.clone()
        }

        fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
            unimplemented!()
        }
    }

    /// Creates a mock engine metadata batch with size_in_bytes field
    fn create_engine_metadata(size_in_bytes: i64) -> Box<dyn EngineData> {
        // Create Arrow schema with size_in_bytes field
        let schema = ArrowSchema::new(vec![Field::new("sizeInBytes", ArrowDataType::Int64, false)]);

        let size_array = Int64Array::from(vec![size_in_bytes]);
        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(size_array)])
            .expect("Failed to create record batch");
        Box::new(ArrowEngineData::new(record_batch))
    }
}
