use crate::actions::{Metadata, Protocol, Sidecar};
use crate::engine::arrow_data::ArrowEngineData;
use crate::path::ParsedLogPath;
use crate::snapshot::{self, CheckpointMetadata, Snapshot};
use crate::{error, DeltaResult, Engine, EngineData, Error, Version};
use arrow_53::array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use std::iter::Peekable;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use url::Url;

use super::log_replay::v2_checkpoint_actions_iter;
use super::{get_last_checkpoint_schema, get_sidecar_schema, get_v2_checkpoint_schema};

/// The filtered version of engine data with a boolean vector indicating which actions to process
pub type FilteredEngineData = (Box<dyn EngineData>, Vec<bool>);

/// Iterator for file data
pub type FileData = Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>;

/// A tuple containing file data and its associated URL
pub type FileDataWithUrl = (FileData, Url);

/// Iterator for file data with URLs
pub type FileDataIter = Box<dyn Iterator<Item = DeltaResult<FileDataWithUrl>> + Send>;

/// Iterator for V1 checkpoint files
/// Returns a single file that contains all checkpoint data
pub struct V1CheckpointFileIterator {
    /// The underlying data iterator, wrapped in Option to allow consumption
    iter: Option<Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>>,
    /// Root URL of the Delta table
    table_root: Url,
    /// Version of the checkpoint
    checkpoint_version: Version,
    /// Counter tracking the total number of actions
    total_actions_counter: Arc<AtomicUsize>,
    /// Counter tracking the total number of add actions
    total_add_actions_counter: Arc<AtomicUsize>,
}

impl V1CheckpointFileIterator {
    /// Creates a new V1 checkpoint file iterator
    pub fn new(
        iter: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>,
        table_root: Url,
        checkpoint_version: Version,
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        V1CheckpointFileIterator {
            iter: Some(iter),
            table_root,
            checkpoint_version,
            total_actions_counter,
            total_add_actions_counter,
        }
    }

    /// Consumes the iterator and returns the underlying data with the checkpoint file path
    /// This method can only be called once per iterator instance
    pub fn get_file_data_iter(&mut self) -> DeltaResult<FileDataWithUrl> {
        let v1_checkpoint_file_path =
            ParsedLogPath::new_v1_checkpoint(&self.table_root, self.checkpoint_version)?;

        match self.iter.take() {
            Some(iter) => Ok((Box::new(iter) as FileData, v1_checkpoint_file_path.location)),
            None => Err(Error::Generic(
                "Iterator has already been consumed".to_string(),
            )),
        }
    }

    /// Writes the checkpoint metadata file (_last_checkpoint.json)
    /// This is the final step in the V1 checkpoint process
    pub fn checkpoint_metadata(
        self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        let last_checkpoint_path = self
            .table_root
            .join("_delta_log/")?
            .join("_last_checkpoint.json")?;

        let data = last_checkpoint_record_batch(
            self.checkpoint_version,
            self.total_actions_counter,
            self.total_add_actions_counter,
        )?;
        let iter_data = Box::new(std::iter::once(Ok(data)));
        engine
            .get_json_handler()
            .write_json_file(&last_checkpoint_path, iter_data, true)?;

        Ok(())
    }
}

/// Iterator for sidecars in V2 checkpoints
/// This is an intermediate stage for V2 checkpoints
pub struct SidecarIterator {
    /// The underlying data iterator, wrapped in Option to allow consumption
    iter: Option<Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>>,
    /// Root URL of the Delta table
    table_location: Url,
    /// Snapshot representing the state of the table
    snapshot: Snapshot,
    /// Counter tracking the total number of actions
    total_actions_counter: Arc<AtomicUsize>,
    /// Counter tracking the total number of add actions
    total_add_actions_counter: Arc<AtomicUsize>,
}

impl SidecarIterator {
    /// Creates a new sidecar iterator for V2 checkpoints
    pub fn new(
        iter: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>,
        table_location: Url,
        snapshot: Snapshot,
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        SidecarIterator {
            iter: Some(iter),
            table_location,
            snapshot,
            total_actions_counter,
            total_add_actions_counter,
        }
    }

    /// Consumes the iterator and returns the underlying data with the sidecar file path
    pub fn get_file_data_iter(&mut self) -> DeltaResult<FileDataWithUrl> {
        match self.iter.take() {
            Some(iter) => {
                // Generate a simple sidecar path
                let file_name = "sidecar.parquet";
                let sidecar_path = self
                    .table_location
                    .join("_delta_log/sidecars/")?
                    .join(file_name)?;

                Ok((iter, sidecar_path))
            }
            None => Err(Error::Generic(
                "Iterator has already been consumed".to_string(),
            )),
        }
    }

    /// Generates V2 checkpoint files with sidecar metadata
    /// Transitions from the sidecar generation stage to the final V2 checkpoint stage
    pub fn sidecar_metadata(
        self,
        sidecar_metadata: Vec<Sidecar>,
        engine: &dyn Engine,
    ) -> DeltaResult<V2CheckpointFileIterator> {
        let schema = get_v2_checkpoint_schema();

        // Replay the log segment to get checkpoint actions
        let inner_iter =
            self.snapshot
                .log_segment()
                .replay(engine, schema.clone(), schema.clone(), None)?;

        // Create action iterator with counter tracking
        let actions_iter =
            v2_checkpoint_actions_iter(inner_iter, self.total_actions_counter.clone());

        // Create sidecar action batches from metadata
        let sidecar_batches = create_sidecar_batches(sidecar_metadata)?;

        // Combine the action iterator with generated sidecar action batches
        let total_actions_iter = actions_iter.chain(sidecar_batches.into_iter());

        let boxed_total_actions_iter: Box<
            dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send,
        > = Box::new(total_actions_iter);

        // Transition to V2CheckpointFileIterator
        Ok(V2CheckpointFileIterator {
            iter: Some(boxed_total_actions_iter),
            table_root: self.table_location,
            checkpoint_version: self.snapshot.version(),
            total_actions_counter: self.total_actions_counter,
            total_add_actions_counter: self.total_add_actions_counter,
        })
    }
}

/// Final iterator for V2 checkpoint files after sidecar generation
/// Responsible for writing the main V2 checkpoint file
pub struct V2CheckpointFileIterator {
    /// The underlying data iterator, wrapped in Option to allow consumption
    iter: Option<Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>>,
    /// Root URL of the Delta table
    table_root: Url,
    /// Version of the checkpoint
    checkpoint_version: Version,
    /// Counter tracking the total number of actions
    total_actions_counter: Arc<AtomicUsize>,
    /// Counter tracking the total number of add actions
    total_add_actions_counter: Arc<AtomicUsize>,
}

impl V2CheckpointFileIterator {
    /// Consumes the iterator and returns the underlying data with the V2 checkpoint file path
    /// This method can only be called once per iterator instance
    pub fn get_file_data_iter(&mut self) -> DeltaResult<FileDataWithUrl> {
        match self.iter.take() {
            Some(iter) => {
                let file_data = Box::new(iter) as FileData;

                // Generate the V2 checkpoint file path with consistent formatting
                let checkpoint_path = self.table_root.join("_delta_log/")?.join(&format!(
                    "{:020}.checkpoint.parquet",
                    self.checkpoint_version
                ))?;

                Ok((file_data, checkpoint_path))
            }
            None => Err(Error::Generic(
                "Iterator has already been consumed".to_string(),
            )),
        }
    }

    /// Writes the checkpoint metadata file (_last_checkpoint.json)
    /// This is the final step in the V2 checkpoint process
    pub fn checkpoint_metadata(
        self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        let last_checkpoint_path = self
            .table_root
            .join("_delta_log/")?
            .join("_last_checkpoint.json")?;

        let data = last_checkpoint_record_batch(
            self.checkpoint_version,
            self.total_actions_counter,
            self.total_add_actions_counter,
        )?;
        let iter_data = Box::new(std::iter::once(Ok(data)));
        engine
            .get_json_handler()
            .write_json_file(&last_checkpoint_path, iter_data, true)?;

        Ok(())
    }
}

/// Creates batches of sidecar entries from metadata
/// Each sidecar is converted to a record batch with its path, size and modification time
fn create_sidecar_batches(
    metadata: Vec<Sidecar>,
) -> DeltaResult<Vec<DeltaResult<FilteredEngineData>>> {
    let mut result = Vec::with_capacity(metadata.len());

    for sidecar in metadata {
        // Create arrays for each sidecar field
        let path = Arc::new(StringArray::from(vec![sidecar.path]));
        let size_in_bytes = Arc::new(Int64Array::from(vec![sidecar.size_in_bytes]));
        let modification_time = Arc::new(Int64Array::from(vec![sidecar.modification_time]));

        // Create a record batch with the sidecar schema
        let record_batch = RecordBatch::try_new(
            Arc::new(get_sidecar_schema().as_ref().try_into()?),
            vec![path, size_in_bytes, modification_time],
        )?;

        // Add to results, marking all entries as included (true in the boolean vector)
        result.push(Ok((
            Box::new(ArrowEngineData::new(record_batch)) as Box<dyn EngineData>,
            vec![true],
        )));
    }

    Ok(result)
}

/// Creates a record batch for the _last_checkpoint.json file
/// This contains metadata about the checkpoint, such as version and size
fn last_checkpoint_record_batch(
    version: Version,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
) -> DeltaResult<Box<dyn EngineData>> {
    // Load counter values atomically
    let total_actions = total_actions_counter.load(std::sync::atomic::Ordering::SeqCst) as i64;
    let total_add_actions =
        total_add_actions_counter.load(std::sync::atomic::Ordering::SeqCst) as i64;

    // Create arrays for each field in the checkpoint metadata
    let version = Arc::new(Int64Array::from(vec![version as i64]));
    let size = Arc::new(Int64Array::from(vec![total_actions]));
    let parts = Arc::new(Int64Array::from(vec![1]));
    let num_of_add_files = Arc::new(Int64Array::from(vec![total_add_actions]));

    // Construct the record batch with the last checkpoint schema
    let record_batch = RecordBatch::try_new(
        Arc::new(get_last_checkpoint_schema().as_ref().try_into()?),
        vec![
            version,
            size,
            parts,
            Arc::new(Int64Array::from(vec![None])), // Optional: sizeInBytes (checksum column)
            num_of_add_files,
            Arc::new(StringArray::from(vec![None::<&str>])), // Optional: checksum
        ],
    )?;
    Ok(Box::new(ArrowEngineData::new(record_batch)))
}
