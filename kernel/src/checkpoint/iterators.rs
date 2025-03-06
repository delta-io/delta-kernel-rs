use crate::actions::{Metadata, Protocol, Sidecar};
use crate::engine::arrow_data::ArrowEngineData;
use crate::path::ParsedLogPath;
use crate::snapshot::CheckpointMetadata;
use crate::{error, DeltaResult, Engine, EngineData, Error, Version};
use arrow_53::array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use std::iter::Peekable;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use url::Url;

use super::{get_checkpoint_metadata_schema, get_last_checkpoint_schema};

pub type FileData = Box<dyn Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send>;

/// An iterator that lazily groups incoming actions into chunks based on a threshold.
pub struct SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>>,
{
    iter: Peekable<I>,
    threshold: usize,
    table_location: Url,
    num_sidecars: usize,
    protocol: Protocol,
    metadata: Metadata,
}

impl<I> SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>>,
{
    pub fn new(
        iter: I,
        threshold: usize,
        table_location: Url,
        protocol: Protocol,
        metadata: Metadata,
    ) -> Self {
        SidecarIterator {
            iter: iter.peekable(),
            threshold,
            table_location,
            num_sidecars: 0,
            protocol,
            metadata,
        }
    }

    /// Helper to count the number of true flags in a vector.
    fn count_true_flags(bools: &[bool]) -> usize {
        bools.iter().filter(|&&flag| flag).count()
    }
}

impl<I> CheckpointIterator for SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>>,
{
    fn sidecar_metadata(
        &self,
        sidecar_metadata: Vec<Sidecar>,
    ) -> DeltaResult<V2CheckpointFileIterator> {
        Ok(V2CheckpointFileIterator {
            protocol: self.protocol.clone(),
            metadata: self.metadata.clone(),
            sidecar_metadata,
            table_location: self.table_location.clone(),
        })
    }

    fn checkpoint_metadata(
        &self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        return Err(Error::GenericError {
        source: "Writing a V2 checkpoint, and still need to write top-level V2 checkpoint file. First call sidecar() instead!".to_string().into(),
    });
    }
}

impl<I> Iterator for SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>>,
{
    type Item = DeltaResult<(FileData, Url)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();
        let mut actions_in_chunk = 0;

        // Continue as long as there’s a batch and the threshold hasn’t been reached.
        while self.iter.peek().is_some() {
            let num_selected_actions = self
                .iter
                .peek()
                .and_then(|res| res.as_ref().ok())
                .map(|(_, bools)| Self::count_true_flags(bools))
                .unwrap_or(0);

            // If the chunk is not empty and adding the next action would exceed the threshold,
            if !chunk.is_empty() && actions_in_chunk + num_selected_actions > self.threshold {
                break;
            }

            // Should not panic as we've checked for None.
            let item = self.iter.next()?;
            actions_in_chunk += num_selected_actions;
            chunk.push(item);
        }

        if chunk.is_empty() {
            return None;
        }

        // If we've exhausted the iterator and haven't yet written a sidecar file,
        // we can move the file actions into the top-level v2 checkpoint file
        if self.iter.peek().is_none() && self.num_sidecars == 0 {
            // Optimization
            todo!();
        }

        // Process chunk and create a sidecar file.
        // TODO: Follow sidecar naming convention
        let file_name = format!("{:020}.parquet", self.num_sidecars);
        self.num_sidecars += 1;
        let sidecar_path_res = self
            .table_location
            .join("_delta_log/sidecars/")
            .and_then(|url| url.join(&file_name));

        match sidecar_path_res {
            Ok(sidecar_path) => {
                // Wrap the collected Vec into an iterator for FileData.
                let sidecar_iter = Box::new(chunk.into_iter());
                Some(Ok((sidecar_iter, sidecar_path)))
            }
            Err(e) => Some(Err(error::Error::InvalidUrl(e))),
        }
    }
}

pub struct V2CheckpointFileIterator {
    protocol: Protocol,
    metadata: Metadata,
    sidecar_metadata: Vec<Sidecar>,
    table_location: Url,
}

// Writes the V2 checkpoint file and transitions to the CheckpointMetadataIterator
impl Iterator for V2CheckpointFileIterator {
    type Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.sidecar_metadata.is_empty() {
            return Some(Err(error::Error::GenericError {
                source: "Invalid state".to_string().into(),
            }));
        }

        // Convert sidecar metadata to Sidecar action and write to checkpoint file.
        // self.sidecar_metadata.pop().map(|sidecar| {
        //     let sidecar_action = sidecar.into_action();
        //     let sidecar_data = Box::new(sidecar_action);
        //     Ok((sidecar_data, vec![true]))
        // });

        None
    }
}

pub struct V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static,
{
    iter: Option<Peekable<I>>, // Store in Option to allow moving out
    table_root: Url,
    checkpoint_version: Version,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
}

impl<I> V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static,
{
    pub fn new(
        iter: I,
        table_root: Url,
        checkpoint_version: Version,
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        V1CheckpointFileIterator {
            iter: Some(iter.peekable()), // Wrap in Option
            table_root,
            checkpoint_version,
            total_actions_counter,
            total_add_actions_counter,
        }
    }

    fn last_checkpoint_record_batch(&self) -> DeltaResult<Box<dyn EngineData>> {
        let total_actions_counter: i64 =
            self.total_actions_counter
                .load(std::sync::atomic::Ordering::SeqCst) as i64;

        let total_add_actions_counter: i64 =
            self.total_add_actions_counter
                .load(std::sync::atomic::Ordering::SeqCst) as i64;

        // TODO: Return error on failure to cast. Look into correcting the types anyways.
        let version = Arc::new(Int64Array::from(vec![self.checkpoint_version as i64]));
        let size = Arc::new(Int64Array::from(vec![total_actions_counter]));
        // TODO: Parts is usize but we are using i64
        let parts = Arc::new(Int64Array::from(vec![1]));
        let num_of_add_files = Arc::new(Int64Array::from(vec![total_add_actions_counter]));
        // TODO: Checksum

        // Construct the record batch.
        let record_batch = RecordBatch::try_new(
            Arc::new(get_last_checkpoint_schema().as_ref().try_into()?),
            vec![
                version,
                size,
                parts,
                Arc::new(Int64Array::from(vec![None])), // Optional: sizeInBytes (checksum column)
                num_of_add_files,
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )?;
        Ok(Box::new(ArrowEngineData::new(record_batch)))
    }
}

impl<I> CheckpointIterator for V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static,
{
    fn sidecar_metadata(
        &self,
        _sidecar_metadata: Vec<Sidecar>,
    ) -> DeltaResult<V2CheckpointFileIterator> {
        // Error/inform the user that they should instead call checkpoint_metadata().
        return Err(Error::GenericError {
            source: "Writing a V1 checkpoint. Cannot transition to V2. Call checkpoint_metadata() instead.".to_string().into(),
        });
    }

    // Write the `_last_checkpoint` metadata file.
    fn checkpoint_metadata(
        &self,
        // Metadata must follow the schema defined in `get_checkpoint_metadata_schema()`.
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        // TODO: make sure metadata conforms to the correct schema & extract `sizeInBytes`.
        let checkpoint_metadata_schema = get_checkpoint_metadata_schema();

        let last_checkpoint_path = self
            .table_root
            .join("_delta_log/")?
            .join("_last_checkpoint.json")?;

        // TODO: leverage create_one() expression API when available?

        // Get the record batch for the metadata and wrap it into an iterator.
        let data = self.last_checkpoint_record_batch()?;
        let iter = Box::new(std::iter::once(Ok(data)));
        engine
            .get_json_handler()
            .write_json_file(&last_checkpoint_path, iter, true)?;

        Ok(())
    }
}

impl<I> Iterator for V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static,
{
    type Item = DeltaResult<(FileData, Url)>;

    // we need to move self.iter out of self, but self.iter is behind a mutable reference (&mut self).
    // Since we don't need self.iter anymore after calling next() once, we can use Option::take() to move it out.
    fn next(&mut self) -> Option<Self::Item> {
        let v1_checkpoint_file_path =
            match ParsedLogPath::new_v1_checkpoint(&self.table_root, self.checkpoint_version) {
                Ok(path) => path,
                Err(_err) => {
                    return Some(Err(Error::Generic(
                        "Error parsing checkpoint file path".to_string(),
                    )))
                }
            };

        // We call take() on self.iter because we expect only a single chunk.
        self.iter
            .take()
            .map(|iter| Ok((Box::new(iter) as FileData, v1_checkpoint_file_path.location)))
    }
}

pub trait CheckpointIterator: Iterator<Item = DeltaResult<(FileData, Url)>> {
    /// Transition state into writing the top-level V2 checkpoint file.
    fn sidecar_metadata(
        &self,
        sidecar_metadata: Vec<Sidecar>,
    ) -> DeltaResult<V2CheckpointFileIterator>;

    // Transition state into writing the `_last_checkpoint` metadata file.
    fn checkpoint_metadata(
        &self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()>;
}
