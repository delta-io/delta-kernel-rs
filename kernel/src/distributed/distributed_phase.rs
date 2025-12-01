use std::sync::Arc;

use crate::{
    log_reader::leaf::LeafCheckpointReader, log_replay::ParallelizableLogReplayProcessor,
    scan::CHECKPOINT_READ_SCHEMA, DeltaResult, Engine, FileMeta,
};

struct DistributedPhase<P: ParallelizableLogReplayProcessor> {
    processor: P,
    leaf_checkpoint_reader: LeafCheckpointReader,
}

impl<P: ParallelizableLogReplayProcessor> DistributedPhase<P> {
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        processor: P,
        sidecars: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let leaf_checkpoint_reader =
            LeafCheckpointReader::new(sidecars, engine, CHECKPOINT_READ_SCHEMA.clone())?;
        Ok(Self {
            processor,
            leaf_checkpoint_reader,
        })
    }
}

impl<P: ParallelizableLogReplayProcessor> Iterator for DistributedPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.leaf_checkpoint_reader.next().map(|batch_res| {
            batch_res.and_then(|batch| self.processor.process_actions_batch(batch))
        })
    }
}
