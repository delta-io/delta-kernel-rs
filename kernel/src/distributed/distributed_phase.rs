use crate::{
    log_replay::{LogReplayProcessor, ParallelizableLogReplayProcessor},
    FileMeta,
};

struct DistributedPhase<P: ParallelizableLogReplayProcessor> {
    processor: P,
    sidecar_reader: RowGroupMetaDataBuilder,
}

impl<P: ParallelizableLogReplayProcessor> DistributedPhase<P> {
    pub(crate) fn try_new(processor: P, sidecars: Vec<FileMeta>) -> Self {
        todo!()
    }
}
