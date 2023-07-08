use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::prelude::*;
use futures::stream::{BoxStream, StreamExt};
use futures::task::{Context, Poll};
use object_store::ObjectStore;

use super::data_skipping::data_skipping_filter;
use crate::expressions::Expression;
use crate::snapshot::replay::{LogReplay, LogSegment};
use crate::DeltaResult;

pub struct LogStream {
    stream: BoxStream<'static, DeltaResult<RecordBatch>>,
    log_replay: LogReplay,
    predicate: Option<Expression>,
}

impl std::fmt::Debug for LogStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Snapshot")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl LogStream {
    pub(crate) fn new(
        stream: BoxStream<'static, DeltaResult<RecordBatch>>,
        predicate: Option<Expression>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            predicate,
            stream,
            log_replay: LogReplay::new(),
        })
    }
}

impl Stream for LogStream {
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<<Self as futures::Stream>::Item>> {
        let stream = Pin::new(&mut self.stream);

        match stream.poll_next(ctx) {
            futures::task::Poll::Ready(value) => match value {
                Some(Ok(actions)) => {
                    let skipped = data_skipping_filter(actions, &self.predicate)?;
                    futures::task::Poll::Ready(Some(self.log_replay.replay(skipped)))
                }
                Some(Err(err)) => futures::task::Poll::Ready(Some(Err(err))),
                None => futures::task::Poll::Ready(None),
            },
            _ => futures::task::Poll::Pending,
        }
    }
}

pub struct ScanFileStream<'a> {
    stream: BoxStream<'a, RecordBatch>,
    log_replay: LogReplay,
    log_segment: &'a LogSegment,
    predicate: &'a Option<Expression>,
}

impl std::fmt::Debug for ScanFileStream<'_> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        todo!()
    }
}

impl Stream for ScanFileStream<'_> {
    type Item = DeltaResult<RecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<<Self as futures::Stream>::Item>> {
        let stream = Pin::new(&mut self.stream);

        match stream.poll_next(ctx) {
            futures::task::Poll::Ready(value) => {
                if let Some(actions) = value {
                    let skipped = data_skipping_filter(actions, self.predicate)?;
                    futures::task::Poll::Ready(Some(self.log_replay.replay(skipped)))
                } else {
                    futures::task::Poll::Ready(Ok(value).transpose())
                }
            }
            _ => futures::task::Poll::Pending,
        }
    }
}

impl<'a> ScanFileStream<'a> {
    pub(crate) fn new(
        log_segment: &'a LogSegment,
        predicate: &'a Option<Expression>,
        storage_client: Arc<dyn ObjectStore>,
    ) -> Self {
        let storage_client = storage_client.clone();

        let stream = stream::iter(log_segment.iter().rev())
            .map(move |log| log.read(storage_client.clone()))
            .then(|f| f)
            .flat_map(stream::iter)
            .boxed();

        Self {
            log_segment,
            predicate,
            stream,
            log_replay: LogReplay::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::replay::LogFile;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    // This test has a bit more scaffolding to make it easier
    // to simulate a real use case of the ScanFileStream
    #[tokio::test]
    async fn test_scanfilestream() {
        let storage_client = LocalFileSystem::new();
        let path1 = Path::from(format!(
            "{}/tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let path2 = Path::from(format!(
            "{}/tests/data/table-with-dv-small/_delta_log/00000000000000000001.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let log_segment = LogSegment {
            log_files: vec![path1.try_into().unwrap(), path2.try_into().unwrap()],
        };

        let mut scan_stream = ScanFileStream::new(&log_segment, &None, Arc::new(storage_client));
        let mut counted = 0;

        while let Some(Ok(batch)) = scan_stream.next().await {
            let _batch: RecordBatch = batch;
            //println!("batch: {batch:?}");
            counted += 1;
        }
        assert_eq!(
            2, counted,
            "Expected to have iterated over some RecordBatches"
        );
    }

    #[tokio::test]
    async fn test_stream_for_scanning() {
        let storage_client = LocalFileSystem::new();
        let path = Path::from(format!(
            "{}/tests/data/table-without-dv-small/_delta_log/00000000000000000000.json",
            std::env!["CARGO_MANIFEST_DIR"]
        ));
        let log_file: LogFile = path.try_into().unwrap();
        let segment = LogSegment {
            log_files: vec![log_file.clone(), log_file],
        };

        let storage_client = Arc::new(storage_client);
        let iterator = stream::iter(
            segment
                .iter()
                .rev()
                .map(|log| log.read(storage_client.clone())),
        )
        .then(|f| f)
        .map(stream::iter)
        .flatten();

        let mut counted = 0;
        for _batch in iterator.collect::<Vec<RecordBatch>>().await {
            counted += 1;
        }
        assert_eq!(
            2, counted,
            "Expected to have iterated over some RecordBatches"
        );
    }
}
