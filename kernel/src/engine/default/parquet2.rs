use std::sync::{mpsc, mpsc::Receiver, Arc};

use arrow_array::RecordBatch;
use futures::future::join_all;
use futures::StreamExt;
use object_store::{path::Path, DynObjectStore};
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

use super::file_stream::{FileOpener, FileStream};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::parquet::{ParquetOpener, PresignedUrlOpener};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, ExpressionRef, FileDataReadResultIterator, FileMeta,
    ParquetHandler,
};

const TASK_LIMIT: usize = 1024;

#[derive(Debug)]
pub struct AsyncParquetHandler {
    object_store: Arc<DynObjectStore>,
    // separate runtime that we spawn all the IO on
    tokio_runtime: Arc<Runtime>,
}

impl AsyncParquetHandler {
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        // default multi-threaded (with thread per core) runtime with work-stealing scheduler
        let tokio_runtime = Arc::new(Runtime::new().unwrap());
        Self {
            object_store,
            tokio_runtime,
        }
    }
}

impl ParquetHandler for AsyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // handle trivial case first
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // get the first FileMeta to decide how to fetch the file.
        // NB: This means that every file in `FileMeta` _must_ have the same scheme or things will break
        // s3://    -> aws   (ParquetOpener)
        // nothing  -> local (ParquetOpener)
        // https:// -> assume presigned URL (and fetch without object_store)
        //   -> reqwest to get data
        //   -> parse to parquet
        // SAFETY: we did is_empty check above, this is ok.
        let file_opener: Box<dyn FileOpener> = match files[0].location.scheme() {
            "http" | "https" => Box::new(PresignedUrlOpener::new(
                1024,
                physical_schema.clone(),
                predicate,
            )),
            _ => Box::new(ParquetOpener::new(
                1024,
                physical_schema.clone(),
                predicate,
                self.object_store.clone(),
                Some(self.tokio_runtime.clone()),
            )),
        };

        read_files(files, file_opener, self.tokio_runtime.clone())
    }
}

// read files using tokio tasks
fn read_files(
    files: &[FileMeta],
    file_opener: Box<dyn FileOpener + Send + Sync>,
    runtime: Arc<Runtime>,
) -> DeltaResult<FileDataReadResultIterator> {
    // let semaphore = Arc::new(Semaphore::new(TASK_LIMIT));
    let (tx, rx) = mpsc::channel();

    // println!("files len: {:?}", files.len());

    let file_opener: Arc<dyn FileOpener + Send + Sync> = Arc::from(file_opener);
    let len = files.len();
    runtime.block_on(async {
        let files = files.to_vec();
        let mut handles = Vec::with_capacity(len);

        for file in files.into_iter() {
            // let permit = semaphore.clone().acquire_owned().await.unwrap();
            let tx_clone = tx.clone();

            let f = file_opener.clone();
            // println!("file: {:?}", file);
            let handle = runtime.spawn(async move {
                // read the file as stream
                let mut stream = f.open(file.clone(), None).unwrap().await.unwrap();
                // tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                // println!("sleeping");
                while let Some(result) = stream.next().await {
                    let _ = tx_clone.send(result);
                }
                // drop(permit); // release permit
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete.
        join_all(handles).await;
    });

    Ok(Box::new(ChunkIter { rx }))
}

struct ChunkIter {
    rx: Receiver<Result<RecordBatch, Error>>,
}

impl Iterator for ChunkIter {
    type Item = Result<Box<dyn EngineData>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx
            .recv()
            .ok()
            .map(|r| r.map(|rb| Box::new(ArrowEngineData::new(rb)) as Box<dyn EngineData>))
    }
}
