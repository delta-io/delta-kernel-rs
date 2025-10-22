use std::collections::VecDeque;
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream, StreamExt};
use futures::FutureExt;
use tracing::error;

use super::executor::TaskExecutor;
use crate::engine::arrow_data::ArrowEngineData;
use crate::{DeltaResult, FileDataReadResultIterator, FileMeta};

/// A fallible future that resolves to a stream of [`RecordBatch`]
/// cbindgen:ignore
pub type FileOpenFuture =
    BoxFuture<'static, DeltaResult<BoxStream<'static, DeltaResult<RecordBatch>>>>;

/// Generic API for opening a file using an [`ObjectStore`] and resolving to a
/// stream of [`RecordBatch`]
///
/// [`ObjectStore`]: object_store::ObjectStore
pub trait FileOpener: Send + Unpin {
    /// Asynchronously open the specified file and return a stream
    /// of [`RecordBatch`]
    fn open(&self, file_meta: FileMeta, range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture>;
}

/// Describes the behavior of the `FileStream` if file opening or scanning fails
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub enum OnError {
    /// Fail the entire stream and return the underlying error
    #[default]
    Fail,
    /// Continue scanning, ignoring the failed file
    Skip,
}

/// Represents the state of the next `FileOpenFuture`. Since we need to poll
/// this future while scanning the current file, we need to store the result if it
/// is ready
enum NextOpen {
    Pending(FileOpenFuture),
    Ready(DeltaResult<BoxStream<'static, DeltaResult<RecordBatch>>>),
}

enum FileStreamState {
    /// The idle state, no file is currently being read
    Idle,
    /// Currently performing asynchronous IO to obtain a stream of RecordBatch
    /// for a given parquet file
    Open {
        /// A [`FileOpenFuture`] returned by [`FileOpener::open`]
        future: FileOpenFuture,
    },
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// The reader instance
        reader: BoxStream<'static, DeltaResult<RecordBatch>>,
        /// A [`FileOpenFuture`] for the next file to be processed,
        /// and its corresponding partition column values, if any.
        /// This allows the next file to be opened in parallel while the
        /// current file is read.
        next: Option<NextOpen>,
    },
    /// Encountered an error
    Error,
}

/// A stream that iterates record batch by record batch, file over file.
#[allow(missing_debug_implementations)]
pub struct FileStream {
    /// An iterator over input files.
    file_iter: VecDeque<FileMeta>,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    #[allow(unused)]
    projected_schema: ArrowSchemaRef,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_opener: Box<dyn FileOpener>,
    /// The stream state
    state: FileStreamState,
    /// Describes the behavior of the `FileStream` if file opening or scanning fails
    on_error: OnError,
}

impl FileStream {
    /// Creates a new `FileStream` from a given schema, `FileOpener`, and files list; the files are
    /// processed asynchronously by the provided `TaskExecutor`. Returns an `Iterator` that consumes
    /// the results.
    pub fn new_async_read_iterator<E: TaskExecutor>(
        task_executor: Arc<E>,
        schema: ArrowSchemaRef,
        file_opener: Box<dyn FileOpener>,
        files: &[FileMeta],
        readahead: usize,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let mut stream = FileStream::new(files.to_vec(), schema, file_opener)?;

        // This channel will become the output iterator
        // The stream will execute in the background, and we allow up to `readahead`
        // batches to be buffered in the channel.
        let (sender, receiver) = std::sync::mpsc::sync_channel(readahead);

        let executor_for_block = task_executor.clone();
        task_executor.spawn(async move {
            while let Some(res) = stream.next().await {
                let sender_clone = sender.clone();
                let join_res = executor_for_block
                    .spawn_blocking(move || sender_clone.send(res))
                    .await;
                match join_res {
                    Ok(send_res) => match send_res {
                        Ok(()) => continue,
                        Err(_) => break,
                    },
                    Err(je) => {
                        error!("Couldn't join spawned task, runtime is likely in bad state: {je}");
                        // Send an error through the channel to be handled by the receiver
                        let _ = sender.send(Err(crate::Error::JoinFailure(format!(
                            "Failed to join spawned task: {je}",
                        ))));
                        break;
                    }
                }
            }
        });

        Ok(Box::new(receiver.into_iter().map(|rbr| {
            rbr.map(|rb| Box::new(ArrowEngineData::new(rb)) as _)
        })))
    }

    /// Create a new `FileStream` using the given `FileOpener` to scan underlying files
    pub fn new(
        files: impl IntoIterator<Item = FileMeta>,
        schema: ArrowSchemaRef,
        file_opener: Box<dyn FileOpener>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            file_iter: files.into_iter().collect(),
            projected_schema: schema,
            file_opener,
            state: FileStreamState::Idle,
            on_error: OnError::Fail,
        })
    }

    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Begin opening the next file in parallel while decoding the current file in FileStream.
    ///
    /// Since file opening is mostly IO (and may involve a
    /// bunch of sequential IO), it can be parallelized with decoding.
    fn start_next_file(&mut self) -> Option<DeltaResult<FileOpenFuture>> {
        let file_meta = self.file_iter.pop_front()?;
        Some(self.file_opener.open(file_meta, None))
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<DeltaResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => match self.start_next_file().transpose() {
                    Ok(Some(future)) => self.state = FileStreamState::Open { future },
                    Ok(None) => return Poll::Ready(None),
                    Err(e) => {
                        self.state = FileStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                FileStreamState::Open { future } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        // include time needed to start opening in `start_next_file`
                        let next = self.start_next_file().transpose();

                        match next {
                            Ok(Some(next_future)) => {
                                self.state = FileStreamState::Scan {
                                    reader,
                                    next: Some(NextOpen::Pending(next_future)),
                                };
                            }
                            Ok(None) => {
                                self.state = FileStreamState::Scan { reader, next: None };
                            }
                            Err(e) => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Err(e) => match self.on_error {
                        OnError::Skip => self.state = FileStreamState::Idle,
                        OnError::Fail => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                },
                FileStreamState::Scan { reader, next } => {
                    // We need to poll the next `FileOpenFuture` here to drive it forward
                    if let Some(next_open_future) = next {
                        if let NextOpen::Pending(f) = next_open_future {
                            if let Poll::Ready(reader) = f.as_mut().poll(cx) {
                                *next_open_future = NextOpen::Ready(reader);
                            }
                        }
                    }
                    match ready!(reader.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Some(Err(err)) => {
                            match self.on_error {
                                // If `OnError::Skip` we skip the file as soon as we hit the first error
                                OnError::Skip => match mem::take(next) {
                                    Some(future) => match future {
                                        NextOpen::Pending(future) => {
                                            self.state = FileStreamState::Open { future }
                                        }
                                        NextOpen::Ready(reader) => {
                                            self.state = FileStreamState::Open {
                                                future: Box::pin(std::future::ready(reader)),
                                            }
                                        }
                                    },
                                    None => return Poll::Ready(None),
                                },
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err)));
                                }
                            }
                        }
                        None => match mem::take(next) {
                            Some(future) => match future {
                                NextOpen::Pending(future) => {
                                    self.state = FileStreamState::Open { future }
                                }
                                NextOpen::Ready(reader) => {
                                    self.state = FileStreamState::Open {
                                        future: Box::pin(std::future::ready(reader)),
                                    }
                                }
                            },
                            None => return Poll::Ready(None),
                        },
                    }
                }
                FileStreamState::Error => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for FileStream {
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}
