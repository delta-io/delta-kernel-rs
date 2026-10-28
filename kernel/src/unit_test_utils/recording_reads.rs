//! Recording handlers for cancellation tests across adapter and snapshot read paths.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use url::Url;

use super::TestCancellationToken;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::{
    CancellationTokenRef, Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize,
    FileSlice, FilteredEngineData, JsonHandler, KernelError, KernelResult, KernelResultIterator,
    ParquetFooter, ParquetHandler, PredicateRef, SchemaRef, StorageHandler,
};

/// Read boundary observed by the recording handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Read {
    List,
    Hint,
    Crc,
    Json,
    Parquet,
    Footer,
}

/// Records scopes at the real engine's read boundaries; all data comes from the inner handlers.
pub(crate) struct RecordingReads {
    /// Engine supplying fixture data and delegated operations.
    pub(crate) inner: Arc<dyn Engine>,
    /// Token cancelled by the configured read trigger.
    pub(crate) token: Arc<TestCancellationToken>,
    /// Expected token identity or absence at each read boundary.
    pub(crate) expected_token: Option<CancellationTokenRef>,
    /// Read boundary that triggers cancellation, when configured.
    pub(crate) cancel_on: Option<Read>,
    /// Cancels the test token after each JSON iterator item.
    pub(crate) cancel_json_after_batch: bool,
    /// Read boundaries observed in call order.
    pub(crate) events: Mutex<Vec<Read>>,
}

impl RecordingReads {
    /// Creates a recorder that can cancel at a selected read boundary.
    pub(crate) fn new(cancel_on: Option<Read>) -> Arc<Self> {
        let token = Arc::new(TestCancellationToken::default());
        Arc::new(Self {
            inner: Arc::new(SyncEngine::new()),
            expected_token: Some(token.clone()),
            token,
            cancel_on,
            cancel_json_after_batch: false,
            events: Mutex::new(Vec::new()),
        })
    }

    /// Builds an engine that records reads through handler-based replay.
    pub(crate) fn engine(self: &Arc<Self>) -> DelegatingEngine {
        let engine = DelegatingEngine::new(self.inner.clone())
            .with_storage_handler(self.clone())
            .with_json_handler(self.clone())
            .with_parquet_handler(self.clone());
        #[cfg(feature = "declarative-plans")]
        let engine = engine.without_plan_executor();
        engine
    }

    /// Verifies token identity, records the read, and applies its cancellation trigger.
    fn record(&self, read: Read, token: &Option<CancellationTokenRef>) -> KernelResult<()> {
        match (token, &self.expected_token) {
            (Some(token), Some(expected)) => assert!(Arc::ptr_eq(token, expected)),
            (None, None) => {}
            _ => panic!("missing or replaced token on {read:?}"),
        }
        self.events.lock().unwrap().push(read);
        if self.cancel_on == Some(read) {
            self.token.cancel();
            if token.is_none() {
                return Err(KernelError::Cancelled);
            }
        }
        Ok(())
    }
}

/// Routes plain reads through the recorder without supplying a cancellation token.
macro_rules! plain_reads {
    ($(
        $(#[$doc:meta])*
        fn $plain:ident, $cancellable:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;
    )*) => {$(
        $(#[$doc])*
        fn $plain(&self, $($arg: $ty),*) -> $result {
            self.$cancellable($($arg,)* None)
        }
    )*};
}

impl StorageHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped listing request.
        fn list_from, list_from_with_cancellation(path: &Url)
            -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>>;

        /// Records an unscoped byte-range read.
        fn read_files, read_files_with_cancellation(files: Vec<FileSlice>)
            -> KernelResult<Box<dyn KernelResultIterator<Bytes>>>;
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        self.record(Read::List, &token)?;
        self.inner
            .storage_handler()
            .list_from_with_cancellation(path, token)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        let read = if files.iter().any(|(url, _)| url.path().ends_with(".crc")) {
            Read::Crc
        } else {
            Read::Hint
        };
        self.record(read, &token)?;
        self.inner
            .storage_handler()
            .read_files_with_cancellation(files, token)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.storage_handler().put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        self.inner.storage_handler().head(path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.storage_handler().delete(path)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.inner.storage_handler().copy_atomic(src, dest)
    }
}

impl JsonHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped JSON read.
        fn read_json_files, read_json_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
    }

    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        if !files.is_empty() {
            self.record(Read::Json, &token)?;
        }
        let batches = self
            .inner
            .json_handler()
            .read_json_files_with_cancellation(files, schema, predicate, token)?;
        if self.cancel_json_after_batch {
            let token = self.token.clone();
            Ok(Box::new(batches.inspect(move |_| token.cancel())))
        } else {
            Ok(batches)
        }
    }

    fn parse_json(
        &self,
        data: Box<dyn EngineData>,
        schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.json_handler().parse_json(data, schema)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        self.inner
            .json_handler()
            .write_json_file(path, data, overwrite)
    }
}

impl ParquetHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped Parquet read.
        fn read_parquet_files, read_parquet_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;

        /// Records an unscoped Parquet footer read.
        fn read_parquet_footer, read_parquet_footer_with_cancellation(file: &FileMeta)
            -> KernelResult<ParquetFooter>;
    }

    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.record(Read::Parquet, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_files_with_cancellation(files, schema, predicate, token)
    }

    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<ParquetFooter> {
        self.record(Read::Footer, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_footer_with_cancellation(file, token)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
    ) -> KernelResult<FileSize> {
        self.inner
            .parquet_handler()
            .write_parquet_file(location, data)
    }
}
