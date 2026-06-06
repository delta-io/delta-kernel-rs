//! Iterator wrapper that counts items as they stream and emits the kernel's standard
//! `"storage"` tracing span on drop. Used by [`MeteredStorageHandler`] for handlers
//! whose counts are only known as items flow (`list_from`, `read_files`).
//!
//! The span fires on the thread that drops the iterator (the caller's thread),
//! avoiding background-thread emission where no tracing subscriber may be installed.
//!
//! [`MeteredStorageHandler`]: crate::metrics::MeteredStorageHandler

use std::marker::PhantomData;
use std::time::Instant;

use bytes::Bytes;

use crate::metrics::events::STORAGE_SPAN;
use crate::{DeltaResult, FileMeta};

/// Counts items observed and emits a `"storage"` span on drop. The type parameter
/// `T` selects whether bytes are counted: `Bytes` counts bytes, `FileMeta` does not.
pub(crate) struct MetricsIterator<I, T> {
    inner: I,
    name: &'static str,
    start: Instant,
    num_files: u64,
    bytes_read: u64,
    _phantom: PhantomData<T>,
}

impl<I, T> MetricsIterator<I, T> {
    pub(crate) fn new(inner: I, name: &'static str, start: Instant) -> Self {
        Self {
            inner,
            name,
            start,
            num_files: 0,
            bytes_read: 0,
            _phantom: PhantomData,
        }
    }

    fn record_file(&mut self) {
        self.num_files += 1;
    }

    fn record_bytes(&mut self, n: u64) {
        self.num_files += 1;
        self.bytes_read += n;
    }
}

impl<I, T> Drop for MetricsIterator<I, T> {
    fn drop(&mut self) {
        emit_storage_span(
            self.name,
            self.start.elapsed(),
            self.num_files,
            self.bytes_read,
        );
    }
}

/// Emit a one-shot `"storage"` span describing a completed operation. Use when the
/// operation does not return an iterator (e.g. `copy_atomic`).
pub(crate) fn emit_storage_span(
    name: &'static str,
    elapsed: std::time::Duration,
    num_files: u64,
    bytes_read: u64,
) {
    // Storage ops > 584 years (u64::MAX nanos) imply a bug, not a metric we care about.
    let duration_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    let _span = tracing::span!(
        tracing::Level::INFO,
        STORAGE_SPAN,
        report = tracing::field::Empty,
        name = name,
        num_files = num_files,
        bytes_read = bytes_read,
        duration_ns = duration_ns,
    );
}

impl<I> Iterator for MetricsIterator<I, FileMeta>
where
    I: Iterator<Item = DeltaResult<FileMeta>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if matches!(item, Some(Ok(_))) {
            self.record_file();
        }
        item
    }
}

impl<I> Iterator for MetricsIterator<I, Bytes>
where
    I: Iterator<Item = DeltaResult<Bytes>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(Ok(ref bytes)) = item {
            self.record_bytes(bytes.len() as u64);
        }
        item
    }
}
