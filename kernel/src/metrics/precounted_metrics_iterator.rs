//! Iterator wrapper that emits a metric with pre-computed `(num_files, bytes_read)`
//! exactly once -- when the inner iterator is exhausted or the wrapper is dropped.
//!
//! Used by handlers that know their counts up-front from the input `&[FileMeta]` --
//! today, JSON and Parquet `read_*_files` -- so there's no need to count items as
//! they flow. Pair with [`MetricsIterator`] (count-as-you-stream) for handlers whose
//! counts aren't known until the work happens.
//!
//! [`MetricsIterator`]: crate::metrics::MetricsIterator

/// Emits a precomputed `(num_files, bytes_read)` payload exactly once via `emit_fn`
/// when the inner iterator finishes (either exhausted via `next` returning `None`
/// or dropped early).
pub(crate) struct PrecountedMetricsIterator<I> {
    inner: I,
    num_files: u64,
    bytes_read: u64,
    emitted: bool,
    emit_fn: fn(u64, u64),
}

impl<I> PrecountedMetricsIterator<I> {
    /// Wrap `inner` with the precomputed counts. `emit_fn` is called once on
    /// exhaustion or drop with `(num_files, bytes_read)`; typically a thin
    /// wrapper around `tracing::span!` so the kernel's `ReportGeneratorLayer`
    /// picks it up.
    pub(crate) fn new(inner: I, num_files: u64, bytes_read: u64, emit_fn: fn(u64, u64)) -> Self {
        Self {
            inner,
            num_files,
            bytes_read,
            emitted: false,
            emit_fn,
        }
    }

    fn emit_once(&mut self) {
        if !self.emitted {
            self.emitted = true;
            (self.emit_fn)(self.num_files, self.bytes_read);
        }
    }
}

impl<I: Iterator> Iterator for PrecountedMetricsIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if item.is_none() {
            self.emit_once();
        }
        item
    }
}

impl<I> Drop for PrecountedMetricsIterator<I> {
    fn drop(&mut self) {
        self.emit_once();
    }
}
