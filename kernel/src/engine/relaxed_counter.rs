use std::sync::atomic::{AtomicU64, Ordering};

/// An atomic `u64` counter using [`Ordering::Relaxed`] throughout.
///
/// Relaxed ordering is sufficient here: metrics are reported after the operations they
/// describe have completed, so there are no inter-counter ordering dependencies.
#[derive(Debug, Default)]
pub struct RelaxedCounter(AtomicU64);

impl RelaxedCounter {
    /// Increment the counter by one.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n` to the counter.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Read the current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Reset the counter to zero.
    pub fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}
