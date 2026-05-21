//! Declarative full snapshot read (FSR) entry point on [`Snapshot`].
//!
//! Routes [`Snapshot::full_state`] to the canonical FSR coroutine
//! ([`crate::plans::operations::scan::FullState`]) whose terminal
//! [`ResultPlan`] names the reconstructed-action relation the engine reads
//! after running the plan's plans.

use std::sync::Arc;

use crate::plans::errors::DeltaErrAsKernel;
use crate::plans::ir::ResultPlan;
use crate::plans::operations::framework::coroutine::driver::Coroutine;
use crate::plans::operations::scan::{FullState, FullStateBuilder};
use crate::scan::ScanBuilder as KernelScanBuilder;
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Snapshot};

impl Snapshot {
    /// Declarative **full snapshot read** entry: a multi-plan FSR [`Coroutine`] derived
    /// from this snapshot's log listing.
    ///
    /// # What it models
    ///
    /// The returned coroutine yields exactly one
    /// [`Step::Plans`](crate::plans::operations::framework::step::Step::Plans)
    /// step (optionally preceded by a
    /// [`Step::SchemaQuery`](crate::plans::operations::framework::step::Step::SchemaQuery)
    /// prelude when `_last_checkpoint` is absent but checkpoint files are present), bundling
    /// the full *window-on-commits + anti-join-on-checkpoint* algorithm in a single
    /// engine-driven step. See [`crate::plans::operations::scan::full_state`] for the
    /// per-plan breakdown and the dedup-key contract.
    ///
    /// The SM's terminal value is a [`ResultPlan`] naming the FSR result
    /// relation. The engine drives the SM, executes the result plan's plans,
    /// and reads the result relation to obtain reconstructed action rows
    /// (add / remove / protocol / metaData / domainMetadata / txn).
    ///
    /// # Feature gate
    ///
    /// Available only with the `declarative-plans` feature. There is **no** runtime fallback:
    /// if the feature is disabled at compile time, callers rely on
    /// [`Snapshot::scan_builder`](crate::snapshot::Snapshot::scan_builder) and classic
    /// kernel replay instead.
    pub fn full_state(self: &SnapshotRef) -> DeltaResult<Coroutine<ResultPlan>> {
        FullState::for_table(Arc::clone(self))
            .build()
            .and_then(|fs| fs.state_machine())
            .map_err(|e| e.into_kernel_default())
    }

    /// Create a canonical FSR plan builder rooted at this snapshot.
    pub fn full_state_builder(self: &SnapshotRef) -> FullStateBuilder {
        FullState::for_table(Arc::clone(self))
    }

    /// Create a split-phase scan replay plan builder rooted at this snapshot.
    pub fn scan_replay_builder(self: &SnapshotRef) -> KernelScanBuilder {
        KernelScanBuilder::new(Arc::clone(self))
    }
}
