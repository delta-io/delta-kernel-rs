//! Core `KernelReducer` trait, identity types, and typed-output companion.
//!
//! Adding a new KDF: declare the struct, derive `Clone`, write
//! `impl KernelReducer for T { ... }` with `kind`, `apply`, `finish`, and pair it with a
//! `KernelReducerOutput` impl declaring the typed output. KDFs ride on the
//! `Reduce` engine request defined by the state-machine framework (consumer module);
//! the request is dispatched in-process and never serialized.
// Intra-doc links to the state-machine framework intentionally degrade to plain text in
// this slice; consumer types live in sibling stacks.
#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]

//! # Object-safety notes
//!
//! - Associated types are NOT on [`KernelReducer`] -- `Box<dyn KernelReducer>` must be
//!   heterogeneous across concrete reducer implementations (the executor mixes reducers with
//!   different state types). Typed output lives on the [`KernelReducerOutput`] companion trait via
//!   static dispatch.
//! - `finish(self: Box<Self>) -> Box<dyn Any + Send>` erases the per-impl state type to keep the
//!   trait object-safe. Typed factories downcast inside their extract closure.

use std::any::Any;

use dyn_clone::DynClone;
use strum::Display as StrumDisplay;
use uuid::Uuid;

use crate::plans::errors::DeltaError;
use crate::{DeltaResult, EngineData};

// === Control flow ===

/// Loop control returned by a reducer after each batch.
///
/// `Break` lets a reducer stop driving more input once it has everything
/// it needs (e.g. `CheckpointHintReader` after the first row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KdfControl {
    Continue,
    Break,
}

// === Identity ===

/// Stable diagnostic identifier per reducer impl. Used in tracing spans, metrics labels,
/// panic messages, and [`KernelReducerToken`] construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum KernelReducerKind {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

/// Identity for a kernel-reducer entry on a finished handle.
///
/// Stamped at plan-build time when an [`EngineRequest::Reduce`] step is constructed. The fresh
/// UUID `id` ensures stale handles from a prior plan can't be confused with current
/// ones -- a [`FinishedHandle`] arriving with a token from a dead plan fails the
/// [`Extractor`] sanity check at decode time.
///
/// `Display` emits `<kind>#<id>`.
///
/// [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::state_machine::EngineRequest::Reduce
/// [`FinishedHandle`]: super::handle::FinishedHandle
/// [`Extractor`]: super::handle::Extractor
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KernelReducerToken {
    pub kind: KernelReducerKind,
    pub id: Uuid,
}

impl KernelReducerToken {
    /// Mint a fresh token for a kernel reducer with a UUID id.
    pub fn new(kind: KernelReducerKind) -> Self {
        Self {
            kind,
            id: Uuid::new_v4(),
        }
    }
}

impl std::fmt::Display for KernelReducerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kind, self.id)
    }
}

// === Reducer trait ===

/// Stateful observer over batches. Returns [`KdfControl`] per batch for
/// early termination.
///
/// Useful for accumulating log segments, reading hint files, collecting
/// sidecar references, etc.
///
/// `Box<dyn KernelReducer>` is cloneable via the [`DynClone`] supertrait -- the
/// blanket `impl<T: Clone> DynClone for T` covers every concrete KDF state
/// that derives `Clone`, so impls never write their own `clone_boxed`.
pub trait KernelReducer: DynClone + Send + Sync + std::fmt::Debug {
    /// Diagnostic identifier, stamped into the [`KernelReducerToken`] at plan-build time.
    fn kind(&self) -> KernelReducerKind;

    /// Observe one batch. Return [`KdfControl::Break`] to stop driving
    /// further input; the kernel treats it as "child exhausted."
    ///
    /// TODO: enforce cardinality of applied rows. Each reducer impl should declare an
    /// expected per-batch (or total) row-count contract so the runtime can assert that
    /// the engine isn't incorrectly providing rows (e.g. `CheckpointHintReader` is
    /// single-row, `MetadataProtocolReader` short-circuits on Break, etc.). Impls
    /// silently accept whatever the engine hands them.
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl>;

    /// Consume the finalized KDF, returning its state erased to
    /// `Box<dyn Any + Send>`. Typed factories downcast inside their extract
    /// closure back to the concrete state type.
    ///
    /// Signature takes `Box<Self>` rather than `self` so it's object-safe;
    /// concrete impls are usually `fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
    /// Box::new(*self) }`.
    fn finish(self: Box<Self>) -> Box<dyn Any + Send>;
}

// `Clone` for `Box<dyn KernelReducer>` -- delegates to `DynClone` (which every
// concrete `Clone` impl gets for free via the blanket).
dyn_clone::clone_trait_object!(KernelReducer);

// === Typed-output companion ===

/// Typed-output companion. Each KDF state impls this once, declaring the
/// typed output callers receive and how the finalized state reduces to it.
///
/// ```ignore
/// impl KernelReducerOutput for SidecarCollector {
///     type Output = Vec<FileMeta>;
///     fn into_output(self) -> Result<Self::Output, DeltaError> {
///         /* project on self */
///     }
/// }
/// ```
pub trait KernelReducerOutput: KernelReducer + Any + Sized + 'static {
    /// What downstream callers receive after `phase.execute(...)` completes.
    type Output: Send + 'static;

    /// Reduce the finalized state to [`Self::Output`].
    ///
    /// Each reduce sink is single-partition by construction (the executor
    /// drains one root partition; the planner pins `target_partitions = 1`),
    /// so this consumes `Self` directly. Token-keyed identity validation
    /// happens upstream in [`Extractor::for_reducer`]'s closure.
    ///
    /// [`Extractor::for_reducer`]: super::handle::Extractor::for_reducer
    fn into_output(self) -> Result<Self::Output, DeltaError>;
}
