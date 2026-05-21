//! KDF trait. One trait every KDF state implements.
//!
//! Adding a new KDF: declare the struct, derive `Clone`, write
//! `impl KernelConsumer for T { ... }` with `kind`, `apply`, `finish`. No
//! enum to edit, no registry to update, no serde wiring — KDFs ride on
//! the [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume)
//! sink, which is dispatched in-process and never serialized.
//!
//! # Object-safety notes
//!
//! - Associated types are NOT on the trait — `Box<dyn KernelConsumer>` must be heterogeneous across
//!   concrete consumer implementations (the executor mixes consumers with different state types).
//!   Typed output lives on the [`crate::plans::kernel_consumers::KernelConsumerOutput`] companion
//!   trait via static dispatch.
//! - `finish(self: Box<Self>) -> Box<dyn Any + Send>` erases the per-impl state type to keep the
//!   trait object-safe. Typed factories downcast inside their extract closure.

use std::any::Any;

use dyn_clone::DynClone;

use crate::plans::kernel_consumers::token::KernelConsumerKind;
use crate::{DeltaResult, EngineData};

/// Loop control returned by a consumer after each batch.
///
/// `Break` lets a consumer stop driving more input once it has everything
/// it needs (e.g. `CheckpointHintReader` after the first row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KdfControl {
    Continue,
    Break,
}

/// Stateful observer over batches. Returns [`KdfControl`] per batch for
/// early termination.
///
/// Useful for accumulating log segments, reading hint files, collecting
/// sidecar references, etc.
///
/// `Box<dyn KernelConsumer>` is cloneable via the [`DynClone`] supertrait — the
/// blanket `impl<T: Clone> DynClone for T` covers every concrete KDF state
/// that derives `Clone`, so impls never write their own `clone_boxed`.
pub trait KernelConsumer: DynClone + Send + Sync + std::fmt::Debug {
    /// Diagnostic identifier, used in tracing spans, metrics labels, panic
    /// messages, and [`crate::plans::kernel_consumers::KernelConsumerToken`] construction.
    /// Stable per-type.
    fn kind(&self) -> KernelConsumerKind;

    /// Observe one batch. Return [`KdfControl::Break`] to stop driving
    /// further input; the kernel treats it as "child exhausted."
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

// `Clone` for `Box<dyn KernelConsumer>` — delegates to `DynClone` (which every
// concrete `Clone` impl gets for free via the blanket).
dyn_clone::clone_trait_object!(KernelConsumer);
