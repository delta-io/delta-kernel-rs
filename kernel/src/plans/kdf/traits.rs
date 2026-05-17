//! KDF traits.
//!
//! A shared [`Kdf`] supertrait carries the two methods every KDF has
//! ([`Kdf::kdf_id`] and [`Kdf::finish`]); [`ConsumerKdf`] is the object-safe
//! subtrait that observes batches.
//!
//! Adding a new KDF is a pure addition: declare the struct, derive `Clone`,
//! write `impl Kdf` and `impl ConsumerKdf`. No enum to edit, no registry to
//! update, no serde wiring — KDFs ride on the
//! [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume)
//! sink, which is dispatched in-process and never serialized.
//!
//! # Object-safety notes
//!
//! - Associated types are NOT on the subtrait — `Box<dyn ConsumerKdf>` must be heterogeneous across
//!   concrete consumer implementations (the executor mixes consumers with different state types).
//!   Typed output lives on the [`crate::plans::kdf::KdfOutput`] companion trait via static
//!   dispatch.
//! - `finish(self: Box<Self>) -> Box<dyn Any + Send>` erases the per-impl state type to keep the
//!   subtrait object-safe. Typed factories downcast inside their extract closure.

use std::any::Any;

use dyn_clone::DynClone;

use crate::plans::ir::nodes::OrderingSpec;
use crate::plans::kdf::token::ConsumerKdfId;
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

// ============================================================================
// Kdf — shared supertrait
// ============================================================================

/// Shared methods common to every KDF (both filter and consumer).
///
/// Kept deliberately small: `kdf_id` and `finish` are the only methods that
/// look identical across every KDF implementation. Type-specific work
/// (per-row `apply` signatures, clone) lives on the [`ConsumerKdf`] subtrait.
///
/// This supertrait is object-safe, but is never materialized as a trait
/// object on its own — `Box<dyn Kdf>` would lose the differentiation needed
/// by the executor. The executor always holds `Box<dyn ConsumerKdf>`.
pub trait Kdf: Send + std::fmt::Debug {
    /// Diagnostic identifier, used in tracing spans, metrics labels, panic
    /// messages, and [`crate::plans::kdf::KdfStateToken`] construction.
    ///
    /// Convention: `consumer.<name>` with `<name>` = snake_case.
    /// Stable per-type.
    fn kdf_id(&self) -> ConsumerKdfId;

    /// Consume the finalized KDF, returning its state erased to
    /// `Box<dyn Any + Send>`. Typed factories downcast inside their extract
    /// closure back to the concrete state type.
    ///
    /// Signature takes `Box<Self>` rather than `self` so it's object-safe;
    /// concrete impls are usually `fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
    /// Box::new(*self) }`.
    fn finish(self: Box<Self>) -> Box<dyn Any + Send>;
}

// ============================================================================
// ConsumerKdf — batch → Continue / Break
// ============================================================================

/// Stateful observer over batches. Returns [`KdfControl`] per batch for
/// early termination.
///
/// Unlike a filter, a consumer doesn't alter the row stream — it just
/// observes. Useful for accumulating log segments, reading hint files,
/// collecting sidecar references, etc.
///
/// `Box<dyn ConsumerKdf>` is cloneable via the [`DynClone`] supertrait — the
/// blanket `impl<T: Clone> DynClone for T` covers every concrete KDF state
/// that derives `Clone`, so impls never write their own `clone_boxed`.
pub trait ConsumerKdf: Kdf + DynClone + Send + Sync {
    /// Observe one batch. Return [`KdfControl::Break`] to stop driving
    /// further input; the kernel treats it as "child exhausted."
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl>;

    /// Row-ordering requirement, or `None`. Consumers that depend on
    /// ordering (e.g. `MetadataProtocolReader` needs `version DESC` to
    /// stop at the newest protocol) declare it here.
    fn required_ordering(&self) -> Option<OrderingSpec> {
        None
    }
}

// `Clone` for `Box<dyn ConsumerKdf>` — delegates to `DynClone` (which every
// concrete `Clone` impl gets for free via the blanket).
dyn_clone::clone_trait_object!(ConsumerKdf);
