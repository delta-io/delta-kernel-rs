//! Runtime state for KDF consumers.
//!
//! [`Handle<K>`] is the executor's working buffer for one [`ConsumeSink`]: created when a
//! phase starts, fed batches via [`Handle::apply_consumer`], finalized via [`Handle::finish`]
//! when the child is exhausted. Type-erased into [`FinishedHandle`] for `StepResult`
//! submission.
//!
//! Generic over `K: KernelConsumer + ?Sized`; executor code uses `Handle<dyn KernelConsumer>`.
//! Handles dispatch in-process and never cross a serialization boundary.
//!
//! Every handle is stamped with its owning SM's `(sm_id, sm_kind, step_name)` triple,
//! threaded through tracing spans and `StepResult` cross-checks.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink

use std::any::Any;

use uuid::Uuid;

use super::token::KernelConsumerToken;
use super::traits::{KdfControl, KernelConsumer};
use crate::{DeltaResult, EngineData};

/// Runtime state carrier. Generic over the KDF kind; executor code holds
/// `Handle<dyn KernelConsumer>`.
///
/// `inner` is the mutable working buffer (a `Box<dyn KernelConsumer>`). `token`
/// joins this handle's eventual finalized state back to the plan-tree node.
/// `sm_id` / `sm_kind` / `step_name` identify the owning SM/phase for tracing
/// and error attribution.
#[derive(Debug)]
pub struct Handle<K: KernelConsumer + ?Sized> {
    token: KernelConsumerToken,
    sm_id: Uuid,
    sm_kind: &'static str,
    step_name: &'static str,
    inner: Box<K>,
}

impl<K: KernelConsumer + ?Sized> Handle<K> {
    /// Construct a handle stamped with the owning SM's identity tuple.
    pub fn new(
        token: KernelConsumerToken,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
        inner: Box<K>,
    ) -> Self {
        Self {
            token,
            sm_id,
            sm_kind,
            step_name,
            inner,
        }
    }

    /// Apply the consumer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kernel_consumer.apply",
        skip(self, batch),
        ret,
        fields(
            sm_id = %self.sm_id,
            sm_kind = self.sm_kind,
            step_name = self.step_name,
            kind = %self.inner.kind(),
            token_id = self.token.id,
        ),
    )]
    pub fn apply_consumer(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.inner.apply(batch)
    }

    /// Consume the handle, returning the finalized identity-stamped state.
    #[tracing::instrument(
        level = "debug",
        name = "kernel_consumer.finish",
        skip(self),
        fields(
            sm_id = %self.sm_id,
            sm_kind = self.sm_kind,
            step_name = self.step_name,
            kind = %self.inner.kind(),
            token_id = self.token.id,
        ),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kernel consumer handle finished");
        FinishedHandle {
            token: self.token,
            sm_id: self.sm_id,
            sm_kind: self.sm_kind,
            step_name: self.step_name,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`Handle::finish`] — carries the token, the owning SM's identity
/// tuple, and the type-erased final state.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KernelConsumerToken,
    pub sm_id: Uuid,
    pub sm_kind: &'static str,
    pub step_name: &'static str,
    pub erased: Box<dyn Any + Send>,
}

// `Clone` for `Handle<dyn KernelConsumer>` comes from
// `dyn_clone::clone_trait_object!` on the trait — `Box<K>: Clone` is
// automatic, so this is a single generic impl.
impl<K: KernelConsumer + ?Sized> Clone for Handle<K>
where
    Box<K>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            sm_id: self.sm_id,
            sm_kind: self.sm_kind,
            step_name: self.step_name,
            inner: self.inner.clone(),
        }
    }
}
