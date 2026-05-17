//! Runtime state for KDF consumers.
//!
//! [`Handle<K>`] is the executor's working buffer for one consumer sink.
//! It's created from a [`ConsumeSink`] template when a phase starts,
//! fed batches via [`Handle::apply_consumer`], and finalized via [`Handle::finish`]
//! when the child is exhausted.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink
//!
//! Generic over `K: Kdf + ?Sized`; executor code uses `Handle<dyn ConsumerKdf>`
//! directly. KDFs ride on the
//! [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume)
//! sink, which is dispatched in-process — handles never need to cross a
//! serialization boundary.

use std::any::Any;

use super::token::KdfStateToken;
use super::trace::TraceContext;
use super::traits::{ConsumerKdf, Kdf, KdfControl};
use crate::{DeltaResult, EngineData};

/// Runtime state carrier. Generic over the KDF kind; executor code holds
/// `Handle<dyn ConsumerKdf>`.
///
/// `inner` is the mutable working buffer (a `Box<dyn ConsumerKdf>`). `token`
/// joins this handle's eventual finalized state back to the plan-tree node.
/// `ctx` identifies the execution context for tracing and error attribution.
#[derive(Debug)]
pub struct Handle<K: Kdf + ?Sized> {
    token: KdfStateToken,
    ctx: TraceContext,
    inner: Box<K>,
}

impl<K: Kdf + ?Sized> Handle<K> {
    /// Construct a handle.
    pub fn new(token: KdfStateToken, ctx: TraceContext, inner: Box<K>) -> Self {
        Self { token, ctx, inner }
    }

    pub fn token(&self) -> &KdfStateToken {
        &self.token
    }
    pub fn ctx(&self) -> &TraceContext {
        &self.ctx
    }
    pub fn kdf_id(&self) -> &'static str {
        self.inner.kdf_id()
    }
}

impl<K: ConsumerKdf + ?Sized> Handle<K> {
    /// Apply the consumer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kdf.apply",
        skip(self, batch),
        fields(
            sm = self.ctx.sm,
            phase = self.ctx.phase,
            kdf_id = self.inner.kdf_id(),
            token_id = self.token.id,
        ),
    )]
    pub fn apply_consumer(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let out = self.inner.apply(batch)?;
        tracing::trace!(control = ?out, "consumer batch applied");
        Ok(out)
    }
}

impl<K: Kdf + ?Sized> Handle<K> {
    /// Consume the handle, returning `(token, ctx, erased state)`.
    #[tracing::instrument(
        level = "debug",
        name = "kdf.finish",
        skip(self),
        fields(
            sm = self.ctx.sm,
            phase = self.ctx.phase,
            kdf_id = self.inner.kdf_id(),
            token_id = self.token.id,
        ),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kdf handle finished");
        FinishedHandle {
            token: self.token,
            ctx: self.ctx,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`Handle::finish`] — carries the token, execution context,
/// and erased final state. Consumed by
/// `PhaseState::submit_kdf_handle`.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KdfStateToken,
    pub ctx: TraceContext,
    pub erased: Box<dyn Any + Send>,
}

// `Clone` for `Handle<dyn ConsumerKdf>` comes from
// `dyn_clone::clone_trait_object!` on the subtrait — `Box<K>: Clone` is
// automatic, so this is a single generic impl.

impl<K: Kdf + ?Sized> Clone for Handle<K>
where
    Box<K>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            ctx: self.ctx.clone(),
            inner: self.inner.clone(),
        }
    }
}
