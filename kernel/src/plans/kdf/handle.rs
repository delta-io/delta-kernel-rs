//! Runtime state for KDF consumers.
//!
//! [`Handle<K>`] is the executor's working buffer for one consumer sink.
//! It's created from a [`ConsumeSink`] template when a phase starts,
//! fed batches via [`Handle::apply_consumer`], and finalized via [`Handle::finish`]
//! when the child is exhausted.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink
//!
//! Generic over `K: ConsumerKdf + ?Sized`; executor code uses
//! `Handle<dyn ConsumerKdf>` directly. KDFs ride on the
//! [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume)
//! sink, which is dispatched in-process — handles never need to cross a
//! serialization boundary.
//!
//! # Identity
//!
//! Every handle carries the owning state machine's identity tuple:
//!
//! - `sm_id: Uuid` — fresh per [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM)
//!   instance.
//! - `sm_kind: &'static str` — static label for the SM's logical kind (e.g. `"scan_metadata"`).
//! - `phase_name: &'static str` — the phase that minted this handle, as supplied to
//!   [`Context::execute`](crate::plans::state_machines::framework::coroutine::context::Context::execute).
//!
//! Together they form the `(sm_id, sm_kind, phase_name)` correlation triple used by tracing
//! span fields and `PhaseState` cross-checks.

use std::any::Any;

use uuid::Uuid;

use super::token::ConsumerKdfId;
use super::token::KdfStateToken;
use super::traits::{ConsumerKdf, KdfControl};
use crate::{DeltaResult, EngineData};

/// Runtime state carrier. Generic over the KDF kind; executor code holds
/// `Handle<dyn ConsumerKdf>`.
///
/// `inner` is the mutable working buffer (a `Box<dyn ConsumerKdf>`). `token`
/// joins this handle's eventual finalized state back to the plan-tree node.
/// `sm_id` / `sm_kind` / `phase_name` identify the owning SM/phase for tracing
/// and error attribution.
#[derive(Debug)]
pub struct Handle<K: ConsumerKdf + ?Sized> {
    token: KdfStateToken,
    sm_id: Uuid,
    sm_kind: &'static str,
    phase_name: &'static str,
    inner: Box<K>,
}

impl<K: ConsumerKdf + ?Sized> Handle<K> {
    /// Construct a handle stamped with the owning SM's identity tuple.
    pub fn new(
        token: KdfStateToken,
        sm_id: Uuid,
        sm_kind: &'static str,
        phase_name: &'static str,
        inner: Box<K>,
    ) -> Self {
        Self {
            token,
            sm_id,
            sm_kind,
            phase_name,
            inner,
        }
    }

    pub fn token(&self) -> &KdfStateToken {
        &self.token
    }
    pub fn kdf_id(&self) -> ConsumerKdfId {
        self.inner.kdf_id()
    }

    /// Apply the consumer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kdf.apply",
        skip(self, batch),
        ret,
        fields(
            sm_id = %self.sm_id,
            sm_kind = self.sm_kind,
            phase_name = self.phase_name,
            kdf_id = %self.inner.kdf_id(),
            token_id = self.token.id,
        ),
    )]
    pub fn apply_consumer(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.inner.apply(batch)
    }

    /// Consume the handle, returning the finalized identity-stamped state.
    #[tracing::instrument(
        level = "debug",
        name = "kdf.finish",
        skip(self),
        fields(
            sm_id = %self.sm_id,
            sm_kind = self.sm_kind,
            phase_name = self.phase_name,
            kdf_id = %self.inner.kdf_id(),
            token_id = self.token.id,
        ),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kdf handle finished");
        FinishedHandle {
            token: self.token,
            sm_id: self.sm_id,
            sm_kind: self.sm_kind,
            phase_name: self.phase_name,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`Handle::finish`] — carries the token, the owning SM's identity
/// tuple, and the erased final state. Consumed by `PhaseState::submit_kdf_handle`.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KdfStateToken,
    pub sm_id: Uuid,
    pub sm_kind: &'static str,
    pub phase_name: &'static str,
    pub erased: Box<dyn Any + Send>,
}

// `Clone` for `Handle<dyn ConsumerKdf>` comes from
// `dyn_clone::clone_trait_object!` on the trait — `Box<K>: Clone` is
// automatic, so this is a single generic impl.
impl<K: ConsumerKdf + ?Sized> Clone for Handle<K>
where
    Box<K>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            sm_id: self.sm_id,
            sm_kind: self.sm_kind,
            phase_name: self.phase_name,
            inner: self.inner.clone(),
        }
    }
}
