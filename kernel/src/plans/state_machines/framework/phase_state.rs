//! Phase-level success-payload accumulator.
//!
//! [`PhaseState`] is the unified container the executor fills when a phase
//! finishes successfully and the SM consumes on the next `advance` call. It
//! holds two kinds of payloads the executor produces:
//!
//! - **KDF outputs** -- [`FinishedHandle`]s submitted by drained
//!   [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume) pipelines and arbitrary
//!   executor-side telemetry (e.g. write row counts), keyed by [`KdfStateToken`].
//! - **Schema** -- a single [`SchemaRef`] produced by a
//!   [`PhaseOperation::SchemaQuery`](super::phase_operation::PhaseOperation::SchemaQuery) phase.
//!
//! ## Lifecycle
//!
//! 1. Executor calls [`PhaseState::empty`] at the start of a phase.
//! 2. The drained KDF iterator calls [`Handle::finish`](crate::plans::kdf::Handle::finish) and
//!    pushes the result into [`PhaseState::submit_kdf_handle`]. Each consume sink is
//!    single-partition by construction, so exactly one submission per token is expected; duplicates
//!    are an invariant violation.
//! 3. Schema-query phases push the parsed footer schema into [`PhaseState::submit_schema`].
//! 4. The driver hands the populated `PhaseState` back to the SM through `advance`.
//! 5. Typed extractors built on top of `consume()` pull payloads via [`PhaseState::take_by_token`];
//!    schema-query bodies pull via [`PhaseState::take_schema`].
//!
//! ## Errors
//!
//! Submission is infallible. Invariant violations (duplicate submit) are
//! stashed in a first-wins internal slot and surface on the next `take_*`.
//! Engine failures still flow on the `Result<PhaseState, EngineError>` arm.
//!
//! ## Mutex poisoning
//!
//! The internal [`Mutex`] is taken only for trivial, infallible critical
//! sections (a handful of `HashMap`/`Option` ops). A poisoned guard always
//! contains a consistent value, so we recover via
//! [`PoisonError::into_inner`](std::sync::PoisonError::into_inner) instead of
//! panicking. This matches the convention used by the DataFusion engine
//! crate (`unwrap_or_else(|e| e.into_inner())`).

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::kdf::{FinishedHandle, KdfStateToken};
use crate::schema::SchemaRef;

/// Internal storage. All public methods take the outer [`Mutex`] briefly.
#[derive(Debug, Default)]
struct PhaseStateInner {
    kdf_entries: HashMap<KdfStateToken, Box<dyn Any + Send>>,
    schema: Option<SchemaRef>,
    /// First-wins slot drained by the next `take_*`.
    error: Option<DeltaError>,
}

impl PhaseStateInner {
    /// First-wins: keeps the original cause if `error` is already set.
    fn set_error(&mut self, err: DeltaError) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }
}

/// Thread-safe success-payload accumulator for one phase execution.
///
/// Cheap to [`Clone`] -- inner state is shared through an `Arc<Mutex<_>>`.
#[derive(Clone, Debug, Default)]
pub struct PhaseState {
    inner: Arc<Mutex<PhaseStateInner>>,
}

impl PhaseState {
    /// Construct an empty accumulator.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Submit a finalized consumer state. A duplicate submission under the
    /// same [`KdfStateToken`] stashes an invariant violation that surfaces on
    /// the next `take_*` call.
    pub fn submit_kdf_handle(&self, handle: FinishedHandle) {
        let mut inner = self.lock();
        let token = handle.token;
        if inner
            .kdf_entries
            .insert(token.clone(), handle.erased)
            .is_some()
        {
            inner.set_error(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "submit_kdf_handle: token `{token}` already submitted",
            ));
        }
    }

    /// Submit a schema-query result. A second submission stashes an invariant
    /// violation (surfaced on the next `take_*`) and leaves the original
    /// schema in place.
    pub fn submit_schema(&self, schema: SchemaRef) {
        let mut inner = self.lock();
        if inner.schema.is_some() {
            inner.set_error(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "submit_schema: schema already submitted for this phase",
            ));
            return;
        }
        inner.schema = Some(schema);
    }

    /// Remove and return the erased payload for `token`. Stashed
    /// submission-time errors win over both the payload and absence.
    pub fn take_by_token(&self, token: &KdfStateToken) -> Result<Box<dyn Any + Send>, DeltaError> {
        let mut inner = self.lock();
        if let Some(err) = inner.error.take() {
            return Err(err);
        }
        inner.kdf_entries.remove(token).ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "take_by_token: no entry for token `{token}`",
            )
        })
    }

    /// Remove and return the schema produced by a schema-query phase.
    /// Stashed submission-time errors win over both the schema and absence.
    pub fn take_schema(&self) -> Result<SchemaRef, DeltaError> {
        let mut inner = self.lock();
        if let Some(err) = inner.error.take() {
            return Err(err);
        }
        inner.schema.take().ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "take_schema: no schema submitted in this phase",
            )
        })
    }

    /// Take the inner mutex, recovering from poisoning. See the
    /// "Mutex poisoning" section in this module's docs for rationale.
    fn lock(&self) -> MutexGuard<'_, PhaseStateInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::plans::kdf::ConsumerKdfId;
    use crate::schema::StructType;

    fn finished_handle(token: &KdfStateToken, payload: i64) -> FinishedHandle {
        FinishedHandle {
            token: token.clone(),
            sm_id: Uuid::new_v4(),
            sm_kind: "test",
            phase_name: "phase",
            erased: Box::new(payload),
        }
    }

    #[test]
    fn submit_and_take_kdf_roundtrips_payload() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);

        s.submit_kdf_handle(finished_handle(&t, 42));

        let payload = s.take_by_token(&t).expect("payload present");
        let v = *payload.downcast_ref::<i64>().unwrap();
        assert_eq!(v, 42);
    }

    #[test]
    fn submit_and_take_schema_roundtrips() {
        let s = PhaseState::empty();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        s.submit_schema(schema.clone());
        let taken = s.take_schema().expect("schema present");
        assert!(Arc::ptr_eq(&taken, &schema));
    }

    #[test]
    fn duplicate_submit_under_same_token_errors_via_take_by_token() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        s.submit_kdf_handle(finished_handle(&t, 10));
        s.submit_kdf_handle(finished_handle(&t, 20));
        let err = s.take_by_token(&t).unwrap_err();
        assert!(format!("{err}").contains("already submitted"));
    }

    #[test]
    fn duplicate_submit_schema_errors_via_take_schema() {
        let s = PhaseState::empty();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        s.submit_schema(schema.clone());
        s.submit_schema(schema);
        let err = s.take_schema().unwrap_err();
        assert!(format!("{err}").contains("already submitted"));
    }
}
