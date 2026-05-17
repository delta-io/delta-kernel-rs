//! Phase-level success-payload accumulator.
//!
//! [`PhaseState`] is the unified container the executor fills when a phase
//! finishes successfully and the SM consumes on the next `advance` call. It
//! holds two kinds of payloads the executor produces:
//!
//! - **KDF outputs** -- [`FinishedHandle`]s submitted by drained
//!   [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume) pipelines and
//!   arbitrary executor-side telemetry (e.g. write row counts), keyed by [`KdfStateToken`].
//! - **Schema** -- a single [`SchemaRef`] produced by a
//!   [`PhaseOperation::SchemaQuery`](super::phase_operation::PhaseOperation::SchemaQuery) phase.
//!
//! ## Lifecycle
//!
//! 1. Executor calls [`PhaseState::empty`] at the start of a phase.
//! 2. The drained KDF iterator calls [`Handle::finish`](crate::plans::kdf::Handle::finish)
//!    and pushes the result into [`PhaseState::submit_kdf_handle`]. Each consume sink is
//!    single-partition by construction, so exactly one submission per token is expected;
//!    duplicates are an invariant violation.
//! 3. Schema-query phases push the parsed footer schema into [`PhaseState::submit_schema`].
//! 4. The driver hands the populated `PhaseState` back to the SM through `advance`.
//! 5. Typed extractors built on top of `consume()` pull payloads via [`PhaseState::take_by_token`];
//!    schema-query bodies pull via [`PhaseState::take_schema`].
//!
//! ## Errors
//!
//! `PhaseState` only stores success payloads. Engine failures flow on the
//! `Result<PhaseState, EngineError>` arm of the protocol's resume value, never as
//! entries inside the state.
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

    /// Submit a finalized consumer state. Returns an error if a state has
    /// already been submitted under the same [`KdfStateToken`] — each
    /// consume sink is single-partition by construction, so a duplicate
    /// submission indicates a routing bug or a re-fired handle.
    pub fn submit_kdf_handle(&self, handle: FinishedHandle) -> Result<(), DeltaError> {
        let mut inner = self.lock();
        let token = handle.token;
        if inner.kdf_entries.insert(token.clone(), handle.erased).is_some() {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "submit_kdf_handle: token `{token}` already submitted",
            ));
        }
        Ok(())
    }

    /// Submit a schema-query result. Overwrites any prior schema (one
    /// schema per phase by construction).
    pub fn submit_schema(&self, schema: SchemaRef) {
        self.lock().schema = Some(schema);
    }

    /// Remove and return the erased payload for `token`, if one was
    /// submitted.
    pub fn take_by_token(&self, token: &KdfStateToken) -> Option<Box<dyn Any + Send>> {
        self.lock().kdf_entries.remove(token)
    }

    /// Remove and return the schema produced by a schema-query phase, if any.
    pub fn take_schema(&self) -> Option<SchemaRef> {
        self.lock().schema.take()
    }

    /// Take the inner mutex, recovering from poisoning. See the
    /// "Mutex poisoning" section in this module's docs for rationale.
    fn lock(&self) -> MutexGuard<'_, PhaseStateInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::kdf::{ConsumerKdfId, TraceContext};
    use crate::schema::StructType;

    fn finished_handle(token: &KdfStateToken, payload: i64) -> FinishedHandle {
        FinishedHandle {
            token: token.clone(),
            ctx: TraceContext {
                sm: "test".to_string(),
                phase: "phase".to_string(),
            },
            erased: Box::new(payload),
        }
    }

    #[test]
    fn empty_accumulator_yields_no_payloads_for_any_token() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        assert!(s.take_by_token(&t).is_none());
        assert!(s.take_schema().is_none());
    }

    #[test]
    fn submit_and_take_kdf_roundtrips_payload() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);

        s.submit_kdf_handle(finished_handle(&t, 42)).unwrap();

        let payload = s.take_by_token(&t).expect("payload present");
        let v = *payload.downcast_ref::<i64>().unwrap();
        assert_eq!(v, 42);

        // Second take drains -- nothing left under that token.
        assert!(s.take_by_token(&t).is_none());
    }

    #[test]
    fn duplicate_submit_under_same_token_errors() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        s.submit_kdf_handle(finished_handle(&t, 10)).unwrap();
        let err = s.submit_kdf_handle(finished_handle(&t, 20)).unwrap_err();
        assert!(format!("{err}").contains("already submitted"));
    }

    #[test]
    fn take_by_unknown_token_returns_none() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        assert!(s.take_by_token(&t).is_none());
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
        assert!(s.take_schema().is_none(), "schema is consumed by take");
    }

    #[test]
    fn clone_shares_inner_state() {
        let a = PhaseState::empty();
        let b = a.clone();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);

        a.submit_kdf_handle(finished_handle(&t, 99)).unwrap();

        let via_b = b.take_by_token(&t).expect("payload visible through clone");
        assert_eq!(*via_b.downcast_ref::<i64>().unwrap(), 99);
        assert!(
            a.take_by_token(&t).is_none(),
            "take through clone drained the shared state"
        );
    }

    #[test]
    fn schema_and_kdf_payloads_coexist() {
        let s = PhaseState::empty();
        let t = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        s.submit_kdf_handle(finished_handle(&t, 1)).unwrap();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        s.submit_schema(schema.clone());

        assert!(s.take_schema().is_some());
        assert!(s.take_by_token(&t).is_some());
        // Both were drained by the takes above.
        assert!(s.take_by_token(&t).is_none());
        assert!(s.take_schema().is_none());
    }
}
