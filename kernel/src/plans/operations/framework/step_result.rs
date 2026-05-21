//! Phase-level success-payload accumulator.
//!
//! [`StepResult`] is the container the executor fills on phase success; the SM consumes it
//! on the next `submit` call. It holds two payload kinds:
//!
//! - **KDF outputs** -- [`FinishedHandle`]s from drained
//!   [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume) pipelines and executor
//!   telemetry, keyed by [`KernelConsumerToken`].
//! - **Schema** -- a single [`SchemaRef`] from a `SchemaQuery` phase.
//!
//! Submission is infallible. Each consume sink is single-partition, so duplicate submits are
//! an invariant violation stashed first-wins in an internal slot and surfaced on the next
//! `take_*`. The internal [`Mutex`] guards only trivial critical sections; poisoned guards
//! recover via [`PoisonError::into_inner`](std::sync::PoisonError::into_inner) so the no-panic
//! rule holds.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::kernel_consumers::{FinishedHandle, KernelConsumerToken};
use crate::schema::SchemaRef;

/// Internal storage. All public methods take the outer [`Mutex`] briefly.
#[derive(Debug, Default)]
struct StepResultInner {
    consumer_entries: HashMap<KernelConsumerToken, Box<dyn Any + Send>>,
    schema: Option<SchemaRef>,
    /// First-wins slot drained by the next `take_*`.
    error: Option<DeltaError>,
}

impl StepResultInner {
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
pub struct StepResult {
    inner: Arc<Mutex<StepResultInner>>,
}

impl StepResult {
    /// Construct an empty accumulator.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Submit a finalized consumer state. A duplicate submission under the
    /// same [`KernelConsumerToken`] stashes an invariant violation that surfaces on
    /// the next `take_*` call.
    pub fn submit_consumer_handle(&self, handle: FinishedHandle) {
        let mut inner = self.lock();
        let token = handle.token;
        if inner
            .consumer_entries
            .insert(token.clone(), handle.erased)
            .is_some()
        {
            inner.set_error(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "submit_consumer_handle: token `{token}` already submitted",
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
    pub fn take_by_token(
        &self,
        token: &KernelConsumerToken,
    ) -> Result<Box<dyn Any + Send>, DeltaError> {
        let mut inner = self.lock();
        if let Some(err) = inner.error.take() {
            return Err(err);
        }
        inner.consumer_entries.remove(token).ok_or_else(|| {
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
    fn lock(&self) -> MutexGuard<'_, StepResultInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::plans::kernel_consumers::KernelConsumerKind;
    use crate::schema::StructType;

    fn finished_handle(token: &KernelConsumerToken, payload: i64) -> FinishedHandle {
        FinishedHandle {
            token: token.clone(),
            sm_id: Uuid::new_v4(),
            sm_kind: "test",
            step_name: "phase",
            erased: Box::new(payload),
        }
    }

    #[test]
    fn submit_and_take_consumer_roundtrips_payload() {
        let s = StepResult::empty();
        let t = KernelConsumerToken::new(KernelConsumerKind::CheckpointHint);

        s.submit_consumer_handle(finished_handle(&t, 42));

        let payload = s.take_by_token(&t).expect("payload present");
        let v = *payload.downcast_ref::<i64>().unwrap();
        assert_eq!(v, 42);
    }

    #[test]
    fn submit_and_take_schema_roundtrips() {
        let s = StepResult::empty();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        s.submit_schema(schema.clone());
        let taken = s.take_schema().expect("schema present");
        assert!(Arc::ptr_eq(&taken, &schema));
    }

    #[test]
    fn duplicate_submit_under_same_token_errors_via_take_by_token() {
        let s = StepResult::empty();
        let t = KernelConsumerToken::new(KernelConsumerKind::CheckpointHint);
        s.submit_consumer_handle(finished_handle(&t, 10));
        s.submit_consumer_handle(finished_handle(&t, 20));
        let err = s.take_by_token(&t).unwrap_err();
        assert!(format!("{err}").contains("already submitted"));
    }

    #[test]
    fn duplicate_submit_schema_errors_via_take_schema() {
        let s = StepResult::empty();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        s.submit_schema(schema.clone());
        s.submit_schema(schema);
        let err = s.take_schema().unwrap_err();
        assert!(format!("{err}").contains("already submitted"));
    }
}
