//! Runtime state for KDF reducers.
//!
//! [`ReducerHandle`] is the executor's working buffer for one Reduce step: created when a
//! phase starts, fed batches via [`ReducerHandle::apply`], finalized via
//! [`ReducerHandle::finish`] when the child is exhausted. Type-erased into [`FinishedHandle`]
//! and returned to the state machine as the Reduce response.
//!
//! Handles dispatch in-process and never cross a serialization boundary.
//!
//! Callers recover typed output from a [`FinishedHandle`] via the paired [`Extractor`], minted
//! at plan-build time and threaded through to the SM body.
// Intra-doc links to the state-machine framework / IR sink types intentionally degrade to
// plain text in this module slice (the linked items live in sibling stacks).
#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]

use std::any::Any;

use super::reducer::{KdfControl, KernelReducer, KernelReducerOutput, KernelReducerToken};
use crate::plans::errors::DeltaError;
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::{DeltaResult, EngineData};

/// Runtime state carrier. Holds the mutable reducer working buffer and the token that joins
/// its eventual finalized state back to the plan-tree node.
#[derive(Debug)]
pub struct ReducerHandle {
    token: KernelReducerToken,
    inner: Box<dyn KernelReducer>,
}

impl ReducerHandle {
    /// Construct a handle from a fresh token and a cloned initial state.
    pub fn new(token: KernelReducerToken, inner: Box<dyn KernelReducer>) -> Self {
        Self { token, inner }
    }

    /// Apply the reducer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kernel_reducer.apply",
        skip(self, batch),
        ret,
        fields(kind = %self.inner.kind(), token_id = %self.token.id),
    )]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.inner.apply(batch)
    }

    /// Consume the handle, returning the finalized token-stamped state.
    #[tracing::instrument(
        level = "debug",
        name = "kernel_reducer.finish",
        skip(self),
        fields(kind = %self.inner.kind(), token_id = %self.token.id),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kernel reducer handle finished");
        FinishedHandle {
            token: self.token,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`ReducerHandle::finish`] -- carries the token and the type-erased final state.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KernelReducerToken,
    pub erased: Box<dyn Any + Send>,
}

// === Typed extraction ===

/// A typed adapter for pulling the typed output of a single reduce sink
/// out of a [`FinishedHandle`].
///
/// SM bodies build an `Extractor` while planting an [`EngineRequest::Reduce`] (via the
/// `Context::reduce` helper, added in a sibling submodule) and feed the engine's
/// [`FinishedHandle`] back through [`Self::extract`] on resume.
///
/// [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::state_machine::EngineRequest::Reduce
pub struct Extractor<O> {
    token: KernelReducerToken,
    extract: fn(Box<dyn Any + Send>) -> Result<O, DeltaError>,
}

impl<O: Send + 'static> Extractor<O> {
    /// Build an `Extractor` for KDF state `S` at `token`. The stored function pointer
    /// downcasts the erased payload back to `S` and runs `S::into_output`.
    // Consumer (`Context::reduce` dispatch) lives in a sibling state-machine module.
    #[allow(dead_code)]
    pub(crate) fn for_reducer<S>(token: KernelReducerToken) -> Self
    where
        S: KernelReducerOutput<Output = O> + 'static,
    {
        Self {
            token,
            extract: extract_reducer::<S>,
        }
    }

    /// Decode `handle`'s payload into the typed output `O`.
    ///
    /// Sanity-checks that `handle.token` matches this extractor's token (cross-wired
    /// finished handles surface as an internal error) and runs the typed reduction.
    /// Decoding failures are wrapped in [`EngineError::internal`] so SM bodies can
    /// uniformly handle them on the engine-error path.
    pub fn extract(self, handle: FinishedHandle) -> Result<O, EngineError> {
        if handle.token != self.token {
            return Err(EngineError::internal(DeltaError::invariant(format!(
                "kernel_reducer::extract: token mismatch -- handle token `{}` vs expected `{}`",
                handle.token, self.token,
            ))));
        }
        (self.extract)(handle.erased).map_err(EngineError::internal)
    }
}

impl<O> std::fmt::Debug for Extractor<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extractor")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Downcast the erased payload back to `S` and run its typed reduction. Bound to a
/// concrete `S` via `Extractor::for_reducer`'s generic fn-pointer coercion.
#[allow(dead_code)] // only consumer is for_reducer, gated above
fn extract_reducer<S>(erased: Box<dyn Any + Send>) -> Result<S::Output, DeltaError>
where
    S: KernelReducerOutput + 'static,
{
    let single = erased.downcast::<S>().map(|b| *b).map_err(|_| {
        DeltaError::invariant(format!(
            "kernel_reducer::extract: expected `{}`",
            std::any::type_name::<S>(),
        ))
    })?;
    single.into_output()
}

#[cfg(test)]
mod tests {
    use super::super::{CheckpointHintReader, KernelReducerKind, KernelReducerToken};
    use super::*;

    /// Exercise the Extractor round-trip end-to-end: build a CheckpointHintReader, drive it
    /// through ReducerHandle, finish it, hand the FinishedHandle to a matching Extractor,
    /// and assert the typed output. Catches any drift between token wiring, downcast typing,
    /// and KernelReducerOutput::into_output.
    #[test]
    fn extractor_round_trip_returns_typed_state() {
        // Construct a populated reader via the visit path so we don't need to peek at
        // private fields. We feed it a synthetic single-row batch shaped like
        // `_last_checkpoint`.
        use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch};
        use crate::arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        };
        use crate::engine::arrow_data::ArrowEngineData;
        let schema = std::sync::Arc::new(ArrowSchema::new(vec![
            ArrowField::new("version", ArrowDataType::Int64, false),
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new("parts", ArrowDataType::Int64, true),
            ArrowField::new("sizeInBytes", ArrowDataType::Int64, true),
            ArrowField::new("numOfAddFiles", ArrowDataType::Int64, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            std::sync::Arc::new(Int64Array::from(vec![7])),
            std::sync::Arc::new(Int64Array::from(vec![Some(1024)])),
            std::sync::Arc::new(Int64Array::from(vec![None])),
            std::sync::Arc::new(Int64Array::from(vec![Some(2048)])),
            std::sync::Arc::new(Int64Array::from(vec![Some(3)])),
        ];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let engine_data = ArrowEngineData::new(batch);

        let token = KernelReducerToken::new(KernelReducerKind::CheckpointHint);
        let reader = CheckpointHintReader::default();
        let mut handle = ReducerHandle::new(token.clone(), Box::new(reader));
        assert_eq!(handle.apply(&engine_data).unwrap(), KdfControl::Break);
        let finished = handle.finish();

        let extractor =
            Extractor::<<CheckpointHintReader as KernelReducerOutput>::Output>::for_reducer::<
                CheckpointHintReader,
            >(token);
        let out = extractor
            .extract(finished)
            .expect("typed extract must succeed");
        let rec = out.expect("record");
        assert_eq!(rec.version, 7);
        assert_eq!(rec.num_of_add_files, Some(3));
    }

    /// Mismatched tokens MUST surface as an EngineError::internal -- the extractor's whole
    /// raison d'etre is to detect cross-wired handles between phases of a plan.
    #[test]
    fn extractor_rejects_mismatched_token() {
        let token_a = KernelReducerToken::new(KernelReducerKind::CheckpointHint);
        let token_b = KernelReducerToken::new(KernelReducerKind::CheckpointHint);
        let reader = CheckpointHintReader::default();
        let handle = ReducerHandle::new(token_a, Box::new(reader));
        let finished = handle.finish();

        let extractor =
            Extractor::<<CheckpointHintReader as KernelReducerOutput>::Output>::for_reducer::<
                CheckpointHintReader,
            >(token_b);
        let err = extractor
            .extract(finished)
            .expect_err("mismatched tokens must error");
        // EngineError::internal hides the source in Display; assert the source chain
        // (which is what SM bodies surface via display_with_source_chain) contains the
        // diagnostic message.
        let chain = err.display_with_source_chain();
        assert!(
            chain.contains("token mismatch"),
            "source chain missing token mismatch: {chain}"
        );
    }
}
