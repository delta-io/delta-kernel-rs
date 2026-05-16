//! Canonical Full Snapshot Read (FSR) declarative plans — *window-on-commits +
//! anti-join-on-checkpoint*.
//!
//! Mirrors Delta log-replay semantics (`kernel/src/action_reconciliation/log_replay.rs`,
//! `kernel/src/log_replay/deduplicator.rs`) by composing three or four declarative plans
//! that flow as one [`PhaseOperation::Plans`] step:
//!
//! 1. **commit_load** — `Values(commit metadata) → LoadSink(JSON, action schema,
//!    passthrough=[version])` materializes the raw per-commit action stream into
//!    [`FSR_COMMIT_RAW`]. The kernel cover over `ascending_commit_files ∪
//!    ascending_compaction_files` is materialized verbatim so that downstream steps can `ORDER BY
//!    version DESC` to recover Delta's "newest action wins" semantics inside the commit tail.
//! 2. **commit_dedup** — `RelationRef(commit_raw) → Filter(has identity) → Project(action_cols +
//!    key) → Window(row_number PARTITION BY key ORDER BY version DESC) → Filter(__rn <= k) →
//!    Project(action_cols + key)` yields the *commit winners*, materialized into
//!    [`FSR_COMMIT_DEDUP`]. Commits supersede (and remove-tombstone) checkpoint state for any
//!    `(action_kind, identity)` pair they touch.
//! 3. **(only when `has_sidecars`) sidecar_load** — `Scan(top-level checkpoint, sidecar-only
//!    schema) → Filter(sidecar IS NOT NULL) → Project(sidecar.path, sidecar.sizeInBytes) →
//!    LoadSink(Parquet, action schema)` materializes each V2-multipart sidecar parquet's action
//!    rows into [`FSR_SIDECAR_ACTIONS`]. V1 / V2-inline checkpoints carry no sidecars; this plan is
//!    omitted entirely so the executor never opens them.
//! 4. **results** — `(Scan(top-level checkpoint) [∪ RelationRef(sidecar_actions)]) → Filter(has
//!    identity) → Project(action_cols + key) → LeftAntiJoin(probe=this,
//!    build=RelationRef(commit_dedup).project(key))` materializes the *checkpoint survivors* (rows
//!    the commit tail didn't touch). The plan completes with `Union(RelationRef(commit_dedup),
//!    survivors) -> Filter(retention) -> Project(action_read_schema) -> into_results()` so the
//!    engine's `Results` consumer sees the reconstructed action stream.
//!
//! The window applies only to the (typically-small) commit-tail stream; the (typically-large)
//! checkpoint stream goes through a single hash anti-join keyed on the dedup column. Compared
//! with windowing the union of commits and checkpoint, this avoids materializing per-key
//! orderings over the entire snapshot.
//!
//! ## Decision notes
//!
//! - **`dv_unique_id`**: Per-row deletion-vector identity, used only as a `partition_by` / join-key
//!   contribution to the dedup key. Implemented as `If(storageType IS NULL, NULL,
//!   ToJson(Array(storageType, pathOrInlineDv)))` — semantics-equivalent to
//!   [`crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id_from_parts`] (matches
//!   [`crate::log_replay::deduplicator::Deduplicator::extract_dv_unique_id`]) for equality /
//!   non-equality but not byte-for-byte. The exact byte form is not protocol-stable, so the
//!   difference does not affect correctness; it only avoids overloading
//!   [`crate::expressions::BinaryExpressionOp::Plus`] with UTF-8 concat semantics. Note: `offset`
//!   is intentionally omitted (UUID DVs have no offset; inline DVs with the same `pathOrInlineDv`
//!   and distinct offsets would imply two distinct in-line DV byte payloads, which is not
//!   representable). A follow-up can include `offset` once kernel grows an int-to-string cast.
//! - **`action_read_schema`**: Full reconstructed action stream (add / remove / protocol / metaData
//!   / domainMetadata / txn).
//! - **Retention thresholds**: Derived like checkpoint reconciliation via
//!   [`crate::action_reconciliation::deleted_file_retention_timestamp_with_time`] and
//!   [`crate::action_reconciliation::calculate_transaction_expiration_timestamp`] against
//!   [`crate::snapshot::Snapshot::table_properties`] (`kernel/src/table_properties/mod.rs`).
//! - **`CheckpointShape.file_format`**: Taken from the first checkpoint part's filename extension
//!   in the snapshot listing (`crate::path::ParsedLogPath::extension`), with `_last_checkpoint`
//!   schema falling back through [`crate::log_segment::LogSegment::checkpoint_schema`].

use std::sync::Arc;

use super::checkpoint_shape::{checkpoint_shape_from_last_checkpoint, CheckpointShape};
use super::plans::build_fsr_plans;
// Re-exports for the FSR relation handle name constants. External code references these via
// `fsr::full_state::FSR_COMMIT_DEDUP`; the actual definitions live in `super::plans`.
pub use super::plans::{
    CommitFileMeta, FSR_CHECKPOINT_TOP, FSR_COMMIT_DEDUP, FSR_COMMIT_RAW, FSR_SIDECAR_ACTIONS,
};
use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::Plan;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::snapshot::Snapshot;

/// Builder namespace for canonical Full State Reconstruction planning.
#[derive(Debug, Clone, Copy, Default)]
pub struct FullState;

/// Builder for canonical Full State Reconstruction plans.
#[derive(Debug, Clone)]
pub struct FullStateBuilder {
    snapshot: Arc<Snapshot>,
    checkpoint_shape: Option<CheckpointShape>,
}

impl FullState {
    /// Start building canonical FSR plans for `snapshot`.
    pub fn for_table(snapshot: Arc<Snapshot>) -> FullStateBuilder {
        FullStateBuilder {
            snapshot,
            checkpoint_shape: None,
        }
    }
}

impl FullStateBuilder {
    /// Override checkpoint shape discovery used by [`Self::build`].
    pub fn with_checkpoint_shape(mut self, shape: CheckpointShape) -> Self {
        self.checkpoint_shape = Some(shape);
        self
    }

    /// Build canonical FSR plans for this snapshot.
    pub fn build(self) -> Result<Vec<Plan>, DeltaError> {
        let shape = self
            .checkpoint_shape
            .unwrap_or(checkpoint_shape_from_last_checkpoint(
                self.snapshot.as_ref(),
            )?);
        build_fsr_plans(self.snapshot.as_ref(), shape)
    }

    /// Enable stats-aware planning on this builder.
    ///
    /// This is currently an API-level compatibility shim to align the FullState
    /// builder surface with scan replay builders. Canonical FSR output plans
    /// already preserve Add action stats columns, so there is no additional
    /// rewrite needed at this layer.
    pub fn with_stats(self) -> Self {
        self
    }
}

/// Public entry for full-state reconstruction.
pub fn full_state_sm(snapshot: Arc<Snapshot>) -> Result<CoroutineSM<()>, DeltaError> {
    CoroutineSM::new(move |mut co| async move {
        let mut phase = Phase(&mut co);

        // Derive shape from log-segment metadata (including checkpoint file format) so
        // JSON-v2 checkpoints are lowered correctly.
        let plans = FullState::for_table(Arc::clone(&snapshot)).build()?;
        let _state = phase
            .execute(PhaseOperation::Plans(plans), "fsr.full_state")
            .await
            .map_err(|e| {
                let detail = e.display_with_source_chain();
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    source = e,
                    "fsr::full_state::execute: {detail}",
                )
            })?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::checkpoint_shape::checkpoint_shape_from_last_checkpoint;
    use super::super::plans::build_fsr_plans;
    use super::*;
    use crate::utils::test_utils::load_test_table;

    const REPLAY_COVERAGE_TABLES: &[&str] = &[
        "app-txn-no-checkpoint",
        "app-txn-checkpoint",
        "v2-checkpoints-parquet-without-sidecars",
        "v2-checkpoints-parquet-with-sidecars",
    ];

    #[test]
    fn full_state_builder_matches_direct_fsr_build() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(&snapshot).unwrap();
        let direct = build_fsr_plans(&snapshot, shape.clone()).unwrap();
        let built = FullState::for_table(Arc::clone(&snapshot))
            .with_stats()
            .with_checkpoint_shape(shape)
            .build()
            .unwrap();
        assert_eq!(direct.len(), built.len());
    }

    #[test]
    fn full_state_builder_with_stats_works_across_checkpoint_configs() {
        for table in REPLAY_COVERAGE_TABLES {
            let (_engine, snapshot, _tmp) = load_test_table(table).unwrap();
            let shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref()).unwrap();
            let direct = build_fsr_plans(snapshot.as_ref(), shape.clone()).unwrap();
            let built = FullState::for_table(Arc::clone(&snapshot))
                .with_stats()
                .with_checkpoint_shape(shape)
                .build()
                .unwrap();
            assert_eq!(
                direct.len(),
                built.len(),
                "FullState builder should match direct build for table {table}",
            );
        }
    }
}
