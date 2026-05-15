//! Classic single-file checkpoint parquet write plans for DataFusion-backed SM drivers.
//!
//! ## Supported path
//!
//! Classic-named parquet checkpoints (`NNNN.checkpoint.parquet`), including tables with the
//! `v2Checkpoint` feature where checkpoint rows include a trailing `checkpointMetadata` batch.
//!
//! ## Explicit gaps (TODO)
//!
//! - **Multipart V2 checkpoints / sidecars**: emitting shard parquet files plus manifest sidecars
//!   is not modeled as declarative plans yet. [`crate::checkpoint::CheckpointWriter`] continues to
//!   use single-file classic paths only; DF [`crate::plans::ir::nodes::SinkType::PartitionedWrite`]
//!   is not wired for checkpoint shards.
//! - **`_last_checkpoint` extras**: [`crate::checkpoint::create_last_checkpoint_data`] documents
//!   protocol TODOs (`checkpoint_schema`, `tags`, checksum, ...).

use std::sync::Arc;

use url::Url;

use crate::action_reconciliation::ActionReconciliationIteratorState;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::CheckpointWriter;
use crate::engine::arrow_data::EngineDataArrowExt;
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{RelationHandle, WriteSink};
use crate::plans::ir::{DeclarativePlanNode, Plan};
use crate::plans::state_machines::df::insert::insert_write_sm;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::{DeltaResult, Engine};

/// Materialize checkpoint rows as Arrow batches and mint a [`RelationHandle`] for DF consumption.
///
/// Drains [`CheckpointWriter::checkpoint_data`]. Callers register `(handle.id, batches)` on the DF
/// executor relation registry before compiling [`checkpoint_classic_parquet_write_plan`].
///
/// Returns shared reconciliation iterator state for [`crate::checkpoint::LastCheckpointHintStats`]
/// once the parquet write completes (single-file checkpoints use `num_sidecars = 0` today).
pub fn prepare_classic_checkpoint_parquet_materialization(
    engine: &dyn Engine,
    writer: &CheckpointWriter,
) -> DeltaResult<(
    RelationHandle,
    Vec<RecordBatch>,
    Url,
    Arc<ActionReconciliationIteratorState>,
)> {
    let schema = writer.checkpoint_output_schema(engine)?;
    let handle = RelationHandle::fresh("checkpoint_parquet_rows", schema);
    let destination = writer.checkpoint_path()?;
    let iter = writer.checkpoint_data(engine)?;
    let state = iter.state();
    let mut batches = Vec::new();
    for batch in iter {
        let rb = batch?.apply_selection_vector()?.try_into_record_batch()?;
        batches.push(rb);
    }
    Ok((handle, batches, destination, state))
}

/// Declarative plan: stream registered checkpoint batches into the classic parquet checkpoint path.
pub fn checkpoint_classic_parquet_write_plan(handle: RelationHandle, destination: Url) -> Plan {
    DeclarativePlanNode::RelationRef(handle).into_write(WriteSink::parquet(destination))
}

/// Single-phase SM: write a classic parquet checkpoint plan.
///
/// Thin alias over [`super::insert_write_sm`] — both reduce to "drive one
/// [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan to completion."
pub fn checkpoint_classic_parquet_write_sm(plan: Plan) -> Result<CoroutineSM<()>, DeltaError> {
    insert_write_sm(plan)
}
