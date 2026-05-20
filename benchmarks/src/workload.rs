//! Shared workload-execution helpers.
//!
//! Centralizes the [`Scan`] construction step that is common to every read workload
//! (default-engine benchmark runner, DataFusion benchmark runner, default-engine
//! acceptance harness, and DataFusion acceptance harness), plus the DataFusion-specific
//! drive step used by the DataFusion benchmark runner and the DataFusion acceptance
//! harness.

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::{Predicate, PredicateRef};
use delta_kernel::scan::Scan;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Error};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use futures::TryStreamExt;

use crate::models::ReadSpec;
use crate::predicate_parser::parse_predicate;

/// Result of executing a read workload via any engine.
#[derive(Debug)]
pub struct ReadResult {
    /// The record batches from the scan, after applying any post-scan residual filter.
    pub batches: Vec<RecordBatch>,
    /// The logical (output) schema of the scan.
    pub schema: SchemaRef,
    /// Total number of rows across all filtered batches.
    pub row_count: u64,
}

/// Filter record batches using a predicate expression, evaluating it per batch and
/// keeping rows where the predicate evaluates to TRUE.
pub fn filter_batches_with_predicate(
    batches: Vec<RecordBatch>,
    predicate: Option<&Predicate>,
) -> DeltaResult<Vec<RecordBatch>> {
    let Some(predicate) = predicate else {
        return Ok(batches);
    };
    batches
        .into_iter()
        .map(|batch| {
            let selection = evaluate_predicate(predicate, &batch, false)?;
            let filtered = filter_record_batch(&batch, &selection)?;
            Ok(filtered)
        })
        .collect()
}

/// Build a [`Scan`] configured per the [`ReadSpec`] (predicate + column projection),
/// alongside the original logical-schema predicate (if any).
///
/// The original predicate is returned in addition to being installed on the scan
/// because kernel rewrites the predicate to physical column names inside the scan;
/// only the original predicate can be evaluated against the scan's logical-schema
/// output batches. This matters for column-mapping tables where logical and physical
/// names diverge.
///
/// Engines that don't need a post-scan residual filter can simply ignore the returned
/// predicate.
pub fn build_scan_for_spec(
    snapshot: Arc<Snapshot>,
    read_spec: &ReadSpec,
) -> DeltaResult<(Scan, Option<PredicateRef>)> {
    let table_schema = snapshot.schema();
    let predicate = read_spec
        .predicate
        .as_deref()
        .map(|sql| parse_predicate(sql, &table_schema))
        .transpose()
        .map_err(Error::generic)?
        .map(Arc::new);

    let mut builder = snapshot.scan_builder();
    if let Some(ref predicate_ref) = predicate {
        builder = builder.with_predicate(predicate_ref.clone());
    }
    if let Some(cols) = read_spec.columns.as_ref() {
        let projected_schema = table_schema.project(cols)?;
        builder = builder.with_schema(projected_schema);
    }
    let scan = builder.build()?;
    Ok((scan, predicate))
}

/// Drive an already-built [`Scan`] to completion via the [`DataFusionExecutor`],
/// collect the result batches, apply the residual predicate (if any), and return a
/// [`ReadResult`].
pub async fn execute_read_via_datafusion(
    executor: &DataFusionExecutor,
    scan: &Scan,
    post_filter_predicate: Option<&Predicate>,
) -> DeltaResult<ReadResult> {
    let sm = scan
        .scan_state_machine()
        .map_err(|e| Error::generic(format!("build replay scan SM: {e}")))?;
    let batches: Vec<RecordBatch> = executor
        .drive_to_stream(sm)
        .await
        .map_err(|e| Error::generic(format!("drive replay scan via DataFusionExecutor: {e}")))?
        .try_collect()
        .await
        .map_err(|e| Error::generic(format!("collect replay scan stream: {e}")))?;
    let batches = filter_batches_with_predicate(batches, post_filter_predicate)?;
    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(ReadResult {
        batches,
        schema: scan.logical_schema().clone(),
        row_count,
    })
}
