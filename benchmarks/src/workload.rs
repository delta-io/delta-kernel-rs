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

/// Drive an already-built [`Scan`] to completion via
/// [`DataFusionExecutor::scan_data`], collect the result batches, apply the residual
/// predicate (if any) post-collect via [`filter_batches_with_predicate`], and return a
/// [`ReadResult`].
///
/// The residual predicate runs post-collect rather than via `df.filter(...)` because
/// the scan's underlying plan still mixes batches whose nested partition-column
/// metadata isn't consistent across the parquet-read and partition-broadcast paths
/// (see `partitionValues_parsed.col-<uuid>`); pushing a filter into the DataFusion
/// plan triggers an arrow-side `coalesce_batches` assertion when these mixed-metadata
/// batches need to be combined. The outer projection installed by the SSA scan compile
/// path makes the *output* schema consistent (logical names + Delta metadata everywhere),
/// but the *input-side* coalesce inside the scan plan still sees the unstamped variants.
/// Until those upstream operators stamp consistent metadata too, predicate pushdown stays
/// disabled here. The post-collect path runs on already-stamped batches, sidestepping the
/// mismatch.
pub async fn execute_read_via_datafusion(
    executor: &DataFusionExecutor,
    scan: &Scan,
    post_filter_predicate: Option<&Predicate>,
) -> DeltaResult<ReadResult> {
    let raw_batches = executor
        .scan_data(scan)
        .await
        .map_err(|e| Error::generic(format!("drive replay scan via DataFusionExecutor: {e}")))?
        .collect()
        .await
        .map_err(|e| Error::generic(format!("collect replay scan DataFrame: {e}")))?;
    let batches = filter_batches_with_predicate(raw_batches, post_filter_predicate)?;
    let logical_schema = scan.logical_schema();
    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(ReadResult {
        batches,
        schema: logical_schema.clone(),
        row_count,
    })
}
