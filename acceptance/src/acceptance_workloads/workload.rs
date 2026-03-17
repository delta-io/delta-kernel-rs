//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use super::validation::{validate_read_result, validate_snapshot};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use delta_kernel_benchmarks::models::{ReadSpec, SnapshotConstructionSpec, Spec, TimeTravel};
use delta_kernel_benchmarks::predicate_parser::parse_predicate;
use itertools::Itertools;
use url::Url;

/// Result of executing a read workload.
#[derive(Debug)]
pub struct ReadResult {
    /// The record batches from the scan.
    pub batches: Vec<RecordBatch>,
    /// The kernel schema of the data.
    pub schema: Arc<Schema>,
    /// Total number of rows in the result.
    pub row_count: u64,
}

/// Result of executing a snapshot workload.
#[derive(Debug)]
pub struct SnapshotResult {
    /// The version of the snapshot.
    pub version: Version,
    /// The protocol at this version.
    pub protocol: Protocol,
    /// The table metadata at this version.
    pub metadata: Metadata,
}

/// Build a snapshot with optional time travel.
fn build_snapshot(
    engine: &dyn Engine,
    table_root: &Url,
    time_travel: Option<&TimeTravel>,
) -> DeltaResult<Arc<Snapshot>> {
    let version = time_travel
        .map(TimeTravel::as_version)
        .transpose()
        .map_err(Error::generic)?;

    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    builder.build(engine)
}

/// Execute a read workload.
pub fn execute_read_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    read_spec: &ReadSpec,
) -> DeltaResult<ReadResult> {
    let snapshot = build_snapshot(engine.as_ref(), table_root, read_spec.time_travel.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with optional predicate and column projection
    let mut scan_builder = snapshot.scan_builder();

    // Extract and parse the predicate if one is present
    let predicate = if let Some(ref predicate_string) = read_spec.predicate {
        let predicate = parse_predicate(predicate_string, &table_schema).map_err(Error::generic)?;
        scan_builder = scan_builder.with_predicate(Arc::new(predicate.clone()));
        Some(predicate)
    } else {
        None
    };

    if let Some(ref cols) = read_spec.columns {
        let projected_schema = table_schema.project(cols)?;
        scan_builder = scan_builder.with_schema(projected_schema);
    }
    let scan = scan_builder.build()?;

    let schema = scan.logical_schema();

    // Execute scan
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;

    // Apply row-level filtering if predicate is present
    // (kernel only does data skipping, not row filtering)
    let batches = if let Some(ref pred) = predicate {
        batches
            .into_iter()
            .map(|batch| {
                let mask = evaluate_predicate(pred, &batch, false)?;
                filter_record_batch(&batch, &mask).map_err(Error::from)
            })
            .try_collect()?
    } else {
        batches
    };

    // Compute row count from filtered batches
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    Ok(ReadResult {
        batches,
        schema: schema.clone(),
        row_count,
    })
}

/// Execute a snapshot workload (for metadata validation).
pub fn execute_snapshot_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    snapshot_spec: &SnapshotConstructionSpec,
) -> DeltaResult<SnapshotResult> {
    let snapshot = build_snapshot(
        engine.as_ref(),
        table_root,
        snapshot_spec.time_travel.as_ref(),
    )?;

    let config = snapshot.table_configuration();

    Ok(SnapshotResult {
        version: snapshot.version(),
        protocol: config.protocol().clone(),
        metadata: config.metadata().clone(),
    })
}

/// Execute a workload and validate results.
pub fn execute_and_validate_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    spec: &Spec,
    expected_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    match spec {
        Spec::Read(read_spec) => {
            let expected = read_spec
                .expected
                .as_ref()
                .ok_or("ReadSpec must have expected or error field")?;
            let result = execute_read_workload(engine, table_root, read_spec);
            validate_read_result(result, expected_dir, expected)?;
        }
        Spec::SnapshotConstruction(snapshot_spec) => {
            let expected = snapshot_spec
                .expected
                .as_ref()
                .ok_or("SnapshotSpec must have expected or error field")?;
            let result = execute_snapshot_workload(engine, table_root, snapshot_spec.as_ref());
            validate_snapshot(result, expected)?;
        }
    }
    Ok(())
}
