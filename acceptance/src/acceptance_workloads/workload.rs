//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use delta_kernel_benchmarks::models::{ReadSpec, SnapshotConstructionSpec, Spec, TimeTravel};
use delta_kernel_benchmarks::workload::build_scan_for_spec;
// Re-export `ReadResult` and `filter_batches_with_predicate` for callers that import
// them from this module. The canonical definitions live in
// `delta_kernel_benchmarks::workload` so the bench runners and the acceptance harness
// share one source of truth.
pub use delta_kernel_benchmarks::workload::{filter_batches_with_predicate, ReadResult};
use itertools::Itertools;
use url::Url;

use super::validation::{validate_read_result, validate_snapshot};

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
pub fn build_snapshot(
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
    let (scan, predicate) = build_scan_for_spec(snapshot, read_spec)?;

    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;
    let batches = filter_batches_with_predicate(batches, predicate.as_deref())?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    Ok(ReadResult {
        batches,
        schema: scan.logical_schema().clone(),
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
