//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use super::validation::{validate_read_result, validate_snapshot};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use delta_kernel_benchmarks::models::{ReadSpec, SnapshotSpec, Spec, TimeTravel};
use itertools::Itertools;
use url::Url;

/// Result of executing a read workload.
pub struct ReadResult {
    /// The record batches from the scan.
    pub batches: Vec<RecordBatch>,
    /// The kernel schema of the data.
    pub schema: Arc<Schema>,
}

/// Result of executing a snapshot workload.
#[derive(Debug)]
pub struct SnapshotResult {
    pub version: Version,
    pub protocol: Protocol,
    pub metadata: Metadata,
}

/// Execute a read workload.
pub fn execute_read_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    read_spec: &ReadSpec,
) -> DeltaResult<ReadResult> {
    // Resolve version from time_travel
    let version: Option<Version> = match &read_spec.time_travel {
        Some(TimeTravel::Version { version }) => Some(*version),
        Some(TimeTravel::Timestamp { timestamp: _ }) => {
            return Err(Error::generic(
                "Timestamp-based timetravel is not yet supported",
            ))
        }

        None => None,
    };

    if let Some(_predicate_str) = &read_spec.predicate {
        return Err(Error::generic("Workload predicates are not yet supported"));
    }

    // Build snapshot
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with optional column projection
    let mut scan_builder = snapshot.scan_builder();
    if let Some(ref cols) = read_spec.columns {
        let projected_schema = table_schema.project(cols)?;
        scan_builder = scan_builder.with_schema(projected_schema);
    }
    let scan = scan_builder.build()?;

    let schema = scan.logical_schema();

    // Execute scan
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            Ok(record_batch)
        })
        .try_collect()?;

    Ok(ReadResult {
        batches,
        schema: schema.clone(),
    })
}

/// Execute a snapshot workload (for metadata validation).
pub fn execute_snapshot_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    snapshot_spec: &SnapshotSpec,
) -> DeltaResult<SnapshotResult> {
    let version: Option<Version> = match &snapshot_spec.time_travel {
        Some(TimeTravel::Version { version }) => Some(*version),
        Some(TimeTravel::Timestamp { timestamp: _ }) => {
            return Err(Error::generic(
                "Timestamp-based timetravel is not yet supported",
            ))
        }
        None => None,
    };

    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

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
                .ok_or("ReadSpec missing expected field")?;
            let result = execute_read_workload(engine, table_root, read_spec);
            validate_read_result(result, expected_dir, expected)?;
        }
        Spec::Snapshot(snapshot_spec) => {
            let expected = snapshot_spec
                .expected
                .as_ref()
                .ok_or("SnapshotSpec missing expected field")?;
            let result = execute_snapshot_workload(engine, table_root, snapshot_spec);
            validate_snapshot(result, expected)?;
        }
    }
    Ok(())
}
