//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use super::validation::{validate_read_result, validate_snapshot};
use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::scan::state::{transform_to_logical, ScanFile};
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, FileMeta, Version};
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
    /// Number of files read.
    pub file_count: u64,
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
    // Parse predicate if present
    let predicate = read_spec
        .predicate
        .as_deref()
        .map(|p| parse_predicate(p))
        .transpose()
        .map_err(|e| Error::generic(format!("Failed to parse predicate: {e}")))?;

    let snapshot = build_snapshot(engine.as_ref(), table_root, read_spec.time_travel.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with column projection (no predicate pushdown - we filter after)
    let mut scan_builder = snapshot.scan_builder();
    if let Some(ref cols) = read_spec.columns {
        let projected_schema = table_schema.project(cols)?;
        scan_builder = scan_builder.with_schema(projected_schema);
    }
    let scan = scan_builder.build()?;

    let schema = scan.logical_schema();
    let physical_schema = scan.physical_schema().clone();
    let logical_schema = scan.logical_schema().clone();

    // Collect scan files from metadata, counting files as we go
    fn scan_file_callback(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
    }

    let scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;
    let scan_files: Vec<ScanFile> = scan_metadata_iter
        .map(|res| {
            let scan_metadata = res?;
            scan_metadata.visit_scan_files(vec![], scan_file_callback)
        })
        .flatten_ok()
        .try_collect()?;

    let file_count = scan_files.len() as u64;

    // Read each file and collect batches
    let mut batches = Vec::new();
    for scan_file in scan_files {
        let file_path = table_root.join(&scan_file.path)?;
        let mut selection_vector = scan_file
            .dv_info
            .get_selection_vector(engine.as_ref(), table_root)?;

        let meta = FileMeta {
            last_modified: 0,
            size: scan_file.size.try_into().map_err(|_| {
                Error::generic("Unable to convert scan file size into FileSize")
            })?,
            location: file_path,
        };

        let read_result_iter = engine.parquet_handler().read_parquet_files(
            &[meta],
            physical_schema.clone(),
            None,
        )?;

        for read_result in read_result_iter {
            let read_result = read_result?;
            let logical = transform_to_logical(
                engine.as_ref(),
                read_result,
                &physical_schema,
                &logical_schema,
                scan_file.transform.clone(),
            );
            let len = logical.as_ref().map_or(0, |res| res.len());

            // Split selection vector for this batch
            let mut sv = selection_vector.take();
            let rest = split_vector(sv.as_mut(), len, None);
            let result = match sv {
                Some(sv) => logical.and_then(|data| data.apply_selection_vector(sv)),
                None => logical,
            };
            selection_vector = rest;

            let batch = result?.try_into_record_batch()?;
            batches.push(batch);
        }
    }

    // Filter batches using the predicate if present
    let batches = filter_batches_with_predicate(batches, predicate.as_ref())?;

    // Compute row count from filtered batches
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    Ok(ReadResult {
        batches,
        schema: schema.clone(),
        file_count,
        row_count,
    })
}

/// Filter record batches using a predicate expression.
fn filter_batches_with_predicate(
    batches: Vec<RecordBatch>,
    predicate: Option<&Predicate>,
) -> DeltaResult<Vec<RecordBatch>> {
    let Some(predicate) = predicate else {
        return Ok(batches);
    };

    batches
        .into_iter()
        .map(|batch| {
            // Evaluate predicate to get boolean selection array
            let selection = evaluate_predicate(predicate, &batch, false)?;
            // Filter the batch using the selection
            let filtered = filter_record_batch(&batch, &selection)?;
            Ok(filtered)
        })
        .collect()
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
