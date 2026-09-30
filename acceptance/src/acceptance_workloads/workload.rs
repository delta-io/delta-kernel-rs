//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{Engine, KernelError, Result, Version};
use delta_kernel_workloads::models::{ReadSpec, SnapshotConstructionSpec, Spec, TimeTravel};
use delta_kernel_workloads::predicate_parser::parse_predicate;
use itertools::Itertools;
use url::Url;

use super::validation::{validate_read_result, validate_snapshot};

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
) -> Result<Arc<Snapshot>> {
    let version = time_travel
        .map(TimeTravel::as_version)
        .transpose()
        .map_err(KernelError::generic)?;

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
) -> Result<ReadResult> {
    let snapshot = build_snapshot(engine.as_ref(), table_root, read_spec.time_travel.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with optional predicate and column projection
    let mut scan_builder = snapshot.scan_builder();

    // Extract and parse the predicate if one is present
    let predicate = if let Some(ref predicate_string) = read_spec.predicate {
        let predicate =
            parse_predicate(predicate_string, &table_schema).map_err(KernelError::generic)?;
        let predicate = Arc::new(predicate);
        scan_builder = scan_builder.with_predicate(predicate.clone());
        Some(predicate)
    } else {
        None
    };

    let projected_schema = read_spec
        .columns
        .as_ref()
        .map(|columns| table_schema.project(columns))
        .transpose()?;
    let mut needs_post_projection = false;
    if let Some(columns) = &read_spec.columns {
        let scan_columns = scan_columns(columns, predicate.as_deref());
        needs_post_projection = scan_columns.len() != columns.len();
        scan_builder = scan_builder.with_schema(table_schema.project(&scan_columns)?);
    }
    let scan = scan_builder.build()?;

    let schema = projected_schema.unwrap_or_else(|| scan.logical_schema().clone());

    // Execute scan and apply row-level filtering
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;
    let batches = filter_batches_with_predicate(batches, predicate.as_deref())?;
    let batches = if needs_post_projection {
        project_batches(batches, read_spec.columns.as_deref())?
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

fn scan_columns(columns: &[String], predicate: Option<&Predicate>) -> Vec<String> {
    let mut scan_columns = columns.to_vec();
    if let Some(predicate) = predicate {
        for reference in predicate.references() {
            if let Some(column) = reference.path().first() {
                if !scan_columns.contains(column) {
                    scan_columns.push(column.clone());
                }
            }
        }
    }
    scan_columns
}

fn project_batches(
    batches: Vec<RecordBatch>,
    columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    let Some(columns) = columns else {
        return Ok(batches);
    };
    batches
        .into_iter()
        .map(|batch| {
            let indices: Vec<usize> = columns
                .iter()
                .map(|column| batch.schema().index_of(column).map_err(KernelError::from))
                .try_collect()?;
            Ok(batch.project(&indices)?)
        })
        .collect()
}

/// Filter record batches using a predicate expression.
fn filter_batches_with_predicate(
    batches: Vec<RecordBatch>,
    predicate: Option<&Predicate>,
) -> Result<Vec<RecordBatch>> {
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
) -> Result<SnapshotResult> {
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

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{Int32Array, RecordBatch};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::expressions::{col, lit, Predicate};

    use super::*;

    #[test]
    fn predicate_columns_are_available_until_after_projection() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("selected", ArrowDataType::Int32, true),
            Field::new("filter", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        let predicate = Predicate::gt(col!("filter"), lit(1));

        assert_eq!(
            scan_columns(&["selected".to_string()], Some(&predicate)),
            ["selected", "filter"]
        );

        let filtered = filter_batches_with_predicate(vec![batch], Some(&predicate)).unwrap();
        let projected = project_batches(filtered, Some(&["selected".to_string()])).unwrap();

        assert_eq!(projected[0].num_rows(), 2);
        assert_eq!(projected[0].schema().field(0).name(), "selected");
    }
}
