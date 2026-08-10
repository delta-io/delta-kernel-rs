//! End-to-end insert benchmark: partition-column materialization on vs off.
//!
//! Each iteration creates a fresh partitioned table (9 `long` data columns + 1 `string`
//! partition column = 10 columns total) and times the full write+commit of a single
//! 100k-row batch. With materialization the partition column is physically written into the
//! Parquet data file; without it, the value lives only in the Add action's `partitionValues`.
//!
//! Table creation (setup) is excluded from the timing via `iter_batched`. The data batch never
//! contains the partition column -- per the kernel write contract the connector always supplies
//! data without partition columns, and the kernel inserts them when materializing.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use test_utils::write_batch_to_table;

const NUM_ROWS: usize = 1_000_000;
const NUM_DATA_COLS: usize = 9; // + 1 partition column = 10 columns total
const PARTITION_COL: &str = "p";
const PARTITION_VALUE: &str = "p0";

type Engine = DefaultEngine<TokioMultiThreadExecutor>;

// Long data columns `c0..cN`, excluding the partition column.
fn data_fields() -> impl Iterator<Item = StructField> {
    (0..NUM_DATA_COLS).map(|i| StructField::nullable(format!("c{i}"), DataType::LONG))
}

// The 10-column logical table schema: data columns plus one string partition column.
fn table_schema() -> Arc<StructType> {
    let fields = data_fields().chain([StructField::nullable(PARTITION_COL, DataType::STRING)]);
    Arc::new(StructType::try_new(fields).unwrap())
}

// A NUM_ROWS batch of the data columns only (no partition column).
fn data_batch() -> RecordBatch {
    let kernel_schema = StructType::try_new(data_fields()).unwrap();
    let arrow_schema: Arc<ArrowSchema> = Arc::new((&kernel_schema).try_into_arrow().unwrap());
    let columns: Vec<ArrayRef> = (0..NUM_DATA_COLS)
        .map(|i| {
            let base = (i * NUM_ROWS) as i64;
            Arc::new(Int64Array::from_iter_values(
                (0..NUM_ROWS as i64).map(|r| base + r),
            )) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(arrow_schema, columns).unwrap()
}

// Creates a fresh empty partitioned table and returns its post-create snapshot. `materialize`
// toggles the `materializePartitionColumns` writer feature.
fn fresh_table(
    runtime: &tokio::runtime::Runtime,
    materialize: bool,
) -> (tempfile::TempDir, Arc<Engine>, Arc<Snapshot>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_url = url::Url::from_directory_path(temp_dir.path()).unwrap();
    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(runtime.handle().clone()));
    let engine = Arc::new(
        DefaultEngine::builder(store)
            .with_task_executor(executor)
            .build(),
    );

    // Mirror the integration-test contract: table creation runs inside the runtime context.
    let snapshot = runtime.block_on(async {
        let mut builder = create_table(table_url.as_str(), table_schema(), "bench/1.0")
            .with_data_layout(DataLayout::partitioned([PARTITION_COL]));
        if materialize {
            builder = builder
                .with_table_properties([("delta.feature.materializePartitionColumns", "supported")]);
        }
        let _ = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap()
            .commit(engine.as_ref())
            .unwrap();
        Snapshot::builder_for(table_url.as_str())
            .build(engine.as_ref())
            .unwrap()
    });

    (temp_dir, engine, snapshot)
}

fn insert_benchmarks(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let batch = data_batch();
    let partition_values =
        HashMap::from([(PARTITION_COL.to_string(), Scalar::String(PARTITION_VALUE.into()))]);

    let mut group = c.benchmark_group("insert_1m");
    for materialize in [false, true] {
        let label = if materialize { "materialized" } else { "not_materialized" };
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let (temp_dir, engine, snapshot) = fresh_table(&runtime, materialize);
                    (temp_dir, engine, snapshot, batch.clone(), partition_values.clone())
                },
                |(temp_dir, engine, snapshot, batch, pvs)| {
                    runtime
                        .block_on(write_batch_to_table(&snapshot, engine.as_ref(), batch, pvs))
                        .expect("write failed");
                    // Keep the table dir alive until the timed write completes.
                    drop(temp_dir);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default().sample_size(20)
}

criterion_group! {
    name = benches;
    config = config();
    targets = insert_benchmarks
}
criterion_main!(benches);
