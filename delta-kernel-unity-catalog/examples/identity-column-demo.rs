//! End-to-end walkthrough of Concurrent Identity Columns (CIC).
//!
//! Drives the full flow -- allocate sequences in UC, stamp the returned
//! `sequence_id`s into a Delta schema, create the table, reserve ranges, inject
//! identity values into a data batch, write a Parquet file, commit v1, and
//! read the table back -- printing every step so you can verify what kernel
//! does on disk.
//!
//! This example uses the [`InMemorySequenceClient`] — no external services
//! required. A LiteBox-transport variant lives outside this OSS repository
//! (the LiteBox client is Databricks-internal); see
//! `cic-duckdb-demo/test/delta-kernel-rs/litebox-sequence-client/` for that
//! version.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example identity-column-demo -p delta-kernel-unity-catalog --all-features
//! ```

use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::identity_columns::{
    detect_identity_columns, identity_column_cic, IdentityColumnFiller, IdentityReservation,
    SequenceReserver,
};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Engine as KernelEngine;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::storage::store_from_url;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel_unity_catalog::{
    create_identity_sequences, IdentityColumnSpec, UCSequenceReserver,
};
use unity_catalog_delta_client_api::{InMemorySequenceClient, SequenceClient};

type DemoEngine = DefaultEngine<TokioMultiThreadExecutor>;
type DynError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DynError> {
    let table_path = env::temp_dir()
        .join(format!(
            "cic-demo-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
        ))
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&table_path)?;
    let table_url = url::Url::from_directory_path(&table_path)
        .map_err(|_| "failed to build file:// URL for temp dir")?;

    let store = store_from_url(&table_url)?;
    let engine = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );

    println!("=== CIC walkthrough ===");
    println!("Table path: {table_path}");
    println!("Backend:    in-memory mock (InMemorySequenceClient)");

    let client = Arc::new(InMemorySequenceClient::new());
    run_flow(client, "tbl-cic-demo", &table_path, table_url, engine).await
}

async fn run_flow<C>(
    client: Arc<C>,
    table_id: &str,
    table_path: &str,
    table_url: url::Url,
    engine: Arc<DemoEngine>,
) -> Result<(), DynError>
where
    C: SequenceClient + 'static,
{
    let specs = [
        IdentityColumnSpec::new("id", 1, 1),
        IdentityColumnSpec::new("row_id", 1000, 10),
    ];

    println!(
        "\n[1/6] Allocating {} sequences (table_id={table_id})",
        specs.len()
    );
    let infos = create_identity_sequences(client.as_ref(), table_id, &specs).await?;
    for info in &infos {
        println!(
            "    minted sequence: column={} sequence_id={} start={} step={}",
            info.column_name, info.sequence_id, info.start, info.step
        );
    }

    println!("\n[2/6] Building schema with identity_column_cic and calling create_table");
    let schema = Arc::new(StructType::try_new(vec![
        identity_column_cic(
            &infos[0].column_name,
            &infos[0].sequence_id,
            infos[0].start,
            infos[0].step,
        ),
        StructField::new("payload", DataType::STRING, true),
        identity_column_cic(
            &infos[1].column_name,
            &infos[1].sequence_id,
            infos[1].start,
            infos[1].step,
        ),
    ])?);

    let _commit = create_table(table_path, schema, "cic-demo/0.1")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;
    println!("    committed version 0");

    let log_path = format!("{table_path}/_delta_log/00000000000000000000.json");
    println!("\n[3/6] Delta log on disk:");
    println!("    {log_path}");
    println!("    Look for:");
    println!("      * 'protocol' action -> writerFeatures should include 'identityColumnsCic'");
    println!("      * 'metaData' action -> schemaString contains delta.identity.v2.* keys");

    println!("\n[4/6] Reloading snapshot");
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let table_config = snapshot.table_configuration();
    println!(
        "    identityColumnsCic in protocol: {}",
        table_config.is_feature_supported(&TableFeature::IdentityColumnsCic)
    );
    let read_schema = snapshot.schema();
    let detected = detect_identity_columns(&read_schema)?;
    println!(
        "    detect_identity_columns found {} CIC column(s):",
        detected.len()
    );
    for info in &detected {
        println!(
            "      - {} sequence_id={} start={} step={}",
            info.column_name, info.sequence_id, info.start, info.step
        );
    }

    println!("\n[5/6] Writing a Parquet file and committing v1");
    let reserver = UCSequenceReserver::new(client.clone(), table_id);

    const BATCH_ROWS: u64 = 3;
    let reservations: Vec<IdentityReservation> = detected
        .iter()
        .map(|info| {
            let range = reserver.reserve_ids(&info.sequence_id, info.step, BATCH_ROWS)?;
            Ok::<_, DynError>(IdentityReservation {
                column_name: info.column_name.clone(),
                range_start: range.range_start,
                range_end: range.range_end,
                step: info.step,
            })
        })
        .collect::<Result<_, _>>()?;
    for r in &reservations {
        println!(
            "    reserved for '{}': [{}, {}] step={}",
            r.column_name, r.range_start, r.range_end, r.step
        );
    }

    // Engine-side batch: only the non-identity columns. Kernel fills the rest.
    let payload: ArrayRef = Arc::new(StringArray::from(vec![
        "hello".to_string(),
        "world".to_string(),
        "!".to_string(),
    ]));
    let input_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "payload",
            ArrowDataType::Utf8,
            true,
        )])),
        vec![payload],
    )?;

    let mut filler = IdentityColumnFiller::new(reservations)?;
    let filled = filler.fill_arrow_batch(&input_batch, &read_schema)?;
    println!(
        "    filled batch ({} rows x {} columns):",
        filled.num_rows(),
        filled.num_columns()
    );
    println!(
        "{}",
        delta_kernel::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(&filled))?
    );

    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("cic-demo/0.1")
        .with_operation("WRITE".to_string())
        .with_data_change(true);
    let write_context = txn.write_state()?.write_context_builder().build()?;

    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(filled), &write_context)
        .await?;
    txn.add_files(add_files_metadata);

    match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(c) => {
            println!("    committed version {}", c.commit_version());
        }
        CommitResult::ConflictedTransaction(_) => {
            return Err("unexpected commit conflict at v1".into());
        }
        CommitResult::RetryableTransaction(_) => {
            return Err("unexpected retryable failure at v1".into());
        }
    }

    println!("\n[6/6] Reading the table back");
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    println!("    snapshot version: {}", snapshot_v1.version());
    let scan = snapshot_v1.scan_builder().build()?;
    let engine_dyn: Arc<dyn KernelEngine> = engine.clone();
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine_dyn)? {
        let engine_data = batch_result?;
        let record_batch = ArrowEngineData::try_from_engine_data(engine_data)?
            .record_batch()
            .clone();
        batches.push(record_batch);
    }
    println!(
        "{}",
        delta_kernel::arrow::util::pretty::pretty_format_batches(&batches)?
    );

    println!("\n=== Done ===");
    println!("Clean up with: rm -rf {table_path}");
    Ok(())
}
