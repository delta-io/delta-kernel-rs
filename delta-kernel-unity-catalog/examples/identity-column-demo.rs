//! End-to-end walkthrough of Concurrent Identity Columns (CIC).
//!
//! Drives the full flow -- mint sequence ids, register them in UC, stamp them
//! into a Delta schema, create the table, reserve ranges and fill the identity
//! columns into a data batch, write a Parquet file, commit v1, and read the
//! table back -- printing every step so you can verify what kernel does on disk.
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

use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::identity_columns::{cic_column, detect_identity_columns, IdentityColumnInfo};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Engine as KernelEngine;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::storage::store_from_url;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use unity_catalog_delta_client_api::{
    CreateIdentitySequences, IdentityReservation, IdentitySequenceSpec, InMemorySequenceClient,
    ReserveIdentityRanges, SequenceClient,
};
use uuid::Uuid;

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
    // Mint a sequence_id per identity column. Registration in UC happens only after the
    // CREATE-table commit below, so a failed commit never orphans a sequence.
    let mint = |name: &str, start, step| IdentityColumnInfo {
        column_name: name.to_string(),
        sequence_id: Uuid::new_v4().to_string(),
        start,
        step,
        allow_explicit_insert: false,
    };
    let infos = [mint("id", 1, 1), mint("row_id", 1000, 10)];

    println!(
        "\n[1/6] Minting {} sequence ids (table_id={table_id})",
        infos.len()
    );
    for info in &infos {
        println!(
            "    minted sequence: column={} sequence_id={} start={} step={}",
            info.column_name, info.sequence_id, info.start, info.step
        );
    }

    println!(
        "\n[2/6] Building schema with cic_column, committing create_table, then \
         registering sequences in UC"
    );
    let schema = Arc::new(StructType::try_new(vec![
        cic_column(
            &infos[0].column_name,
            &infos[0].sequence_id,
            infos[0].start,
            infos[0].step,
        ),
        StructField::new("payload", DataType::STRING, true),
        cic_column(
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

    // Register the minted sequences with the service *after* the commit, so a failed create never
    // orphans a sequence. The connector builds the batch from the columns it minted above.
    client
        .create_identity_sequences(CreateIdentitySequences {
            table_id: table_id.to_string(),
            sequences: infos
                .iter()
                .map(|c| IdentitySequenceSpec {
                    sequence_id: c.sequence_id.clone(),
                    start: c.start,
                    step: c.step,
                })
                .collect(),
        })
        .await?;
    println!("    registered sequences in UC (after commit)");

    let log_path = format!("{table_path}/_delta_log/00000000000000000000.json");
    println!("\n[3/6] Delta log on disk:");
    println!("    {log_path}");
    println!("    Look for:");
    println!(
        "      * 'protocol' action -> writerFeatures should include 'concurrentIdentityColumns'"
    );
    println!("      * 'metaData' action -> schemaString contains delta.identity.concurrent.sequenceId + delta.identity.start/step");

    println!("\n[4/6] Reloading snapshot");
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let table_config = snapshot.table_configuration();
    println!(
        "    concurrentIdentityColumns in protocol: {}",
        table_config.is_feature_supported(&TableFeature::ConcurrentIdentityColumns)
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

    // Build the write transaction, then ask kernel which columns the connector must fill.
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("cic-demo/0.1")
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    const BATCH_ROWS: i64 = 3;
    let payload = ["hello", "world", "!"];

    // Connector-owned: reserve BATCH_ROWS from every CIC in one batched RPC, then generate the
    // values by enumerating each reserved range (`range_start + step * i`), keyed by logical column
    // name. Kernel neither reserves, generates, nor inserts values.
    let cics = txn.concurrent_identity_columns()?;
    let response = client
        .reserve_identity_ranges(ReserveIdentityRanges {
            table_id: table_id.to_string(),
            reservations: cics
                .iter()
                .map(|c| IdentityReservation {
                    sequence_id: c.sequence_id().to_string(),
                    count: BATCH_ROWS,
                    step: Some(c.step()),
                })
                .collect(),
        })
        .await?;
    let ranges: std::collections::HashMap<&str, _> = response
        .ranges
        .iter()
        .map(|r| (r.sequence_id.as_str(), r))
        .collect();
    let mut generated: std::collections::HashMap<String, Vec<i64>> =
        std::collections::HashMap::new();
    for c in &cics {
        let r = ranges[c.sequence_id()];
        let values = (0..BATCH_ROWS)
            .map(|i| r.range_start + r.step * i)
            .collect();
        generated.insert(c.column_name().to_string(), values);
    }
    drop(cics); // release the borrow of `txn` before mutating it

    // The connector assembles the full batch itself (kernel does not insert values). Columns must
    // be in the table's schema order: id, payload, row_id.
    let filled = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("payload", ArrowDataType::Utf8, true),
            ArrowField::new("row_id", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(generated["id"].clone())) as ArrayRef,
            Arc::new(StringArray::from(payload.to_vec())),
            Arc::new(Int64Array::from(generated["row_id"].clone())),
        ],
    )?;
    println!(
        "    filled batch ({} rows x {} columns):",
        filled.num_rows(),
        filled.num_columns()
    );
    println!(
        "{}",
        delta_kernel::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(&filled))?
    );

    // Acknowledge that the connector filled the identity columns, then write through the normal
    // path.
    txn.ack_concurrent_identity_columns();
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
