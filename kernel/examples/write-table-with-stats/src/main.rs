//! Example: Write a partitioned Delta table with parsed stats (stats_parsed) and
//! parsed partition values (partitionValues_parsed) enabled.
//!
//! This example demonstrates the full workflow:
//! 1. Create a new partitioned Delta table with `writeStatsAsStruct=true`
//! 2. Write data to the table (stats are automatically captured)
//! 3. Create a checkpoint (transforms JSON stats to stats_parsed and partitionValues to partitionValues_parsed)
//! 4. Read the table to verify stats-based data skipping works
//!
//! Run with: cargo run -p write-table-with-stats -- /tmp/my-stats-table

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use url::Url;

use delta_kernel::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use delta_kernel::checkpoint::CheckpointDataIterator;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::parquet::arrow::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Error, FileMeta, Snapshot};

use serde_json::json;

/// An example program that creates a Delta table with parsed stats enabled,
/// writes data, creates a checkpoint with stats_parsed, and verifies data skipping.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();
    match try_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("{e:#?}");
            ExitCode::FAILURE
        }
    }
}

async fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse_with_examples(env!("CARGO_PKG_NAME"), "WriteStats", "write-stats", "");

    // Delete and recreate directory if it exists
    let path = Path::new(&cli.location_args.path);
    if path.exists() {
        std::fs::remove_dir_all(path).map_err(|e| {
            Error::generic(format!(
                "Failed to remove existing directory {}: {e}",
                path.display()
            ))
        })?;
    }
    create_dir_all(path).map_err(|e| {
        Error::generic(format!(
            "Failed to create directory {}: {e}",
            path.display()
        ))
    })?;

    let table_url = delta_kernel::try_parse_uri(&cli.location_args.path)?;
    let table_path = Path::new(&cli.location_args.path).to_path_buf();

    println!("=== Delta Kernel: Partitioned Table with Parsed Stats ===\n");
    println!("Table location: {}\n", table_url);

    // Create the engine
    use delta_kernel::engine::default::storage::store_from_url;
    let store = store_from_url(&table_url)?;
    let engine = DefaultEngineBuilder::new(store).build();

    // Define the table schema (with partition column 'year')
    let schema: SchemaRef = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("year", DataType::INTEGER), // partition column
    ]));

    // Step 1: Create the table with writeStatsAsStruct=true
    println!("Step 1: Creating table with stats configuration...");
    create_table_with_stats_config(&table_path, &table_url, &schema)?;

    // Step 2: Write data in multiple commits to create multiple files (different partitions)
    println!("\nStep 2: Writing data to table (partitioned by year)...");
    write_data_to_table(&engine, &table_url, &schema, 1..=25, 2023).await?;
    write_data_to_table(&engine, &table_url, &schema, 26..=50, 2024).await?;
    write_data_to_table(&engine, &table_url, &schema, 51..=100, 2025).await?;

    // Step 3: Create a checkpoint
    println!("\nStep 3: Creating checkpoint with stats_parsed...");
    create_checkpoint(&table_path, &engine, &table_url)?;

    // Step 4: Verify stats work for data skipping
    println!("\nStep 4: Verifying stats-based data skipping...");
    verify_stats_work(&engine, &table_url)?;

    println!("\n=== Example completed successfully! ===");
    println!("\nInspect the table at: {}", cli.location_args.path);

    Ok(())
}

/// Create a table with writeStatsAsStruct enabled
fn create_table_with_stats_config(
    table_path: &Path,
    table_url: &Url,
    schema: &SchemaRef,
) -> DeltaResult<()> {
    let schema_json = serde_json::to_string(schema)?;

    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["v2Checkpoint"],
            "writerFeatures": ["v2Checkpoint"]
        }
    });

    let metadata = json!({
        "metaData": {
            "id": "example-table-with-stats",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_json,
            "partitionColumns": ["year"],
            "configuration": {
                "delta.checkpoint.writeStatsAsStruct": "true",
                "delta.checkpoint.writeStatsAsJson": "false"
            },
            "createdTime": chrono::Utc::now().timestamp_millis()
        }
    });

    let content = format!(
        "{}\n{}",
        serde_json::to_string(&protocol)?,
        serde_json::to_string(&metadata)?
    );

    let delta_log_path = table_path.join("_delta_log");
    create_dir_all(&delta_log_path)
        .map_err(|e| Error::generic(format!("Failed to create _delta_log directory: {e}")))?;

    let commit_path = delta_log_path.join("00000000000000000000.json");
    std::fs::write(&commit_path, content)
        .map_err(|e| Error::generic(format!("Failed to write initial commit: {e}")))?;

    println!(
        "Created table with writeStatsAsStruct=true at {}",
        table_url
    );
    Ok(())
}

/// Write data to the table using kernel's transaction API
async fn write_data_to_table(
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    table_url: &Url,
    schema: &SchemaRef,
    id_range: std::ops::RangeInclusive<i64>,
    year: i32,
) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot
        .transaction(committer)?
        .with_engine_info("write-table-with-stats-example")
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let write_context = Arc::new(txn.get_write_context());
    let arrow_schema: Arc<delta_kernel::arrow::datatypes::Schema> =
        Arc::new(schema.as_ref().try_into_arrow()?);

    // Create data with partition column
    let num_rows = id_range.clone().count();
    let ids: Vec<i64> = id_range.clone().collect();
    let names: Vec<String> = id_range.map(|i| format!("name_{}", i)).collect();
    let years: Vec<i32> = vec![year; num_rows];
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(years)),
        ],
    )?;

    // Pass partition values when writing
    let mut partition_values = HashMap::new();
    partition_values.insert("year".to_string(), year.to_string());

    let engine_data = Box::new(ArrowEngineData::new(batch));
    let file_metadata = engine
        .write_parquet(&engine_data, write_context.as_ref(), partition_values)
        .await?;
    txn.add_files(file_metadata);

    match txn.commit(engine)? {
        CommitResult::CommittedTransaction(committed) => {
            println!(
                "Committed version {} (year={})",
                committed.commit_version(),
                year
            );
            Ok(())
        }
        _ => Err(Error::generic("Commit failed")),
    }
}

/// Create a checkpoint and write it to storage
fn create_checkpoint(
    table_path: &Path,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    table_url: &Url,
) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let version = snapshot.version();
    println!("Creating checkpoint at version {}...", version);

    let writer = snapshot.create_checkpoint_writer()?;
    let checkpoint_path = writer.checkpoint_path()?;
    let mut data_iter = writer.checkpoint_data(engine)?;

    // Check that output schema includes stats_parsed and partitionValues_parsed
    let output_schema = data_iter.output_schema();
    if let Some(add_field) = output_schema.field("add") {
        if let DataType::Struct(add_struct) = add_field.data_type() {
            println!(
                "Checkpoint Add schema fields: stats={}, stats_parsed={}, partitionValues={}, partitionValues_parsed={}",
                add_struct.field("stats").is_some(),
                add_struct.field("stats_parsed").is_some(),
                add_struct.field("partitionValues").is_some(),
                add_struct.field("partitionValues_parsed").is_some()
            );
            // Print partitionValues_parsed schema if present
            if let Some(pv_parsed) = add_struct.field("partitionValues_parsed") {
                if let DataType::Struct(pv_struct) = pv_parsed.data_type() {
                    let fields: Vec<_> = pv_struct
                        .fields()
                        .map(|f| format!("{}: {:?}", f.name, f.data_type))
                        .collect();
                    println!("  partitionValues_parsed schema: {{{}}}", fields.join(", "));
                }
            }
        }
    }

    // Get first batch to determine schema
    let first = data_iter
        .next()
        .ok_or_else(|| Error::generic("No batches in checkpoint data"))??;
    let first_batch = first.apply_selection_vector()?.try_into_record_batch()?;

    // Write checkpoint to parquet
    let mut buffer: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    let mut parquet_writer = ArrowWriter::try_new(&mut buffer, first_batch.schema(), Some(props))?;
    parquet_writer.write(&first_batch)?;

    for data_res in data_iter.by_ref() {
        let batch = data_res?
            .apply_selection_vector()?
            .try_into_record_batch()?;
        parquet_writer.write(&batch)?;
    }
    parquet_writer.close()?;

    // Write checkpoint file
    let checkpoint_filename = checkpoint_path
        .path_segments()
        .and_then(|s| s.last())
        .unwrap_or("checkpoint.parquet");
    let checkpoint_file_path = table_path.join("_delta_log").join(checkpoint_filename);
    let size = buffer.len() as u64;
    std::fs::write(&checkpoint_file_path, &buffer)
        .map_err(|e| Error::generic(format!("Failed to write checkpoint file: {e}")))?;

    println!("Wrote checkpoint: {} ({} bytes)", checkpoint_path, size);

    // Finalize checkpoint
    let file_meta = FileMeta {
        location: checkpoint_path,
        last_modified: chrono::Utc::now().timestamp_millis(),
        size,
    };
    writer.finalize(engine, &file_meta, data_iter)?;
    println!("Checkpoint finalized");

    Ok(())
}

/// Read back the table and verify stats work for data skipping
fn verify_stats_work(
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    table_url: &Url,
) -> DeltaResult<()> {
    use delta_kernel::expressions::{column_expr, Expression, Predicate};

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    println!(
        "Verifying data skipping at version {}...",
        snapshot.version()
    );

    // Scan with a predicate that should skip some files (ids 1-50 should be skipped)
    let predicate = Arc::new(Predicate::gt(
        column_expr!("id"),
        Expression::literal(50i64),
    ));

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let scan_metadata: Vec<_> = scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>()?;

    let mut total_files = 0;
    let mut selected_files = 0;

    for metadata in &scan_metadata {
        let sv = metadata.scan_files.selection_vector();
        let data_len = metadata.scan_files.data().len();
        total_files += data_len;

        // Count selected files (true in selection vector or not in vector means selected)
        for i in 0..data_len {
            if sv.get(i).copied().unwrap_or(true) {
                selected_files += 1;
            }
        }
    }

    println!(
        "Data skipping: {} of {} files selected with predicate 'id > 50'",
        selected_files, total_files
    );

    if selected_files < total_files {
        println!("Stats-based data skipping is working!");
    }

    Ok(())
}
