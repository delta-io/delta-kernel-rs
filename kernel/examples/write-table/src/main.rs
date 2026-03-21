use std::collections::HashMap;
use std::fs::File;
use std::fs::create_dir_all;
use std::io::BufReader;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use delta_kernel::arrow::util::pretty::print_batches;
use itertools::Itertools;
use url::Url;
use serde_json::Value;

use delta_kernel::arrow::array::TimestampMicrosecondArray;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table as create_delta_table;
use delta_kernel::transaction::{CommitResult, RetryableTransaction};
use delta_kernel::{DeltaResult, Engine, Error, Snapshot, SnapshotRef};

/// An example program that writes to a Delta table and creates it if necessary.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,

    /// Path to a JSON file
    #[arg(long, short = 'i')]
    data: Option<String>,

    /// Comma-separated schema specification of the form `field_name:data_type`
    #[arg(
        long,
        short = 's',
        default_value = "id:integer,name:string,score:double"
    )]
    schema: String,

    /// Number of rows to generate for the example data
    #[arg(long, short, default_value = "10")]
    num_rows: usize,

    // TODO: Support specifying whether the transaction should overwrite, append, or error if the table already exists
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
    let cli = Cli::parse_with_examples(env!("CARGO_PKG_NAME"), "Write", "write", "");

    // Check if path is a directory and if not, create it
    if !Path::new(&cli.location_args.path).exists() {
        create_dir_all(&cli.location_args.path).map_err(|e| {
            Error::generic(format!(
                "Failed to create directory {}: {e}",
                &cli.location_args.path
            ))
        })?;
    }

    let url = delta_kernel::try_parse_uri(&cli.location_args.path)?;
    println!("Using Delta table at: {url}");

    // Get the engine for local filesystem
    use delta_kernel::engine::default::storage::store_from_url;
    let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();

    // Create or get the table
    let snapshot = create_or_get_base_snapshot(&url, &engine, &cli.schema).await?;

    // Create sample data based on the schema
    let sample_data = if let Some(input_path) = &cli.data {
        read_json_data(input_path, &snapshot.schema())?
    } else {
        create_sample_data(&snapshot.schema(), cli.num_rows)?
    };

    // Write sample data to the table
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot
        .transaction(committer, &engine)?
        .with_operation("INSERT".to_string())
        .with_engine_info("default_engine/write-table-example")
        .with_data_change(true);

    // Write the data using the engine
    let write_context = Arc::new(txn.get_write_context());
    let file_metadata = engine
        .write_parquet(&sample_data, write_context.as_ref(), HashMap::new())
        .await?;

    // Add the file metadata to the transaction
    txn.add_files(file_metadata);

    // Commit the transaction (in a simple retry loop)
    let mut retries = 0;
    let committed = loop {
        if retries > 5 {
            return Err(Error::generic(
                "Exceeded maximum 5 retries for committing transaction",
            ));
        }
        txn = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(committed) => break committed,
            CommitResult::ConflictedTransaction(conflicted) => {
                let conflicting_version = conflicted.conflict_version();
                println!("✗ Failed to write data, transaction conflicted with version: {conflicting_version}");
                return Err(Error::generic("Commit failed"));
            }
            CommitResult::RetryableTransaction(RetryableTransaction { transaction, error }) => {
                println!("✗ Failed to commit, retrying... retryable error: {error}");
                transaction
            }
        };
        retries += 1;
    };

    let version = committed.commit_version();
    println!("✓ Committed transaction at version {version}");
    println!("✓ Successfully wrote {} rows to the table", cli.num_rows);

    // Read and display the data
    read_and_display_data(&url, engine).await?;
    println!("✓ Successfully read data from the table");

    Ok(())
}

/// Creates a new Delta table or gets an existing one.
async fn create_or_get_base_snapshot(
    url: &Url,
    engine: &dyn Engine,
    schema_str: &str,
) -> DeltaResult<SnapshotRef> {
    // Check if table already exists
    match Snapshot::builder_for(url.clone()).build(engine) {
        Ok(snapshot) => {
            println!("✓ Found existing table at version {}", snapshot.version());
            Ok(snapshot)
        }
        Err(_) => {
            // Create new table
            println!("Creating new Delta table...");
            let schema = parse_schema(schema_str)?;
            create_table(url, &schema, engine).await?;
            Snapshot::builder_for(url.clone()).build(engine)
        }
    }
}

/// Parse a schema string into a SchemaRef.
fn parse_schema(schema_str: &str) -> DeltaResult<SchemaRef> {
    let fields = schema_str
        .split(',')
        .map(|field| {
            let parts: Vec<&str> = field.split(':').collect();
            if parts.len() != 2 {
                return Err(Error::generic(format!(
                    "Invalid field specification: {field}. Expected format: field_name:data_type"
                )));
            }

            let (name, data_type) = (parts[0].trim(), parts[1].trim());
            let data_type = match data_type {
                "string" => DataType::STRING,
                "integer" => DataType::INTEGER,
                "long" => DataType::LONG,
                "double" => DataType::DOUBLE,
                "boolean" => DataType::BOOLEAN,
                "timestamp" => DataType::TIMESTAMP,
                _ => {
                    return Err(Error::generic(format!(
                        "Unsupported data type: {data_type}"
                    )));
                }
            };

            Ok(StructField::nullable(name, data_type))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(Arc::new(StructType::try_new(fields)?))
}

/// Create a new Delta table with the given schema using the official CreateTable API.
async fn create_table(table_url: &Url, schema: &SchemaRef, engine: &dyn Engine) -> DeltaResult<()> {
    // Use the create_table API to create the table
    let table_path = table_url.as_str();
    let _result = create_delta_table(table_path, schema.clone(), "write-table-example/1.0")
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;

    println!("✓ Created Delta table with schema: {schema:#?}");
    Ok(())
}

/// A factory that builds Arrow tables by asking a provided 'helper' for each cell's value.
fn build_data<F>(schema: &SchemaRef, num_rows: usize, mut provider: F) -> DeltaResult<ArrowEngineData>
where
    F: FnMut(&StructField, usize) -> Option<Value>,
{
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let column: ArrayRef = match *field.data_type() {
            DataType::STRING => {
                let data: Vec<Option<String>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_str().map(|s| s.to_string())))
                    .collect();
                Arc::new(StringArray::from(data))
            }
            DataType::INTEGER => {
                let data: Vec<Option<i32>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_i64().map(|n| n as i32)))
                    .collect();
                Arc::new(Int32Array::from(data))
            }
            DataType::LONG => {
                let data: Vec<Option<i64>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_i64()))
                    .collect();
                Arc::new(Int64Array::from(data))
            }
            DataType::DOUBLE => {
                let data: Vec<Option<f64>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_f64()))
                    .collect();
                Arc::new(Float64Array::from(data))
            }
            DataType::BOOLEAN => {
                let data: Vec<Option<bool>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_bool()))
                    .collect();
                Arc::new(BooleanArray::from(data))
            }
            DataType::TIMESTAMP => {
                let data: Vec<Option<i64>> = (0..num_rows)
                    .map(|i| provider(field, i).and_then(|v| v.as_i64()))
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(data))
            }
            _ => return Err(Error::generic(format!("Unsupported type: {:?}", field.data_type()))),
        };
        columns.push(column);
    }

    let arrow_schema = schema.as_ref().try_into_arrow()?;
    Ok(ArrowEngineData::new(RecordBatch::try_new(Arc::new(arrow_schema), columns)?))
}

/// Create sample data based on the schema.
fn create_sample_data(schema: &SchemaRef, num_rows: usize) -> DeltaResult<ArrowEngineData> {
    let now = chrono::Utc::now();

    build_data(schema, num_rows, |field, i| {
        match *field.data_type() {
            DataType::STRING => Some(Value::from(format!("{}_{}", field.name(), i))),
            DataType::INTEGER | DataType::LONG => Some(Value::from(i as i64)),
            DataType::DOUBLE => Some(Value::from(i as f64 * 1.5)),
            DataType::BOOLEAN => Some(Value::from(i % 2 == 0)),
            DataType::TIMESTAMP => Some(Value::from((now + chrono::Duration::seconds(i as i64)).timestamp_micros())),
            _ => None,
        }
    })
}

// Reads a JSON file into Arrow memory.
fn read_json_data(path: &str, schema: &SchemaRef) -> DeltaResult<ArrowEngineData> {
    let file = File::open(path).map_err(|e| Error::generic(format!("File error: {e}")))?;
    let json: Value = serde_json::from_reader(BufReader::new(file))
        .map_err(|e| Error::generic(format!("JSON error: {e}")))?;

    let rows = json.as_array().ok_or_else(|| Error::generic("JSON must be an array"))?;

    build_data(schema, rows.len(), |field, i| {
        rows.get(i).and_then(|row| row.get(field.name()).cloned())
    })
}

/// Read and display data from the table.
async fn read_and_display_data(
    table_url: &Url,
    engine: DefaultEngine<TokioBackgroundExecutor>,
) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;

    let batches: Vec<RecordBatch> = scan
        .execute(Arc::new(engine))?
        .map(EngineDataArrowExt::try_into_record_batch)
        .try_collect()?;

    print_batches(&batches)?;
    Ok(())
}