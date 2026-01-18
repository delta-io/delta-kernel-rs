use std::collections::HashMap;
use std::fs::read_to_string;
use std::fs::{create_dir_all, write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::util::pretty::print_batches;
use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use itertools::Itertools;
use serde_json::{json, to_vec};
use url::Url;
use uuid::Uuid;

use delta_kernel::arrow::array::TimestampMicrosecondArray;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::{CommitResult, RetryableTransaction};
use delta_kernel::{DeltaResult, Engine, Error, Snapshot, SnapshotRef};

/// An example program that writes to a Delta table and creates it if necessary.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,

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
    // TODO: Support providing input data from a JSON file instead of generating random data
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

// TODO: Update the example once official write APIs are introduced (issue#1123)
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
    let sample_data = create_sample_data(&snapshot.schema(), cli.num_rows)?;

    // Write sample data to the table
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot
        .transaction(committer)?
        .with_operation("INSERT".to_string())
        .with_engine_info("default_engine/write-table-example")
        .with_data_change(true);

    // DEMONSTRATION: Query which statistics to collect
    // The transaction provides two ways to determine which columns need statistics:

    // 1. Get the full stats schema (useful for schema-aware engines)
    let stats_schema = txn.stats_schema();
    println!("\n=== Statistics Schema ===");
    println!("The stats schema shows the full structure for statistics:");
    println!("{:#?}", stats_schema);

    // 2. Get a flat list of column names (simpler API)
    let stats_columns = txn.stats_columns();
    println!("\n=== Statistics Columns ===");
    println!(
        "Collect statistics for these {} columns:",
        stats_columns.len()
    );
    for col in &stats_columns {
        println!("  - {}", col);
    }

    // 3. Get the full add_files_schema that engines must conform to
    let add_files_schema = txn.add_files_schema();
    println!("\n=== Add Files Schema ===");
    println!("Engines must return data matching this schema:");
    println!("{:#?}", add_files_schema);

    /*
    HOW AN ENGINE WOULD COLLECT STATISTICS:

    When writing a Parquet file, the engine would:

    1. Query txn.stats_columns() or txn.stats_schema() to know which columns to track
    2. While writing data, track for each column:
       - numRecords: total row count
       - nullCount: count of null values
       - minValues: minimum value seen
       - maxValues: maximum value seen
    3. After writing, construct a stats struct matching the schema:

    ```rust
    use arrow::array::{Int64Array, StructArray};

    // Example: For a table with columns "id" and "name"
    let stats = StructArray::try_new_with_length(
        vec![
            Field::new("numRecords", DataType::Int64, true),
            Field::new("nullCount", DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Int64, true),
            ])), true),
            Field::new("minValues", DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ])), true),
            Field::new("maxValues", DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ])), true),
            Field::new("tightBounds", DataType::Boolean, true),
        ].into(),
        vec![
            Arc::new(Int64Array::from(vec![1000])), // numRecords
            Arc::new(StructArray::try_new(...)?),    // nullCount: {id: 0, name: 5}
            Arc::new(StructArray::try_new(...)?),    // minValues: {id: 1, name: "Alice"}
            Arc::new(StructArray::try_new(...)?),    // maxValues: {id: 1000, name: "Zoe"}
            Arc::new(BooleanArray::from(vec![true])), // tightBounds
        ],
        None,
        1,
    )?;
    ```

    The engine returns this stats struct as part of the file metadata, and Kernel
    automatically converts it to JSON when committing the transaction.
    */

    // Write the data using the engine (which internally collects statistics)
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
    println!("\n✓ Committed transaction at version {version}");
    println!("✓ Successfully wrote {} rows to the table", cli.num_rows);

    // Display the commit JSON to inspect statistics
    println!("\n=== Inspecting Commit JSON ===");
    display_commit_json(&url, version)?;

    // Read and display the data
    read_and_display_data(&url, engine).await?;
    println!("\n✓ Successfully read data from the table");

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
            create_table(url, &schema).await?;
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

/// Create a new Delta table with the given schema.
///
/// Creating a Delta table is not officially supported by kernel-rs yet, so we manually create the
/// initial transaction log.
async fn create_table(table_url: &Url, schema: &SchemaRef) -> DeltaResult<()> {
    let table_id = Uuid::new_v4().to_string();
    let schema_str = serde_json::to_string(&schema)?;

    let (reader_features, writer_features) = {
        let reader_features: Vec<&'static str> = vec![];
        let writer_features: Vec<&'static str> = vec![];

        // TODO: Support adding specific table features
        (reader_features, writer_features)
    };

    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": reader_features,
            "writerFeatures": writer_features,
        }
    });
    let partition_columns: Vec<String> = vec![];
    let metadata = json!({
        "metaData": {
            "id": table_id,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": schema_str,
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": 1677811175819u64
        }
    });

    let data = [
        to_vec(&protocol).unwrap(),
        b"\n".to_vec(),
        to_vec(&metadata).unwrap(),
    ]
    .concat();

    // Write the initial transaction with protocol and metadata to 0.json
    let delta_log_path = table_url
        .join("_delta_log/")?
        .to_file_path()
        .map_err(|_e| Error::generic("URL cannot be converted to local file path"))?;
    let file_path = delta_log_path.join("00000000000000000000.json");

    // Create the _delta_log directory if it doesn't exist
    create_dir_all(&delta_log_path)
        .map_err(|e| Error::generic(format!("Failed to create _delta_log directory: {e}")))?;

    // Write the file using standard filesystem operations
    write(&file_path, data)
        .map_err(|e| Error::generic(format!("Failed to write initial transaction log: {e}")))?;

    println!("✓ Created Delta table with schema: {schema:#?}");
    Ok(())
}

/// Create sample data based on the schema.
fn create_sample_data(schema: &SchemaRef, num_rows: usize) -> DeltaResult<ArrowEngineData> {
    let fields = schema.fields();
    let mut columns = Vec::new();

    for field in fields {
        let column: Arc<dyn arrow::array::Array> = match *field.data_type() {
            DataType::STRING => {
                let data: Vec<String> = (0..num_rows).map(|i| format!("item_{i}")).collect();
                Arc::new(StringArray::from(data))
            }
            DataType::INTEGER => {
                let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
                Arc::new(Int32Array::from(data))
            }
            DataType::LONG => {
                let data: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
                Arc::new(arrow::array::Int64Array::from(data))
            }
            DataType::DOUBLE => {
                let data: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();
                Arc::new(Float64Array::from(data))
            }
            DataType::BOOLEAN => {
                let data: Vec<bool> = (0..num_rows).map(|i| i % 2 == 0).collect();
                Arc::new(BooleanArray::from(data))
            }
            DataType::TIMESTAMP => {
                let now = chrono::Utc::now();
                let data: Vec<i64> = (0..num_rows)
                    .map(|i| (now + chrono::Duration::seconds(i as i64)).timestamp_micros())
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(data))
            }
            _ => {
                return Err(Error::generic(format!(
                    "Unsupported data type for sample data: {:?}",
                    field.data_type()
                )));
            }
        };
        columns.push(column);
    }

    let arrow_schema = schema.as_ref().try_into_arrow()?;
    let record_batch = RecordBatch::try_new(Arc::new(arrow_schema), columns)?;

    Ok(ArrowEngineData::new(record_batch))
}

/// Display the commit JSON to inspect statistics.
fn display_commit_json(table_url: &Url, version: u64) -> DeltaResult<()> {
    // Construct path to the commit JSON file
    let commit_file = format!("{:020}.json", version);
    let delta_log_path = table_url
        .join("_delta_log/")?
        .join(&commit_file)?
        .to_file_path()
        .map_err(|_e| Error::generic("URL cannot be converted to local file path"))?;

    println!("Reading commit file: {}", delta_log_path.display());

    // Read and parse the commit JSON
    let content = read_to_string(&delta_log_path)
        .map_err(|e| Error::generic(format!("Failed to read commit file: {e}")))?;

    println!("\nCommit JSON content:");
    println!("{}", "=".repeat(80));

    // Pretty print each JSON line
    for (idx, line) in content.lines().enumerate() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            // Check if this is an Add action with stats
            if let Some(add) = json.get("add") {
                println!("\nAdd Action #{}:", idx + 1);
                println!("{}", serde_json::to_string_pretty(&add)?);

                // Highlight the stats field
                if let Some(stats_str) = add.get("stats").and_then(|s| s.as_str()) {
                    println!("\n📊 Statistics (parsed):");
                    if let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(stats_str) {
                        println!("{}", serde_json::to_string_pretty(&stats_json)?);

                        // Extract and display key statistics
                        if let Some(num_records) = stats_json.get("numRecords") {
                            println!("\n  • Total records: {}", num_records);
                        }
                        if let Some(null_count) = stats_json.get("nullCount") {
                            println!("  • Null counts: {}", null_count);
                        }
                        if let Some(min_values) = stats_json.get("minValues") {
                            println!("  • Min values: {}", min_values);
                        }
                        if let Some(max_values) = stats_json.get("maxValues") {
                            println!("  • Max values: {}", max_values);
                        }
                        if let Some(tight_bounds) = stats_json.get("tightBounds") {
                            println!("  • Tight bounds: {}", tight_bounds);
                        }
                    }
                }
            }
        }
    }

    println!("{}", "=".repeat(80));
    Ok(())
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
