use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::util::pretty::print_batches;
use clap::Args;
use common::LocationArgs;

use delta_kernel::arrow::array::TimestampMicrosecondArray;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::{path::Path, ObjectStore};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{DeltaResult, Engine, Snapshot};
use serde_json::{json, to_vec};
use url::Url;

use clap::Parser;
use itertools::Itertools;

/// An example program that writes to a Delta table and creates it if necessary.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    write_args: WriteArgs,

    #[command(flatten)]
    location_args: LocationArgs,
}

#[derive(Args)]
pub struct WriteArgs {
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

async fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    let path = &cli.location_args.path;
    // TODO: Check if path is a directory and if not, create it
    let url = delta_kernel::try_parse_uri(&path)?;
    println!("Using Delta table at: {url}");

    // Get the engine
    let engine = common::get_engine(&url, &cli.location_args)?;
    let store = engine
        .get_object_store_for_url(&url)
        .expect(&format!("Failed to get object store for URL: {url}"));

    // Create or get the table
    let (table_url, schema) =
        create_or_get_table(&url, &engine, &cli.write_args.schema, &store).await?;

    // Create sample data based on the schema
    let sample_data = create_sample_data(&schema, cli.write_args.num_rows)?;

    // Write sample data to the table
    write_data(&table_url, &engine, &sample_data).await?;
    println!(
        "✓ Successfully wrote {} rows to the table",
        cli.write_args.num_rows
    );

    // Read and display the data
    read_and_display_data(&table_url, engine).await?;
    println!("✓ Successfully read data from the table");

    Ok(())
}

/// Creates a new Delta table or gets an existing one
async fn create_or_get_table(
    url: &Url,
    engine: &dyn Engine,
    schema_str: &String,
    store: &Arc<dyn ObjectStore>,
) -> DeltaResult<(Url, SchemaRef)> {
    // Check if table already exists
    match Snapshot::try_new(url.clone(), engine, None) {
        Ok(snapshot) => {
            println!("✓ Found existing table at version {}", snapshot.version());
            Ok((url.clone(), snapshot.schema()))
        }
        Err(_) => {
            // Create new table
            println!("Creating new Delta table...");
            let schema = parse_schema(schema_str)?;
            create_table(store, url, &schema).await?;
            Ok((url.clone(), schema))
        }
    }
}

/// Parse a schema string into a SchemaRef
fn parse_schema(schema_str: &String) -> DeltaResult<SchemaRef> {
    let fields = schema_str
        .split(',')
        .map(|field| {
            let parts: Vec<&str> = field.split(':').collect();
            if parts.len() != 2 {
                return Err(delta_kernel::Error::generic(format!(
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
                    return Err(delta_kernel::Error::generic(format!(
                        "Unsupported data type: {data_type}"
                    )));
                }
            };

            Ok(StructField::nullable(name, data_type))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(Arc::new(StructType::new(fields)))
}

/// Create a new Delta table with the given schema
/// Creating a Delta table is not officially supported by kernel-rs yet, so we manually create the initial transaction log
async fn create_table(
    store: &Arc<dyn ObjectStore>,
    table_url: &Url,
    schema: &SchemaRef,
) -> DeltaResult<()> {
    let table_id = "dummy_table";
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

    // Write the initial transaction with with protocol and metadata to 0.json
    let path = table_url.join("_delta_log/00000000000000000000.json")?;
    store
        .put(&Path::from_url_path(path.path())?, data.into())
        .await?;

    println!("✓ Created Delta table with schema: {:#?}", schema);
    Ok(())
}

/// Create sample data based on the schema
fn create_sample_data(schema: &SchemaRef, num_rows: usize) -> DeltaResult<ArrowEngineData> {
    let fields = schema.fields();
    let mut columns = Vec::new();

    for field in fields {
        let column: Arc<dyn arrow::array::Array> = match field.data_type() {
            &DataType::STRING => {
                let data: Vec<String> = (0..num_rows).map(|i| format!("item_{}", i)).collect();
                Arc::new(StringArray::from(data))
            }
            &DataType::INTEGER => {
                let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
                Arc::new(Int32Array::from(data))
            }
            &DataType::LONG => {
                let data: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
                Arc::new(arrow::array::Int64Array::from(data))
            }
            &DataType::DOUBLE => {
                let data: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();
                Arc::new(Float64Array::from(data))
            }
            &DataType::BOOLEAN => {
                let data: Vec<bool> = (0..num_rows).map(|i| i % 2 == 0).collect();
                Arc::new(BooleanArray::from(data))
            }
            &DataType::TIMESTAMP => {
                let now = chrono::Utc::now();
                let data: Vec<i64> = (0..num_rows)
                    .map(|i| (now + chrono::Duration::seconds(i as i64)).timestamp_micros())
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(data))
            }
            _ => {
                return Err(delta_kernel::Error::generic(format!(
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

/// Write data to the Delta table
async fn write_data(
    table_url: &Url,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    data: &ArrowEngineData,
) -> DeltaResult<()> {
    let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), engine, None)?);
    let mut txn = snapshot
        .transaction()?
        .with_operation("INSERT".to_string())
        .with_engine_info("delta-kernel-rs example");

    // Write the data using the engine
    let write_context = Arc::new(txn.get_write_context());
    let file_metadata = engine
        .write_parquet(&data, write_context.as_ref(), HashMap::new(), true)
        .await?;

    // Add the file metadata to the transaction
    txn.add_files(file_metadata);

    // Commit the transaction
    match txn.commit(engine)? {
        delta_kernel::transaction::CommitResult::Committed { version, .. } => {
            println!("✓ Committed transaction at version {}", version);
        }
        delta_kernel::transaction::CommitResult::Conflict(_, conflicting_version) => {
            println!(
                "✗ Transaction conflicted with version: {}",
                conflicting_version
            );
            return Err(delta_kernel::Error::generic("Commit failed"));
        }
    }

    Ok(())
}

/// Read and display data from the table
async fn read_and_display_data(
    table_url: &Url,
    engine: DefaultEngine<TokioBackgroundExecutor>,
) -> DeltaResult<()> {
    let snapshot = Snapshot::try_new(table_url.clone(), &engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let batches: Vec<RecordBatch> = scan
        .execute(Arc::new(engine))?
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();

            if let Some(mask) = mask {
                Ok(arrow::compute::filter_record_batch(
                    &record_batch,
                    &mask.into(),
                )?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()?;

    print_batches(&batches)?;
    Ok(())
}
