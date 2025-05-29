use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::Schema;
use delta_kernel::{DeltaResult, ResolvedTable};

use clap::Parser;
use itertools::Itertools;

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    columns: Option<Vec<String>>,

    /// Region to specify to the cloud access store (only applies if using the default engine)
    #[arg(long)]
    region: Option<String>,

    /// Specify that the table is "public" (i.e. no cloud credentials are needed). This is required
    /// for things like s3 public buckets, otherwise the kernel will try and authenticate by talking
    /// to the aws metadata server, which will fail unless you're on an ec2 instance.
    #[arg(long)]
    public: bool,

    /// Only print the schema of the table
    #[arg(long)]
    schema_only: bool,
}

fn main() -> ExitCode {
    env_logger::init();
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("{e:#?}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let url = delta_kernel::try_parse_uri(&cli.path)?;

    let mut options = if let Some(region) = cli.region {
        HashMap::from([("region", region)])
    } else {
        HashMap::new()
    };
    if cli.public {
        options.insert("skip_signature", "true".to_string());
    }
    let engine = Arc::new(DefaultEngine::try_new(
        &url,
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let snapshot = ResolvedTable::try_new(url, engine.as_ref(), None)?;
    println!("Reading {}", snapshot.table_root());

    if cli.schema_only {
        println!("{:#?}", snapshot.schema());
        return Ok(());
    }

    let read_schema_opt = cli
        .columns
        .map(|cols| -> DeltaResult<_> {
            let table_schema = snapshot.schema();
            let selected_fields = cols.iter().map(|col| {
                table_schema
                    .field(col)
                    .cloned()
                    .ok_or(delta_kernel::Error::Generic(format!(
                        "Table has no such column: {col}"
                    )))
            });
            Schema::try_new(selected_fields).map(Arc::new)
        })
        .transpose()?;
    let scan = snapshot
        .into_scan_builder()
        .with_schema_opt(read_schema_opt)
        .build()?;

    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
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
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()?;
    print_batches(&batches)?;
    Ok(())
}
