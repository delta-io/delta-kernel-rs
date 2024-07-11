use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::schema::Schema;
use delta_kernel::{DeltaResult, Engine, Table};

use clap::{Parser, ValueEnum};

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// Which Engine to use
    #[arg(short, long, value_enum, default_value_t = EngineType::Default)]
    engine: EngineType,

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
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum EngineType {
    /// Use the default, async engine
    Default,
    /// Use the sync engine (local files only)
    Sync,
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

    // build a table and get the lastest snapshot from it
    let table = Table::try_from_uri(&cli.path)?;
    println!("Reading {}", table.location());

    let engine: Box<dyn Engine> = match cli.engine {
        EngineType::Default => {
            let mut options = if let Some(region) = cli.region {
                HashMap::from([("region", region)])
            } else {
                HashMap::new()
            };
            if cli.public {
                options.insert("skip_signature", "true".to_string());
            }
            Box::new(DefaultEngine::try_new(
                table.location(),
                options,
                Arc::new(TokioBackgroundExecutor::new()),
            )?)
        }
        EngineType::Sync => Box::new(SyncEngine::new()),
    };

    let snapshot = table.snapshot(engine.as_ref(), None)?;

    let read_schema_opt = cli
        .columns
        .map(|cols| {
            use itertools::Itertools;
            let table_schema = snapshot.schema();
            let selected_fields = cols
                .iter()
                .map(|col| {
                    table_schema
                        .field(col)
                        .cloned()
                        .ok_or(delta_kernel::Error::Generic(format!(
                            "Table has no such column: {col}"
                        )))
                })
                .try_collect();
            selected_fields.map(|selected_fields| Arc::new(Schema::new(selected_fields)))
        })
        .transpose()?;
    let scan = snapshot
        .into_scan_builder()
        .with_schema_opt(read_schema_opt)
        .build()?;

    let mut batches = vec![];
    for res in scan.execute(engine.as_ref())?.into_iter() {
        let data = res.raw_data?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into();
        let batch = if let Some(mut mask) = res.mask {
            let extra_rows = record_batch.num_rows() - mask.len();
            if extra_rows > 0 {
                // we need to extend the mask here in case it's too short
                mask.extend(std::iter::repeat(true).take(extra_rows));
            }
            filter_record_batch(&record_batch, &mask.into())?
        } else {
            record_batch
        };
        batches.push(batch);
    }
    print_batches(&batches)?;
    Ok(())
}
