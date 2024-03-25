use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use deltakernel::client::arrow_data::ArrowEngineData;
use deltakernel::client::default::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::default::DefaultEngineInterface;
use deltakernel::client::sync::SyncEngineInterface;
use deltakernel::scan::ScanBuilder;
use deltakernel::schema::{Schema, StructField};
use deltakernel::{DeltaResult, EngineInterface, Table};

use clap::{Parser, ValueEnum};

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// Which EngineInterface to use
    #[arg(short, long, value_enum, default_value_t = Interface::Default)]
    interface: Interface,

    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    columns: Option<Vec<String>>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Interface {
    /// Use the default, async engine interface
    Default,
    /// Use the sync engine interface (local files only)
    Sync,
}

fn main() -> DeltaResult<()> {
    env_logger::init();
    let cli = Cli::parse();
    let url = url::Url::parse(&cli.path)?;

    println!("Reading {url}");
    let engine_interface: Box<dyn EngineInterface> = match cli.interface {
        Interface::Default => Box::new(DefaultEngineInterface::try_new(
            &url,
            HashMap::<String, String>::new(),
            Arc::new(TokioBackgroundExecutor::new()),
        )?),
        Interface::Sync => Box::new(SyncEngineInterface::new()),
    };

    let table = Table::new(url);
    let snapshot = table.snapshot(engine_interface.as_ref(), None)?;

    let scan = match cli.columns {
        Some(cols) => {
            use itertools::Itertools;
            let table_schema = snapshot.schema();
            let selected_fields: Vec<StructField> = cols
                .iter()
                .map(|col| {
                    table_schema
                        .field(col)
                        .cloned()
                        .ok_or(deltakernel::Error::Generic(format!(
                            "Table has no such column: {col}"
                        )))
                })
                .try_collect()?;
            let read_schema = Arc::new(Schema::new(selected_fields));
            let builder = ScanBuilder::new(snapshot).with_schema(read_schema);
            builder.build()
        }
        None => ScanBuilder::new(snapshot).build(),
    };

    let mut batches = vec![];
    for res in scan.execute(engine_interface.as_ref())?.into_iter() {
        let data = res.raw_data?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| deltakernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into();
        let batch = if let Some(mask) = res.mask {
            filter_record_batch(&record_batch, &mask.into())?
        } else {
            record_batch
        };
        batches.push(batch);
    }
    print_batches(&batches)?;
    Ok(())
}
