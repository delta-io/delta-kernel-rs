use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::DefaultTableClient;
use deltakernel::scan::ScanBuilder;
use deltakernel::client::sync::data::SimpleData;
use deltakernel::{DeltaResult, Table};

use clap::Parser;

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,
}

fn main() -> DeltaResult<()> {
    env_logger::init();
    let cli = Cli::parse();
    let url = url::Url::parse(&cli.path)?;

    println!("Reading {url}");
    let engine_interface = DefaultTableClient::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine_interface, None)?;

    let scan = ScanBuilder::new(snapshot).build();

    let mut batches = vec![];
    for res in scan.execute(&engine_interface)?.into_iter() {
        let data = res.raw_data?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<SimpleData>()
            .map_err(|_| deltakernel::Error::EngineDataType("SimpleData".to_string()))?
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
