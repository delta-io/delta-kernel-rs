use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::{compute::filter_record_batch, util::pretty::print_batches};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Table};
use itertools::Itertools;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,
    /// The start version of the table changes
    #[arg(short, long, default_value_t = 0)]
    start_version: u64,
    /// The end version of the table changes
    #[arg(short, long)]
    end_version: Option<u64>,
}

fn main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let table = Table::try_from_uri(cli.path)?;
    let options = HashMap::from([("skip_signature", "true".to_string())]);
    let engine = Arc::new(DefaultEngine::try_new(
        table.location(),
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    )?);
    let table_changes = table.table_changes(engine.as_ref(), cli.start_version, cli.end_version)?;

    let table_changes_scan = table_changes.into_scan_builder().build()?;
    let batches: Vec<RecordBatch> = table_changes_scan
        .execute(engine.clone())?
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
