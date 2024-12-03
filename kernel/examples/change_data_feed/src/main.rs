use std::sync::Arc;

use arrow::{compute::filter_record_batch, util::pretty::print_batches};
use arrow_array::RecordBatch;
use clap::Parser;
use delta_kernel::{
    engine::{arrow_data::ArrowEngineData, sync::SyncEngine},
    DeltaResult, Table,
};
use itertools::Itertools;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    start_version: u64,

    end_version: Option<u64>,
}

fn main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let table = Table::try_from_uri(cli.path)?;
    let engine = Arc::new(SyncEngine::new());
    let table_changes = table.table_changes(engine.as_ref(), cli.start_version, cli.end_version)?;

    let x = table_changes.into_scan_builder().build()?;
    let batches: Vec<RecordBatch> = x
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
