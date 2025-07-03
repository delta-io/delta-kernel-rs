use std::process::ExitCode;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use common::{LocationArgs, ScanArgs};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::DeltaResult;

use clap::Parser;
use itertools::Itertools;

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    // today we don't have any args unique to this version, but we keep this as flattened this way
    // for consistency with the multi-threaded version and to make it easy to add unique options in
    // the future
    #[command(flatten)]
    location_args: LocationArgs,

    #[command(flatten)]
    scan_args: ScanArgs,
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
    let table = common::get_table(&cli.location_args)?;
    println!("Reading {}", table.location());
    let engine = common::get_engine(&table, &cli.location_args)?;
    let scan = match common::get_scan(&table, &engine, &cli.scan_args)? {
        Some(scan) => scan,
        None => return Ok(()),
    };

    let mut rows_so_far = 0;
    let batches: Vec<RecordBatch> = scan
        .execute(Arc::new(engine))?
        .map(|scan_result| -> DeltaResult<Option<_>> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            let batch = if let Some(mask) = mask {
                filter_record_batch(&record_batch, &mask.into())?
            } else {
                record_batch
            };
            if let Some(limit) = cli.scan_args.limit {
                let batch_rows = batch.num_rows();
                let final_batch = if rows_so_far < limit {
                    let batch = if rows_so_far + batch_rows > limit {
                        // truncate this batch
                        common::truncate_batch(batch, limit - rows_so_far)
                    } else {
                        batch
                    };
                    Ok(Some(batch))
                } else {
                    Ok(None)
                };
                rows_so_far += batch_rows;
                final_batch
            } else {
                Ok(Some(batch))
            }
        })
        .flatten_ok()
        .try_collect()?;
    if let Some(limit) = cli.scan_args.limit {
        println!("Printing first {limit} rows of {rows_so_far} total rows");
    }
    print_batches(&batches)?;
    Ok(())
}
