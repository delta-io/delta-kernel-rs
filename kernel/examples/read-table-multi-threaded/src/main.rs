use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::thread;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use deltakernel::client::arrow_data::ArrowEngineData;
use deltakernel::client::default::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::default::DefaultEngineInterface;
use deltakernel::client::sync::SyncEngineInterface;
use deltakernel::scan::ScanFile;
use deltakernel::scan::{state::ScanState, transform_to_logical, ScanBuilder};
use deltakernel::schema::Schema;
use deltakernel::{DeltaResult, EngineInterface, FileMeta, Table};

use clap::{Parser, ValueEnum};
use url::Url;

/// An example program that reads a table using multiple threads. This shows the use of the
/// scan_files and scan_state methods on a Scan, that can be used to partition work to either
/// multiple threads, or workers (in the case of a distributed engine).
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// how many threads to read with
    #[arg(short, long, default_value_t = 2)]
    thread_count: usize,

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
    let url = url::Url::parse(&cli.path)?;

    println!("Reading {url}");

    // create the requested interface
    let engine_interface: Box<dyn EngineInterface> = match cli.interface {
        Interface::Default => Box::new(DefaultEngineInterface::try_new(
            &url,
            HashMap::<String, String>::new(),
            Arc::new(TokioBackgroundExecutor::new()),
        )?),
        Interface::Sync => Box::new(SyncEngineInterface::new()),
    };

    // build a table and get the lastest snapshot from it
    let table = Table::new(url.clone());
    let snapshot = table.snapshot(engine_interface.as_ref(), None)?;

    // process the columns requested and build a schema from them
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
                        .ok_or(deltakernel::Error::Generic(format!(
                            "Table has no such column: {col}"
                        )))
                })
                .try_collect();
            selected_fields.map(|selected_fields| Arc::new(Schema::new(selected_fields)))
        })
        .transpose()?;

    // build a scan with the specified schema
    let scan = ScanBuilder::new(snapshot)
        .with_schema_opt(read_schema_opt)
        .build();

    // this gives us an iterator of `ScanFile`s, which are just paths and metadata for the files
    // that make up the table
    let scan_files = scan.scan_files(engine_interface.as_ref())?;

    // create the channels we'll use. record_batch_[t/r]x are used for the threads to send back the
    // processed RecordBatches to themain thread
    let (record_batch_tx, record_batch_rx) = mpsc::channel();
    // scan_file_[t/r]x are used to send each scan file from the iterator out to the waiting threads
    let (mut scan_file_tx, scan_file_rx) = spmc::channel();

    // fire up each thread. we don't need the handles as we rely on the channels to indicate when
    // things are done
    let _handles: Vec<_> = (0..cli.thread_count)
        .map(|_| {
            // items that we need to send to the other thread
            let interface = cli.interface.clone();
            let scan_state = scan.get_scan_state().into_owned();
            let url = url.clone();
            let rb_tx = record_batch_tx.clone();
            let scan_file_rx = scan_file_rx.clone();
            thread::spawn(move || {
                do_work(interface, scan_state, url, rb_tx, scan_file_rx);
            })
        })
        .collect();

    // have handed out all copies needed, drop so record_batch_rx will exit when the last thread is
    // done sending
    drop(record_batch_tx);

    // send out each scan file
    for scan_file in scan_files {
        let scan_file = scan_file?;
        scan_file_tx.send(scan_file).unwrap();
    }

    // have sent all scan files, drop this so threads will exit when there's no more work
    drop(scan_file_tx);

    // simply gather up each batch and print them
    let mut batches = vec![];
    for received in record_batch_rx {
        batches.push(received);
    }
    print_batches(&batches)?;
    Ok(())
}

// this is the work each thread does
fn do_work(
    interface: Interface, // what type of engine_interface was requested
    scan_state: ScanState<'_>,
    url: Url,
    record_batch_tx: Sender<RecordBatch>,
    scan_file_rx: spmc::Receiver<ScanFile>,
) { // todo: return a result and don't unwrap everywhere
    // each thread needs its own copy since engine_interface isn't Clone, Send, or Sync
    let engine_interface: Box<dyn EngineInterface> = match interface {
        Interface::Default => Box::new(
            DefaultEngineInterface::try_new(
                &url,
                HashMap::<String, String>::new(),
                Arc::new(TokioBackgroundExecutor::new()),
            )
            .unwrap(),
        ),
        Interface::Sync => Box::new(SyncEngineInterface::new()),
    };

    loop {
        // in a loop, try and get a ScanFile
        match scan_file_rx.recv() {
            Ok(scan_file) => {
                // we got a scan file, let's process it

                // get the selection vector (i.e. deletion vector)
                let mut selection_vector = scan_file
                    .selection_vector(engine_interface.as_ref())
                    .unwrap();

                // this example uses the parquet_handler from the engine_client, but an engine could
                // choose to use whatever method it might want to read a parquet file. further
                // parallelism would be possible here as we could read the parquet file in chunks
                // where each thread reads one chunk. The engine would need to ensure enough
                // meta-data was passed to each thread to correctly apply the selection vector

                // build the required metadata and ask our parquet handler to read it.
                let meta = &[FileMeta {
                    last_modified: 0,
                    size: scan_file.size,
                    location: scan_file.location().unwrap(),
                }];
                // could push selection_vector into the read here if desired
                let read_results = engine_interface
                    .get_parquet_handler()
                    .read_parquet_files(meta, scan_state.read_schema(), None)
                    .unwrap();


                for read_result in read_results {
                    let len = if let Ok(ref res) = read_result {
                        res.length()
                    } else {
                        0
                    };

                    // ask the kernel to transform the physical data into the correct logical form
                    let logical = transform_to_logical(
                        engine_interface.as_ref(),
                        read_result.unwrap(),
                        &scan_state,
                        &scan_file,
                    )
                    .unwrap();

                    // we know we're using arrow under the hood, so cast this into something we can
                    // work with
                    let record_batch: RecordBatch = logical
                        .into_any()
                        .downcast::<ArrowEngineData>()
                        .map_err(|_| {
                            deltakernel::Error::EngineDataType("ArrowEngineData".to_string())
                        })
                        .unwrap()
                        .into();

                    // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                    // will cover the following results
                    let rest = selection_vector.as_mut().map(|mask| mask.split_off(len));
                    let batch = if let Some(mask) = selection_vector.clone() {
                        // apply the selection vector
                        filter_record_batch(&record_batch, &mask.into()).unwrap()
                    } else {
                        record_batch
                    };
                    selection_vector = rest;
                    // send back the processed result
                    record_batch_tx.send(batch).unwrap();
                }
            }
            Err(_) => {
                // failed to read, so there's no more work, just exit
                break;
            }
        }
    }
}
