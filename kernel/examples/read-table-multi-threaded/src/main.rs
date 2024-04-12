use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::thread;

use arrow::array::{Array, AsArray, MapArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Int32Type, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::client::arrow_data::ArrowEngineData;
use delta_kernel::client::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::client::default::DefaultEngineInterface;
use delta_kernel::client::sync::SyncEngineInterface;
use delta_kernel::scan::state::GlobalScanState;
use delta_kernel::scan::{transform_to_logical, ScanBuilder};
use delta_kernel::schema::Schema;
use delta_kernel::{DeltaResult, EngineData, EngineInterface, FileMeta, Table};

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

    /// how many threads to read with (1 - 2048)
    #[arg(short, long, default_value_t = 2, value_parser = 1..=2048)]
    thread_count: i64,

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

// package up dv info as primitive types
struct DvInfo {
    storage_type: String,
    path_or_inline_dv: String,
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

// the way we as a connector represent data to scan. this is computed from the raw data returned
// from the scan, and could be any format the engine chooses to use to facilitate distributing work.
struct ScanFile {
    path: String,
    size: usize,
    partition_values: HashMap<String, String>,
    dv_info: Option<DvInfo>,
}

// TODO(nick): Maybe move all the arrow support code into a module so it looks less scary

// we know we're using arrow under the hood, so cast an EngineData into something we can work with
fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

// turn an entry in a MapArray into a HashMap
fn materialize(arry: &MapArray, row_index: usize) -> HashMap<String, String> {
    let mut ret = HashMap::new();
    let map_val = arry.value(row_index);
    let keys = map_val.column(0).as_string::<i32>();
    let values = map_val.column(1).as_string::<i32>();
    for (key, value) in keys.iter().zip(values.iter()) {
        if let (Some(key), Some(value)) = (key, value) {
            ret.insert(key.into(), value.into());
        }
    }
    ret
}

// turn our record batch iteratively into ScanFiles. This looks messy, but it's really just
// iterating the RecordBatch and pulling out the required information
fn get_scan_files(
    batch: RecordBatch,
    selection_vector: Vec<bool>,
) -> DeltaResult<impl Iterator<Item = ScanFile>> {
    Ok((0..batch.num_rows()).filter_map(move |row_index| {
        if selection_vector[row_index] {
            let path_col = batch.column(0).as_string::<i32>();
            let size_col = batch.column(1).as_primitive::<Int64Type>();

            let dv_col = batch.column(3).as_struct();
            let storage_col = dv_col.column(0).as_string::<i32>();
            let dv_info = if storage_col.is_valid(row_index) {
                let dv_path_col = dv_col.column(1).as_string::<i32>();
                let offset_col = dv_col.column(2).as_primitive::<Int32Type>();
                let size_in_bytes_col = dv_col.column(3).as_primitive::<Int32Type>();
                let cardinality_col = dv_col.column(4).as_primitive::<Int64Type>();
                Some(DvInfo {
                    storage_type: storage_col.value(row_index).to_string(),
                    path_or_inline_dv: dv_path_col.value(row_index).to_string(),
                    offset: if offset_col.is_valid(row_index) {
                        Some(offset_col.value(row_index))
                    } else {
                        None
                    },
                    size_in_bytes: size_in_bytes_col.value(row_index),
                    cardinality: cardinality_col.value(row_index),
                })
            } else {
                None
            };

            let constant_vals = batch.column(4).as_struct();
            let partition_col = constant_vals.column(0).as_map();
            let partition_values = materialize(partition_col, row_index);
            Some(ScanFile {
                path: path_col.value(row_index).to_string(),
                size: size_col.value(row_index) as usize,
                partition_values,
                dv_info,
            })
        } else {
            None
        }
    }))
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
                        .ok_or(delta_kernel::Error::Generic(format!(
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

    // this gives us an iterator of (our engine data, selection vector). our engine data is just
    // arrow data. The schema is (TODO link to schema)
    let scan_data = scan.scan_data(engine_interface.as_ref())?;

    // get any global state associated with this scan
    let global_state = scan.global_scan_state();

    // create the channels we'll use. record_batch_[t/r]x are used for the threads to send back the
    // processed RecordBatches to themain thread
    let (record_batch_tx, record_batch_rx) = mpsc::channel();
    // scan_file_[t/r]x are used to send each scan file from the iterator out to the waiting threads
    let (mut scan_file_tx, scan_file_rx) = spmc::channel();

    // Arc up the engine_interface so we can share it between threads
    let engine_interface = Arc::new(engine_interface);

    // fire up each thread. we don't need the handles as we rely on the channels to indicate when
    // things are done
    let _handles: Vec<_> = (0..cli.thread_count)
        .map(|_| {
            // items that we need to send to the other thread
            //let interface = cli.interface.clone();
            let scan_state = global_state.clone();
            let rb_tx = record_batch_tx.clone();
            let scan_file_rx = scan_file_rx.clone();
            let interface = engine_interface.clone();
            thread::spawn(move || {
                do_work(interface, scan_state, rb_tx, scan_file_rx);
            })
        })
        .collect();

    // have handed out all copies needed, drop so record_batch_rx will exit when the last thread is
    // done sending
    drop(record_batch_tx);

    // send out each scan file
    for res in scan_data {
        let (data, vector) = res?;
        let batch = to_arrow(data)?;
        for scan_file in get_scan_files(batch, vector)? {
            scan_file_tx.send(scan_file).unwrap();
        }
    }

    // have sent all scan files, drop this so threads will exit when there's no more work
    drop(scan_file_tx);

    // simply gather up each batch and print them
    let batches: Vec<_> = record_batch_rx.iter().collect();
    print_batches(&batches)?;
    Ok(())
}

// this is the work each thread does
fn do_work(
    engine_interface: Arc<Box<dyn EngineInterface>>,
    scan_state: GlobalScanState,
    record_batch_tx: Sender<RecordBatch>,
    scan_file_rx: spmc::Receiver<ScanFile>,
) {
    // get the type for the function calls
    let engine_interface: &dyn EngineInterface = engine_interface.as_ref().as_ref();
    let read_schema = Arc::new(scan_state.read_schema.clone());
    loop {
        // in a loop, try and get a ScanFile
        match scan_file_rx.recv() {
            Ok(scan_file) => {
                // we got a scan file, let's process it

                let root_url = Url::parse(&scan_state.table_root).unwrap();

                // get the selection vector (i.e. deletion vector)
                let mut selection_vector = scan_file.dv_info.map(|info| {
                    let descriptor = DeletionVectorDescriptor::new(
                        info.storage_type,
                        info.path_or_inline_dv,
                        info.offset,
                        info.size_in_bytes,
                        info.cardinality,
                    );
                    delta_kernel::scan::selection_vector(engine_interface, &descriptor, &root_url)
                        .unwrap()
                });

                // build the required metadata for our parquet handler to read this file
                let location = root_url.join(&scan_file.path).unwrap();
                let meta = FileMeta {
                    last_modified: 0,
                    size: scan_file.size,
                    location,
                };

                // this example uses the parquet_handler from the engine_client, but an engine could
                // choose to use whatever method it might want to read a parquet file. The reader
                // could, for example, fill in the parition columns, or apply deletion vectors. Here
                // we assume a more naive parquet reader and fix the data up after the fact.
                // further parallelism would also be possible here as we could read the parquet file
                // in chunks where each thread reads one chunk. The engine would need to ensure
                // enough meta-data was passed to each thread to correctly apply the selection
                // vector
                let read_results = engine_interface
                    .get_parquet_handler()
                    .read_parquet_files(&[meta], read_schema.clone(), None)
                    .unwrap();

                for read_result in read_results {
                    let len = if let Ok(ref res) = read_result {
                        res.length()
                    } else {
                        0
                    };

                    // // ask the kernel to transform the physical data into the correct logical form
                    let logical = transform_to_logical(
                        engine_interface,
                        read_result.unwrap(),
                        &scan_state,
                        &scan_file.partition_values,
                    )
                    .unwrap();

                    let record_batch = to_arrow(logical).unwrap();

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
