use std::process::ExitCode;

use arrow::array::RecordBatch;
use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use futures::future::{BoxFuture, FutureExt};
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::arrow::AsyncArrowWriter;

use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::{ActionReconciliationIterator, DeltaResult, Error, FileMeta, Snapshot};

/// An example program that checkpoints a table.
/// !!!WARNING!!!: This doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.
/// As such you need to pass --unsafe_i_know_what_im_doing as an argument to get this to actually
/// write the checkpoint, otherwise it will just do all the work it _would_ have done, but not
/// actually write the final checkpoint.
#[derive(Parser)]
#[command(author, version, about, verbatim_doc_comment)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,

    /// This program doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.  As such
    /// you need to pass --unsafe-i-know-what-im-doing as an argument to get this to actually write
    /// the checkpoint
    #[arg(long)]
    unsafe_i_know_what_im_doing: bool,
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

async fn write_data<W: AsyncFileWriter>(
    first_batch: &RecordBatch,
    batch_iter: &mut ActionReconciliationIterator,
    parquet_writer: &mut AsyncArrowWriter<W>,
) -> DeltaResult<()> {
    parquet_writer.write(first_batch).await?;
    for data_res in batch_iter {
        let data = data_res?.apply_selection_vector()?;
        let batch = data.try_into_record_batch()?;
        parquet_writer.write(&batch).await?;
    }
    Ok(())
}

async fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse_with_examples(env!("CARGO_PKG_NAME"), "Write", "write", "");

    let url = delta_kernel::try_parse_uri(&cli.location_args.path)?;
    println!("Checkpointing Delta table at: {url}");

    use delta_kernel::engine::default::storage::store_from_url;
    let store = store_from_url(&url)?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    // first we create a checkpoint writer
    let writer = snapshot.create_checkpoint_writer()?;

    // this tells us the path where we should write the checkpoint file
    let checkpoint_path = writer.checkpoint_path()?;
    // this gives us a iterator of `FilteredEngineData` that needs to be written to the file
    let mut data_iter = writer.checkpoint_data(&engine)?;

    let batch_iter = data_iter.by_ref();
    // we'll use the first batch to determine the schema
    let first = batch_iter.next();

    let Some(first) = first else {
        return Err(Error::generic("No batches in checkpoint data"));
    };
    // Note that with `FilteredEngineData` it's important to `apply_selection_vector` to remove any
    // filtered out rows. It's also possible to use `into_parts` to get the unfiltered batch and the
    // selection vector individually, such that an engine could write only the selected rows out
    // without having to allocate a new engine data.
    // NB: Unselected rows MUST NOT be written to the checkpoint! Doing so will create an invalid
    // checkpoint
    let first_data = first?.apply_selection_vector()?;
    let first_batch = first_data.try_into_record_batch()?;

    if cli.unsafe_i_know_what_im_doing {
        // this block uses the arrow writer to write the data out
        let path = object_store::path::Path::from_url_path(checkpoint_path.path())?;
        let object_writer = ParquetObjectWriter::new(store.clone(), path.clone());
        let mut parquet_writer =
            AsyncArrowWriter::try_new(object_writer, first_batch.schema(), None)?;
        write_data(&first_batch, batch_iter, &mut parquet_writer).await?;
        parquet_writer.close().await?;
        let metadata = store.head(&path).await?;
        let file_meta = FileMeta {
            location: checkpoint_path.clone(),
            last_modified: metadata.last_modified.timestamp() * 1000,
            size: metadata.size,
        };
        // It's important to call `finalize` on the writer, which will create a `_last_checkpoint`
        // file
        writer.finalize(&engine, &file_meta, data_iter)?;
        println!("Table checkpointed");
    } else {
        println!("--unsafe-i-know-what-im-doing not specified, just doing a dry run");
        // this block just writes the checkpoint to a blackhole
        let mut parquet_writer =
            AsyncArrowWriter::try_new(BlackholeWriter::default(), first_batch.schema(), None)?;
        write_data(&first_batch, batch_iter, &mut parquet_writer).await?;
        parquet_writer.finish().await?;
        let blackhole_writer = parquet_writer.into_inner();
        println!(
            "Would have written a checkpoint as:\n\tpath: {checkpoint_path}\n\tsize: {}",
            blackhole_writer.len
        );
        // in this example we don't call `finalize` because we don't want to actually write
        // anything, but if really checkpointing, it's important to call finalize as we do above
    }
    Ok(())
}

/// Simple struct to allow us to go through the motions of writing the data without actually writing
/// it anywhere. Verifies that the actual flow of data does work.
#[derive(Default)]
pub struct BlackholeWriter {
    len: u64,
}

impl AsyncFileWriter for BlackholeWriter {
    fn write(&mut self, bs: bytes::Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.len += bs.len() as u64;
        async move { Ok(()) }.boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move { Ok(()) }.boxed()
    }
}
