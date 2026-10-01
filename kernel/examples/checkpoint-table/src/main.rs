use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::parquet::arrow::async_writer::AsyncFileWriter;
use delta_kernel::parquet::arrow::AsyncArrowWriter;
use delta_kernel::parquet::errors::Result as ParquetResult;
use delta_kernel::{ActionReconciliationIterator, KernelError, KernelResult, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use futures::future::{BoxFuture, FutureExt};

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

    /// This program doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.  As
    /// such you need to pass --unsafe-i-know-what-im-doing as an argument to get this to
    /// actually write the checkpoint
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
) -> KernelResult<()> {
    parquet_writer.write(first_batch).await?;
    for data_res in batch_iter {
        let data = data_res
            .map_err(delta_kernel::Error::into_kernel_error)?
            .apply_selection_vector()
            .map_err(delta_kernel::Error::into_kernel_error)?;
        let batch = data
            .try_into_record_batch()
            .map_err(delta_kernel::Error::into_kernel_error)?;
        parquet_writer.write(&batch).await?;
    }
    Ok(())
}

async fn try_main() -> KernelResult<()> {
    let cli = Cli::parse_with_examples(env!("CARGO_PKG_NAME"), "Write", "write", "");

    let url = delta_kernel::try_parse_uri(&cli.location_args.path)
        .map_err(delta_kernel::Error::into_kernel_error)?;
    println!("Checkpointing Delta table at: {url}");

    use delta_kernel_default_engine::storage::store_from_url;
    let store = store_from_url(&url).map_err(delta_kernel::Error::into_kernel_error)?;
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .build();
    let snapshot = Snapshot::builder_for(url)
        .build(&engine)
        .map_err(delta_kernel::Error::into_kernel_error)?;

    if cli.unsafe_i_know_what_im_doing {
        snapshot
            .checkpoint(&engine, None)
            .map_err(delta_kernel::Error::into_kernel_error)?;
        println!("Table checkpointed");
    } else {
        // first we create a checkpoint writer
        let writer = snapshot
            .create_checkpoint_writer(&engine)
            .map_err(delta_kernel::Error::into_kernel_error)?;

        // this tells us the path where we should write the checkpoint file
        let checkpoint_path = writer
            .checkpoint_path()
            .map_err(delta_kernel::Error::into_kernel_error)?;
        // this gives us a iterator of `FilteredEngineData` that needs to be written to the file
        let mut data_iter = writer
            .checkpoint_data(&engine)
            .map_err(delta_kernel::Error::into_kernel_error)?;

        let batch_iter = data_iter.by_ref();
        // we'll use the first batch to determine the schema
        let first = batch_iter.next();
        let Some(first) = first else {
            return Err(KernelError::generic("No batches in checkpoint data"));
        };
        // Note that with `FilteredEngineData` it's important to `apply_selection_vector` to remove
        // any filtered out rows. It's also possible to use `into_parts` to get the
        // unfiltered batch and the selection vector individually, such that an engine could
        // write only the selected rows out without having to allocate a new engine data.
        // NB: Unselected rows MUST NOT be written to the checkpoint! Doing so will create an
        // invalid checkpoint
        let first_data = first
            .map_err(delta_kernel::Error::into_kernel_error)?
            .apply_selection_vector()
            .map_err(delta_kernel::Error::into_kernel_error)?;
        let first_batch = first_data
            .try_into_record_batch()
            .map_err(delta_kernel::Error::into_kernel_error)?;

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
    fn write(&mut self, bs: bytes::Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        self.len += bs.len() as u64;
        async move { Ok(()) }.boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        async move { Ok(()) }.boxed()
    }
}
