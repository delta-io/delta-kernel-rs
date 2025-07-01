use std::{collections::HashMap, sync::Arc};

use clap::Args;
use delta_kernel::{
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    scan::Scan,
    schema::Schema,
    DeltaResult, Table,
};

#[derive(Args)]
pub struct CommonArgs {
    /// Path to the table to inspect
    pub path: String,

    // /// how many threads to read with (1 - 2048)
    // #[arg(short, long, default_value_t = 2, value_parser = 1..=2048)]
    // thread_count: i64,
    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    pub columns: Option<Vec<String>>,

    /// Region to specify to the cloud access store (only applies if using the default engine)
    #[arg(long)]
    pub region: Option<String>,

    /// Specify that the table is "public" (i.e. no cloud credentials are needed). This is required
    /// for things like s3 public buckets, otherwise the kernel will try and authenticate by talking
    /// to the aws metadata server, which will fail unless you're on an ec2 instance.
    #[arg(long)]
    pub public: bool,

    /// Limit to printing only LIMIT rows.
    #[arg(short, long)]
    pub limit: Option<usize>,

    /// Only print the schema of the table
    #[arg(long)]
    pub schema_only: bool,
}

/// Utility function to take in the specified `CommonArgs` and configure a scan as
/// specified. Returns the constructed engine and scan
pub fn get_scan(
    args: &CommonArgs,
) -> DeltaResult<Option<(Scan, DefaultEngine<TokioBackgroundExecutor>)>> {
    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(&args.path)?;
    println!("Reading {}", table.location());

    let mut options = if let Some(ref region) = args.region {
        HashMap::from([("region", region.clone())])
    } else {
        HashMap::new()
    };
    if args.public {
        options.insert("skip_signature", "true".to_string());
    }
    let engine = DefaultEngine::try_new(
        table.location(),
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let snapshot = table.snapshot(&engine, None)?;

    if args.schema_only {
        println!("{:#?}", snapshot.schema());
        return Ok(None);
    }

    let read_schema_opt = args
        .columns
        .clone()
        .map(|cols| -> DeltaResult<_> {
            let table_schema = snapshot.schema();
            let selected_fields = cols.iter().map(|col| {
                table_schema
                    .field(col)
                    .cloned()
                    .ok_or(delta_kernel::Error::Generic(format!(
                        "Table has no such column: {col}"
                    )))
            });
            Schema::try_new(selected_fields).map(Arc::new)
        })
        .transpose()?;
    Ok(Some((
        snapshot
            .into_scan_builder()
            .with_schema_opt(read_schema_opt)
            .build()?,
        engine,
    )))
}
