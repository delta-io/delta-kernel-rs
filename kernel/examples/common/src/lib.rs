//! Common code to be shared between all examples. Mostly argument parsing, and a few other
//! utilities
use std::{collections::HashMap, sync::Arc};

use clap::Args;
use delta_kernel::{
    arrow::array::RecordBatch,
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    scan::Scan,
    schema::Schema,
    DeltaResult, Table,
};

#[derive(Args)]
pub struct LocationArgs {
    /// Path to the table
    pub path: String,

    /// Region to specify to the cloud access store (only applies to S3)
    #[arg(long)]
    pub region: Option<String>,

    /// Specify that the table is "public" (i.e. no cloud credentials are needed). This is required
    /// for things like s3 public buckets, otherwise the kernel will try and authenticate by talking
    /// to the aws metadata server, which will fail unless you're on an ec2 instance.
    #[arg(long)]
    pub public: bool,
}

#[derive(Args)]
pub struct ScanArgs {
    /// Limit to printing only LIMIT rows.
    #[arg(short, long)]
    pub limit: Option<usize>,

    /// Only print the schema of the table
    #[arg(long)]
    pub schema_only: bool,

    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    pub columns: Option<Vec<String>>,
}

/// Get a [`Table`] from the specified location args
pub fn get_table(args: &LocationArgs) -> DeltaResult<Table> {
    Table::try_from_uri(&args.path)
}

/// Get an engine configured to read table specified by `table` and `LocationArgs`
pub fn get_engine(
    table: &Table,
    args: &LocationArgs,
) -> DeltaResult<DefaultEngine<TokioBackgroundExecutor>> {
    let mut options = if let Some(ref region) = args.region {
        HashMap::from([("region", region.clone())])
    } else {
        HashMap::new()
    };
    if args.public {
        options.insert("skip_signature", "true".to_string());
    }
    DefaultEngine::try_new(
        table.location(),
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    )
}

/// Construct a scan at the latest snapshot. This is over the specified table and using the passed
/// engine. Parameters of the scan are controlled by the specified `ScanArgs`
pub fn get_scan(
    table: &Table,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    args: &ScanArgs,
) -> DeltaResult<Option<Scan>> {
    let snapshot = table.snapshot(engine, None)?;

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
    Ok(Some(
        snapshot
            .into_scan_builder()
            .with_schema_opt(read_schema_opt)
            .build()?,
    ))
}

/// truncate a `RecordBatch` to the specified number of rows
pub fn truncate_batch(batch: RecordBatch, rows: usize) -> RecordBatch {
    let cols = batch
        .columns()
        .iter()
        .map(|col| col.slice(0, rows))
        .collect();
    RecordBatch::try_new(batch.schema(), cols).unwrap()
}
