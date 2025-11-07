//! Common code to be shared between all examples. Mostly argument parsing, and a few other
//! utilities
use std::{collections::HashMap, sync::Arc};

use clap::{Args, CommandFactory, FromArgMatches};
use delta_kernel::{
    arrow::array::RecordBatch,
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    scan::Scan,
    schema::MetadataColumnSpec,
    DeltaResult, SnapshotRef,
};

use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    DynObjectStore, ObjectStoreScheme,
};
use url::Url;

#[derive(Args)]
pub struct LocationArgs {
    /// Path to the table
    pub path: String,

    /// Region to specify to the cloud access store (only applies to S3)
    #[arg(long, conflicts_with = "env_creds", conflicts_with = "option")]
    pub region: Option<String>,

    /// Extra key-value pairs to pass to the ObjectStore builder. Note different object stores
    /// accept different configuration options, see the object_store types: AmazonS3Builder,
    /// MicrosoftAzureBuilder, and GoogleCloudStorageBuilder. Specify as "key=value", and pass
    /// multiple times to set more than one option.
    #[arg(long, conflicts_with = "env_creds", conflicts_with = "region")]
    pub option: Vec<String>,

    /// Get credentials from the environment. For details see the object_store types:
    /// AmazonS3Builder, MicrosoftAzureBuilder, and GoogleCloudStorageBuilder. Specifically the
    /// `from_env` method.
    #[arg(
        long,
        conflicts_with = "region",
        conflicts_with = "option",
        default_value = "false"
    )]
    pub env_creds: bool,

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

    /// Comma separated list of columns to select. Must be passed as a single string, leading and
    /// trailing spaces for each column name will be trimmed
    #[arg(long)]
    pub columns: Option<String>,

    /// Include a _metadata.row_index field
    #[arg(long)]
    pub with_row_index: bool,

    /// Include a _metadata.row_id field if row-tracking is enabled
    #[arg(long)]
    pub with_row_id: bool,
}

pub trait ParseWithExamples<T> {
    /// parse command line, and add examples in help
    /// program_name - name of program
    /// caps_action - actions the program performs (like read), capitalized for start of sentence
    /// action - same as above, for middle of sentence
    /// trailing_args - will be put at the end of each example, used if the command needs extra args
    fn parse_with_examples(
        program_name: &str,
        caps_action: &str,
        action: &str,
        trailing_args: &str,
    ) -> T;
}

impl<T> ParseWithExamples<T> for T
where
    T: clap::Parser,
{
    fn parse_with_examples(
        program_name: &str,
        caps_action: &str,
        action: &str,
        trailing_args: &str,
    ) -> Self {
        let examples = format!("Examples:
  {caps_action} table at foo/bar/bazz, relative to where invoked:
    {program_name} foo/bar/bazz {trailing_args}

  Get S3 credentials, region, etc. from the environment, and {action} table on S3:
    {program_name} --env_creds s3://path/to/table {trailing_args}

  Specify azure credentials on the command line and {action} table in azure:
    {program_name} --option AZURE_STORAGE_ACCOUNT_NAME=my_account --option AZURE_STORAGE_ACCOUNT_KEY=my_key [more --option args] az://account/container/path {trailing_args}

  {caps_action} a table in a public S3 bucket in us-west-2 region:
    {program_name} --region us-west-2 --public s3://my/public/table {trailing_args}");
        let mut matches = <Self as CommandFactory>::command()
            .after_help(examples)
            .get_matches();
        let res = <Self as FromArgMatches>::from_arg_matches_mut(&mut matches)
            .map_err(|e| e.format(&mut Self::command()));
        match res {
            Ok(s) => s,
            Err(e) => e.exit(),
        }
    }
}

/// Get an engine configured to read table at `url` and `LocationArgs`
pub fn get_engine(
    url: &Url,
    args: &LocationArgs,
) -> DeltaResult<DefaultEngine<TokioBackgroundExecutor>> {
    if args.env_creds {
        let (scheme, _path) = ObjectStoreScheme::parse(url).map_err(|e| {
            delta_kernel::Error::Generic(format!("Object store could not parse url: {}", e))
        })?;
        use ObjectStoreScheme::*;
        let url_str = url.to_string();
        let store: Arc<DynObjectStore> = match scheme {
            AmazonS3 => Arc::new(AmazonS3Builder::from_env().with_url(url_str).build()?),
            GoogleCloudStorage => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_url(url_str)
                    .build()?,
            ),
            MicrosoftAzure => Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_url(url_str)
                    .build()?,
            ),
            Local | Memory | Http => {
                return Err(delta_kernel::Error::Generic(format!(
                    "Scheme {scheme:?} doesn't support getting credentials from environment"
                )));
            }
            _ => {
                // scheme is non-exhaustive
                return Err(delta_kernel::Error::Generic(format!(
                    "Unknown schema {scheme:?} doesn't support getting credentials from environment"
                )));
            }
        };
        Ok(DefaultEngine::new(
            store,
            Arc::new(TokioBackgroundExecutor::new()),
        ))
    } else if !args.option.is_empty() {
        let opts = args.option.iter().map(|option| {
            let parts: Vec<&str> = option.split("=").collect();
            (parts[0].to_ascii_lowercase(), parts[1])
        });
        DefaultEngine::try_new(url, opts, Arc::new(TokioBackgroundExecutor::new()))
    } else {
        let mut options = if let Some(ref region) = args.region {
            HashMap::from([("region", region.clone())])
        } else {
            HashMap::new()
        };
        if args.public {
            options.insert("skip_signature", "true".to_string());
        }
        DefaultEngine::try_new(url, options, Arc::new(TokioBackgroundExecutor::new()))
    }
}

/// Construct a scan at the latest snapshot. This is over the specified table and using the passed
/// engine. Parameters of the scan are controlled by the specified `ScanArgs`
pub fn get_scan(snapshot: SnapshotRef, args: &ScanArgs) -> DeltaResult<Option<Scan>> {
    if args.schema_only {
        println!("{:#?}", snapshot.schema());
        return Ok(None);
    }

    let mut scan_schema = snapshot.schema();
    if let Some(cols) = args.columns.as_ref() {
        let cols: Vec<&str> = cols.split(",").map(str::trim).collect();
        scan_schema = scan_schema.project_as_struct(&cols)?.into();
    }

    if args.with_row_index {
        scan_schema = scan_schema
            .add_metadata_column("_metadata.row_index", MetadataColumnSpec::RowIndex)?
            .into();
    }

    if args.with_row_id {
        scan_schema = scan_schema
            .add_metadata_column("_metadata.row_index", MetadataColumnSpec::RowIndex)?
            .into();
    }

    Ok(Some(
        snapshot.scan_builder().with_schema(scan_schema).build()?,
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
