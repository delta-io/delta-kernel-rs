use common::LocationArgs;
use delta_kernel::actions::visitors::{
    visit_metadata_at, visit_protocol_at, AddVisitor, CdcVisitor, DomainMetadataVisitor,
    RemoveVisitor, SetTransactionVisitor,
};
use delta_kernel::actions::{
    get_log_schema, ADD_NAME, CDC_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME,
    REMOVE_NAME, SET_TRANSACTION_NAME,
};
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::{ColumnNamesAndTypes, DataType, Schema, SchemaRef};
use delta_kernel::{DeltaResult, Error, ExpressionRef, Snapshot};

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::LazyLock;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[command(flatten)]
    location_args: LocationArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the most recent version of the table
    TableVersion,
    /// Show the table's metadata
    Metadata,
    /// Show the table's schema
    Schema,
    /// Show the meta-data that would be used to scan the table
    ScanMetadata,
    /// Show each action from the log-segments
    Actions {
        /// Show the log in reverse order (default is log replay order -- newest first)
        #[arg(short, long)]
        oldest_first: bool,
        #[arg(short, long)]
        stats_only: bool,
    },
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

enum Action {
    Metadata(delta_kernel::actions::Metadata),
    Protocol(delta_kernel::actions::Protocol),
    Remove(delta_kernel::actions::Remove),
    Add(delta_kernel::actions::Add),
    SetTransaction(delta_kernel::actions::SetTransaction),
    Cdc(delta_kernel::actions::Cdc),
    DomainMetadata(delta_kernel::actions::DomainMetadata),
}

fn custom_log_schema() -> SchemaRef {
    let fields = get_log_schema()
        .fields()
        .filter(|f| f.name() != "commitInfo")
        .cloned();
    Schema::try_new(fields).unwrap().into()
}
static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| custom_log_schema().leaves(None));

struct LogVisitor {
    actions: Vec<(Action, usize)>,
    stats: HashMap<&'static str, usize>,
    offsets: HashMap<String, (usize, usize)>,
    previous_rows_seen: usize,
    stats_only: bool,
}

impl LogVisitor {
    fn new(stats_only: bool) -> LogVisitor {
        // Grab the start offset for each top-level column name, then compute the end offset by
        // skipping the rest of the leaves for that column.
        let mut offsets = HashMap::new();
        let mut it = NAMES_AND_TYPES.as_ref().0.iter().enumerate().peekable();
        while let Some((start, col)) = it.next() {
            let mut end = start + 1;
            while it.next_if(|(_, other)| col[0] == other[0]).is_some() {
                end += 1;
            }
            offsets.insert(col[0].clone(), (start, end));
        }
        LogVisitor {
            actions: vec![],
            stats: HashMap::new(),
            offsets,
            previous_rows_seen: 0,
            stats_only,
        }
    }
}

impl RowVisitor for LogVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        if getters.len() != 59 {
            return Err(Error::InternalError(format!(
                "Wrong number of LogVisitor getters: {}",
                getters.len()
            )));
        }
        let (add_start, add_end) = self.offsets[ADD_NAME];
        let (remove_start, remove_end) = self.offsets[REMOVE_NAME];
        let (metadata_start, metadata_end) = self.offsets[METADATA_NAME];
        let (protocol_start, protocol_end) = self.offsets[PROTOCOL_NAME];
        let (txn_start, txn_end) = self.offsets[SET_TRANSACTION_NAME];
        let (cdc_start, cdc_end) = self.offsets[CDC_NAME];
        let (dm_start, dm_end) = self.offsets[DOMAIN_METADATA_NAME];

        for i in 0..row_count {
            let (action, name) = if let Some(path) = getters[add_start].get_opt(i, "add.path")? {
                let add = AddVisitor::visit_add(i, path, &getters[add_start..add_end])?;
                (Action::Add(add), "add")
            } else if let Some(path) = getters[remove_start].get_opt(i, "remove.path")? {
                let remove =
                    RemoveVisitor::visit_remove(i, path, &getters[remove_start..remove_end])?;
                (Action::Remove(remove), "remove")
            } else if let Some(metadata) =
                visit_metadata_at(i, &getters[metadata_start..metadata_end])?
            {
                (Action::Metadata(metadata), "metadata")
            } else if let Some(protocol) =
                visit_protocol_at(i, &getters[protocol_start..protocol_end])?
            {
                (Action::Protocol(protocol), "protocol")
            } else if let Some(app_id) = getters[txn_start].get_opt(i, "txn.appId")? {
                let txn =
                    SetTransactionVisitor::visit_txn(i, app_id, &getters[txn_start..txn_end])?;
                (Action::SetTransaction(txn), "setTransaction")
            } else if let Some(path) = getters[cdc_start].get_opt(i, "cdc.path")? {
                let cdc = CdcVisitor::visit_cdc(i, path, &getters[cdc_start..cdc_end])?;
                (Action::Cdc(cdc), "cdc")
            } else if let Some(domain) = getters[dm_start].get_opt(i, "domainMetadata.name")? {
                let dm = DomainMetadataVisitor::visit_domain_metadata(
                    i,
                    domain,
                    &getters[dm_start..dm_end],
                )?;
                (Action::DomainMetadata(dm), "domainMetadata")
            } else {
                // TODO: Add CommitInfo support (tricky because all fields are optional)
                continue;
            };

            *self.stats.entry(name).or_default() += 1;
            if !self.stats_only {
                self.actions.push((action, self.previous_rows_seen + i));
            }
        }
        self.previous_rows_seen += row_count;
        Ok(())
    }
}

// This is the callback that will be called for each valid scan row
fn print_scan_file(
    _: &mut (),
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    partition_values: HashMap<String, String>,
) {
    let num_record_str = if let Some(s) = stats {
        format!("{}", s.num_records)
    } else {
        "[unknown]".to_string()
    };
    println!(
        "Data to process:\n  \
              Path:\t\t{path}\n  \
              Size (bytes):\t{size}\n  \
              Num Records:\t{num_record_str}\n  \
              Has DV?:\t{}\n  \
              Transform:\t{transform:?}\n  \
              Part Vals:\t{partition_values:?}",
        dv_info.has_vector()
    );
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    let url = delta_kernel::try_parse_uri(&cli.location_args.path)?;
    let engine = common::get_engine(&url, &cli.location_args)?;
    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    match cli.command {
        Commands::TableVersion => {
            println!("Latest table version: {}", snapshot.version());
        }
        Commands::Metadata => {
            println!("{:#?}", snapshot.metadata());
        }
        Commands::Schema => {
            println!("{:#?}", snapshot.schema());
        }
        Commands::ScanMetadata => {
            let scan = ScanBuilder::new(snapshot).build()?;
            let scan_metadata_iter = scan.scan_metadata(&engine)?;
            for res in scan_metadata_iter {
                let scan_metadata = res?;
                scan_metadata.visit_scan_files((), print_scan_file)?;
            }
        }
        Commands::Actions {
            oldest_first,
            stats_only,
        } => {
            let log_schema = custom_log_schema();
            let actions = snapshot.log_segment().read_actions(
                &engine,
                log_schema.clone(),
                log_schema.clone(),
                None,
            )?;

            let mut visitor = LogVisitor::new(stats_only);
            for action in actions {
                visitor.visit_rows_of(action?.actions())?;
            }

            let mut stats: Vec<_> = visitor
                .stats
                .iter()
                .map(|(&name, &count)| (count, name))
                .collect();
            stats.sort_by(|(count1, _), (count2, _)| count2.cmp(count1));
            for (count, name) in stats {
                println!("Found {count} '{name}' actions");
            }

            if oldest_first {
                visitor.actions.reverse();
            }
            for (action, row) in visitor.actions.iter() {
                match action {
                    Action::Metadata(md) => println!("\nAction {row}:\n{md:#?}"),
                    Action::Protocol(p) => println!("\nAction {row}:\n{p:#?}"),
                    Action::Remove(r) => println!("\nAction {row}:\n{r:#?}"),
                    Action::Add(a) => println!("\nAction {row}:\n{a:#?}"),
                    Action::SetTransaction(t) => println!("\nAction {row}:\n{t:#?}"),
                    Action::Cdc(c) => println!("\nAction {row}:\n{c:#?}"),
                    Action::DomainMetadata(d) => println!("\n Action {row}:\n{d:#?}"),
                }
            }
        }
    };
    Ok(())
}
