use delta_kernel::actions::visitors::{
    AddVisitor, CdcVisitor, MetadataVisitor, ProtocolVisitor, RemoveVisitor, SetTransactionVisitor,
};
use delta_kernel::actions::{
    get_log_schema, ADD_NAME, CDC_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::{ColumnNamesAndTypes, DataType};
use delta_kernel::{DeltaResult, Error, Table};

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::{Arc, LazyLock};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,

    #[command(subcommand)]
    command: Commands,
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
    ScanData,
    /// Show each action from the log-segments
    Actions {
        /// Show the log in reverse order (default is log replay order -- newest first)
        #[arg(short, long)]
        oldest_first: bool,
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
}

static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| get_log_schema().leaves(None));

struct LogVisitor {
    actions: Vec<(Action, usize)>,
    offsets: HashMap<String, (usize, usize)>,
    previous_rows_seen: usize,
}

impl LogVisitor {
    fn new() -> LogVisitor {
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
            offsets,
            previous_rows_seen: 0,
        }
    }
}

impl RowVisitor for LogVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        if getters.len() != 55 {
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
        for i in 0..row_count {
            let action = if let Some(path) = getters[add_start].get_opt(i, "add.path")? {
                let add = AddVisitor::visit_add(i, path, &getters[add_start..add_end])?;
                Action::Add(add)
            } else if let Some(path) = getters[remove_start].get_opt(i, "remove.path")? {
                let remove =
                    RemoveVisitor::visit_remove(i, path, &getters[remove_start..remove_end])?;
                Action::Remove(remove)
            } else if let Some(id) = getters[metadata_start].get_opt(i, "metadata.id")? {
                let metadata =
                    MetadataVisitor::visit_metadata(i, id, &getters[metadata_start..metadata_end])?;
                Action::Metadata(metadata)
            } else if let Some(min_reader_version) =
                getters[protocol_start].get_opt(i, "protocol.min_reader_version")?
            {
                let protocol = ProtocolVisitor::visit_protocol(
                    i,
                    min_reader_version,
                    &getters[protocol_start..protocol_end],
                )?;
                Action::Protocol(protocol)
            } else if let Some(app_id) = getters[txn_start].get_opt(i, "txn.appId")? {
                let txn =
                    SetTransactionVisitor::visit_txn(i, app_id, &getters[txn_start..txn_end])?;
                Action::SetTransaction(txn)
            } else if let Some(path) = getters[cdc_start].get_opt(i, "cdc.path")? {
                let cdc = CdcVisitor::visit_cdc(i, path, &getters[cdc_start..cdc_end])?;
                Action::Cdc(cdc)
            } else {
                // TODO: Add CommitInfo support (tricky because all fields are optional)
                continue;
            };
            self.actions.push((action, self.previous_rows_seen + i));
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
              Part Vals:\t{partition_values:?}",
        dv_info.has_vector()
    );
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(&cli.path)?;

    let engine = DefaultEngine::try_new(
        table.location(),
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let snapshot = table.snapshot(&engine, None)?;

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
        Commands::ScanData => {
            let scan = ScanBuilder::new(snapshot).build()?;
            let scan_data = scan.scan_data(&engine)?;
            for res in scan_data {
                let (data, vector, _transforms) = res?;
                delta_kernel::scan::state::visit_scan_files(
                    data.as_ref(),
                    &vector,
                    (),
                    print_scan_file,
                )?;
            }
        }
        Commands::Actions { oldest_first } => {
            let log_schema = get_log_schema();
            let actions = snapshot._log_segment().replay(
                &engine,
                log_schema.clone(),
                log_schema.clone(),
                None,
            )?;

            let mut visitor = LogVisitor::new();
            for action in actions {
                visitor.visit_rows_of(action?.0.as_ref())?;
            }

            if oldest_first {
                visitor.actions.reverse();
            }
            for (action, row) in visitor.actions.iter() {
                match action {
                    Action::Metadata(md) => println!("\nAction {row}:\n{:#?}", md),
                    Action::Protocol(p) => println!("\nAction {row}:\n{:#?}", p),
                    Action::Remove(r) => println!("\nAction {row}:\n{:#?}", r),
                    Action::Add(a) => println!("\nAction {row}:\n{:#?}", a),
                    Action::SetTransaction(t) => println!("\nAction {row}:\n{:#?}", t),
                    Action::Cdc(c) => println!("\nAction {row}:\n{:#?}", c),
                }
            }
        }
    };
    Ok(())
}
