/// Code to parse and handle actions from the delta log

pub(crate) mod action_definitions;
pub(crate) mod schemas;
pub(crate) mod types;

pub use action_definitions::{Format, Metadata, Protocol};
pub use types::*;

#[derive(Debug)]
pub enum ActionType {
    /// modify the data in a table by adding individual logical files
    Add,
    /// add a file containing only the data that was changed as part of the transaction
    Cdc,
    /// additional provenance information about what higher-level operation was being performed
    CommitInfo,
    /// contains a configuration (string-string map) for a named metadata domain
    DomainMetadata,
    /// changes the current metadata of the table
    Metadata,
    /// increase the version of the Delta protocol that is required to read or write a given table
    Protocol,
    /// modify the data in a table by removing individual logical files
    Remove,
    Txn,
    CheckpointMetadata,
    Sidecar,
}
