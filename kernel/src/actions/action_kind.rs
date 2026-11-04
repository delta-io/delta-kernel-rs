//! Typed action selections shared by declarative sources and replay.

use super::{
    ADD_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};

/// Actions that share an identity during reconstruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActionKind {
    /// Adds, optionally accompanied by removes that compete for the same file identity.
    FileActions {
        includes_removes: bool,
    },
    Protocol,
    Metadata,
    SetTransaction,
    DomainMetadata,
}

impl ActionKind {
    /// Physical columns carried by this selection.
    pub(crate) fn names(self) -> &'static [&'static str] {
        match self {
            Self::FileActions {
                includes_removes: false,
            } => &[ADD_NAME],
            Self::FileActions {
                includes_removes: true,
            } => &[ADD_NAME, REMOVE_NAME],
            Self::Protocol => &[PROTOCOL_NAME],
            Self::Metadata => &[METADATA_NAME],
            Self::SetTransaction => &[SET_TRANSACTION_NAME],
            Self::DomainMetadata => &[DOMAIN_METADATA_NAME],
        }
    }
}
