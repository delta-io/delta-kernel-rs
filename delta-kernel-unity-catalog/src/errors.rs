//! Error helpers for UC operations. Centralizes error message construction to keep validation
//! logic concise.

use delta_kernel::KernelError;

pub(crate) fn missing_feature(feature: &str) -> KernelError {
    KernelError::InvalidProtocol(format!(
        "UC catalog-managed table requires the '{feature}' table feature"
    ))
}

pub(crate) fn missing_metadata_configuration() -> KernelError {
    KernelError::InvalidProtocol("UC catalog-managed table requires metadata configuration".into())
}

pub(crate) fn missing_property(key: &str) -> KernelError {
    KernelError::InvalidProtocol(format!(
        "UC catalog-managed table requires '{key}' in metadata configuration"
    ))
}

pub(crate) fn table_id_mismatch(expected: &str, actual: &str) -> KernelError {
    KernelError::InvalidTransactionState(format!(
        "UC table ID mismatch: expected '{expected}' but found '{actual}'"
    ))
}

pub(crate) fn ict_not_enabled() -> KernelError {
    KernelError::InvalidProtocol(
        "UC catalog-managed table requires 'delta.enableInCommitTimestamps=true'".into(),
    )
}

pub(crate) fn upgrade_downgrade_unsupported(direction: &str) -> KernelError {
    KernelError::Unsupported(format!(
        "Table {direction} is not yet supported by the UCCommitter"
    ))
}

pub(crate) fn alter_table_unsupported(what: &str) -> KernelError {
    KernelError::Unsupported(format!(
        "UCCommitter does not support commits that change the table {what}. \
         ALTER TABLE is not supported for catalog-managed tables."
    ))
}
