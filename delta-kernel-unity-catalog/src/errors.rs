//! Error helpers for UC operations. Centralizes error message construction to keep validation
//! logic concise.

use delta_kernel::Error as DeltaError;

pub(crate) fn missing_feature(feature: &str) -> DeltaError {
    DeltaError::generic(format!(
        "UC catalog-managed table requires the '{feature}' table feature"
    ))
}

pub(crate) fn missing_metadata_configuration() -> DeltaError {
    DeltaError::generic("UC catalog-managed table requires metadata configuration")
}

pub(crate) fn missing_property(key: &str) -> DeltaError {
    DeltaError::generic(format!(
        "UC catalog-managed table requires '{key}' in metadata configuration"
    ))
}

pub(crate) fn table_id_mismatch(expected: &str, actual: &str) -> DeltaError {
    DeltaError::generic(format!(
        "UC table ID mismatch: expected '{expected}' but found '{actual}'"
    ))
}

pub(crate) fn ict_not_enabled() -> DeltaError {
    DeltaError::generic(
        "UC catalog-managed table requires 'delta.enableInCommitTimestamps=true'",
    )
}

pub(crate) fn upgrade_downgrade_unsupported(direction: &str) -> DeltaError {
    DeltaError::generic(format!(
        "Table {direction} is not yet supported by the UCCommitter"
    ))
}

pub(crate) fn alter_table_unsupported(what: &str) -> DeltaError {
    DeltaError::generic(format!(
        "UCCommitter does not support commits that change the table {what}. \
         ALTER TABLE is not supported for catalog-managed tables."
    ))
}
