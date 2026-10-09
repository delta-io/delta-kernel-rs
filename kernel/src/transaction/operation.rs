use std::fmt;

use strum::{EnumIter, IntoEnumIterator};

use crate::crc::is_incremental_safe_operation;

/// Identifies an operation supported by [`UpdateTableTransactionBuilder`].
///
/// Typed variants provide compiler-checked names. [`Custom`](Self::Custom) preserves the protocol's
/// extensibility. Create-table transactions fix their operation internally. Replace-table
/// operations are not supported.
///
/// [`UpdateTableTransactionBuilder`]: super::UpdateTableTransactionBuilder
#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
#[non_exhaustive]
pub enum UpdateTableOperation {
    /// A batch write.
    Write,
    /// A streaming write.
    StreamingUpdate,
    /// A schema-changing operation.
    AlterTable,
    /// A row deletion.
    Delete,
    /// A row update.
    Update,
    /// A merge of source rows into the table.
    Merge,
    /// A data-file reorganization.
    Optimize,
    /// An operation name recorded verbatim in commit history and successful transaction metrics.
    ///
    /// The name must be nonempty and must not exactly match a typed operation's name or a reserved
    /// create/replace-table name. This comparison is case-sensitive. Kernel does not infer
    /// built-in operation semantics from the name, but all other transaction and protocol
    /// checks still apply. Custom operations do not qualify for incremental version checksum
    /// construction.
    ///
    /// Custom names are not guaranteed to remain valid across Kernel versions. Adding a typed
    /// variant reserves its exact name. Callers using that custom name must select the typed
    /// variant before building a transaction.
    #[strum(disabled)]
    Custom(String),
}

impl UpdateTableOperation {
    /// Returns the operation name written to `commitInfo`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Write => "WRITE",
            Self::StreamingUpdate => "STREAMING UPDATE",
            Self::AlterTable => "ALTER TABLE",
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
            Self::Merge => "MERGE",
            Self::Optimize => "OPTIMIZE",
            Self::Custom(operation) => operation,
        }
    }

    pub(crate) fn is_incremental_safe(&self) -> bool {
        match self {
            Self::Custom(_) => false,
            _ => is_incremental_safe_operation(self.as_str()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        let Self::Custom(name) = self else {
            return Ok(());
        };
        validate_custom_name(name)
    }
}

impl fmt::Display for UpdateTableOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CommitOperation {
    CreateTable,
    UpdateTable(UpdateTableOperation),
}

impl CommitOperation {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::CreateTable => "CREATE TABLE",
            Self::UpdateTable(operation) => operation.as_str(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::UpdateTable(operation) => operation.validate(),
            Self::CreateTable => Ok(()),
        }
    }

    pub(crate) fn is_incremental_safe(&self) -> bool {
        match self {
            Self::CreateTable => is_incremental_safe_operation(self.as_str()),
            Self::UpdateTable(operation) => operation.is_incremental_safe(),
        }
    }
}

impl fmt::Display for CommitOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<UpdateTableOperation> for CommitOperation {
    fn from(operation: UpdateTableOperation) -> Self {
        Self::UpdateTable(operation)
    }
}

const RESERVED_TABLE_OPERATION_NAMES: &[&str] = &[
    "CREATE TABLE",
    "REPLACE TABLE",
    "CREATE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE AS SELECT",
];

fn validate_custom_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("custom operation name cannot be empty".to_string());
    }
    if UpdateTableOperation::iter().any(|operation| operation.as_str() == name)
        || RESERVED_TABLE_OPERATION_NAMES.contains(&name)
    {
        return Err(format!(
            "custom operation name '{name}' is reserved; use the matching transaction builder or typed update-table operation"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::{CommitOperation, UpdateTableOperation};

    #[test]
    fn update_table_operations_have_stable_names() {
        let cases = [
            ("WRITE", UpdateTableOperation::Write, true),
            (
                "STREAMING UPDATE",
                UpdateTableOperation::StreamingUpdate,
                true,
            ),
            ("ALTER TABLE", UpdateTableOperation::AlterTable, false),
            ("DELETE", UpdateTableOperation::Delete, true),
            ("UPDATE", UpdateTableOperation::Update, true),
            ("MERGE", UpdateTableOperation::Merge, true),
            ("OPTIMIZE", UpdateTableOperation::Optimize, true),
        ];
        for (name, operation, incremental_safe) in cases {
            assert_eq!(operation.as_str(), name);
            assert_eq!(operation.to_string(), name);
            assert_eq!(operation.is_incremental_safe(), incremental_safe);
            assert_eq!(CommitOperation::from(operation).as_str(), name);
        }
    }

    #[test]
    fn custom_operations_round_trip_exactly() {
        let value = "vendor.custom/write-v2";
        let operation = UpdateTableOperation::Custom(value.to_string());
        assert_eq!(operation, UpdateTableOperation::Custom(value.to_string()));
        assert_eq!(operation.as_str(), value);
        let operation = CommitOperation::from(operation);
        assert_eq!(operation.as_str(), value);
        assert_eq!(
            CommitOperation::from(UpdateTableOperation::Custom(value.to_string())),
            operation
        );
    }

    #[test]
    fn custom_operations_reject_reserved_names() {
        for name in UpdateTableOperation::iter()
            .map(|operation| operation.as_str().to_owned())
            .chain(
                [
                    "CREATE TABLE",
                    "REPLACE TABLE",
                    "CREATE TABLE AS SELECT",
                    "REPLACE TABLE AS SELECT",
                    "CREATE OR REPLACE TABLE AS SELECT",
                ]
                .map(str::to_owned),
            )
        {
            let operation = UpdateTableOperation::Custom(name.to_string());
            assert!(operation.validate().unwrap_err().contains("reserved"));
        }
    }

    #[test]
    fn operation_iteration_excludes_custom() {
        for operation in UpdateTableOperation::iter() {
            assert!(!matches!(operation, UpdateTableOperation::Custom(_)));
            assert!(operation.validate().is_ok());
        }
    }

    #[test]
    fn custom_name_validation_is_case_sensitive_and_preserves_whitespace() {
        for name in ["write", "Write", " WRITE ", "INSERT"] {
            let operation = UpdateTableOperation::Custom(name.to_string());
            assert!(operation.validate().is_ok());
            assert_eq!(operation.as_str(), name);
        }
    }

    #[test]
    fn empty_custom_operation_is_rejected() {
        assert!(UpdateTableOperation::Custom(String::new())
            .validate()
            .unwrap_err()
            .contains("cannot be empty"));
    }

    #[test]
    fn custom_operations_are_never_incremental_safe() {
        for name in ["vendor.custom/write-v2", "CREATE TABLE AS SELECT", "WRITE"] {
            assert!(!UpdateTableOperation::Custom(name.to_string()).is_incremental_safe());
        }
    }

    #[test]
    fn create_table_operations_are_incremental_safe() {
        assert!(CommitOperation::CreateTable.is_incremental_safe());
    }
}
