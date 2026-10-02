use std::fmt;

/// Identifies an operation supported by [`UpdateTableTransactionBuilder`].
///
/// Known operations provide compiler-checked names. [`Custom`](Self::Custom) preserves the
/// protocol's extensibility for connector-specific operations. Create-table transactions fix their
/// operation internally; replace-table operations are not supported.
///
/// Builder-specific typing prevents create operations from being selected here:
///
/// ```compile_fail
/// use delta_kernel::transaction::UpdateTableOperation;
///
/// let operation = UpdateTableOperation::CreateTable;
/// ```
///
/// [`UpdateTableTransactionBuilder`]: super::UpdateTableTransactionBuilder
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UpdateTableOperation {
    Write,
    StreamingUpdate,
    AlterTable,
    Delete,
    Update,
    Merge,
    Optimize,
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

    pub(crate) fn validate(&self) -> Result<(), String> {
        let Self::Custom(name) = self else {
            return Ok(());
        };
        validate_custom_name(name)
    }

    fn from_name(operation: String) -> Self {
        match operation.as_str() {
            "WRITE" => Self::Write,
            "STREAMING UPDATE" => Self::StreamingUpdate,
            "ALTER TABLE" => Self::AlterTable,
            "DELETE" => Self::Delete,
            "UPDATE" => Self::Update,
            "MERGE" => Self::Merge,
            "OPTIMIZE" => Self::Optimize,
            _ => Self::Custom(operation),
        }
    }
}

impl fmt::Display for UpdateTableOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CommitOperation {
    Create,
    Replace,
    Update(UpdateTableOperation),
}

impl CommitOperation {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Create => "CREATE TABLE",
            Self::Replace => "REPLACE TABLE",
            Self::Update(operation) => operation.as_str(),
        }
    }

    pub(crate) fn metric_label(&self) -> &str {
        // Connector-defined names collapse to one bounded-cardinality metric label while the
        // exact operation name remains available in commitInfo.
        match self {
            Self::Update(UpdateTableOperation::Custom(_)) => "CUSTOM",
            operation => operation.as_str(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Update(operation) => operation.validate(),
            Self::Create | Self::Replace => Ok(()),
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
        Self::Update(operation)
    }
}

impl From<String> for CommitOperation {
    fn from(operation: String) -> Self {
        match operation.as_str() {
            "CREATE TABLE" => Self::Create,
            "REPLACE TABLE" => Self::Replace,
            _ => Self::Update(UpdateTableOperation::from_name(operation)),
        }
    }
}

impl From<&str> for CommitOperation {
    fn from(operation: &str) -> Self {
        operation.to_string().into()
    }
}

fn validate_custom_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("custom operation name cannot be empty".to_string());
    }
    if matches!(
        name,
        "CREATE TABLE"
            | "WRITE"
            | "STREAMING UPDATE"
            | "REPLACE TABLE"
            | "ALTER TABLE"
            | "DELETE"
            | "UPDATE"
            | "MERGE"
            | "OPTIMIZE"
    ) {
        return Err(format!(
            "custom operation name '{name}' is reserved; use the matching transaction builder or typed update-table operation"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CommitOperation, UpdateTableOperation};

    #[test]
    fn update_table_operations_have_stable_names() {
        let cases = [
            ("WRITE", UpdateTableOperation::Write),
            ("STREAMING UPDATE", UpdateTableOperation::StreamingUpdate),
            ("ALTER TABLE", UpdateTableOperation::AlterTable),
            ("DELETE", UpdateTableOperation::Delete),
            ("UPDATE", UpdateTableOperation::Update),
            ("MERGE", UpdateTableOperation::Merge),
            ("OPTIMIZE", UpdateTableOperation::Optimize),
        ];
        for (name, operation) in cases {
            assert_eq!(operation.as_str(), name);
            assert_eq!(operation.to_string(), name);
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
        assert_eq!(operation.metric_label(), "CUSTOM");
        assert_eq!(CommitOperation::from(value), operation);
    }

    #[test]
    fn custom_operations_reject_reserved_names() {
        for name in ["CREATE TABLE", "REPLACE TABLE", "ALTER TABLE"] {
            let operation = UpdateTableOperation::Custom(name.to_string());
            assert!(operation.validate().unwrap_err().contains("reserved"));
        }
    }

    #[test]
    fn empty_custom_operation_is_rejected() {
        assert!(UpdateTableOperation::Custom(String::new())
            .validate()
            .unwrap_err()
            .contains("cannot be empty"));
    }
}
