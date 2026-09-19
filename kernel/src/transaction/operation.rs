use std::fmt;

/// Identifies the high-level operation recorded in a transaction's `commitInfo` action.
///
/// Known operations provide compiler-checked names. [`Custom`](Self::Custom) preserves the
/// protocol's extensibility for connector-specific operations.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Operation {
    CreateTable,
    Write,
    StreamingUpdate,
    ReplaceTable,
    AlterTable,
    Delete,
    Update,
    Merge,
    Optimize,
    Custom(String),
}

impl Operation {
    /// Returns the operation name written to `commitInfo`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::CreateTable => "CREATE TABLE",
            Self::Write => "WRITE",
            Self::StreamingUpdate => "STREAMING UPDATE",
            Self::ReplaceTable => "REPLACE TABLE",
            Self::AlterTable => "ALTER TABLE",
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
            Self::Merge => "MERGE",
            Self::Optimize => "OPTIMIZE",
            Self::Custom(operation) => operation,
        }
    }

    pub(crate) fn metric_label(&self) -> &str {
        match self {
            Self::Custom(_) => "CUSTOM",
            operation => operation.as_str(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        let Self::Custom(name) = self else {
            return Ok(());
        };
        if name.is_empty() {
            return Err("custom operation name cannot be empty".to_string());
        }
        if !matches!(Self::from(name.clone()), Self::Custom(_)) {
            return Err(format!(
                "custom operation name '{name}' is reserved; use its typed Operation variant"
            ));
        }
        Ok(())
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for Operation {
    fn from(operation: String) -> Self {
        match operation.as_str() {
            "CREATE TABLE" => Self::CreateTable,
            "WRITE" => Self::Write,
            "STREAMING UPDATE" => Self::StreamingUpdate,
            "REPLACE TABLE" => Self::ReplaceTable,
            "ALTER TABLE" => Self::AlterTable,
            "DELETE" => Self::Delete,
            "UPDATE" => Self::Update,
            "MERGE" => Self::Merge,
            "OPTIMIZE" => Self::Optimize,
            _ => Self::Custom(operation),
        }
    }
}

impl From<&str> for Operation {
    fn from(operation: &str) -> Self {
        operation.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;

    #[test]
    fn known_operations_parse_to_variants() {
        assert_eq!(Operation::from("WRITE"), Operation::Write);
        assert_eq!(Operation::from("ALTER TABLE"), Operation::AlterTable);
    }

    #[test]
    fn custom_operations_round_trip_exactly() {
        let value = "vendor.custom/write-v2";
        let operation = Operation::from(value);
        assert_eq!(operation, Operation::Custom(value.to_string()));
        assert_eq!(operation.as_str(), value);
        assert_eq!(operation.metric_label(), "CUSTOM");
    }

    #[test]
    fn custom_operations_reject_reserved_names() {
        let operation = Operation::Custom("ALTER TABLE".to_string());
        assert!(operation.validate().unwrap_err().contains("reserved"));
    }
}
