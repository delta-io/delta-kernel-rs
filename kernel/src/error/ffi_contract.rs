//! Typed failures of the connector-facing FFI contract.

/// Invalid arguments or results at the FFI boundary.
#[derive(Debug, thiserror::Error)]
pub enum FfiContractError {
    /// A visitor returned an identifier that does not identify the required object.
    #[error("Invalid {kind} ID {id}")]
    InvalidId {
        /// The kind of object expected.
        kind: &'static str,
        /// The identifier provided by the visitor.
        id: usize,
    },
    /// A numeric tag is outside the supported set.
    #[error("Invalid {field} tag: {value}")]
    InvalidTag {
        /// The tagged field.
        field: &'static str,
        /// The supplied tag.
        value: i64,
    },
    /// A required pointer, field, or callback result is null.
    #[error("{field} must not be null")]
    NullArgument {
        /// The required argument or result.
        field: &'static str,
    },
    /// A required string or collection is empty.
    #[error("{field} must be non-empty")]
    EmptyArgument {
        /// The required argument or result.
        field: &'static str,
    },
    /// A bounded collection exceeds the supported count.
    #[error("{field} count {actual} exceeds max {maximum}")]
    CountExceeded {
        /// The bounded collection.
        field: &'static str,
        /// The supplied count.
        actual: usize,
        /// The largest supported count.
        maximum: usize,
    },
    /// A callback or iterator returned the wrong number of values.
    #[error("{field} returned {actual} values, expected {expected}")]
    CountMismatch {
        /// The callback result or iterator batch.
        field: &'static str,
        /// The required count.
        expected: usize,
        /// The observed count.
        actual: usize,
    },
    /// A configuration option does not have an accepted value.
    #[error("Invalid option {key}={value}: expected {expected}")]
    InvalidOption {
        /// The configuration key.
        key: &'static str,
        /// The supplied value.
        value: String,
        /// The accepted form.
        expected: &'static str,
    },
    /// A single-use stream has already been consumed.
    #[error("incremental scan stream was already consumed")]
    StreamConsumed,
    /// A callback returned without initializing its output slot.
    #[error("Callback {callback} returned without writing a result")]
    CallbackResultUninitialized {
        /// The callback name.
        callback: String,
    },
    /// The FFI commit adapter cannot return a conflicted transaction as a committed version.
    #[error("Commit conflict at version {version}")]
    CommitConflict {
        /// The version at which the commit conflicted.
        version: crate::Version,
    },
}

impl From<FfiContractError> for super::Error {
    fn from(error: FfiContractError) -> Self {
        super::KernelError::from(error).into()
    }
}
