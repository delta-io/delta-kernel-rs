//! Definitions of errors that the delta kernel can encounter

mod delta_error;
mod delta_error_conditions;
pub(crate) mod delta_errors;
mod engine_error;
mod ffi_contract;

use std::backtrace::{Backtrace, BacktraceStatus};
use std::convert::Infallible;
use std::num::{ParseIntError, TryFromIntError};
use std::str::Utf8Error;

pub use delta_error::{DeltaError, DeltaErrorParameter};
pub use delta_error_conditions::DeltaErrorCondition;
pub use engine_error::{
    EngineError, EngineResult, EngineResultIterator, EngineResultIteratorStatic,
};
pub use ffi_contract::FfiContractError;

#[cfg(feature = "need-arrow")]
use crate::arrow::error::ArrowError;
pub use crate::commit_range::CommitRangeError;
pub use crate::committer::PublishError;
pub use crate::crc::CrcError;
#[cfg(any(
    feature = "default-engine-base",
    feature = "arrow-conversion",
    feature = "declarative-plans"
))]
pub use crate::engine::ArrowEngineError;
pub use crate::log_compaction::LogCompactionError;
pub use crate::log_segment::LogSegmentError;
#[cfg(feature = "default-engine-base")]
use crate::object_store;
#[cfg(feature = "declarative-plans")]
pub use crate::plans::PlanError;
pub use crate::scan::ScanError;
use crate::schema::{DataType, StructType};
pub use crate::snapshot::SnapshotError;
pub use crate::table_changes::TableChangesError;
use crate::table_properties::ParseIntervalError;
pub use crate::table_properties::TablePropertyError;
use crate::Version;

/// Details of a failed conversion from a scalar into a Rust value.
///
/// Conversion code adds path elements as an error unwinds, producing a path from the outermost
/// value to the value that failed without carrying mutable path state through successful parsing.
#[derive(Debug)]
pub struct ScalarConversionError {
    expected: String,
    actual: String,
    // Stored innermost-first because parent context is appended as conversion errors unwind.
    path: Vec<String>,
}

impl ScalarConversionError {
    pub(crate) fn new(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self {
            expected: expected.into(),
            actual: actual.into(),
            path: Vec::new(),
        }
    }

    fn add_path_context(mut self, element: impl Into<String>) -> Self {
        self.path.push(element.into());
        self
    }

    fn path_string(&self) -> String {
        let mut path = String::new();
        for element in self.path.iter().rev() {
            if !path.is_empty() && !element.starts_with('[') {
                path.push('.');
            }
            path.push_str(element);
        }
        path
    }
}

impl std::fmt::Display for ScalarConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut target = self.path_string();
        if target.is_empty() {
            target.push_str("scalar");
        }
        write!(
            f,
            "Cannot convert {target}: expected {}, found {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for ScalarConversionError {}

/// Adds an outer path element to a scalar conversion error as nested conversion unwinds.
///
/// Other error variants are returned unchanged: a field's `TryFrom<Scalar>` implementation may
/// report a failure unrelated to scalar shape, and this helper must not reclassify it.
pub(crate) fn add_scalar_path_context(error: Error, element: impl Into<String>) -> Error {
    match error {
        Error::Kernel(KernelError::ScalarConversion(error)) => {
            KernelError::ScalarConversion(error.add_path_context(element)).into()
        }
        other => other,
    }
}

/// A [`std::result::Result`] returned by Delta Kernel operations.
pub type DeltaResult<T> = std::result::Result<T, Error>;

/// A boxed, `Send` iterator of [`DeltaResult<T>`] items.
///
/// Convenience alias for the common pattern of returning a streaming, fallible iterator from
/// kernel APIs.
pub type DeltaResultIterator<'a, T> = Box<dyn Iterator<Item = DeltaResult<T>> + Send + 'a>;

/// `'static` counterpart to [`DeltaResultIterator`] for cases where the iterator does not
/// reference borrowed data.
pub type DeltaResultIteratorStatic<T> = DeltaResultIterator<'static, T>;

/// An error returned by a Delta Kernel operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A stable, user-facing Delta failure.
    #[error(transparent)]
    Delta(#[from] DeltaError),

    /// A failure originating in an engine operation.
    #[error(transparent)]
    Engine(#[from] EngineError),

    /// A failure originating in kernel implementation code.
    #[error(transparent)]
    Kernel(#[from] KernelError),
}

/// The operation in which an already classified Kernel error occurred.
#[derive(Debug, thiserror::Error)]
pub enum ErrorContext {
    /// A named operation; the name must not contain caller-provided data.
    #[error("{0}")]
    Operation(&'static str),
    /// A configuration value being parsed or validated.
    #[error("configuration {key}={value:?}")]
    Configuration {
        /// The configuration key.
        key: &'static str,
        /// The supplied value.
        value: String,
    },
    /// A column being resolved or modified.
    #[error("column {path}")]
    Column {
        /// The complete requested column path.
        path: String,
    },
    /// A domain metadata entry being processed.
    #[error("domain metadata {domain}")]
    DomainMetadata {
        /// The domain name.
        domain: String,
    },
    /// A statistics section being constructed or reduced.
    #[error("statistics section {section}")]
    Statistics {
        /// The protocol-defined statistics section name.
        section: &'static str,
    },
    /// A scalar being appended to a row.
    #[error("row {row}, field {field:?}: expected {expected}, got {actual}")]
    Scalar {
        /// The zero-based row index.
        row: usize,
        /// The field name.
        field: String,
        /// The type required by the schema.
        expected: String,
        /// The input scalar's type.
        actual: String,
    },
    /// A SQL literal being parsed.
    #[error("SQL literal {sql:?}")]
    SqlLiteral {
        /// The original SQL text, including whitespace.
        sql: String,
    },
    /// A transaction log commit being processed.
    #[error("commit v={version}")]
    Commit {
        /// The commit version.
        version: Version,
    },
    /// A data file being processed.
    #[error("file {path}")]
    File {
        /// The file path.
        path: String,
    },
    /// Diagnostic text supplied with a recognized, payload-free callback error.
    #[error("callback ({code}): {message}")]
    ForeignCallback {
        /// The original callback code.
        code: i32,
        /// The callback diagnostic.
        message: String,
    },
}

impl Error {
    /// Adds operation context to a Kernel failure, leaving Delta failures unchanged.
    ///
    /// Returns the contextualized error without changing its underlying classification.
    pub fn with_context(self, context: ErrorContext) -> Self {
        match self {
            Self::Kernel(error) => error.with_context(context).into(),
            error => error,
        }
    }
}

/// All the types of errors that the kernel can run into
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum KernelError {
    /// A classified failure accompanied by a backtrace captured when `RUST_BACKTRACE` is enabled.
    /// Native conversions listed in `from_with_backtrace!` capture backtraces while retaining
    /// their typed source. This wrapper does not change the failure's classification.
    #[error("{source}\n{backtrace}")]
    Backtraced {
        source: Box<Self>,
        backtrace: Box<Backtrace>,
    },

    /// An error performing operations on arrow data
    #[cfg(feature = "need-arrow")]
    #[error("{0}")]
    Arrow(#[source] ArrowError),

    #[error("Error writing checkpoint: {0}")]
    CheckpointWrite(String),

    /// User tried to convert engine data to the wrong type
    #[error("Invalid engine data type. Could not convert to {0}")]
    EngineDataType(String),

    /// Could not extract the specified type
    #[error("Error extracting type {0}: {1}")]
    Extract(&'static str, &'static str),

    /// A scalar could not be converted into the requested Rust value.
    #[error(transparent)]
    ScalarConversion(#[from] ScalarConversionError),

    /// Operation context surrounding an already classified Kernel failure.
    #[error("{context}: {source}")]
    Context {
        /// The operation that failed.
        context: ErrorContext,
        /// The classified failure.
        source: Box<Self>,
    },

    /// An integer cannot be represented by the required type.
    #[error("Cannot convert {field} value {value} to {target}: {source}")]
    IntegerConversion {
        /// The field being converted.
        field: &'static str,
        /// The original value.
        value: String,
        /// The required integer type.
        target: &'static str,
        /// The failed checked conversion.
        source: TryFromIntError,
    },

    /// Arithmetic exceeded the representable range.
    #[error("Numeric overflow in {operation}: {value}")]
    NumericOverflow {
        /// The arithmetic operation.
        operation: &'static str,
        /// The input value or operands.
        value: String,
    },

    /// A JSON encoding operation failed.
    #[error("JSON serialization failed during {operation}: {source}")]
    JsonSerialization {
        /// The encoding operation.
        operation: &'static str,
        /// The JSON encoder failure.
        source: serde_json::Error,
    },

    /// A dependency failed to convert or lower an expression.
    #[error("Expression conversion failed during {operation}: {source}")]
    ExpressionConversion {
        /// The expression conversion operation.
        operation: &'static str,
        /// The native conversion failure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A serialized plan message could not be decoded.
    #[cfg(feature = "declarative-plans")]
    #[error("Cannot decode {message_type}: {source}")]
    ProtobufDecode {
        /// The expected message type.
        message_type: &'static str,
        /// The native protobuf decoder failure.
        source: prost::DecodeError,
    },

    /// A shared resource was poisoned by an unwinding thread.
    #[error("Poisoned lock: {resource}")]
    LockPoisoned {
        /// The protected resource.
        resource: &'static str,
    },

    /// A catalog client operation failed.
    #[error("Catalog operation {operation} failed: {source}")]
    CatalogOperation {
        /// The catalog operation.
        operation: &'static str,
        /// The native client failure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The system clock precedes the Unix epoch.
    #[error("System time before Unix epoch: {0}")]
    ClockBeforeEpoch(#[source] std::time::SystemTimeError),

    /// Execution requires a runtime that is not available.
    #[error("Required runtime is unavailable: {source}")]
    RuntimeUnavailable {
        /// The runtime lookup failure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An external runtime operation unwound instead of returning a result.
    #[error("Runtime operation {operation} panicked: {message:?}")]
    RuntimePanic {
        /// The runtime operation.
        operation: &'static str,
        /// The panic message, or `None` for a non-string payload.
        message: Option<String>,
    },

    /// Storage reported a different size than the writer produced.
    #[error("Written file size mismatch: expected {expected}, got {actual}")]
    WrittenFileSizeMismatch {
        /// The size produced by the writer in bytes.
        expected: u64,
        /// The size reported by storage in bytes.
        actual: u64,
    },

    /// A foreign callback returned a failure whose payload is only available as text.
    #[error("Foreign callback error ({code}): {message}")]
    ForeignCallback {
        /// The original callback error code.
        code: i32,
        /// The callback-owned diagnostic copied across the boundary.
        message: String,
    },

    /// A caller violated an FFI argument, visitor, or callback contract.
    #[error("{0}")]
    FfiContract(#[from] FfiContractError),

    /// A tracing subscriber operation failed.
    #[error("Tracing operation {operation} failed: {source}")]
    TracingFailure {
        /// The subscriber operation.
        operation: &'static str,
        /// The subscriber's original failure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A tracing subscriber slot was not initialized.
    #[error("Tracing slot not installed: {slot}")]
    TracingSlotMissing {
        /// The missing subscriber slot.
        slot: &'static str,
    },

    /// Invalid log-segment coverage or replay state.
    #[error("{0}")]
    LogSegment(#[from] crate::log_segment::LogSegmentError),

    /// Invalid scan state or missing scan metadata.
    #[error("{0}")]
    Scan(#[from] crate::scan::ScanError),

    /// Invalid requested commit range or action projection.
    #[error("{0}")]
    CommitRange(#[from] crate::commit_range::CommitRangeError),

    /// Invalid snapshot metadata or publishing state.
    #[error("{0}")]
    Snapshot(#[from] crate::snapshot::SnapshotError),

    /// Invalid change-data file actions or metadata.
    #[error("{0}")]
    TableChanges(#[from] crate::table_changes::TableChangesError),

    /// Invalid catalog publication request.
    #[error("{0}")]
    Publish(#[from] crate::committer::PublishError),

    /// Invalid table property value or feature configuration.
    #[error("{0}")]
    TableProperty(#[from] crate::table_properties::TablePropertyError),

    /// Invalid version-checksum contents or state.
    #[error("{0}")]
    Crc(#[from] crate::crc::CrcError),

    /// Invalid log-compaction version range.
    #[error("{0}")]
    LogCompaction(#[from] crate::log_compaction::LogCompactionError),

    /// Invalid Arrow input or reader state.
    #[cfg(any(
        feature = "default-engine-base",
        feature = "arrow-conversion",
        feature = "declarative-plans"
    ))]
    #[error("{0}")]
    ArrowEngine(#[from] crate::engine::ArrowEngineError),

    /// Invalid relational plan or executor input.
    #[cfg(feature = "declarative-plans")]
    #[error("{0}")]
    Plan(#[from] crate::plans::PlanError),

    /// An error involving the maximum catalog-ratified version when building a snapshot.
    #[error("Max catalog version error: {0}")]
    MaxCatalogVersion(String),

    /// The supplied log tail contains adjacent versions that are not contiguous.
    #[error("Log tail versions {first_version} and {second_version} are not contiguous")]
    LogTailVersionsNotContiguous {
        /// Earlier version in the invalid adjacent pair.
        first_version: Version,
        /// Later version in the invalid adjacent pair.
        second_version: Version,
    },

    /// Some kind of [`std::io::Error`]
    #[error("{0}")]
    IOError(#[source] std::io::Error),

    /// An internal error that means kernel found an unexpected situation, which is likely a bug
    #[error("Internal error {0}. This is a kernel bug, please report.")]
    InternalError(String),

    /// An error enountered while working with parquet data
    #[cfg(feature = "default-engine-base")]
    #[error("Arrow error: {0}")]
    Parquet(#[from] crate::parquet::errors::ParquetError),

    /// An error interacting with the object_store crate
    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    #[cfg(feature = "default-engine-base")]
    #[error("Error interacting with object store: {0}")]
    ObjectStore(#[source] object_store::Error),

    /// An error working with paths from the object_store crate
    #[cfg(feature = "default-engine-base")]
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[cfg(feature = "default-engine-base")]
    #[error("Reqwest Error: {0}")]
    Reqwest(#[from] reqwest::Error),

    /// A specified file could not be found
    #[error("File not found: {0}")]
    FileNotFound(String),

    /// A column was requested, but not found
    #[error("{0}")]
    MissingColumn(String),

    /// The connector-provided partition values are invalid (missing/extra/duplicate keys,
    /// or a value type does not match the schema column type).
    #[error("Invalid partition values: {0}")]
    InvalidPartitionValues(String),

    /// A column was specified with a specific type, but it is not of that type
    #[error("Expected column type: {0}")]
    UnexpectedColumnType(String),

    /// Data was expected, but not found
    #[error("Expected is missing: {0}")]
    MissingData(String),

    /// A version for the delta table could not be found in the log
    #[error("No table version found.")]
    MissingVersion,

    /// An error occurred while working with deletion vectors
    #[error("Deletion Vector error: {0}")]
    DeletionVector(String),

    /// A selection vector is larger than data length
    #[error("Selection vector is larger than data length: {0}")]
    InvalidSelectionVector(String),

    /// Transaction state is invalid for the requested operation
    #[error("Invalid transaction state: {0}")]
    InvalidTransactionState(String),

    /// A specified URL was invalid
    #[error("Invalid url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// serde encountered malformed json
    #[error("{0}")]
    MalformedJson(#[source] serde_json::Error),

    /// There was no metadata action in the delta log
    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    /// There was no protocol action in the delta log
    #[error("No protocol found in delta log.")]
    MissingProtocol,

    /// Invalid protocol action was read from the log
    #[error("Invalid protocol action in the delta log: {0}")]
    InvalidProtocol(String),

    /// Neither metadata nor protocol could be found in the delta log
    #[error("No table metadata or protocol found in delta log.")]
    MissingMetadataAndProtocol,

    /// A string failed to parse as the specified data type
    #[error("Failed to parse value '{0}' as '{1}'")]
    ParseError(String, DataType),

    /// A tokio executor failed to join a task
    #[error("Join failure: {0}")]
    JoinFailure(String),

    /// Could not convert to string from utf-8
    #[error("Could not convert to string from utf-8: {0}")]
    Utf8Error(#[from] Utf8Error),

    /// Could not parse an integer
    #[error("Could not parse int: {0}")]
    ParseIntError(#[from] ParseIntError),

    #[error("Invalid column mapping mode: {0}")]
    InvalidColumnMappingMode(String),

    /// Asked for a table at an invalid location
    #[error("Invalid table location: {0}.")]
    InvalidTableLocation(String),

    /// Precision or scale not compliant with delta specification
    #[error("Invalid decimal: {0}")]
    InvalidDecimal(String),

    /// Invalid CRS or other parameter for a Geometry / Geography type
    #[error("Invalid geo parameters: {0}")]
    InvalidGeoParams(String),

    /// Inconsistent data passed to struct scalar
    #[error("Invalid struct data: {0}")]
    InvalidStructData(String),

    /// Expressions did not parse or evaluate correctly
    #[error("Invalid expression evaluation: {0}")]
    InvalidExpressionEvaluation(String),

    /// Unable to parse the name of a log path
    #[error("Invalid log path: {0}")]
    InvalidLogPath(String),

    /// The file already exists at the path, prohibiting a non-overwrite write
    #[error("File already exists: {0}")]
    FileAlreadyExists(String),

    /// Some functionality is currently unsupported
    #[error("Unsupported: {0}")]
    Unsupported(String),

    /// Cannot write a version checksum (CRC) file for this snapshot
    #[error("Checksum write unsupported: {0}")]
    ChecksumWriteUnsupported(String),

    /// Parsing error when attempting to deserialize an interval
    #[error(transparent)]
    ParseIntervalError(#[from] ParseIntervalError),

    #[error("Change data feed is unsupported for the table at version {0}")]
    ChangeDataFeedUnsupported(Version),

    /// Row tracking (`delta.enableRowTracking`) must be enabled for the entire version range of a
    /// row-tracking change feed, but it is not enabled at the given version.
    #[error(
        "Row tracking (delta.enableRowTracking) must be enabled for the entire row-tracking change \
         feed range, but it is not enabled at version {0}"
    )]
    RowTrackingChangeFeedUnsupported(Version),

    #[error("Change data feed encountered incompatible schema. Expected {0}, got {1}")]
    ChangeDataFeedIncompatibleSchema(String, String),

    /// Invalid checkpoint files
    #[error("Invalid Checkpoint: {0}")]
    InvalidCheckpoint(String),

    /// Error while transforming a schema + leaves into an Expression of literals
    #[error(transparent)]
    LiteralExpressionTransformError(
        #[from] crate::expressions::literal_expression_transform::Error,
    ),

    /// Schema mismatch has occurred or invalid/not-kernel-supported schema used somewhere
    #[error("Schema error: {0}")]
    Schema(String),

    /// Validation error for file statistics (e.g., missing required clustering column stats)
    #[error("Stats validation error: {0}")]
    StatsValidation(String),

    /// Error during log history operations (timestamp queries, version lookups)
    #[error(transparent)]
    LogHistory(#[from] Box<crate::history_manager::error::LogHistoryError>),

    #[cfg(feature = "declarative-plans")]
    #[error("Declarative plan execution yielded the incorrect type: expected PlanResult::{expected}, got PlanResult::{actual}")]
    PlanResultTypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },

    /// The operation was cancelled via a [`CancellationToken`](crate::CancellationToken).
    ///
    /// Surfaced by cancellation-aware reads as a terminal error, distinct from normal iterator
    /// exhaustion. See [`CancellableIterator`](crate::cancellation) for the enforced contract.
    #[error("Operation cancelled")]
    Cancelled,
}

impl KernelError {
    pub(crate) fn scalar_conversion(
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        ScalarConversionError::new(expected, actual).into()
    }

    pub(crate) fn checkpoint_write(msg: impl ToString) -> Self {
        Self::CheckpointWrite(msg.to_string())
    }

    /// Wraps this classified failure with the supplied operation context.
    ///
    /// Returns an error retaining this error as its source.
    pub fn with_context(self, context: ErrorContext) -> Self {
        Self::Context {
            context,
            source: Box::new(self),
        }
    }

    /// Describes a checked integer conversion failure for `field` and its original `value`.
    ///
    /// Returns an error retaining `source` and the required `target` type.
    pub fn integer_conversion(
        field: &'static str,
        value: impl ToString,
        target: &'static str,
        source: TryFromIntError,
    ) -> Self {
        Self::IntegerConversion {
            field,
            value: value.to_string(),
            target,
            source,
        }
    }
    pub fn file_not_found(path: impl ToString) -> Self {
        Self::FileNotFound(path.to_string())
    }
    pub fn missing_column(name: impl ToString) -> Self {
        Self::MissingColumn(name.to_string()).with_backtrace()
    }
    pub fn unexpected_column_type(name: impl ToString) -> Self {
        Self::UnexpectedColumnType(name.to_string())
    }
    pub fn invalid_partition_values(msg: impl ToString) -> Self {
        Self::InvalidPartitionValues(msg.to_string())
    }
    pub fn missing_data(name: impl ToString) -> Self {
        Self::MissingData(name.to_string())
    }
    pub fn deletion_vector(msg: impl ToString) -> Self {
        Self::DeletionVector(msg.to_string())
    }
    pub fn engine_data_type(msg: impl ToString) -> Self {
        Self::EngineDataType(msg.to_string())
    }
    pub fn join_failure(msg: impl ToString) -> Self {
        Self::JoinFailure(msg.to_string())
    }
    pub fn invalid_table_location(location: impl ToString) -> Self {
        Self::InvalidTableLocation(location.to_string())
    }
    pub fn invalid_column_mapping_mode(mode: impl ToString) -> Self {
        Self::InvalidColumnMappingMode(mode.to_string())
    }
    pub fn invalid_decimal(msg: impl ToString) -> Self {
        Self::InvalidDecimal(msg.to_string())
    }
    #[cfg(feature = "geo-type-in-dev")]
    pub fn invalid_geo_params(msg: impl ToString) -> Self {
        Self::InvalidGeoParams(msg.to_string())
    }
    pub fn invalid_struct_data(msg: impl ToString) -> Self {
        Self::InvalidStructData(msg.to_string())
    }
    pub fn invalid_expression(msg: impl ToString) -> Self {
        Self::InvalidExpressionEvaluation(msg.to_string())
    }
    pub(crate) fn invalid_log_path(msg: impl ToString) -> Self {
        Self::InvalidLogPath(msg.to_string())
    }

    pub fn internal_error(msg: impl ToString) -> Self {
        Self::InternalError(msg.to_string()).with_backtrace()
    }

    pub fn invalid_protocol(msg: impl ToString) -> Self {
        Self::InvalidProtocol(msg.to_string())
    }

    pub fn invalid_transaction_state(msg: impl ToString) -> Self {
        Self::InvalidTransactionState(msg.to_string())
    }

    pub fn unsupported(msg: impl ToString) -> Self {
        Self::Unsupported(msg.to_string())
    }
    pub fn change_data_feed_unsupported(version: impl Into<Version>) -> Self {
        Self::ChangeDataFeedUnsupported(version.into())
    }
    /// Creates an [`KernelError::RowTrackingChangeFeedUnsupported`] for the given version, used
    /// when row tracking is not enabled at some point in a row-tracking change feed's version
    /// range.
    pub(crate) fn row_tracking_change_feed_unsupported(version: impl Into<Version>) -> Self {
        Self::RowTrackingChangeFeedUnsupported(version.into())
    }
    pub(crate) fn change_data_feed_incompatible_schema(
        expected: &StructType,
        actual: &StructType,
    ) -> Self {
        Self::ChangeDataFeedIncompatibleSchema(expected.to_string(), actual.to_string())
    }

    /// Creates an incompatible-schema error that identifies the version of `actual`.
    pub(crate) fn change_data_feed_incompatible_schema_at_version(
        expected: &StructType,
        actual: &StructType,
        version: Version,
    ) -> Self {
        Self::ChangeDataFeedIncompatibleSchema(
            expected.to_string(),
            format!("schema at version {version}: {actual}"),
        )
    }

    pub fn invalid_checkpoint(msg: impl ToString) -> Self {
        Self::InvalidCheckpoint(msg.to_string())
    }

    pub fn schema(msg: impl ToString) -> Self {
        Self::Schema(msg.to_string())
    }

    pub fn stats_validation(msg: impl ToString) -> Self {
        Self::StatsValidation(msg.to_string())
    }

    #[cfg(feature = "declarative-plans")]
    pub fn plan_result_type_mismatch(expected: &'static str, actual: &'static str) -> Self {
        Self::PlanResultTypeMismatch { expected, actual }
    }

    /// Returns this error with a captured backtrace when backtrace capture is enabled.
    /// Otherwise returns it unchanged.
    #[must_use]
    pub fn with_backtrace(self) -> Self {
        let backtrace = Backtrace::capture();
        match backtrace.status() {
            BacktraceStatus::Captured => Self::Backtraced {
                source: Box::new(self),
                backtrace: Box::new(backtrace),
            },
            _ => self,
        }
    }
}

macro_rules! from_with_backtrace(
    ( $(($error_type: ty, $error_variant: ident)), * ) => {
        $(
            impl From<$error_type> for KernelError {
                fn from(value: $error_type) -> Self {
                    Self::$error_variant(value).with_backtrace()
                }
            }
        )*
    };
);

from_with_backtrace!(
    (serde_json::Error, MalformedJson),
    (std::io::Error, IOError)
);

#[cfg(feature = "need-arrow")]
impl From<ArrowError> for KernelError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value).with_backtrace()
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::Error> for KernelError {
    fn from(value: object_store::Error) -> Self {
        match value {
            object_store::Error::NotFound { path, .. } => Self::file_not_found(path),
            err => Self::ObjectStore(err),
        }
    }
}

/// Allows `?` to propagate infallible conversions into a [`DeltaResult`].
impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

/// Allows `?` to propagate infallible conversions into kernel-only results.
impl From<Infallible> for KernelError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn operation_context_retains_io_source_and_classification() {
        let error = KernelError::IOError(std::io::Error::other("disk unavailable"))
            .with_context(ErrorContext::Commit { version: 42 });
        let KernelError::Context { context, source } = &error else {
            panic!("expected operation context");
        };
        assert!(matches!(context, ErrorContext::Commit { version: 42 }));
        assert!(matches!(source.as_ref(), KernelError::IOError(_)));
        let native = source
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(native.kind(), std::io::ErrorKind::Other);
        assert_eq!(native.to_string(), "disk unavailable");
    }

    #[test]
    fn delta_errors_are_not_reclassified_by_context() {
        let error = delta_errors::table_not_found("memory:///missing/")
            .with_context(ErrorContext::Operation("load snapshot"));
        assert!(matches!(error, Error::Delta(_)));
    }

    #[test]
    fn integer_conversion_preserves_original_value_and_native_source() {
        let source = u64::try_from(-1i64).unwrap_err();
        let error = KernelError::integer_conversion("size", -1, "u64", source);
        assert!(matches!(
            &error,
            KernelError::IntegerConversion { field: "size", value, target: "u64", .. }
                if value == "-1"
        ));
        assert!(error.source().unwrap().is::<TryFromIntError>());
    }

    #[test]
    fn json_source_survives_top_level_error_conversion() {
        let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let error = Error::from(KernelError::MalformedJson(source));
        assert!(error.source().unwrap().is::<serde_json::Error>());
    }
}
