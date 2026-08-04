//! Structured errors returned by Delta Kernel.

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;

#[cfg(feature = "default-engine-base")]
use crate::arrow::error::ArrowError;
#[cfg(feature = "default-engine-base")]
use crate::object_store;
use crate::schema::{DataType, StructType};
use crate::table_properties::ParseIntervalError;
use crate::Version;

/// A result whose error distinguishes Delta failures from engine failures.
pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

/// A result returned by an engine implementation.
pub type EngineResult<T> = std::result::Result<T, EngineError>;

/// A boxed, `Send` iterator of engine-produced results.
pub type EngineResultIterator<'a, T> = Box<dyn Iterator<Item = EngineResult<T>> + Send + 'a>;

/// `'static` counterpart to [`EngineResultIterator`].
pub type EngineResultIteratorStatic<T> = EngineResultIterator<'static, T>;

/// A boxed error suitable for crossing kernel module boundaries.
pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// A boxed, `Send` iterator of [`DeltaResult<T>`] items.
pub type DeltaResultIterator<'a, T> = Box<dyn Iterator<Item = DeltaResult<T>> + Send + 'a>;

/// `'static` counterpart to [`DeltaResultIterator`].
pub type DeltaResultIteratorStatic<T> = DeltaResultIterator<'static, T>;

macro_rules! compatibility_constructors {
    ($( $snake:ident => $capitalized:ident : $kind:ident ),+ $(,)?) => {
        $(
            #[allow(dead_code)]
            pub(crate) fn $snake(message: impl ToString) -> Self {
                legacy(LegacyErrorKind::$kind, message.to_string())
            }

            #[allow(dead_code)]
            #[allow(non_snake_case)]
            pub(crate) fn $capitalized(message: String) -> Self {
                legacy(LegacyErrorKind::$kind, message)
            }
        )+
    };
}

/// An unrecoverable error returned by Delta Kernel.
///
/// Recoverable outcomes are represented by successful result variants. An error therefore has
/// exactly one of two origins: Delta Kernel or the connector-provided engine.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A failure interpreting or operating on a Delta table.
    #[error(transparent)]
    Delta(#[from] DeltaError),

    /// A failure produced by the connector-provided engine.
    #[error(transparent)]
    Engine(#[from] EngineError),
}

impl Error {
    /// Returns the structured Delta error, if this error originated in Delta Kernel.
    pub fn as_delta_error(&self) -> Option<&DeltaError> {
        match self {
            Self::Delta(error) => Some(error),
            Self::Engine(_) => None,
        }
    }

    /// Returns the structured engine error, if this error originated in the engine.
    pub fn as_engine_error(&self) -> Option<&EngineError> {
        match self {
            Self::Delta(_) => None,
            Self::Engine(error) => Some(error),
        }
    }

    /// Returns whether this error reports a missing file.
    pub fn is_file_not_found(&self) -> bool {
        self.as_engine_error()
            .is_some_and(EngineError::is_file_not_found)
    }

    /// Returns whether this error reports an existing destination file.
    pub fn is_file_already_exists(&self) -> bool {
        self.as_engine_error()
            .is_some_and(EngineError::is_file_already_exists)
    }

    /// Returns whether this error reports cancellation.
    pub fn is_cancelled(&self) -> bool {
        self.as_engine_error()
            .is_some_and(|error| error.kind() == EngineErrorKind::Cancelled)
    }

    /// Returns the legacy V1 category used by compatibility bindings.
    #[doc(hidden)]
    pub fn legacy_error_kind(&self) -> Option<&'static str> {
        let kind = match self {
            Self::Delta(error) => error
                .legacy_kind
                .or_else(|| legacy_kind_from_source(error.source())),
            Self::Engine(error) => legacy_kind_from_source(error.source()),
        };
        kind.map(LegacyErrorKind::as_str)
    }

    #[cfg(test)]
    pub(crate) fn is_unsupported(&self) -> bool {
        self.as_delta_error().is_some_and(|error| {
            matches!(
                error.code(),
                DeltaErrorCode::DeltaUnsupportedFeaturesForRead
                    | DeltaErrorCode::DeltaUnsupportedFeaturesForWrite
            )
        }) || self.legacy_kind() == Some(LegacyErrorKind::Unsupported)
    }

    #[cfg(test)]
    pub(crate) fn is_invalid_protocol(&self) -> bool {
        self.as_delta_error()
            .is_some_and(|error| error.code() == DeltaErrorCode::DeltaInvalidProtocolVersion)
            || self.legacy_kind() == Some(LegacyErrorKind::InvalidProtocol)
    }

    pub(crate) fn is_parse_error(&self) -> bool {
        (match self {
            Self::Delta(error) => error
                .legacy_kind
                .or_else(|| legacy_kind_from_source(error.source())),
            Self::Engine(error) => legacy_kind_from_source(error.source()),
        }) == Some(LegacyErrorKind::Parse)
    }

    pub(crate) fn is_io_error(&self) -> bool {
        self.as_engine_error()
            .is_some_and(|error| source_chain_contains::<std::io::Error>(StdError::source(error)))
    }

    /// Attaches a diagnostic source to a Delta-originated error.
    pub(crate) fn with_delta_source(self, source: impl Into<BoxedError>) -> Self {
        match self {
            Self::Delta(error) => Self::Delta(error.with_boxed_source(source.into())),
            Self::Engine(error) => Self::Engine(error),
        }
    }

    /// Adds diagnostic context without discarding an unclassified Delta error's source chain.
    pub(crate) fn with_unclassified_context(self, message: impl Into<String>) -> Self {
        let Some(error) = self.as_delta_error() else {
            return self;
        };
        if error.code() != DeltaErrorCode::DeltaKernelUnclassified {
            return self;
        }

        let legacy_kind = self.legacy_kind();
        let mut contextual = DeltaError::new(DeltaErrorCode::DeltaKernelUnclassified, vec![])
            .with_boxed_source(Box::new(ContextError {
                message: message.into(),
                source: self,
            }));
        contextual.legacy_kind = legacy_kind;
        Self::Delta(contextual)
    }

    fn legacy_kind(&self) -> Option<LegacyErrorKind> {
        let error = self.as_delta_error()?;
        error
            .legacy_kind
            .or_else(|| legacy_kind_from_source(error.source()))
    }

    pub(crate) fn legacy_message(&self) -> Option<&str> {
        self.as_delta_error()?
            .source
            .as_deref()?
            .downcast_ref::<LegacyError>()
            .map(|error| error.message.as_str())
    }

    pub(crate) fn missing_metadata() -> Self {
        legacy(
            LegacyErrorKind::MissingMetadata,
            "No table metadata found in delta log.".to_string(),
        )
    }

    pub(crate) fn missing_protocol() -> Self {
        legacy(
            LegacyErrorKind::MissingProtocol,
            "No protocol found in delta log.".to_string(),
        )
    }

    pub(crate) fn missing_metadata_and_protocol() -> Self {
        legacy(
            LegacyErrorKind::MissingMetadataAndProtocol,
            "No table metadata or protocol found in delta log.".to_string(),
        )
    }

    /// Creates a generic, unclassified Delta error while preserving the supplied source.
    pub fn generic_err(source: impl Into<BoxedError>) -> Self {
        delta_errors::kernel_unclassified_boxed(source.into())
    }

    /// Creates a generic, unclassified Delta error.
    pub fn generic(message: impl ToString) -> Self {
        legacy(LegacyErrorKind::Generic, message.to_string())
    }

    #[allow(non_snake_case)]
    pub(crate) fn Generic(message: String) -> Self {
        Self::generic(message)
    }

    /// Creates a missing-file engine error.
    pub fn file_not_found(path: impl Into<String>) -> Self {
        EngineError::file_not_found(path).into()
    }

    #[allow(non_snake_case)]
    #[allow(dead_code)]
    pub(crate) fn FileNotFound(path: String) -> Self {
        Self::file_not_found(path)
    }

    /// Creates an existing-file engine error.
    pub fn file_already_exists(path: impl Into<String>) -> Self {
        EngineError::file_already_exists(path).into()
    }

    #[allow(non_snake_case)]
    #[allow(dead_code)]
    pub(crate) fn FileAlreadyExists(path: String) -> Self {
        Self::file_already_exists(path)
    }

    #[allow(non_snake_case)]
    pub(crate) fn MalformedJson(error: serde_json::Error) -> Self {
        error.into()
    }

    #[allow(non_snake_case)]
    #[allow(dead_code)]
    pub(crate) fn IOError(error: std::io::Error) -> Self {
        error.into()
    }

    /// Creates a corrupt-data engine error.
    pub fn corrupt_data(message: impl Into<String>) -> Self {
        EngineError::corrupt_data(message).into()
    }

    /// Creates a cancelled engine error.
    pub fn cancelled() -> Self {
        EngineError::cancelled().into()
    }

    /// Creates an unclassified engine error.
    pub fn engine(message: impl Into<String>) -> Self {
        EngineError::other(message).into()
    }

    /// Retains source compatibility with message-only error construction sites.
    #[must_use]
    pub fn with_backtrace(self) -> Self {
        self
    }

    compatibility_constructors! {
        checkpoint_write => CheckpointWrite: CheckpointWrite,
        max_catalog_version => MaxCatalogVersion: Generic,
        engine_data_type => EngineDataType: EngineDataType,
        join_failure => JoinFailure: JoinFailure,
        missing_column => MissingColumn: MissingColumn,
        unexpected_column_type => UnexpectedColumnType: UnexpectedColumnType,
        invalid_partition_values => InvalidPartitionValues: Other,
        missing_data => MissingData: MissingData,
        deletion_vector => DeletionVector: DeletionVector,
        invalid_selection_vector => InvalidSelectionVector: Other,
        invalid_table_location => InvalidTableLocation: InvalidTableLocation,
        invalid_column_mapping_mode => InvalidColumnMappingMode: InvalidColumnMappingMode,
        invalid_decimal => InvalidDecimal: InvalidDecimal,
        invalid_struct_data => InvalidStructData: InvalidStructData,
        invalid_expression => InvalidExpressionEvaluation: InvalidExpression,
        invalid_log_path => InvalidLogPath: InvalidLogPath,
        internal_error => InternalError: Internal,
        invalid_transaction_state => InvalidTransactionState: Other,
        invalid_checkpoint => InvalidCheckpoint: InvalidCheckpoint,
        stats_validation => StatsValidation: Other,
    }

    /// Creates the legacy schema-validation fallback used by compatibility bindings.
    #[doc(hidden)]
    pub fn schema(message: impl ToString) -> Self {
        legacy(LegacyErrorKind::Schema, message.to_string())
    }

    #[allow(non_snake_case)]
    pub(crate) fn Schema(message: String) -> Self {
        Self::schema(message)
    }

    /// Creates the legacy invalid-protocol fallback used by compatibility callers.
    pub fn invalid_protocol(message: impl ToString) -> Self {
        legacy(LegacyErrorKind::InvalidProtocol, message.to_string())
    }

    #[allow(non_snake_case)]
    pub(crate) fn InvalidProtocol(message: String) -> Self {
        Self::invalid_protocol(message)
    }

    /// Creates the legacy unsupported-operation fallback used by compatibility callers.
    pub fn unsupported(message: impl ToString) -> Self {
        legacy(LegacyErrorKind::Unsupported, message.to_string())
    }

    #[allow(non_snake_case)]
    #[allow(dead_code)]
    pub(crate) fn Unsupported(message: String) -> Self {
        Self::unsupported(message)
    }

    /// Creates the legacy change-data-feed unsupported error.
    pub fn change_data_feed_unsupported(version: impl Into<Version>) -> Self {
        legacy(
            LegacyErrorKind::CdfUnsupported,
            format!(
                "Change data feed is unsupported for the table at version {}",
                version.into()
            ),
        )
    }

    /// Creates the legacy row-tracking change-feed error used by compatibility callers.
    pub(crate) fn row_tracking_change_feed_unsupported(version: impl Into<Version>) -> Self {
        legacy(
            LegacyErrorKind::RowTrackingCdfUnsupported,
            format!(
                "Row tracking change feed is unsupported at version {}",
                version.into()
            ),
        )
    }

    /// Creates the legacy incompatible CDF schema error used by compatibility callers.
    pub(crate) fn change_data_feed_incompatible_schema(
        expected: &StructType,
        actual: &StructType,
    ) -> Self {
        legacy(
            LegacyErrorKind::CdfIncompatibleSchema,
            format!(
            "Change data feed encountered incompatible schema. Expected {expected}, got {actual}"
        ),
        )
    }

    /// Creates the legacy incompatible CDF schema error with version context.
    pub(crate) fn change_data_feed_incompatible_schema_at_version(
        expected: &StructType,
        actual: &StructType,
        version: Version,
    ) -> Self {
        legacy(LegacyErrorKind::CdfIncompatibleSchema, format!(
            "Change data feed encountered incompatible schema. Expected {expected}, got schema at version {version}: {actual}"
        ))
    }

    #[allow(non_snake_case)]
    pub(crate) fn ChecksumWriteUnsupported(message: String) -> Self {
        legacy(LegacyErrorKind::Other, message)
    }

    #[allow(non_snake_case)]
    pub(crate) fn ParseError(value: String, data_type: DataType) -> Self {
        legacy(
            LegacyErrorKind::Parse,
            format!("Failed to parse value '{value}' as '{data_type}'"),
        )
    }

    #[allow(non_snake_case)]
    pub(crate) fn LogHistory(error: Box<crate::history_manager::error::LogHistoryError>) -> Self {
        error.into()
    }

    #[cfg(feature = "default-engine-base")]
    #[allow(non_snake_case)]
    pub(crate) fn Arrow(error: ArrowError) -> Self {
        error.into()
    }

    #[cfg(feature = "geo-type-in-dev")]
    /// Creates an unclassified Delta error for invalid geospatial type parameters.
    pub fn invalid_geo_params(message: impl ToString) -> Self {
        legacy(LegacyErrorKind::Other, message.to_string())
    }

    #[cfg(feature = "declarative-plans")]
    /// Creates an unclassified Delta error for a declarative-plan result type mismatch.
    pub fn plan_result_type_mismatch(expected: &'static str, actual: &'static str) -> Self {
        legacy(LegacyErrorKind::Other, format!(
            "Declarative plan execution yielded the incorrect type: expected PlanResult::{expected}, got PlanResult::{actual}"
        ))
    }
}

/// Stable, string-identified Delta error conditions.
///
/// Enum layout and discriminant values are deliberately unspecified. Consumers must persist or
/// transmit [`Self::condition`] rather than casting this enum to an integer.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeltaErrorCode {
    /// A Delta log already exists at the requested creation path.
    DeltaLogAlreadyExists,
    /// A requested table version is outside the available version range.
    DeltaVersionNotFound,
    /// A multipart checkpoint is missing one or more part files.
    DeltaMissingPartFiles,
    /// Protocol or metadata state could not be reconstructed.
    DeltaStateRecoverError,
    /// A table requires an unsupported protocol version.
    DeltaInvalidProtocolVersion,
    /// A table requires unsupported reader features.
    DeltaUnsupportedFeaturesForRead,
    /// A table requires unsupported writer features.
    DeltaUnsupportedFeaturesForWrite,
    /// Delta log versions needed to reconstruct a table are not contiguous.
    DeltaVersionsNotContiguous,
    /// A kernel failure without a specific Delta condition.
    DeltaKernelUnclassified,
}

impl DeltaErrorCode {
    /// Returns the stable string identity of this condition.
    pub const fn condition(self) -> &'static str {
        match self {
            Self::DeltaLogAlreadyExists => "DELTA_LOG_ALREADY_EXISTS",
            Self::DeltaVersionNotFound => "DELTA_VERSION_NOT_FOUND",
            Self::DeltaMissingPartFiles => "DELTA_MISSING_PART_FILES",
            Self::DeltaStateRecoverError => "DELTA_STATE_RECOVER_ERROR",
            Self::DeltaInvalidProtocolVersion => "DELTA_INVALID_PROTOCOL_VERSION",
            Self::DeltaUnsupportedFeaturesForRead => "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
            Self::DeltaUnsupportedFeaturesForWrite => "DELTA_UNSUPPORTED_FEATURES_FOR_WRITE",
            Self::DeltaVersionsNotContiguous => "DELTA_VERSIONS_NOT_CONTIGUOUS",
            Self::DeltaKernelUnclassified => "DELTA_KERNEL_UNCLASSIFIED",
        }
    }

    /// Returns the SQLSTATE associated with this condition, when defined.
    pub const fn sql_state(self) -> Option<&'static str> {
        match self {
            Self::DeltaLogAlreadyExists => Some("42K04"),
            Self::DeltaVersionNotFound => Some("22003"),
            Self::DeltaMissingPartFiles => Some("42KD6"),
            Self::DeltaStateRecoverError => Some("XXKDS"),
            Self::DeltaInvalidProtocolVersion => Some("KD004"),
            Self::DeltaUnsupportedFeaturesForRead | Self::DeltaUnsupportedFeaturesForWrite => {
                Some("56038")
            }
            Self::DeltaVersionsNotContiguous => Some("KD00C"),
            Self::DeltaKernelUnclassified => None,
        }
    }

    /// Returns the ordered names of this condition's message parameters.
    pub const fn parameter_names(self) -> &'static [&'static str] {
        match self {
            Self::DeltaLogAlreadyExists => &["path"],
            Self::DeltaVersionNotFound => &["userVersion", "earliest", "latest"],
            Self::DeltaMissingPartFiles => &["version"],
            Self::DeltaStateRecoverError => &["operation", "version"],
            Self::DeltaInvalidProtocolVersion => &[
                "tableNameOrPath",
                "readerRequired",
                "writerRequired",
                "deltaVersion",
                "supportedReaders",
                "supportedWriters",
            ],
            Self::DeltaUnsupportedFeaturesForRead | Self::DeltaUnsupportedFeaturesForWrite => {
                &["tableNameOrPath", "deltaVersion", "unsupported"]
            }
            Self::DeltaVersionsNotContiguous => {
                &["versionList", "startVersion", "endVersion", "versionToLoad"]
            }
            Self::DeltaKernelUnclassified => &[],
        }
    }

    pub(crate) const fn message_template(self) -> &'static str {
        match self {
            Self::DeltaLogAlreadyExists => "A Delta log already exists at <path>.",
            Self::DeltaVersionNotFound => "Cannot time travel Delta table to version <userVersion>. Available versions: [<earliest>, <latest>].",
            Self::DeltaMissingPartFiles => "Couldn't find all part files of the checkpoint version: <version>.",
            Self::DeltaStateRecoverError => "The <operation> of your Delta table could not be recovered while reconstructing version <version>. Did you manually delete files in the _delta_log directory?",
            Self::DeltaInvalidProtocolVersion => "Unsupported Delta protocol version: table \"<tableNameOrPath>\" requires reader version <readerRequired> and writer version <writerRequired>, but Delta Lake \"<deltaVersion>\" supports reader versions <supportedReaders> and writer versions <supportedWriters>. Please upgrade to a newer release.",
            Self::DeltaUnsupportedFeaturesForRead => "Unsupported Delta read feature: table \"<tableNameOrPath>\" requires reader table feature(s) that are unsupported by Delta Lake \"<deltaVersion>\": <unsupported>.",
            Self::DeltaUnsupportedFeaturesForWrite => "Unsupported Delta write feature: table \"<tableNameOrPath>\" requires writer table feature(s) that are unsupported by Delta Lake \"<deltaVersion>\": <unsupported>.",
            Self::DeltaVersionsNotContiguous => "Versions (<versionList>) are not contiguous. A gap in the Delta log between versions <startVersion> and <endVersion> was detected while trying to load version <versionToLoad>.",
            Self::DeltaKernelUnclassified => "An unclassified Delta Kernel error occurred.",
        }
    }
}

#[cfg(test)]
const ALL_DELTA_ERROR_CODES: &[DeltaErrorCode] = &[
    DeltaErrorCode::DeltaLogAlreadyExists,
    DeltaErrorCode::DeltaVersionNotFound,
    DeltaErrorCode::DeltaMissingPartFiles,
    DeltaErrorCode::DeltaStateRecoverError,
    DeltaErrorCode::DeltaInvalidProtocolVersion,
    DeltaErrorCode::DeltaUnsupportedFeaturesForRead,
    DeltaErrorCode::DeltaUnsupportedFeaturesForWrite,
    DeltaErrorCode::DeltaVersionsNotContiguous,
    DeltaErrorCode::DeltaKernelUnclassified,
];

/// A named message parameter carried by a [`DeltaError`].
#[derive(Debug, Eq, PartialEq)]
pub struct DeltaErrorParameter {
    name: &'static str,
    value: String,
}

impl DeltaErrorParameter {
    fn new(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
        }
    }

    /// Returns this parameter's stable name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns this parameter's display value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// A structured, user-facing Delta error.
///
/// The condition string, SQLSTATE, and parameter names are stable API. Rendered message wording is
/// diagnostic and can change.
pub struct DeltaError {
    code: DeltaErrorCode,
    parameters: Box<[DeltaErrorParameter]>,
    legacy_kind: Option<LegacyErrorKind>,
    source: Option<BoxedError>,
    backtrace: Backtrace,
}

impl DeltaError {
    fn new(code: DeltaErrorCode, parameters: Vec<DeltaErrorParameter>) -> Self {
        Self {
            code,
            parameters: parameters.into_boxed_slice(),
            legacy_kind: None,
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    fn with_legacy_kind(mut self, kind: LegacyErrorKind) -> Self {
        self.legacy_kind = Some(kind);
        self
    }

    fn with_boxed_source(mut self, source: BoxedError) -> Self {
        self.source = Some(source);
        self
    }

    /// Returns this error's typed condition.
    pub fn code(&self) -> DeltaErrorCode {
        self.code
    }

    /// Returns this error's stable string condition.
    pub fn condition(&self) -> &'static str {
        self.code.condition()
    }

    /// Returns this error's SQLSTATE, when one is defined.
    pub fn sql_state(&self) -> Option<&'static str> {
        self.code.sql_state()
    }

    /// Returns this error's named message parameters in template order.
    pub fn parameters(&self) -> &[DeltaErrorParameter] {
        &self.parameters
    }

    /// Renders the user-facing message.
    pub fn message(&self) -> String {
        if self.code == DeltaErrorCode::DeltaKernelUnclassified {
            if let Some(source) = &self.source {
                return source.to_string();
            }
        }
        render_template(self.code.message_template(), &self.parameters)
    }

    /// Returns the backtrace captured when this structured error was created.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl fmt::Display for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[{}] {}", self.condition(), self.message())?;
        if let Some(sql_state) = self.sql_state() {
            write!(formatter, "\nSQLSTATE: {sql_state}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaError")
            .field("condition", &self.condition())
            .field("sql_state", &self.sql_state())
            .field("parameters", &self.parameters)
            .field(
                "legacy_error_kind",
                &self.legacy_kind.map(LegacyErrorKind::as_str),
            )
            .field("source", &self.source)
            .field("backtrace", &self.backtrace)
            .finish()
    }
}

impl StdError for DeltaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// The typed category of an [`EngineError`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EngineErrorKind {
    /// A requested file does not exist.
    FileNotFound,
    /// A non-overwrite operation targeted an existing file.
    FileAlreadyExists,
    /// Data returned by the engine is corrupt or malformed.
    CorruptData,
    /// Engine execution was cancelled.
    Cancelled,
    /// An engine failure not otherwise classified.
    Other,
}

/// A failure produced by a connector-provided engine.
#[non_exhaustive]
#[derive(Debug)]
pub enum EngineError {
    /// A requested file does not exist.
    FileNotFound {
        /// Missing file path.
        path: String,
        /// Optional underlying engine error.
        source: Option<BoxedError>,
        /// Backtrace captured at the engine-error boundary.
        backtrace: Backtrace,
    },
    /// A non-overwrite operation targeted an existing file.
    FileAlreadyExists {
        /// Existing destination path.
        path: String,
        /// Optional underlying engine error.
        source: Option<BoxedError>,
        /// Backtrace captured at the engine-error boundary.
        backtrace: Backtrace,
    },
    /// Data returned by the engine is corrupt or malformed.
    CorruptData {
        /// User-safe failure description.
        message: String,
        /// Optional underlying engine error.
        source: Option<BoxedError>,
        /// Backtrace captured at the engine-error boundary.
        backtrace: Backtrace,
    },
    /// Engine execution was cancelled.
    Cancelled {
        /// Optional underlying engine error.
        source: Option<BoxedError>,
        /// Backtrace captured at the engine-error boundary.
        backtrace: Backtrace,
    },
    /// An engine failure not otherwise classified.
    Other {
        /// User-safe failure description.
        message: String,
        /// Optional underlying engine error.
        source: Option<BoxedError>,
        /// Backtrace captured at the engine-error boundary.
        backtrace: Backtrace,
    },
}

impl EngineError {
    /// Creates a missing-file engine error.
    pub fn file_not_found(path: impl Into<String>) -> Self {
        Self::FileNotFound {
            path: path.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Creates an existing-file engine error.
    pub fn file_already_exists(path: impl Into<String>) -> Self {
        Self::FileAlreadyExists {
            path: path.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Creates a corrupt-data engine error.
    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self::CorruptData {
            message: message.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Creates a cancelled engine error.
    pub fn cancelled() -> Self {
        Self::Cancelled {
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Creates an unclassified engine error.
    pub fn other(message: impl Into<String>) -> Self {
        Self::Other {
            message: message.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Attaches the underlying engine error used for debugging.
    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        let slot = match &mut self {
            Self::FileNotFound { source, .. }
            | Self::FileAlreadyExists { source, .. }
            | Self::CorruptData { source, .. }
            | Self::Cancelled { source, .. }
            | Self::Other { source, .. } => source,
        };
        *slot = Some(Box::new(source));
        self
    }

    /// Returns this error's typed category.
    pub fn kind(&self) -> EngineErrorKind {
        match self {
            Self::FileNotFound { .. } => EngineErrorKind::FileNotFound,
            Self::FileAlreadyExists { .. } => EngineErrorKind::FileAlreadyExists,
            Self::CorruptData { .. } => EngineErrorKind::CorruptData,
            Self::Cancelled { .. } => EngineErrorKind::Cancelled,
            Self::Other { .. } => EngineErrorKind::Other,
        }
    }

    /// Returns the affected path for file errors.
    pub fn path(&self) -> Option<&str> {
        match self {
            Self::FileNotFound { path, .. } | Self::FileAlreadyExists { path, .. } => Some(path),
            _ => None,
        }
    }

    /// Returns the user-facing message without source or backtrace details.
    pub fn message(&self) -> &str {
        match self {
            Self::FileNotFound { .. } => "File not found",
            Self::FileAlreadyExists { .. } => "File already exists",
            Self::CorruptData { message, .. } | Self::Other { message, .. } => message,
            Self::Cancelled { .. } => "Operation cancelled",
        }
    }

    /// Returns the backtrace captured when this engine error was created.
    pub fn backtrace(&self) -> &Backtrace {
        match self {
            Self::FileNotFound { backtrace, .. }
            | Self::FileAlreadyExists { backtrace, .. }
            | Self::CorruptData { backtrace, .. }
            | Self::Cancelled { backtrace, .. }
            | Self::Other { backtrace, .. } => backtrace,
        }
    }

    /// Returns whether this error reports a missing file.
    pub fn is_file_not_found(&self) -> bool {
        self.kind() == EngineErrorKind::FileNotFound
    }

    /// Returns whether this error reports an existing destination file.
    pub fn is_file_already_exists(&self) -> bool {
        self.kind() == EngineErrorKind::FileAlreadyExists
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileNotFound { path, .. } => write!(formatter, "File not found: {path}"),
            Self::FileAlreadyExists { path, .. } => {
                write!(formatter, "File already exists: {path}")
            }
            Self::CorruptData { message, .. } => write!(formatter, "Corrupt data: {message}"),
            Self::Cancelled { .. } => formatter.write_str("Operation cancelled"),
            Self::Other { message, .. } => write!(formatter, "Engine error: {message}"),
        }
    }
}

impl StdError for EngineError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        let source = match self {
            Self::FileNotFound { source, .. }
            | Self::FileAlreadyExists { source, .. }
            | Self::CorruptData { source, .. }
            | Self::Cancelled { source, .. }
            | Self::Other { source, .. } => source,
        };
        source.as_deref().map(|source| source as _)
    }
}

/// Named constructors are kept inside kernel so connectors cannot manufacture Delta conditions.
pub(crate) mod delta_errors {
    use super::*;

    pub(crate) fn log_already_exists(path: impl ToString) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaLogAlreadyExists,
            vec![DeltaErrorParameter::new("path", path)],
        )
        .into()
    }

    pub(crate) fn version_not_found(
        user_version: Version,
        earliest: Version,
        latest: Version,
    ) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaVersionNotFound,
            vec![
                DeltaErrorParameter::new("userVersion", user_version),
                DeltaErrorParameter::new("earliest", earliest),
                DeltaErrorParameter::new("latest", latest),
            ],
        )
        .into()
    }

    pub(crate) fn missing_part_files(version: Version, source: impl Into<BoxedError>) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaMissingPartFiles,
            vec![DeltaErrorParameter::new("version", version)],
        )
        .with_boxed_source(source.into())
        .into()
    }

    pub(crate) fn state_recover_error(
        operation: impl ToString,
        version: Version,
        source: impl Into<BoxedError>,
    ) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaStateRecoverError,
            vec![
                DeltaErrorParameter::new("operation", operation),
                DeltaErrorParameter::new("version", version),
            ],
        )
        .with_boxed_source(source.into())
        .into()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn invalid_protocol_version(
        table_name_or_path: impl ToString,
        reader_required: i32,
        writer_required: i32,
        delta_version: impl ToString,
        supported_readers: impl ToString,
        supported_writers: impl ToString,
    ) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaInvalidProtocolVersion,
            vec![
                DeltaErrorParameter::new("tableNameOrPath", table_name_or_path),
                DeltaErrorParameter::new("readerRequired", reader_required),
                DeltaErrorParameter::new("writerRequired", writer_required),
                DeltaErrorParameter::new("deltaVersion", delta_version),
                DeltaErrorParameter::new("supportedReaders", supported_readers),
                DeltaErrorParameter::new("supportedWriters", supported_writers),
            ],
        )
        .into()
    }

    pub(crate) fn unsupported_features_for_read(
        table_name_or_path: impl ToString,
        delta_version: impl ToString,
        unsupported: impl ToString,
    ) -> Error {
        unsupported_features(
            DeltaErrorCode::DeltaUnsupportedFeaturesForRead,
            table_name_or_path,
            delta_version,
            unsupported,
        )
    }

    pub(crate) fn unsupported_features_for_write(
        table_name_or_path: impl ToString,
        delta_version: impl ToString,
        unsupported: impl ToString,
    ) -> Error {
        unsupported_features(
            DeltaErrorCode::DeltaUnsupportedFeaturesForWrite,
            table_name_or_path,
            delta_version,
            unsupported,
        )
    }

    pub(crate) fn versions_not_contiguous(
        version_list: impl ToString,
        start_version: Version,
        end_version: Version,
        version_to_load: Version,
    ) -> Error {
        DeltaError::new(
            DeltaErrorCode::DeltaVersionsNotContiguous,
            vec![
                DeltaErrorParameter::new("versionList", version_list),
                DeltaErrorParameter::new("startVersion", start_version),
                DeltaErrorParameter::new("endVersion", end_version),
                DeltaErrorParameter::new("versionToLoad", version_to_load),
            ],
        )
        .into()
    }

    pub(super) fn kernel_unclassified_boxed(source: BoxedError) -> Error {
        DeltaError::new(DeltaErrorCode::DeltaKernelUnclassified, vec![])
            .with_legacy_kind(LegacyErrorKind::Generic)
            .with_boxed_source(source)
            .into()
    }

    fn unsupported_features(
        code: DeltaErrorCode,
        table_name_or_path: impl ToString,
        delta_version: impl ToString,
        unsupported: impl ToString,
    ) -> Error {
        DeltaError::new(
            code,
            vec![
                DeltaErrorParameter::new("tableNameOrPath", table_name_or_path),
                DeltaErrorParameter::new("deltaVersion", delta_version),
                DeltaErrorParameter::new("unsupported", unsupported),
            ],
        )
        .into()
    }
}

fn render_template(template: &str, parameters: &[DeltaErrorParameter]) -> String {
    parameters
        .iter()
        .fold(template.to_string(), |message, parameter| {
            message.replace(&format!("<{}>", parameter.name), &parameter.value)
        })
}

fn legacy(kind: LegacyErrorKind, message: String) -> Error {
    DeltaError::new(DeltaErrorCode::DeltaKernelUnclassified, vec![])
        .with_legacy_kind(kind)
        .with_boxed_source(Box::new(LegacyError { kind, message }))
        .into()
}

fn legacy_source(kind: LegacyErrorKind, source: impl Into<BoxedError>) -> Error {
    legacy_boxed_source(kind, source.into())
}

fn legacy_boxed_source(kind: LegacyErrorKind, source: BoxedError) -> Error {
    DeltaError::new(DeltaErrorCode::DeltaKernelUnclassified, vec![])
        .with_legacy_kind(kind)
        .with_boxed_source(source)
        .into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LegacyErrorKind {
    #[cfg(feature = "default-engine-base")]
    Arrow,
    CheckpointWrite,
    EngineDataType,
    Generic,
    IOError,
    MalformedJson,
    MissingColumn,
    MissingMetadata,
    MissingMetadataAndProtocol,
    MissingProtocol,
    UnexpectedColumnType,
    MissingData,
    DeletionVector,
    InvalidUrl,
    InvalidProtocol,
    JoinFailure,
    ParseInt,
    InvalidColumnMappingMode,
    InvalidTableLocation,
    InvalidDecimal,
    InvalidStructData,
    Internal,
    InvalidExpression,
    InvalidLogPath,
    Parse,
    Unsupported,
    ParseInterval,
    CdfUnsupported,
    CdfIncompatibleSchema,
    RowTrackingCdfUnsupported,
    InvalidCheckpoint,
    LiteralExpressionTransform,
    #[cfg(feature = "default-engine-base")]
    ObjectStore,
    #[cfg(feature = "default-engine-base")]
    ObjectStorePath,
    #[cfg(feature = "default-engine-base")]
    Parquet,
    #[cfg(feature = "default-engine-base")]
    Reqwest,
    Schema,
    LogHistory,
    Other,
    Utf8,
}

impl LegacyErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            #[cfg(feature = "default-engine-base")]
            Self::Arrow => "Arrow",
            Self::CheckpointWrite => "CheckpointWrite",
            Self::EngineDataType => "EngineDataType",
            Self::Generic => "Generic",
            Self::IOError => "IOError",
            Self::MalformedJson => "MalformedJson",
            Self::MissingColumn => "MissingColumn",
            Self::MissingMetadata => "MissingMetadata",
            Self::MissingMetadataAndProtocol => "MissingMetadataAndProtocol",
            Self::MissingProtocol => "MissingProtocol",
            Self::UnexpectedColumnType => "UnexpectedColumnType",
            Self::MissingData => "MissingData",
            Self::DeletionVector => "DeletionVector",
            Self::InvalidUrl => "InvalidUrl",
            Self::InvalidProtocol => "InvalidProtocol",
            Self::JoinFailure => "JoinFailure",
            Self::ParseInt => "ParseInt",
            Self::InvalidColumnMappingMode => "InvalidColumnMappingMode",
            Self::InvalidTableLocation => "InvalidTableLocation",
            Self::InvalidDecimal => "InvalidDecimal",
            Self::InvalidStructData => "InvalidStructData",
            Self::Internal => "Internal",
            Self::InvalidExpression => "InvalidExpression",
            Self::InvalidLogPath => "InvalidLogPath",
            Self::Parse => "Parse",
            Self::Unsupported => "Unsupported",
            Self::ParseInterval => "ParseInterval",
            Self::CdfUnsupported => "CdfUnsupported",
            Self::CdfIncompatibleSchema => "CdfIncompatibleSchema",
            Self::RowTrackingCdfUnsupported => "RowTrackingCdfUnsupported",
            Self::InvalidCheckpoint => "InvalidCheckpoint",
            Self::LiteralExpressionTransform => "LiteralExpressionTransform",
            #[cfg(feature = "default-engine-base")]
            Self::ObjectStore => "ObjectStore",
            #[cfg(feature = "default-engine-base")]
            Self::ObjectStorePath => "ObjectStorePath",
            #[cfg(feature = "default-engine-base")]
            Self::Parquet => "Parquet",
            #[cfg(feature = "default-engine-base")]
            Self::Reqwest => "Reqwest",
            Self::Schema => "Schema",
            Self::LogHistory => "LogHistory",
            Self::Other => "Other",
            Self::Utf8 => "Utf8",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct LegacyError {
    kind: LegacyErrorKind,
    message: String,
}

#[derive(Debug)]
struct ContextError {
    message: String,
    source: Error,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for ContextError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

fn source_chain_contains<T>(mut source: Option<&(dyn StdError + 'static)>) -> bool
where
    T: StdError + 'static,
{
    while let Some(error) = source {
        if error.is::<T>() {
            return true;
        }
        source = error.source();
    }
    false
}

fn legacy_kind_from_source(
    mut source: Option<&(dyn StdError + 'static)>,
) -> Option<LegacyErrorKind> {
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<LegacyError>() {
            return Some(error.kind);
        }
        #[cfg(feature = "default-engine-base")]
        if error.is::<ArrowError>() {
            return Some(LegacyErrorKind::Arrow);
        }
        #[cfg(feature = "default-engine-base")]
        if error.is::<crate::parquet::errors::ParquetError>() {
            return Some(LegacyErrorKind::Parquet);
        }
        #[cfg(feature = "default-engine-base")]
        if error.is::<object_store::path::Error>() {
            return Some(LegacyErrorKind::ObjectStorePath);
        }
        #[cfg(feature = "default-engine-base")]
        if error.is::<object_store::Error>() {
            return Some(LegacyErrorKind::ObjectStore);
        }
        #[cfg(feature = "default-engine-base")]
        if error.is::<reqwest::Error>() {
            return Some(LegacyErrorKind::Reqwest);
        }
        if error.is::<serde_json::Error>() {
            return Some(LegacyErrorKind::MalformedJson);
        }
        if error.is::<std::str::Utf8Error>() {
            return Some(LegacyErrorKind::Utf8);
        }
        if error.is::<std::io::Error>() {
            return Some(LegacyErrorKind::IOError);
        }
        if error.is::<std::num::ParseIntError>() {
            return Some(LegacyErrorKind::ParseInt);
        }
        if error.is::<url::ParseError>() {
            return Some(LegacyErrorKind::InvalidUrl);
        }
        if error.is::<ParseIntervalError>() {
            return Some(LegacyErrorKind::ParseInterval);
        }
        if error.is::<crate::expressions::literal_expression_transform::Error>() {
            return Some(LegacyErrorKind::LiteralExpressionTransform);
        }
        if error.is::<crate::history_manager::error::LogHistoryError>() {
            return Some(LegacyErrorKind::LogHistory);
        }
        source = error.source();
    }
    None
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        legacy_source(LegacyErrorKind::IOError, error)
    }
}

impl From<std::io::Error> for EngineError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => {
                EngineError::file_not_found(error.to_string()).with_source(error)
            }
            std::io::ErrorKind::AlreadyExists => {
                EngineError::file_already_exists(error.to_string()).with_source(error)
            }
            _ => EngineError::other(error.to_string()).with_source(error),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        legacy_source(LegacyErrorKind::MalformedJson, error)
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(error: serde_json::Error) -> Self {
        EngineError::corrupt_data("Malformed JSON").with_source(error)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        legacy_source(LegacyErrorKind::Utf8, error)
    }
}

impl From<std::str::Utf8Error> for EngineError {
    fn from(error: std::str::Utf8Error) -> Self {
        EngineError::corrupt_data("Invalid UTF-8").with_source(error)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(error: std::num::ParseIntError) -> Self {
        legacy_source(LegacyErrorKind::ParseInt, error)
    }
}

impl From<url::ParseError> for Error {
    fn from(error: url::ParseError) -> Self {
        legacy_source(LegacyErrorKind::InvalidUrl, error)
    }
}

impl From<ParseIntervalError> for Error {
    fn from(error: ParseIntervalError) -> Self {
        legacy_source(LegacyErrorKind::ParseInterval, error)
    }
}

impl From<crate::expressions::literal_expression_transform::Error> for Error {
    fn from(error: crate::expressions::literal_expression_transform::Error) -> Self {
        legacy_source(LegacyErrorKind::LiteralExpressionTransform, error)
    }
}

impl From<Box<crate::history_manager::error::LogHistoryError>> for Error {
    fn from(error: Box<crate::history_manager::error::LogHistoryError>) -> Self {
        legacy_boxed_source(LegacyErrorKind::LogHistory, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<ArrowError> for Error {
    fn from(error: ArrowError) -> Self {
        legacy_source(LegacyErrorKind::Arrow, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<ArrowError> for EngineError {
    fn from(error: ArrowError) -> Self {
        EngineError::corrupt_data("Arrow operation failed").with_source(error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<crate::parquet::errors::ParquetError> for Error {
    fn from(error: crate::parquet::errors::ParquetError) -> Self {
        legacy_source(LegacyErrorKind::Parquet, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<crate::parquet::errors::ParquetError> for EngineError {
    fn from(error: crate::parquet::errors::ParquetError) -> Self {
        EngineError::corrupt_data("Parquet operation failed").with_source(error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::path::Error> for Error {
    fn from(error: object_store::path::Error) -> Self {
        legacy_source(LegacyErrorKind::ObjectStorePath, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::path::Error> for EngineError {
    fn from(error: object_store::path::Error) -> Self {
        EngineError::other("Object-store path operation failed").with_source(error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        legacy_source(LegacyErrorKind::ObjectStore, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::Error> for EngineError {
    fn from(error: object_store::Error) -> Self {
        match &error {
            object_store::Error::NotFound { path, .. } => {
                EngineError::file_not_found(path.clone()).with_source(error)
            }
            object_store::Error::AlreadyExists { path, .. } => {
                EngineError::file_already_exists(path.clone()).with_source(error)
            }
            _ => EngineError::other("Object-store operation failed").with_source(error),
        }
    }
}

#[cfg(feature = "default-engine-base")]
impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        legacy_source(LegacyErrorKind::Reqwest, error)
    }
}

#[cfg(feature = "default-engine-base")]
impl From<reqwest::Error> for EngineError {
    fn from(error: reqwest::Error) -> Self {
        EngineError::other("HTTP request failed").with_source(error)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::Value;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("nested I/O failure")]
    struct NestedIoError(#[source] std::io::Error);

    #[test]
    fn catalog_has_unique_string_conditions_and_matching_parameters() {
        let mut conditions = HashSet::new();
        for code in ALL_DELTA_ERROR_CODES {
            assert!(conditions.insert(code.condition()));
            let template = code.message_template();
            let placeholders: Vec<&str> = template
                .split('<')
                .skip(1)
                .filter_map(|suffix| suffix.split_once('>').map(|(name, _)| name))
                .collect();
            assert_eq!(placeholders, code.parameter_names(), "{}", code.condition());
        }
    }

    #[test]
    fn curated_conditions_match_pinned_sqlstates_and_parameter_names() {
        let catalog: Value =
            serde_json::from_str(include_str!("error/delta-error-codes-curated.json")).unwrap();
        assert_eq!(
            catalog["source"]["repository"].as_str(),
            Some("delta-io/delta")
        );
        assert_eq!(
            catalog["source"]["commit"].as_str(),
            Some("8ba5e37b9a21aba2859cdd09063d561928f7c641")
        );
        let conditions = &catalog["conditions"];
        assert_eq!(
            conditions.as_object().unwrap().len(),
            ALL_DELTA_ERROR_CODES.len() - 1
        );
        for code in ALL_DELTA_ERROR_CODES {
            if code == &DeltaErrorCode::DeltaKernelUnclassified {
                continue;
            }
            let entry = &conditions[code.condition()];
            assert!(entry.is_object(), "missing {}", code.condition());
            assert_eq!(entry["sqlState"].as_str(), code.sql_state());
            let parameter_names: Vec<_> = entry["parameterNames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| name.as_str().unwrap())
                .collect();
            assert_eq!(
                parameter_names,
                code.parameter_names(),
                "{}",
                code.condition()
            );
        }
    }

    #[test]
    fn delta_error_exposes_structured_fields_without_source_in_display() {
        let source = std::io::Error::other("internal detail");
        let error = delta_errors::state_recover_error("metadata", 12, source);
        let error = error.as_delta_error().unwrap();
        assert_eq!(error.code(), DeltaErrorCode::DeltaStateRecoverError);
        assert_eq!(error.condition(), "DELTA_STATE_RECOVER_ERROR");
        assert_eq!(error.sql_state(), Some("XXKDS"));
        assert_eq!(error.parameters()[0].name(), "operation");
        assert_eq!(error.parameters()[0].value(), "metadata");
        assert!(error.to_string().contains("reconstructing version 12"));
        assert!(!error.to_string().contains("internal detail"));
        assert_eq!(error.source().unwrap().to_string(), "internal detail");
        let _ = error.backtrace();
    }

    #[test]
    fn engine_error_exposes_kind_path_source_and_backtrace() {
        let error = EngineError::file_not_found("/table/part.parquet")
            .with_source(std::io::Error::other("storage detail"));
        assert_eq!(error.kind(), EngineErrorKind::FileNotFound);
        assert_eq!(error.path(), Some("/table/part.parquet"));
        assert_eq!(error.message(), "File not found");
        assert!(!error.to_string().contains("storage detail"));
        assert_eq!(error.source().unwrap().to_string(), "storage detail");
        let _ = error.backtrace();
    }

    #[test]
    fn top_level_error_has_exactly_two_origins() {
        let delta = delta_errors::log_already_exists("/table");
        assert!(delta.as_delta_error().is_some());
        assert!(delta.as_engine_error().is_none());
        assert_eq!(
            delta.to_string(),
            "[DELTA_LOG_ALREADY_EXISTS] A Delta log already exists at /table.\nSQLSTATE: 42K04"
        );

        let engine = Error::file_not_found("/table/missing");
        assert!(engine.as_delta_error().is_none());
        assert!(engine.as_engine_error().is_some());
        assert!(engine.is_file_not_found());
    }

    #[test]
    fn unclassified_fallback_has_no_sqlstate_and_preserves_source() {
        let error = Error::generic("legacy detail");
        let delta = error.as_delta_error().unwrap();
        assert_eq!(delta.code(), DeltaErrorCode::DeltaKernelUnclassified);
        assert_eq!(delta.sql_state(), None);
        assert_eq!(delta.source().unwrap().to_string(), "legacy detail");
        assert_eq!(
            delta.to_string(),
            "[DELTA_KERNEL_UNCLASSIFIED] legacy detail"
        );
    }

    #[test]
    fn typed_kernel_conversions_are_unclassified_and_preserve_typed_sources() {
        let json_error = serde_json::from_str::<Value>("{").unwrap_err();
        let error = Error::from(json_error);
        let delta = error.as_delta_error().unwrap();
        assert_eq!(delta.code(), DeltaErrorCode::DeltaKernelUnclassified);
        assert!(delta.source().unwrap().is::<serde_json::Error>());
        assert_eq!(error.legacy_error_kind(), Some("MalformedJson"));

        let io_error = std::io::Error::other("kernel I/O detail");
        let error = Error::from(io_error);
        let delta = error.as_delta_error().unwrap();
        assert_eq!(delta.code(), DeltaErrorCode::DeltaKernelUnclassified);
        assert!(delta.source().unwrap().is::<std::io::Error>());
        assert_eq!(error.legacy_error_kind(), Some("IOError"));

        let invalid_utf8 = vec![0xff];
        let utf8_error = std::str::from_utf8(&invalid_utf8).unwrap_err();
        let error = Error::from(utf8_error);
        let delta = error.as_delta_error().unwrap();
        assert_eq!(delta.code(), DeltaErrorCode::DeltaKernelUnclassified);
        assert!(delta.source().unwrap().is::<std::str::Utf8Error>());
        assert_eq!(error.legacy_error_kind(), Some("Utf8"));
    }

    #[test]
    fn engine_source_chains_preserve_legacy_categories_and_io_retry_origin() {
        let error = Error::from(
            EngineError::other("engine adapter")
                .with_source(std::io::Error::other("engine I/O detail")),
        );
        assert!(error.is_io_error());
        assert_eq!(error.legacy_error_kind(), Some("IOError"));

        let nested = NestedIoError(std::io::Error::other("nested engine I/O detail"));
        let error = Error::from(EngineError::other("engine adapter").with_source(nested));
        assert!(error.is_io_error());
        assert_eq!(error.legacy_error_kind(), Some("IOError"));

        let json_error = serde_json::from_str::<Value>("{").unwrap_err();
        let error = Error::from(EngineError::corrupt_data("bad JSON").with_source(json_error));
        assert_eq!(error.legacy_error_kind(), Some("MalformedJson"));

        let error = Error::generic_err(std::io::Error::other("kernel I/O detail"));
        assert!(!error.is_io_error());
        assert_eq!(error.legacy_error_kind(), Some("Generic"));
    }

    #[test]
    fn engine_source_chains_preserve_parse_classification() {
        let parse_error = Error::ParseError("not-an-integer".to_string(), DataType::LONG);
        let error = Error::from(
            EngineError::other("expression evaluation failed").with_source(parse_error),
        );

        assert!(error.is_parse_error());
        assert_eq!(error.legacy_error_kind(), Some("Parse"));
    }

    #[cfg(feature = "default-engine-base")]
    #[test]
    fn arrow_conversions_preserve_origin_source_and_legacy_category() {
        let arrow_error = ArrowError::InvalidArgumentError("kernel Arrow detail".to_string());
        let error = Error::from(arrow_error);
        assert!(error
            .as_delta_error()
            .unwrap()
            .source()
            .unwrap()
            .is::<ArrowError>());
        assert_eq!(error.legacy_error_kind(), Some("Arrow"));

        let arrow_error = ArrowError::InvalidArgumentError("engine Arrow detail".to_string());
        let error = Error::from(EngineError::from(arrow_error));
        assert!(error.as_engine_error().is_some());
        assert_eq!(error.legacy_error_kind(), Some("Arrow"));
    }

    #[cfg(feature = "default-engine-base")]
    #[test]
    fn object_store_not_found_preserves_origin_kind_wrapper_and_legacy_category() {
        let object_store_error = object_store::Error::NotFound {
            path: "table/missing".to_string(),
            source: Box::new(std::io::Error::other("storage detail")),
        };
        let error = Error::from(object_store_error);
        assert!(error
            .as_delta_error()
            .unwrap()
            .source()
            .unwrap()
            .is::<object_store::Error>());
        assert_eq!(error.legacy_error_kind(), Some("ObjectStore"));
        assert!(!error.is_file_not_found());

        let object_store_error = object_store::Error::NotFound {
            path: "table/missing".to_string(),
            source: Box::new(std::io::Error::other("storage detail")),
        };
        let error = Error::from(EngineError::from(object_store_error));
        assert!(error.is_file_not_found());
        assert!(error.is_io_error());
        assert!(error
            .as_engine_error()
            .unwrap()
            .source()
            .unwrap()
            .is::<object_store::Error>());
        assert_eq!(error.legacy_error_kind(), Some("ObjectStore"));
    }

    #[cfg(feature = "default-engine-base")]
    #[test]
    fn object_store_already_exists_preserves_engine_kind_and_wrapper() {
        let object_store_error = object_store::Error::AlreadyExists {
            path: "table/existing".to_string(),
            source: Box::new(std::io::Error::other("storage detail")),
        };
        let error = Error::from(EngineError::from(object_store_error));
        assert!(error.is_file_already_exists());
        assert!(error
            .as_engine_error()
            .unwrap()
            .source()
            .unwrap()
            .is::<object_store::Error>());
        assert_eq!(error.legacy_error_kind(), Some("ObjectStore"));
    }
}
