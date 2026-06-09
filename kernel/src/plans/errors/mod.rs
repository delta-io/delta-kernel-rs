//! Typed error surface for the declarative plans layer.
//!
//! [`DeltaError`] is the error type used by state machines and the plan executor. It coexists
//! with [`Error`]; lifting an [`Error`] into a [`DeltaError`] goes through the named bridge
//! [`KernelErrAsDelta`] -- deliberately no `From<Error>` impl, so the conversion sites are
//! grep-able. (A `From<BoxedSource>` does exist for the catch-all `dyn Error + Send + Sync`
//! shape used by Delta-agnostic helpers; that path tags every lifted error with
//! [`DeltaErrorCode::DeltaCommandInvariantViolation`].)
//!
//! # Construction
//!
//! Every error is built through a named constructor on [`DeltaError`], one per
//! [`DeltaErrorCode`]. The human message is fixed per code ([`DeltaErrorCode::message`]); the
//! constructor takes only the runtime detail / underlying cause, which becomes the error's
//! [`source`](DeltaError::source). The detail is `impl Into<BoxedSource>`, so it accepts a bare
//! string (auto-wrapped as a trivial error) or any real error (kept with its type and chain).
//!
//! [`DeltaResultExt::or_delta`] does the same at a `?` site: it attaches a code and keeps the
//! original error as the source.
//!
//! ```ignore
//! use delta_kernel::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
//!
//! return Err(DeltaError::table_not_found(format!("{name}")));
//! let v: u64 = "42".parse().or_delta(DeltaErrorCode::DeltaVersionInvalid)?;
//! let e = DeltaError::file_not_found(io_err);
//! ```

use std::backtrace::Backtrace;

use crate::Error;

// ============================================================================
// DeltaErrorCode -- macro-driven declaration
// ============================================================================

/// Declarative table of Delta error codes. Each row binds a variant to its FFI-stable
/// discriminant, SCREAMING_SNAKE name, SQLSTATE, and a static human message.
///
/// The message is fixed per code; runtime detail (the offending path, version, cause, ...) is
/// never formatted into it -- it lives in [`DeltaError::source`].
///
/// Discriminants are explicit because the enum is `#[repr(u32)]` for FFI ABI stability.
/// Reassigning or reusing a discriminant is a breaking change; pick the next unused integer.
macro_rules! delta_codes {
    (
        $(
            $( #[$meta:meta] )*
            $variant:ident = $disc:literal,
            $name:literal,
            $sqlstate:literal,
            $message:literal
        ),+
        $(,)?
    ) => {
        /// Typed identifier for a Delta error.
        ///
        /// `#[repr(u32)]` pins the integer layout for FFI; `#[non_exhaustive]` reserves space
        /// for additional codes -- downstream consumers must treat unknown integers as opaque.
        #[repr(u32)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum DeltaErrorCode {
            $( $( #[$meta] )* $variant = $disc ),+
        }

        impl DeltaErrorCode {
            /// SCREAMING_SNAKE name for this error code.
            pub fn name(&self) -> &'static str {
                match self { $( Self::$variant => $name ),+ }
            }

            /// 5-character SQLSTATE classifying the error (SQL-standard class + subclass).
            pub fn sqlstate(&self) -> &'static str {
                match self { $( Self::$variant => $sqlstate ),+ }
            }

            /// Static human-readable message for this code. Carries no runtime detail.
            pub fn message(&self) -> &'static str {
                match self { $( Self::$variant => $message ),+ }
            }
        }
    };
}

delta_codes! {
    /// `DELTA_COMMAND_INVARIANT_VIOLATION` -- internal-error catch-all for SMs detecting an
    /// impossible state. Discriminant `0` is deliberate: a zero-initialized wire value decodes
    /// to "invariant violation" so engines that fail to populate the code slot still surface
    /// a meaningful error.
    DeltaCommandInvariantViolation = 0, "DELTA_COMMAND_INVARIANT_VIOLATION", "XXKDS",
        "internal invariant violated",
    /// Delta table does not exist.
    DeltaTableNotFound = 1, "DELTA_TABLE_NOT_FOUND", "42P01", "Delta table not found",
    /// Path is not a Delta table.
    DeltaMissingDeltaTable = 2, "DELTA_MISSING_DELTA_TABLE", "42P01", "not a Delta table",
    /// Path doesn't exist, or is not a Delta table.
    DeltaPathDoesNotExist = 3, "DELTA_PATH_DOES_NOT_EXIST", "42K03", "path does not exist",
    /// Requested version is outside the available range.
    DeltaVersionNotFound = 4, "DELTA_VERSION_NOT_FOUND", "22003", "version not found",
    /// A provided version string / number is not parseable.
    DeltaVersionInvalid = 5, "DELTA_VERSION_INVALID", "42815", "invalid version",
    /// A commit file needed to reconstruct state is missing.
    DeltaLogFileNotFound = 6, "DELTA_LOG_FILE_NOT_FOUND", "42K03", "commit log file not found",
    /// A data file referenced by the log is missing.
    DeltaFileNotFound = 7, "DELTA_FILE_NOT_FOUND", "42K03", "file not found",
    /// A multipart checkpoint is missing shards.
    DeltaMissingPartFiles = 8, "DELTA_MISSING_PART_FILES", "42KD6", "checkpoint is missing part files",
    /// Protocol / metadata could not be reconstructed.
    DeltaStateRecoverError = 9, "DELTA_STATE_RECOVER_ERROR", "XXKDS",
        "failed to reconstruct table state",
    /// The table requires a protocol the kernel doesn't support.
    DeltaInvalidProtocolVersion = 10, "DELTA_INVALID_PROTOCOL_VERSION", "KD004",
        "unsupported protocol version",
    /// A commit raced with another transaction.
    DeltaConcurrentWrite = 11, "DELTA_CONCURRENT_WRITE", "2D521",
        "commit failed due to a concurrent write",
    /// Attempt to create a Delta log that already exists.
    DeltaLogAlreadyExists = 12, "DELTA_LOG_ALREADY_EXISTS", "42K04", "Delta log already exists",
}

// ============================================================================
// DeltaError
// ============================================================================

/// Boxed, sendable source error held by [`DeltaError::source`].
pub type BoxedSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Typed Delta error.
///
/// The human message is static per [`DeltaErrorCode`] ([`DeltaErrorCode::message`]); all runtime
/// detail (the offending path, version, underlying cause) lives in [`source`](Self::source).
/// `Display` emits `[<CODE_NAME>] <static message>`, and `source` is forwarded via
/// `std::error::Error::source()` so standard ecosystem walkers see the detail.
#[derive(Debug, thiserror::Error)]
#[error("[{}] {}", self.code.name(), self.code.message())]
pub struct DeltaError {
    /// Stable typed identifier; also determines the human message.
    pub code: DeltaErrorCode,
    /// Runtime detail / underlying cause, forwarded via `std::error::Error::source()`. A bare
    /// `String`/`&str` is accepted and wrapped as a trivial error (stdlib `From`).
    #[source]
    pub source: Option<BoxedSource>,
    /// `Some(Backtrace::capture())` -- a no-op without `RUST_BACKTRACE=1`, so the "always on"
    /// default is free in production.
    pub backtrace: Option<Backtrace>,
}

impl DeltaError {
    /// Crate-internal primitive: build an error from a code and its runtime detail / cause. The
    /// per-code constructors below and [`DeltaResultExt::or_delta`] funnel through here.
    pub(crate) fn new(code: DeltaErrorCode, source: impl Into<BoxedSource>) -> Self {
        Self {
            code,
            source: Some(source.into()),
            backtrace: Some(Backtrace::capture()),
        }
    }

    // === Per-code constructors ==========================================================
    //
    // Each takes the runtime detail as `impl Into<BoxedSource>`: a bare string (auto-wrapped)
    // or any underlying error (kept with its type and source chain). The human message is the
    // code's static one.

    /// `DELTA_COMMAND_INVARIANT_VIOLATION` -- an internal "this should never happen" failure.
    pub fn invariant(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaCommandInvariantViolation, detail)
    }

    /// `DELTA_TABLE_NOT_FOUND` -- the Delta table does not exist.
    pub fn table_not_found(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaTableNotFound, detail)
    }

    /// `DELTA_MISSING_DELTA_TABLE` -- the path is not a Delta table.
    pub fn missing_delta_table(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaMissingDeltaTable, detail)
    }

    /// `DELTA_PATH_DOES_NOT_EXIST` -- the path does not exist.
    pub fn path_does_not_exist(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaPathDoesNotExist, detail)
    }

    /// `DELTA_VERSION_NOT_FOUND` -- the requested version is outside the available range.
    pub fn version_not_found(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaVersionNotFound, detail)
    }

    /// `DELTA_VERSION_INVALID` -- a version string/number is not parseable.
    pub fn version_invalid(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaVersionInvalid, detail)
    }

    /// `DELTA_LOG_FILE_NOT_FOUND` -- a commit file needed to reconstruct state is missing.
    pub fn log_file_not_found(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaLogFileNotFound, detail)
    }

    /// `DELTA_FILE_NOT_FOUND` -- a data file referenced by the log is missing.
    pub fn file_not_found(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaFileNotFound, detail)
    }

    /// `DELTA_MISSING_PART_FILES` -- a multipart checkpoint is missing shards.
    pub fn missing_part_files(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaMissingPartFiles, detail)
    }

    /// `DELTA_STATE_RECOVER_ERROR` -- protocol/metadata could not be reconstructed.
    pub fn state_recover_error(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaStateRecoverError, detail)
    }

    /// `DELTA_INVALID_PROTOCOL_VERSION` -- the table requires an unsupported protocol.
    pub fn invalid_protocol_version(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaInvalidProtocolVersion, detail)
    }

    /// `DELTA_CONCURRENT_WRITE` -- a commit raced with another transaction.
    pub fn concurrent_write(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaConcurrentWrite, detail)
    }

    /// `DELTA_LOG_ALREADY_EXISTS` -- attempt to create a Delta log that already exists.
    pub fn log_already_exists(detail: impl Into<BoxedSource>) -> Self {
        Self::new(DeltaErrorCode::DeltaLogAlreadyExists, detail)
    }
}

/// Auto-convert a [`BoxedSource`] (the boxed `dyn Error` shape used by Delta-agnostic helpers
/// like `schema_expr` and `plan_context`) into a [`DeltaError`] tagged with the catch-all
/// [`DeltaErrorCode::DeltaCommandInvariantViolation`]. Lets `?` propagate these errors into
/// `Result<_, DeltaError>` functions without an explicit `.or_delta(...)` wrap.
///
/// Use [`DeltaResultExt::or_delta`] when a more specific code is appropriate -- this impl is
/// intentionally the catch-all path.
impl From<BoxedSource> for DeltaError {
    fn from(source: BoxedSource) -> Self {
        Self::new(DeltaErrorCode::DeltaCommandInvariantViolation, source)
    }
}

// ============================================================================
// Bridges -- kernel::Error <-> DeltaError
// ============================================================================

/// Lift an [`Error`] into a [`DeltaError`] tagged with
/// [`DeltaErrorCode::DeltaCommandInvariantViolation`]. Use when a more specific code is not
/// known; upgrading to a specific constructor (e.g. `DeltaError::file_not_found(path)`) is a
/// drop-in refactor.
pub trait KernelErrAsDelta {
    fn into_delta_internal(self) -> DeltaError;
}

impl KernelErrAsDelta for Error {
    fn into_delta_internal(self) -> DeltaError {
        DeltaError::invariant(self)
    }
}

// ============================================================================
// or_delta at ? sites
// ============================================================================

/// Attach a [`DeltaErrorCode`] to any `Result<T, E>` whose error can be lifted into a
/// [`BoxedSource`]. The original error is preserved as [`DeltaError::source`] (walkable via
/// `std::error::Error::source()`); the message comes from the code.
///
/// The bound `E: Into<BoxedSource>` covers two cases without an extra `Box` layer:
/// - any concrete `E: Error + Send + Sync + 'static` (stdlib's blanket `From` impl), and
/// - errors that are already a [`BoxedSource`] (low-level modules that return a `Box<dyn Error +
///   Send + Sync + 'static>` directly to stay Delta-agnostic).
pub trait DeltaResultExt<T> {
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError>;
}

impl<T, E> DeltaResultExt<T> for Result<T, E>
where
    E: Into<BoxedSource>,
{
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError> {
        self.map_err(|e| DeltaError::new(code, e))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn display_is_code_name_plus_static_message() {
        let err = DeltaError::table_not_found("my_table");
        assert_eq!(err.code, DeltaErrorCode::DeltaTableNotFound);
        let s = err.to_string();
        assert!(s.contains("DELTA_TABLE_NOT_FOUND"), "got: {s}");
        assert!(s.contains("Delta table not found"), "got: {s}");
    }

    #[test]
    fn string_detail_becomes_source() {
        let err = DeltaError::invariant("snapshot.rebuild: protocol missing");
        assert_eq!(err.code, DeltaErrorCode::DeltaCommandInvariantViolation);
        assert_eq!(err.code.message(), "internal invariant violated");
        let src = err.source().expect("string detail preserved as source");
        assert_eq!(src.to_string(), "snapshot.rebuild: protocol missing");
    }

    #[test]
    fn error_detail_keeps_its_type_in_source() {
        let io = std::io::Error::other("boom");
        let err = DeltaError::file_not_found(io);
        assert_eq!(err.code, DeltaErrorCode::DeltaFileNotFound);
        let src = err.source().expect("source");
        assert!(src.is::<std::io::Error>());
        assert!(src.to_string().contains("boom"));
    }

    #[test]
    fn or_delta_preserves_original_error_in_source() {
        fn parse_version(s: &str) -> Result<u64, DeltaError> {
            s.parse::<u64>()
                .or_delta(DeltaErrorCode::DeltaVersionInvalid)
        }
        let err = parse_version("not-a-number").unwrap_err();
        assert_eq!(err.code, DeltaErrorCode::DeltaVersionInvalid);
        let src = err.source().expect("parse error preserved");
        assert!(src.is::<std::num::ParseIntError>());
    }
}
