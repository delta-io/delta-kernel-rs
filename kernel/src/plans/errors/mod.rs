//! Typed error surface for the declarative plans layer.
//!
//! [`DeltaError`] is the error type used by state machines and the plan executor. It coexists
//! with [`crate::Error`]; conversion at the boundary goes through the named bridges
//! [`KernelErrAsDelta`] / [`DeltaErrAsKernel`] — deliberately no `From` impls, so the
//! conversion sites are grep-able.
//!
//! # Construction
//!
//! Two macros:
//!
//! - [`crate::delta_error!`] — build a value. First arg is a [`DeltaErrorCode`], then an optional
//!   `source = expr`, then an optional `format!`-style message.
//! - [`crate::bail_delta!`] — early-return from an enclosing function.
//!
//! Plus [`DeltaResultExt::or_delta`] for attaching a code at a `?` site while preserving the
//! underlying error in [`DeltaError::source`].
//!
//! ```ignore
//! use delta_kernel::plans::errors::{DeltaErrorCode, DeltaResultExt};
//!
//! bail_delta!(DeltaErrorCode::DeltaTableNotFound, "table `{}` does not exist", name);
//! let v: u64 = "42".parse().or_delta(DeltaErrorCode::DeltaVersionInvalid)?;
//! let e = delta_error!(DeltaErrorCode::DeltaFileNotFound, source = io_err, "path={path}");
//! ```

use std::backtrace::Backtrace;

// ============================================================================
// DeltaErrorCode — macro-driven declaration
// ============================================================================

/// Declarative table of Delta error codes. Each row binds a variant to its FFI-stable
/// discriminant, SCREAMING_SNAKE name, and SQLSTATE.
///
/// Discriminants are explicit because the enum is `#[repr(u32)]` for FFI ABI stability.
/// Reassigning or reusing a discriminant is a breaking change; pick the next unused integer.
macro_rules! delta_codes {
    (
        $(
            $( #[$meta:meta] )*
            $variant:ident = $disc:literal,
            $name:literal,
            $sqlstate:literal
        ),+
        $(,)?
    ) => {
        /// Typed identifier for a Delta error.
        ///
        /// `#[repr(u32)]` pins the integer layout for FFI; `#[non_exhaustive]` reserves space
        /// for future additions — downstream consumers must treat unknown integers as opaque.
        #[repr(u32)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum DeltaErrorCode {
            $( $( #[$meta] )* $variant = $disc ),+
        }

        impl DeltaErrorCode {
            /// SCREAMING_SNAKE identifier (matches the Databricks upstream catalog key).
            pub fn name(&self) -> &'static str {
                match self { $( Self::$variant => $name ),+ }
            }

            /// Standard 5-character SQLSTATE, sourced from the upstream catalog.
            pub fn sqlstate(&self) -> &'static str {
                match self { $( Self::$variant => $sqlstate ),+ }
            }
        }
    };
}

delta_codes! {
    /// `DELTA_COMMAND_INVARIANT_VIOLATION` — internal-error catch-all for SMs detecting an
    /// impossible state. Discriminant `0` is deliberate: a zero-initialized wire value decodes
    /// to "invariant violation" so engines that fail to populate the code slot still surface
    /// a meaningful error.
    DeltaCommandInvariantViolation = 0, "DELTA_COMMAND_INVARIANT_VIOLATION", "XXKDS",
    /// Delta table does not exist.
    DeltaTableNotFound = 1, "DELTA_TABLE_NOT_FOUND", "42P01",
    /// Path is not a Delta table.
    DeltaMissingDeltaTable = 2, "DELTA_MISSING_DELTA_TABLE", "42P01",
    /// Path doesn't exist, or is not a Delta table.
    DeltaPathDoesNotExist = 3, "DELTA_PATH_DOES_NOT_EXIST", "42K03",
    /// Requested version is outside the available range.
    DeltaVersionNotFound = 4, "DELTA_VERSION_NOT_FOUND", "22003",
    /// A provided version string / number is not parseable.
    DeltaVersionInvalid = 5, "DELTA_VERSION_INVALID", "42815",
    /// A commit file needed to reconstruct state is missing.
    DeltaLogFileNotFound = 6, "DELTA_LOG_FILE_NOT_FOUND", "42K03",
    /// A data file referenced by the log is missing.
    DeltaFileNotFound = 7, "DELTA_FILE_NOT_FOUND", "42K03",
    /// A multipart checkpoint is missing shards.
    DeltaMissingPartFiles = 8, "DELTA_MISSING_PART_FILES", "42KD6",
    /// Protocol / metadata could not be reconstructed.
    DeltaStateRecoverError = 9, "DELTA_STATE_RECOVER_ERROR", "XXKDS",
    /// The table requires a protocol the kernel doesn't support.
    DeltaInvalidProtocolVersion = 10, "DELTA_INVALID_PROTOCOL_VERSION", "KD004",
    /// A commit raced with another transaction.
    DeltaConcurrentWrite = 11, "DELTA_CONCURRENT_WRITE", "2D521",
    /// Attempt to create a Delta log that already exists.
    DeltaLogAlreadyExists = 12, "DELTA_LOG_ALREADY_EXISTS", "42K04",
}

// ============================================================================
// DeltaError
// ============================================================================

/// Boxed, sendable source error held by [`DeltaError::source`].
pub type BoxedSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Typed Delta error.
///
/// `Display` emits `[<CODE_NAME>] <message>`. The `source` field is forwarded via
/// `std::error::Error::source()` so standard ecosystem walkers see the full chain.
#[derive(Debug, thiserror::Error)]
#[error("[{}] {message}", self.code.name())]
pub struct DeltaError {
    /// Stable typed identifier.
    pub code: DeltaErrorCode,
    /// Pre-formatted message — built at construction time by [`crate::delta_error!`].
    pub message: String,
    /// Optional underlying error, forwarded via `std::error::Error::source()`.
    #[source]
    pub source: Option<BoxedSource>,
    /// `Some(Backtrace::capture())` — a no-op without `RUST_BACKTRACE=1`, so the "always on"
    /// default is free in production.
    pub backtrace: Option<Backtrace>,
}

impl DeltaError {
    /// Constructor used by [`delta_error!`]. Public-but-hidden so the macro can expand at
    /// caller scope.
    #[doc(hidden)]
    pub fn __new(code: DeltaErrorCode, message: String, source: Option<BoxedSource>) -> Self {
        Self {
            code,
            message,
            source,
            backtrace: Some(Backtrace::capture()),
        }
    }
}

// ============================================================================
// Bridges — kernel::Error <-> DeltaError
// ============================================================================

/// Lift a [`crate::Error`] into a [`DeltaError`] tagged with
/// [`DeltaErrorCode::DeltaCommandInvariantViolation`]. Use when a more specific code isn't yet
/// known; upgrading to a better `delta_error!(code, source = e, "...")` is a drop-in refactor.
pub trait KernelErrAsDelta {
    fn into_delta_default(self) -> DeltaError;
}

impl KernelErrAsDelta for crate::Error {
    fn into_delta_default(self) -> DeltaError {
        let detail = self.to_string();
        crate::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            source = self,
            "kernel.default: {detail}",
        )
    }
}

/// Wrap a [`DeltaError`] back into a [`crate::Error`] via [`crate::Error::GenericError`],
/// preserving code/message/source through `std::error::Error::source()`. Used at public-API
/// boundaries whose outer signatures still return [`crate::DeltaResult`].
pub trait DeltaErrAsKernel {
    fn into_kernel_default(self) -> crate::Error;
}

impl DeltaErrAsKernel for DeltaError {
    fn into_kernel_default(self) -> crate::Error {
        crate::Error::GenericError {
            source: Box::new(self),
        }
    }
}

// ============================================================================
// or_delta at ? sites
// ============================================================================

/// Attach a [`DeltaErrorCode`] to any `Result<T, E>` where `E: std::error::Error`. The original
/// error is preserved in [`DeltaError::source`], walkable via `std::error::Error::source()`.
/// Use [`crate::delta_error!`] directly when you also need a custom message.
pub trait DeltaResultExt<T> {
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError>;
}

impl<T, E> DeltaResultExt<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError> {
        self.map_err(|e| DeltaError::__new(code, e.to_string(), Some(Box::new(e) as BoxedSource)))
    }
}

// ============================================================================
// Macros
// ============================================================================

/// Build a [`DeltaError`] value. See module-level docs for syntax.
#[macro_export]
macro_rules! delta_error {
    // bare code, no message, no source
    ($code:expr $(,)?) => {
        $crate::plans::errors::DeltaError::__new(
            $code,
            ::std::string::String::new(),
            ::std::option::Option::None,
        )
    };
    // `source = expr` only, no message — terminator `$` disambiguates from the variant that
    // requires a format string.
    ($code:expr, source = $src:expr $(,)?) => {{
        // Bind once so non-`Copy` move-consume expressions (e.g. `DataFusionError::External(x)`)
        // aren't substituted twice by the macro expansion.
        let __src = $src;
        $crate::plans::errors::DeltaError::__new(
            $code,
            ::std::string::ToString::to_string(&__src),
            ::std::option::Option::Some(
                ::std::boxed::Box::new(__src) as $crate::plans::errors::BoxedSource,
            ),
        )
    }};
    // `source = expr, "fmt", args...`
    ($code:expr, source = $src:expr, $fmt:literal $($args:tt)*) => {
        $crate::plans::errors::DeltaError::__new(
            $code,
            ::std::format!($fmt $($args)*),
            ::std::option::Option::Some(
                ::std::boxed::Box::new($src) as $crate::plans::errors::BoxedSource,
            ),
        )
    };
    // `"fmt", args...` — no source
    ($code:expr, $fmt:literal $($args:tt)*) => {
        $crate::plans::errors::DeltaError::__new(
            $code,
            ::std::format!($fmt $($args)*),
            ::std::option::Option::None,
        )
    };
}

/// Early-return `Err(delta_error!(...))` from the enclosing function.
#[macro_export]
macro_rules! bail_delta {
    ($($tt:tt)*) => {
        return ::std::result::Result::Err($crate::delta_error!($($tt)*))
    };
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn display_includes_code_name_and_message() {
        let err = DeltaError::__new(DeltaErrorCode::DeltaTableNotFound, "t1".into(), None);
        let s = err.to_string();
        assert!(s.contains("DELTA_TABLE_NOT_FOUND"), "got: {s}");
        assert!(s.contains("t1"), "got: {s}");
    }

    #[test]
    fn macro_with_format_args() {
        let err = crate::delta_error!(
            DeltaErrorCode::DeltaTableNotFound,
            "table `{}` does not exist",
            "my_table",
        );
        assert_eq!(err.code, DeltaErrorCode::DeltaTableNotFound);
        assert!(err.message.contains("my_table"));
        assert!(err.source.is_none());
    }

    #[test]
    fn macro_lifts_source_kwarg() {
        let io = std::io::Error::other("boom");
        let err = crate::delta_error!(
            DeltaErrorCode::DeltaFileNotFound,
            source = io,
            "file `{}` missing",
            "/tmp/x",
        );
        assert!(err.message.contains("/tmp/x"));
        assert!(err.source().expect("source").to_string().contains("boom"));
    }

    #[test]
    fn macro_accepts_named_format_args() {
        let version: u64 = 42;
        let err = crate::delta_error!(DeltaErrorCode::DeltaVersionInvalid, "version={version}",);
        assert!(err.message.contains("42"));
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

    #[test]
    fn bail_delta_returns_error_result() {
        fn check() -> Result<(), DeltaError> {
            crate::bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "snapshot.rebuild: protocol missing",
            );
        }
        let err = check().unwrap_err();
        assert_eq!(err.code, DeltaErrorCode::DeltaCommandInvariantViolation);
    }
}
