//! Typed error surface for the declarative plans layer.
//!
//! [`DeltaError`] is the error type used by state machines and the plan
//! executor. It coexists with [`crate::Error`]; conversion at the boundary
//! goes through the named bridges [`KernelErrAsDelta`] / [`DeltaErrAsKernel`]
//! — deliberately no `From` impls, so the conversion sites are grep-able.
//!
//! # Construction
//!
//! Three supported paths:
//!
//! - [`delta_error!`] — build a value. First arg is a [`DeltaErrorCode`], followed by `name =
//!   value` template params. The reserved name `source` is lifted into [`DeltaError::source`].
//! - [`bail_delta!`] — early-return from an enclosing function.
//! - [`DeltaResultExt::or_delta`] — attach a code at a `?` site, preserving the underlying error as
//!   [`DeltaError::source`].
//!
//! ```ignore
//! use delta_kernel::plans::errors::{DeltaErrorCode, DeltaResultExt};
//!
//! bail_delta!(DeltaErrorCode::DeltaTableNotFound, tableName = "t");
//! let v: u64 = "42".parse().or_delta(DeltaErrorCode::DeltaVersionInvalid)?;
//! let e = delta_error!(DeltaErrorCode::DeltaFileNotFound, path = "/x", source = io_err);
//! ```

use std::backtrace::Backtrace;

// ============================================================================
// DeltaErrorCode — macro-driven declaration
// ============================================================================

/// Declarative table of Delta error codes. Each row binds a variant to its
/// FFI-stable discriminant, SCREAMING_SNAKE name, SQLSTATE, and message
/// template with `<name>` placeholders.
///
/// Adding a code is a one-line edit. The parity test at the bottom of this
/// module verifies the name + sqlstate against `delta-error-codes.json`,
/// the verbatim Databricks upstream catalog.
///
/// Discriminants are explicit because the enum is `#[repr(u32)]` for FFI
/// ABI stability. Reassigning or reusing a discriminant is a breaking
/// change; pick the next unused integer.
macro_rules! delta_codes {
    (
        $(
            $( #[$meta:meta] )*
            $variant:ident = $disc:literal,
            $name:literal,
            $sqlstate:literal,
            $template:literal
        ),+
        $(,)?
    ) => {
        /// Typed identifier for a Delta error.
        ///
        /// `#[repr(u32)]` pins the integer layout so engines across the FFI
        /// boundary can branch on the same variant without a parallel wire
        /// enum. `#[non_exhaustive]` reserves space for future additions —
        /// downstream consumers must treat unknown integers as opaque.
        #[repr(u32)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum DeltaErrorCode {
            $( $( #[$meta] )* $variant = $disc ),+
        }

        impl DeltaErrorCode {
            /// SCREAMING_SNAKE identifier (matches the key in the upstream
            /// Databricks catalog). Stable; used by FFI and the parity test.
            pub fn name(&self) -> &'static str {
                match self { $( Self::$variant => $name ),+ }
            }

            /// Standard 5-character SQLSTATE, sourced from the upstream catalog.
            pub fn sqlstate(&self) -> &'static str {
                match self { $( Self::$variant => $sqlstate ),+ }
            }

            /// Message template with `<name>` placeholders. Reworded from
            /// Databricks upstream (whose positional placeholders don't
            /// round-trip). Parity is enforced on name + sqlstate, not wording.
            pub fn message_template(&self) -> &'static str {
                match self { $( Self::$variant => $template ),+ }
            }
        }

        #[cfg(test)]
        const ALL_CODES: &[DeltaErrorCode] = &[ $( DeltaErrorCode::$variant ),+ ];
    };
}

delta_codes! {
    /// `DELTA_COMMAND_INVARIANT_VIOLATION` — internal-error catch-all for SMs
    /// detecting an impossible state.
    ///
    /// Discriminant `0` is deliberate: a zero-initialized wire value decodes
    /// to "invariant violation", the safest fallback for engines that haven't
    /// populated the code slot.
    DeltaCommandInvariantViolation = 0, "DELTA_COMMAND_INVARIANT_VIOLATION", "XXKDS",
        "Internal invariant violated in `<operation>`: <detail>. This is a kernel bug — please file a report.",

    /// Delta table `<tableName>` does not exist.
    DeltaTableNotFound = 1, "DELTA_TABLE_NOT_FOUND", "42P01",
        "Delta table `<tableName>` does not exist.",

    /// `<path>` is not a Delta table.
    DeltaMissingDeltaTable = 2, "DELTA_MISSING_DELTA_TABLE", "42P01",
        "`<path>` is not a Delta table.",

    /// `<path>` doesn't exist, or is not a Delta table.
    DeltaPathDoesNotExist = 3, "DELTA_PATH_DOES_NOT_EXIST", "42K03",
        "`<path>` doesn't exist, or is not a Delta table.",

    /// Requested version is outside the available range.
    DeltaVersionNotFound = 4, "DELTA_VERSION_NOT_FOUND", "22003",
        "Cannot time-travel Delta table to version <version>. Available versions: [<earliest>, <latest>].",

    /// A provided version string / number is not parseable.
    DeltaVersionInvalid = 5, "DELTA_VERSION_INVALID", "42815",
        "The provided version (<version>) is not a valid Delta version.",

    /// A commit file needed to reconstruct state is missing.
    DeltaLogFileNotFound = 6, "DELTA_LOG_FILE_NOT_FOUND", "42K03",
        "Unable to retrieve Delta log files to reconstruct version starting from checkpoint at <path>.",

    /// A data file referenced by the log is missing.
    DeltaFileNotFound = 7, "DELTA_FILE_NOT_FOUND", "42K03",
        "File path <path> not found in storage.",

    /// A multipart checkpoint is missing shards.
    DeltaMissingPartFiles = 8, "DELTA_MISSING_PART_FILES", "42KD6",
        "Couldn't find all part files of checkpoint version <version>.",

    /// Protocol / metadata could not be reconstructed.
    DeltaStateRecoverError = 9, "DELTA_STATE_RECOVER_ERROR", "XXKDS",
        "<component> of this Delta table could not be recovered while reconstructing version <version>. Did files in _delta_log get deleted out-of-band?",

    /// The table requires a protocol the kernel doesn't support.
    DeltaInvalidProtocolVersion = 10, "DELTA_INVALID_PROTOCOL_VERSION", "KD004",
        "Unsupported Delta protocol version for `<path>`: table requires reader=<requiredReader>/writer=<requiredWriter>, kernel supports reader=<supportedReader>/writer=<supportedWriter>.",

    /// A commit raced with another transaction.
    DeltaConcurrentWrite = 11, "DELTA_CONCURRENT_WRITE", "2D521",
        "A concurrent transaction committed at version <version> since this transaction read the table. Retry the operation.",

    /// Attempt to create a Delta log that already exists.
    DeltaLogAlreadyExists = 12, "DELTA_LOG_ALREADY_EXISTS", "42K04",
        "A Delta log already exists at <path>.",
}

// ============================================================================
// DeltaError
// ============================================================================

/// Boxed, sendable source error held by [`DeltaError::source`].
pub type BoxedSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Typed Delta error with optional source chain and backtrace.
///
/// `Display` emits `[<CODE_NAME>] <rendered_message>`, rendered on demand
/// from [`DeltaErrorCode::message_template`] and [`Self::params`]. The
/// `source` field is forwarded via `std::error::Error::source()` so standard
/// ecosystem walkers see the full chain.
#[derive(Debug, thiserror::Error)]
#[error("[{}] {}", self.code.name(), self.rendered_message())]
pub struct DeltaError {
    /// Stable typed identifier.
    pub code: DeltaErrorCode,

    /// Ordered template substitutions. Left-to-right; linear scan on render.
    pub params: Vec<(&'static str, String)>,

    /// Optional underlying error, forwarded via `std::error::Error::source()`.
    pub source: Option<BoxedSource>,

    /// `Some(Backtrace::capture())` — a no-op without `RUST_BACKTRACE=1`,
    /// so the "always on" default is free in production.
    pub backtrace: Option<Backtrace>,
}

impl DeltaError {
    /// Low-level constructor used by [`delta_error!`]. `pub` (not
    /// `pub(crate)`) because macro expansion happens at caller scope; not
    /// part of the public contract.
    #[doc(hidden)]
    pub fn __new_from_macro(
        code: DeltaErrorCode,
        params: Vec<(&'static str, String)>,
        source: Option<BoxedSource>,
    ) -> Self {
        Self {
            code,
            params,
            source,
            backtrace: Some(Backtrace::capture()),
        }
    }

    /// Substitute `<name>` placeholders in the code's template with
    /// [`Self::params`]. Unknown placeholders are emitted verbatim so
    /// rendering bugs are visible.
    pub fn rendered_message(&self) -> String {
        render_template(self.code.message_template(), &self.params)
    }
}

/// Substitute `<name>` placeholders in `template` against `params`
/// (first-match-wins). Unknown names are emitted verbatim (`<unknownName>`).
/// Stray `<` without a matching `>` is emitted as-is.
pub(crate) fn render_template(template: &str, params: &[(&'static str, String)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('<') {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        match after_open.find('>') {
            Some(close) => {
                let name = &after_open[..close];
                let tail = &after_open[close + 1..];
                match params.iter().find(|(n, _)| *n == name) {
                    Some((_, v)) => out.push_str(v),
                    None => {
                        out.push('<');
                        out.push_str(name);
                        out.push('>');
                    }
                }
                rest = tail;
            }
            None => {
                out.push_str(&rest[open..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

// ============================================================================
// Bridges — kernel::Error <-> DeltaError
// ============================================================================

/// Lift a [`crate::Error`] into a [`DeltaError`] tagged with
/// [`DeltaErrorCode::DeltaCommandInvariantViolation`]. Use when a more
/// specific code isn't yet known; upgrading to a better
/// `delta_error!(code, source = e, ...)` is a drop-in refactor.
pub trait KernelErrAsDelta {
    fn into_delta_default(self) -> DeltaError;
}

impl KernelErrAsDelta for crate::Error {
    fn into_delta_default(self) -> DeltaError {
        // Populate `<operation>` / `<detail>` so the rendered message carries
        // the underlying kernel error text — without this, the final string
        // still contains literal `<operation>` / `<detail>` placeholders.
        let detail = self.to_string();
        crate::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "kernel.default",
            detail = detail,
            source = self,
        )
    }
}

/// Wrap a [`DeltaError`] back into a [`crate::Error`] via
/// [`crate::Error::GenericError`], preserving the code / params / source
/// chain through `std::error::Error::source()`. Used at public-API
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

/// Attach a [`DeltaErrorCode`] to any `Result<T, E>` where `E: std::error::Error`.
/// The original error is preserved in [`DeltaError::source`], walkable via
/// `std::error::Error::source()`. Use [`delta_error!`] directly when you
/// also need template parameters.
pub trait DeltaResultExt<T> {
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError>;
}

impl<T, E> DeltaResultExt<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn or_delta(self, code: DeltaErrorCode) -> Result<T, DeltaError> {
        self.map_err(|e| {
            DeltaError::__new_from_macro(code, Vec::new(), Some(Box::new(e) as BoxedSource))
        })
    }
}

// ============================================================================
// Macros
// ============================================================================

/// Build a [`DeltaError`] value. See module-level docs for syntax.
#[macro_export]
macro_rules! delta_error {
    ($code:expr $(, $name:ident = $value:expr)* $(,)?) => {{
        let mut __params: ::std::vec::Vec<(&'static str, ::std::string::String)> =
            ::std::vec::Vec::new();
        #[allow(unused_mut)]
        let mut __source: ::std::option::Option<$crate::plans::errors::BoxedSource> =
            ::std::option::Option::None;
        $( $crate::__delta_error_bind_kwarg!(@one __params __source $name = $value); )*
        $crate::plans::errors::DeltaError::__new_from_macro($code, __params, __source)
    }};
}

/// Early-return `Err(delta_error!(...))` from the enclosing function.
#[macro_export]
macro_rules! bail_delta {
    ($($tt:tt)*) => {
        return ::std::result::Result::Err($crate::delta_error!($($tt)*))
    };
}

/// Internal: dispatch one `name = value` macro kwarg into the params vec or
/// the source slot. `#[macro_export]` only because [`delta_error!`] references
/// it by path at caller scope.
#[doc(hidden)]
#[macro_export]
macro_rules! __delta_error_bind_kwarg {
    // `source = expr` — lifted into DeltaError.source. First arm so it matches
    // before the generic ident arm below.
    (@one $params:ident $source:ident source = $value:expr) => {
        $source = ::std::option::Option::Some(
            ::std::boxed::Box::new($value) as $crate::plans::errors::BoxedSource
        );
    };
    // General case: template parameter.
    (@one $params:ident $source:ident $name:ident = $value:expr) => {
        $params.push((
            stringify!($name),
            ::std::string::ToString::to_string(&$value),
        ));
    };
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    // --- render_template ----------------------------------------------------

    #[rstest::rstest]
    #[case::substitutes_known_placeholders(
        "hello <name>, version <v>",
        vec![("name", "world"), ("v", "42")],
        "hello world, version 42"
    )]
    #[case::leaves_unknown_placeholder_verbatim(
        "a=<x>, b=<y>",
        vec![("x", "1")],
        "a=1, b=<y>"
    )]
    #[case::handles_stray_open_bracket("a < b", vec![], "a < b")]
    fn render_template_cases(
        #[case] template: &str,
        #[case] params: Vec<(&'static str, &'static str)>,
        #[case] expected: &str,
    ) {
        let owned: Vec<(&str, String)> =
            params.into_iter().map(|(k, v)| (k, v.to_string())).collect();
        let out = render_template(template, &owned);
        assert_eq!(out, expected);
    }

    // --- DeltaError Display + source ----------------------------------------

    #[test]
    fn display_includes_code_name_and_rendered_message() {
        let err = DeltaError::__new_from_macro(
            DeltaErrorCode::DeltaTableNotFound,
            vec![("tableName", "t1".into())],
            None,
        );
        let s = err.to_string();
        assert!(s.contains("DELTA_TABLE_NOT_FOUND"), "got: {s}");
        assert!(s.contains("t1"), "got: {s}");
    }

    // --- Macros -------------------------------------------------------------

    #[test]
    fn delta_error_macro_captures_template_params() {
        let err = crate::delta_error!(DeltaErrorCode::DeltaTableNotFound, tableName = "my_table",);
        assert_eq!(err.code, DeltaErrorCode::DeltaTableNotFound);
        assert_eq!(err.params, vec![("tableName", "my_table".to_string())]);
        assert!(err.source.is_none());
        assert!(format!("{err}").contains("my_table"));
    }

    #[test]
    fn delta_error_macro_lifts_source_kwarg() {
        let io = std::io::Error::other("boom");
        let err = crate::delta_error!(
            DeltaErrorCode::DeltaFileNotFound,
            path = "/tmp/x",
            source = io,
        );
        assert_eq!(err.params.len(), 1);
        let src = err.source().expect("source populated");
        assert!(src.to_string().contains("boom"));
    }

    #[test]
    fn delta_error_macro_accepts_any_displayable_value() {
        let err = crate::delta_error!(DeltaErrorCode::DeltaVersionInvalid, version = 42u64,);
        assert_eq!(err.params, vec![("version", "42".to_string())]);
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
                operation = "snapshot.rebuild",
                detail = "protocol missing",
            );
        }
        let err = check().unwrap_err();
        assert_eq!(err.code, DeltaErrorCode::DeltaCommandInvariantViolation);
    }

    // --- Parity against upstream catalog ------------------------------------

    const UPSTREAM_JSON: &str = include_str!("delta-error-codes.json");

    /// Every [`DeltaErrorCode`] variant must appear in the upstream Databricks
    /// catalog with the declared `sqlState`. Adding a fabricated code fails
    /// this test; fix by either removing the variant or matching to an
    /// existing upstream code.
    #[test]
    fn every_code_exists_in_upstream_catalog_with_matching_sqlstate() {
        let upstream: serde_json::Value =
            serde_json::from_str(UPSTREAM_JSON).expect("upstream catalog must be valid JSON");
        let root = upstream.as_object().expect("upstream root is an object");
        for &code in ALL_CODES {
            let entry = root.get(code.name()).unwrap_or_else(|| {
                panic!(
                    "DeltaErrorCode::{:?} (name={}) is NOT present in upstream \
                     delta-error-codes.json — pick an existing upstream code or drop the variant.",
                    code,
                    code.name(),
                );
            });
            let upstream_sqlstate = entry
                .get("sqlState")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| panic!("upstream {} has no sqlState", code.name()));
            assert_eq!(
                code.sqlstate(),
                upstream_sqlstate,
                "sqlState drift for {}: ours={}, upstream={}",
                code.name(),
                code.sqlstate(),
                upstream_sqlstate,
            );
        }
    }
}
