//! Structured execution errors returned by engine plan execution.
//!
//! # Layering contract
//!
//! [`EngineError`] is an **engine-facing** error. The engine produces one
//! when plan execution fails in a well-understood way; the kernel matches on
//! [`EngineError::kind`] at SM level and translates to a typed
//! [`DeltaError`](crate::plans::errors::DeltaError) via
//! [`bail_delta!`](crate::bail_delta) or `.or_delta(...)`. Kernel code MUST
//! NOT gate control flow on the *text* of `message` fields inside variants:
//! those strings are non-semantic and exist only for display.
//!
//! # Shape
//!
//! ```ignore
//! EngineError {
//!     kind: EngineErrorKind::FileNotFound { path: "..." },
//!     source: Some(Box::new(the_engine_error)),
//! }
//! ```
//!
//! `source` is in-process only. Any FFI encoder that crosses this error
//! drops the chain — the kind (and its rendered `Display`) is what
//! travels over the wire.

use crate::plans::errors::{BoxedSource, DeltaError, DeltaErrorCode};
use crate::Version;

/// A structured, serializable error from executing a `Plan`.
///
/// Combines a typed [`EngineErrorKind`] (the semantic signal) with an
/// optional boxed source (the underlying engine error), forwarded through
/// `std::error::Error::source()` via `thiserror`'s implicit `#[source]` on
/// any field named `source`.
///
/// `Clone` is implemented manually: cloning drops the `source` chain
/// (boxed `dyn Error` is not `Clone`). The kind is preserved, which is
/// what kernel call sites match on; callers that need the underlying
/// cause should walk `std::error::Error::source()` on the original
/// before cloning, or render it via [`Self::display_with_source_chain`].
#[derive(Debug, thiserror::Error)]
#[error("{kind}")]
pub struct EngineError {
    /// The semantic variant. Kernel SMs match on this.
    pub kind: EngineErrorKind,
    /// Optional in-process-only source chain.
    pub source: Option<BoxedSource>,
}

/// Semantic tag of an [`EngineError`].
///
/// Adding a new variant is the preferred way to express a new engine
/// failure — string-matching on `message` fields is explicitly forbidden.
/// `#[non_exhaustive]` keeps downstream matches forward-compatible.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EngineErrorKind {
    /// A required input relation was empty when the plan expected at least
    /// one row.
    #[error("empty input for sink: {sink_description}")]
    EmptyInput { sink_description: String },

    /// A file path referenced by the plan was not found in storage.
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// A generic I/O failure that doesn't fit a more specific variant.
    /// `message` is non-semantic — for display only.
    #[error("I/O error: {message}")]
    IoError { message: String },

    /// Engine-side failure the kernel does not classify further.
    ///
    /// No payload: diagnostic detail lives entirely in
    /// [`EngineError::source`]. This variant is what the engine emits
    /// when it has an arbitrary error to surface — the kernel does *not*
    /// need to interpret the Rust type; it just propagates the source
    /// into a [`DeltaError`](crate::plans::errors::DeltaError) with a
    /// coarse code like `DeltaCommandInvariantViolation`.
    ///
    /// Prefer constructing via [`EngineError::internal`], which
    /// attaches the originating error as `source`. When a specific cause
    /// starts recurring, add a dedicated typed variant above and migrate
    /// call sites.
    #[error("internal engine error")]
    Internal,

    /// Commit lost a put-if-absent race or catalog ratify reported a
    /// conflicting table version.
    #[error("commit conflict at version {version}")]
    CommitConflict { version: Version },
}

/// ABI-stable wire discriminant for [`EngineErrorKind`] variants.
///
/// Mirrors the
/// [`DeltaError`](crate::plans::errors::DeltaError) /
/// [`DeltaErrorCode`](crate::plans::errors::DeltaErrorCode) split:
/// [`EngineErrorKind`] owns the typed payload; `EngineErrorCode` is
/// the unit projection that crosses the FFI. `#[repr(u32)]` pins the
/// integer layout so downstream bindings can send it as a plain fixed-
/// width int; `#[non_exhaustive]` reserves space for future variants
/// without a lockstep wire break.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineErrorCode {
    Internal = 0,
    FileNotFound = 1,
    IoError = 2,
    EmptyInput = 3,
    CommitConflict = 4,
}

impl EngineErrorCode {
    /// Lift a wire `u32` into a typed code. Unknown values degrade to
    /// [`Internal`](Self::Internal), so engine additions roll out without
    /// a lockstep FFI bump.
    pub fn from_wire(disc: u32) -> Self {
        match disc {
            1 => Self::FileNotFound,
            2 => Self::IoError,
            3 => Self::EmptyInput,
            4 => Self::CommitConflict,
            _ => Self::Internal,
        }
    }

    /// Reconstruct the fieldful [`EngineErrorKind`] this code represents.
    ///
    /// Payload semantics per variant:
    /// - [`Internal`](Self::Internal) — `payload` is ignored.
    /// - [`FileNotFound`](Self::FileNotFound) — `payload` is the path.
    /// - [`IoError`](Self::IoError) — `payload` is the message.
    /// - [`EmptyInput`](Self::EmptyInput) — `payload` is the sink description.
    /// - [`CommitConflict`](Self::CommitConflict) — `payload` parses as [`Version`]; non-numeric
    ///   returns `None`.
    ///
    /// Returns `None` only for ill-typed payloads (today, a
    /// `CommitConflict` whose payload can't parse as a version). FFI
    /// callers should degrade to [`EngineErrorKind::Internal`] with
    /// the original payload attached as source.
    pub fn construct(self, payload: String) -> Option<EngineErrorKind> {
        match self {
            Self::Internal => Some(EngineErrorKind::Internal),
            Self::FileNotFound => Some(EngineErrorKind::FileNotFound { path: payload }),
            Self::IoError => Some(EngineErrorKind::IoError { message: payload }),
            Self::EmptyInput => Some(EngineErrorKind::EmptyInput {
                sink_description: payload,
            }),
            Self::CommitConflict => payload
                .parse::<Version>()
                .ok()
                .map(|version| EngineErrorKind::CommitConflict { version }),
        }
    }
}

impl EngineErrorKind {
    /// FFI-visible discriminant; inverse of [`EngineErrorCode::construct`].
    pub fn code(&self) -> EngineErrorCode {
        match self {
            Self::Internal => EngineErrorCode::Internal,
            Self::FileNotFound { .. } => EngineErrorCode::FileNotFound,
            Self::IoError { .. } => EngineErrorCode::IoError,
            Self::EmptyInput { .. } => EngineErrorCode::EmptyInput,
            Self::CommitConflict { .. } => EngineErrorCode::CommitConflict,
        }
    }
}

impl EngineError {
    /// Build a sourceless [`EngineError`]. The fast path when the engine
    /// has no underlying error to attach.
    pub fn new(kind: EngineErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Render this error together with its full source chain for
    /// diagnostics. Formatted as `"{kind}: {source}: {source_of_source}: ..."`.
    ///
    /// Distinct from [`Self::to_string`] (which only renders `kind`):
    /// [`EngineErrorKind::Internal`] carries its entire diagnostic payload
    /// in `source`, so `to_string()` collapses to the static string
    /// `"internal engine error"` when a wire-reconstructed `Internal`
    /// comes back from the engine across the FFI. SM bodies that lift an
    /// `EngineError` returned by
    /// [`Phase::execute`](crate::plans::state_machines::framework::coroutine::phase::Phase::execute)
    /// into a [`DeltaError`](crate::plans::errors::DeltaError) `detail`
    /// must use this method instead of `to_string`, or the underlying
    /// cause is silently swallowed and the log line reads just
    /// `"internal engine error"`.
    pub fn display_with_source_chain(&self) -> String {
        use std::error::Error;
        let mut out = self.kind.to_string();
        let mut cur: Option<&(dyn Error + 'static)> = self.source();
        while let Some(s) = cur {
            out.push_str(": ");
            out.push_str(&s.to_string());
            cur = s.source();
        }
        out
    }

    /// Attach a boxed source to an existing `EngineError`. Fluent.
    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    /// Construct an [`EngineErrorKind::Internal`] with the originating
    /// error attached as `source`. Use this whenever the engine has a
    /// failure that doesn't fit a typed variant — kernel call sites
    /// inspect `source` (via `std::error::Error::source()`) if they need
    /// the underlying message or type.
    ///
    /// Adding a dedicated typed variant is preferred whenever a given
    /// cause starts recurring.
    pub fn internal<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            kind: EngineErrorKind::Internal,
            source: Some(Box::new(err)),
        }
    }

    /// Lift this engine error into a kernel [`DeltaError`] tagged with `code`.
    ///
    /// The full engine source chain is preserved via `source =` on the resulting `DeltaError`,
    /// and the detail string is built with
    /// [`display_with_source_chain`](Self::display_with_source_chain) rather than
    /// [`to_string`](Self::to_string) — the latter would collapse [`EngineErrorKind::Internal`]
    /// to the static string `"internal engine error"` and silently drop the underlying cause.
    ///
    /// SM bodies match on [`Self::kind`] and pick a semantically appropriate [`DeltaErrorCode`].
    /// Bodies that don't (yet) discriminate by kind may pass
    /// [`DeltaErrorCode::DeltaCommandInvariantViolation`] as a conservative default.
    pub fn into_delta(self, code: DeltaErrorCode) -> DeltaError {
        let detail = self.display_with_source_chain();
        crate::delta_error!(code, source = self, "{detail}")
    }
}

impl From<EngineErrorKind> for EngineError {
    fn from(kind: EngineErrorKind) -> Self {
        Self::new(kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_delegates_to_kind() {
        let err = EngineError::new(EngineErrorKind::FileNotFound {
            path: "/tmp/x".into(),
        });
        assert_eq!(err.to_string(), "file not found: /tmp/x");
    }

    #[test]
    fn source_is_exposed_via_std_error() {
        use std::error::Error;
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err = EngineError::new(EngineErrorKind::FileNotFound {
            path: "/tmp/x".into(),
        })
        .with_source(inner);
        assert!(err.source().unwrap().to_string().contains("missing"));
    }

    #[test]
    fn display_with_source_chain_renders_internal_payload() {
        let err = EngineError::internal(std::io::Error::other(
            "Non-Results plan failed: IllegalStateException: boom",
        ));
        let rendered = err.display_with_source_chain();
        assert!(
            rendered.contains("internal engine error"),
            "kind prefix missing: {rendered}"
        );
        assert!(
            rendered.contains("Non-Results plan failed: IllegalStateException: boom"),
            "source payload missing: {rendered}"
        );
    }

    #[test]
    fn display_with_source_chain_is_stable_when_no_source() {
        let err = EngineError::new(EngineErrorKind::FileNotFound {
            path: "/tmp/x".into(),
        });
        assert_eq!(err.display_with_source_chain(), err.to_string());
    }

    #[test]
    fn internal_tags_kind_and_preserves_source() {
        use std::error::Error;
        let io = std::io::Error::other("boom");
        let err = EngineError::internal(io);
        assert_eq!(err.kind, EngineErrorKind::Internal);
        let src = err.source().expect("source preserved");
        assert!(src.to_string().contains("boom"));
    }

    #[test]
    fn from_kind_produces_sourceless_error() {
        let err: EngineError = EngineErrorKind::EmptyInput {
            sink_description: "sink".into(),
        }
        .into();
        assert!(err.source.is_none());
    }

    #[test]
    fn code_and_construct_roundtrip_for_every_variant() {
        let samples = [
            (
                EngineErrorKind::EmptyInput {
                    sink_description: "sink".into(),
                },
                "sink",
            ),
            (
                EngineErrorKind::FileNotFound {
                    path: "/tmp/x".into(),
                },
                "/tmp/x",
            ),
            (
                EngineErrorKind::IoError {
                    message: "boom".into(),
                },
                "boom",
            ),
            (EngineErrorKind::Internal, ""),
            (EngineErrorKind::CommitConflict { version: 7 }, "7"),
        ];
        for (kind, payload) in samples {
            let code = kind.code();
            let round_tripped = code
                .construct(payload.into())
                .expect("known code must round-trip");
            assert_eq!(kind, round_tripped, "round-trip drift for {code:?}");
        }
    }
}
