//! Structured execution errors returned by engine plan execution.
//!
//! [`EngineError`] is **engine-facing**: the engine produces one when plan execution fails in a
//! well-understood way, and SM bodies match on [`EngineError::kind`] to react. Crossing into the
//! kernel-facing `DeltaError` happens at the call site (e.g. `DeltaError::invariant(engine_err)`),
//! which keeps the `EngineError` as the source.
//!
//! Kernel code MUST NOT gate control flow on the *text* of `message` fields inside variants:
//! those strings are non-semantic and exist only for display.

use std::error::Error;

use crate::plans::errors::BoxedSource;
use crate::Version;

/// A structured error from executing a `Plan`. Pairs a typed [`EngineErrorKind`] (the semantic
/// signal SMs match on) with an optional source chain forwarded through
/// `Error::source()`.
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
/// Adding a new variant is the preferred way to express a new engine failure -- string-matching
/// on `message` fields is explicitly forbidden. `#[non_exhaustive]` reserves space for additional
/// variants without breaking callers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EngineErrorKind {
    /// A required input relation was empty when the plan expected at least one row.
    #[error("empty input for sink: {sink_description}")]
    EmptyInput { sink_description: String },

    /// A file path referenced by the plan was not found in storage.
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// A generic I/O failure that doesn't fit a more specific variant. `message` is
    /// non-semantic -- for display only.
    #[error("I/O error: {message}")]
    IoError { message: String },

    /// Engine-side failure the kernel does not classify further. Diagnostic detail lives in
    /// [`EngineError::source`]; construct via [`EngineError::internal`].
    #[error("internal engine error")]
    Internal,

    /// Commit lost a put-if-absent race or catalog ratify reported a conflicting table version.
    #[error("commit conflict at version {version}")]
    CommitConflict { version: Version },
}

impl EngineError {
    /// Build a sourceless [`EngineError`]. The fast path when the engine
    /// has no underlying error to attach.
    pub fn new(kind: EngineErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Render this error together with its full source chain. Formatted as
    /// `"{kind}: {source}: {source_of_source}: ..."`.
    ///
    /// Distinct from [`ToString::to_string`] (which only renders `kind`):
    /// [`EngineErrorKind::Internal`] carries its entire diagnostic payload in `source`, so
    /// `to_string()` collapses to the static `"internal engine error"`. Use this method for
    /// any diagnostic detail derived from an `Internal`.
    pub fn display_with_source_chain(&self) -> String {
        let mut out = self.kind.to_string();
        let mut cur: Option<&(dyn Error + 'static)> = self.source();
        while let Some(s) = cur {
            out.push_str(": ");
            out.push_str(&s.to_string());
            cur = s.source();
        }
        out
    }

    /// Construct an [`EngineErrorKind::Internal`] with the originating
    /// error attached as `source`. Use this whenever the engine has a
    /// failure that doesn't fit a typed variant -- kernel call sites
    /// inspect `source` (via `Error::source()`) if they need
    /// the underlying message or type.
    ///
    /// Adding a dedicated typed variant is preferred whenever a given
    /// cause starts recurring.
    pub fn internal<E>(err: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind: EngineErrorKind::Internal,
            source: Some(Box::new(err)),
        }
    }
}

impl From<EngineErrorKind> for EngineError {
    fn from(kind: EngineErrorKind) -> Self {
        Self::new(kind)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use super::*;

    #[test]
    fn display_delegates_to_kind() {
        let err = EngineError::new(EngineErrorKind::FileNotFound {
            path: "/tmp/x".into(),
        });
        assert_eq!(err.to_string(), "file not found: /tmp/x");
    }

    #[test]
    fn display_with_source_chain_renders_internal_payload() {
        let err = EngineError::internal(io::Error::other(
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
        let io = io::Error::other("boom");
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
}
