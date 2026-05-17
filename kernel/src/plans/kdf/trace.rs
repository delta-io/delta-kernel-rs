//! Execution-time context for KDF handles.
//!
//! [`TraceContext`] is the "where am I running" identity — set by the
//! state-machine framework when it hands a plan to the executor. It lives
//! on every [`crate::plans::kdf::Handle`] and on every
//! [`FinishedHandle`](crate::plans::kdf::FinishedHandle) submitted into a
//! `PhaseState`.
//!
//! Distinct from [`crate::plans::kdf::KdfStateToken`]:
//!
//! - `KdfStateToken` = "which KDF node in the plan" — stamped at plan-build time, lives in the plan
//!   tree.
//! - `TraceContext` = "which SM/phase is executing" — set at phase-execute time, lives on handles.
//!
//! Used by:
//! - **Tracing**: framework-level `#[tracing::instrument]` attributes include the context as span
//!   fields.
//! - **Safety checks**: the phase-state container (landing with the state-machine framework stack)
//!   rejects handles whose ctx doesn't match the phase's — catches stale handles or cross-SM
//!   contamination.
//! - **Error messages**: `DeltaError` detail strings render ctx so failures are attributable to the
//!   enclosing SM/phase.

/// Execution-time identity of a KDF's containing phase.
///
/// Fields are `String`-backed (not `&'static str`) so the context survives
/// across SM boundaries.
///
/// The `partition` field is NOT here — partitions vary per-handle within
/// one phase. It lives directly on [`crate::plans::kdf::Handle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// Kernel state-machine name, e.g. `"snapshot"`, `"scan.streaming"`,
    /// `"dml.insert"`. Set once per SM at the top of the coroutine.
    pub sm: String,

    /// Phase name within the SM, e.g. `"checkpoint_hint"`,
    /// `"commit_replay"`. Set by `Phase::execute(prep, "phase_name")`.
    pub phase: String,
}

impl std::fmt::Display for TraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.sm, self.phase)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(sm: &str, phase: &str) -> TraceContext {
        TraceContext {
            sm: sm.to_string(),
            phase: phase.to_string(),
        }
    }

    #[test]
    fn display_formats_as_sm_phase() {
        let c = ctx("snapshot", "checkpoint_hint");
        assert_eq!(c.to_string(), "snapshot::checkpoint_hint");
    }

    #[test]
    fn equality_distinguishes_sm_and_phase() {
        let a = ctx("snapshot", "list_files");
        let b = ctx("snapshot", "load_metadata");
        let c = ctx("scan", "list_files");
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
