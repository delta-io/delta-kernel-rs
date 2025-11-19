//! Phase-based distributed log replay implementation.
//!
//! This module provides a composable phase-driven architecture for log replay
//! where each phase is an independent iterator that returns transition enums
//! indicating what comes next.

pub(crate) mod commit;
pub(crate) mod manifest;
pub(crate) mod sidecar;

pub(crate) use commit::{AfterCommit, CommitPhase, NextPhase};
pub(crate) use manifest::{AfterManifest, ManifestPhase};
pub(crate) use sidecar::SidecarPhase;

