//! Internal factory helpers used by [`super::CommitRangeBuilder::build`].
//!
//! Mirrors the Java `CommitRangeFactory`: timestamp resolution, delta-log listing,
//! catalog-commit merging, and contiguity validation.

use super::builder::CommitRangeBuilder;
use super::commit_range::CommitRange;
use crate::{DeltaResult, Engine, Version};

/// Resolve boundaries, list the delta log, merge catalog-supplied commits, and produce
/// a fully-constructed [`CommitRange`]. Mirrors `CommitRangeFactory.create` in Java.
pub(crate) fn create(builder: CommitRangeBuilder, engine: &dyn Engine) -> DeltaResult<CommitRange> {
    let _ = (builder, engine);
    todo!("commit_range::factory::create")
}

/// Resolve the start version from the builder's start boundary.
///
/// For [`super::CommitBoundary::Version`] this is a no-op. For
/// [`super::CommitBoundary::Timestamp`] this delegates to the history manager's
/// timestamp-to-version search.
pub(crate) fn resolve_start_version(
    builder: &CommitRangeBuilder,
    engine: &dyn Engine,
) -> DeltaResult<Version> {
    let _ = (builder, engine);
    todo!("commit_range::factory::resolve_start_version")
}

/// Resolve the end version from the builder's end boundary, if specified.
///
/// Returns `Ok(None)` when no end boundary is set; the caller defaults to the latest
/// observed version after listing.
pub(crate) fn resolve_end_version_if_specified(
    builder: &CommitRangeBuilder,
    engine: &dyn Engine,
) -> DeltaResult<Option<Version>> {
    let _ = (builder, engine);
    todo!("commit_range::factory::resolve_end_version_if_specified")
}

/// Validate that `start_version <= end_version` and that any
/// `max_catalog_version` constraint is satisfied.
pub(crate) fn validate_version_range(
    start_version: Version,
    end_version: Option<Version>,
    max_catalog_version: Option<Version>,
) -> DeltaResult<()> {
    let _ = (start_version, end_version, max_catalog_version);
    todo!("commit_range::factory::validate_version_range")
}
