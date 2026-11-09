//! Action source schemas, representations, and physical reads.

// Declarative callers land in separate stack layers.
#![allow(dead_code)]

mod metadata_planner;
pub(crate) mod sources;

pub(crate) use metadata_planner::MetadataPlanner;
#[cfg(test)]
pub(crate) use metadata_planner::StatsKind;

pub(crate) const VERSION: &str = "version";
