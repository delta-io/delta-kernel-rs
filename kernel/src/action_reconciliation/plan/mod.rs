//! Action source schemas, representations, and physical reads.

// Declarative callers land in separate stack layers.
#![allow(dead_code)]

pub(crate) mod action_set;
mod metadata_planner;
pub(crate) mod sources;

pub(crate) use metadata_planner::MetadataPlanner;

pub(crate) const VERSION: &str = "version";
