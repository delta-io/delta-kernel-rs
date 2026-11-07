//! Declarative action sources and newest-action-wins replay shared by scans and checkpoints.
//!
//! Metadata planning filters and keys each source, then prepares its stats and partition values
//! before replay. Consumers apply pruning and retention at their required boundaries.

// Declarative callers land in separate stack layers.
#![allow(dead_code)]

mod metadata_planner;
pub(crate) mod sources;
pub(crate) use metadata_planner::MetadataPlanner;
#[cfg(test)]
pub(crate) use metadata_planner::StatsKind;

pub(crate) use crate::actions::ACTION_KEY;
pub(crate) const VERSION: &str = "version";

#[cfg(test)]
mod tests;
