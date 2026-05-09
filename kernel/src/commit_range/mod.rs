//! Read a contiguous range of Delta commits without requiring a snapshot at the start version.
//!
//! A [`CommitRange`] is a *plan* over an inclusive `[start_version, end_version]` range of
//! commits. Construction performs a single delta-log listing (plus an optional merge with
//! catalog-supplied commits via [`CommitRangeBuilder::with_log_tail`]); no JSON is read until
//! the caller iterates [`CommitRange::commits`].
//!
//! Unlike [`crate::table_changes::TableChanges`], `CommitRange` does not load a snapshot at
//! `start_version`. This is the property streaming sources need: log cleanup may have removed
//! the start version's snapshot prerequisites, and per-batch protocol validation already
//! covers the safety concerns that snapshot-level validation would.
//!
//! # Example
//! ```ignore
//! use delta_kernel::commit_range::{CommitBoundary, CommitRange, DeltaAction};
//!
//! let range = CommitRange::builder_for("file:///data/T", CommitBoundary::Version(0))
//!     .with_end_boundary(CommitBoundary::Version(4))
//!     .build(engine)?;
//!
//! for commit in range.commits(engine, &[DeltaAction::Add, DeltaAction::Remove]) {
//!     let commit = commit?;
//!     println!("v={} ts={}", commit.version(), commit.timestamp());
//!     for batch in commit.into_actions() {
//!         let _batch = batch?;
//!     }
//! }
//! ```

mod actions;
mod builder;
mod commit_range;
mod factory;

pub use actions::{CommitActions, DeltaAction};
pub use builder::CommitRangeBuilder;
pub use commit_range::{CommitBoundary, CommitRange};
