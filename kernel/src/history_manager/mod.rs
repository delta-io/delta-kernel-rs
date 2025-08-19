//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.
//!
//! # Usage
//!
//! Use this module to:
//! - Convert timestamps or timestamp ranges into Delta versions or version ranges
//! - Perform time travel queries using [`Snapshot::try_new`]
//! - Execute timestamp-based change data feed queries using [`TableChanges::try_new`]
//!
//! The history_manager module works with tables regardless of whether they have In-Commit
//! Timestamps enabled.
//!
//! # Limitations
//!
//!  All timestamp queries are limited to the state captured in the [`Snapshot`]
//! provided during construction.
//!
//! [`TableChanges::try_new`]: crate::table_changes::TableChanges::try_new

pub(crate) mod search;
