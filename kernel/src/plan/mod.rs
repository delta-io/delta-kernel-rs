//! Declarative plan algebra for data-intensive operations.
//!
//! This module defines a SQL-like plan algebra that describes *what* to do without prescribing
//! *how*. A [`PlanExecutor`] interprets the plan and performs the actual I/O and computation,
//! returning results as columnar [`EngineData`](crate::EngineData) batches via [`PlanResult`].
//!
//! The algebra will evolve over time to include relational operators (filter, project, join,
//! etc.). For now it contains simple 1-for-1 operation nodes like
//! [`DeclarativePlanNode::FileListing`].

mod executor;
mod result;
mod schema;

pub use executor::{PlanExecutor, PlanExecutorRef};
pub use result::PlanResult;
pub use schema::FILE_META_SCHEMA;
use url::Url;

/// A declarative plan node representing a data-intensive operation.
///
/// Each variant describes an operation and its parameters. The output schema of the operation
/// is documented on the variant and defined as a schema constant (e.g. [`FILE_META_SCHEMA`]).
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    /// List files at the given URL, returning file metadata as columnar data.
    ///
    /// The result contains one row per file with the schema defined by [`FILE_META_SCHEMA`]:
    /// - `path` (String): fully qualified URL of the file
    /// - `last_modified` (Long): milliseconds since Unix epoch
    /// - `size` (Long): file size in bytes
    ///
    /// Files are returned sorted lexicographically by path. If the URL is directory-like
    /// (ends with '/'), all files in that directory are listed. Otherwise, files
    /// lexicographically greater than the given path are listed.
    FileListing {
        /// The URL to list from.
        url: Url,
    },
}
