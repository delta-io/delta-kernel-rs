//! Declarative plan algebra for data-intensive operations.
//!
//! This module defines a SQL-like plan algebra that describes *what* to do without prescribing
//! *how*. A [`PlanExecutor`] interprets the plan and performs the actual I/O and computation,
//! returning results via [`PlanResult`].
//!
//! The algebra will evolve over time to include relational operators (filter, project, join,
//! etc.). For now it contains simple 1-for-1 operation nodes that mirror [`StorageHandler`]
//! methods.
//!
//! [`StorageHandler`]: crate::StorageHandler

mod executor;
mod result;
mod schema;

use bytes::Bytes;
pub use executor::{PlanExecutor, PlanExecutorRef};
pub use result::PlanResult;
pub use schema::FILE_META_SCHEMA;
use url::Url;

use crate::FileSlice;

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
    /// Read raw bytes from one or more files (or byte ranges within files).
    ///
    /// Each [`FileSlice`] specifies a file URL and an optional byte range. Results are returned
    /// as [`PlanResult::ByteStream`] in the same order as the input slices.
    ReadBytes {
        /// The file slices to read.
        files: Vec<FileSlice>,
    },
    /// Write raw bytes to a file at the given URL.
    ///
    /// Returns [`PlanResult::Unit`] on success. If `overwrite` is false and the file already
    /// exists, the executor must return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists).
    WriteBytes {
        /// The destination URL.
        url: Url,
        /// The data to write.
        data: Bytes,
        /// Whether to overwrite an existing file.
        overwrite: bool,
    },
    /// Retrieve metadata for a single file (HEAD request).
    ///
    /// Returns [`PlanResult::Data`] with a single-row batch matching [`FILE_META_SCHEMA`].
    /// If the file does not exist, the executor must return an error.
    HeadFile {
        /// The URL of the file to inspect.
        url: Url,
    },
}
