//! Top-level [`Operation`] dispatch -- I/O ops and relational queries.
//!
//! The executor receives an [`Operation`] and produces a
//! [`PlanResult`](crate::plans::PlanResult).

use bytes::Bytes;
use url::Url;

use super::plan::Plan;
use crate::{FileMeta, FileSlice};

/// Represents a set of instructions that the
/// [`PlanExecutor`](crate::plans::PlanExecutor) should perform.
///
/// It can either be an IO operation or a declarative query.
#[derive(Debug)]
pub enum Operation {
    /// A singular I/O operation that returns concretely typed data such as bytes or file metadata.
    IoOperation(IoOperation),
    /// A query on relational-like data, expressed as a [`Plan`]: a DAG of
    /// [`PlanNode`](super::plan::PlanNode)s whose terminal (last) node produces the rows the
    /// engine streams to the caller.
    QueryPlan(Plan),
}

/// A singular I/O operation that returns typed data such as raw bytes or file metadata.
///
/// Each variant describes an operation and its parameters. The shape of the result it produces
/// is documented on the variant in terms of [`PlanResult`](crate::plans::PlanResult).
#[derive(Debug)]
pub enum IoOperation {
    /// Recursively list files at the given URL.
    ///
    /// Should return a [`PlanResult::FileMeta`](crate::plans::PlanResult::FileMeta) with one entry
    /// per file. See [`StorageHandler::list_from`] for more details on the ordering contract.
    ///
    /// [`StorageHandler::list_from`]: crate::StorageHandler::list_from
    FileListing { url: Url },
    /// Read raw bytes from one or more files (or byte ranges within files).
    ///
    /// Each [`FileSlice`] specifies a file URL and an optional byte range. Results are returned
    /// as [`PlanResult::Bytes`](crate::plans::PlanResult::Bytes) in the same order as the input
    /// slices, with one Bytes buffer per file slice.
    ReadBytes { files: Vec<FileSlice> },
    /// Write raw bytes to a file at the given URL.
    ///
    /// Returns [`PlanResult::Unit`](crate::plans::PlanResult::Unit) on success. If `overwrite`
    /// is false and the file already exists, the executor should return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists).
    WriteBytes {
        url: Url,
        data: Bytes,
        overwrite: bool,
    },
    /// Retrieve metadata for a single file (HEAD request).
    ///
    /// Returns [`PlanResult::FileMeta`](crate::plans::PlanResult::FileMeta) with a single entry.
    /// If the file does not exist, the executor should return an error.
    HeadFile { url: Url },
    /// Atomically copy a file from `source` to `destination`.
    ///
    /// The copy must be atomic: if `destination` already exists, the executor should return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists) without modifying it.
    /// Returns [`PlanResult::Unit`](crate::plans::PlanResult::Unit) on success.
    AtomicCopy { source: Url, destination: Url },
    /// Read the footer of a Parquet file.
    ///
    /// Returns [`PlanResult::ParquetFooter`] containing the file's footer metadata (schema and
    /// any future extensions). See [`ParquetHandler::read_parquet_footer`] for the full contract.
    ///
    /// [`PlanResult::ParquetFooter`]: crate::plans::PlanResult::ParquetFooter
    /// [`ParquetHandler::read_parquet_footer`]: crate::ParquetHandler::read_parquet_footer
    ParquetFooter { file: FileMeta },
    // TODO(#2820): add a `Delete { url: Url }` variant (plus an `IoOperation::delete`
    // constructor) so `PlanBasedStorageHandler::delete` can route through the plan path
    // instead of `unimplemented!()`.
}

impl IoOperation {
    /// Constructs an [`IoOperation::FileListing`] for the given URL.
    pub fn file_listing(url: Url) -> Self {
        Self::FileListing { url }
    }

    /// Constructs an [`IoOperation::ReadBytes`] for the given file slices.
    pub fn read_bytes(files: Vec<FileSlice>) -> Self {
        Self::ReadBytes { files }
    }

    /// Constructs an [`IoOperation::WriteBytes`] for the given URL, data, and overwrite flag.
    pub fn write_bytes(url: Url, data: Bytes, overwrite: bool) -> Self {
        Self::WriteBytes {
            url,
            data,
            overwrite,
        }
    }

    /// Constructs an [`IoOperation::HeadFile`] for the given URL.
    pub fn head_file(url: Url) -> Self {
        Self::HeadFile { url }
    }

    /// Constructs an [`IoOperation::AtomicCopy`] for the given source and destination URLs.
    pub fn atomic_copy(source: Url, destination: Url) -> Self {
        Self::AtomicCopy {
            source,
            destination,
        }
    }

    /// Constructs an [`IoOperation::ParquetFooter`] for the given file metadata.
    pub fn parquet_footer(file: FileMeta) -> Self {
        Self::ParquetFooter { file }
    }
}
