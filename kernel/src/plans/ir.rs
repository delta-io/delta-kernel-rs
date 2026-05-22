//! Intermediate representation (IR) for declarative plans.
//!
//! The top-level type is a [`Plan`], which communicates to the
//! [`PlanExecutor`](super::PlanExecutor) what to do.

use bytes::Bytes;
use url::Url;

use crate::schema::SchemaRef;
use crate::{FileMeta, FileSlice, PredicateRef};

/// Represents a set of instructions that the [`PlanExecutor`](super::PlanExecutor) should perform.
///
/// It can either be an IO operation or a declarative query.
#[derive(Debug)]
pub enum Plan {
    /// A singular I/O operation that returns concretely typed data such as bytes or file metadata.
    IOOperation(IOOperation),
    /// A query on relational-like data, expressed through a plan algebra.
    QueryPlan(QueryPlan),
}

/// A singular I/O operation that returns typed data such as raw bytes or file metadata.
///
/// Each variant describes an operation and its parameters. The shape of the result it produces
/// is documented on the variant in terms of [`PlanResult`](super::PlanResult).
#[derive(Debug)]
pub enum IOOperation {
    /// Recursively list files at the given URL.
    ///
    /// Should return a [`PlanResult::FileMetaIter`](super::PlanResult::FileMetaIter) with one
    /// entry per file. Files are returned sorted lexicographically by path. If the URL is
    /// directory-like (ends with '/'), all files (recursively)in that directory are listed.
    /// Otherwise, files lexicographically greater than the given path are listed.
    FileListing {
        /// The URL to list from.
        url: Url,
    },
    /// Read raw bytes from one or more files (or byte ranges within files).
    ///
    /// Each [`FileSlice`] specifies a file URL and an optional byte range. Results are returned
    /// as [`PlanResult::BytesIter`](super::PlanResult::BytesIter) in the same order as the
    /// input slices, with one Bytes buffer per file slice.
    ReadBytes {
        /// The file slices to read.
        files: Vec<FileSlice>,
    },
    /// Write raw bytes to a file at the given URL.
    ///
    /// Returns [`PlanResult::Unit`](super::PlanResult::Unit) on success. If `overwrite` is false
    /// and the file already exists, the executor should return
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
    /// Returns [`PlanResult::FileMeta`](super::PlanResult::FileMeta). If the file does not
    /// exist, the executor should return an error.
    HeadFile {
        /// The URL of the file to inspect.
        url: Url,
    },
    /// Atomically copy a file from `source` to `destination`.
    ///
    /// The copy must be atomic: if `destination` already exists, the executor should return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists) without modifying it.
    /// Returns [`PlanResult::Unit`](super::PlanResult::Unit) on success.
    AtomicCopy {
        /// The URL of the file to copy from.
        source: Url,
        /// The URL to copy to. Must not already exist.
        destination: Url,
    },
}

impl IOOperation {
    /// Construct a [`Self::FileListing`] over the given URL.
    pub fn file_listing(url: Url) -> Self {
        Self::FileListing { url }
    }

    /// Construct a [`Self::ReadBytes`] over the given file slices.
    pub fn read_bytes(files: Vec<FileSlice>) -> Self {
        Self::ReadBytes { files }
    }

    /// Construct a [`Self::WriteBytes`] for the given URL.
    pub fn write_bytes(url: Url, data: Bytes, overwrite: bool) -> Self {
        Self::WriteBytes {
            url,
            data,
            overwrite,
        }
    }

    /// Construct a [`Self::HeadFile`] for the given URL.
    pub fn head_file(url: Url) -> Self {
        Self::HeadFile { url }
    }

    /// Construct a [`Self::AtomicCopy`] from `source` to `destination`.
    pub fn atomic_copy(source: Url, destination: Url) -> Self {
        Self::AtomicCopy {
            source,
            destination,
        }
    }
}

/// Representation of a query on relational data.
///
/// TODO: We expect this to evolve as we flesh out the plan algebra (e.g. towards SSA form
/// with multiple linked nodes), but for now a [`QueryPlan`] holds a single [`QueryPlanNode`].
pub type QueryPlan = QueryPlanNode;

/// A single node in a [`QueryPlan`]. Each variant describes a relational operation.
#[derive(Debug)]
pub enum QueryPlanNode {
    /// Read and parse structured data files (Parquet or JSON), returning columnar data.
    ///
    /// Returns [`PlanResult::UnboundedData`](super::PlanResult::UnboundedData) with columns
    /// matching the provided `physical_schema`. See [`JsonHandler::read_json_files`] and
    /// [`ParquetHandler::read_parquet_files`] for more details on column resolution rules and
    /// ordering contracts.
    ///
    /// [`JsonHandler::read_json_files`]: crate::JsonHandler::read_json_files
    /// [`ParquetHandler::read_parquet_files`]: crate::ParquetHandler::read_parquet_files
    Scan {
        /// The format of the files to read.
        format: ScanFileFormat,
        /// Metadata for each file to read.
        files: Vec<FileMeta>,
        /// Select list and order of columns to read.
        physical_schema: SchemaRef,
        /// Optional push-down predicate hint (the executor is free to ignore it)
        predicate: Option<PredicateRef>,
    },
}

/// The file format for a Scan operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanFileFormat {
    /// Apache Parquet format.
    Parquet,
    /// Newline-delimited JSON format.
    Json,
}
