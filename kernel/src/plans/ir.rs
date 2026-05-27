//! Intermediate representation (IR) for declarative plans.
//!
//! The top-level type is an [`Operation`], which communicates to the
//! [`PlanExecutor`](super::PlanExecutor) what to do.

use bytes::Bytes;
use url::Url;

use crate::schema::SchemaRef;
use crate::{FileMeta, FileSlice, PredicateRef};

/// Represents a set of instructions that the [`PlanExecutor`](super::PlanExecutor)
/// should perform.
///
/// It can either be an IO operation or a declarative query.
#[derive(Debug)]
pub enum Operation {
    /// A singular I/O operation that returns concretely typed data such as bytes or file metadata.
    IoOperation(IoOperation),
    /// A query on relational-like data, expressed through a plan algebra.
    QueryPlan(QueryPlan),
}

/// A singular I/O operation that returns typed data such as raw bytes or file metadata.
///
/// Each variant describes an operation and its parameters. The shape of the result it produces
/// is documented on the variant in terms of [`PlanResult`](super::PlanResult).
#[derive(Debug)]
pub enum IoOperation {
    /// Recursively list files at the given URL.
    ///
    /// Should return a [`PlanResult::FileMeta`](super::PlanResult::FileMeta) with one
    /// entry per file. See [`StorageHandler::list_from`] for more details on the ordering
    /// contract.
    ///
    /// [`StorageHandler::list_from`]: crate::StorageHandler::list_from
    FileListing { url: Url },
    /// Read raw bytes from one or more files (or byte ranges within files).
    ///
    /// Each [`FileSlice`] specifies a file URL and an optional byte range. Results are returned
    /// as [`PlanResult::Bytes`](super::PlanResult::Bytes) in the same order as the
    /// input slices, with one Bytes buffer per file slice.
    ReadBytes { files: Vec<FileSlice> },
    /// Write raw bytes to a file at the given URL.
    ///
    /// Returns [`PlanResult::Unit`](super::PlanResult::Unit) on success. If `overwrite`
    /// is false and the file already exists, the executor should return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists).
    WriteBytes {
        url: Url,
        data: Bytes,
        overwrite: bool,
    },
    /// Retrieve metadata for a single file (HEAD request).
    ///
    /// Returns [`PlanResult::FileMeta`](super::PlanResult::FileMeta) with a single
    /// entry. If the file does not exist, the executor should return an error.
    HeadFile { url: Url },
    /// Atomically copy a file from `source` to `destination`.
    ///
    /// The copy must be atomic: if `destination` already exists, the executor should return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists) without modifying it.
    /// Returns [`PlanResult::Unit`](super::PlanResult::Unit) on success.
    AtomicCopy { source: Url, destination: Url },
    /// Read the schema from the footer of a Parquet file.
    ///
    /// Returns [`PlanResult::Schema`](super::PlanResult::Schema) containing the file's schema
    /// converted to Delta Kernel's format. See [`ParquetHandler::read_parquet_footer`] for the
    /// full contract.
    ///
    /// [`ParquetHandler::read_parquet_footer`]: crate::ParquetHandler::read_parquet_footer
    ParquetSchema { file: FileMeta },
}

impl IoOperation {
    pub fn file_listing(url: Url) -> Self {
        Self::FileListing { url }
    }

    pub fn read_bytes(files: Vec<FileSlice>) -> Self {
        Self::ReadBytes { files }
    }

    pub fn write_bytes(url: Url, data: Bytes, overwrite: bool) -> Self {
        Self::WriteBytes {
            url,
            data,
            overwrite,
        }
    }

    pub fn head_file(url: Url) -> Self {
        Self::HeadFile { url }
    }

    pub fn atomic_copy(source: Url, destination: Url) -> Self {
        Self::AtomicCopy {
            source,
            destination,
        }
    }

    pub fn parquet_schema(file: FileMeta) -> Self {
        Self::ParquetSchema { file }
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
    /// Read and parse newline-delimited JSON files, returning columnar data.
    ///
    /// Returns [`PlanResult::Data`](super::PlanResult::Data) with columns matching the
    /// provided `physical_schema`. See [`JsonHandler::read_json_files`] for more details on column
    /// resolution rules and ordering contracts.
    ///
    /// [`JsonHandler::read_json_files`]: crate::JsonHandler::read_json_files
    ScanJson {
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    },
    /// Read and parse Apache Parquet files, returning columnar data.
    ///
    /// Returns [`PlanResult::Data`](super::PlanResult::Data) with columns matching the
    /// provided `physical_schema`. See [`ParquetHandler::read_parquet_files`] for more details on
    /// column resolution rules and ordering contracts.
    ///
    /// [`ParquetHandler::read_parquet_files`]: crate::ParquetHandler::read_parquet_files
    ScanParquet {
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    },
}
