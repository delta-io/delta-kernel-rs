//! # TableClient interfaces
//!
//! The TableClient interfaces allow connectors to bring their own implementation of functionality
//! such as reading parquet files, listing files in a file system, parsing a JSON string etc.
//!
//! The [`TableClient`] trait exposes methods to get sub-clients which expose the core
//! functionalitites customizable by connectors.
//!
//! ## Expression handling
//!
//! Expression handling is done via the [`ExpressionHandler`], which in turn allows the creation
//! of [`ExpressionEvaluator`]s. These evaluators are created for a specific predicate [`Expression`]
//! and allow evaluation of that predicate for a specific batches of data.
//!
//! ## File system interactions
//!
//! Delta Kernel needs to perform some basic operations against file systems like listing and reading files.
//! These interactions are encapsulated in the [`FileSystemClient`] trait. Implementors must take take
//! care that all assumtions on the behavior if the functions - like sorted results - are respected.
//!
//! ## Reading log and data files
//!
//! Delta Kernel requires the capability to read json and parquet files, which is exposed via the
//! [`JsonHandler`] and [`ParquetHandler`] respectively. When reading files, connectors are asked
//! to provide the context imformation it requires to execute the actual read. This is done by
//! invoking methods on the [`FileHandler`] trait. All specifc file handlers must also provide
//! the contextualization APis.
//!

#![warn(
    unreachable_pub,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility,
    missing_debug_implementations
)]

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use tracing::error;
use url::Url;

pub mod actions;
pub mod error;
pub mod expressions;
pub mod parquet_reader;
pub mod path;
pub mod scan;
pub mod schema;
pub mod snapshot;
pub mod table;

pub use actions::{types::*, ActionType};
pub use error::{DeltaResult, Error};
pub use expressions::Expression;
pub use snapshot::replay::LogFile;
pub use table::Table;

#[cfg(feature = "default-client")]
pub mod client;
#[cfg(feature = "default-client")]
pub use client::*;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;

pub type FileSlice = (Url, Option<Range<usize>>);

/// Data read from a Delta table file and the corresponding scan file information.
pub type FileDataReadResult = (FileMeta, RecordBatch);

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// The fully qualified path to the object
    pub location: Url,
    /// The last modified time
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: usize,
}

/// Interface for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this interface to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator {
    /// Evaluate the expression on given ColumnarBatch data.
    ///
    /// Contains one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<RecordBatch>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this client to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait ExpressionHandler {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batchs with the given [`Schema`].
    ///
    /// # Parameters
    ///
    /// - `schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    ///
    /// [`Schema`]: arrow_schema::Schema
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
    ) -> Arc<dyn ExpressionEvaluator>;
}

/// Provides file system related functionalities to Delta Kernel.
///
/// Delta Kernel uses this client whenever it needs to access the underlying
/// file system where the Delta table is present. Connector implementation of
/// this interface can hide filesystem specific details from Delta Kernel.
#[async_trait::async_trait]
pub trait FileSystemClient {
    /// List the paths in the same directory that are lexicographically greater or equal to
    /// (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
    async fn list_from(&self, path: &Url) -> DeltaResult<BoxStream<'_, DeltaResult<FileMeta>>>;

    /// Read data specified by the start and end offset from the file.
    async fn read_files(&self, files: Vec<FileSlice>) -> DeltaResult<Vec<Bytes>>;
}

/// Provides file handling functionality to Delta Kernel.
///
/// Connectors can implement this client to provide Delta Kernel
/// their own custom implementation of file splitting, additional
/// predicate pushdown or any other connector-specific capabilities.
pub trait FileHandler {
    /// Placeholder interface allowing connectors to attach their own custom implementation.
    ///
    /// Connectors can use this to pass additional context about a scan file through
    /// Delta Kernel and back to the connector for interpretation.
    type FileReadContext: Send;

    /// Associates a connector specific FileReadContext for each scan file.
    ///
    /// Delta Kernel will supply the returned FileReadContexts back to
    /// the connector when reading the file (for example, in ParquetHandler.readParquetFiles.
    /// Delta Kernel does not interpret FileReadContext. For example, a connector can attach
    /// split information in its own implementation of FileReadContext or attach any predicates.
    ///
    /// # Parameters
    ///
    /// - `files` - iterator of [`FileMeta`]
    /// - `predicate` - Predicate to prune data. This is optional for the
    ///   connector to use for further optimization. Filtering by this predicate is not required.
    fn contextualize_file_reads(
        &self,
        files: Vec<FileMeta>,
        predicate: Option<Expression>,
    ) -> DeltaResult<Vec<Self::FileReadContext>>;
}

/// Provides JSON handling functionality to Delta Kernel.
///
/// Delta Kernel can use this client to parse JSON strings into Row or read content from JSON files.
/// Connectors can leverage this interface to provide their best implementation of the JSON parsing
/// capability to Delta Kernel.
#[async_trait::async_trait]
pub trait JsonHandler: FileHandler {
    /// Parse the given json strings and return the fields requested by output schema as columns in a [`RecordBatch`].
    fn parse_json(
        &self,
        json_strings: StringArray,
        output_schema: SchemaRef,
    ) -> DeltaResult<RecordBatch>;

    /// Read and parse the JSON format file at given locations and return
    /// the data as a RecordBatch with the columns requested by physical schema.
    ///
    /// # Parameters
    ///
    /// - `files` - Vec of FileReadContext objects to read data from.
    /// - `physical_schema` - Select list of columns to read from the JSON file.
    async fn read_json_files(
        &self,
        files: Vec<<Self as FileHandler>::FileReadContext>,
        physical_schema: SchemaRef,
    ) -> DeltaResult<Vec<FileDataReadResult>>;
}

/// Provides Parquet file related functionalities to Delta Kernel.
///
/// Connectors can leverage this interface to provide their own custom
/// implementation of Parquet data file functionalities to Delta Kernel.
#[async_trait::async_trait]
pub trait ParquetHandler: FileHandler {
    /// Read and parse the JSON format file at given locations and return
    /// the data as a RecordBatch with the columns requested by physical schema.
    ///
    /// # Parameters
    ///
    /// - `files` - Vec of FileReadContext objects to read data from.
    /// - `physical_schema` - Select list of columns to read from the JSON file.
    async fn read_parquet_files(
        &self,
        files: Vec<<Self as FileHandler>::FileReadContext>,
        physical_schema: SchemaRef,
    ) -> DeltaResult<Vec<FileDataReadResult>>;
}

/// Interface encapsulating all clients needed by the Delta Kernel in order to read the Delta table.
///
/// Connectors are expected to pass an implementation of this interface when reading a Delta table.
pub trait TableClient {
    /// Read context when operating on json files
    type JsonReadContext: Send;

    /// Read context when operating on parquet files
    type ParquetReadContext: Send;

    /// Get the connector provided [`ExpressionHandler`].
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler>;

    /// Get the connector provided [`FileSystemClient`]
    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient>;

    /// Get the connector provided [`JsonHandler`].
    fn get_json_handler(&self) -> Arc<dyn JsonHandler<FileReadContext = Self::JsonReadContext>>;

    /// Get the connector provided [`ParquetHandler`].
    fn get_parquet_handler(
        &self,
    ) -> Arc<dyn ParquetHandler<FileReadContext = Self::ParquetReadContext>>;
}

/**
 * Parse the given [object_store::path::Path] to identify a commit log version
 */
fn version_from_path(path: &Path) -> Result<Version, Error> {
    if let Some(filename) = path.filename() {
        if let Some(part) = filename.split('.').next() {
            return part
                .parse()
                .map_err(|source: std::num::ParseIntError| Error::GenericError {
                    source: Box::new(source),
                });
        }
        Ok(0)
    } else {
        let e = format!(
            "Provided path does not have a filename at the end: {:?}",
            path
        );
        error!("{}", &e);
        Err(Error::Generic(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_from_path() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001.json");
        let version = version_from_path(&path).unwrap();

        assert_eq!(1, version);
    }

    #[test]
    fn test_version_from_path_invalid_stem() {
        let path = Path::from("/tmp/test/_delta_log/some-garbage");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_invalid_extension() {
        let path = Path::from("/tmp/test/_delta_log/some-garbage.txt");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_without_filename() {
        let path = Path::from("/");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_with_dir() {
        let path = Path::from("/tmp/test/_delta_log/");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }
}
