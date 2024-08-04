//! # Delta Kernel
//!
//! Delta-kernel-rs is an experimental [Delta](https://github.com/delta-io/delta/) implementation
//! focused on interoperability with a wide range of query engines. It currently only supports
//! reads. This library defines a number of traits which must be implemented to provide a
//! working "delta reader". They are detailed below. There is a provided "default engine" that
//! implements all these traits and can be used to ease integration work. See
//! [`DefaultEngine`](engine/default/index.html) for more information.
//!
//! A full `rust` example for reading table data using the default engine can be found in the
//! [read-table-single-threaded] example (and for a more complex multi-threaded reader see the
//! [read-table-multi-threaded] example).
//!
//! [read-table-single-threaded]: https://github.com/delta-incubator/delta-kernel-rs/tree/main/kernel/examples/read-table-single-threaded
//! [read-table-multi-threaded]: https://github.com/delta-incubator/delta-kernel-rs/tree/main/kernel/examples/read-table-multi-threaded
//!
//! # Engine traits
//!
//! The [`Engine`] trait allow connectors to bring their own implementation of functionality such as
//! reading parquet files, listing files in a file system, parsing a JSON string etc.  This trait
//! exposes methods to get sub-engines which expose the core functionalities customizable by
//! connectors.
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
//! These interactions are encapsulated in the [`FileSystemClient`] trait. Implementors must take
//! care that all assumptions on the behavior if the functions - like sorted results - are respected.
//!
//! ## Reading log and data files
//!
//! Delta Kernel requires the capability to read json and parquet files, which is exposed via the
//! [`JsonHandler`] and [`ParquetHandler`] respectively. When reading files, connectors are asked to
//! provide the context information it requires to execute the actual read. This is done by invoking
//! methods on the [`FileSystemClient`] trait.
//!

#![cfg_attr(all(doc, NIGHTLY_CHANNEL), feature(doc_auto_cfg))]
#![warn(
    unreachable_pub,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility
)]

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use self::schema::{DataType, SchemaRef};

pub mod actions;
pub mod engine_data;
pub mod error;
pub mod expressions;
pub mod features;
pub(crate) mod path;
pub mod scan;
pub mod schema;
pub mod snapshot;
pub mod table;
pub mod transaction;
pub(crate) mod utils;

pub use engine_data::{DataVisitor, EngineData};
pub use error::{DeltaResult, Error};
pub use expressions::Expression;
pub use table::Table;

#[cfg(any(
    feature = "default-engine",
    feature = "sync-engine",
    feature = "arrow-conversion"
))]
pub mod engine;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;

/// A specification for a range of bytes to read from a file location
pub type FileSlice = (Url, Option<Range<usize>>);

/// Data read from a Delta table file and the corresponding scan file information.
pub type FileDataReadResult = (FileMeta, Box<dyn EngineData>);

/// An iterator of data read from specified files
pub type FileDataReadResultIterator =
    Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>;

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

/// Trait for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this trait to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator: Send + Sync {
    /// Evaluate the expression on a given EngineData.
    ///
    /// Contains one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this handler to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait ExpressionHandler: Send + Sync {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batches with the given [`Schema`] to produce data of [`DataType`].
    ///
    /// # Parameters
    ///
    /// - `schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    /// - `output_type`: Expected result data type.
    ///
    /// [`Schema`]: crate::schema::StructType
    /// [`DataType`]: crate::schema::DataType
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator>;
}

/// Provides file system related functionalities to Delta Kernel.
///
/// Delta Kernel uses this client whenever it needs to access the underlying
/// file system where the Delta table is present. Connector implementation of
/// this trait can hide filesystem specific details from Delta Kernel.
pub trait FileSystemClient: Send + Sync {
    /// List the paths in the same directory that are lexicographically greater or equal to
    /// (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
    fn list_from(&self, path: &Url)
        -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>>;

    /// Read data specified by the start and end offset from the file.
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>>;
}

/// Provides JSON handling functionality to Delta Kernel.
///
/// Delta Kernel can use this client to parse JSON strings into Row or read content from JSON files.
/// Connectors can leverage this trait to provide their best implementation of the JSON parsing
/// capability to Delta Kernel.
pub trait JsonHandler: Send + Sync {
    /// Parse the given json strings and return the fields requested by output schema as columns in [`EngineData`].
    /// json_strings MUST be a single column batch of engine data, and the column type must be string
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>>;

    /// Read and parse the JSON format file at given locations and return
    /// the data as EngineData with the columns requested by physical schema.
    ///
    /// # Parameters
    ///
    /// - `files` - File metadata for files to be read.
    /// - `physical_schema` - Select list of columns to read from the JSON file.
    /// - `predicate` - Optional push-down predicate hint (engine is free to ignore it).
    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator>;
}

/// Provides Parquet file related functionalities to Delta Kernel.
///
/// Connectors can leverage this trait to provide their own custom
/// implementation of Parquet data file functionalities to Delta Kernel.
pub trait ParquetHandler: Send + Sync {
    /// Read and parse the Parquet file at given locations and return the data as EngineData with
    /// the columns requested by physical schema . The ParquetHandler _must_ return exactly the
    /// columns specified in `physical_schema`, and they _must_ be in schema order.
    ///
    /// # Parameters
    ///
    /// - `files` - File metadata for files to be read.
    /// - `physical_schema` - Select list and order of columns to read from the Parquet file.
    /// - `predicate` - Optional push-down predicate hint (engine is free to ignore it).
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator>;
}

/// The `Engine` trait encapsulates all the functionality an engine or connector needs to provide
/// to the Delta Kernel in order to read the Delta table.
///
/// Engines/Connectors are expected to pass an implementation of this trait when reading a Delta
/// table.
pub trait Engine: Send + Sync {
    /// Get the connector provided [`ExpressionHandler`].
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler>;

    /// Get the connector provided [`FileSystemClient`]
    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient>;

    /// Get the connector provided [`JsonHandler`].
    fn get_json_handler(&self) -> Arc<dyn JsonHandler>;

    /// Get the connector provided [`ParquetHandler`].
    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler>;
}
