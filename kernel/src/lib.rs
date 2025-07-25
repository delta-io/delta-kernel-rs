//! # Delta Kernel
//!
//! Delta-kernel-rs is an experimental [Delta](https://github.com/delta-io/delta/) implementation
//! focused on interoperability with a wide range of query engines. It supports reads and
//! (experimental) writes (only blind appends in the write path currently). This library defines a
//! number of traits which must be implemented to provide a working delta implementation. They are
//! detailed below. There is a provided "default engine" that implements all these traits and can
//! be used to ease integration work. See [`DefaultEngine`](engine/default/index.html) for more
//! information.
//!
//! A full `rust` example for reading table data using the default engine can be found in the
//! [read-table-single-threaded] example (and for a more complex multi-threaded reader see the
//! [read-table-multi-threaded] example). An example for reading the table changes for a table
//! using the default engine can be found in the [read-table-changes] example.
//!
//!
//! [read-table-single-threaded]:
//! https://github.com/delta-io/delta-kernel-rs/tree/main/kernel/examples/read-table-single-threaded
//! [read-table-multi-threaded]:
//! https://github.com/delta-io/delta-kernel-rs/tree/main/kernel/examples/read-table-multi-threaded
//! [read-table-changes]:
//! https://github.com/delta-io/delta-kernel-rs/tree/main/kernel/examples/read-table-changes
//!
//! Simple write examples can be found in the [`write.rs`] integration tests. Standalone write
//! examples are coming soon!
//!
//! [`write.rs`]: https://github.com/delta-io/delta-kernel-rs/tree/main/kernel/tests/write.rs
//!
//! # Engine traits
//!
//! The [`Engine`] trait allow connectors to bring their own implementation of functionality such
//! as reading parquet files, listing files in a file system, parsing a JSON string etc.  This
//! trait exposes methods to get sub-engines which expose the core functionalities customizable by
//! connectors.
//!
//! ## Expression handling
//!
//! Expression handling is done via the [`EvaluationHandler`], which in turn allows the creation of
//! [`ExpressionEvaluator`]s. These evaluators are created for a specific predicate [`Expression`]
//! and allow evaluation of that predicate for a specific batches of data.
//!
//! ## File system interactions
//!
//! Delta Kernel needs to perform some basic operations against file systems like listing and
//! reading files. These interactions are encapsulated in the [`StorageHandler`] trait.
//! Implementers must take care that all assumptions on the behavior if the functions - like sorted
//! results - are respected.
//!
//! ## Reading log and data files
//!
//! Delta Kernel requires the capability to read and write json files and read parquet files, which
//! is exposed via the [`JsonHandler`] and [`ParquetHandler`] respectively. When reading files,
//! connectors are asked to provide the context information it requires to execute the actual
//! operation. This is done by invoking methods on the [`StorageHandler`] trait.

#![cfg_attr(all(doc, NIGHTLY_CHANNEL), feature(doc_auto_cfg))]
#![warn(
    unreachable_pub,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]
// we re-allow panics in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

/// This `extern crate` declaration allows the macro to reliably refer to
/// `delta_kernel::schema::DataType` no matter which crate invokes it. Without that, `delta_kernel`
/// cannot invoke the macro because `delta_kernel` is an unknown crate identifier (you have to use
/// `crate` instead). We could make the macro use `crate::schema::DataType` instead, but then the
/// macro is useless outside the `delta_kernel` crate.
// TODO: when running `cargo package -p delta_kernel` this gives 'unused' warning - #1095
#[allow(unused_extern_crates)]
extern crate self as delta_kernel;

use std::any::Any;
use std::fs::DirEntry;
use std::sync::Arc;
use std::time::SystemTime;
use std::{cmp::Ordering, ops::Range};

use bytes::Bytes;
use url::Url;

use self::schema::{DataType, SchemaRef};

pub mod actions;
pub mod checkpoint;
pub mod engine_data;
pub mod error;
pub mod expressions;
pub mod scan;
pub mod schema;
pub mod snapshot;
pub mod table_changes;
pub mod table_configuration;
pub mod table_features;
pub mod table_properties;
pub mod transaction;

mod arrow_compat;
#[cfg(any(feature = "arrow-54", feature = "arrow-55"))]
pub use arrow_compat::*;

pub mod kernel_predicates;
pub(crate) mod utils;

#[cfg(feature = "internal-api")]
pub use utils::try_parse_uri;

// for the below modules, we cannot introduce a macro to clean this up. rustfmt doesn't follow into
// macros, and so will not format the files associated with these modules if we get too clever. see:
// https://github.com/rust-lang/rustfmt/issues/3253

#[cfg(feature = "internal-api")]
pub mod path;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod path;

#[cfg(feature = "internal-api")]
pub mod log_replay;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod log_replay;

#[cfg(feature = "internal-api")]
pub mod log_segment;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod log_segment;

#[cfg(feature = "internal-api")]
pub mod history_manager;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod history_manager;

pub use delta_kernel_derive;
pub use engine_data::{EngineData, RowVisitor};
pub use error::{DeltaResult, Error};
pub use expressions::{Expression, ExpressionRef, Predicate, PredicateRef};
pub use snapshot::Snapshot;

use expressions::literal_expression_transform::LiteralExpressionTransform;
use expressions::Scalar;
use schema::{SchemaTransform, StructField, StructType};

#[cfg(any(feature = "default-engine", feature = "arrow-conversion"))]
pub mod engine;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;
pub type FileSize = u64;
pub type FileIndex = u64;

/// A specification for a range of bytes to read from a file location
pub type FileSlice = (Url, Option<Range<FileIndex>>);

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
    /// The last modified time as milliseconds since unix epoch
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: FileSize,
}

impl Ord for FileMeta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.location.cmp(&other.location)
    }
}

impl PartialOrd for FileMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<DirEntry> for FileMeta {
    type Error = Error;

    fn try_from(ent: DirEntry) -> DeltaResult<FileMeta> {
        let metadata = ent.metadata()?;
        let last_modified = metadata
            .modified()?
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::generic("Failed to convert file timestamp to milliseconds"))?;
        let location = Url::from_file_path(ent.path())
            .map_err(|_| Error::generic(format!("Invalid path: {:?}", ent.path())))?;
        let last_modified = last_modified.as_millis().try_into().map_err(|_| {
            Error::generic(format!(
                "Failed to convert file modification time {:?} into i64",
                last_modified.as_millis()
            ))
        })?;
        let metadata_len = metadata.len();
        #[cfg(all(feature = "arrow-54", not(feature = "arrow-55")))]
        let metadata_len = metadata_len
            .try_into()
            .map_err(|_| Error::generic("unable to convert DirEntry metadata to file size"))?;
        Ok(FileMeta {
            location,
            last_modified,
            size: metadata_len,
        })
    }
}

impl FileMeta {
    /// Create a new instance of `FileMeta`
    pub fn new(location: Url, last_modified: i64, size: u64) -> Self {
        Self {
            location,
            last_modified,
            size,
        }
    }
}

/// Extension trait that makes it easier to work with traits objects that implement [`Any`],
/// implemented automatically for any type that satisfies `Any`, `Send`, and `Sync`. In particular,
/// given some `trait T: Any + Send + Sync`, it allows upcasting `T` to `dyn Any + Send + Sync`,
/// which in turn allows downcasting the result to a concrete type. For example:
///
/// ```
/// # use delta_kernel::AsAny;
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo : AsAny {}
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let a: Arc<dyn Any + Send + Sync> = f.as_any();
/// let b: Arc<Bar> = a.downcast().unwrap();
/// ```
///
/// In contrast, very similar code that relies only on `Any` would fail to compile:
///
/// ```fail_compile
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo: Any + Send + Sync {}
///
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let b: Arc<Bar> = f.downcast().unwrap(); // `Arc::downcast` method not found
/// ```
///
/// As would this:
///
/// ```fail_compile
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo: Any + Send + Sync {}
///
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let a: Arc<dyn Any + Send + Sync> = f; // trait upcasting coercion is not stable rust
/// let f: Arc<Bar> = a.downcast().unwrap();
/// ```
///
/// NOTE: `AsAny` inherits the `Send + Sync` constraint from [`Arc::downcast`].
pub trait AsAny: Any + Send + Sync {
    /// Obtains a `dyn Any` reference to the object:
    ///
    /// ```
    /// # use delta_kernel::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: &dyn Foo = &Bar;
    /// let a: &dyn Any = f.any_ref();
    /// let b: &Bar = a.downcast_ref().unwrap();
    /// ```
    fn any_ref(&self) -> &(dyn Any + Send + Sync);

    /// Obtains an `Arc<dyn Any>` reference to the object:
    ///
    /// ```
    /// # use delta_kernel::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: Arc<dyn Foo> = Arc::new(Bar);
    /// let a: Arc<dyn Any + Send + Sync> = f.as_any();
    /// let b: Arc<Bar> = a.downcast().unwrap();
    /// ```
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;

    /// Converts the object to `Box<dyn Any>`:
    ///
    /// ```
    /// # use delta_kernel::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: Box<dyn Foo> = Box::new(Bar);
    /// let a: Box<dyn Any> = f.into_any();
    /// let b: Box<Bar> = a.downcast().unwrap();
    /// ```
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;

    /// Convenient wrapper for [`std::any::type_name`], since [`Any`] does not provide it and
    /// [`Any::type_id`] is useless as a debugging aid (its `Debug` is just a mess of hex digits).
    fn type_name(&self) -> &'static str;
}

// Blanket implementation for all eligible types
impl<T: Any + Send + Sync> AsAny for T {
    fn any_ref(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Extension trait that facilitates object-safe implementations of `PartialEq`.
pub trait DynPartialEq: AsAny {
    fn dyn_eq(&self, other: &dyn Any) -> bool;
}

// Blanket implementation for all eligible types
impl<T: PartialEq + AsAny> DynPartialEq for T {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<T>().is_some_and(|other| self == other)
    }
}

/// Trait for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this trait to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator: AsAny {
    /// Evaluate the expression on a given EngineData.
    ///
    /// Produces one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>>;
}

/// Trait for implementing a Predicate evaluator.
///
/// It contains one Predicate which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this trait to optimize the evaluation using the
/// connector specific capabilities.
pub trait PredicateEvaluator: AsAny {
    /// Evaluate the predicate on a given EngineData.
    ///
    /// Produces one boolean value for each row of the input.
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this handler to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait EvaluationHandler: AsAny {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batches with the given [`Schema`] to produce data of [`DataType`].
    ///
    /// If the provided output type is a struct, its fields describe the columns of output produced
    /// by the evaluator. Otherwise, the output schema is a single column named "output" of the
    /// specified `output_type`. In all cases, the output schema is only used for its names (all
    /// field names will be updated to match) and nullability (non-nullable columns can be converted
    /// to nullable). Any mismatch in types (including number of columns) will produce an error.
    ///
    /// # Parameters
    ///
    /// - `input_schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    /// - `output_type`: Expected result data type.
    ///
    /// [`Schema`]: crate::schema::StructType
    /// [`DataType`]: crate::schema::DataType
    fn new_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator>;

    /// Create a [`PredicateEvaluator`] that can evaluate the given [`Predicate`] on columnar
    /// batches with the given [`Schema`] to produce a column of boolean results.
    ///
    /// The output schema is a single nullable boolean column named "output".
    ///
    /// # Parameters
    ///
    /// - `input_schema`: Schema of the input data.
    /// - `predicate`: Predicate to evaluate.
    ///
    /// [`Schema`]: crate::schema::StructType
    fn new_predicate_evaluator(
        &self,
        input_schema: SchemaRef,
        predicate: Predicate,
    ) -> Arc<dyn PredicateEvaluator>;

    /// Create a single-row all-null-value [`EngineData`] with the schema specified by
    /// `output_schema`.
    // NOTE: we should probably allow DataType instead of SchemaRef, but can expand that in the
    // future.
    fn null_row(&self, output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>>;
}

/// Internal trait to allow us to have a private `create_one` API that's implemented for all
/// EvaluationHandlers.
// For some reason rustc doesn't detect it's usage so we allow(dead_code) here...
#[allow(dead_code)]
trait EvaluationHandlerExtension: EvaluationHandler {
    /// Create a single-row [`EngineData`] by applying the given schema to the leaf-values given in
    /// `values`.
    // Note: we will stick with a Schema instead of DataType (more constrained can expand in
    // future)
    fn create_one(&self, schema: SchemaRef, values: &[Scalar]) -> DeltaResult<Box<dyn EngineData>> {
        // just get a single int column (arbitrary)
        let null_row_schema = Arc::new(StructType::new(vec![StructField::nullable(
            "null_col",
            DataType::INTEGER,
        )]));
        let null_row = self.null_row(null_row_schema.clone())?;

        // Convert schema and leaf values to an expression
        let mut schema_transform = LiteralExpressionTransform::new(values);
        schema_transform.transform_struct(schema.as_ref());
        let row_expr = schema_transform.try_into_expr()?;

        let eval = self.new_expression_evaluator(null_row_schema, row_expr, schema.into());
        eval.evaluate(null_row.as_ref())
    }
}

// Auto-implement the extension trait for all EvaluationHandlers
impl<T: EvaluationHandler + ?Sized> EvaluationHandlerExtension for T {}

/// A trait that allows converting a type into (single-row) EngineData
///
/// This is typically used with the `#[derive(IntoEngineData)]` macro
/// which leverages the traits `ToDataType` and `Into<Scalar>` for struct fields
/// to convert a struct into EngineData.
///
/// # Example
/// ```ignore
/// # use std::sync::Arc;
/// # use delta_kernel_derive::{Schema, IntoEngineData};
///
/// #[derive(Schema, IntoEngineData)]
/// struct MyStruct {
///    a: i32,
///    b: String,
/// }
///
/// let my_struct = MyStruct { a: 42, b: "Hello".to_string() };
/// // typically used with ToSchema
/// let schema = Arc::new(MyStruct::to_schema());
/// // single-row EngineData
/// let engine = todo!(); // create an engine
/// let engine_data = my_struct.into_engine_data(schema, engine);
/// ```
pub(crate) trait IntoEngineData {
    /// Consume this type to produce a single-row EngineData using the provided schema.
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>>;
}

/// Provides file system related functionalities to Delta Kernel.
///
/// Delta Kernel uses this handler whenever it needs to access the underlying
/// file system where the Delta table is present. Connector implementation of
/// this trait can hide filesystem specific details from Delta Kernel.
pub trait StorageHandler: AsAny {
    /// List the paths in the same directory that are lexicographically greater than
    /// (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
    ///
    /// If the path is directory-like (ends with '/'), the result should contain
    /// all the files in the directory.
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
/// Delta Kernel can use this handler to parse JSON strings into Row or read content from JSON files.
/// Connectors can leverage this trait to provide their best implementation of the JSON parsing
/// capability to Delta Kernel.
pub trait JsonHandler: AsAny {
    /// Parse the given json strings and return the fields requested by output schema as columns in [`EngineData`].
    /// json_strings MUST be a single column batch of engine data, and the column type must be string
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>>;

    /// Read and parse the JSON format file at given locations and return the data as EngineData with
    /// the columns requested by physical schema. Note: The [`FileDataReadResultIterator`] must emit
    /// data from files in the order that `files` is given. For example if files ["a", "b"] is provided,
    /// then the engine data iterator must first return all the engine data from file "a", _then_ all
    /// the engine data from file "b". Moreover, for a given file, all of its [`EngineData`] and
    /// constituent rows must be in order that they occur in the file. Consider a file with rows
    /// (1, 2, 3). The following are legal iterator batches:
    ///    iter: [EngineData(1, 2), EngineData(3)]
    ///    iter: [EngineData(1), EngineData(2, 3)]
    ///    iter: [EngineData(1, 2, 3)]
    /// The following are illegal batches:
    ///    iter: [EngineData(3), EngineData(1, 2)]
    ///    iter: [EngineData(1), EngineData(3, 2)]
    ///    iter: [EngineData(2, 1, 3)]
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
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator>;

    /// Atomically (!) write a single JSON file. Each row of the input data should be written as a
    /// new JSON object appended to the file. this write must:
    /// (1) serialize the data to newline-delimited json (each row is a json object literal)
    /// (2) write the data to storage atomically (i.e. if the file already exists, fail unless the
    ///     overwrite flag is set)
    ///
    /// For example, the JSON data should be written as { "column1": "val1", "column2": "val2", .. }
    /// with each row on a new line.
    ///
    /// NOTE: Null columns should not be written to the JSON file. For example, if a row has columns
    /// ["a", "b"] and the value of "b" is null, the JSON object should be written as
    /// { "a": "..." }. Note that including nulls is technically valid JSON, but would bloat the
    /// log, therefore we recommend omitting them.
    ///
    /// # Parameters
    ///
    /// - `path` - URL specifying the location to write the JSON file
    /// - `data` - Iterator of EngineData to write to the JSON file. Each row should be written as
    ///   a new JSON object appended to the file. (that is, the file is newline-delimited JSON, and
    ///   each row is a JSON object on a single line)
    /// - `overwrite` - If true, overwrite the file if it exists. If false, the call must fail if
    ///   the file exists.
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()>;
}

/// Provides Parquet file related functionalities to Delta Kernel.
///
/// Connectors can leverage this trait to provide their own custom
/// implementation of Parquet data file functionalities to Delta Kernel.
pub trait ParquetHandler: AsAny {
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
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator>;
}

/// The `Engine` trait encapsulates all the functionality an engine or connector needs to provide
/// to the Delta Kernel in order to read the Delta table.
///
/// Engines/Connectors are expected to pass an implementation of this trait when reading a Delta
/// table.
pub trait Engine: AsAny {
    /// Get the connector provided [`EvaluationHandler`].
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler>;

    /// Get the connector provided [`StorageHandler`]
    fn storage_handler(&self) -> Arc<dyn StorageHandler>;

    /// Get the connector provided [`JsonHandler`].
    fn json_handler(&self) -> Arc<dyn JsonHandler>;

    /// Get the connector provided [`ParquetHandler`].
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler>;
}

// we have an 'internal' feature flag: default-engine-base, which is actually just the shared
// pieces of default-engine and default-engine-rustls. the crate can't compile with _only_
// default-engine-base, so we give a friendly error here.
#[cfg(all(
    feature = "default-engine-base",
    not(any(feature = "default-engine", feature = "default-engine-rustls",))
))]
compile_error!(
    "The default-engine-base feature flag is not meant to be used directly. \
    Please use either default-engine or default-engine-rustls."
);

// Rustdoc's documentation tests can do some things that regular unit tests can't. Here we are
// using doctests to test macros. Specifically, we are testing for failed macro invocations due
// to invalid input, not the macro output when the macro invocation is successful (which can/should be
// done in unit tests). This module is not exclusively for macro tests only so other doctests can also be added.
// https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html#include-items-only-when-collecting-doctests
#[cfg(doctest)]
mod doc_tests {

    /// ```
    /// # use delta_kernel_derive::ToSchema;
    /// #[derive(ToSchema)]
    /// pub struct WithFields {
    ///     some_name: String,
    /// }
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithField;

    /// ```compile_fail
    /// # use delta_kernel_derive::ToSchema;
    /// #[derive(ToSchema)]
    /// pub struct NoFields;
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithoutField;

    /// ```
    /// # use delta_kernel_derive::ToSchema;
    /// # use std::collections::HashMap;
    /// #[derive(ToSchema)]
    /// pub struct WithAngleBracketPath {
    ///     map_field: HashMap<String, String>,
    /// }
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithAngleBracketedPathField;

    /// ```
    /// # use delta_kernel_derive::ToSchema;
    /// # use std::collections::HashMap;
    /// #[derive(ToSchema)]
    /// pub struct WithAttributedField {
    ///     #[allow_null_container_values]
    ///     map_field: HashMap<String, String>,
    /// }
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithAttributedField;

    /// ```compile_fail
    /// # use delta_kernel_derive::ToSchema;
    /// #[derive(ToSchema)]
    /// pub struct WithInvalidAttributeTarget {
    ///     #[allow_null_container_values]
    ///     some_name: String,
    /// }
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithInvalidAttributeTarget;

    /// ```compile_fail
    /// # use delta_kernel_derive::ToSchema;
    /// # use syn::Token;
    /// #[derive(ToSchema)]
    /// pub struct WithInvalidFieldType {
    ///     token: Token![struct],
    /// }
    /// ```
    #[cfg(doctest)]
    pub struct MacroTestStructWithInvalidFieldType;
}
