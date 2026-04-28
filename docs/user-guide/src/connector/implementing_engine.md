# Implementing the Engine trait

The `Engine` trait is the main integration point between your connector and Delta Kernel. For
background on what the Engine trait is and when you need a custom one, see the
[Connector Overview](./overview.md) and [The Engine Trait](../concepts/engine_trait.md).

## The Engine trait

The `Engine` trait has four required methods, each returning a handler:

```rust,ignore
pub trait Engine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler>;
    fn storage_handler(&self) -> Arc<dyn StorageHandler>;
    fn json_handler(&self) -> Arc<dyn JsonHandler>;
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler>;
}
```

You don't have to implement all four handlers from scratch. A common approach is to start
with `DefaultEngine` and selectively replace handlers. For example, you might provide a
custom `ParquetHandler` that reads into your engine's native columnar format while reusing
the default handlers for everything else.

Many of the `Engine` handlers take or return `EngineData`. See [EngineData](engine_data.md) for more
information about this type.

## StorageHandler

`StorageHandler` provides file system operations. The kernel calls this to list and read
files (as bytes) from storage.

```rust,ignore
pub trait StorageHandler {
    fn list_from(&self, path: &Url)
        -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>>;

    fn read_files(&self, files: Vec<FileSlice>)
        -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>>;

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()>;

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()>;

    fn head(&self, path: &Url) -> DeltaResult<FileMeta>;
}
```

### Key contracts

- **`list_from`**: Results must be sorted lexicographically by path. If the path ends with
  `/`, list all files in that directory. Otherwise, list files lexicographically greater than
  the given path in the same directory.

- **`copy_atomic`**: Must fail with `Error::FileAlreadyExists` if the destination exists.
  This is used for commit publishing in catalog-managed tables.

- **`put`**: Writes raw bytes to the given path. If `overwrite` is false and the file already
  exists, must fail with `Error::FileAlreadyExists`.

- **`head`**: Must return `Error::FileNotFound` if the file doesn't exist.

- **`read_files`**: Each `FileSlice` is a `(Url, Option<Range<u64>>)`. When the range is
  `None`, read the entire file.

### Default implementation

The `DefaultEngine` uses [`object_store`](https://docs.rs/object_store) for storage, which supports
local filesystem, S3, GCS, and Azure out of the box.

## JsonHandler

`JsonHandler` reads and writes JSON. The kernel uses this for Delta log commits
(`_delta_log/*.json`) and checkpoint sidecars.

```rust,ignore
pub trait JsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>>;

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator>;

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()>;
}
```

### Key contracts

- **`parse_json`**: Input is a single-column batch of strings (JSON objects). Output
  columns match the `output_schema`. Missing fields should produce nulls for nullable columns.

- **`read_json_files`**: Data must be returned in file order (same order as the `files` slice
  argument), and rows within a file must be in source order. The predicate is an optional hint.  The
  engine may ignore it.

- **`write_json_file`**: Must write newline-delimited JSON (one JSON object per line). Null
  columns should be omitted from the output to save space. The write must be atomic. If
  `overwrite` is false and the file exists, fail with an error.

### Default implementation

The `DefaultEngine` uses `arrow_json` for parsing and the `object_store` crate for I/O.

## ParquetHandler

`ParquetHandler` reads and writes Parquet files. This is typically the most important
handler to customize, since it's on the critical path for data reading performance.

```rust,ignore
pub trait ParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator>;

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    ) -> DeltaResult<()>;

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter>;
}
```

### Key contracts for `read_parquet_files`

**Column resolution**: When reading, the handler must resolve columns from the Parquet file
to the `physical_schema`:

1. If a `StructField` in the schema has a field ID (via `ColumnMetadataKey::ParquetFieldId`
   metadata), match by field ID first.
2. Otherwise, fall back to matching by column name.
3. If no match is found: return nulls for nullable columns, or an error for non-nullable
   columns.

**Column Ordering**: Columns must be returned in the order specified in the `physical_schema`
argument, which is _not_ necessarily the order they may be specified in the parquet file itself.

**Missing Columns**: If a column is specified in the schema, and is nullable, the parquet reader
must return a column of all nulls.

**Ordering**: Like `JsonHandler`, data must be returned in file order, and rows within a
file must be in source order.

**Metadata columns**: The handler must support two virtual metadata columns that are not
stored in the Parquet file but generated at read time:

| Metadata column | How to detect | Type | Values |
|-----------------|---------------|------|--------|
| Row index | `StructField` created with `MetadataColumnSpec::RowIndex` | `LONG`, non-nullable | Sequential 0-based position within the file |
| File name | `StructField` has reserved field ID `2147483646` | `STRING`, non-nullable | Full file path/URL |

**Footer reading**: `read_parquet_footer` reads only the Parquet metadata (no data). If the
file has field IDs (column mapping), they must be preserved in the returned schema's
`StructField` metadata under the `ParquetFieldId` key.

### Default implementation

The `DefaultEngine` uses the Apache Arrow Parquet reader/writer with support for column
projection, predicate pushdown, metadata columns, and field-ID-based column matching.

## EvaluationHandler

`EvaluationHandler` creates reusable evaluators for expressions and predicates. The kernel
uses this for data skipping (evaluating predicates against file statistics) and for per-file
transformations (partition value injection, row tracking).

```rust,ignore
pub trait EvaluationHandler {
    fn new_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: ExpressionRef,
        output_type: DataType,
    ) -> DeltaResult<Arc<dyn ExpressionEvaluator>>;

    fn new_predicate_evaluator(
        &self,
        input_schema: SchemaRef,
        predicate: PredicateRef,
    ) -> DeltaResult<Arc<dyn PredicateEvaluator>>;

    fn null_row(&self, output_schema: SchemaRef)
        -> DeltaResult<Box<dyn EngineData>>;

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: &[&[Scalar]],
    ) -> DeltaResult<Box<dyn EngineData>>;
}
```

The returned evaluators are reusable objects. The kernel creates them once and calls
`evaluate()` on multiple batches:

```rust,ignore
pub trait ExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>>;
}

pub trait PredicateEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>>;
}
```

### Key contracts

- **Expression evaluators** produce one output row per input row. If `output_type` is a
  struct, its fields describe the output columns. Otherwise, the output is a single column.

- **Predicate evaluators** produce a single nullable boolean column. `true` means the row
  matches, `false` or `null` means it doesn't.

- **`null_row`** creates a single-row `EngineData` with all null values. The kernel uses
  this internally for partition column construction.

- **`create_many`** creates a multi-row `EngineData` by applying the given schema to
  multiple rows of `Scalar` values. Each element in `rows` contains one scalar per top-level
  field in the schema. Returns an error if any row's scalar count doesn't match the schema's
  field count, or if a scalar value's type doesn't match its corresponding field.

### Default implementation

The `DefaultEngine` uses Arrow compute kernels for expression evaluation.

