//! Plan node operator kinds and their payloads.
//!
//! [`Operator`] enumerates every operator. Each operator's payload struct is defined
//! below.

use strum::Display;
use url::Url;

use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar};
use crate::plans::kernel_reducers::{KernelReducer, KernelReducerToken, ReducerHandle};
use crate::schema::SchemaRef;
use crate::FileMeta;

// ============================================================================
// Operator: enumerates every operator kind
// ============================================================================

/// Plan node operators.
///
/// Output schemas are stored on the payload struct for operators whose caller
/// declares them (`ScanParquet`, `ScanJson`, `Values`, `Load`, `Project`,
/// `MaxByVersion`); the remaining operators pass an input's schema through
/// unchanged:
/// - `Filter` from its input.
/// - `UnionAll` from its inputs' common schema.
/// - `SemiJoin` from the probe input.
#[derive(Debug, Clone, Display)]
pub enum Operator {
    // === Source operators (0 inputs) =========================================
    ScanParquet(ScanParquet),
    ScanJson(ScanJson),
    Values(Values),

    // === Unary operators (1 input) ===========================================
    Project(Project),
    Filter(Filter),
    Load(Load),
    MaxByVersion(MaxByVersion),

    // === Binary operators (2 inputs) =========================================
    SemiJoin(SemiJoin),

    // === N-ary operators (variable inputs) ===================================
    UnionAll(UnionAll),
}

/// One file to scan plus literal values broadcast to every row read from that file.
///
/// `file_constants` holds one [`Scalar`] per entry in the parent scan node's
/// [`ScanParquet::file_constant_columns`] / [`ScanJson::file_constant_columns`], in the
/// same order.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanFile {
    pub meta: FileMeta,
    /// One [`Scalar`] per `file_constant_columns` on the enclosing scan node, same order.
    pub file_constants: Vec<Scalar>,
}

impl ScanFile {
    /// A scan file with no file-constant column values.
    pub fn new(meta: FileMeta) -> Self {
        Self {
            meta,
            file_constants: Vec::new(),
        }
    }
}

impl From<FileMeta> for ScanFile {
    fn from(meta: FileMeta) -> Self {
        Self::new(meta)
    }
}

/// Reads Parquet `files` into row batches matching `schema`. The engine returns exactly the
/// columns named by `schema`, in schema order.
///
/// Output row order is unspecified: the engine is free to read `files` in any order, in
/// parallel, and to interleave rows from different files.
///
/// # Column resolution
///
/// The engine iterates `schema`'s fields in order; for each field it produces one column of
/// output:
///
/// 1. **Metadata columns**: if the field is annotated as a metadata column (e.g. via
///    [`StructField::create_metadata_column`] with [`MetadataColumnSpec::RowIndex`]), the engine
///    populates it from the read context rather than from the Parquet file. See [Metadata columns]
///    below.
/// 2. **File-constant columns**: if the field's name appears in [`Self::file_constant_columns`],
///    the engine broadcasts the corresponding entry from [`ScanFile::file_constants`] for the file
///    being read (not from Parquet bytes). See [File-constant columns] below.
/// 3. **Data columns**: otherwise the engine attempts to locate the field in the Parquet file, in
///    this order:
///    - **Field ID**: if the field carries a Parquet field ID via
///      [`ColumnMetadataKey::ParquetFieldId`] metadata, match it against the Parquet column with
///      the same field id.
///    - **Field name**: otherwise, or if no Parquet column has the requested field id, match by
///      column name.
///    - **No match**: the output column is filled with NULLs when the field is nullable, or an
///      error is returned when it is non-nullable.
///
/// Parquet columns not referenced by any `schema` field are ignored.
///
/// [Metadata columns]: #metadata-columns
/// [File-constant columns]: #file-constant-columns
/// [`StructField::create_metadata_column`]: crate::schema::StructField::create_metadata_column
/// [`MetadataColumnSpec::RowIndex`]: crate::schema::MetadataColumnSpec::RowIndex
/// [`ColumnMetadataKey::ParquetFieldId`]: crate::schema::ColumnMetadataKey::ParquetFieldId
///
/// ## Example
///
/// Consider a `schema` with the following fields (none of which are metadata columns):
/// - Column 0: `"i_logical"` (integer, non-null) with field ID 1 (via
///   [`ColumnMetadataKey::ParquetFieldId`])
/// - Column 1: `"s"` (string, nullable) with no field ID metadata
/// - Column 2: `"i2"` (integer, nullable) with no field ID metadata
///
/// And a Parquet file containing these columns:
/// - Column 0: `"i2"` (integer, nullable) with field ID 3
/// - Column 1: `"i"` (integer, non-null) with field ID 1
/// - No `"s"` column present
///
/// Resolving each `schema` field in turn:
/// - `"i_logical"` matches `"i"` by field ID (both have ID 1).
/// - `"s"` has no matching Parquet column, so the output column is filled with NULLs.
/// - `"i2"` matches `"i2"` by column name (no field ID to match on).
///
/// The returned data contains exactly 3 columns in schema order:
/// `{i_logical: parquet[1], s: NULL.., i2: parquet[0]}`.
///
/// # Metadata columns
///
/// A field marked as a row index metadata column (via [`StructField::create_metadata_column`]
/// with [`MetadataColumnSpec::RowIndex`]) is populated by the engine with the 0-based row
/// position within the file (`LONG`, non-nullable); a file with 5 rows yields `[0, 1, 2, 3, 4]`.
/// The column name is caller-chosen (commonly `"row_index"`).
///
/// # File-constant columns
///
/// [`Self::file_constant_columns`] names output fields whose values are identical for every row
/// in a given file (for example Delta partition columns or a table-changes `version`). Types and
/// nullability come from [`Self::schema`]; [`ScanFile::file_constants`] supplies the per-file
/// literals in the same order as `file_constant_columns`.
///
/// File-constant columns are distinct from [metadata columns], which are engine-generated
/// (such as row index). [`Load::file_constant_columns`] is the same concept for the [`Load`] node.
///
/// # Invariants
///
/// - `files[i].file_constants.len() == file_constant_columns.len()` for every `i`.
/// - Every name in `file_constant_columns` resolves to a field in `schema` that is not a metadata
///   column.
/// - Each scalar in `file_constants` is compatible with its schema field's type.
#[derive(Debug, Clone)]
pub struct ScanParquet {
    pub files: Vec<ScanFile>,
    pub file_constant_columns: Vec<ColumnName>,
    pub schema: SchemaRef,
}

/// Reads newline-delimited JSON `files` (one JSON object per line) into row batches matching
/// `schema`.
///
/// Column resolution matches [`ScanParquet`]: metadata columns, then file-constant columns
/// (see [`Self::file_constant_columns`] and [`ScanFile::file_constants`]), then fields read from
/// each JSON line. Missing JSON fields produce NULL for nullable `schema` fields and an error for
/// non-nullable fields.
///
/// Output row order is unspecified: the engine is free to read `files` in any order, in
/// parallel, and to interleave rows from different files.
///
/// # File-constant columns
///
/// Same contract as [`ScanParquet::file_constant_columns`].
///
/// # Invariants
///
/// Same invariants as [`ScanParquet`].
#[derive(Debug, Clone)]
pub struct ScanJson {
    pub files: Vec<ScanFile>,
    pub file_constant_columns: Vec<ColumnName>,
    pub schema: SchemaRef,
}

/// Inline literal rows. Each `rows[i]` carries one [`Scalar`] per **top-level** field
/// of `schema`, in field order; `rows[i].len() == schema.fields().count()` for every
/// row. Nested struct values are encoded as [`Scalar::Struct`], and array / map
/// values as [`Scalar::Array`] / [`Scalar::Map`]; nested leaves are not flattened
/// into the row vec.
///
/// # Example (flat)
///
/// Two rows over `{ id: int, active: bool }`:
///
/// ```text
/// Values {
///     schema: { id: int, active: bool },
///     rows: [
///         [1, true],
///         [2, false],
///     ],
/// }
/// ```
///
/// produces:
///
/// ```text
/// id | active
/// ---+--------
///  1 |  true
///  2 | false
/// ```
///
/// # Example (nested)
///
/// Two rows over `{ id: int, address: { city: string, zip: int } }`. The `address`
/// field is one top-level slot in the row vec, populated with a single
/// `Scalar::Struct`:
///
/// ```text
/// Values {
///     schema: { id: int, address: { city: string, zip: int } },
///     rows: [
///         [1, Scalar::Struct({ city: "NYC", zip: 10001 })],
///         [2, Scalar::Struct({ city: "SF",  zip: 94102 })],
///     ],
/// }
/// ```
///
/// produces:
///
/// ```text
/// id | address.city | address.zip
/// ---+--------------+------------
///  1 |     NYC      |    10001
///  2 |     SF       |    94102
/// ```
#[derive(Debug, Clone)]
pub struct Values {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

/// Projects the input through `expr` into rows of `schema`.
///
/// `expr` must be a struct constructor or struct patch whose fields match `schema`. It is
/// evaluated with `schema` as its output struct type: the struct's fields are the output
/// columns, `schema` supplies names and nullability, and any type or arity mismatch is an error.
/// Downstream nodes see the logical field names declared in `schema`.
///
/// A struct patch carries the input struct through field by field, naming only the columns that
/// change -- replacing or dropping existing fields and injecting new ones -- while everything else
/// passes through unchanged, so it costs O(changes) rather than O(schema width). The patched
/// result still covers every field in `schema`.
///
/// # Example
///
/// Input `{ id, first, last, add: { path, size, stats_parsed: { numRecords } } }` projected to
/// `{ id, names, file_meta }`, showing passthrough, array construction, nested input access, and a
/// struct output column:
///
/// ```text
/// Project {
///     expr: Expression::struct_from([
///         col("id"),
///         Expression::array([col("first"), col("last")]),
///         Expression::struct_from([
///             col("add.path"),
///             col("add.size"),
///             col("add.stats_parsed.numRecords"),
///         ]),
///     ]),
///     schema: {
///         id: int,
///         names: array<string>,
///         file_meta: { path: string, size: long, num_records: long },
///     },
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Project {
    pub expr: ExpressionRef,
    pub schema: SchemaRef,
}

/// Keeps input rows where `predicate` evaluates true (SQL null semantics).
/// Output schema is the input schema unchanged.
#[derive(Debug, Clone)]
pub struct Filter {
    pub predicate: PredicateRef,
}

/// File formats supported by [`Load`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Names the columns a [`Load`] reads from each upstream row to locate and size each file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadColumnFileMeta {
    /// Non-nullable column holding the per-row file path / URL fragment.
    pub path_column: ColumnName,
    /// Nullable column with the file's total size in bytes. Engines use non-NULL size and
    /// row-count values as split-sizing / pruning hints.
    pub file_size_column: ColumnName,
    /// Nullable column with the file's row-count.
    pub num_records_column: ColumnName,
}

/// Reads data files from an upstream stream of file-metadata tuples, one input row per file.
/// For each row, `file_meta` locates and sizes the file, the engine resolves its path against
/// `base_url` (see below), opens it as `file_type`, and reads columns matching `schema`.
///
/// `file_constant_columns` lists upstream columns whose per-file values are broadcast onto
/// every emitted file row. This is file-constant metadata, the same concept as
/// [`ScanParquet::file_constant_columns`]. See the example below.
///
/// `dv_column` names a nullable column on the upstream row holding a Delta
/// [`DeletionVectorDescriptor`] struct. The engine resolves it into a roaring bitmap
/// and drops file rows whose row index appears in the DV. A NULL value for a given
/// input row means "no DV for this file", so all file rows are emitted.
///
/// [`DeletionVectorDescriptor`]: crate::actions::deletion_vector::DeletionVectorDescriptor
///
/// Each upstream path is resolved against `base_url`:
///
/// - **`Some(base)`**: each path column value is treated as a path relative to `base` and resolved
///   via [`Url::join`]. Paths that are themselves absolute URLs (any scheme prefix) bypass the join
///   and are used as-is.
/// - **`None`**: every path column value must already be an absolute URL; the engine errors on
///   relative paths.
///
/// Output row order is unspecified: the engine is free to read files in any order, in
/// parallel, and to interleave rows from different files. The relative order of upstream
/// rows is not preserved.
///
/// # Example
///
/// Given an upstream metadata stream and a `Load` configuration:
///
/// ```text
/// upstream (metadata)
///     path             | size | num_records | version | dv
///     -----------------+------+-------------+---------+------
///     part-0.parquet   | 1024 |        NULL |       7 | NULL
///     part-1.parquet   | 2048 |        NULL |       8 | NULL
/// ```
/// ```text
/// Load {
///     schema: { id: int, name: string },
///     file_type: Parquet,
///     base_url: "s3://table/",
///     file_constant_columns: ["version"],
///     file_meta: {
///         path_column: "path",
///         file_size_column: "size",
///         num_records_column: "num_records",
///     },
///     dv_column: "dv",
/// }
/// ```
/// The engine opens `s3://table/part-0.parquet` and `s3://table/part-1.parquet`, reads
/// `{id, name}` from each, sees a NULL DV for each file so all rows survive, and
/// broadcasts the row's `version` onto every emitted file row. One possible output
/// (row order is not guaranteed):
/// ```text
///     | id | name | version
///     +----+------+--------
///     |  3 |  c   |       8
///     |  2 |  b   |       7
///     |  4 |  d   |       8
///     |  1 |  a   |       7
/// ```
#[derive(Debug, Clone)]
pub struct Load {
    pub schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub file_constant_columns: Vec<ColumnName>,
    pub file_meta: LoadColumnFileMeta,
    pub dv_column: ColumnName,
}

/// Per group, keep the input row with the greatest `version_column` value and project
/// the columns named in `schema` from that row. Kernel uses this for dedupe
/// across table versions (e.g. latest `add` per path in scan metadata).
///
/// Each `schema` field selects a column from the winning row; group-by keys and
/// the version column must appear in `schema` when they should be emitted.
///
/// # SQL equivalents
///
/// The same semantics are expressible as SQL `MAX BY` or as a query with
/// `ROW_NUMBER()`. Both express the same "keep the row with the greatest
/// `version_column` per group" behavior over a single ordering column.
///
/// As `MAX BY`:
///
/// ```sql
/// SELECT
///     <group_by fields>,
///     MAX_BY(col_a, <version_column>),
///     MAX_BY(col_b, <version_column>),
///     ...
/// FROM input
/// GROUP BY <group_by fields>
/// ```
///
/// Equivalent window rewrite:
///
/// ```sql
/// SELECT <schema fields>
/// FROM (
///     SELECT *,
///            ROW_NUMBER() OVER (
///                PARTITION BY <group_by>
///                ORDER BY <version_column> DESC
///            ) AS rn
///     FROM input
/// ) WHERE rn = 1
/// ```
///
/// # Example
///
/// ```text
/// MaxByVersion {
///     group_by: [col("person")],
///     version_column: "year",
///     schema: { person: string, year: int, likes_to_eat: string },
/// }
/// ```
///
/// Input:
///
/// ```text
/// input
/// person   | year   | likes_to_eat
/// ---------+--------+-----------
///  Alice   | 2020   | pizza
///  Alice   | 2026   | sushi
///  Bob     | 2020   | pizza
///  Bob     | 2025   | watermelon
///  Charlie | 2021   | ice cream
///  Charlie | 2026   | egg
/// ```
///
/// Output:
///
/// ```text
/// output
/// person   | year   | likes_to_eat
/// ---------+--------+-----------
///  Alice   | 2026   | sushi
///  Bob     | 2025   | watermelon
///  Charlie | 2026   | egg
/// ```
#[derive(Debug, Clone)]
pub struct MaxByVersion {
    pub group_by: Vec<ExpressionRef>,
    pub version_column: ColumnName,
    pub schema: SchemaRef,
}

/// Performs a semi join between two inputs, `inputs.len() == 2`, the child
/// nodes are `[probe, build]` in this order. It emits a subset of probe rows;
/// the build side acts as a filter and never contributes columns. This is
/// analogous to a SQL `SEMI JOIN` (`inverted = false`) or `ANTI JOIN`
/// (`inverted = true`). A semi join finds all probe rows that are present in
/// the build side, and an anti join finds all probe rows **not** present in the
/// build side. This is analogous to set intersection and set difference,
/// respectively.
///
/// The output schema is the same as the probe input's schema.
///
/// # Example
///
/// ```text
/// SemiJoin { probe_keys: ["path"], build_keys: ["path"] }
///
/// probe               build
/// path | version      path
/// -----+--------      ----
///  a   |   1           b
///  b   |   2           d
///  c   |   3
///
/// output (inverted = false, semi join: probe rows whose path is in build):
/// path | version
/// -----+--------
///  b   |   2
///
/// output (inverted = true, anti join: probe rows whose path is not in build):
/// path | version
/// -----+--------
///  a   |   1
///  c   |   3
/// ```
#[derive(Debug, Clone)]
pub struct SemiJoin {
    pub inverted: bool,
    pub probe_keys: Vec<ColumnName>,
    pub build_keys: Vec<ColumnName>,
}

/// The unordered bag union of N input relations. All rows of all inputs appear in the
/// output, in arbitrary order. All input schemas must agree, and the output schema
/// is the common schema of the inputs.
///
/// # Example
///
/// `UnionAll` over two relations with schema `{ id: int }`:
///
/// ```text
/// input 0:
/// id
/// --
///  1
///  2
///  3
///
/// input 1:
/// id
/// --
///  3
///  4
///  5
///
/// output (arbitrary order; bag semantics keep the duplicate 3):
/// id
/// --
///  4
///  1
///  3
///  2
///  5
///  3
/// ```
#[derive(Debug, Clone)]
pub struct UnionAll;

// ============================================================================
// Reducer-drain sink (referenced by `EngineRequest::Reduce`)
// ============================================================================

/// Template for draining a row stream into a [`KernelReducer`] via [`EngineRequest::Reduce`].
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone) into a
///   [`ReducerHandle`].
/// - `token`: keys the finished handle returned from the executor and validated at decode time by
///   the paired [`Extractor`].
///
/// [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::state_machine::EngineRequest::Reduce
/// [`Extractor`]: crate::plans::kernel_reducers::Extractor
#[derive(Debug, Clone)]
pub struct ReduceSink {
    pub initial_state: Box<dyn KernelReducer>,
    pub token: KernelReducerToken,
}

impl ReduceSink {
    /// Construct from a concrete reducer and mint a fresh token from its `kind`.
    pub fn new<R: KernelReducer + 'static>(state: R) -> Self {
        let token = KernelReducerToken::new(state.kind());
        Self {
            initial_state: Box::new(state),
            token,
        }
    }

    /// Mint a runtime [`ReducerHandle`] for this sink template by cloning the initial state.
    pub fn new_handle(&self) -> ReducerHandle {
        ReducerHandle::new(self.token.clone(), self.initial_state.clone())
    }
}

// Token identity drives equality: tokens are process-unique by id, and the
// `initial_state` trait object (`Box<dyn KernelReducer>`) is not `Eq`-able. Two
// sinks sharing a token were constructed from the same plan node and therefore
// describe the same reducer.
impl PartialEq for ReduceSink {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Eq for ReduceSink {}
