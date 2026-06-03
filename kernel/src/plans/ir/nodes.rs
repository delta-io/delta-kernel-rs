//! Plan node operator kinds and their payloads.
//!
//! [`NodeKind`] enumerates every operator. Each operator's payload struct is defined
//! below.

use strum::Display;
use url::Url;

use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar};
use crate::schema::SchemaRef;
use crate::FileMeta;

// ============================================================================
// NodeKind -- enumerates every operator kind
// ============================================================================

/// Plan node operator kinds.
///
/// Sources take zero inputs; unary operators take one; binary operators take two;
/// n-ary operators take a variable number. Output schemas are stored on the payload
/// struct for operators whose caller declares them (`ScanParquet`, `ScanJson`,
/// `Values`, `Load`, `Project`, `MaxByVersion`); the remaining operators pass an
/// input's schema through unchanged (`Filter` and `UnionAll` from the sole/first
/// input; `SemiJoin` from the probe input).
#[derive(Debug, Clone, Display)]
pub enum NodeKind {
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
/// parallel, and to interleave rows from different files. Downstream nodes that depend on
/// a specific ordering must impose it explicitly.
///
/// # Column resolution
///
/// The engine iterates `schema`'s fields in order; for each field it produces one column of
/// output:
///
/// 1. **Metadata columns** -- if the field is annotated as a metadata column (e.g. via
///    [`StructField::create_metadata_column`] with [`MetadataColumnSpec::RowIndex`], or the
///    reserved [`FILE_NAME`] field ID), the engine populates it from the read context rather
///    than from the Parquet file. See [Metadata columns] below.
/// 2. **File-constant columns** -- if the field's name appears in [`Self::file_constant_columns`],
///    the engine broadcasts the corresponding entry from [`ScanFile::file_constants`] for the file
///    being read (not from Parquet bytes). See [File-constant columns] below.
/// 3. **Data columns** -- otherwise the engine attempts to locate the field in the Parquet
///    file, in this order:
///    - **Field ID**: if the field carries a Parquet field ID via
///      [`ColumnMetadataKey::ParquetFieldId`] metadata, match it against the Parquet column
///      with the same field id.
///    - **Field name**: otherwise, or if no Parquet column has the requested field id, fall
///      back to matching by column name.
///
///    If no Parquet column matches, the output column is filled with NULLs when the field is
///    nullable, or an error is returned when it is non-nullable.
///
/// Parquet columns not referenced by any `schema` field are ignored.
///
/// [Metadata columns]: #metadata-columns
/// [File-constant columns]: #file-constant-columns
/// [`StructField::create_metadata_column`]: crate::schema::StructField::create_metadata_column
/// [`MetadataColumnSpec::RowIndex`]: crate::schema::MetadataColumnSpec::RowIndex
/// [`FILE_NAME`]: crate::reserved_field_ids::FILE_NAME
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
/// # Metadata Columns
///
/// The engine must support virtual metadata columns that provide additional information about
/// each row. These columns are not stored in the Parquet file but are generated at read time.
///
/// ## Row Index Column
///
/// When a column in `schema` is marked as a row index metadata column (via
/// [`StructField::create_metadata_column`] with [`MetadataColumnSpec::RowIndex`]), the engine
/// must populate it with the 0-based row position within the Parquet file:
///
/// - **Column name**: User-specified (commonly `"row_index"` or `"_metadata.row_index"`)
/// - **Type**: `LONG` (non-nullable)
/// - **Values**: Sequential integers starting at 0 for each file
/// - **Use case**: Track row positions for downstream processing, or internally used to compute Row
///   IDs
///
/// Example: A file with 5 rows would have row_index values `[0, 1, 2, 3, 4]`.
///
/// ## File Name Column (Reserved Field ID)
///
/// When a column in `schema` has the reserved field ID [`FILE_NAME`] (2147483646), the engine
/// must populate it with the file path/name:
///
/// - **Column name**: `"_file"`
/// - **Type**: `STRING` (non-nullable)
/// - **Field ID**: 2147483646 (reserved)
/// - **Values**: The file path/URL (e.g., `"s3://bucket/path/file.parquet"`)
/// - **Use case**: Track which file each row came from in multi-file reads
///
/// Example: All rows from the same file would have the same `_file` value.
///
/// # File-constant columns
///
/// [`Self::file_constant_columns`] names output fields whose values are identical for every row
/// in a given file (for example Delta partition columns or a table-changes `version`). Types and
/// nullability come from [`Self::schema`]; [`ScanFile::file_constants`] supplies the per-file
/// literals in the same order as `file_constant_columns`.
///
/// File-constant columns are distinct from [metadata columns] (engine-generated, such as row
/// index or `_file`) and from [`Load::metadata_derived_columns`] (values taken from an
/// upstream metadata row at read time).
///
/// # Invariants
///
/// - `files[i].file_constants.len() == file_constant_columns.len()` for every `i`.
/// - Every name in `file_constant_columns` resolves to a field in `schema` that is not a
///   metadata column.
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
/// parallel, and to interleave rows from different files. Downstream nodes that depend on
/// a specific ordering must impose it explicitly.
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
/// values as [`Scalar::Array`] / [`Scalar::Map`] -- nested leaves are not flattened
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

/// Projects the single input through `exprs` into rows of `output_schema`.
///
/// `exprs.len() == output_schema.fields().count()`. For each output index `i`, the engine
/// evaluates `exprs[i]` against an input row and binds the result to the **logical name**
/// `output_schema.fields()[i].name`. Output column names always come from `output_schema`,
/// not from the expression tree -- expressions only supply values. Downstream nodes see the
/// logical field names declared in `output_schema`.
///
/// # Example
///
/// Input `{ id, first, last, add: { path, size, modificationTime } }` projected to
/// `{ id, name, file_meta }` -- passthrough, concatenation, nested input access, and a
/// struct output column:
///
/// ```text
/// Project {
///     exprs: [
///         col("id"),
///         concat(col("first"), " ", col("last")),
///         Expression::Struct([
///             col("add.path"),
///             col("add.size"),
///             col("add.stats.numRecords"),
///         ]),
///     ],
///     output_schema: {
///         id: int,
///         name: string,
///         file_meta: { path: string, size: long, num_records: long },
///     },
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Project {
    pub exprs: Vec<ExpressionRef>,
    pub output_schema: SchemaRef,
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

/// Column names a [`Load`] reads from each upstream row to resolve which file to
/// open. All three columns must be present on the upstream relation; `file_size_column`
/// and `num_records_column` may be NULL on any given row. Engines use non-NULL size and
/// row-count values as split-sizing / pruning hints.
///
/// All three are [`ColumnName`]s and may reference nested fields (e.g. `add.path` on
/// a Delta-checkpoint upstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadColumnFileMeta {
    /// Column on the upstream relation holding the per-row file path /
    /// URL fragment. Joined to [`Load::base_url`] when set.
    pub path_column: ColumnName,
    /// Column with the file's total size in bytes (nullable per row).
    pub file_size_column: ColumnName,
    /// Column with the file's row-count, parquet-encoded `numRecords` (nullable per row).
    pub num_records_column: ColumnName,
}

/// Reads data files identified by an upstream stream of file-metadata tuples. Each
/// input row describes one file. `file_meta` names the path, file-size, and row-count
/// columns on the upstream relation (all required; size and row-count nullable per row).
/// The engine resolves each path against
/// `base_url`, opens the file as `file_type`, and reads columns matching `file_schema`
/// from it.
///
/// `metadata_derived_columns` lists columns on the upstream row whose values are
/// broadcast onto every emitted file row (e.g. `version` for table-changes scans, or
/// partition values).
///
/// `dv_column` names a (nullable) column on the upstream row holding a Delta
/// [`DeletionVectorDescriptor`] struct. The engine resolves it into a roaring bitmap
/// and drops file rows whose row index appears in the DV. A NULL value for a given
/// input row means "no DV for this file" -- all file rows are emitted. Tables that
/// never use deletion vectors should plumb an all-NULL upstream column.
///
/// [`DeletionVectorDescriptor`]: crate::actions::deletion_vector::DeletionVectorDescriptor
///
/// Each upstream path is resolved against `base_url`:
///
/// - **`Some(base)`**: each path column value is treated as a path relative to `base` and resolved
///   via [`Url::join`]. Paths that are themselves absolute URLs (any scheme prefix) bypass the join
///   and are used as-is, matching the Delta protocol convention for `Add.path`
///   (relative-to-table-root OR a fully-qualified URI).
/// - **`None`**: every path column value must already be an absolute URL; the engine errors on
///   relative paths.
///
/// Output row order is unspecified: the engine is free to read files in any order, in
/// parallel, and to interleave rows from different files. The relative order of upstream
/// rows is not preserved. Downstream nodes that depend on a specific ordering must impose
/// it explicitly.
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
///     file_schema: { id: int, name: string },
///     file_type: Parquet,
///     base_url: "s3://table/",
///     metadata_derived_columns: ["version"],
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
///     |  1 |  a   |       7
///     |  2 |  b   |       7
///     |  3 |  c   |       8
///     |  4 |  d   |       8
/// ```
///
/// # Errors
///
/// [`Error::FileNotFound`](crate::Error::FileNotFound) -- a file does not exist.
/// [`Error::IOError`](crate::Error::IOError) -- an error occurred while reading the file.
#[derive(Debug, Clone)]
pub struct Load {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub metadata_derived_columns: Vec<ColumnName>,
    pub file_meta: LoadColumnFileMeta,
    pub dv_column: ColumnName,
}

// === MaxByVersion ===========================================================

/// Per group, keep the input row with the greatest `version_column` value and project
/// the columns named in `output_schema` from that row. This is the plan-IR form of
/// "top 1 per group by version desc" that kernel uses when reconciling duplicate keys
/// across table versions (e.g. latest `add` per path in scan metadata).
///
/// Each `output_schema` field selects a column from the winning row; its declared type
/// must match that column's type in the input. Group-by expressions and the version
/// column are not implicitly projected -- include them in `output_schema` when needed.
///
/// # When this operator fits
///
/// The operator scales cleanly when there is **one** version column and the goal is to
/// carry **several columns from the same winning row** -- no other aggregates (`MIN`,
/// `MAX`, etc.) on different columns. That matches kernel's common dedupe pattern.
///
/// Engines that support SQL `MAX BY` can implement the same semantics more directly:
///
/// ```sql
/// SELECT
///     <group_by fields>,
///     MAX_BY(col_a, version_column),
///     MAX_BY(col_b, version_column),
///     ...
/// FROM input
/// GROUP BY <group_by>
/// ```
///
/// A nested query plus `ROW_NUMBER()` (or `QUALIFY`) is equivalent but grows verbose
/// when many columns must come from the winner; `MAX BY` (or this node) stays compact.
///
/// When a plan needs **multiple version columns** or **mixed aggregates** on the same
/// group, prefer a general `GROUP BY` plan (or an engine rewrite) instead of
/// `MaxByVersion`.
///
/// # Tiebreaking
///
/// If multiple input rows share the same group keys and the same `version_column`
/// value, which row wins is unspecified.
///
/// # Equivalent SQL (window form)
///
/// ```sql
/// SELECT <output_schema fields>
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
///     output_schema: { person: string, year: int, likes_to_eat: string },
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
    pub output_schema: SchemaRef,
}

// ============================================================================
// Binary operators (2 inputs)
// ============================================================================

/// Equi-join two inputs (`inputs.len() == 2`, convention `[probe, build]`) and emit a
/// subset of probe rows -- a SQL `SEMI JOIN` (`inverted = false`) or `ANTI JOIN`
/// (`inverted = true`). The build side filters but never contributes columns; output
/// schema equals the probe input's schema unchanged.
///
/// `probe_keys[i]` is matched for equality against `build_keys[i]` for each `i`;
/// the two vectors must have the same length (builder-enforced). A probe row "matches"
/// iff there exists at least one build row whose key tuple equals the probe row's
/// key tuple.
///
/// - `inverted = false` (semi join): emit each probe row that has at least one match.
/// - `inverted = true` (anti join): emit each probe row that has zero matches.
///
/// ```sql
/// -- inverted = false: SEMI JOIN
/// SELECT probe.*
/// FROM probe
/// WHERE EXISTS (
///     SELECT 1 FROM build
///     WHERE probe.k1 = build.k1 AND ... AND probe.kN = build.kN
/// );
///
/// -- inverted = true: ANTI JOIN
/// SELECT probe.*
/// FROM probe
/// WHERE NOT EXISTS (
///     SELECT 1 FROM build
///     WHERE probe.k1 = build.k1 AND ... AND probe.kN = build.kN
/// );
/// ```
///
/// # Example
///
/// ```text
/// SemiJoin { inverted: true, probe_keys: ["path"], build_keys: ["path"] }
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

// ============================================================================
// N-ary operators (variable inputs)
// ============================================================================

/// Concatenates N inputs (`inputs.len() >= 1`). All input schemas must agree.
///
/// # Example
///
/// `UnionAll` over two inputs with schema `{ id: int }`:
///
/// ```text
/// input 0       input 1
/// id            id
/// --            --
///  1             3
///  2             4
///
/// output:
/// id
/// --
///  1
///  2
///  3
///  4
/// ```
#[derive(Debug, Clone)]
pub struct UnionAll();
