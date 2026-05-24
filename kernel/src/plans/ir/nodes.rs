//! Plan node operator kinds and their payloads.
//!
//! [`NodeKind`] enumerates every operator. Each operator's payload struct is defined
//! below.

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
/// `Values`, `Load`, `Project`, `MaxByVersion`); for the rest the engine derives
/// the output schema from inputs and parameters.
#[derive(Debug, Clone)]
pub enum NodeKind {
    // === Source operators (0 inputs) =========================================
    ScanParquet(ScanParquetNode),
    ScanJson(ScanJsonNode),
    Values(ValuesNode),

    // === Unary operators (1 input) ===========================================
    Project(ProjectNode),
    Filter(FilterNode),
    Load(LoadNode),
    MaxByVersion(MaxByVersionNode),

    // === Binary operators (2 inputs) =========================================
    EquiJoin(EquiJoinNode),

    // === N-ary operators (variable inputs) ===================================
    UnionAll(UnionAllNode),
}

impl std::fmt::Display for NodeKind {
    /// Writes the variant name (e.g. `ScanParquet`, `Project`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NodeKind::ScanParquet(_) => "ScanParquet",
            NodeKind::ScanJson(_) => "ScanJson",
            NodeKind::Values(_) => "Values",
            NodeKind::Project(_) => "Project",
            NodeKind::Filter(_) => "Filter",
            NodeKind::Load(_) => "Load",
            NodeKind::MaxByVersion(_) => "MaxByVersion",
            NodeKind::EquiJoin(_) => "EquiJoin",
            NodeKind::UnionAll(_) => "UnionAll",
        })
    }
}

// ============================================================================
// Source operators (0 inputs)
// ============================================================================

/// Reads Parquet `files` into row batches matching `schema`. The engine must return exactly the
/// columns specified in `schema`, and they must be in schema order.
///
/// # Resolving Parquet schema to `schema`
///
/// When reading the Parquet file, the columns are resolved from the Parquet schema to `schema`.
/// To do so, the parquet reader must match each Parquet column to a [`StructField`] in `schema`.
/// All columns in the returned `EngineData` must be in the same order as specified in `schema`.
///
/// Parquet columns are matched to `schema` [`StructField`]s using the following rules:
/// 1. **Field ID**: If a [`StructField`] in `schema` contains a field ID (specified in
///    [`ColumnMetadataKey::ParquetFieldId`] metadata), use the ID to match the Parquet column's
///    field id.
/// 2. **Field Name**: If no field ID is present in the `schema`'s [`StructField`] or no matching
///    parquet field ID is found, fall back to matching by column name.
///
/// If no matching Parquet column is found, `NULL` values are returned for nullable columns in
/// `schema`. For non-nullable columns, an error is returned.
///
/// ## Column Matching Examples
///
/// Consider a `schema` with the following fields:
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
/// The column matching would work as follows:
/// - `"i_logical"` matches `"i"` by field ID (both have ID 1)
/// - `"i2"` matches `"i2"` by column name (no field ID to match on)
/// - `"s"` has no matching Parquet column, so NULL values are returned
///
/// The returned data will contain exactly 3 columns in schema order:
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
/// When a column in `schema` has the reserved field ID [`reserved_field_ids::FILE_NAME`]
/// (2147483646), the engine must populate it with the file path/name:
///
/// - **Column name**: `"_file"`
/// - **Type**: `STRING` (non-nullable)
/// - **Field ID**: 2147483646 (reserved)
/// - **Values**: The file path/URL (e.g., `"s3://bucket/path/file.parquet"`)
/// - **Use case**: Track which file each row came from in multi-file reads
///
/// Example: All rows from the same file would have the same `_file` value.
///
/// # Emission order
///
/// The engine must emit data from files in the order that `files` is given. For example if
/// files `["a", "b"]` is provided, the engine must first return all the data from file `"a"`,
/// _then_ all the data from file `"b"`. Moreover, for a given file, all of its rows must be in
/// order that they occur in the file. Consider a file with rows `(1, 2, 3)`. The following are
/// legal iterator batches:
///
/// ```text
/// iter: [EngineData(1, 2), EngineData(3)]
/// iter: [EngineData(1), EngineData(2, 3)]
/// iter: [EngineData(1, 2, 3)]
/// ```
///
/// The following are illegal batches:
///
/// ```text
/// iter: [EngineData(3), EngineData(1, 2)]
/// iter: [EngineData(1), EngineData(3, 2)]
/// iter: [EngineData(2, 1, 3)]
/// ```
///
/// Additionally, engines must not merge engine data across file boundaries.
///
/// # Errors
///
/// [`Error::Generic`](crate::Error::Generic) -- `schema` contains a non-nullable field that is
/// missing from the Parquet file. [`Error::FileNotFound`](crate::Error::FileNotFound) -- `files`
/// contains a file that does not exist. [`Error::IOError`](crate::Error::IOError) -- an error
/// occurred while reading the Parquet file.
///
/// [`ColumnMetadataKey::ParquetFieldId`]: crate::schema::ColumnMetadataKey::ParquetFieldId
/// [`StructField`]: crate::schema::StructField
/// [`StructField::create_metadata_column`]: crate::schema::StructField::create_metadata_column
/// [`MetadataColumnSpec::RowIndex`]: crate::schema::MetadataColumnSpec::RowIndex
/// [`reserved_field_ids::FILE_NAME`]: crate::reserved_field_ids::FILE_NAME
#[derive(Debug, Clone)]
pub struct ScanParquetNode {
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
}

/// Reads newline-delimited JSON `files` (one JSON object per line) into row batches matching
/// `schema`.
///
/// Missing fields in a row produce NULL for nullable `schema` fields and an error for
/// non-nullable fields.
///
/// # Emission order
///
/// The engine must emit data from files in the order that `files` is given. For example if
/// files `["a", "b"]` is provided, the engine must first return all the data from file `"a"`,
/// _then_ all the data from file `"b"`. Moreover, for a given file, all of its rows must be in
/// order that they occur in the file. Consider a file with rows `(1, 2, 3)`. The following are
/// legal iterator batches:
///
/// ```text
/// iter: [EngineData(1, 2), EngineData(3)]
/// iter: [EngineData(1), EngineData(2, 3)]
/// iter: [EngineData(1, 2, 3)]
/// ```
///
/// The following are illegal batches:
///
/// ```text
/// iter: [EngineData(3), EngineData(1, 2)]
/// iter: [EngineData(1), EngineData(3, 2)]
/// iter: [EngineData(2, 1, 3)]
/// ```
///
/// Additionally, engines may not merge engine data across file boundaries.
#[derive(Debug, Clone)]
pub struct ScanJsonNode {
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
}

/// Inline literal rows. Each `rows[i]` has one [`Scalar`] per field in `schema`, in
/// field order; `rows[i].len() == schema.fields().count()` for every row.
///
/// # Example
///
/// Two rows over `{ id: int, active: bool }`:
///
/// ```text
/// ValuesNode {
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
#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

// ============================================================================
// Unary operators (1 input)
// ============================================================================

/// Projects the single input through `exprs` into rows of `output_schema`.
///
/// `exprs.len() == output_schema.fields().count()`: for each output field `i`, the engine
/// evaluates `exprs[i]` against an input row and binds the value to the field named by
/// `output_schema.fields()[i].name`. Engines compile against `output_schema` directly and do
/// not re-derive it from the expressions.
///
/// # Example
///
/// Projecting an input `{ id: int, first: string, last: string }` to
/// `{ id: int, name: string }` by renaming and concatenating:
///
/// ```text
/// ProjectNode {
///     exprs: [
///         col("id"),
///         concat(col("first"), " ", col("last")),
///     ],
///     output_schema: { id: int, name: string },
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub exprs: Vec<ExpressionRef>,
    pub output_schema: SchemaRef,
}

/// Keeps input rows where `predicate` evaluates true (SQL null semantics).
/// Output schema is the input schema unchanged.
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub predicate: PredicateRef,
}

// === Load ===================================================================

/// File formats supported by [`LoadNode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Column names a [`LoadNode`] reads from each upstream row to resolve which file to
/// open. `path_column` is required; `file_size_column` and `num_records_column` are
/// optional and used by engines as split-sizing / pruning hints.
///
/// All three are [`ColumnName`]s and may reference nested fields (e.g. `add.path` on
/// a Delta-checkpoint upstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadColumnFileMeta {
    /// Column on the upstream relation holding the per-row file path /
    /// URL fragment. Joined to [`LoadNode::base_url`] when set.
    pub path_column: ColumnName,
    /// Optional column with the file's total size in bytes.
    pub file_size_column: Option<ColumnName>,
    /// Optional column with the file's row-count (parquet-encoded `numRecords`).
    pub num_records_column: Option<ColumnName>,
}

/// Reads data files identified by an upstream stream of file-metadata tuples. Each
/// input row describes one file. `file_meta.path_column` names the path column on the
/// upstream relation (required); `file_meta.file_size_column` and
/// `file_meta.num_records_column` name the file's size and row-count columns (optional,
/// engines use them as split-sizing hints). The engine resolves each path against
/// `base_url`, opens the file as `file_type`, and reads columns matching `file_schema`
/// from it.
///
/// `metadata_derived_columns` lists columns on the upstream row whose values are
/// broadcast onto every emitted file row (e.g. `version` for table-changes scans, or
/// partition values).
///
/// `dv_column`, when set, names a column on the upstream row holding a Delta
/// [`DeletionVectorDescriptor`] struct. The engine resolves it into a roaring bitmap and
/// drops file rows whose row index appears in the DV.
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
/// # Example
///
/// Given an upstream metadata stream and a `LoadNode` configuration:
///
/// ```text
/// upstream (metadata)
///     path             | size | version
///     -----------------+------+---------
///     part-0.parquet   | 1024 |       7
///     part-1.parquet   | 2048 |       8
/// ```
/// ```text
/// LoadNode {
///     file_schema: { id: int, name: string },
///     file_type: Parquet,
///     base_url: "s3://table/",
///     metadata_derived_columns: ["version"],
///     file_meta: {
///         path_column: "path",
///         file_size_column: Some("size"),
///         num_records_column: None
///     },
///     dv_column: None,
/// }
/// ```
/// The engine opens `s3://table/part-0.parquet` and `s3://table/part-1.parquet`, reads
/// `{id, name}` from each, and broadcasts the row's `version` onto every emitted file
/// row. Output:
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
pub struct LoadNode {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub metadata_derived_columns: Vec<ColumnName>,
    pub file_meta: LoadColumnFileMeta,
    pub dv_column: Option<ColumnName>,
}

// === MaxByVersion ===========================================================

/// "Top 1 per group, ordered by version desc" -- a specialized aggregate. Emitted rows
/// match `output_schema`: each field name selects a column from the winning input row,
/// and the field's declared type must match that column's type in the input. Group-by
/// expressions and the version column are not implicitly projected. They must be included
/// in the `output_schema`.
///
/// Equivalent SQL:
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
/// Ties (multiple input rows with the same group keys AND the same version value) are
/// broken by input order: the first such row encountered wins.
///
/// # Example
///
/// ```text
/// MaxByVersionNode {
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
pub struct MaxByVersionNode {
    pub group_by: Vec<ExpressionRef>,
    pub version_column: ColumnName,
    pub output_schema: SchemaRef,
}

// ============================================================================
// Binary operators (2 inputs)
// ============================================================================

/// Equi-join semantics. Each variant documents which rows it emits and the resulting
/// output schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Emit each left row whose key matches no right row. Right rows contribute
    /// nothing to the output; the output schema equals the left input schema.
    LeftAnti,
}

/// Equi-join two inputs (`inputs.len() == 2`, convention `[left, right]`).
/// `left_keys[i]` is matched against `right_keys[i]` for each `i`; the two vectors
/// must have the same length, which the builder enforces.
///
/// # Example
///
/// `LeftAnti` join "keep `left` rows whose `path` does not appear in `right`":
///
/// ```text
/// EquiJoinNode {
///     kind: JoinKind::LeftAnti,
///     left_keys:  ["path"],
///     right_keys: ["path"],
/// }
///
/// left                right
/// path | version      path
/// -----+--------      ----
///  a   |   1           b
///  b   |   2           d
///  c   |   3
///
/// output (left rows whose path is not in right):
/// path | version
/// -----+--------
///  a   |   1
///  c   |   3
/// ```
#[derive(Debug, Clone)]
pub struct EquiJoinNode {
    pub kind: JoinKind,
    pub left_keys: Vec<ColumnName>,
    pub right_keys: Vec<ColumnName>,
}

// ============================================================================
// N-ary operators (variable inputs)
// ============================================================================

/// Concatenates N inputs (`inputs.len() >= 1`). All input schemas must agree.
/// `ordered=true` preserves child order; `ordered=false` permits reordering.
///
/// # Example
///
/// `UnionAllNode { ordered: true }` over two inputs with schema `{ id: int }`:
///
/// ```text
/// input 0       input 1
/// id            id
/// --            --
///  1             3
///  2             4
///
/// output (ordered=true preserves child order):
/// id
/// --
///  1
///  2
///  3
///  4
/// ```
///
/// With `ordered=false` the engine may interleave or reorder the inputs' rows.
#[derive(Debug, Clone)]
pub struct UnionAllNode {
    pub ordered: bool,
}
