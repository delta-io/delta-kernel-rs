//! Plan node operator kinds and their payloads.
//!
//! [`Operator`] enumerates every operator. Each operator's payload struct is defined
//! below.

use std::sync::Arc;

use strum::Display;
use url::Url;

use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar};
use crate::scan::data_skipping::stats_schema::StripFieldMetadataTransform;
use crate::schema::{SchemaRef, StructField, StructType};
use crate::transforms::SchemaTransform as _;
use crate::utils::CollectInto;
use crate::{DeltaResult, Error, FileMeta};

// ============================================================================
// Operator: enumerates every operator kind
// ============================================================================

/// Plan node operators.
///
/// Output schemas are stored on the payload struct for operators whose caller
/// declares them (`ScanParquet`, `ScanJson`, `Values`, `Load`, `Project`,
/// `Aggregate`); the remaining operators pass an input's schema through
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
    Aggregate(Aggregate),

    // === Binary operators (2 inputs) =========================================
    SemiJoin(SemiJoin),

    // === N-ary operators (variable inputs) ===================================
    UnionAll(UnionAll),
}

/// Generate `From<Payload> for Operator` for each listed variant, wrapping the payload in the
/// same-named [`Operator`] variant. Example: `Filter { .. }.into()` yields `Operator::Filter`).
macro_rules! impl_from_payload_for_operator {
    ($($variant:ident),+ $(,)?) => {
        $(impl From<$variant> for Operator {
            fn from(payload: $variant) -> Self {
                Operator::$variant(payload)
            }
        })+
    };
}

impl_from_payload_for_operator!(
    ScanParquet,
    ScanJson,
    Values,
    Project,
    Filter,
    Load,
    Aggregate,
    SemiJoin,
    UnionAll,
);

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
    pub file_constant_columns: Vec<String>,
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
    pub file_constant_columns: Vec<String>,
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

impl Values {
    /// Literal `rows` matching `schema`. Empty `rows` is the uninhabited relation over `schema`.
    pub fn new(schema: impl Into<SchemaRef>, rows: Vec<Vec<Scalar>>) -> Self {
        Self {
            schema: schema.into(),
            rows,
        }
    }
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

impl LoadColumnFileMeta {
    /// The columns naming each file's path, size, and row-count.
    pub fn new(
        path_column: ColumnName,
        file_size_column: ColumnName,
        num_records_column: ColumnName,
    ) -> Self {
        Self {
            path_column,
            file_size_column,
            num_records_column,
        }
    }
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
///     schema: { id: int, name: string, version: long },
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
    pub file_constant_columns: Vec<String>,
    pub file_meta: LoadColumnFileMeta,
    pub dv_column: ColumnName,
}

impl Load {
    /// A [`Load`] over `schema` reading `file_type` files, with no base URL and no file-constant
    /// columns. Add those with [`Self::with_base_url`] / [`Self::with_file_constant_columns`].
    pub fn new(
        schema: impl Into<SchemaRef>,
        file_type: FileType,
        file_meta: LoadColumnFileMeta,
        dv_column: ColumnName,
    ) -> Self {
        Self {
            schema: schema.into(),
            file_type,
            base_url: None,
            file_constant_columns: Vec::new(),
            file_meta,
            dv_column,
        }
    }

    /// Set the base URL that per-row file paths resolve against.
    pub fn with_base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Set the output columns broadcast from the upstream row (see
    /// [`Self::file_constant_columns`]).
    pub fn with_file_constant_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.file_constant_columns = columns.into_iter().map(Into::into).collect();
        self
    }
}

/// Groups input rows by `group_by` (a global aggregation over all rows when `group_by` is
/// empty) and computes one output column per [`Agg`] in `aggs`. The output `schema` lists the
/// group-by key columns first (in order), then the aggregate columns (in order).
///
/// Build an `Aggregate` with [`Aggregate::group_by`], which derives `schema` from the input
/// schema -- including each output column's name, type, and nullability.
///
/// # Output schema
///
/// - **Group keys** pass through verbatim: each key column keeps its input type, nullability, and
///   metadata.
/// - **Aggregate columns**: name, type, and nullability come from each [`Agg`] (see per-function
///   docs); use [`AggregateBuilder::aggregate_as`] to override the name.
///
/// # SQL equivalent
///
/// ```sql
/// SELECT
///     <group_by fields>,
///     <aggs>
/// FROM input
/// GROUP BY <group_by fields>
/// ```
///
/// SQL grouping uses null-safe equals, so `(NULL, b)`, `(a, NULL)`, and `(NULL, NULL)` are all
/// different groups.
///
/// # Example
///
/// Each person's favorite food as of their most recent year -- group by `person`, then take the
/// `likes_to_eat` from the row with the greatest `year`:
///
/// ```text
/// Aggregate {
///     group_by: [person],
///     aggs: [max_non_null_by(likes_to_eat, year)],
///     schema: { person: string, likes_to_eat: string },
/// }
/// ```
///
/// Input:
///
/// ```text
/// person   | year | likes_to_eat
/// ---------+------+-------------
///  Bob     | 2020 | pizza
///  Alice   | 2026 | sushi
///  Charlie | 2021 | ice cream
///  Bob     | 2025 | watermelon
///  Alice   | 2020 | pizza
///  Charlie | 2026 | egg
/// ```
///
/// Output:
///
/// ```text
/// person   | likes_to_eat
/// ---------+-------------
///  Bob     | watermelon
///  Charlie | egg
///  Alice   | sushi
/// ```
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Group-by key columns, emitted first in the output schema. Empty means a single global
    /// group over all input rows.
    pub group_by: Vec<ColumnName>,
    /// The aggregate columns, emitted after the group keys in the output schema.
    pub aggs: Vec<Agg>,
    /// Output schema: group-by key columns followed by aggregate columns.
    pub schema: SchemaRef,
}

impl Aggregate {
    /// Starts building an [`Aggregate`] over `input_schema`, grouped by `keys`. Pass an empty
    /// iterator for a global aggregate (a single group over all input rows). Group keys are emitted
    /// first in the output schema, in iteration order; add aggregators with the named helpers
    /// ([`max`](AggregateBuilder::max), ...) or [`aggregate`](AggregateBuilder::aggregate). The
    /// builder derives the output schema from the keys and aggregators.
    pub fn group_by(
        input_schema: SchemaRef,
        keys: impl CollectInto<Vec<ColumnName>>,
    ) -> AggregateBuilder {
        AggregateBuilder {
            input_schema,
            group_by: keys.collect_into(),
            aggs: Vec::new(),
        }
    }
}

/// An aggregate function and its operand column(s) within an [`Aggregate`] operator.
#[derive(Debug, Clone)]
pub enum Agg {
    Min(Min),
    Max(Max),
    MinNonNullBy(MinNonNullBy),
    MaxNonNullBy(MaxNonNullBy),
}

impl Agg {
    /// Like [`max`](Self::max), but selects the least non-NULL value in each group.
    ///
    /// ```text
    /// [3, NULL, 5, 1] -> 1
    /// [NULL, NULL]    -> NULL
    /// []              -> NULL
    /// ```
    pub fn min(value: impl Into<ColumnName>) -> Self {
        Self::Min(Min {
            value: value.into(),
        })
    }

    /// The greatest non-NULL value in each group, or NULL if the group has no non-NULL value.
    /// The output is always nullable, with name and type matching `value`.
    ///
    /// ```text
    /// [3, NULL, 5, 1] -> 5
    /// [NULL, NULL]    -> NULL
    /// []              -> NULL
    /// ```
    pub fn max(value: impl Into<ColumnName>) -> Self {
        Self::Max(Max {
            value: value.into(),
        })
    }

    /// Like [`max_non_null_by`](Self::max_non_null_by), but selects the `value` from the row with
    /// the *least* `key`.
    pub fn min_non_null_by(value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        Self::MinNonNullBy(MinNonNullBy {
            value: value.into(),
            key: key.into(),
        })
    }

    /// The `value` from the row with the greatest `key`, considering only rows where *both* `value`
    /// and `key` are non-null. NULL if no such row exists. If multiple rows tie for the greatest
    /// `key`, which one's `value` is returned is unspecified. The output is always nullable, with
    /// name and type matching `value`.
    ///
    /// ```text
    ///  key | value     ->  c
    /// -----+------
    ///    1 | a
    ///    3 | c             (greatest key with both key and value non-NULL)
    ///    5 | NULL          (ignored: NULL value)
    /// NULL | d             (ignored: NULL key)
    ///
    /// (no rows)        ->  NULL
    /// ```
    ///
    /// Equivalent to SQL `max_by(value, key) FILTER (WHERE value IS NOT NULL)`: `max_by` already
    /// ignores NULL keys, and the filter additionally drops NULL values.
    ///
    /// In systems without `max_by`, it can also be expressed using window functions:
    ///
    /// ```sql
    /// SELECT
    ///     <group_by columns>,
    ///     value
    /// FROM (
    ///     SELECT
    ///         value,
    ///         <group_by columns>,
    ///         ROW_NUMBER() OVER (
    ///             PARTITION BY <group_by columns>
    ///             ORDER BY key DESC
    ///         ) AS rn
    ///     FROM input
    ///     WHERE key IS NOT NULL AND value IS NOT NULL
    /// ) WHERE rn = 1
    /// ```
    pub fn max_non_null_by(value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        Self::MaxNonNullBy(MaxNonNullBy {
            value: value.into(),
            key: key.into(),
        })
    }

    /// Derives this aggregate's output [`StructField`] over `input_schema`, validating that every
    /// operand column resolves. The output takes the value column's type (with field metadata
    /// stripped).
    fn output_field(
        &self,
        input_schema: &StructType,
        alias: Option<String>,
    ) -> DeltaResult<StructField> {
        let value = match self {
            Agg::Min(Min { value }) | Agg::Max(Max { value }) => value,
            Agg::MinNonNullBy(MinNonNullBy { value, key })
            | Agg::MaxNonNullBy(MaxNonNullBy { value, key }) => {
                input_schema.field_at(key)?;
                value
            }
        };
        let name = alias
            .or_else(|| value.path().last().cloned())
            .ok_or_else(|| {
                Error::generic("Cannot derive default output name from empty column path")
            })?;
        let data_type = StripFieldMetadataTransform
            .transform(input_schema.field_at(value)?.data_type())
            .into_owned();
        Ok(StructField::nullable(name, data_type))
    }
}

/// Operands for [`Agg::min`].
#[derive(Debug, Clone)]
pub struct Min {
    pub value: ColumnName,
}

/// Operands for [`Agg::max`].
#[derive(Debug, Clone)]
pub struct Max {
    pub value: ColumnName,
}

/// Operands for [`Agg::min_non_null_by`].
#[derive(Debug, Clone)]
pub struct MinNonNullBy {
    /// Column whose value the aggregate emits.
    pub value: ColumnName,
    /// Column compared across rows to pick the winning (least-key) row.
    pub key: ColumnName,
}

/// Operands for [`Agg::max_non_null_by`].
#[derive(Debug, Clone)]
pub struct MaxNonNullBy {
    /// Column whose value the aggregate emits.
    pub value: ColumnName,
    /// Column compared across rows to pick the winning (greatest-key) row.
    pub key: ColumnName,
}

/// Builds an [`Aggregate`] over an input schema, deriving the output schema from the group keys
/// and aggregators.
///
/// Created by [`Aggregate::group_by`], which fixes the group keys. Aggregators are then collected
/// by the named helpers or [`aggregate`](Self::aggregate); [`build`](Self::build) resolves keys and
/// aggregators against the input schema; derives each output column's name, type and nullability
/// from its [`Agg`] or group-by column; and validates that all output column names are unique.
#[derive(Debug)]
pub struct AggregateBuilder {
    input_schema: SchemaRef,
    group_by: Vec<ColumnName>,
    aggs: Vec<(Agg, Option<String>)>,
}

impl AggregateBuilder {
    /// Adds an aggregate column, emitted after the group keys in call order, using each [`Agg`]'s
    /// default output name (see the per-function docs). Prefer the named helpers for the common
    /// case (e.g. [`max`](Self::max); use [`aggregate_as`](Self::aggregate_as) to override the
    /// output name.
    pub fn aggregate(mut self, agg: Agg) -> Self {
        self.aggs.push((agg, None));
        self
    }

    /// Like [`aggregate`](Self::aggregate), but with the specified output name.
    pub fn aggregate_as(mut self, agg: Agg, name: impl Into<String>) -> Self {
        self.aggs.push((agg, Some(name.into())));
        self
    }

    /// Adds an unaliased [`Agg::min`] over `value`.
    pub fn min(self, value: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::min(value))
    }

    /// Adds an unaliased [`Agg::max`] over `value`.
    pub fn max(self, value: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::max(value))
    }

    /// Adds an unaliased [`Agg::min_non_null_by`] over `value`, keyed on `key`.
    pub fn min_non_null_by(self, value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::min_non_null_by(value, key))
    }

    /// Adds an unaliased [`Agg::max_non_null_by`] over `value`, keyed on `key`.
    pub fn max_non_null_by(self, value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::max_non_null_by(value, key))
    }

    /// Resolves group keys and aggregators against the input schema and builds the [`Aggregate`].
    ///
    /// # Errors
    ///
    /// Returns an error if a group key or an aggregate's operand column is not found in the input
    /// schema, or if two output columns would share a name (case-insensitive).
    pub fn build(self) -> DeltaResult<Aggregate> {
        let mut fields = Vec::with_capacity(self.group_by.len() + self.aggs.len());
        for key in &self.group_by {
            fields.push(self.input_schema.field_at(key)?.clone());
        }
        let mut aggs = Vec::with_capacity(self.aggs.len());
        for (agg, alias) in self.aggs {
            fields.push(agg.output_field(&self.input_schema, alias)?);
            aggs.push(agg);
        }
        // NOTE: `StructType::try_new` rejects duplicate (case-insensitive) output column names.
        Ok(Aggregate {
            group_by: self.group_by,
            aggs,
            schema: Arc::new(StructType::try_new(fields)?),
        })
    }
}

impl TryFrom<AggregateBuilder> for Aggregate {
    type Error = Error;

    fn try_from(builder: AggregateBuilder) -> DeltaResult<Self> {
        builder.build()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::column_name;
    use crate::schema::{DataType, StructField};
    use crate::utils::test_utils::assert_result_error_with_message;

    /// Builds a flat `LONG` schema from `(name, nullable)` pairs.
    fn schema(fields: &[(&str, bool)]) -> SchemaRef {
        Arc::new(StructType::new_unchecked(fields.iter().map(
            |(name, nullable)| StructField::new(*name, DataType::LONG, *nullable),
        )))
    }

    #[test]
    fn output_lists_group_keys_then_aggregates_in_order() {
        let input = schema(&[("g", false), ("a", true), ("b", true)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .max(column_name!("a"))
            .min(column_name!("b"))
            .build()
            .unwrap();
        let names: Vec<&str> = agg.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["g", "a", "b"]);
    }

    #[test]
    fn group_key_passes_through_verbatim() {
        let input = schema(&[("g", false), ("a", true)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .max(column_name!("a"))
            .build()
            .unwrap();
        let key = agg.schema.field("g").unwrap();
        // A non-nullable key column stays non-nullable.
        assert!(!key.nullable);
    }

    /// Aggregate output is always nullable, regardless of grouping or input column nullability.
    #[rstest::rstest]
    fn output_field_always_nullable(
        #[values(true, false)] grouped: bool,
        #[values(true, false)] has_key: bool,
        #[values(true, false)] value_nullable: bool,
        #[values(true, false)] key_nullable: bool,
    ) {
        let mut fields = vec![("g", false), ("a", value_nullable)];
        if has_key {
            fields.push(("v", key_nullable));
        }
        let input = schema(&fields);
        let keys: Vec<ColumnName> = grouped.then(|| column_name!("g")).into_iter().collect();
        let builder = Aggregate::group_by(input, keys);
        let builder = if has_key {
            builder.max_non_null_by(column_name!("a"), column_name!("v"))
        } else {
            builder.max(column_name!("a"))
        };
        let agg = builder.build().unwrap();
        assert!(agg.schema.field("a").unwrap().nullable);
    }

    #[test]
    fn alias_overrides_default_output_name() {
        let input = schema(&[("a", true)]);
        let agg = Aggregate::group_by(input, [])
            .aggregate_as(Agg::max(column_name!("a")), "a_max")
            .build()
            .unwrap();
        assert!(agg.schema.field("a_max").is_some());
        assert!(agg.schema.field("a").is_none());
    }

    #[test]
    fn duplicate_output_names_are_rejected() {
        let input = schema(&[("a", true)]);
        // min and max of the same column collide on the default name "a".
        let result = Aggregate::group_by(input, [])
            .min(column_name!("a"))
            .max(column_name!("a"))
            .build();
        assert_result_error_with_message(result, "Duplicate field name");
    }

    #[test]
    fn distinct_aliases_resolve_min_max_collision() {
        let input = schema(&[("a", true)]);
        let agg = Aggregate::group_by(input, [])
            .aggregate_as(Agg::min(column_name!("a")), "a_min")
            .aggregate_as(Agg::max(column_name!("a")), "a_max")
            .build()
            .unwrap();
        let names: Vec<&str> = agg.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["a_min", "a_max"]);
    }

    #[rstest::rstest]
    #[case::missing_value_column(false)]
    #[case::missing_group_key(true)]
    fn build_rejects_missing_column(#[case] missing_in_key: bool) {
        let input = schema(&[("a", true)]);
        let (keys, value) = if missing_in_key {
            (vec![column_name!("missing")], column_name!("a"))
        } else {
            (vec![], column_name!("missing"))
        };
        let result = Aggregate::group_by(input, keys).max(value).build();
        assert_result_error_with_message(result, "missing");
    }

    /// A `*_non_null_by` aggregate whose key column is absent is rejected, even with no group keys
    /// (i.e. the validation does not ride on the grouped/nullability path).
    #[test]
    fn build_rejects_missing_non_null_by_key() {
        let input = schema(&[("a", true)]);
        let result = Aggregate::group_by(input, [])
            .max_non_null_by(column_name!("a"), column_name!("missing"))
            .build();
        assert_result_error_with_message(result, "missing");
    }
}
