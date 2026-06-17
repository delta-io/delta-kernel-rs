//! Plan node operator kinds and their payloads.
//!
//! [`Operator`] enumerates every operator. Each operator's payload struct is defined
//! below.

use std::sync::Arc;

use strum::Display;
use url::Url;

use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
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
    /// The unordered bag union of N input relations. All rows of all inputs appear in the
    /// output, in arbitrary order. All input schemas must agree, and the output schema
    /// is the common schema of the inputs.
    ///
    /// # Example
    ///
    /// `UnionAll` over two relations with schema `{ id: int }`:
    ///
    /// ```text
    /// input 0:          input 1:
    /// id                id
    /// --                --
    ///  1                 3
    ///  2                 4
    ///  3                 5
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
    UnionAll,
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

/// Groups input rows by `group_by` (a global aggregation over all rows when `group_by` is
/// empty) and computes one output column per [`Agg`] in `aggs`. The output `schema` lists the
/// group-by key columns first (in order), then the aggregate columns (in order).
///
/// Build an `Aggregate` with [`Aggregate::group_by`], which derives `schema` from the input
/// schema -- including each output column's type and nullability -- so callers never restate it.
///
/// # Output schema
///
/// - **Group keys** pass through verbatim: each key column keeps its input type, nullability, and
///   metadata.
/// - **Aggregate columns** take the type of their value column. Nullability is determined by the
///   function and whether the group is guaranteed non-empty (see [`Agg`]).
///
/// # SQL equivalent
///
/// ```sql
/// SELECT <group_by columns>, <aggs>
/// FROM input
/// GROUP BY <group_by columns>
/// ```
///
/// # Example
///
/// Latest non-null protocol and metadata across an unordered union of log actions (the kernel
/// Protocol & Metadata query), a global aggregation with no group keys:
///
/// ```text
/// Aggregate {
///     group_by: [],
///     aggs: [
///         max_by(protocol, version) FILTER (protocol IS NOT NULL),
///         max_by(metadata, version) FILTER (metadata IS NOT NULL),
///     ],
///     schema: { protocol: <struct>?, metadata: <struct>? },
/// }
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
    /// first in the output schema, in iteration order; add aggregators with
    /// [`aggregate`](AggregateBuilder::aggregate). The builder derives the output schema from the
    /// keys and aggregators.
    pub fn group_by(
        input_schema: SchemaRef,
        keys: impl CollectInto<Vec<ColumnName>>,
    ) -> AggregateBuilder {
        AggregateBuilder::new(input_schema, keys)
    }

    /// The single row a global (empty `group_by`) aggregate emits over an empty input: one
    /// [`Scalar`] per output field, in schema order, each being the corresponding [`Agg`]'s value
    /// over the empty multiset (see `AggOp::empty_value`).
    ///
    /// Only meaningful for a global aggregate: a grouped one yields zero rows over empty input. With
    /// no group keys the output schema is exactly the aggregate columns, so aggs and fields align
    /// 1:1.
    pub(crate) fn empty_group_row(&self) -> Vec<Scalar> {
        self.aggs
            .iter()
            .zip(self.schema.fields())
            .map(|(agg, field)| agg.op.empty_value(field.data_type()))
            .collect()
    }
}

/// One aggregate function application within an [`Aggregate`] operator.
///
/// An `Agg` is a function ([`AggOp`]) applied to input column(s), optionally restricted by a
/// `filter` predicate (SQL `FILTER (WHERE ...)`) and renamed via `alias`. When `alias` is unset,
/// the output column takes the name of the value column.
///
/// Construct with [`min`](Self::min) / [`max`](Self::max) / [`min_by`](Self::min_by) /
/// [`max_by`](Self::max_by), then chain [`filter`](Self::filter) / [`alias`](Self::alias):
///
/// ```ignore
/// Agg::max_by(column_name!("protocol"), column_name!("version"))
///     .filter(protocol_is_not_null)
///     .alias("latest_protocol")
/// ```
///
/// # Nullability
///
/// An aggregate output is nullable when the per-group input multiset can be empty -- either there
/// are no group keys (a global aggregate over possibly-empty input) or a `filter` is present
/// (which can remove every row of a group) -- or when the function itself can yield NULL over a
/// non-empty group:
/// - [`min`](Self::min) / [`max`](Self::max): nullable iff the value column is nullable.
/// - [`min_by`](Self::min_by) / [`max_by`](Self::max_by): nullable iff the value column or the key
///   column is nullable.
#[derive(Debug, Clone)]
pub struct Agg {
    /// The aggregate function and its operand column(s).
    pub op: AggOp,
    /// Optional `FILTER (WHERE ...)` predicate restricting the rows the function sees.
    pub filter: Option<PredicateRef>,
    /// Optional output column name. Defaults to the value column's name when unset.
    pub alias: Option<String>,
}

impl Agg {
    /// `min(value)`: the least non-null value in each group.
    pub fn min(value: impl Into<ColumnName>) -> Self {
        Self::new(AggOp::Min(Min {
            value: value.into(),
        }))
    }

    /// `max(value)`: the greatest non-null value in each group.
    pub fn max(value: impl Into<ColumnName>) -> Self {
        Self::new(AggOp::Max(Max {
            value: value.into(),
        }))
    }

    /// `min_by(value, key)`: the `value` from the row with the least non-null `key`.
    pub fn min_by(value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        Self::new(AggOp::MinBy(MinBy {
            value: value.into(),
            key: key.into(),
        }))
    }

    /// `max_by(value, key)`: the `value` from the row with the greatest non-null `key`.
    pub fn max_by(value: impl Into<ColumnName>, key: impl Into<ColumnName>) -> Self {
        Self::new(AggOp::MaxBy(MaxBy {
            value: value.into(),
            key: key.into(),
        }))
    }

    fn new(op: AggOp) -> Self {
        Self {
            op,
            filter: None,
            alias: None,
        }
    }

    /// Restricts the aggregate to rows where `predicate` is true (SQL `FILTER (WHERE ...)`).
    pub fn filter(mut self, predicate: impl Into<PredicateRef>) -> Self {
        self.filter = Some(predicate.into());
        self
    }

    /// Sets the output column name, overriding the default (the value column's name).
    pub fn alias(mut self, name: impl Into<String>) -> Self {
        self.alias = Some(name.into());
        self
    }

    /// The output column name: the alias if set, else the value column's leaf name.
    fn output_name(&self) -> DeltaResult<&str> {
        if let Some(alias) = &self.alias {
            return Ok(alias.as_str());
        }
        self.op
            .value()
            .path()
            .last()
            .map(String::as_str)
            .ok_or_else(|| Error::generic("Aggregate value column has an empty path"))
    }

    /// Derives this aggregate's output [`StructField`] over `input_schema`. `grouped` indicates
    /// whether the enclosing [`Aggregate`] has any group keys (which guarantees non-empty groups).
    fn output_field(&self, input_schema: &StructType, grouped: bool) -> DeltaResult<StructField> {
        let name = self.output_name()?;
        let data_type = input_schema.field_at(self.op.value())?.data_type().clone();
        // A group's input multiset can be empty when there are no group keys (global aggregate
        // over possibly-empty input) or a filter is present (can remove every row).
        let can_be_empty = !grouped || self.filter.is_some();
        let nullable = can_be_empty || self.op.base_nullable(input_schema)?;
        Ok(StructField::new(name, data_type, nullable))
    }
}

/// An aggregate function and its operand column(s).
///
/// Each variant wraps a payload carrying exactly that function's operands, mirroring the
/// [`Operator`] convention.
#[derive(Debug, Clone)]
pub enum AggOp {
    Min(Min),
    Max(Max),
    MinBy(MinBy),
    MaxBy(MaxBy),
}

/// `min(value)` operands.
#[derive(Debug, Clone)]
pub struct Min {
    pub value: ColumnName,
}

/// `max(value)` operands.
#[derive(Debug, Clone)]
pub struct Max {
    pub value: ColumnName,
}

/// `min_by(value, key)` operands.
#[derive(Debug, Clone)]
pub struct MinBy {
    pub value: ColumnName,
    pub key: ColumnName,
}

/// `max_by(value, key)` operands.
#[derive(Debug, Clone)]
pub struct MaxBy {
    pub value: ColumnName,
    pub key: ColumnName,
}

impl AggOp {
    /// The column whose value this aggregate emits (and whose type the output column takes).
    pub fn value(&self) -> &ColumnName {
        match self {
            AggOp::Min(Min { value })
            | AggOp::Max(Max { value })
            | AggOp::MinBy(MinBy { value, .. })
            | AggOp::MaxBy(MaxBy { value, .. }) => value,
        }
    }

    /// This function's value over an *empty* input multiset, typed as `data_type`. Every current
    /// aggregate (min/max/min_by/max_by) is NULL over the empty multiset; a future row-counting
    /// aggregate would override this (e.g. `count` -> `0`).
    fn empty_value(&self, data_type: &DataType) -> Scalar {
        match self {
            AggOp::Min(_) | AggOp::Max(_) | AggOp::MinBy(_) | AggOp::MaxBy(_) => {
                Scalar::null(data_type.clone())
            }
        }
    }

    /// Nullability contributed by the function over a guaranteed non-empty group, ignoring the
    /// empty-group / filter cases handled by [`Agg::output_field`].
    fn base_nullable(&self, input_schema: &StructType) -> DeltaResult<bool> {
        let value_nullable = input_schema.field_at(self.value())?.nullable;
        let nullable = match self {
            AggOp::Min(_) | AggOp::Max(_) => value_nullable,
            AggOp::MinBy(MinBy { key, .. }) | AggOp::MaxBy(MaxBy { key, .. }) => {
                value_nullable || input_schema.field_at(key)?.nullable
            }
        };
        Ok(nullable)
    }
}

/// Builds an [`Aggregate`] over an input schema, deriving the output schema from the group keys
/// and aggregators.
///
/// Created by [`Aggregate::group_by`], which fixes the group keys. Aggregators are then collected
/// by [`aggregate`](Self::aggregate); [`build`](Self::build) resolves keys and aggregators against
/// the input schema, derives each output column's type and nullability, and validates that all
/// output column names are unique.
#[derive(Debug)]
pub struct AggregateBuilder {
    input_schema: SchemaRef,
    group_by: Vec<ColumnName>,
    aggs: Vec<Agg>,
}

impl AggregateBuilder {
    /// Creates a builder over `input_schema` grouped by `keys`. Use [`Aggregate::group_by`].
    fn new(input_schema: SchemaRef, keys: impl CollectInto<Vec<ColumnName>>) -> Self {
        Self {
            input_schema,
            group_by: keys.collect_into(),
            aggs: Vec::new(),
        }
    }

    /// Adds an aggregate column, emitted after the group keys in call order.
    pub fn aggregate(mut self, agg: Agg) -> Self {
        self.aggs.push(agg);
        self
    }

    /// Resolves group keys and aggregators against the input schema and builds the [`Aggregate`].
    ///
    /// # Errors
    ///
    /// Returns an error if a group key or an aggregate's operand column is not found in the input
    /// schema, or if two output columns would share a name (case-insensitive).
    pub fn build(self) -> DeltaResult<Aggregate> {
        let grouped = !self.group_by.is_empty();
        let mut fields = Vec::with_capacity(self.group_by.len() + self.aggs.len());
        for key in &self.group_by {
            fields.push(self.input_schema.field_at(key)?.clone());
        }
        for agg in &self.aggs {
            fields.push(agg.output_field(&self.input_schema, grouped)?);
        }
        // `try_new` rejects duplicate (case-insensitive) output column names.
        let schema = Arc::new(StructType::try_new(fields)?);
        Ok(Aggregate {
            group_by: self.group_by,
            aggs: self.aggs,
            schema,
        })
    }
}

impl TryFrom<AggregateBuilder> for Aggregate {
    type Error = Error;

    /// Finalizes the builder via [`AggregateBuilder::build`], so APIs can accept an
    /// `impl TryInto<Aggregate>` and let callers pass an unbuilt builder directly.
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
///
/// # Collapse over absent inputs
///
/// A `semi_join`/`anti_join` builder combinator collapses over an absent input per the set
/// semantics above. The two differ in whether that collapse can be made orphan-safe (see the
/// hazard on [`PlanBuilder`](super::plan::PlanBuilder)):
/// - **Anti join** (difference) collapses to absent only if the *probe* is absent; an absent build
///   subtracts nothing, so it forwards the probe unchanged. Building the probe first is therefore
///   orphan-safe: skip the build side when the probe is absent, and an absent build simply forwards
///   a present probe.
/// - **Semi join** (intersection) collapses to absent if *either* input is absent (an absent probe
///   emits nothing; an absent build matches nothing). This *cannot* be made orphan-safe by
///   construction order: whichever side is built second strands the first when it turns out absent,
///   and a side's absence is only discovered by building it. Callers must instead decide semi join
///   presence from conditions known *before* constructing either side -- in practice the leaf scan
///   file lists, since absence propagates deterministically from the leaves -- and build the two
///   sides only when both are guaranteed present.
#[derive(Debug, Clone)]
pub struct SemiJoin {
    pub inverted: bool,
    pub probe_keys: Vec<ColumnName>,
    pub build_keys: Vec<ColumnName>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{column_name, Expression, Predicate};
    use crate::schema::{DataType, StructField};
    use crate::utils::test_utils::assert_result_error_with_message;

    /// Builds a flat `LONG` schema from `(name, nullable)` pairs.
    fn schema(fields: &[(&str, bool)]) -> SchemaRef {
        Arc::new(StructType::new_unchecked(fields.iter().map(
            |(name, nullable)| {
                if *nullable {
                    StructField::nullable(*name, DataType::LONG)
                } else {
                    StructField::not_null(*name, DataType::LONG)
                }
            },
        )))
    }

    #[test]
    fn output_lists_group_keys_then_aggregates_in_order() {
        let input = schema(&[("g", false), ("a", true), ("b", true)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .aggregate(Agg::max(column_name!("a")))
            .aggregate(Agg::min(column_name!("b")))
            .build()
            .unwrap();
        let names: Vec<&str> = agg.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["g", "a", "b"]);
    }

    #[test]
    fn group_key_passes_through_verbatim() {
        let input = schema(&[("g", false), ("a", true)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .aggregate(Agg::max(column_name!("a")))
            .build()
            .unwrap();
        let key = agg.schema.field("g").unwrap();
        // A non-nullable key column stays non-nullable.
        assert!(!key.nullable);
    }

    #[test]
    fn global_aggregate_output_is_nullable_even_for_nonnull_inputs() {
        let input = schema(&[("a", false), ("v", false)]);
        let agg = Aggregate::group_by(input, [])
            .aggregate(Agg::max_by(column_name!("a"), column_name!("v")))
            .build()
            .unwrap();
        // No group keys -> input multiset may be empty -> nullable, despite non-null inputs.
        assert!(agg.schema.field("a").unwrap().nullable);
    }

    #[rstest::rstest]
    #[case::nonnull_value(false, false)]
    #[case::nullable_value(true, true)]
    fn grouped_max_nullability_follows_value(#[case] value_nullable: bool, #[case] expected: bool) {
        let input = schema(&[("g", false), ("a", value_nullable)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .aggregate(Agg::max(column_name!("a")))
            .build()
            .unwrap();
        assert_eq!(agg.schema.field("a").unwrap().nullable, expected);
    }

    #[rstest::rstest]
    #[case::both_nonnull(false, false, false)]
    #[case::value_nullable(true, false, true)]
    #[case::key_nullable(false, true, true)]
    fn grouped_max_by_nullable_iff_value_or_key_nullable(
        #[case] value_nullable: bool,
        #[case] key_nullable: bool,
        #[case] expected: bool,
    ) {
        let input = schema(&[("g", false), ("a", value_nullable), ("v", key_nullable)]);
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .aggregate(Agg::max_by(column_name!("a"), column_name!("v")))
            .build()
            .unwrap();
        assert_eq!(agg.schema.field("a").unwrap().nullable, expected);
    }

    #[test]
    fn filter_forces_nullable_in_grouped_aggregate() {
        let input = schema(&[("g", false), ("a", false)]);
        let filter = Predicate::is_not_null(Expression::column(column_name!("a")));
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .aggregate(Agg::max(column_name!("a")).filter(filter))
            .build()
            .unwrap();
        // A filter can remove every row of a group, so the output is nullable.
        assert!(agg.schema.field("a").unwrap().nullable);
    }

    #[test]
    fn alias_overrides_default_output_name() {
        let input = schema(&[("a", true)]);
        let agg = Aggregate::group_by(input, [])
            .aggregate(Agg::max(column_name!("a")).alias("a_max"))
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
            .aggregate(Agg::min(column_name!("a")))
            .aggregate(Agg::max(column_name!("a")))
            .build();
        assert_result_error_with_message(result, "Duplicate field name");
    }

    #[test]
    fn distinct_aliases_resolve_min_max_collision() {
        let input = schema(&[("a", true)]);
        let agg = Aggregate::group_by(input, [])
            .aggregate(Agg::min(column_name!("a")).alias("a_min"))
            .aggregate(Agg::max(column_name!("a")).alias("a_max"))
            .build()
            .unwrap();
        let names: Vec<&str> = agg.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["a_min", "a_max"]);
    }

    #[test]
    fn missing_value_column_is_rejected() {
        let input = schema(&[("a", true)]);
        let result = Aggregate::group_by(input, [])
            .aggregate(Agg::max(column_name!("missing")))
            .build();
        assert_result_error_with_message(result, "missing");
    }

    #[test]
    fn missing_group_key_is_rejected() {
        let input = schema(&[("a", true)]);
        let result = Aggregate::group_by(input, [column_name!("missing")])
            .aggregate(Agg::max(column_name!("a")))
            .build();
        assert_result_error_with_message(result, "missing");
    }
}
