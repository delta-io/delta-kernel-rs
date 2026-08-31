//! Plan node operator kinds and their payloads.
//!
//! [`Operator`] enumerates every operator. Each operator's payload struct is defined
//! below.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use strum::Display;
use url::Url;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::error::add_scalar_path_context;
use crate::expressions::{ColumnName, ExpressionRef, PredicateRef, Scalar, StructData};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::utils::CollectInto;
use crate::{DeltaResult, Error, FileMeta};

// ============================================================================
// Operator: enumerates every operator kind
// ============================================================================

/// Plan node operators, grouped below by input arity. Each variant wraps a payload struct
/// documenting that operator's semantics, invariants, and output shape.
///
/// An operator that reshapes its rows (a source, projection, aggregation, or file scan) carries a
/// caller-declared `schema` field holding its output schema. The rest emit rows they were given, so
/// they inherit an input's schema; each payload's docs name which input.
#[derive(Debug, Clone, Display)]
#[strum(serialize_all = "snake_case")]
pub enum Operator {
    // === Source operators (0 inputs) =========================================
    ScanParquet(ScanParquet),
    ScanJson(ScanJson),
    Values(Values),

    // === Unary operators (1 input) ===========================================
    Project(Project),
    Filter(Filter),
    DynamicScan(DynamicScan),
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
    DynamicScan,
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
/// (such as row index). [`DynamicScan::file_constant_columns`] is the same concept for the
/// [`DynamicScan`] node.
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

/// Collect rows of `T` into a [`Values`] node.
///
/// Schema comes from [`ToSchema`]. Each row is converted via [`Into<StructData>`] and peeled into
/// top-level field scalars (nested fields remain [`Scalar::Struct`]).
impl<T: Into<StructData> + ToSchema> FromIterator<T> for Values {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let rows = iter.into_iter().map(|row| row.into().into_parts().1);
        Self::new(Arc::new(T::to_schema()), rows.collect())
    }
}

/// Inverse of [`FromIterator<T> for Values`]: rebuild each row as [`StructData`] and convert via
/// [`TryFrom`].
impl<T> TryFrom<Values> for Vec<T>
where
    T: TryFrom<StructData, Error = Error> + ToSchema,
{
    type Error = Error;

    fn try_from(Values { schema, rows }: Values) -> DeltaResult<Self> {
        rows.into_iter()
            .enumerate()
            .map(|(index, row)| {
                let schema = schema.as_ref().clone();
                T::try_from(StructData::from_values_unchecked(schema, row))
                    .map_err(|error| add_scalar_path_context(error, format!("[{index}]")))
            })
            .try_collect()
    }
}

/// Projects the input through `expr` into rows of `schema`.
///
/// `expr` must be a struct constructor or struct patch whose fields match `schema`. It is
/// evaluated with `schema` as its output struct type: the struct's fields are the output
/// columns, `schema` supplies names and nullability, and any type or arity mismatch is an error.
/// Standard child expressions first resolve their own result types according to the [SQL
/// expression contract](crate::expressions::semantics). The declared field type is then an
/// assignment target; it does not change operator inference. Downstream nodes see the logical
/// field names declared in `schema`.
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
///         col!("id"),
///         Expression::array([col!("first"), col!("last")]),
///         Expression::struct_from([
///             col!("add.path"),
///             col!("add.size"),
///             col!("add.stats_parsed.numRecords"),
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

/// File formats supported by [`DynamicScan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Reads data files from an upstream stream of file-metadata tuples, one input row per file.
/// For each row, the path, size, and last-modified columns describe the file; the engine resolves
/// its path against `base_url` (see below), opens it as `file_type`, and reads columns matching
/// `schema`.
///
/// `file_constant_columns` lists upstream columns whose per-file values are broadcast onto
/// every emitted file row. This is file-constant metadata, the same concept as
/// [`ScanParquet::file_constant_columns`]. Each named input field must have the same type and
/// nullability as its output field. See the example below.
///
/// `dv_column` names a nullable column on the upstream row holding a Delta
/// [`DeletionVectorDescriptor`] struct. The engine resolves it into a roaring bitmap
/// and drops file rows whose row index appears in the DV. A NULL value for a given
/// input row means "no DV for this file", so all file rows are emitted.
///
/// [`DeletionVectorDescriptor`]: crate::actions::deletion_vector::DeletionVectorDescriptor
///
/// Each path value is resolved against `base_url` via [`Url::join`]. URL-reference resolution need
/// not stay under `base_url`: a different-scheme absolute URL replaces the base, while a value
/// starting with `/` or `//` replaces its path or authority.
///
/// Output row order is unspecified: the engine is free to read files in any order, in
/// parallel, and to interleave rows from different files. The relative order of upstream
/// rows is not preserved.
///
/// # Example
///
/// Given an upstream metadata stream and a `DynamicScan` configuration:
///
/// ```text
/// upstream (metadata)
///     path             | size | filemod | version | dv
///     -----------------+------+---------+---------+------
///     part-0.parquet   | 1024 |  100000 |       7 | NULL
///     part-1.parquet   | 2048 |  200000 |       8 | NULL
/// ```
/// ```text
/// DynamicScan {
///     schema: { id: int, name: string, version: long },
///     file_type: Parquet,
///     base_url: "s3://table/",
///     file_constant_columns: ["version"],
///     path_column: "path",
///     file_size_column: "size",
///     last_modified_column: "filemod",
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
pub struct DynamicScan {
    pub schema: SchemaRef,
    pub file_type: FileType,
    /// Hierarchical base URL ending in `/` against which per-row path values resolve.
    pub base_url: Url,
    pub file_constant_columns: Vec<String>,
    /// Non-nullable input column holding the per-row file path or URL fragment.
    pub path_column: ColumnName,
    /// Non-nullable input column with the file's total size in bytes.
    pub file_size_column: ColumnName,
    /// Non-nullable input column with the last-modified timestamp in milliseconds since epoch.
    pub last_modified_column: ColumnName,
    /// Nullable input column with the schema of [`DeletionVectorDescriptor`].
    pub dv_column: ColumnName,
}

impl DynamicScan {
    /// Constructs a [`DynamicScan`] whose emitted rows match `output_schema`.
    ///
    /// `input_schema` describes the upstream rows containing file metadata. The scan reads
    /// `file_type` files relative to `base_url`.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is not hierarchical or does not end in `/`; when a required
    /// metadata or deletion-vector column is absent from `input_schema`, has an incompatible type,
    /// or has invalid nullability; or when a file-constant column is absent from either schema, is
    /// a metadata column, or has different input and output types or nullability.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input_schema: &SchemaRef,
        output_schema: impl Into<SchemaRef>,
        file_type: FileType,
        base_url: Url,
        file_constant_columns: impl IntoIterator<Item = impl Into<String>>,
        path_column: ColumnName,
        file_size_column: ColumnName,
        last_modified_column: ColumnName,
        dv_column: ColumnName,
    ) -> DeltaResult<Self> {
        let schema = output_schema.into();
        let file_constant_columns = file_constant_columns
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let dynamic_scan = Self {
            schema,
            file_type,
            base_url,
            file_constant_columns,
            path_column,
            file_size_column,
            last_modified_column,
            dv_column,
        };
        dynamic_scan.validate_input(input_schema)?;
        Ok(dynamic_scan)
    }

    /// Validates the columns consumed by this scan against an upstream `input_schema`.
    ///
    /// Returns `Ok(())` when the base URL is valid and every configured column resolves with the
    /// required type and nullability.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is not hierarchical or does not end in `/`; when a required
    /// metadata or deletion-vector column is absent, has an incompatible type, or has invalid
    /// nullability; or when a file-constant column is absent from either schema, is a metadata
    /// column, or has different input and output types or nullability.
    pub fn validate_input(&self, input_schema: &SchemaRef) -> DeltaResult<()> {
        static DELETION_VECTOR_DATA_TYPE: LazyLock<DataType> =
            LazyLock::new(|| DataType::from(DeletionVectorDescriptor::to_schema()));

        if self.base_url.cannot_be_a_base() || !self.base_url.path().ends_with('/') {
            return Err(Error::generic(format!(
                "dynamic scan: base URL `{}` must be hierarchical and end in `/`",
                self.base_url
            )));
        }

        Self::validate_required_column(input_schema, &self.path_column, &DataType::STRING)?;
        Self::validate_required_column(input_schema, &self.file_size_column, &DataType::LONG)?;
        Self::validate_required_column(input_schema, &self.last_modified_column, &DataType::LONG)?;
        Self::validate_file_constant_columns(
            input_schema,
            &self.schema,
            &self.file_constant_columns,
        )?;

        let fields = input_schema
            .fields_of_path(&self.dv_column)
            .map_err(|err| {
                Error::generic(format!(
                    "dynamic scan: deletion-vector column `{}` is invalid: {err}",
                    self.dv_column
                ))
            })?;
        let Some((field, _ancestors)) = fields.split_last() else {
            return Err(Error::internal_error("fields_of_path returned no fields"));
        };
        let expected = &*DELETION_VECTOR_DATA_TYPE;
        if field.data_type() != expected {
            return Err(Error::generic(format!(
                "dynamic scan: deletion-vector column `{}` must have type {expected}, found {}",
                self.dv_column,
                field.data_type()
            )));
        }
        if !field.is_nullable() {
            return Err(Error::generic(format!(
                "dynamic scan: deletion-vector column `{}` must be nullable",
                self.dv_column
            )));
        }

        Ok(())
    }

    fn validate_required_column(
        schema: &SchemaRef,
        column: &ColumnName,
        expected_type: &DataType,
    ) -> DeltaResult<()> {
        let fields = schema.fields_of_path(column)?;
        let Some((field, ancestors)) = fields.split_last() else {
            return Err(Error::internal_error("fields_of_path returned no fields"));
        };
        if field.data_type() != expected_type {
            return Err(Error::generic(format!(
                "dynamic scan: column `{column}` must have type {expected_type}, found {}",
                field.data_type()
            )));
        }
        if field.is_nullable() || ancestors.iter().any(|field| field.is_nullable()) {
            return Err(Error::generic(format!(
                "dynamic scan: required column `{column}` is nullable"
            )));
        }
        Ok(())
    }

    fn validate_file_constant_columns(
        input_schema: &SchemaRef,
        output_schema: &SchemaRef,
        file_constant_columns: &[String],
    ) -> DeltaResult<()> {
        for name in file_constant_columns {
            let Some(input_field) = input_schema.field(name) else {
                return Err(Error::generic(format!(
                    "dynamic scan file_constant source: column `{name}` not found; schema has \
                     {:?}",
                    Vec::from_iter(input_schema.fields().map(|field| field.name())),
                )));
            };
            if input_field.is_metadata_column() {
                return Err(Error::generic(format!(
                    "dynamic scan file_constant source: column `{name}` is a metadata column"
                )));
            }
            let Some(output_field) = output_schema.field(name) else {
                return Err(Error::generic(format!(
                    "dynamic scan file_constant: column `{name}` not found; schema has {:?}",
                    Vec::from_iter(output_schema.fields().map(|field| field.name())),
                )));
            };
            if output_field.is_metadata_column() {
                return Err(Error::generic(format!(
                    "dynamic scan file_constant: column `{name}` is a metadata column"
                )));
            }
            if input_field.data_type() != output_field.data_type()
                || input_field.is_nullable() != output_field.is_nullable()
            {
                return Err(Error::generic(format!(
                    "dynamic scan file_constant: column `{name}` must have the same type and \
                     nullability in input and output"
                )));
            }
        }
        Ok(())
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
///   docs); use [`AggregateBuilder::aggregate_as`] to override the name. Aggregates that preserve
///   the input type also preserve its field metadata; fixed-LONG aggregates emit a bare field.
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
/// Each person's best and worst score across their bowling games:
///
/// ```text
/// Aggregate {
///     group_by: [name],
///     aggs: [max(score) AS high, min(score) AS low],
///     schema: { name: string, high: long, low: long },
/// }
/// ```
///
/// Input:
///
/// ```text
/// name    | score
/// --------+------
///  Alice  | 140
///  Bob    | 200
///  Alice  | 180
///  Bob    | 160
///  Alice  | 155
///  Charlie| 175
/// ```
///
/// Output:
///
/// ```text
/// name    | high | low
/// --------+------+-----
///  Bob    |  200 | 160
///  Charlie|  175 | 175
///  Alice  |  180 | 140
/// ```
///
/// An ungrouped aggregate (`group_by` empty) always emits one row. Over empty input that row holds
/// each agg's initial value (i.e. NULL for [`Agg::min`] and `0` for [`Agg::count`]). See individual
/// [`Agg`] docs for per-function initial values and NULL-handling semantics.
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
    /// Starts building an ungrouped [`Aggregate`] over `input_schema`. Add aggregators directly
    /// with [`aggregate`](AggregateBuilder::aggregate) or using named helpers
    /// (e.g. [`max`](AggregateBuilder::max)), and finalize the aggregate by calling
    /// [`build`](AggregateBuilder::build). The output schema follows aggregator insertion order.
    pub fn ungrouped(input_schema: SchemaRef) -> AggregateBuilder {
        Self::group_by(input_schema, std::iter::empty::<ColumnName>())
    }

    /// Starts building an [`Aggregate`] over `input_schema`, grouped by `grouping_keys`. Add
    /// aggregators directly with [`aggregate`](AggregateBuilder::aggregate) or using named helpers
    /// (e.g. [`max`](AggregateBuilder::max)), and finalize the aggregate by calling
    /// [`build`](AggregateBuilder::build). Grouping keys are emitted first in the output schema,
    /// followed by aggregators in insertion order.
    pub fn group_by(
        input_schema: SchemaRef,
        grouping_keys: impl CollectInto<Vec<ColumnName>>,
    ) -> AggregateBuilder {
        AggregateBuilder {
            input_schema,
            group_by: grouping_keys.collect_into(),
            aggs: Vec::new(),
        }
    }
}

/// An aggregate function and its operand column(s) within an [`Aggregate`] operator.
#[derive(Debug, Clone)]
pub enum Agg {
    /// Operand for [`Agg::min`].
    Min(ColumnName),
    /// Operand for [`Agg::max`].
    Max(ColumnName),
    /// Operand for [`Agg::sum`].
    Sum(ColumnName),
    /// Operand for [`Agg::count`].
    Count(ColumnName),
    /// [`Agg::count_star`] has no operands.
    CountStar,
    /// Operands for [`Agg::min_non_null_by`].
    MinNonNullBy(NonNullByOperands),
    /// Operands for [`Agg::max_non_null_by`].
    MaxNonNullBy(NonNullByOperands),
}

/// Operands for [`Agg::min_non_null_by`] and [`Agg::max_non_null_by`].
#[derive(Debug, Clone)]
pub struct NonNullByOperands {
    pub value: ColumnName,
    pub null_sentinel: ColumnName,
    pub key: ColumnName,
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
        Self::Min(value.into())
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
        Self::Max(value.into())
    }

    /// The sum of non-NULL LONG values in each group, or NULL if the group has no non-NULL value.
    /// The output is always a nullable LONG, with default name matching `value`.
    ///
    /// ```text
    /// [3, NULL, 5, 1] -> 9
    /// [NULL, NULL]    -> NULL
    /// []              -> NULL
    /// ```
    pub fn sum(value: impl Into<ColumnName>) -> Self {
        Self::Sum(value.into())
    }

    /// The number of non-NULL values in `value` for each group. The output is always a non-nullable
    /// LONG, with default name matching `value`.
    ///
    /// ```text
    /// [3, NULL, 5, 1] -> 3
    /// [NULL, NULL]    -> 0
    /// []              -> 0
    /// ```
    pub fn count(value: impl Into<ColumnName>) -> Self {
        Self::Count(value.into())
    }

    /// The number of input rows in each group (`COUNT(*)`). The output is always a non-nullable
    /// LONG named `count` by default.
    ///
    /// ```text
    /// [3, NULL, 5, 1] -> 4
    /// [NULL, NULL]    -> 2
    /// []              -> 0
    /// ```
    pub fn count_star() -> Self {
        Self::CountStar
    }

    /// Like [`max_non_null_by`](Self::max_non_null_by), but selects the `value` from the qualifying
    /// row with the *least* `key`.
    pub fn min_non_null_by(
        value: impl Into<ColumnName>,
        null_sentinel: impl Into<ColumnName>,
        key: impl Into<ColumnName>,
    ) -> Self {
        Self::MinNonNullBy(NonNullByOperands {
            value: value.into(),
            null_sentinel: null_sentinel.into(),
            key: key.into(),
        })
    }

    /// The `value` from a row with the greatest `key` where `null_sentinel` and `key` are both
    /// non-NULL. Returns NULL if no qualifying row exists. A winning `value` may itself be NULL. It
    /// is unspecified which of multiple rows with greatest `key` provides the winning `value`. The
    /// output is always nullable, with name and type matching `value`.
    ///
    /// ```text
    ///  key | sentinel | value  ->  NULL
    /// -----+----------+------
    ///    1 | present  | a
    ///    3 | present  | c
    ///    5 | present  | NULL       (greatest qualifying key; NULL value is retained)
    ///    7 | NULL     | d          (ignored: NULL sentinel)
    /// NULL | present  | e          (ignored: NULL key)
    ///
    /// (no rows)        ->  NULL
    /// ```
    ///
    /// Most systems with a native `max_by` only provide a two-arg form that considers all rows with
    /// non-NULL keys. The sentinel check can be added manually in one of two ways:
    ///
    /// ```sql
    /// -- FILTER that drops NULL-sentinel rows before aggregating
    /// max_by(value, key) FILTER (WHERE sentinel IS NOT NULL)
    ///
    /// -- NULL out the key when sentinel is NULL, which max_by then ignores. Use this where
    /// -- FILTER is unavailable, such as a DataFrame API with no filtered-aggregate form.
    /// max_by(value, CASE WHEN sentinel IS NOT NULL THEN key END)
    /// ```
    ///
    /// In systems without `max_by`, it can also be expressed using window functions, with the
    /// caveat that window functions don't work correctly for ungrouped aggs over empty input
    /// (produces no rows when it should produce one row containing initial agg values):
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
    ///     WHERE key IS NOT NULL AND null_sentinel IS NOT NULL
    /// ) WHERE rn = 1
    /// ```
    pub fn max_non_null_by(
        value: impl Into<ColumnName>,
        null_sentinel: impl Into<ColumnName>,
        key: impl Into<ColumnName>,
    ) -> Self {
        Self::MaxNonNullBy(NonNullByOperands {
            value: value.into(),
            null_sentinel: null_sentinel.into(),
            key: key.into(),
        })
    }

    /// Derives this aggregate's output [`StructField`] over `input_schema`, validating that every
    /// operand column resolves.
    fn output_field(
        &self,
        input_schema: &StructType,
        alias: Option<String>,
    ) -> DeltaResult<StructField> {
        // `output_data_type: None` preserves the input field's type and metadata; `Some` overrides
        // the type and strips metadata (new column).
        let resolve = |value: &ColumnName, output_data_type: Option<DataType>, nullable: bool| {
            let field = input_schema.field_at(value)?;
            let (data_type, metadata) = match output_data_type {
                Some(data_type) => (data_type, HashMap::new()),
                None => (field.data_type.clone(), field.metadata.clone()),
            };
            Ok(StructField {
                // Without clone, we capture `alias` by value and `CountStar` arm can't use it
                name: alias.clone().unwrap_or_else(|| field.name.clone()),
                data_type,
                metadata,
                nullable,
            })
        };
        match self {
            Agg::Min(value) | Agg::Max(value) => resolve(value, None, true),
            Agg::Sum(value) => resolve(value, Some(DataType::LONG), true),
            Agg::Count(value) => resolve(value, Some(DataType::LONG), false),
            Agg::CountStar => Ok(StructField::not_null(
                alias.unwrap_or_else(|| "count".to_string()),
                DataType::LONG,
            )),
            Agg::MinNonNullBy(operands) | Agg::MaxNonNullBy(operands) => {
                let _ = input_schema.field_at(&operands.key)?;
                let _ = input_schema.field_at(&operands.null_sentinel)?;
                resolve(&operands.value, None, true)
            }
        }
    }
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

    /// Adds an unaliased [`Agg::sum`] over `value`.
    pub fn sum(self, value: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::sum(value))
    }

    /// Adds an unaliased [`Agg::count`] over `value`.
    pub fn count(self, value: impl Into<ColumnName>) -> Self {
        self.aggregate(Agg::count(value))
    }

    /// Adds an unaliased [`Agg::count_star`].
    pub fn count_star(self) -> Self {
        self.aggregate(Agg::count_star())
    }

    /// Adds an unaliased [`Agg::min_non_null_by`] over `value`, qualifying rows with
    /// `null_sentinel` and keyed on `key`.
    pub fn min_non_null_by(
        self,
        value: impl Into<ColumnName>,
        null_sentinel: impl Into<ColumnName>,
        key: impl Into<ColumnName>,
    ) -> Self {
        self.aggregate(Agg::min_non_null_by(value, null_sentinel, key))
    }

    /// Adds an unaliased [`Agg::max_non_null_by`] over `value`, qualifying rows with
    /// `null_sentinel` and keyed on `key`.
    pub fn max_non_null_by(
        self,
        value: impl Into<ColumnName>,
        null_sentinel: impl Into<ColumnName>,
        key: impl Into<ColumnName>,
    ) -> Self {
        self.aggregate(Agg::max_non_null_by(value, null_sentinel, key))
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
    use delta_kernel_derive::{IntoStructData, ToSchema, TryFromStructData};

    use super::*;
    use crate::expressions::column_name;
    use crate::schema::{schema_ref, DataType, MetadataValue, StructField};
    use crate::unit_test_utils::assert_result_error_with_message;

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

    /// Group keys and type-preserving aggregates keep input field metadata; only nullability
    /// changes. Fixed-LONG aggregates build a fresh field and so carry no metadata.
    #[test]
    fn output_fields_preserve_input_field_metadata() {
        let metadata = [("k", MetadataValue::Number(7))];
        let input = schema_ref! {
            (StructField::not_null("g", DataType::LONG).with_metadata(metadata.clone())),
            (StructField::not_null("a", DataType::LONG).with_metadata(metadata.clone())),
            (StructField::not_null("s", DataType::LONG).with_metadata(metadata)),
        };
        let agg = Aggregate::group_by(input, [column_name!("g")])
            .max(column_name!("a"))
            .sum(column_name!("s"))
            .build()
            .unwrap();

        let key = agg.schema.field("g").unwrap();
        assert!(!key.nullable);
        assert_eq!(key.metadata()["k"], MetadataValue::Number(7));
        let max = agg.schema.field("a").unwrap();
        assert!(max.nullable);
        assert_eq!(max.metadata()["k"], MetadataValue::Number(7));
        assert!(agg.schema.field("s").unwrap().metadata().is_empty());
    }

    /// Output nullability is fixed by the aggregate kind, independent of input nullability.
    #[rstest::rstest]
    #[case::min(Agg::min(column_name!("a")), "a", true)]
    #[case::max(Agg::max(column_name!("a")), "a", true)]
    #[case::sum(Agg::sum(column_name!("a")), "a", true)]
    #[case::count(Agg::count(column_name!("a")), "a", false)]
    #[case::count_star(Agg::count_star(), "count", false)]
    #[case::min_non_null_by(
        Agg::min_non_null_by(column_name!("a"), column_name!("s"), column_name!("v")),
        "a",
        true
    )]
    #[case::max_non_null_by(
        Agg::max_non_null_by(column_name!("a"), column_name!("s"), column_name!("v")),
        "a",
        true
    )]
    fn agg_output_nullability(
        #[case] agg: Agg,
        #[case] name: &str,
        #[case] nullable: bool,
        #[values(true, false)] value_nullable: bool,
    ) {
        let input = schema(&[("a", value_nullable), ("s", true), ("v", true)]);
        let built = Aggregate::ungrouped(input).aggregate(agg).build().unwrap();
        let field = built.schema.field(name).unwrap();
        assert_eq!(field.nullable, nullable);
        assert_eq!(field.data_type(), &DataType::LONG);
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
            .max_non_null_by(
                column_name!("a"),
                column_name!("a"),
                column_name!("missing"),
            )
            .build();
        assert_result_error_with_message(result, "missing");
    }

    #[test]
    fn build_rejects_missing_non_null_by_sentinel_column() {
        let input = schema(&[("a", true), ("v", true)]);
        let result = Aggregate::group_by(input, [])
            .max_non_null_by(
                column_name!("a"),
                column_name!("missing"),
                column_name!("v"),
            )
            .build();
        assert_result_error_with_message(result, "missing");
    }

    #[derive(Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData)]
    struct Address {
        city: String,
    }

    #[derive(Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData)]
    struct Person {
        id: i32,
        address: Address,
    }

    #[test]
    fn values_from_iter_peels_top_level_and_keeps_nested_struct() {
        let values = Values::from_iter([Person {
            id: 1,
            address: Address { city: "NYC".into() },
        }]);

        assert_eq!(
            values
                .schema
                .fields()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "address"]
        );
        assert_eq!(values.rows.len(), 1);
        assert_eq!(values.rows[0].len(), 2);
        assert_eq!(values.rows[0][0], Scalar::Integer(1));
        let Scalar::Struct(address) = &values.rows[0][1] else {
            panic!("expected nested Struct for address");
        };
        assert_eq!(address.values(), &[Scalar::String("NYC".into())]);
    }

    #[test]
    fn values_from_iter_empty_still_carries_schema() {
        let values: Values = std::iter::empty::<Person>().collect();
        assert!(values.rows.is_empty());
        assert_eq!(values.schema.num_fields(), 2);
    }

    #[test]
    fn values_round_trips_through_vec() {
        let people = vec![
            Person {
                id: 1,
                address: Address { city: "NYC".into() },
            },
            Person {
                id: 2,
                address: Address { city: "SF".into() },
            },
        ];
        let values = Values::from_iter(people.clone());
        assert_eq!(Vec::<Person>::try_from(values).unwrap(), people);
    }

    #[test]
    fn values_conversion_adds_row_index_to_error_path() {
        let mut values = Values::from_iter([Person {
            id: 1,
            address: Address { city: "NYC".into() },
        }]);
        values.rows[0][0] = Scalar::from("not an integer");
        assert_result_error_with_message(
            Vec::<Person>::try_from(values),
            "[0].id: expected i32, found string",
        );
    }
}
