//! Stats manipulation utilities for Adaptive Metadata Tree (AMT).
//!
//! The AMT stores statistics as shredded columns within parquet files. Stats are stored
//! in a column-major format (each leaf column has an associated struct with fields representing
//! min, max, etc).
//!
//! The layout is flat: each primitive/variant *leaf* of the table schema -- however deeply nested
//! inside structs -- contributes one stats struct stored as a direct child of `content_stats`,
//! keyed by the leaf's base stats field ID: data leaves at `10_000 + 200 * field_id`, and the
//! supported reserved-metadata leaves (`_row_id`, `_last_updated_sequence_number`) into
//! `[9_000, 10_000)` (see [`field_id_to_statistics_base`]).
//!
//! As with all fields in Iceberg, statistics are projected by field ID.

use std::sync::Arc;

use tracing::warn;

use crate::actions::{
    MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS as DELTA_TIGHT_BOUNDS,
};
use crate::content_tree::{
    AVG_VALUE_SIZE_IN_BYTES, LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, TIGHT_BOUNDS,
    UPPER_BOUND, VALUE_COUNT,
};
use crate::expressions::{
    lit, null_lit, ColumnName, Expression, ExpressionRef, ExpressionStructPatchBuilder,
    VariadicExpressionOp,
};
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Engine, EngineData, Error};

/// Field ID offsets for stats fields within a column's stats struct.
const STATS_OFFSET_LOWER_BOUND: i32 = 1;
const STATS_OFFSET_UPPER_BOUND: i32 = 2;
const STATS_OFFSET_TIGHT_BOUNDS: i32 = 3;
const STATS_OFFSET_VALUE_COUNT: i32 = 4;
const STATS_OFFSET_NULL_VALUE_COUNT: i32 = 5;
const STATS_OFFSET_NAN_VALUE_COUNT: i32 = 6;
const STATS_OFFSET_AVG_VALUE_SIZE_IN_BYTES: i32 = 7;

/// Number of supported stats per column (each column gets a range of 200 field IDs).
/// This value is the upper bound on the number of "statistic types", e.g. min/max.
/// Each subfield is a constant offset from the top level stats structure.
const NUM_SUPPORTED_STATS_PER_COLUMN: i32 = 200;

/// Starting field ID of the stats space for data field IDs (regular column stats).
const STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS: i32 = 10_000;

/// Starting field ID of the stats space for metadata (reserved) field IDs.
/// Metadata stats occupy `[9_000, 10_000)`, just below the data stats space.
const STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS: i32 = 9_000;

/// Exclusive upper bound of the stats field ID range reserved for content_stats.
/// Valid stats field IDs are in `[STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS,
/// STATS_SPACE_FIELD_ID_END)`.
const STATS_SPACE_FIELD_ID_END: i32 = 200_000_000;

/// The maximum stats field ID for data columns (the base-id for the last data field that fits).
const MAX_DATA_STATS_FIELD_ID: i32 = STATS_SPACE_FIELD_ID_END - NUM_SUPPORTED_STATS_PER_COLUMN;

/// The maximum data field ID whose stats struct fits within the reserved range.
const MAX_DATA_FIELD_ID: i32 = (MAX_DATA_STATS_FIELD_ID
    - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
    / NUM_SUPPORTED_STATS_PER_COLUMN;

// Duplicated as `i32` (`crate::reserved_field_ids` declares `FILE_NAME` from the same Iceberg
// reserved space as `i64`) to keep the stats field-id arithmetic in `i32`.

/// Iceberg reserved field ID for `_last_updated_sequence_number` (`Integer.MAX_VALUE - 108`).
const LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID: i32 = 2_147_483_539;

/// Iceberg reserved field ID for `_row_id` (`Integer.MAX_VALUE - 107`).
const ROW_ID_FIELD_ID: i32 = 2_147_483_540;

/// The set of reserved metadata field IDs that have stats tracked in `content_stats`.
/// Per the spec, only `_last_updated_sequence_number` and `_row_id` are supported.
const SUPPORTED_METADATA_FIELD_IDS: [i32; 2] =
    [LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, ROW_ID_FIELD_ID];

/// The smallest field ID in [`SUPPORTED_METADATA_FIELD_IDS`]. Metadata stats offsets are
/// computed relative to this value.
const FIRST_SUPPORTED_METADATA_FIELD_ID: i32 = SUPPORTED_METADATA_FIELD_IDS[0];

/// A contiguous region of the stats field ID space with a fixed [`NUM_SUPPORTED_STATS_PER_COLUMN`]
/// stride, mapping field IDs to their stats base.
///
/// A field ID `f` maps to base `start + 200 * (f - field_base)`; the data space uses
/// `field_base == 0` so its base is simply `start + 200 * f`.
struct StatsSpace {
    start: i32,
    field_base: i32,
}

impl StatsSpace {
    /// The base stats field ID for `field_id` within this space.
    const fn base(&self, field_id: i32) -> i32 {
        self.start + NUM_SUPPORTED_STATS_PER_COLUMN * (field_id - self.field_base)
    }
}

const METADATA_SPACE: StatsSpace = StatsSpace {
    start: STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS,
    field_base: FIRST_SUPPORTED_METADATA_FIELD_ID,
};

const DATA_SPACE: StatsSpace = StatsSpace {
    start: STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS,
    field_base: 0,
};

/// Computes the base field ID for a leaf column's stats struct from that leaf's own field ID.
///
/// Stats field IDs occupy the range `[9_000, 200_000_000)`:
/// - Metadata fields in [`SUPPORTED_METADATA_FIELD_IDS`] map into `[9_000, 10_000)`.
/// - Data fields `[0, MAX_DATA_FIELD_ID]` map into `[10_000, 200_000_000)`.
///
/// Returns `None` for negative field IDs, unsupported metadata field IDs, or data field IDs
/// whose stats would fall outside the reserved range.
pub(crate) fn field_id_to_statistics_base(field_id: i32) -> Option<i32> {
    if SUPPORTED_METADATA_FIELD_IDS.contains(&field_id) {
        Some(METADATA_SPACE.base(field_id))
    } else if (0..=MAX_DATA_FIELD_ID).contains(&field_id) {
        Some(DATA_SPACE.base(field_id))
    } else {
        None
    }
}

/// Creates a physical-schema [`StructField`] carrying the Iceberg/Parquet field ID.
///
/// The AMT `content_stats` schema is a physical schema projected by field ID, so it needs only
/// [`ColumnMetadataKey::ParquetFieldId`] -- the same annotation the `#[field_id = N]` derive macro
/// attaches to the other `content_tree` structs. It carries no `delta.columnMapping.*` logical
/// annotations, which are consumed by (not produced from) logical->physical conversion.
fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> StructField {
    StructField::new(name, data_type, nullable).with_metadata([(
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        MetadataValue::Number(field_id as i64),
    )])
}

/// Extracts the parquet field ID from a [`StructField`]'s metadata, or `None` if absent.
fn get_field_id(field: &StructField) -> Option<i32> {
    match field.get_config_value(&ColumnMetadataKey::ParquetFieldId) {
        Some(MetadataValue::Number(id)) => (*id).try_into().ok(),
        _ => None,
    }
}

/// Which Delta JSON stat categories a leaf appears in. Selects which sub-fields
/// [`build_stats_struct`] emits under the projection (see there).
#[derive(Clone, Copy)]
struct StatCategories {
    /// Present in `nullCount` -- backs `value_count`/`null_value_count`.
    null_count: bool,
    /// Present in `minValues` -- backs `lower_bound`.
    min_values: bool,
    /// Present in `maxValues` -- backs `upper_bound`.
    max_values: bool,
}

impl StatCategories {
    /// All three categories present: the unprojected case, keeping every type-eligible sub-field.
    const ALL: StatCategories = StatCategories {
        null_count: true,
        min_values: true,
        max_values: true,
    };

    /// Whether the leaf appears in at least one category (i.e. survives the projection).
    fn any(&self) -> bool {
        self.null_count || self.min_values || self.max_values
    }
}

/// Builds a single column's stats struct, with each stat sub-field's ID an offset from
/// `base_field_id`:
/// - offset 1/2: `lower_bound` / `upper_bound` (typed as `bounds_type`)
/// - offset 3: `tight_bounds` (boolean) - excluded for variants
/// - offset 4: `value_count` (long)
/// - offset 5: `null_value_count` (long) - emitted regardless of the column's nullability
/// - offset 6: `nan_value_count` (long) - only for float/double `bounds_type`
/// - offset 7: `avg_value_size_in_bytes` (int) - for string/binary `bounds_type`, or any variant
///
/// `bounds_type` is the type the bounds are recorded at: the column's own type for primitives, or
/// an unshredded variant type for variant columns.
///
/// `categories` restricts the sub-fields to the Delta stat categories the leaf appears in. `None`
/// (the unprojected path) keeps every type-eligible sub-field. When `Some`, a sub-field is kept
/// only when a category backing it is present: `lower_bound`<-`minValues`,
/// `upper_bound`<-`maxValues`, `value_count`/`null_value_count`<-`nullCount`,
/// `tight_bounds`/`nan_value_count`<-either bound category. `avg_value_size_in_bytes` has no
/// backing category and is dropped whenever projecting -- so `null_value_count`, though independent
/// of nullability, is still dropped when the leaf is absent from `nullCount`.
fn build_stats_struct(
    base_field_id: i32,
    bounds_type: &DataType,
    categories: Option<StatCategories>,
) -> StructType {
    let is_variant = matches!(bounds_type, DataType::Variant(_));
    let (has_nan_count, has_size_stats) = match bounds_type {
        DataType::Primitive(ptype) => (
            matches!(ptype, PrimitiveType::Float | PrimitiveType::Double),
            matches!(ptype, PrimitiveType::String | PrimitiveType::Binary),
        ),
        DataType::Variant(_) => (false, true),
        _ => (false, false),
    };

    // With no projection (`None`), `StatCategories::ALL` makes every category check below `true`,
    // keeping every type-eligible sub-field.
    let projecting = categories.is_some();
    let StatCategories {
        null_count: has_null,
        min_values: has_min,
        max_values: has_max,
    } = categories.unwrap_or(StatCategories::ALL);
    let has_bounds = has_min || has_max;

    // (name, type, offset, include) -- kept in declaration order to preserve field ordering.
    let specs = [
        (
            LOWER_BOUND,
            bounds_type.clone(),
            STATS_OFFSET_LOWER_BOUND,
            has_min,
        ),
        (
            UPPER_BOUND,
            bounds_type.clone(),
            STATS_OFFSET_UPPER_BOUND,
            has_max,
        ),
        (
            TIGHT_BOUNDS,
            DataType::BOOLEAN,
            STATS_OFFSET_TIGHT_BOUNDS,
            !is_variant && has_bounds,
        ),
        (
            VALUE_COUNT,
            DataType::LONG,
            STATS_OFFSET_VALUE_COUNT,
            has_null,
        ),
        (
            NULL_VALUE_COUNT,
            DataType::LONG,
            STATS_OFFSET_NULL_VALUE_COUNT,
            has_null,
        ),
        (
            NAN_VALUE_COUNT,
            DataType::LONG,
            STATS_OFFSET_NAN_VALUE_COUNT,
            has_nan_count && has_bounds,
        ),
        (
            AVG_VALUE_SIZE_IN_BYTES,
            DataType::INTEGER,
            STATS_OFFSET_AVG_VALUE_SIZE_IN_BYTES,
            has_size_stats && !projecting,
        ),
    ];
    let fields = specs.into_iter().filter_map(|(name, ty, offset, include)| {
        include.then(|| field_with_id(name, ty, true, base_field_id + offset))
    });

    StructType::new_unchecked(fields)
}

/// Builds the flat stats field for a non-struct leaf, or `None` when the leaf carries no stats: an
/// array/map column, or a field ID outside the supported stats range (skipped with a warning). The
/// returned field is named by the leaf's full dotted path (`path`, whose last segment is the leaf
/// itself) and keyed at the leaf's base stats field ID (see [`build_stats_struct`] for the
/// sub-fields).
///
/// Errors if the leaf is missing its field-id metadata, or is an (as-yet unimplemented) geospatial
/// column.
///
/// `categories` is forwarded to [`build_stats_struct`] to restrict the sub-fields (see there);
/// `None` emits the full stats struct.
fn leaf_stats_field(
    field: &StructField,
    path: &[String],
    categories: Option<StatCategories>,
) -> DeltaResult<Option<StructField>> {
    // Only leaves carry a field ID that matters for stats. A field ID that is absent, or present
    // but not `i32`-representable, is malformed => error. The spec limits which fields may carry
    // stats, so a field ID outside the supported range is expected for some reserved metadata
    // columns; skip (warn) rather than error in that case.
    let field_id = get_field_id(field).ok_or_else(|| {
        Error::generic(format!(
            "Field '{}' has no usable (present, i32-representable) field ID. metadata: {:#?}",
            field.name(),
            field.metadata()
        ))
    })?;

    // Geospatial stats generation is not implemented yet. Error (rather than silently dropping the
    // column) so this is not forgotten once geospatial support lands -- checked before the range
    // check below so an out-of-range geo field ID still errors. Reachable only with the
    // `geo-type-in-dev` feature enabled.
    // TODO: emit proper stats for geospatial columns.
    #[cfg(feature = "geo-type-in-dev")]
    if matches!(
        field.data_type(),
        DataType::Primitive(PrimitiveType::Geometry(_) | PrimitiveType::Geography(_))
    ) {
        return Err(Error::unsupported(format!(
            "AMT stats schema generation is not yet implemented for geospatial column '{}' (type {})",
            field.name(),
            field.data_type(),
        )));
    }

    let Some(base_stats_id) = field_id_to_statistics_base(field_id) else {
        warn!(
            "Skipping stats for field '{}' (field_id={field_id}): outside supported stats range",
            field.name(),
        );
        return Ok(None);
    };

    let stats_struct = match field.data_type() {
        DataType::Primitive(_) => build_stats_struct(base_stats_id, field.data_type(), categories),
        // A variant's inner fields carry no field IDs; the base stats ID covers the whole variant,
        // and its bounds are always recorded as unshredded variants regardless of physical
        // shredding.
        DataType::Variant(_) => {
            build_stats_struct(base_stats_id, &DataType::unshredded_variant(), categories)
        }
        // Array/map columns carry no leaf stats.
        _ => return Ok(None),
    };

    // Keyed by the base stats field ID, not the source column's field ID, and without its logical
    // (column-mapping) metadata.
    let name = ColumnName::new(path).to_string();
    Ok(Some(field_with_id(
        &name,
        stats_struct.into(),
        true,
        base_stats_id,
    )))
}

/// Classification of `schema.<field_name>` for a struct descent. In a Delta stats schema every
/// category node mirroring a table struct is itself a struct; a scalar where a struct is expected
/// is a shape mismatch.
enum SubSchema<'a> {
    /// The field is absent -- the category simply omits this subtree.
    Absent,
    /// The field is a struct: the category mirrors the table's nesting here.
    Struct(&'a StructType),
    /// The field is present but a scalar where a struct nesting is expected. The Delta stats
    /// schema is kernel-generated to mirror the table (see [`from_delta_stats_schema`]), so
    /// this can only arise from an internal invariant violation, never from legitimate input;
    /// callers surface it as an error.
    ///
    /// [`from_delta_stats_schema`]: CategoryScopes::from_delta_stats_schema
    Mismatch,
}

/// Looks up `schema.<field_name>` and classifies it (see [`SubSchema`]).
fn struct_sub_schema<'a>(schema: &'a StructType, field_name: &str) -> SubSchema<'a> {
    match schema.field(field_name).map(StructField::data_type) {
        None => SubSchema::Absent,
        Some(DataType::Struct(s)) => SubSchema::Struct(s.as_ref()),
        Some(_) => SubSchema::Mismatch,
    }
}

/// The three Delta JSON stat category sub-schemas (`nullCount`/`minValues`/`maxValues`) in scope
/// at the current position of a schema walk. Each is `None` when the category omits that subtree.
///
/// The Delta stats schema is *nested* (it mirrors the table struct), while `content_stats` is
/// *flat*. As the collector descends into a struct field, [`descend`](Self::descend) steps each
/// category into the matching sub-schema so a leaf's membership can be tested segment-by-segment
/// against the leaf's own name -- never by parsing the flat dotted output name (a column name may
/// itself contain a dot).
#[derive(Clone, Copy)]
struct CategoryScopes<'a> {
    /// Scopes for the three categories, indexed by [`STAT_CATEGORIES`]. Every method treats them
    /// uniformly, so they live in one array rather than three named fields.
    categories: [Option<&'a StructType>; 3],
}

/// The three Delta JSON stat category names, in the order [`CategoryScopes::categories`] indexes.
const STAT_CATEGORIES: [&str; 3] = [NULL_COUNT, MIN_VALUES, MAX_VALUES];

impl<'a> CategoryScopes<'a> {
    /// Builds the top-level scopes from a Delta JSON stats schema. A category header may be absent
    /// (that category simply records nothing); when present it must be a struct mirroring the
    /// table.
    ///
    /// Errors if a header is present as a scalar. This can only be an internal invariant violation:
    /// the Delta stats schema is kernel-generated (by `expected_stats_schema`) with every category
    /// as a struct mirroring the table, so a scalar header indicates a bug, not bad input. Erroring
    /// (rather than silently dropping) keeps this consistent with [`descend`](Self::descend), which
    /// applies the same rule at every deeper level.
    fn from_delta_stats_schema(delta_stats_schema: &'a StructType) -> DeltaResult<Self> {
        let mut categories = [None; 3];
        for (i, category) in STAT_CATEGORIES.iter().enumerate() {
            categories[i] = match struct_sub_schema(delta_stats_schema, category) {
                SubSchema::Absent => None,
                SubSchema::Struct(s) => Some(s),
                SubSchema::Mismatch => {
                    return Err(Error::generic(format!(
                    "Delta stats schema invariant violation: category '{category}' is a scalar, \
                         but it must be a struct mirroring the table"
                )))
                }
            };
        }
        Ok(CategoryScopes { categories })
    }

    /// Steps every category into its `<name>` sub-schema for a struct descent. `path` (which ends
    /// with `name`) is used only for error context.
    ///
    /// Errors if a category has `name` present as a scalar where the table nests a struct. Like
    /// [`from_delta_stats_schema`](Self::from_delta_stats_schema), this can only be an internal
    /// invariant violation (the kernel-generated Delta stats schema must mirror the table), so it
    /// is surfaced as an error rather than silently dropped.
    fn descend(&self, name: &str, path: &[String]) -> DeltaResult<CategoryScopes<'a>> {
        let mut categories = [None; 3];
        for (i, scope) in self.categories.iter().enumerate() {
            categories[i] = match scope {
                // The category already omitted this subtree above; nothing to descend into.
                None => None,
                Some(s) => match struct_sub_schema(s, name) {
                    SubSchema::Absent => None,
                    SubSchema::Struct(sub) => Some(sub),
                    SubSchema::Mismatch => {
                        return Err(Error::generic(format!(
                            "Delta stats schema invariant violation at '{}': category '{}' has \
                             '{name}' as a scalar, but the table nests a struct there",
                            ColumnName::new(path),
                            STAT_CATEGORIES[i],
                        )))
                    }
                },
            };
        }
        Ok(CategoryScopes { categories })
    }

    /// Which categories a leaf named `leaf_name` appears in. Every leaf -- variants included -- is
    /// a scalar in each category (a variant appears in `nullCount` as a scalar `LONG` and is
    /// absent from `minValues`/`maxValues`), so presence is a same-name field lookup.
    fn leaf_categories(&self, leaf_name: &str) -> StatCategories {
        let [null_count, min_values, max_values] = self
            .categories
            .map(|scope| scope.is_some_and(|s| s.field(leaf_name).is_some()));
        StatCategories {
            null_count,
            min_values,
            max_values,
        }
    }
}

/// Walks a table schema for both AMT `content_stats` consumers ([`collect_stats_schema`] and the
/// pivot's [`build_amt_flat_stats_expr`]): descends structs, threads the optional Delta stat
/// projection, and invokes `on_leaf` for each non-struct leaf the projection keeps, passing its
/// field, root-to-leaf path, and category membership (`None` when unprojected). Stat-eligibility is
/// left to the sink. The `SchemaTransform` output is discarded -- `on_leaf` is the sink -- and an
/// `Err` short-circuits the walk.
struct StatsLeafWalker<'a, F> {
    /// Field names from the root to the current node; the last segment is the leaf being visited.
    path: Vec<String>,
    /// Delta stat categories in scope at the current position, or `None` to visit every leaf.
    projection: Option<CategoryScopes<'a>>,
    /// Invoked once per surviving leaf with `(field, path, categories)`.
    on_leaf: F,
}

impl<'a, F> SchemaTransform<'a> for StatsLeafWalker<'a, F>
where
    F: FnMut(&'a StructField, &[String], Option<StatCategories>) -> Result<(), Error>,
{
    transform_output_type!(|'a, T| Result<(), Error>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Result<(), Error> {
        self.path.push(field.name().to_string());
        // Descend into structs; every other type is a leaf. On `Err` the walk aborts and `path` is
        // discarded, so the skipped pop is harmless.
        let result = if let DataType::Struct(_) = field.data_type() {
            // The stat-category scopes step into this struct's sub-schema and are restored on the
            // way back up so siblings are unaffected. A category that is a scalar where the table
            // nests a struct (an internal invariant violation) aborts the walk here.
            let saved_projection = self.projection;
            self.projection = self
                .projection
                .map(|s| s.descend(field.name(), &self.path))
                .transpose()?;
            let result = self.recurse_into_struct_field(field);
            self.projection = saved_projection;
            result
        } else {
            // Every non-struct type is a leaf -- including variants (never descended into: their
            // inner fields carry no field IDs) and array/map columns (which produce no stats). When
            // projecting, a leaf absent from every category is dropped here.
            let categories = self.projection.map(|s| s.leaf_categories(field.name()));
            if categories.is_some_and(|c| !c.any()) {
                Ok(())
            } else {
                (self.on_leaf)(field, &self.path, categories)
            }
        };
        self.path.pop();
        result
    }
}

/// Generates the AMT `content_stats` schema for the given table struct.
///
/// Produces a *flat* stats schema: each primitive/variant leaf of `table_struct` -- however deeply
/// nested inside structs -- becomes one direct child of the output struct, named by the leaf's full
/// dotted path (e.g. `a.b`) and keyed at its base stats field ID (data leaves at
/// `10_000 + 200 * leaf_field_id`, supported reserved-metadata leaves into `[9_000, 10_000)` via
/// [`field_id_to_statistics_base`]; see [`build_stats_struct`]
/// for its sub-fields). This matches the AMT layout, where every leaf's stats struct is a direct
/// child of `content_stats` and the reader projects each leaf by field ID following that flat path.
///
/// Struct columns are descended into and produce no stats entry of their own (their field IDs are
/// never read). `table_struct` must be a physical schema carrying `parquet.field.id` metadata on
/// each leaf (as produced by [`StructField::make_physical`]); logical schemas that annotate field
/// IDs only under `delta.columnMapping.id` are not accepted. Leaf field IDs must be unique (as a
/// physical schema guarantees); duplicates map to the same stats base and are not diagnosed.
///
/// A leaf is omitted when it is an array/map column, or its field ID is outside the supported
/// stats range (e.g. reserved metadata columns like
/// `_file`/`_pos`, or data field IDs above the reserved range), which are skipped with a warning.
/// Returns an error if a leaf is missing its field-id metadata entirely, or is an (as-yet
/// unimplemented) geospatial column.
pub(crate) fn stats_schema(table_struct: &StructType) -> DeltaResult<StructType> {
    collect_stats_schema(table_struct, None)
}

/// Generates the AMT `content_stats` schema projected to the leaves that carry Delta stats.
///
/// Same flat layout as [`stats_schema`], but a leaf is emitted only if its column appears in at
/// least one Delta stat category (`nullCount`/`minValues`/`maxValues`) of `delta_stats_schema`,
/// and a surviving leaf keeps only the stats sub-fields backed by a category it appears in (e.g. a
/// leaf present only in `minValues` keeps `lower_bound` but not `upper_bound`; see
/// [`build_stats_struct`]). This avoids reading per-column stats that no Delta stat records.
/// `delta_stats_schema` is the nested Delta Protocol stats schema (its nesting mirrors the table
/// schema).
///
/// `table_struct` must be a physical schema (carrying `parquet.field.id`, as [`stats_schema`]
/// requires), and `delta_stats_schema` must use the same physical names. Membership is matched by
/// field name, so a naming mismatch (e.g. logical stat names against a physical `table_struct`
/// under column mapping) matches nothing and drops every leaf.
///
/// In addition to the per-leaf errors of [`stats_schema`], returns an error if `delta_stats_schema`
/// violates its shape invariant relative to `table_struct`: a category is a scalar where the table
/// nests a struct. Because `delta_stats_schema` is kernel-generated to mirror the table, this can
/// only be an internal bug, so it is surfaced rather than silently dropped.
pub(crate) fn projected_stats_schema(
    table_struct: &StructType,
    delta_stats_schema: &StructType,
) -> DeltaResult<StructType> {
    collect_stats_schema(
        table_struct,
        Some(CategoryScopes::from_delta_stats_schema(delta_stats_schema)?),
    )
}

/// Shared body of [`stats_schema`] and [`projected_stats_schema`]: walks `table_struct` with the
/// (optional) Delta stat projection and returns the flat `content_stats` schema.
fn collect_stats_schema<'a>(
    table_struct: &'a StructType,
    projection: Option<CategoryScopes<'a>>,
) -> DeltaResult<StructType> {
    let mut fields: Vec<StructField> = Vec::new();
    {
        let mut walker = StatsLeafWalker {
            path: Vec::new(),
            projection,
            on_leaf: |field: &'a StructField, path: &[String], categories| {
                leaf_stats_field(field, path, categories).map(|stats| fields.extend(stats))
            },
        };
        walker.transform_struct(table_struct)?;
    }
    // `new_unchecked` skips name dedup; safe because `ColumnName`'s `Display` is lossless -- a leaf
    // whose name contains a dot is backtick-escaped, so it never collides with a nested path.
    Ok(StructType::new_unchecked(fields))
}

// == Delta stats -> AMT content_stats columnar conversion ==

/// Replaces `data`'s parsed Delta-stats column with the flat AMT `content_stats` layout in place.
///
/// `stats_column_name` names a struct column of parsed Delta stats (`numRecords`, `tightBounds`,
/// and `minValues`/`maxValues`/`nullCount` nested to mirror the table); on success it becomes an
/// AMT `content_stats` struct (see [`stats_schema`]) and every other column passes through. For
/// example, `{stats: {numRecords, minValues: {id}, ...}, ...}` becomes
/// `{stats: {id: {lower_bound, value_count, ...}}, ...}`.
///
/// Returns `Ok(None)` (leaving `content_stats` null, which disables manifest-level data skipping)
/// when `input_schema`'s stats column is not Delta-stats shape (no `numRecords`) or when evaluation
/// fails. `table_schema` must be physical (carrying `parquet.field.id`, as [`stats_schema`]
/// requires); precondition violations in *building* the pivot propagate as errors.
pub(crate) fn try_preconvert_stats_column(
    engine: &dyn Engine,
    data: &dyn EngineData,
    stats_column_name: &str,
    table_schema: &StructType,
    input_schema: &StructType,
) -> DeltaResult<Option<Box<dyn EngineData>>> {
    if !is_delta_json_stats_schema(input_schema, stats_column_name) {
        return Ok(None);
    }
    // Reference only the stat sources `input_schema` declares. `is_delta_json_stats_schema`
    // guarantees the stats column is a struct, so `declared_stats` is always `Some`.
    let Some(known) = input_schema
        .field(stats_column_name)
        .and_then(|f| match f.data_type() {
            DataType::Struct(s) => Some(s.as_ref()),
            _ => None,
        })
    else {
        return Ok(None);
    };

    // Building the pivot fails only on deterministic precondition violations (a non-physical
    // `table_schema`, a leaf missing its field id, or an unsupported column type) independent of
    // the input data, so those errors propagate rather than silently disabling data skipping.
    let (expr, amt_stats_schema) =
        build_delta_to_amt_pivot_expression(table_schema, stats_column_name, Some(known))?;
    // Output schema is the input with the stats column's type swapped for the AMT struct.
    let output_fields = input_schema.fields().map(|f| {
        if f.name() == stats_column_name {
            StructField::new(f.name(), amt_stats_schema.clone(), f.nullable)
        } else {
            f.clone()
        }
    });
    let output_schema = StructType::new_unchecked(output_fields);

    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        Arc::new(input_schema.clone()),
        Arc::new(expr),
        output_schema.into(),
    )?;
    match evaluator.evaluate(data) {
        Ok(result) => Ok(Some(result)),
        Err(e) => {
            warn!(
                "AMT stats pivot failed; leaving content_stats null (data skipping disabled): {e}"
            );
            Ok(None)
        }
    }
}

/// Builds a [`struct_patch`](Expression::struct_patch) replacing `stats_column_name` with the flat
/// AMT `content_stats` struct, returning the expression and the schema it produces (see
/// [`stats_schema`]).
///
/// `known_stats_schema` guards which Delta stat sources are referenced: `None` references every
/// stat the AMT layout can source; `Some(schema)` references only fields present in it and nulls
/// the rest. It never changes which AMT leaves are emitted.
fn build_delta_to_amt_pivot_expression(
    table_schema: &StructType,
    stats_column_name: &str,
    known_stats_schema: Option<&StructType>,
) -> DeltaResult<(Expression, StructType)> {
    let (amt_struct_expr, amt_stats_schema) =
        build_amt_flat_stats_expr(table_schema, stats_column_name, known_stats_schema)?;
    let patch =
        ExpressionStructPatchBuilder::new().replace(stats_column_name, Arc::new(amt_struct_expr));
    Ok((Expression::struct_patch(patch)?, amt_stats_schema))
}

/// Walks `table_schema` (via [`StatsLeafWalker`], no projection) and, for each stat-eligible leaf,
/// builds both its flat AMT `content_stats` output field and the expression that fills it from the
/// parsed Delta stats under `stats_col`. Returns the flat AMT struct expression and its schema,
/// whose fields align by construction (both derived from the same [`leaf_stats_field`] result).
fn build_amt_flat_stats_expr<'a>(
    table_schema: &'a StructType,
    stats_col: &str,
    known_stats_schema: Option<&StructType>,
) -> DeltaResult<(Expression, StructType)> {
    let mut fields: Vec<StructField> = Vec::new();
    let mut exprs: Vec<ExpressionRef> = Vec::new();
    {
        let mut walker = StatsLeafWalker {
            path: Vec::new(),
            projection: None,
            on_leaf: |field: &'a StructField, path: &[String], categories| {
                let Some(leaf_field) = leaf_stats_field(field, path, categories)? else {
                    return Ok(());
                };
                let DataType::Struct(stats_struct) = leaf_field.data_type() else {
                    return Ok(());
                };
                let leaf_expr = build_leaf_pivot_expr(
                    stats_struct,
                    field.data_type(),
                    stats_col,
                    path,
                    known_stats_schema,
                );
                exprs.push(Arc::new(leaf_expr));
                fields.push(leaf_field);
                Ok(())
            },
        };
        walker.transform_struct(table_schema)?;
    }
    Ok((
        Expression::struct_from(exprs),
        StructType::new_unchecked(fields),
    ))
}

/// Builds one leaf's AMT stats struct expression, filling each sub-field of `stats_struct` from the
/// parsed Delta stats under `stats_col`. `leaf_type` drives the `tight_bounds` rule; `path` is the
/// leaf's root-to-leaf path, so its Delta bounds/counts live at `<stats_col>.<category>.<path>`.
/// With `known_stats_schema = Some`, only stat columns it declares are referenced; absent sources
/// become null literals.
fn build_leaf_pivot_expr(
    stats_struct: &StructType,
    leaf_type: &DataType,
    stats_col: &str,
    path: &[String],
    known_stats_schema: Option<&StructType>,
) -> Expression {
    let leaf_path: Vec<&str> = path.iter().map(String::as_str).collect();
    // Whether a Delta stat at `segments` (within the stats sub-schema) is present; always true when
    // unprojected. A nested category mirrors the table, so its path is the category plus
    // `leaf_path`.
    let exists = |segments: &[&str]| {
        known_stats_schema.is_none_or(|schema| has_nested_field(schema, segments))
    };
    let nested_path = |category: &'static str| -> Vec<&str> {
        std::iter::once(category)
            .chain(leaf_path.iter().copied())
            .collect()
    };
    // Column reference into a nested (table-mirroring) Delta stat category.
    let nested_col = |category: &str| {
        let mut segments: Vec<String> = Vec::with_capacity(2 + path.len());
        segments.push(stats_col.to_string());
        segments.push(category.to_string());
        segments.extend(path.iter().cloned());
        Expression::column(segments)
    };

    let exprs = stats_struct.fields().map(|f| {
        let expr = match f.name().as_str() {
            // TODO: Delta records NaN in min/max, but Iceberg bounds exclude it (NaN is tracked
            // separately in nan_value_count). Copying Delta's bound directly can misstate the
            // Iceberg bound when the column contains NaN; a follow-up should detect NaN and null
            // the affected bound (needs a NaN-detection expression, which does not
            // exist yet).
            LOWER_BOUND if exists(&nested_path(MIN_VALUES)) => nested_col(MIN_VALUES),
            UPPER_BOUND if exists(&nested_path(MAX_VALUES)) => nested_col(MAX_VALUES),
            NULL_VALUE_COUNT if exists(&nested_path(NULL_COUNT)) => nested_col(NULL_COUNT),
            // Delta has no per-column value count. Iceberg `value_count` counts present values
            // (nulls included; a leaf is absent only under a null nullable struct). `numRecords` is
            // exact for top-level leaves and an over-approximation under a nullable struct -- Delta
            // records no better source.
            VALUE_COUNT if exists(&[NUM_RECORDS]) => Expression::column([stats_col, NUM_RECORDS]),
            TIGHT_BOUNDS => tight_bounds_expr(leaf_type, stats_col, exists(&[DELTA_TIGHT_BOUNDS])),
            // No Delta source (nan_value_count, avg_value_size_in_bytes) or the source column is
            // absent from the known input schema.
            _ => null_lit(f.data_type().clone()),
        };
        Arc::new(expr)
    });
    Expression::struct_from(exprs)
}

/// Builds the `tight_bounds` expression for a leaf.
///
/// String, binary, `TIMESTAMP`, and `TIMESTAMP_NTZ` bounds may be truncated by Delta, so they are
/// never tight. Every other type takes the file's Delta `tightBounds`, defaulting to `true` when
/// the column is absent (`tight_bounds_present == false`) or null in a row -- preserving the
/// invariant that a `false` file value forces `false` for every column.
///
/// TODO: a conservative subset of the full rules. Float/double leaves whose Delta bounds include
/// NaN are not yet handled (see [`build_leaf_pivot_expr`]), and MDV -> `tight_bounds = false` is
/// applied by the write-path caller, not here.
fn tight_bounds_expr(
    leaf_type: &DataType,
    stats_col: &str,
    tight_bounds_present: bool,
) -> Expression {
    if tight_bounds_forced_false(leaf_type) {
        return lit(false);
    }
    if !tight_bounds_present {
        return lit(true);
    }
    Expression::variadic(
        VariadicExpressionOp::Coalesce,
        [
            Expression::column([stats_col, DELTA_TIGHT_BOUNDS]),
            lit(true),
        ],
    )
}

/// Whether a leaf's `tight_bounds` must be forced to `false` because Delta may store truncated
/// (non-tight) bounds for it: string, binary, `TIMESTAMP`, and `TIMESTAMP_NTZ` columns.
///
/// Variant and geospatial leaves never reach here: variants get no `tight_bounds` sub-field
/// ([`build_stats_struct`] excludes it via `!is_variant`), and geospatial leaves error earlier in
/// [`leaf_stats_field`] (stats are unimplemented for them), so neither type is handled below.
fn tight_bounds_forced_false(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Primitive(
            PrimitiveType::String
                | PrimitiveType::Binary
                | PrimitiveType::Timestamp
                | PrimitiveType::TimestampNtz
        )
    )
}

/// Whether the nested field `path` exists in `schema`, descending structs and variants.
fn has_nested_field(schema: &StructType, path: &[&str]) -> bool {
    let [first, rest @ ..] = path else {
        return true;
    };
    match schema.field(*first) {
        None => false,
        Some(_) if rest.is_empty() => true,
        Some(f) => match f.data_type() {
            DataType::Struct(s) => has_nested_field(s, rest),
            DataType::Variant(s) => has_nested_field(s, rest),
            _ => false,
        },
    }
}

/// Whether `stats_column_name` in `schema` is a Delta stats struct, identified by a `numRecords`
/// field.
fn is_delta_json_stats_schema(schema: &StructType, stats_column_name: &str) -> bool {
    schema
        .field(stats_column_name)
        .and_then(|f| match f.data_type() {
            DataType::Struct(s) => Some(s),
            _ => None,
        })
        .is_some_and(|s| s.field(NUM_RECORDS).is_some())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{AsArray, RecordBatch, StructArray};
    use crate::create_row;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, StructData};
    use crate::scan::data_skipping::stats_schema::{expected_stats_schema, StatsConfig};
    use crate::schema::{ArrayType, MapType};
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};
    use crate::table_properties::DataSkippingNumIndexedCols;

    #[rstest]
    #[case(0, 10_000)]
    #[case(1, 10_200)]
    #[case(2, 10_400)]
    #[case(5, 11_000)]
    #[case(100, 30_000)]
    #[case(MAX_DATA_FIELD_ID, MAX_DATA_STATS_FIELD_ID)]
    #[case(LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, 9_000)]
    #[case(ROW_ID_FIELD_ID, 9_200)]
    fn valid_mapping_roundtrips(#[case] field_id: i32, #[case] stats_base: i32) {
        assert_eq!(field_id_to_statistics_base(field_id), Some(stats_base));
    }

    /// Field IDs that `field_id_to_statistics_base` must reject.
    #[rstest]
    #[case(-1)] // negative
    #[case(MAX_DATA_FIELD_ID + 1)] // data field ID above the reserved range
    #[case(2_147_483_541)] // _commit_snapshot_id (unsupported reserved metadata)
    #[case(2_147_483_645)] // _pos (unsupported reserved metadata)
    #[case(2_147_483_646)] // _file (unsupported reserved metadata)
    fn field_id_to_statistics_base_rejects_invalid(#[case] field_id: i32) {
        assert_eq!(field_id_to_statistics_base(field_id), None);
    }

    /// Returns the stats struct named `name` (the flat dotted output key) in `stats`, panicking if
    /// absent or not a struct.
    fn stats_struct_for_name(name: &str, stats: &StructType) -> StructType {
        let field_stats = stats.field(name).expect("stats field should exist");
        match field_stats.data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            other => panic!("expected struct stats, got {other:?}"),
        }
    }

    /// Asserts every stats sub-field of `stats_struct` carries the expected offset from `base_id`.
    /// `null_value_count` is always present. Variants omit `tight_bounds` and always carry the size
    /// stat.
    fn assert_stats_field_ids(stats_struct: &StructType, base_id: i32, field: &StructField) {
        let is_variant = matches!(field.data_type(), DataType::Variant(_));
        let assert_offset = |name: &str, offset: i32| {
            assert_eq!(
                get_field_id(stats_struct.field(name).unwrap()),
                Some(base_id + offset)
            );
        };
        assert_offset(VALUE_COUNT, STATS_OFFSET_VALUE_COUNT);
        assert_offset(NULL_VALUE_COUNT, STATS_OFFSET_NULL_VALUE_COUNT);
        if field.data_type() == &DataType::FLOAT || field.data_type() == &DataType::DOUBLE {
            assert_offset(NAN_VALUE_COUNT, STATS_OFFSET_NAN_VALUE_COUNT);
        }
        if is_variant
            || field.data_type() == &DataType::STRING
            || field.data_type() == &DataType::BINARY
        {
            assert_offset(
                AVG_VALUE_SIZE_IN_BYTES,
                STATS_OFFSET_AVG_VALUE_SIZE_IN_BYTES,
            );
        }
        assert_offset(LOWER_BOUND, STATS_OFFSET_LOWER_BOUND);
        assert_offset(UPPER_BOUND, STATS_OFFSET_UPPER_BOUND);
        if !is_variant {
            assert_offset(TIGHT_BOUNDS, STATS_OFFSET_TIGHT_BOUNDS);
        }
    }

    // `null_value_count` is always present, so counts include it regardless of nullability.
    #[rstest]
    #[case(DataType::INTEGER, false, 1, 10_200, 5)] // fixed-length
    #[case(DataType::STRING, true, 2, 10_400, 6)] // size stats
    #[case(DataType::DOUBLE, true, 5, 11_000, 6)] // nan count
    #[case(DataType::FLOAT, false, 100, 30_000, 6)] // nan count
    #[case(DataType::LONG, true, 42, 18_400, 5)] // fixed-length
    #[case(DataType::BINARY, true, 3, 10_600, 6)] // size stats
    #[case(DataType::BINARY, false, 4, 10_800, 6)] // size stats
    #[case(
        DataType::INTEGER,
        false,
        MAX_DATA_FIELD_ID,
        MAX_DATA_STATS_FIELD_ID,
        5
    )] // top of range
    fn stats_schema_primitive_field(
        #[case] data_type: DataType,
        #[case] nullable: bool,
        #[case] field_id: i32,
        #[case] expected_base: i32,
        #[case] expected_count: usize,
    ) {
        let field = field_with_id("c", data_type.clone(), nullable, field_id);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let stats_struct = stats_struct_for_name(field.name(), &stats);

        assert_eq!(stats_struct.fields().count(), expected_count);
        // Bounds preserve the column's type.
        assert_eq!(
            stats_struct.field(LOWER_BOUND).unwrap().data_type(),
            &data_type
        );
        assert_eq!(
            stats_struct.field(UPPER_BOUND).unwrap().data_type(),
            &data_type
        );
        // The stats group field itself carries the base stats ID, not the original field ID.
        assert_eq!(get_field_id(stats.field("c").unwrap()), Some(expected_base));
        assert_stats_field_ids(&stats_struct, expected_base, &field);
    }

    #[test]
    fn stats_schema_multiple_fields() {
        let schema = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            field_with_id("name", DataType::STRING, true, 1),
            field_with_id("score", DataType::DOUBLE, true, 2),
        ]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 3);
        assert!(stats.field("id").is_some());
        assert!(stats.field("name").is_some());
        assert!(stats.field("score").is_some());
    }

    #[test]
    fn stats_schema_missing_field_id_errors() {
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);
        assert!(stats_schema(&schema).is_err());
    }

    #[test]
    fn stats_schema_nested_missing_field_id_errors() {
        // A child missing its field ID must error (not silently drop) even inside a valid parent
        // struct -- the abort-vs-drop distinction of the projection carrier survives recursion.
        let inner = StructType::new_unchecked([StructField::not_null("b", DataType::INTEGER)]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1)]);
        assert!(stats_schema(&schema).is_err());
    }

    #[test]
    fn stats_schema_non_numeric_field_id_metadata_errors() {
        // A field-id annotation of the wrong metadata type is treated as missing => error.
        let field = StructField::not_null("c", DataType::INTEGER).with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::String("1".to_string()),
        )]);
        assert!(stats_schema(&StructType::new_unchecked([field])).is_err());
    }

    #[test]
    fn stats_schema_nested_struct() {
        // { a: struct { b: int (non-null), c: double (nullable) } } -- flattens to `a.b`, `a.c`.
        let field_b = field_with_id("b", DataType::INTEGER, false, 2);
        let field_c = field_with_id("c", DataType::DOUBLE, true, 3);
        let inner = StructType::new_unchecked([field_b.clone(), field_c.clone()]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        // Flat layout: no `a` entry; leaves are direct children keyed by their own base IDs.
        assert_eq!(stats.fields().count(), 2);
        assert!(stats.field("a").is_none());

        // `b` is a not-null int; it still carries a null_value_count (always present), so 5
        // sub-fields (lower, upper, tight, value, null).
        let b_stats = stats_struct_for_name("a.b", &stats);
        assert_eq!(b_stats.fields().count(), 5);
        assert_eq!(get_field_id(stats.field("a.b").unwrap()), Some(10_400));
        assert_stats_field_ids(&b_stats, 10_400, &field_b);

        let c_stats = stats_struct_for_name("a.c", &stats);
        assert_eq!(c_stats.fields().count(), 6); // null + nan count
        assert_eq!(get_field_id(stats.field("a.c").unwrap()), Some(10_600));
        assert_stats_field_ids(&c_stats, 10_600, &field_c);
    }

    #[rstest]
    #[case::array(DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))))]
    #[case::map(DataType::Map(Box::new(MapType::new(
        DataType::STRING,
        DataType::INTEGER,
        false
    ))))]
    fn stats_schema_complex_leaf_is_omitted(#[case] data_type: DataType) {
        // Array element / map key-value nodes carry no field-id context, so they produce no leaf
        // stats. The column is omitted rather than emitting an unbuildable empty stats struct.
        let schema = StructType::new_unchecked([field_with_id("c", data_type, true, 1)]);
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 0);
        assert!(stats.field("c").is_none());
    }

    #[test]
    fn stats_schema_struct_of_only_complex_is_omitted() {
        let inner = StructType::new_unchecked([field_with_id(
            "f0",
            DataType::Array(Box::new(ArrayType::new(DataType::FLOAT, true))),
            true,
            2,
        )]);
        let schema = StructType::new_unchecked([field_with_id("s", inner.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 0);
        assert!(stats.field("s").is_none());
    }

    #[test]
    fn stats_schema_struct_with_primitive_and_complex_keeps_primitive() {
        let inner = StructType::new_unchecked([
            field_with_id("p", DataType::INTEGER, true, 2),
            field_with_id(
                "a",
                DataType::Array(Box::new(ArrayType::new(DataType::FLOAT, true))),
                true,
                3,
            ),
        ]);
        let schema = StructType::new_unchecked([field_with_id("s", inner.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        // Only the primitive leaf 's.p' survives (flattened); the array field 's.a' is omitted.
        assert_eq!(stats.fields().count(), 1);
        assert!(stats.field("s").is_none());
        assert!(stats.field("s.p").is_some());
        assert!(stats.field("s.a").is_none());
        assert_eq!(get_field_id(stats.field("s.p").unwrap()), Some(10_400));
    }

    #[test]
    fn stats_schema_deeply_nested() {
        // { a: struct { b: struct { c: int } } } -- flattens to a single leaf `a.b.c`.
        let innermost =
            StructType::new_unchecked([field_with_id("c", DataType::INTEGER, false, 3)]);
        let middle = StructType::new_unchecked([field_with_id("b", innermost.into(), true, 2)]);
        let schema = StructType::new_unchecked([field_with_id("a", middle.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);
        assert!(stats.field("a").is_none());
        assert!(stats.field("a.b").is_none());

        let c_stats = stats_struct_for_name("a.b.c", &stats);
        // Not-null int; null_value_count is always present, so 5 sub-fields.
        assert_eq!(c_stats.fields().count(), 5);
        assert!(c_stats.field(VALUE_COUNT).is_some());
        assert!(c_stats.field(LOWER_BOUND).is_some());
        assert!(c_stats.field(NULL_VALUE_COUNT).is_some());
        assert_eq!(get_field_id(stats.field("a.b.c").unwrap()), Some(10_600));
    }

    /// Both unshredded and shredded variant inputs produce the same ordinary stats struct: the
    /// bounds are recorded as unshredded variants, `tight_bounds` is excluded, and the size stat is
    /// present. The physical inner layout (e.g. a shredded `typed_value`) does not affect the
    /// generated stats.
    #[rstest]
    #[case::unshredded(DataType::unshredded_variant())]
    #[case::shredded(
        DataType::variant_type([
            StructField::not_null("metadata", DataType::BINARY),
            StructField::not_null("value", DataType::BINARY),
            StructField::nullable("typed_value", DataType::INTEGER),
        ])
        .expect("variant type")
    )]
    fn stats_schema_variant_column(#[case] variant_type: DataType) {
        let field = field_with_id("v", variant_type, false, 3);
        let schema = StructType::new_unchecked([field.clone()]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        // The stats container is an ordinary struct carrying the base stats ID (10_600 for id 3).
        let stats_field = stats.field("v").expect("variant column should exist");
        assert!(matches!(stats_field.data_type(), DataType::Struct(_)));
        assert_eq!(get_field_id(stats_field), Some(10_600));

        let v_stats = stats_struct_for_name(field.name(), &stats);
        // Bounds are recorded as unshredded variants (metadata + value), not the physical encoding.
        assert_eq!(
            v_stats.field(LOWER_BOUND).unwrap().data_type(),
            &DataType::unshredded_variant()
        );
        assert_eq!(
            v_stats.field(UPPER_BOUND).unwrap().data_type(),
            &DataType::unshredded_variant()
        );
        // Variants exclude tight_bounds and always include the size stat.
        assert!(v_stats.field(TIGHT_BOUNDS).is_none());
        assert!(v_stats.field(AVG_VALUE_SIZE_IN_BYTES).is_some());
        assert_stats_field_ids(&v_stats, 10_600, &field);
    }

    #[test]
    fn stats_schema_variant_nested_in_struct() {
        let data = field_with_id("data", DataType::unshredded_variant(), false, 6);
        let inner = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 5),
            data.clone(),
        ]);
        let schema = StructType::new_unchecked([field_with_id("record", inner.into(), false, 3)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        // Flat layout: `record.id` and `record.data` are direct children keyed by their own IDs.
        assert_eq!(stats.fields().count(), 2);
        assert!(stats.field("record").is_none());
        assert!(stats.field("record.id").is_some());
        assert_eq!(
            get_field_id(stats.field("record.id").unwrap()),
            Some(11_000)
        ); // 10_000+200*5

        // The nested variant column becomes an ordinary stats struct with unshredded-variant
        // bounds.
        let data_stats = stats_struct_for_name("record.data", &stats);
        assert_eq!(
            data_stats.field(LOWER_BOUND).unwrap().data_type(),
            &DataType::unshredded_variant()
        );
        assert_eq!(
            get_field_id(stats.field("record.data").unwrap()),
            Some(11_200)
        ); // 10_000+200*6
    }

    /// `null_value_count` is always emitted, independent of the leaf's or any ancestor's
    /// nullability. Covers plain and variant leaves, at top level and nested under a not-null
    /// struct (a not-null leaf under a not-null ancestor must still carry it).
    #[rstest]
    #[case::top_level_not_null_int(DataType::INTEGER, false, None)]
    #[case::top_level_nullable_variant(DataType::unshredded_variant(), true, None)]
    #[case::not_null_int_under_not_null_struct(DataType::INTEGER, false, Some(false))]
    #[case::not_null_variant_under_not_null_struct(
        DataType::unshredded_variant(),
        false,
        Some(false)
    )]
    fn null_value_count_always_present(
        #[case] leaf_type: DataType,
        #[case] leaf_nullable: bool,
        #[case] parent_nullable: Option<bool>,
    ) {
        let leaf = field_with_id("leaf", leaf_type, leaf_nullable, 2);
        let (schema, leaf_name) = match parent_nullable {
            None => (StructType::new_unchecked([leaf]), "leaf"),
            Some(nullable) => (
                StructType::new_unchecked([field_with_id(
                    "parent",
                    StructType::new_unchecked([leaf]).into(),
                    nullable,
                    1,
                )]),
                "parent.leaf",
            ),
        };
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let leaf_stats = stats_struct_for_name(leaf_name, &stats);
        assert!(leaf_stats.field(NULL_VALUE_COUNT).is_some());
    }

    #[test]
    fn stats_schema_variant_stats_independent_of_physical_layout() {
        // Stats are emitted for any variant column regardless of its physical inner layout: even a
        // variant whose inner struct lacks a `value` field still gets unshredded-variant bounds.
        let variant = DataType::Variant(Box::new(StructType::new_unchecked([
            StructField::not_null("metadata", DataType::BINARY),
        ])));
        let field = field_with_id("v", variant, false, 3);
        let schema = StructType::new_unchecked([field]);
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let v_stats = stats_struct_for_name("v", &stats);
        assert_eq!(
            v_stats.field(LOWER_BOUND).unwrap().data_type(),
            &DataType::unshredded_variant()
        );
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest]
    #[case::geometry_in_range(DataType::from(GeometryType::try_new("EPSG:4326").expect("valid crs")), 1)]
    #[case::geography_in_range(DataType::from(
        GeographyType::try_new("EPSG:4326", EdgeInterpolationAlgorithm::Spherical).expect("valid crs")
    ), 1)]
    // Out of range: still errors, because the geospatial check precedes the range check.
    #[case::geometry_out_of_range(
        DataType::from(GeometryType::try_new("EPSG:4326").expect("valid crs")),
        MAX_DATA_FIELD_ID + 1
    )]
    fn stats_schema_geospatial_column_errors(#[case] geo_type: DataType, #[case] field_id: i32) {
        let schema = StructType::new_unchecked([field_with_id("g", geo_type, true, field_id)]);
        let err = stats_schema(&schema).expect_err("geospatial columns are not yet supported");
        assert!(
            err.to_string().contains("geospatial"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stats_schema_out_of_range_data_field_id_is_dropped() {
        // A valid column at the top of the data range is kept; a data field ID just past the
        // range is warn-dropped (not an error), unlike a missing field ID.
        let ok = field_with_id("hi", DataType::INTEGER, false, MAX_DATA_FIELD_ID);
        let over = field_with_id("over", DataType::INTEGER, false, MAX_DATA_FIELD_ID + 1);
        let schema = StructType::new_unchecked([ok, over]);
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(
            get_field_id(stats.field("hi").unwrap()),
            Some(MAX_DATA_STATS_FIELD_ID)
        );
        assert!(stats.field("over").is_none());
    }

    #[rstest]
    #[case(MAX_DATA_FIELD_ID + 1)] // out of range
    #[case(-5)] // negative
    fn stats_schema_nested_out_of_range_child_is_dropped(#[case] bad_id: i32) {
        // A leaf whose field ID is out of range (or negative) is warn-dropped, not an error; the
        // surviving sibling leaf is still emitted (flattened as `a.keep`).
        let inner = StructType::new_unchecked([
            field_with_id("keep", DataType::INTEGER, false, 2),
            field_with_id("drop", DataType::INTEGER, false, bad_id),
        ]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1)]);
        let stats = stats_schema(&schema).expect("out-of-range child warn-drops, not errors");
        assert_eq!(stats.fields().count(), 1);
        assert!(stats.field("a.keep").is_some());
        assert!(stats.field("a.drop").is_none());
        assert_eq!(get_field_id(stats.field("a.keep").unwrap()), Some(10_400));
    }

    #[rstest]
    #[case(MAX_DATA_FIELD_ID + 1)] // out of range
    #[case(-5)] // negative
    fn stats_schema_out_of_range_struct_ancestor_keeps_leaves(#[case] bad_id: i32) {
        // A struct ancestor's own field ID is never read, so an out-of-range (or negative) struct
        // ID must not suppress its valid in-range leaves.
        let inner = StructType::new_unchecked([field_with_id("b", DataType::INTEGER, false, 2)]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, bad_id)]);
        let stats = stats_schema(&schema).expect("struct ancestor id is irrelevant");
        assert_eq!(stats.fields().count(), 1);
        assert_eq!(get_field_id(stats.field("a.b").unwrap()), Some(10_400));
    }

    #[test]
    fn stats_schema_struct_ancestor_missing_field_id_keeps_leaves() {
        // A struct's own field ID is never read, so an ancestor missing its field ID entirely is
        // tolerated -- only its leaves' IDs matter. Contrast with a LEAF missing its ID, which
        // errors (see `stats_schema_nested_missing_field_id_errors`).
        let inner = StructType::new_unchecked([field_with_id("b", DataType::INTEGER, false, 2)]);
        let schema = StructType::new_unchecked([StructField::nullable("a", inner)]);
        let stats = stats_schema(&schema).expect("struct ancestor missing id is tolerated");
        assert_eq!(stats.fields().count(), 1);
        assert_eq!(get_field_id(stats.field("a.b").unwrap()), Some(10_400));
    }

    #[test]
    fn stats_schema_empty_input_is_empty() {
        let stats = stats_schema(&StructType::new_unchecked([])).expect("should succeed");
        assert_eq!(stats.fields().count(), 0);
    }

    #[test]
    fn stats_schema_with_metadata_columns_skips_unsupported() {
        let id = field_with_id("id", DataType::LONG, false, 0);
        let name = field_with_id("name", DataType::STRING, true, 1);
        let score = field_with_id("score", DataType::DOUBLE, true, 2);
        // _file and _pos are unsupported reserved metadata fields -- skipped (warn).
        let file = field_with_id("_file", DataType::STRING, false, 2_147_483_646);
        let pos = field_with_id("_pos", DataType::LONG, false, 2_147_483_645);
        // _row_id and _last_updated_sequence_number are supported reserved metadata fields.
        let row_id = field_with_id("_row_id", DataType::LONG, false, 2_147_483_540);
        let last_updated_seq_no = field_with_id(
            "_last_updated_sequence_number",
            DataType::LONG,
            false,
            2_147_483_539,
        );
        let schema = StructType::new_unchecked([
            id.clone(),
            name.clone(),
            score.clone(),
            file,
            pos,
            row_id.clone(),
            last_updated_seq_no.clone(),
        ]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        // 3 data + 2 supported metadata; _file and _pos are skipped.
        assert_eq!(stats.fields().count(), 5);
        assert_stats_field_ids(&stats_struct_for_name(id.name(), &stats), 10_000, &id);
        assert_stats_field_ids(&stats_struct_for_name(name.name(), &stats), 10_200, &name);
        assert_stats_field_ids(&stats_struct_for_name(score.name(), &stats), 10_400, &score);
        assert!(stats.field("_file").is_none());
        assert!(stats.field("_pos").is_none());
        assert_stats_field_ids(
            &stats_struct_for_name(row_id.name(), &stats),
            9_200,
            &row_id,
        );
        assert_stats_field_ids(
            &stats_struct_for_name(last_updated_seq_no.name(), &stats),
            9_000,
            &last_updated_seq_no,
        );
    }

    #[test]
    fn stats_schema_field_id_exceeding_i32_range_errors() {
        // A field ID present but exceeding i32 range is unusable (not merely "missing") and errors,
        // rather than being silently mis-keyed.
        let field = StructField::not_null("c", DataType::INTEGER).with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(i32::MAX as i64 + 1),
        )]);
        assert!(stats_schema(&StructType::new_unchecked([field])).is_err());
    }

    #[test]
    fn stats_schema_leaf_name_with_dot_is_escaped_and_does_not_collide() {
        // A struct `a` with leaf `b` flattens to output key `a.b`. A sibling top-level leaf whose
        // name literally contains a dot ("a.b") must NOT collide: `ColumnName` backtick-escapes the
        // dotted name, keeping the two distinct under `new_unchecked` (which does not dedup).
        let inner = StructType::new_unchecked([field_with_id("b", DataType::INTEGER, false, 2)]);
        let dotted = field_with_id("a.b", DataType::INTEGER, false, 3);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1), dotted]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 2);
        // The genuine nested path.
        assert_eq!(get_field_id(stats.field("a.b").unwrap()), Some(10_400));
        // The escaped top-level name. Assert via `ColumnName` rather than hardcoding the escape
        // spelling, so the test tracks the naming contract, not a specific rendering.
        let escaped = ColumnName::new(["a.b"]).to_string();
        assert_ne!(
            escaped, "a.b",
            "a dotted leaf name must escape to avoid collision"
        );
        assert_eq!(get_field_id(stats.field(&escaped).unwrap()), Some(10_600));
    }

    #[test]
    fn stats_schema_sibling_structs_preserve_order_and_path_prefixes() {
        // Sibling structs must each prefix their own leaves (no path-stack leakage), and outputs
        // must appear in schema order. `a`/`b` are deliberately not adjacent to `top0`/`top1`.
        let a = field_with_id(
            "a",
            StructType::new_unchecked([
                field_with_id("x", DataType::INTEGER, false, 10),
                field_with_id("y", DataType::INTEGER, false, 11),
            ])
            .into(),
            true,
            1,
        );
        let b = field_with_id(
            "b",
            StructType::new_unchecked([field_with_id("z", DataType::INTEGER, false, 20)]).into(),
            true,
            2,
        );
        let schema = StructType::new_unchecked([
            field_with_id("top0", DataType::INTEGER, false, 5),
            a,
            b,
            field_with_id("top1", DataType::INTEGER, false, 6),
        ]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let names: Vec<_> = stats.fields().map(|f| f.name().to_string()).collect();
        assert_eq!(names, ["top0", "a.x", "a.y", "b.z", "top1"]);
        assert_eq!(
            get_field_id(stats.field("a.y").unwrap()),
            Some(10_000 + 200 * 11)
        );
        assert_eq!(
            get_field_id(stats.field("b.z").unwrap()),
            Some(10_000 + 200 * 20)
        );
    }

    // === projected_stats_schema ===

    /// Builds a Delta JSON stats schema from its three category sub-structs (any may be omitted).
    /// Only field names and struct nesting matter to the projection, so leaf types are arbitrary.
    fn delta_stats(
        null_count: Option<StructType>,
        min_values: Option<StructType>,
        max_values: Option<StructType>,
    ) -> StructType {
        let categories = [
            (NULL_COUNT, null_count),
            (MIN_VALUES, min_values),
            (MAX_VALUES, max_values),
        ];
        let fields = categories
            .into_iter()
            .filter_map(|(name, sub)| sub.map(|s| StructField::nullable(name, s)));
        StructType::new_unchecked(fields)
    }

    /// A stat category sub-struct listing `names` as (arbitrary-typed) leaves.
    fn stat_cols<'a>(names: impl IntoIterator<Item = &'a str>) -> StructType {
        StructType::new_unchecked(
            names
                .into_iter()
                .map(|n| StructField::nullable(n, DataType::LONG)),
        )
    }

    /// A stat category with a single nested struct field `name` whose leaves are `leaves` --
    /// mirrors a nested table column in one category.
    fn nested_cat<'a>(name: &str, leaves: impl IntoIterator<Item = &'a str>) -> StructType {
        StructType::new_unchecked([StructField::nullable(name, stat_cols(leaves))])
    }

    /// A physical (field-id-carrying) struct column `name` wrapping `children`. Shorthand for the
    /// `field_with_id(name, StructType::new_unchecked([...]).into(), true, id)` boilerplate.
    fn struct_field_with_id(
        name: &str,
        field_id: i32,
        children: impl IntoIterator<Item = StructField>,
    ) -> StructField {
        field_with_id(
            name,
            StructType::new_unchecked(children).into(),
            true,
            field_id,
        )
    }

    /// The exact flat stats entry the projection emits for one surviving leaf: the dotted `name`
    /// keyed at the leaf's base stats ID, holding the sub-fields [`build_stats_struct`]
    /// produces for `bounds_type` restricted to the Delta stat `categories` the leaf appears in
    /// (each a [`NULL_COUNT`]/[`MIN_VALUES`]/[`MAX_VALUES`] name). Lets a case assert the full
    /// projected schema by naming a leaf's categories rather than field-presence alone.
    fn expected_leaf(
        name: &str,
        bounds_type: DataType,
        field_id: i32,
        categories: &[&str],
    ) -> StructField {
        let present = StatCategories {
            null_count: categories.contains(&NULL_COUNT),
            min_values: categories.contains(&MIN_VALUES),
            max_values: categories.contains(&MAX_VALUES),
        };
        let base =
            field_id_to_statistics_base(field_id).expect("field id in supported stats range");
        field_with_id(
            name,
            build_stats_struct(base, &bounds_type, Some(present)).into(),
            true,
            base,
        )
    }

    #[test]
    fn projected_leaf_in_all_categories_keeps_all_category_backed_subfields() {
        // {id: long, a: struct{b: int}, v: variant}, with every leaf in all three categories.
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            struct_field_with_id("a", 1, [field_with_id("b", DataType::INTEGER, true, 2)]),
            field_with_id("v", DataType::unshredded_variant(), true, 3),
        ]);
        // Each category mirrors the table: `a` is a nested struct; `v` is a scalar (a variant's
        // real shape in a Delta stats schema -- a `LONG` in `nullCount`, never nested).
        let category = || {
            StructType::new_unchecked([
                StructField::nullable("id", DataType::LONG),
                StructField::nullable("a", stat_cols(["b"])),
                StructField::nullable("v", DataType::LONG),
            ])
        };
        let delta = delta_stats(Some(category()), Some(category()), Some(category()));

        let projected = projected_stats_schema(&table, &delta).expect("projected should succeed");
        // Every leaf keeps its full category-backed set. This differs from the unprojected
        // `stats_schema` only for the variant, whose `avg_value_size_in_bytes` (no backing Delta
        // category) is dropped under the projection.
        let expected = StructType::new_unchecked([
            expected_leaf("id", DataType::LONG, 0, &STAT_CATEGORIES),
            expected_leaf("a.b", DataType::INTEGER, 2, &STAT_CATEGORIES),
            expected_leaf("v", DataType::unshredded_variant(), 3, &STAT_CATEGORIES),
        ]);
        assert_eq!(projected, expected);
    }

    #[test]
    fn projected_drops_array_and_map_even_when_in_null_count() {
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            field_with_id(
                "arr",
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
                true,
                1,
            ),
            field_with_id(
                "m",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::INTEGER,
                    true,
                ))),
                true,
                2,
            ),
        ]);
        // All three columns appear in nullCount; array/map carry no leaf stats regardless.
        let delta = delta_stats(
            Some(stat_cols(["id", "arr", "m"])),
            Some(stat_cols(["id"])),
            Some(stat_cols(["id"])),
        );
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        let expected =
            StructType::new_unchecked([expected_leaf("id", DataType::LONG, 0, &STAT_CATEGORIES)]);
        assert_eq!(projected, expected);
    }

    /// Independently checks the category -> sub-field mapping: a surviving leaf keeps exactly the
    /// named sub-fields, in schema order, for the categories it appears in. Unlike the other
    /// projected tests (which build expectations via `build_stats_struct`), the expected sub-field
    /// names here are spelled out per case, so a wrong condition in `build_stats_struct` is caught.
    /// The leaf type varies to exercise the type-specific conditions (`nan_value_count`,
    /// `avg_value_size_in_bytes`). Column `c` is always field id 1.
    #[rstest]
    // Int: lower<-minValues, upper<-maxValues, tight_bounds<-either bound, value/null_value_count
    // <-nullCount.
    #[case::int_null_count(
        DataType::INTEGER,
        delta_stats(Some(stat_cols(["c"])), None, None),
        &[VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    #[case::int_min(
        DataType::INTEGER,
        delta_stats(None, Some(stat_cols(["c"])), None),
        &[LOWER_BOUND, TIGHT_BOUNDS],
    )]
    #[case::int_max(
        DataType::INTEGER,
        delta_stats(None, None, Some(stat_cols(["c"]))),
        &[UPPER_BOUND, TIGHT_BOUNDS],
    )]
    #[case::int_min_and_max(
        DataType::INTEGER,
        delta_stats(None, Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND, TIGHT_BOUNDS],
    )]
    #[case::int_all_categories(
        DataType::INTEGER,
        delta_stats(Some(stat_cols(["c"])), Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND, TIGHT_BOUNDS, VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    // Double: nan_value_count rides along with the bounds ...
    #[case::double_min_keeps_nan(
        DataType::DOUBLE,
        delta_stats(None, Some(stat_cols(["c"])), None),
        &[LOWER_BOUND, TIGHT_BOUNDS, NAN_VALUE_COUNT],
    )]
    // ... but is pruned when the leaf carries no bounds (nullCount only).
    #[case::double_null_count_drops_nan(
        DataType::DOUBLE,
        delta_stats(Some(stat_cols(["c"])), None, None),
        &[VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    // String/variant: avg_value_size_in_bytes has no backing category, so it is dropped under the
    // projection even when the leaf is in every category.
    #[case::string_all_drops_avg(
        DataType::STRING,
        delta_stats(Some(stat_cols(["c"])), Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND, TIGHT_BOUNDS, VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    #[case::variant_null_count_drops_avg(
        DataType::unshredded_variant(),
        delta_stats(Some(stat_cols(["c"])), None, None),
        &[VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    fn projected_leaf_keeps_only_category_backed_subfields(
        #[case] leaf_type: DataType,
        #[case] delta: StructType,
        #[case] expected_subfields: &[&str],
    ) {
        let table = StructType::new_unchecked([field_with_id("c", leaf_type, false, 1)]);
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        let stats = stats_struct_for_name("c", &projected);
        let names: Vec<&str> = stats.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, expected_subfields);
    }

    /// Nested-leaf membership is decided per leaf: a leaf survives (and is pruned) by its own
    /// category presence, independent of its siblings.
    #[rstest]
    // `a.c` via nullCount, `a.b` via minValues => both survive with their own pruned subsets.
    #[case::each_via_a_different_category(
        delta_stats(Some(nested_cat("a", ["c"])), Some(nested_cat("a", ["b"])), None),
        StructType::new_unchecked([
            expected_leaf("a.b", DataType::INTEGER, 2, &[MIN_VALUES]),
            expected_leaf("a.c", DataType::STRING, 3, &[NULL_COUNT]),
        ]),
    )]
    // Only `a.b` appears anywhere => `a.c` is dropped.
    #[case::sibling_absent_is_dropped(
        delta_stats(None, Some(nested_cat("a", ["b"])), None),
        StructType::new_unchecked([expected_leaf("a.b", DataType::INTEGER, 2, &[MIN_VALUES])]),
    )]
    fn projected_nested_leaf_membership_is_per_leaf(
        #[case] delta: StructType,
        #[case] expected: StructType,
    ) {
        // {a: struct{b: int, c: string}}
        let table = StructType::new_unchecked([struct_field_with_id(
            "a",
            1,
            [
                field_with_id("b", DataType::INTEGER, true, 2),
                field_with_id("c", DataType::STRING, true, 3),
            ],
        )]);
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(projected, expected);
    }

    #[test]
    fn projected_drops_entire_substruct_absent_from_all_categories() {
        // {a: struct{b: int}, d: long}; only `d` is present, so the whole `a` subtree is dropped.
        let table = StructType::new_unchecked([
            struct_field_with_id("a", 1, [field_with_id("b", DataType::INTEGER, true, 2)]),
            field_with_id("d", DataType::LONG, true, 3),
        ]);
        let delta = delta_stats(Some(stat_cols(["d"])), None, None);
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        let expected =
            StructType::new_unchecked([expected_leaf("d", DataType::LONG, 3, &[NULL_COUNT])]);
        assert_eq!(projected, expected);
    }

    #[test]
    fn projected_errors_when_category_node_is_scalar_where_table_nests_struct() {
        // {zzz: struct{b: int}} but `zzz` is a scalar in `nullCount`: the stats schema violates its
        // shape invariant relative to the table (a struct is nested there), so the walk errors
        // rather than silently dropping the subtree.
        let table = StructType::new_unchecked([struct_field_with_id(
            "zzz",
            1,
            [field_with_id("b", DataType::INTEGER, true, 2)],
        )]);
        let delta = delta_stats(Some(stat_cols(["zzz"])), None, None);
        let err = projected_stats_schema(&table, &delta)
            .expect_err("scalar-where-struct is an invariant violation")
            .to_string();
        assert!(
            err.contains("invariant violation"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("nullCount"),
            "error should name the category: {err}"
        );
        // `zzz` is distinctive, so this is not vacuously satisfied by other words in the message.
        assert!(err.contains("zzz"), "error should name the path: {err}");
    }

    /// A variant survives the projection exactly when present in some category, like any other
    /// leaf. When present only in `nullCount` (its usual shape) it keeps just the count
    /// sub-fields -- its bounds and `avg_value_size_in_bytes` have no backing category and are
    /// pruned.
    #[rstest]
    #[case::present_in_null_count(
        delta_stats(Some(stat_cols(["v"])), None, None),
        StructType::new_unchecked([expected_leaf("v", DataType::unshredded_variant(), 3, &[NULL_COUNT])]),
    )]
    #[case::absent_from_all(delta_stats(None, None, None), StructType::new_unchecked([]))]
    fn projected_variant_membership_matches_scalar_presence(
        #[case] delta: StructType,
        #[case] expected: StructType,
    ) {
        let table = StructType::new_unchecked([field_with_id(
            "v",
            DataType::unshredded_variant(),
            true,
            3,
        )]);
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(projected, expected);
    }

    /// A scalar-where-struct invariant violation is detected in any category and at any depth, not
    /// just `nullCount` at the root.
    #[rstest]
    #[case::mismatch_in_min_values(delta_stats(None, Some(stat_cols(["a"])), None))]
    #[case::mismatch_in_max_values(delta_stats(None, None, Some(stat_cols(["a"]))))]
    fn projected_errors_on_shape_mismatch_in_any_category(#[case] delta: StructType) {
        let table = StructType::new_unchecked([struct_field_with_id(
            "a",
            1,
            [field_with_id("b", DataType::INTEGER, true, 2)],
        )]);
        assert!(projected_stats_schema(&table, &delta).is_err());
    }

    #[test]
    fn projected_errors_on_nested_shape_mismatch() {
        // {a: struct{b: struct{c: int}}} but `a.b` is a scalar in `nullCount` (deeper than root).
        let table = StructType::new_unchecked([struct_field_with_id(
            "a",
            1,
            [struct_field_with_id(
                "b",
                2,
                [field_with_id("c", DataType::INTEGER, true, 3)],
            )],
        )]);
        let delta = delta_stats(Some(nested_cat("a", ["b"])), None, None);
        let err = projected_stats_schema(&table, &delta)
            .expect_err("nested scalar-where-struct is an invariant violation")
            .to_string();
        assert!(
            err.contains("invariant violation"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn projected_errors_when_category_header_is_scalar() {
        // A category header must be a struct mirroring the table; a scalar `nullCount` header is an
        // invariant violation caught at the root (in `from_delta_stats_schema`), consistent with
        // how nested descent treats a scalar-where-struct.
        let table = StructType::new_unchecked([field_with_id("c", DataType::INTEGER, false, 1)]);
        let delta = StructType::new_unchecked([StructField::nullable(NULL_COUNT, DataType::LONG)]);
        let err = projected_stats_schema(&table, &delta)
            .expect_err("scalar category header is an invariant violation")
            .to_string();
        assert!(
            err.contains("invariant violation"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("nullCount"),
            "error should name the category: {err}"
        );
    }

    /// Guards against fixture-vs-reality drift: drives the projection with the Delta stats schema
    /// the kernel really produces ([`expected_stats_schema`]) rather than a hand-built fixture,
    /// and asserts the exact pruned result. Primitives appear in all three categories (full
    /// set); the variant appears only in `nullCount`, so its bounds and size stat are pruned.
    #[test]
    fn projected_prunes_leaves_from_real_expected_stats_schema() {
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            field_with_id("s", DataType::STRING, true, 1),
            field_with_id("v", DataType::unshredded_variant(), true, 2),
        ]);
        let config = StatsConfig {
            data_skipping_stats_columns: None,
            data_skipping_num_indexed_cols: Some(DataSkippingNumIndexedCols::AllColumns),
        };
        let delta = expected_stats_schema(&table, &config, None, None).expect("stats schema");

        let projected = projected_stats_schema(&table, &delta).expect("projected should succeed");
        let expected = StructType::new_unchecked([
            expected_leaf("id", DataType::LONG, 0, &STAT_CATEGORIES),
            expected_leaf("s", DataType::STRING, 1, &STAT_CATEGORIES),
            expected_leaf("v", DataType::unshredded_variant(), 2, &[NULL_COUNT]),
        ]);
        assert_eq!(projected, expected);
    }

    #[test]
    fn projected_leaf_in_category_still_warn_dropped_when_out_of_range() {
        // Presence in a category does not bypass the supported-range check in `leaf_stats_field`.
        let table = StructType::new_unchecked([field_with_id(
            "c",
            DataType::INTEGER,
            false,
            MAX_DATA_FIELD_ID + 1,
        )]);
        let delta = delta_stats(Some(stat_cols(["c"])), None, None);
        let projected = projected_stats_schema(&table, &delta).expect("out-of-range warn-drops");
        assert_eq!(projected.fields().count(), 0);
    }

    #[test]
    fn projected_leaf_in_category_missing_field_id_still_errors() {
        // Presence in a category does not bypass the missing-field-id error either.
        let table = StructType::new_unchecked([StructField::not_null("c", DataType::INTEGER)]);
        let delta = delta_stats(Some(stat_cols(["c"])), None, None);
        assert!(projected_stats_schema(&table, &delta).is_err());
    }

    #[test]
    fn projected_restores_scope_for_sibling_struct_at_nested_level() {
        // {p: struct{x: struct{a}, y: struct{b}}}: `p.x.a` via nullCount, `p.y.b` via minValues.
        // `y` must descend from `p`'s saved scope -- not `x`'s child scope, not root -- so both
        // survive. This exercises restoration to a non-root saved scope, unlike the other tests.
        let table = StructType::new_unchecked([struct_field_with_id(
            "p",
            1,
            [
                struct_field_with_id("x", 2, [field_with_id("a", DataType::INTEGER, true, 3)]),
                struct_field_with_id("y", 4, [field_with_id("b", DataType::INTEGER, true, 5)]),
            ],
        )]);
        let null_count = StructType::new_unchecked([StructField::nullable(
            "p",
            StructType::new_unchecked([StructField::nullable("x", stat_cols(["a"]))]),
        )]);
        let min_values = StructType::new_unchecked([StructField::nullable(
            "p",
            StructType::new_unchecked([StructField::nullable("y", stat_cols(["b"]))]),
        )]);
        let delta = delta_stats(Some(null_count), Some(min_values), None);

        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        // `p.x.a` via nullCount, `p.y.b` via minValues -- each pruned to its own category.
        let expected = StructType::new_unchecked([
            expected_leaf("p.x.a", DataType::INTEGER, 3, &[NULL_COUNT]),
            expected_leaf("p.y.b", DataType::INTEGER, 5, &[MIN_VALUES]),
        ]);
        assert_eq!(projected, expected);
    }

    #[rstest]
    // A table column named like a stat category is matched one level deep (inside the category),
    // not against the top-level category header.
    #[case::leaf_inside_category(
        stat_cols(["nullCount"]),
        StructType::new_unchecked([expected_leaf("nullCount", DataType::LONG, 1, &[NULL_COUNT])]),
    )]
    #[case::only_the_category_header(stat_cols(["other"]), StructType::new_unchecked([]))]
    fn projected_handles_column_named_like_a_stat_category(
        #[case] null_count: StructType,
        #[case] expected: StructType,
    ) {
        let table =
            StructType::new_unchecked([field_with_id("nullCount", DataType::LONG, true, 1)]);
        let delta = delta_stats(Some(null_count), None, None);
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(projected, expected);
    }

    /// C1 contract: `stats_schema` emits a reserved-metadata leaf unconditionally, but the
    /// projection emits it only when the caller lists it in a category. The caller decides
    /// membership.
    #[rstest]
    #[case::included_survives(
        delta_stats(Some(stat_cols(["_row_id"])), None, None),
        StructType::new_unchecked([expected_leaf("_row_id", DataType::LONG, ROW_ID_FIELD_ID, &[NULL_COUNT])]),
    )]
    #[case::omitted_dropped(delta_stats(None, None, None), StructType::new_unchecked([]))]
    fn projected_reserved_metadata_leaf_requires_caller_inclusion(
        #[case] delta: StructType,
        #[case] expected: StructType,
    ) {
        let table = StructType::new_unchecked([field_with_id(
            "_row_id",
            DataType::LONG,
            true,
            ROW_ID_FIELD_ID,
        )]);
        // The unprojected path always emits the reserved-metadata leaf, regardless of the
        // projection.
        assert!(stats_schema(&table)
            .expect("stats_schema should succeed")
            .field("_row_id")
            .is_some());
        let projected = projected_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(projected, expected);
    }

    // == Delta -> AMT pivot tests ==

    /// A two-column physical table (`id: int`, `name: string`) with parquet field IDs 0 and 1.
    fn pivot_table_schema() -> StructType {
        StructType::new_unchecked([
            field_with_id("id", DataType::INTEGER, true, 0),
            field_with_id("name", DataType::STRING, true, 1),
        ])
    }

    /// A struct scalar from named fields; each field takes its type from its scalar value (so a
    /// nested category is passed as an already-built struct scalar).
    fn struct_scalar(fields: &[(&str, Scalar)]) -> Scalar {
        let schema = fields
            .iter()
            .map(|(name, v)| StructField::nullable(*name, v.data_type()))
            .collect();
        let values = fields.iter().map(|(_, v)| v.clone()).collect();
        Scalar::Struct(StructData::try_new(schema, values).expect("struct scalar"))
    }

    /// Runs the pivot for `table` over a one-row batch whose `stats` column is `stats` (its schema
    /// is taken from the scalar), returning the resulting `content_stats` struct array (`None`
    /// when the pivot leaves it null).
    fn run_pivot(table: &StructType, stats: Scalar) -> DeltaResult<Option<StructArray>> {
        let input_schema =
            StructType::new_unchecked([StructField::nullable("stats", stats.data_type())]);
        let engine = SyncEngine::new();
        let data = create_row(&engine, Arc::new(input_schema.clone()), stats).expect("create_row");
        Ok(
            try_preconvert_stats_column(&engine, data.as_ref(), "stats", table, &input_schema)?
                .map(stats_column),
        )
    }

    /// The `content_stats` (AMT) struct column of a batch whose stats column is named `stats`.
    fn stats_column(data: Box<dyn EngineData>) -> StructArray {
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)
            .expect("arrow engine data")
            .into();
        batch
            .column_by_name("stats")
            .expect("content_stats column")
            .as_struct()
            .clone()
    }

    /// The preconstructed expected `content_stats` struct array for `table`, built from each named
    /// leaf's sub-field values and round-tripped through the engine so it can be compared in bulk
    /// against the pivot output. Each leaf's sub-fields and their field-id metadata are taken from
    /// the AMT schema; any sub-field not listed defaults to a typed null.
    fn expected_stats_column(
        table: &StructType,
        leaves: &[(&str, Vec<(&str, Scalar)>)],
    ) -> StructArray {
        let amt = stats_schema(table).expect("stats schema");
        let leaf_scalar = |leaf_field: &StructField| {
            let DataType::Struct(stats) = leaf_field.data_type() else {
                panic!("AMT leaf must be a struct");
            };
            let subs = &leaves
                .iter()
                .find(|(name, _)| leaf_field.name() == name)
                .expect("expected value for every AMT leaf")
                .1;
            let values = stats
                .fields()
                .map(|f| {
                    subs.iter()
                        .find(|(name, _)| f.name() == name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| Scalar::Null(f.data_type().clone()))
                })
                .collect();
            let fields = stats.fields().cloned().collect();
            Scalar::Struct(StructData::try_new(fields, values).expect("leaf stats"))
        };
        let leaf_values = amt.fields().map(leaf_scalar).collect();
        let content_stats = Scalar::Struct(
            StructData::try_new(amt.fields().cloned().collect(), leaf_values)
                .expect("content_stats"),
        );
        let output_schema = StructType::new_unchecked([StructField::nullable("stats", amt)]);
        let engine = SyncEngine::new();
        let data = create_row(&engine, Arc::new(output_schema), content_stats).expect("create_row");
        stats_column(data)
    }

    #[test]
    fn is_delta_json_stats_schema_detects_num_records() {
        let with = StructType::new_unchecked([StructField::nullable(
            "stats",
            StructType::new_unchecked([StructField::nullable(NUM_RECORDS, DataType::LONG)]),
        )]);
        assert!(is_delta_json_stats_schema(&with, "stats"));
        // Missing stats column, non-struct column, and a struct without numRecords are all
        // rejected.
        assert!(!is_delta_json_stats_schema(&with, "absent"));
        let no_num = StructType::new_unchecked([StructField::nullable(
            "stats",
            StructType::new_unchecked([StructField::nullable(MIN_VALUES, DataType::INTEGER)]),
        )]);
        assert!(!is_delta_json_stats_schema(&no_num, "stats"));
    }

    #[test]
    fn has_nested_field_descends_structs() {
        let schema = StructType::new_unchecked([StructField::nullable(
            MIN_VALUES,
            StructType::new_unchecked([StructField::nullable("id", DataType::INTEGER)]),
        )]);
        assert!(has_nested_field(&schema, &[MIN_VALUES, "id"]));
        assert!(has_nested_field(&schema, &[MIN_VALUES]));
        assert!(has_nested_field(&schema, &[])); // empty path is trivially present
        assert!(!has_nested_field(&schema, &[MIN_VALUES, "missing"]));
        assert!(!has_nested_field(&schema, &[MAX_VALUES]));
    }

    #[test]
    fn pivot_expression_schema_matches_flat_stats_schema() {
        let table = pivot_table_schema();
        let (expr, amt_schema) =
            build_delta_to_amt_pivot_expression(&table, "stats", None).expect("pivot expr");
        // The pivot's output schema is exactly the (full) flat stats schema, spliced in via a
        // patch.
        assert_eq!(amt_schema, stats_schema(&table).expect("stats schema"));
        assert!(matches!(expr, Expression::StructPatch(_)));
    }

    /// End-to-end `tight_bounds` by leaf type with the file's `tightBounds = true`: truncatable
    /// types (string/binary/timestamp/timestamp_ntz) force `false`; exact types follow the file.
    #[rstest]
    #[case::string(DataType::STRING, false)]
    #[case::binary(DataType::BINARY, false)]
    #[case::timestamp(DataType::TIMESTAMP, false)]
    #[case::timestamp_ntz(DataType::TIMESTAMP_NTZ, false)]
    #[case::int(DataType::INTEGER, true)]
    #[case::long(DataType::LONG, true)]
    #[case::double(DataType::DOUBLE, true)]
    fn pivot_tight_bounds_by_leaf_type(#[case] leaf_type: DataType, #[case] expected_tight: bool) {
        let table = StructType::new_unchecked([field_with_id("c", leaf_type, true, 0)]);
        let stats = struct_scalar(&[
            (NUM_RECORDS, 4i64.into()),
            (DELTA_TIGHT_BOUNDS, true.into()),
        ]);
        let actual = run_pivot(&table, stats)
            .expect("pivot ok")
            .expect("pivot produced data");
        let expected = expected_stats_column(
            &table,
            &[(
                "c",
                vec![
                    (TIGHT_BOUNDS, expected_tight.into()),
                    (VALUE_COUNT, 4i64.into()),
                ],
            )],
        );
        assert_eq!(actual, expected);
    }

    /// Round-trips the flat two-column table. `tight_input` is the file's `tightBounds` (`None`
    /// absent, `Some(None)` a present-null cell, `Some(Some(b))` a value); `min_max` toggles the
    /// bound categories. Numeric `id` always resolves `tight_bounds = true` (file true, or the
    /// default when null/absent); string `name` is always forced false; counts come from
    /// `numRecords`/`nullCount`, and bounds are null when `min_max` is off.
    #[rstest]
    #[case::full_stats(Some(Some(true)), true)]
    #[case::null_tight_bounds_defaults_true(Some(None), true)]
    #[case::missing_min_max_nulls_bounds(None, false)]
    fn pivot_round_trip_maps_categories(
        #[case] tight_input: Option<Option<bool>>,
        #[case] min_max: bool,
    ) {
        let mut entries: Vec<(&str, Scalar)> = vec![(NUM_RECORDS, 10i64.into())];
        match tight_input {
            None => {}
            Some(None) => entries.push((DELTA_TIGHT_BOUNDS, Scalar::Null(DataType::BOOLEAN))),
            Some(Some(b)) => entries.push((DELTA_TIGHT_BOUNDS, b.into())),
        }
        if min_max {
            entries.push((
                MIN_VALUES,
                struct_scalar(&[("id", 1i32.into()), ("name", "aaa".into())]),
            ));
            entries.push((
                MAX_VALUES,
                struct_scalar(&[("id", 5i32.into()), ("name", "zzz".into())]),
            ));
        }
        entries.push((
            NULL_COUNT,
            struct_scalar(&[("id", 0i64.into()), ("name", 2i64.into())]),
        ));

        let table = pivot_table_schema();
        let actual = run_pivot(&table, struct_scalar(&entries))
            .expect("pivot ok")
            .expect("pivot produced data");

        let mut id = vec![
            (TIGHT_BOUNDS, true.into()),
            (VALUE_COUNT, 10i64.into()),
            (NULL_VALUE_COUNT, 0i64.into()),
        ];
        let mut name = vec![
            (TIGHT_BOUNDS, false.into()),
            (VALUE_COUNT, 10i64.into()),
            (NULL_VALUE_COUNT, 2i64.into()),
        ];
        if min_max {
            id.extend([(LOWER_BOUND, 1i32.into()), (UPPER_BOUND, 5i32.into())]);
            name.extend([(LOWER_BOUND, "aaa".into()), (UPPER_BOUND, "zzz".into())]);
        }
        let expected = expected_stats_column(&table, &[("id", id), ("name", name)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn pivot_round_trip_nested_leaf_reads_nested_source_path() {
        // Table a: { b: { c: int (field id 3) } }. The flat AMT leaf "a.b.c" is filled from the
        // nested Delta source columns at <stats>.<category>.a.b.c.
        let table = StructType::new_unchecked([StructField::nullable(
            "a",
            StructType::new_unchecked([StructField::nullable(
                "b",
                StructType::new_unchecked([field_with_id("c", DataType::INTEGER, true, 3)]),
            )]),
        )]);
        let nest = |c: Scalar| {
            struct_scalar(&[("a", struct_scalar(&[("b", struct_scalar(&[("c", c)]))]))])
        };
        let stats = struct_scalar(&[
            (NUM_RECORDS, 7i64.into()),
            (DELTA_TIGHT_BOUNDS, true.into()),
            (MIN_VALUES, nest(2i32.into())),
            (MAX_VALUES, nest(9i32.into())),
            (NULL_COUNT, nest(1i64.into())),
        ]);
        let actual = run_pivot(&table, stats)
            .expect("pivot ok")
            .expect("pivot produced data");
        let expected = expected_stats_column(
            &table,
            &[(
                "a.b.c",
                vec![
                    (LOWER_BOUND, 2i32.into()),
                    (UPPER_BOUND, 9i32.into()),
                    (TIGHT_BOUNDS, true.into()),
                    (VALUE_COUNT, 7i64.into()),
                    (NULL_VALUE_COUNT, 1i64.into()),
                ],
            )],
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn pivot_round_trip_variant_nulls_bounds_and_returns_some() {
        // A variant leaf's stats struct carries variant-typed lower_bound/upper_bound (from
        // `build_stats_struct`), and Delta records no variant bounds (variants appear only in
        // nullCount), so the pivot emits null for them -- a null variant scalar, which the
        // evaluator supports (see `null_variant_scalar_builds_null_struct_array`). The
        // table must therefore still produce content_stats (not be dropped), with variant
        // bounds null and the counts populated.
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::INTEGER, true, 0),
            field_with_id("v", DataType::unshredded_variant(), true, 1),
        ]);
        let stats = struct_scalar(&[
            (NUM_RECORDS, 10i64.into()),
            (
                NULL_COUNT,
                struct_scalar(&[("id", 0i64.into()), ("v", 3i64.into())]),
            ),
        ]);
        let actual = run_pivot(&table, stats)
            .expect("pivot ok")
            .expect("variant table must not be dropped");
        // The variant leaf keeps variant-typed bounds (both null, since Delta records no variant
        // bounds) and its counts; id has null bounds and a defaulted-true numeric tight_bounds.
        let expected = expected_stats_column(
            &table,
            &[
                (
                    "id",
                    vec![
                        (TIGHT_BOUNDS, true.into()),
                        (VALUE_COUNT, 10i64.into()),
                        (NULL_VALUE_COUNT, 0i64.into()),
                    ],
                ),
                (
                    "v",
                    vec![(VALUE_COUNT, 10i64.into()), (NULL_VALUE_COUNT, 3i64.into())],
                ),
            ],
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn pivot_build_error_on_missing_field_id_propagates() {
        // A table leaf lacking parquet.field.id violates the physical-schema precondition;
        // build errors must propagate as Err, not collapse to Ok(None).
        let table = StructType::new_unchecked([StructField::nullable("c", DataType::INTEGER)]);
        let stats = struct_scalar(&[
            (NUM_RECORDS, 10i64.into()),
            (MIN_VALUES, struct_scalar(&[("c", 2i32.into())])),
        ]);
        assert!(run_pivot(&table, stats).is_err());
    }
}
