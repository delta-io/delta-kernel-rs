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

use std::mem;
use std::sync::Arc;

use tracing::warn;

use crate::actions::{
    MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, STATS_PARSED,
    TIGHT_BOUNDS as DELTA_TIGHT_BOUNDS, TIGHT_STATS as DELTA_TIGHT_STATS,
};
use crate::content_tree::{
    CONTENT_STATS_FIELD_NAME, LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, RECORD_COUNT,
    TIGHT_STATS, TOTAL_BYTES, UPPER_BOUND, VALUE_COUNT,
};
use crate::expressions::{lit, null_lit, ColumnName, Expression, ExpressionRef};
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, SchemaRef, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{KernelError, KernelResult};

/// Field ID offsets for stats fields within a column's stats struct.
const STATS_OFFSET_LOWER_BOUND: i32 = 1;
const STATS_OFFSET_UPPER_BOUND: i32 = 2;
const STATS_OFFSET_VALUE_COUNT: i32 = 3;
const STATS_OFFSET_NULL_VALUE_COUNT: i32 = 4;
const STATS_OFFSET_NAN_VALUE_COUNT: i32 = 5;
const STATS_OFFSET_TOTAL_BYTES: i32 = 6;

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

    /// Category membership indexed like [`STAT_CATEGORIES`].
    fn as_array(&self) -> [bool; 3] {
        [self.null_count, self.min_values, self.max_values]
    }
}

/// Builds a single column's stats struct, with each stat sub-field's ID an offset from
/// `base_field_id`:
/// - offset 1/2: `lower_bound` / `upper_bound` (typed as `bounds_type`)
/// - offset 3: `value_count` (long)
/// - offset 4: `null_value_count` (long) - emitted regardless of the column's nullability
/// - offset 5: `nan_value_count` (long) - only for float/double `bounds_type`
/// - offset 6: `total_bytes` (long) - total uncompressed size of the column's values; for
///   string/binary `bounds_type`, or any variant
///
/// `bounds_type` is the type the bounds are recorded at: the column's own type for primitives, or
/// an unshredded variant type for variant columns.
///
/// `categories` restricts the sub-fields to the Delta stat categories the leaf appears in. `None`
/// (the unprojected path) keeps every type-eligible sub-field. When `Some`, a sub-field is kept
/// only when a category backing it is present: `lower_bound`<-`minValues`,
/// `upper_bound`<-`maxValues`, `value_count`/`null_value_count`<-`nullCount`,
/// `nan_value_count`<-either bound category. `total_bytes` has no
/// backing category and is dropped whenever projecting -- so `null_value_count`, though independent
/// of nullability, is still dropped when the leaf is absent from `nullCount`.
fn build_stats_struct(
    base_field_id: i32,
    bounds_type: &DataType,
    categories: Option<StatCategories>,
) -> StructType {
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
            TOTAL_BYTES,
            DataType::LONG,
            STATS_OFFSET_TOTAL_BYTES,
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
) -> KernelResult<Option<StructField>> {
    // Only leaves carry a field ID that matters for stats. A field ID that is absent, or present
    // but not `i32`-representable, is malformed => error. The spec limits which fields may carry
    // stats, so a field ID outside the supported range is expected for some reserved metadata
    // columns; skip (warn) rather than error in that case.
    let field_id = get_field_id(field).ok_or_else(|| {
        KernelError::generic(format!(
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
        return Err(KernelError::unsupported(format!(
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
        // Array/map/file columns carry no leaf stats.
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
    fn from_delta_stats_schema(delta_stats_schema: &'a StructType) -> KernelResult<Self> {
        let mut categories = [None; 3];
        for (i, category) in STAT_CATEGORIES.iter().enumerate() {
            categories[i] = match struct_sub_schema(delta_stats_schema, category) {
                SubSchema::Absent => None,
                SubSchema::Struct(s) => Some(s),
                SubSchema::Mismatch => {
                    return Err(KernelError::generic(format!(
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
    fn descend(&self, name: &str, path: &[String]) -> KernelResult<CategoryScopes<'a>> {
        let mut categories = [None; 3];
        for (i, scope) in self.categories.iter().enumerate() {
            categories[i] = match scope {
                // The category already omitted this subtree above; nothing to descend into.
                None => None,
                Some(s) => match struct_sub_schema(s, name) {
                    SubSchema::Absent => None,
                    SubSchema::Struct(sub) => Some(sub),
                    SubSchema::Mismatch => {
                        return Err(KernelError::generic(format!(
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

    /// Which categories a leaf named `leaf_name` appears in. Presence is a same-name field lookup:
    /// every leaf occupies one field per category it appears in, whatever that field's type (a
    /// variant is a scalar `LONG` in `nullCount`, and its own struct in the bound categories when
    /// the caller admitted it there).
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

/// A fold over the table schema driven by [`StatsLeafWalker`]: every surviving leaf and every
/// struct becomes an [`Output`](Self::Output), with a struct's output built from its children's.
/// `path` runs from the root to the visited field (its last segment is the field itself).
trait StatsLeafVisitor<'a> {
    type Output;

    /// Folds a non-struct leaf the projection does not drop. `categories` is the leaf's category
    /// membership, or `None` when not projecting.
    fn on_leaf(
        &self,
        field: &'a StructField,
        path: &[String],
        categories: Option<StatCategories>,
    ) -> KernelResult<Self::Output>;

    /// Folds a struct field from its surviving children's outputs, in field order. Called for every
    /// struct, so `children` is empty for an empty struct or one whose leaves the projection drops
    /// entirely. `projection` is the struct's own scope, or `None` when not projecting.
    fn on_struct(
        &self,
        field: &'a StructField,
        path: &[String],
        projection: Option<&CategoryScopes<'a>>,
        children: Vec<Self::Output>,
    ) -> KernelResult<Self::Output>;
}

/// Drives a [`StatsLeafVisitor`] fold over a table schema for AMT `content_stats` generation: it
/// descends structs, threads the optional Delta stat projection through each descent, and drops a
/// leaf that a projection omits from every category before the visitor sees it. Stat-eligibility
/// (field IDs, geospatial, array/map producing no stats) is left to the visitor.
///
/// Uses the `KernelResult<()>` carrier: the rebuilt schema is discarded, the folded outputs
/// accumulate in `outputs`, and an `Err` short-circuits the walk.
struct StatsLeafWalker<'a, 'v, V: StatsLeafVisitor<'a>> {
    /// Field names from the root to the current node; the last segment is the field being visited.
    path: Vec<String>,
    /// Delta stat categories in scope at the current position, or `None` to visit every leaf.
    projection: Option<CategoryScopes<'a>>,
    /// Outputs of the current struct's already-visited fields, in field order.
    outputs: Vec<V::Output>,
    visitor: &'v V,
}

impl<'a, 'v, V: StatsLeafVisitor<'a>> StatsLeafWalker<'a, 'v, V> {
    /// Folds `table_struct` with `visitor` under the optional Delta stat `projection`, returning
    /// the top-level fields' outputs in field order.
    ///
    /// Errors if the visitor errors, or if the projection is a scalar where the table nests a
    /// struct (see [`CategoryScopes::descend`]).
    fn walk(
        table_struct: &'a StructType,
        projection: Option<CategoryScopes<'a>>,
        visitor: &'v V,
    ) -> KernelResult<Vec<V::Output>> {
        let mut walker = Self {
            path: Vec::new(),
            projection,
            outputs: Vec::new(),
            visitor,
        };
        walker.transform_struct(table_struct)?;
        Ok(walker.outputs)
    }
}

impl<'a, V: StatsLeafVisitor<'a>> SchemaTransform<'a> for StatsLeafWalker<'a, '_, V> {
    transform_output_type!(|'a, T| KernelResult<()>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> KernelResult<()> {
        self.path.push(field.name().to_string());
        // Descend into structs; every other type is a leaf. On `Err` the walk aborts and `walk`
        // drops the walker, so the skipped `path` pop and state restores are harmless.
        if let DataType::Struct(_) = field.data_type() {
            // The stat-category scopes step into this struct's sub-schema, and the parent's scope
            // and sibling outputs are restored on the way back up. A category that is a scalar
            // where the table nests a struct (an internal invariant violation) aborts the walk.
            let saved_projection = self.projection;
            self.projection = self
                .projection
                .map(|s| s.descend(field.name(), &self.path))
                .transpose()?;
            let siblings = mem::take(&mut self.outputs);
            self.recurse_into_struct_field(field)?;
            let children = mem::replace(&mut self.outputs, siblings);
            let output =
                self.visitor
                    .on_struct(field, &self.path, self.projection.as_ref(), children)?;
            self.outputs.push(output);
            self.projection = saved_projection;
        } else {
            // Every non-struct type is a leaf -- including variants (never descended into: their
            // inner fields carry no field IDs) and array/map columns (values nested in a list or
            // map carry no `content_stats` entry, matching Iceberg). When projecting, a leaf
            // absent from every category is dropped here.
            let categories = self.projection.map(|s| s.leaf_categories(field.name()));
            if categories.is_none_or(|c| c.any()) {
                let output = self.visitor.on_leaf(field, &self.path, categories)?;
                self.outputs.push(output);
            }
        }
        self.path.pop();
        Ok(())
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
pub(crate) fn stats_schema(table_struct: &StructType) -> KernelResult<StructType> {
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
) -> KernelResult<StructType> {
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
) -> KernelResult<StructType> {
    let fields = StatsLeafWalker::walk(table_struct, projection, &StatsSchemaCollector)?;
    // `new_unchecked` skips name dedup; safe because `ColumnName`'s `Display` is lossless -- a leaf
    // whose name contains a dot is backtick-escaped, so it never collides with a nested path.
    Ok(StructType::new_unchecked(fields.into_iter().flatten()))
}

/// Folds each leaf into its flat `content_stats` field (see [`leaf_stats_field`]) and each struct
/// into its children's fields, so the walk yields the flat list.
struct StatsSchemaCollector;

impl<'a> StatsLeafVisitor<'a> for StatsSchemaCollector {
    type Output = Vec<StructField>;

    fn on_leaf(
        &self,
        field: &'a StructField,
        path: &[String],
        categories: Option<StatCategories>,
    ) -> KernelResult<Vec<StructField>> {
        Ok(leaf_stats_field(field, path, categories)?
            .into_iter()
            .collect())
    }

    fn on_struct(
        &self,
        _field: &'a StructField,
        _path: &[String],
        _projection: Option<&CategoryScopes<'a>>,
        children: Vec<Vec<StructField>>,
    ) -> KernelResult<Vec<StructField>> {
        Ok(children.into_iter().flatten().collect())
    }
}

// == AMT content_stats -> parsed Delta stats ==

/// Builds an expression that replaces the flat AMT [`CONTENT_STATS_FIELD_NAME`] column of
/// `input_schema` with a parsed Delta stats column named [`STATS_PARSED`], passing every other
/// column through.
///
/// Returns `(output_schema, expression)`: `output_schema` is `input_schema` with `content_stats`
/// replaced by a nullable `stats_parsed` column of type `delta_stats_schema`, and `expression`
/// evaluates rows of `input_schema` into `output_schema`.
///
/// The output column has the shape of `delta_stats_schema`:
///
/// ```text
/// numRecords:  long               <- recordCount
/// nullCount:   {<table nesting>}  <- content_stats.<leaf>.null_value_count
/// minValues:   {<table nesting>}  <- content_stats.<leaf>.lower_bound
/// maxValues:   {<table nesting>}  <- content_stats.<leaf>.upper_bound
/// tightBounds: boolean            <- coalesce(tightStats, false)
/// tightStats:  boolean            <- coalesce(tightStats, false)
/// ```
///
/// `content_stats` must have been read with [`projected_stats_schema`] for the same `table_schema`
/// and `delta_stats_schema`: both are produced by the same table walk, so leaf names always agree.
///
/// Both Delta tightness flags come from the entry's [`TIGHT_STATS`] column. AMT-only stats
/// (`value_count`, `nan_value_count`, `total_bytes`) are not carried over. A leaf without a
/// `content_stats` entry (array/map columns, out-of-range field IDs) yields null.
pub(crate) fn build_parsed_stats_expression(
    table_schema: &StructType,
    delta_stats_schema: &StructType,
    input_schema: &StructType,
) -> KernelResult<(SchemaRef, ExpressionRef)> {
    match input_schema
        .field(CONTENT_STATS_FIELD_NAME)
        .map(StructField::data_type)
    {
        Some(DataType::Struct(_)) => {}
        other => {
            return Err(KernelError::generic(format!(
                "content_stats column '{CONTENT_STATS_FIELD_NAME}' must be a struct in the input \
                 schema, found: {other:?}"
            )))
        }
    }
    for column in [RECORD_COUNT, TIGHT_STATS] {
        if input_schema.field(column).is_none() {
            return Err(KernelError::generic(format!(
                "AMT entry column '{column}' not found in input schema"
            )));
        }
    }

    let scopes = CategoryScopes::from_delta_stats_schema(delta_stats_schema)?;
    let root = StatsLeafWalker::walk(table_schema, Some(scopes), &ParsedStatsExprBuilder)?;
    let stats_expr = assemble_parsed_stats_expr(root, delta_stats_schema, &scopes)?;

    let output_field = StructField::nullable(STATS_PARSED, delta_stats_schema.clone());
    ProjectionStructPatchBuilder::new(input_schema)
        .replace(CONTENT_STATS_FIELD_NAME, output_field, Arc::new(stats_expr))
        .build()
}

/// The `content_stats` leaf sub-field backing each category, indexed like [`STAT_CATEGORIES`].
const CATEGORY_SUB_FIELDS: [&str; 3] = [NULL_VALUE_COUNT, LOWER_BOUND, UPPER_BOUND];

/// A built child expression and the name of the output field it fills.
type NamedExpr<'a> = (&'a str, ExpressionRef);

/// One folded field of the parsed Delta stats expression.
struct StatsExprNode<'a> {
    /// The field's name in each category's output struct.
    name: &'a str,
    /// The field's expression in each category it appears in, indexed like [`STAT_CATEGORIES`].
    exprs: [Option<ExpressionRef>; 3],
}

/// [`StatsLeafVisitor`] that builds the parsed Delta stats expression. Each leaf's `content_stats`
/// entry comes from [`leaf_stats_field`], the same function that names it in the read schema.
struct ParsedStatsExprBuilder;

impl<'a> StatsLeafVisitor<'a> for ParsedStatsExprBuilder {
    type Output = StatsExprNode<'a>;

    fn on_leaf(
        &self,
        field: &'a StructField,
        path: &[String],
        categories: Option<StatCategories>,
    ) -> KernelResult<StatsExprNode<'a>> {
        let categories = require_projection(categories)?;
        // The read schema keeps exactly the sub-fields backed by the leaf's categories, so the
        // entry has every sub-field referenced below.
        let entry = leaf_stats_field(field, path, Some(categories))?;
        let present = categories.as_array();
        let exprs = std::array::from_fn(|i| {
            present[i].then(|| {
                let sub_field = CATEGORY_SUB_FIELDS[i];
                let expr = match &entry {
                    Some(entry) => {
                        Expression::column([CONTENT_STATS_FIELD_NAME, entry.name(), sub_field])
                    }
                    None if sub_field == NULL_VALUE_COUNT => null_lit(DataType::LONG),
                    None => null_lit(field.data_type().clone()),
                };
                Arc::new(expr)
            })
        });
        Ok(StatsExprNode {
            name: field.name(),
            exprs,
        })
    }

    fn on_struct(
        &self,
        field: &'a StructField,
        path: &[String],
        projection: Option<&CategoryScopes<'a>>,
        children: Vec<StatsExprNode<'a>>,
    ) -> KernelResult<StatsExprNode<'a>> {
        let exprs = combine_children(children, require_projection(projection)?, path)?;
        Ok(StatsExprNode {
            name: field.name(),
            exprs: exprs.map(|expr| expr.map(Arc::new)),
        })
    }
}

/// Assembles the top-level stats struct expression from the walk's `root` outputs, field for field
/// with `delta_stats_schema`.
fn assemble_parsed_stats_expr(
    root: Vec<StatsExprNode<'_>>,
    delta_stats_schema: &StructType,
    scopes: &CategoryScopes<'_>,
) -> KernelResult<Expression> {
    let mut categories = combine_children(root, scopes, &[])?;
    let mut fields = Vec::new();
    for field in delta_stats_schema.fields() {
        let name = field.name().as_str();
        let expr = match name {
            NUM_RECORDS => Expression::column([RECORD_COUNT]),
            // An absent `tight_stats` means not tight, while Delta reads an absent `tightBounds`
            // as tight, so a null flag must become an explicit `false`.
            DELTA_TIGHT_BOUNDS | DELTA_TIGHT_STATS => {
                Expression::coalesce([Expression::column([TIGHT_STATS]), lit(false)])
            }
            _ => STAT_CATEGORIES
                .iter()
                .position(|category| *category == name)
                .and_then(|i| categories[i].take())
                .ok_or_else(|| {
                    KernelError::generic(format!("unexpected top-level Delta stats field '{name}'"))
                })?,
        };
        fields.push(Arc::new(expr));
    }
    Ok(Expression::struct_from(fields))
}

/// Combines a struct's `children` into one struct expression per stat category. `path` locates the
/// struct for error context.
///
/// Returns expressions indexed like [`STAT_CATEGORIES`]: entry `i` is a struct over the children
/// present in category `i`, or `None` when `scopes` omits that category here (so no leaf under the
/// struct is in it).
///
/// For example, for `s: struct{a: int, b: string}` with `a` in `nullCount` and `minValues`, `b`
/// only in `nullCount`, and `s` absent from `maxValues`:
///
/// ```text
/// exprs[nullCount] = Some(struct(a: content_stats.`s.a`.null_value_count,
///                                b: content_stats.`s.b`.null_value_count))
/// exprs[minValues] = Some(struct(a: content_stats.`s.a`.lower_bound))
/// exprs[maxValues] = None
/// ```
///
/// Errors if a category's children don't line up with its sub-schema (see [`aligned_struct_expr`]).
fn combine_children<'a>(
    children: Vec<StatsExprNode<'a>>,
    scopes: &CategoryScopes<'_>,
    path: &[String],
) -> KernelResult<[Option<Expression>; 3]> {
    let mut per_category: [Vec<NamedExpr<'a>>; 3] = Default::default();
    for child in children {
        for (category, expr) in per_category.iter_mut().zip(child.exprs) {
            category.extend(expr.map(|expr| (child.name, expr)));
        }
    }
    let mut exprs = [None, None, None];
    for (i, children) in per_category.into_iter().enumerate() {
        if let Some(scope) = scopes.categories[i] {
            exprs[i] = Some(aligned_struct_expr(children, scope, path, i)?);
        }
    }
    Ok(exprs)
}

/// Unwraps projection-derived visitor input, which the parsed stats walk always supplies.
fn require_projection<T>(projection: Option<T>) -> KernelResult<T> {
    projection.ok_or_else(|| KernelError::internal_error("parsed stats walk requires a projection"))
}

/// Wraps a struct's `children` into a struct expression for the category sub-schema `scope`.
/// `path` and `category` are for error context.
///
/// Errors if the children's names don't match `scope`'s fields, in order.
fn aligned_struct_expr(
    children: Vec<NamedExpr<'_>>,
    scope: &StructType,
    path: &[String],
    category: usize,
) -> KernelResult<Expression> {
    // The evaluator matches struct fields by position, so the children must line up with `scope`.
    // Both come from the same table walk, so a mismatch is an internal error.
    let child_names = || children.iter().map(|(name, _)| *name);
    if !child_names().eq(scope.field_names().map(String::as_str)) {
        return Err(KernelError::internal_error(format!(
            "Delta stats category '{}' at '{}' has fields {:?}, but the table walk produced {:?}",
            STAT_CATEGORIES[category],
            ColumnName::new(path),
            scope.field_names().collect::<Vec<_>>(),
            child_names().collect::<Vec<_>>(),
        )));
    }
    Ok(Expression::struct_from(
        children.into_iter().map(|(_, expr)| expr),
    ))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, StructData};
    use crate::scan::data_skipping::stats_schema::{expected_stats_schema, StatsConfig};
    use crate::schema::{ArrayType, MapType};
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};
    use crate::table_properties::DataSkippingNumIndexedCols;
    use crate::unit_test_utils::assert_result_error_with_message;
    use crate::{Engine, EngineData};

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
    /// `null_value_count` is always present. Variants always carry the size stat.
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
            assert_offset(TOTAL_BYTES, STATS_OFFSET_TOTAL_BYTES);
        }
        assert_offset(LOWER_BOUND, STATS_OFFSET_LOWER_BOUND);
        assert_offset(UPPER_BOUND, STATS_OFFSET_UPPER_BOUND);
    }

    // `null_value_count` is always present, so counts include it regardless of nullability.
    #[rstest]
    #[case(DataType::INTEGER, false, 1, 10_200, 4)] // fixed-length
    #[case(DataType::STRING, true, 2, 10_400, 5)] // size stats
    #[case(DataType::DOUBLE, true, 5, 11_000, 5)] // nan count
    #[case(DataType::FLOAT, false, 100, 30_000, 5)] // nan count
    #[case(DataType::LONG, true, 42, 18_400, 4)] // fixed-length
    #[case(DataType::BINARY, true, 3, 10_600, 5)] // size stats
    #[case(DataType::BINARY, false, 4, 10_800, 5)] // size stats
    #[case(
        DataType::INTEGER,
        false,
        MAX_DATA_FIELD_ID,
        MAX_DATA_STATS_FIELD_ID,
        4
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

        // `b` is a not-null int; it still carries a null_value_count (always present), so 4
        // sub-fields (lower, upper, value, null).
        let b_stats = stats_struct_for_name("a.b", &stats);
        assert_eq!(b_stats.fields().count(), 4);
        assert_eq!(get_field_id(stats.field("a.b").unwrap()), Some(10_400));
        assert_stats_field_ids(&b_stats, 10_400, &field_b);

        let c_stats = stats_struct_for_name("a.c", &stats);
        assert_eq!(c_stats.fields().count(), 5); // null + nan count
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
        // Not-null int; null_value_count is always present, so 4 sub-fields.
        assert_eq!(c_stats.fields().count(), 4);
        assert!(c_stats.field(VALUE_COUNT).is_some());
        assert!(c_stats.field(LOWER_BOUND).is_some());
        assert!(c_stats.field(NULL_VALUE_COUNT).is_some());
        assert_eq!(get_field_id(stats.field("a.b.c").unwrap()), Some(10_600));
    }

    /// Both unshredded and shredded variant inputs produce the same ordinary stats struct: the
    /// bounds are recorded as unshredded variants and the size stat is present. The physical inner
    /// layout (e.g. a shredded `typed_value`) does not affect the generated stats.
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
        // Variants always include the size stat.
        assert!(v_stats.field(TOTAL_BYTES).is_some());
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
        // `stats_schema` only for the variant, whose `total_bytes` (no backing Delta
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
    /// `total_bytes`). Column `c` is always field id 1.
    #[rstest]
    // Int: lower<-minValues, upper<-maxValues, value/null_value_count<-nullCount.
    #[case::int_null_count(
        DataType::INTEGER,
        delta_stats(Some(stat_cols(["c"])), None, None),
        &[VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    #[case::int_min(
        DataType::INTEGER,
        delta_stats(None, Some(stat_cols(["c"])), None),
        &[LOWER_BOUND],
    )]
    #[case::int_max(
        DataType::INTEGER,
        delta_stats(None, None, Some(stat_cols(["c"]))),
        &[UPPER_BOUND],
    )]
    #[case::int_min_and_max(
        DataType::INTEGER,
        delta_stats(None, Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND],
    )]
    #[case::int_all_categories(
        DataType::INTEGER,
        delta_stats(Some(stat_cols(["c"])), Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND, VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    // Double: nan_value_count rides along with the bounds ...
    #[case::double_min_keeps_nan(
        DataType::DOUBLE,
        delta_stats(None, Some(stat_cols(["c"])), None),
        &[LOWER_BOUND, NAN_VALUE_COUNT],
    )]
    // ... but is pruned when the leaf carries no bounds (nullCount only).
    #[case::double_null_count_drops_nan(
        DataType::DOUBLE,
        delta_stats(Some(stat_cols(["c"])), None, None),
        &[VALUE_COUNT, NULL_VALUE_COUNT],
    )]
    // String/variant: total_bytes has no backing category, so it is dropped under the
    // projection even when the leaf is in every category.
    #[case::string_all_drops_total_bytes(
        DataType::STRING,
        delta_stats(Some(stat_cols(["c"])), Some(stat_cols(["c"])), Some(stat_cols(["c"]))),
        &[LOWER_BOUND, UPPER_BOUND, VALUE_COUNT, NULL_VALUE_COUNT],
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
    /// sub-fields -- its bounds and `total_bytes` have no backing category and are
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
            ..Default::default()
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

    // === StatsLeafWalker ===

    /// Renders the walk as `name` per leaf and `name<scope>{children}` per struct, where `<scope>`
    /// is the struct's `[nullCount|minValues|maxValues]` field names (`-` for an omitted category),
    /// or empty when not projecting. Records each callback's path and fails at `fail_at`.
    #[derive(Default)]
    struct TreeRenderer {
        visited: RefCell<Vec<String>>,
        fail_at: Option<&'static str>,
    }

    impl TreeRenderer {
        fn visit(&self, path: &[String]) -> KernelResult<()> {
            let path = ColumnName::new(path).to_string();
            self.visited.borrow_mut().push(path.clone());
            if self.fail_at == Some(path.as_str()) {
                return Err(KernelError::generic(format!(
                    "injected failure at '{path}'"
                )));
            }
            Ok(())
        }
    }

    impl<'a> StatsLeafVisitor<'a> for TreeRenderer {
        type Output = String;

        fn on_leaf(
            &self,
            field: &'a StructField,
            path: &[String],
            _categories: Option<StatCategories>,
        ) -> KernelResult<String> {
            self.visit(path)?;
            Ok(field.name().clone())
        }

        fn on_struct(
            &self,
            field: &'a StructField,
            path: &[String],
            projection: Option<&CategoryScopes<'a>>,
            children: Vec<String>,
        ) -> KernelResult<String> {
            self.visit(path)?;
            let scope = projection.map_or(String::new(), |scopes| {
                let names = scopes.categories.map(|scope| {
                    scope.map_or("-".to_string(), |s| {
                        s.field_names().cloned().collect::<Vec<_>>().join(",")
                    })
                });
                format!("[{}]", names.join("|"))
            });
            Ok(format!("{}{scope}{{{}}}", field.name(), children.join(",")))
        }
    }

    /// `p: struct{x: struct{a: int}, y: struct{b: int}}`.
    fn sibling_structs() -> StructField {
        let child = |name: &str, leaf: &str| {
            StructField::nullable(
                name,
                StructType::new_unchecked([StructField::nullable(leaf, DataType::INTEGER)]),
            )
        };
        StructField::nullable(
            "p",
            StructType::new_unchecked([child("x", "a"), child("y", "b")]),
        )
    }

    #[rstest]
    #[case::nested_and_sibling_structs(
        vec![
            sibling_structs(),
            StructField::nullable("arr", ArrayType::new(DataType::INTEGER, true)),
            StructField::nullable("m", MapType::new(DataType::STRING, DataType::INTEGER, true)),
            StructField::nullable("d", DataType::LONG),
        ],
        None,
        &["p{x{a},y{b}}", "arr", "m", "d"],
    )]
    #[case::empty_struct(
        vec![StructField::nullable("e", StructType::new_unchecked([]))],
        None,
        &["e{}"],
    )]
    #[case::fully_dropped_struct(
        vec![
            StructField::nullable("a", stat_cols(["b"])),
            StructField::nullable("d", DataType::LONG),
        ],
        Some(delta_stats(Some(stat_cols(["d"])), None, None)),
        &["a[-|-|-]{}", "d"],
    )]
    #[case::scope_is_structs_own(
        vec![sibling_structs()],
        Some(delta_stats(
            Some(StructType::new_unchecked([StructField::nullable("p", nested_cat("x", ["a"]))])),
            Some(StructType::new_unchecked([StructField::nullable("p", nested_cat("y", ["b"]))])),
            None,
        )),
        &["p[x|y|-]{x[a|-|-]{a},y[-|b|-]{b}}"],
    )]
    fn walker_folds_structs_and_leaves(
        #[case] table: Vec<StructField>,
        #[case] delta_stats_schema: Option<StructType>,
        #[case] expected: &[&str],
    ) {
        let table = StructType::new_unchecked(table);
        let projection = delta_stats_schema
            .as_ref()
            .map(|s| CategoryScopes::from_delta_stats_schema(s).unwrap());
        let rendered = StatsLeafWalker::walk(&table, projection, &TreeRenderer::default()).unwrap();
        assert_eq!(rendered, expected);
    }

    #[rstest]
    #[case::leaf_hook("p.x.a", &["p.x.a"])]
    #[case::struct_hook("p.x", &["p.x.a", "p.x"])]
    fn walker_error_stops_later_callbacks(
        #[case] fail_at: &'static str,
        #[case] expected_visited: &[&str],
    ) {
        let table = StructType::new_unchecked([sibling_structs()]);
        let renderer = TreeRenderer {
            fail_at: Some(fail_at),
            ..Default::default()
        };
        let err = StatsLeafWalker::walk(&table, None, &renderer).unwrap_err();
        assert!(err
            .to_string()
            .contains(&format!("injected failure at '{fail_at}'")));
        assert_eq!(*renderer.visited.borrow(), expected_visited);
    }

    // == AMT content_stats -> parsed Delta stats ==

    /// Builds a `Scalar::Struct` of `values` typed by `schema` (values in field order).
    fn struct_scalar(schema: &StructType, values: Vec<Scalar>) -> Scalar {
        Scalar::Struct(StructData::try_new(schema.fields().cloned().collect(), values).unwrap())
    }

    /// The single-column `content_stats` leaf struct for `name` in `content_stats_schema`, filled
    /// with `sub_values` in the leaf's sub-field order (see [`build_stats_struct`]).
    fn leaf_scalar(
        content_stats_schema: &StructType,
        name: &str,
        sub_values: Vec<Scalar>,
    ) -> Scalar {
        struct_scalar(
            &stats_struct_for_name(name, content_stats_schema),
            sub_values,
        )
    }

    /// Converts an `EngineData` to its backing Arrow `RecordBatch` for comparison.
    fn to_batch(data: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(data)
            .unwrap()
            .record_batch()
            .clone()
    }

    /// Evaluates [`build_parsed_stats_expression`] over `manifest_data`.
    fn convert_amt_stats_to_delta_stats(
        engine: &dyn Engine,
        manifest_data: &dyn EngineData,
        table_schema: &StructType,
        delta_stats_schema: &StructType,
        input_schema: &StructType,
    ) -> KernelResult<Box<dyn EngineData>> {
        let (output_schema, expr) =
            build_parsed_stats_expression(table_schema, delta_stats_schema, input_schema)?;
        engine
            .evaluation_handler()
            .new_expression_evaluator(Arc::new(input_schema.clone()), expr, output_schema.into())?
            .evaluate(manifest_data)
    }

    /// The input schema the conversion reads: `content_stats` read with [`projected_stats_schema`],
    /// plus the entry's `recordCount` and `tightStats` columns.
    fn parsed_stats_input_schema(table: &StructType, delta: &StructType) -> StructType {
        StructType::new_unchecked([
            StructField::nullable(
                CONTENT_STATS_FIELD_NAME,
                projected_stats_schema(table, delta).expect("projected schema"),
            ),
            StructField::nullable(RECORD_COUNT, DataType::LONG),
            StructField::nullable(TIGHT_STATS, DataType::BOOLEAN),
        ])
    }

    /// Converts a single input row (`content_stats`, `recordCount`, `tightStats`) and asserts the
    /// produced batch equals one built directly from `expected_stats` (the Delta stats struct
    /// scalar) and the same passthrough columns. Uses the real evaluator end to end.
    fn assert_parsed_stats(
        table: &StructType,
        delta: &StructType,
        content_stats: Scalar,
        record_count: i64,
        tight_stats: Option<bool>,
        expected_stats: Scalar,
    ) {
        let input_schema = parsed_stats_input_schema(table, delta);
        let engine = SyncEngine::new();
        let handler = engine.evaluation_handler();
        let tight_stats = tight_stats.map_or(Scalar::Null(DataType::BOOLEAN), Scalar::Boolean);
        let input = handler
            .create_many(
                Arc::new(input_schema.clone()),
                vec![vec![
                    content_stats,
                    Scalar::Long(record_count),
                    tight_stats.clone(),
                ]],
            )
            .unwrap();

        let (output_schema, _) =
            build_parsed_stats_expression(table, delta, &input_schema).unwrap();
        let expected = handler
            .create_many(
                output_schema,
                vec![vec![
                    expected_stats,
                    Scalar::Long(record_count),
                    tight_stats,
                ]],
            )
            .unwrap();

        let actual =
            convert_amt_stats_to_delta_stats(&engine, input.as_ref(), table, delta, &input_schema)
                .unwrap();

        assert_eq!(to_batch(actual), to_batch(expected));
    }

    /// A nested Delta stats schema with `numRecords`, the given category sub-structs,
    /// `tightBounds`, and `tightStats`. Mirrors the canonical Delta stats shape for building parsed
    /// stats targets.
    fn delta_stats_with_bounds(
        null_count: StructType,
        min_values: StructType,
        max_values: StructType,
    ) -> StructType {
        StructType::new_unchecked([
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(NULL_COUNT, null_count),
            StructField::nullable(MIN_VALUES, min_values),
            StructField::nullable(MAX_VALUES, max_values),
            StructField::nullable(DELTA_TIGHT_BOUNDS, DataType::BOOLEAN),
            StructField::nullable(DELTA_TIGHT_STATS, DataType::BOOLEAN),
        ])
    }

    #[test]
    fn parsed_stats_maps_categories_and_record_count() {
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, true, 0),
            field_with_id("score", DataType::LONG, true, 1),
        ]);
        let delta = delta_stats_with_bounds(
            stat_cols(["id", "score"]),
            stat_cols(["id", "score"]),
            stat_cols(["id", "score"]),
        );
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        // Leaf sub-field order (LONG, all categories): lower bound, upper bound, value count, null
        // value count.
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![
                leaf_scalar(
                    &content_stats_schema,
                    "id",
                    vec![
                        Scalar::Long(1),
                        Scalar::Long(10),
                        Scalar::Long(100),
                        Scalar::Long(5),
                    ],
                ),
                leaf_scalar(
                    &content_stats_schema,
                    "score",
                    vec![
                        Scalar::Long(20),
                        Scalar::Long(99),
                        Scalar::Long(100),
                        Scalar::Long(4),
                    ],
                ),
            ],
        );

        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(100), // numRecords <- recordCount
                struct_scalar(
                    &stat_cols(["id", "score"]),
                    vec![Scalar::Long(5), Scalar::Long(4)],
                ),
                struct_scalar(
                    &stat_cols(["id", "score"]),
                    vec![Scalar::Long(1), Scalar::Long(20)],
                ),
                struct_scalar(
                    &stat_cols(["id", "score"]),
                    vec![Scalar::Long(10), Scalar::Long(99)],
                ),
                Scalar::Boolean(true), // tightBounds <- tightStats
                Scalar::Boolean(true), // tightStats <- tightStats
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 100, Some(true), expected);
    }

    #[test]
    fn parsed_stats_nested_leaf_rebuilds_nested_path() {
        // Table a:{b: long}; the flat content_stats leaf is keyed by the dotted path "a.b".
        let table = StructType::new_unchecked([struct_field_with_id(
            "a",
            9,
            [field_with_id("b", DataType::LONG, true, 0)],
        )]);
        let delta = delta_stats_with_bounds(
            nested_cat("a", ["b"]),
            nested_cat("a", ["b"]),
            nested_cat("a", ["b"]),
        );
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![leaf_scalar(
                &content_stats_schema,
                "a.b",
                vec![
                    Scalar::Long(3),
                    Scalar::Long(7),
                    Scalar::Long(50),
                    Scalar::Long(2),
                ],
            )],
        );

        let nested = |v: Scalar| {
            struct_scalar(
                &nested_cat("a", ["b"]),
                vec![struct_scalar(&stat_cols(["b"]), vec![v])],
            )
        };
        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(50),
                nested(Scalar::Long(2)),
                nested(Scalar::Long(3)),
                nested(Scalar::Long(7)),
                Scalar::Boolean(true),
                Scalar::Boolean(true),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 50, Some(true), expected);
    }

    #[test]
    fn parsed_stats_array_in_null_count_reconstructs_null_long() {
        // Delta records nullCount (but no bounds) for array columns. AMT keeps no content_stats
        // entry for the array itself, so the conversion emits a null LONG there, which also keeps
        // the nullCount struct field count aligned.
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, true, 0),
            field_with_id(
                "arr",
                DataType::from(ArrayType::new(DataType::LONG, true)),
                true,
                1,
            ),
        ]);
        let delta = delta_stats_with_bounds(
            stat_cols(["id", "arr"]), // nullCount includes the array
            stat_cols(["id"]),        // min/max exclude it
            stat_cols(["id"]),
        );
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        // Only "id" has a content_stats entry; "arr" is dropped.
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![leaf_scalar(
                &content_stats_schema,
                "id",
                vec![
                    Scalar::Long(2),
                    Scalar::Long(9),
                    Scalar::Long(10),
                    Scalar::Long(1),
                ],
            )],
        );

        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(10),
                struct_scalar(
                    &stat_cols(["id", "arr"]),
                    vec![Scalar::Long(1), Scalar::Null(DataType::LONG)],
                ),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(2)]),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(9)]),
                Scalar::Boolean(true),
                Scalar::Boolean(true),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 10, Some(true), expected);
    }

    /// Both Delta tightness flags follow the entry's `tightStats`, with an absent flag read as
    /// not tight.
    #[rstest]
    #[case::tight(Some(true), true)]
    #[case::wide(Some(false), false)]
    #[case::absent(None, false)]
    fn parsed_stats_tightness_follows_entry_tight_stats(
        #[case] tight_stats: Option<bool>,
        #[case] expected_tight: bool,
    ) {
        let table = StructType::new_unchecked([field_with_id("id", DataType::LONG, true, 0)]);
        let delta =
            delta_stats_with_bounds(stat_cols(["id"]), stat_cols(["id"]), stat_cols(["id"]));
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![leaf_scalar(
                &content_stats_schema,
                "id",
                vec![
                    Scalar::Long(1),
                    Scalar::Long(2),
                    Scalar::Long(3),
                    Scalar::Long(1),
                ],
            )],
        );

        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(3),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(1)]),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(1)]),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(2)]),
                Scalar::Boolean(expected_tight),
                Scalar::Boolean(expected_tight),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 3, tight_stats, expected);
    }

    #[test]
    fn parsed_stats_struct_only_in_null_count_is_omitted_from_bounds() {
        // Boolean is not min/max eligible, so struct `a` appears in nullCount but in neither bound
        // category, which must not gain an entry for it.
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, true, 0),
            struct_field_with_id("a", 9, [field_with_id("b", DataType::BOOLEAN, true, 1)]),
        ]);
        let null_cols = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("a", stat_cols(["b"])),
        ]);
        let delta =
            delta_stats_with_bounds(null_cols.clone(), stat_cols(["id"]), stat_cols(["id"]));
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        // "a.b" is only in nullCount, so it keeps just value count and null value count.
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![
                leaf_scalar(
                    &content_stats_schema,
                    "id",
                    vec![
                        Scalar::Long(1),
                        Scalar::Long(9),
                        Scalar::Long(10),
                        Scalar::Long(2),
                    ],
                ),
                leaf_scalar(
                    &content_stats_schema,
                    "a.b",
                    vec![Scalar::Long(10), Scalar::Long(3)],
                ),
            ],
        );

        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(10),
                struct_scalar(
                    &null_cols,
                    vec![
                        Scalar::Long(2),
                        struct_scalar(&stat_cols(["b"]), vec![Scalar::Long(3)]),
                    ],
                ),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(1)]),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(9)]),
                Scalar::Boolean(false),
                Scalar::Boolean(false),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 10, Some(false), expected);
    }

    #[test]
    fn parsed_stats_projected_schema_emits_only_present_categories() {
        // A Delta stats schema carrying only numRecords + nullCount: the output has exactly those
        // two fields, and neither tightness flag is emitted.
        let table = StructType::new_unchecked([field_with_id("id", DataType::LONG, true, 0)]);
        let delta = StructType::new_unchecked([
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(NULL_COUNT, stat_cols(["id"])),
        ]);
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        // A leaf only in nullCount keeps value count and null value count (order per
        // build_stats_struct).
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![leaf_scalar(
                &content_stats_schema,
                "id",
                vec![Scalar::Long(7), Scalar::Long(2)],
            )],
        );
        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(7),
                struct_scalar(&stat_cols(["id"]), vec![Scalar::Long(2)]),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 7, Some(true), expected);
    }

    #[test]
    fn parsed_stats_variant_null_count_only() {
        // Variants carry stats only in nullCount today. The leaf's projected content_stats struct
        // is just value count and null value count.
        let table = StructType::new_unchecked([field_with_id(
            "v",
            DataType::unshredded_variant(),
            true,
            0,
        )]);
        let delta = StructType::new_unchecked([
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(NULL_COUNT, stat_cols(["v"])),
        ]);
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        let content_stats = struct_scalar(
            &content_stats_schema,
            vec![leaf_scalar(
                &content_stats_schema,
                "v",
                vec![Scalar::Long(8), Scalar::Long(3)],
            )],
        );
        let expected = struct_scalar(
            &delta,
            vec![
                Scalar::Long(8),
                struct_scalar(&stat_cols(["v"]), vec![Scalar::Long(3)]),
            ],
        );

        assert_parsed_stats(&table, &delta, content_stats, 8, Some(true), expected);
    }

    /// `content_stats_col` names the input's stats column; `with_record_count` and
    /// `with_tight_stats` control whether the input schema carries the required [`RECORD_COUNT`]
    /// and [`TIGHT_STATS`] entry columns.
    #[rstest]
    #[case::content_stats_missing("other_stats", true, true, "must be a struct")]
    #[case::record_count_missing(CONTENT_STATS_FIELD_NAME, false, true, "'recordCount' not found")]
    #[case::tight_stats_missing(CONTENT_STATS_FIELD_NAME, true, false, "'tightStats' not found")]
    fn parsed_stats_errors_on_bad_columns(
        #[case] content_stats_col: &str,
        #[case] with_record_count: bool,
        #[case] with_tight_stats: bool,
        #[case] needle: &str,
    ) {
        let table = StructType::new_unchecked([field_with_id("id", DataType::LONG, true, 0)]);
        let delta =
            delta_stats_with_bounds(stat_cols(["id"]), stat_cols(["id"]), stat_cols(["id"]));
        let content_stats_schema = projected_stats_schema(&table, &delta).unwrap();
        let mut fields = vec![StructField::nullable(
            content_stats_col,
            content_stats_schema,
        )];
        if with_record_count {
            fields.push(StructField::nullable(RECORD_COUNT, DataType::LONG));
        }
        if with_tight_stats {
            fields.push(StructField::nullable(TIGHT_STATS, DataType::BOOLEAN));
        }
        let input_schema = StructType::new_unchecked(fields);

        assert_result_error_with_message(
            build_parsed_stats_expression(&table, &delta, &input_schema),
            needle,
        );
    }

    #[test]
    fn parsed_stats_errors_when_category_order_differs_from_table() {
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, true, 0),
            field_with_id("score", DataType::LONG, true, 1),
        ]);
        let delta = delta_stats_with_bounds(
            stat_cols(["score", "id"]),
            stat_cols(["id", "score"]),
            stat_cols(["id", "score"]),
        );
        let input_schema = parsed_stats_input_schema(&table, &delta);

        assert_result_error_with_message(
            build_parsed_stats_expression(&table, &delta, &input_schema),
            "but the table walk produced",
        );
    }
}
