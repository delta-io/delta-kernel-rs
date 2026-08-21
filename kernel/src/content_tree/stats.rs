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

use tracing::warn;

use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT};
use crate::content_tree::{
    AVG_VALUE_SIZE_IN_BYTES, LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, TIGHT_BOUNDS,
    UPPER_BOUND, VALUE_COUNT,
};
use crate::expressions::ColumnName;
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error};

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

/// Builds a single column's stats struct, with each stat sub-field's ID an offset from
/// `base_field_id`:
/// - offset 1/2: `lower_bound` / `upper_bound` (typed as `bounds_type`)
/// - offset 3: `tight_bounds` (boolean) - excluded for variants
/// - offset 4: `value_count` (long)
/// - offset 5: `null_value_count` (long) - always present
/// - offset 6: `nan_value_count` (long) - only for float/double `bounds_type`
/// - offset 7: `avg_value_size_in_bytes` (int) - for string/binary `bounds_type`, or any variant
///
/// `bounds_type` is the type the bounds are recorded at: the column's own type for primitives, or
/// an unshredded variant type for variant columns.
fn build_stats_struct(base_field_id: i32, bounds_type: &DataType) -> StructType {
    let is_variant = matches!(bounds_type, DataType::Variant(_));
    let (has_nan_count, has_size_stats) = match bounds_type {
        DataType::Primitive(ptype) => (
            matches!(ptype, PrimitiveType::Float | PrimitiveType::Double),
            matches!(ptype, PrimitiveType::String | PrimitiveType::Binary),
        ),
        DataType::Variant(_) => (false, true),
        _ => (false, false),
    };

    // (name, type, offset, include) -- filtered in declaration order to preserve field ordering.
    let specs = [
        (
            LOWER_BOUND,
            bounds_type.clone(),
            STATS_OFFSET_LOWER_BOUND,
            true,
        ),
        (
            UPPER_BOUND,
            bounds_type.clone(),
            STATS_OFFSET_UPPER_BOUND,
            true,
        ),
        (
            TIGHT_BOUNDS,
            DataType::BOOLEAN,
            STATS_OFFSET_TIGHT_BOUNDS,
            !is_variant,
        ),
        (VALUE_COUNT, DataType::LONG, STATS_OFFSET_VALUE_COUNT, true),
        (
            NULL_VALUE_COUNT,
            DataType::LONG,
            STATS_OFFSET_NULL_VALUE_COUNT,
            true,
        ),
        (
            NAN_VALUE_COUNT,
            DataType::LONG,
            STATS_OFFSET_NAN_VALUE_COUNT,
            has_nan_count,
        ),
        (
            AVG_VALUE_SIZE_IN_BYTES,
            DataType::INTEGER,
            STATS_OFFSET_AVG_VALUE_SIZE_IN_BYTES,
            has_size_stats,
        ),
    ];
    let fields = specs.into_iter().filter_map(|(name, ty, offset, include)| {
        include.then(|| field_with_id(name, ty, true, base_field_id + offset))
    });

    StructType::new_unchecked(fields)
}

/// Builds the flat stats field for a non-struct leaf, or `None` when the leaf carries no stats:
/// an array/map column, or a field ID outside the supported stats range (skipped with a
/// warning). The returned field is named by the leaf's full
/// dotted path (`path`, whose last segment is the leaf itself) and keyed at the leaf's base stats
/// field ID (see [`build_stats_struct`] for the sub-fields).
///
/// Errors if the leaf is missing its field-id metadata, or is an (as-yet unimplemented) geospatial
/// column.
fn leaf_stats_field(field: &StructField, path: &[String]) -> DeltaResult<Option<StructField>> {
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
    // gate below so an out-of-range geo field ID still errors. Reachable only with the
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
        DataType::Primitive(_) => build_stats_struct(base_stats_id, field.data_type()),
        // A variant's inner fields carry no field IDs; the base stats ID covers the whole variant,
        // and its bounds are always recorded as unshredded variants regardless of physical
        // shredding.
        DataType::Variant(_) => build_stats_struct(base_stats_id, &DataType::unshredded_variant()),
        // Array/map columns carry no leaf stats. (Structs are descended into by the collector and
        // never reach here.) Note the missing/out-of-range field-id checks above run first, so a
        // malformed array/map leaf errors/warn-drops rather than reaching this silent drop.
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

/// Returns the struct sub-schema of `schema.<field_name>`, or `None` if the field is absent or is
/// not a struct. In a Delta stats schema every category node is a struct mirroring the table
/// nesting (leaves -- variants included -- are scalars), so a non-struct node is a leaf or a
/// shape mismatch and terminates the descent.
fn get_struct_sub_schema<'a>(schema: &'a StructType, field_name: &str) -> Option<&'a StructType> {
    match schema.field(field_name).map(StructField::data_type) {
        Some(DataType::Struct(s)) => Some(s.as_ref()),
        _ => None,
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
    /// Builds the top-level scopes from a Delta JSON stats schema.
    fn from_delta_stats_schema(delta_stats_schema: &'a StructType) -> Self {
        CategoryScopes {
            categories: STAT_CATEGORIES.map(|c| get_struct_sub_schema(delta_stats_schema, c)),
        }
    }

    /// Steps every category into its `<name>` sub-schema for a struct descent.
    fn descend(&self, name: &str) -> CategoryScopes<'a> {
        CategoryScopes {
            categories: self
                .categories
                .map(|scope| scope.and_then(|s| get_struct_sub_schema(s, name))),
        }
    }

    /// Whether a leaf named `leaf_name` appears in *any* category. Every leaf -- variants included
    /// -- is a scalar in each category (a variant appears in `nullCount` as a scalar `LONG` and is
    /// absent from `minValues`/`maxValues`), so presence is a same-name field lookup.
    fn leaf_present(&self, leaf_name: &str) -> bool {
        self.categories
            .iter()
            .any(|scope| scope.is_some_and(|s| s.field(leaf_name).is_some()))
    }
}

/// A [`SchemaTransform`] that collects the flat AMT `content_stats` schema by visiting every leaf
/// of a table schema (see [`stats_schema`] for the layout, [`leaf_stats_field`] for each leaf).
///
/// When `filter` is `Some`, only leaves present in at least one Delta stat category are emitted
/// (see [`filtered_stats_schema`]); when `None`, every stat-eligible leaf is emitted.
///
/// Uses the `Result<(), Error>` carrier: the rebuilt output is discarded, [`Self::fields`] is the
/// real result, and an `Err` short-circuits the walk.
struct StatsSchemaCollector<'a> {
    /// Field names from the root to the current node; the last segment is the leaf being visited.
    path: Vec<String>,
    /// Accumulated flat stats fields, in schema order.
    fields: Vec<StructField>,
    /// Delta stat categories in scope at the current position, or `None` to emit every leaf.
    filter: Option<CategoryScopes<'a>>,
}

impl<'a> SchemaTransform<'a> for StatsSchemaCollector<'a> {
    transform_output_type!(|'a, T| Result<(), Error>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Result<(), Error> {
        self.path.push(field.name().to_string());
        // Descend into structs; every other type is a leaf. On `Err` the walk aborts and `path` is
        // discarded, so the skipped pop is harmless.
        let result = if let DataType::Struct(_) = field.data_type() {
            // The stat-category scopes step into this struct's sub-schema and are restored on the
            // way back up so siblings are unaffected.
            let saved_filter = self.filter;
            self.filter = self.filter.map(|s| s.descend(field.name()));
            let result = self.recurse_into_struct_field(field);
            self.filter = saved_filter;
            result
        } else if self.filter.is_some_and(|s| !s.leaf_present(field.name())) {
            // Absent from every category: emit no stats for this leaf.
            Ok(())
        } else {
            // Every non-struct type is a leaf handled by `leaf_stats_field` -- including variants
            // (never descended into: their inner fields carry no field IDs) and array/map columns
            // (which produce no stats). The filter is column-granular: a surviving leaf keeps its
            // full stats struct regardless of which categories it appeared in.
            // TODO: prune per-category sub-fields (e.g. drop `upper_bound` when only `minValues`
            // is present).
            leaf_stats_field(field, &self.path).map(|stats| self.fields.extend(stats))
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

/// Generates the AMT `content_stats` schema restricted to the leaves that carry Delta stats.
///
/// Same flat layout as [`stats_schema`], but a leaf is emitted only if its column appears in at
/// least one Delta stat category (`nullCount`/`minValues`/`maxValues`) of `delta_stats_schema`.
/// This avoids reading per-column stats that no Delta stat records. `delta_stats_schema` is the
/// nested Delta Protocol stats schema (nesting in table schema is reflected in the stats schema)
/// tested segment-by-segment as the walk descends (see [`CategoryScopes`]).
///
///
/// `table_struct` must be a physical schema (carrying `parquet.field.id`, as [`stats_schema`]
/// requires), and `delta_stats_schema` must use the same physical names. Membership is matched by
/// field name, so a naming mismatch (e.g. logical stat names against a physical `table_struct`
/// under column mapping) matches nothing and drops every leaf.
///
pub(crate) fn filtered_stats_schema(
    table_struct: &StructType,
    delta_stats_schema: &StructType,
) -> DeltaResult<StructType> {
    collect_stats_schema(
        table_struct,
        Some(CategoryScopes::from_delta_stats_schema(delta_stats_schema)),
    )
}

/// Shared body of [`stats_schema`] and [`filtered_stats_schema`]: walks `table_struct` with the
/// (optional) Delta stat filter and returns the flat `content_stats` schema.
fn collect_stats_schema<'a>(
    table_struct: &'a StructType,
    filter: Option<CategoryScopes<'a>>,
) -> DeltaResult<StructType> {
    let mut collector = StatsSchemaCollector {
        path: Vec::new(),
        fields: Vec::new(),
        filter,
    };
    collector.transform_struct(table_struct)?;
    // `new_unchecked` skips name dedup; safe because `ColumnName`'s `Display` is lossless -- a leaf
    // whose name contains a dot is backtick-escaped, so it never collides with a nested path.
    Ok(StructType::new_unchecked(collector.fields))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
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
        // struct -- the abort-vs-drop distinction of the filtering carrier survives recursion.
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
    /// struct (the case that historically omitted it).
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
    // Out of range: still errors, because the geospatial check precedes the range gate.
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

    // === filtered_stats_schema ===

    /// Builds a Delta JSON stats schema from its three category sub-structs (any may be omitted).
    /// Only field names and struct nesting matter to the filter, so leaf types are arbitrary.
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

    #[test]
    fn filtered_equals_stats_schema_when_every_leaf_present() {
        // {id: long, a: struct{b: int}, v: variant}, with every leaf in all three categories.
        let table = StructType::new_unchecked([
            field_with_id("id", DataType::LONG, false, 0),
            field_with_id(
                "a",
                StructType::new_unchecked([field_with_id("b", DataType::INTEGER, true, 2)]).into(),
                true,
                1,
            ),
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

        let filtered = filtered_stats_schema(&table, &delta).expect("filtered should succeed");
        let full = stats_schema(&table).expect("stats_schema should succeed");
        assert_eq!(filtered, full);
    }

    #[test]
    fn filtered_drops_array_and_map_even_when_in_null_count() {
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
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.fields().count(), 1);
        assert!(filtered.field("id").is_some());
    }

    #[rstest]
    #[case::only_null_count(true, false, false, true)]
    #[case::only_min(false, true, false, true)]
    #[case::only_max(false, false, true, true)]
    #[case::min_and_max(false, true, true, true)]
    #[case::none(false, false, false, false)]
    fn filtered_includes_leaf_iff_in_any_category(
        #[case] in_null_count: bool,
        #[case] in_min: bool,
        #[case] in_max: bool,
        #[case] expected_present: bool,
    ) {
        let table = StructType::new_unchecked([field_with_id("c", DataType::INTEGER, false, 1)]);
        let cat = |included: bool| included.then(|| stat_cols(["c"]));
        let delta = delta_stats(cat(in_null_count), cat(in_min), cat(in_max));

        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.field("c").is_some(), expected_present);
        if expected_present {
            // Int leaf => lower, upper, tight_bounds, value_count, null_value_count (the full
            // unfiltered set; null_value_count is always present).
            assert_eq!(stats_struct_for_name("c", &filtered).fields().count(), 5);
        }
    }

    /// Nested-leaf membership is decided per leaf: a leaf survives iff it appears in some category,
    /// independent of its siblings.
    #[rstest]
    // `a.c` via nullCount, `a.b` via minValues => both survive.
    #[case::each_via_a_different_category(Some(nested_cat("a", ["c"])), Some(nested_cat("a", ["b"])), &["a.b", "a.c"])]
    // Only `a.b` appears anywhere => `a.c` is dropped.
    #[case::sibling_absent_is_dropped(None, Some(nested_cat("a", ["b"])), &["a.b"])]
    fn filtered_nested_leaf_membership_is_per_leaf(
        #[case] null_count: Option<StructType>,
        #[case] min_values: Option<StructType>,
        #[case] expected: &[&str],
    ) {
        // {a: struct{b: int, c: string}}
        let table = StructType::new_unchecked([field_with_id(
            "a",
            StructType::new_unchecked([
                field_with_id("b", DataType::INTEGER, true, 2),
                field_with_id("c", DataType::STRING, true, 3),
            ])
            .into(),
            true,
            1,
        )]);
        let delta = delta_stats(null_count, min_values, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.fields().count(), expected.len());
        for path in expected {
            assert!(filtered.field(path).is_some(), "expected {path} to survive");
        }
    }

    #[test]
    fn filtered_drops_entire_substruct_absent_from_all_categories() {
        // {a: struct{b: int}, d: long}; only `d` is present, so the whole `a` subtree is dropped.
        let table = StructType::new_unchecked([
            field_with_id(
                "a",
                StructType::new_unchecked([field_with_id("b", DataType::INTEGER, true, 2)]).into(),
                true,
                1,
            ),
            field_with_id("d", DataType::LONG, true, 3),
        ]);
        let delta = delta_stats(Some(stat_cols(["d"])), None, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.fields().count(), 1);
        assert!(filtered.field("d").is_some());
        assert!(filtered.field("a.b").is_none());
    }

    #[test]
    fn filtered_drops_substruct_when_ancestor_category_node_is_leaf() {
        // {a: struct{b: int}}; `a`'s category node is a scalar (shape mismatch), so the descent
        // terminates (get_struct_sub_schema returns None) and the whole `a` subtree is dropped.
        let table = StructType::new_unchecked([field_with_id(
            "a",
            StructType::new_unchecked([field_with_id("b", DataType::INTEGER, true, 2)]).into(),
            true,
            1,
        )]);
        let delta = delta_stats(Some(stat_cols(["a"])), None, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.fields().count(), 0);
    }

    /// A variant survives the filter exactly when present in some category, like any other leaf.
    #[rstest]
    #[case::present_in_null_count(true, true)]
    #[case::absent_from_all(false, false)]
    fn filtered_variant_membership_matches_scalar_presence(
        #[case] in_null_count: bool,
        #[case] expected_present: bool,
    ) {
        let table = StructType::new_unchecked([field_with_id(
            "v",
            DataType::unshredded_variant(),
            true,
            3,
        )]);
        let delta = delta_stats(in_null_count.then(|| stat_cols(["v"])), None, None);
        assert_eq!(
            filtered_stats_schema(&table, &delta)
                .expect("should succeed")
                .field("v")
                .is_some(),
            expected_present
        );
    }

    /// Guards against fixture-vs-reality drift: derive the Delta stats schema from the real
    /// [`expected_stats_schema`] (the only nested stats schema the kernel produces) and confirm
    /// that a variant survives the filter alongside the primitive leaves. With every leaf carrying
    /// a `nullCount` stat, the filter is a no-op, so the result equals the unfiltered
    /// [`stats_schema`].
    #[test]
    fn filtered_keeps_all_leaves_from_real_expected_stats_schema() {
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

        let filtered = filtered_stats_schema(&table, &delta).expect("filtered should succeed");
        assert!(
            filtered.field("v").is_some(),
            "variant present in nullCount must survive"
        );
        assert_eq!(
            filtered,
            stats_schema(&table).expect("stats_schema should succeed")
        );
    }

    #[test]
    fn filtered_leaf_in_category_still_warn_dropped_when_out_of_range() {
        // Presence in a category does not bypass the supported-range gate in `leaf_stats_field`.
        let table = StructType::new_unchecked([field_with_id(
            "c",
            DataType::INTEGER,
            false,
            MAX_DATA_FIELD_ID + 1,
        )]);
        let delta = delta_stats(Some(stat_cols(["c"])), None, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("out-of-range warn-drops");
        assert_eq!(filtered.fields().count(), 0);
    }

    #[test]
    fn filtered_leaf_in_category_missing_field_id_still_errors() {
        // Presence in a category does not bypass the missing-field-id error either.
        let table = StructType::new_unchecked([StructField::not_null("c", DataType::INTEGER)]);
        let delta = delta_stats(Some(stat_cols(["c"])), None, None);
        assert!(filtered_stats_schema(&table, &delta).is_err());
    }

    #[test]
    fn filtered_skips_missing_field_id_leaf_when_absent_from_all_categories() {
        // The filter runs before `leaf_stats_field`, so a leaf absent from every category is
        // skipped without triggering its missing-field-id error.
        let table = StructType::new_unchecked([
            StructField::not_null("no_id", DataType::INTEGER),
            field_with_id("c", DataType::INTEGER, false, 1),
        ]);
        let delta = delta_stats(Some(stat_cols(["c"])), None, None);
        let filtered = filtered_stats_schema(&table, &delta)
            .expect("absent leaf is filtered before its missing-id check");
        assert_eq!(filtered.fields().count(), 1);
        assert!(filtered.field("c").is_some());
    }

    #[test]
    fn filtered_restores_scope_for_sibling_struct_at_nested_level() {
        // {p: struct{x: struct{a}, y: struct{b}}}: `p.x.a` via nullCount, `p.y.b` via minValues.
        // `y` must descend from `p`'s saved scope -- not `x`'s child scope, not root -- so both
        // survive. This exercises restoration to a non-root saved scope, unlike the other tests.
        let table = StructType::new_unchecked([field_with_id(
            "p",
            StructType::new_unchecked([
                field_with_id(
                    "x",
                    StructType::new_unchecked([field_with_id("a", DataType::INTEGER, true, 3)])
                        .into(),
                    true,
                    2,
                ),
                field_with_id(
                    "y",
                    StructType::new_unchecked([field_with_id("b", DataType::INTEGER, true, 5)])
                        .into(),
                    true,
                    4,
                ),
            ])
            .into(),
            true,
            1,
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

        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.fields().count(), 2);
        assert!(filtered.field("p.x.a").is_some());
        assert!(filtered.field("p.y.b").is_some());
    }

    #[test]
    fn filtered_drops_all_leaves_on_naming_mode_mismatch() {
        // Physical table names vs (logical) stats-schema names => no overlap => empty, no error.
        let table = StructType::new_unchecked([
            field_with_id("col-abc", DataType::INTEGER, false, 1),
            field_with_id("col-def", DataType::LONG, true, 2),
        ]);
        let delta = delta_stats(Some(stat_cols(["logical_a", "logical_b"])), None, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("mismatch drops all, no error");
        assert_eq!(filtered.fields().count(), 0);
    }

    #[rstest]
    // A table column named like a stat category is matched one level deep (inside the category),
    // not against the top-level category header.
    #[case::leaf_inside_category(stat_cols(["nullCount"]), true)]
    #[case::only_the_category_header(stat_cols(["other"]), false)]
    fn filtered_handles_column_named_like_a_stat_category(
        #[case] null_count: StructType,
        #[case] expected_present: bool,
    ) {
        let table =
            StructType::new_unchecked([field_with_id("nullCount", DataType::LONG, true, 1)]);
        let delta = delta_stats(Some(null_count), None, None);
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.field("nullCount").is_some(), expected_present);
    }

    /// C1 contract: `stats_schema` emits a reserved-metadata leaf unconditionally, but the filter
    /// emits it only when the caller lists it in a category. The caller decides membership.
    #[rstest]
    #[case::included_survives(true, true)]
    #[case::omitted_dropped(false, false)]
    fn filtered_reserved_metadata_leaf_requires_caller_inclusion(
        #[case] include_in_category: bool,
        #[case] expected_present: bool,
    ) {
        let table = StructType::new_unchecked([field_with_id(
            "_row_id",
            DataType::LONG,
            true,
            ROW_ID_FIELD_ID,
        )]);
        // The unfiltered path always emits the reserved-metadata leaf.
        assert!(stats_schema(&table)
            .expect("stats_schema should succeed")
            .field("_row_id")
            .is_some());
        // The filtered path emits it only when the caller includes it in a category.
        let delta = delta_stats(
            include_in_category.then(|| stat_cols(["_row_id"])),
            None,
            None,
        );
        let filtered = filtered_stats_schema(&table, &delta).expect("should succeed");
        assert_eq!(filtered.field("_row_id").is_some(), expected_present);
    }
}
