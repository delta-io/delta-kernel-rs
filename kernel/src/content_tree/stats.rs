//! Stats manipulation utilities for Adaptive Metadata Tree (AMT).
//!
//! The AMT stores statistics as shredded columns within parquet files. Stats are stored
//! in a column-major format (each column has an associated struct with fields representing
//! min, max, etc).
//!
//! As with all fields in Iceberg, statistics are projected by field ID.

use std::borrow::Cow;

use tracing::warn;

use crate::content_tree::{
    AVG_VALUE_SIZE_IN_BYTES, LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, TIGHT_BOUNDS,
    UPPER_BOUND, VALUE_COUNT,
};
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error};

/// Variant inner field name whose statistics are tracked (variants only track their `value`).
const VARIANT_VALUE_FIELD_NAME: &str = "value";

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

/// Computes the base field ID for a column's stats struct, given a parent struct field ID.
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
/// - offset 5: `null_value_count` (long) - only if `nullable`
/// - offset 6: `nan_value_count` (long) - only for float/double `bounds_type`
/// - offset 7: `avg_value_size_in_bytes` (int) - for string/binary `bounds_type`, or any variant
///
/// `bounds_type` is the type the bounds are recorded at: the column's own type for primitives, or
/// an unshredded variant type for variant columns.
fn build_stats_struct(
    base_field_id: i32,
    bounds_type: &DataType,
    nullable: bool,
    is_variant: bool,
) -> StructType {
    let (has_nan_count, has_size_stats) = match bounds_type {
        DataType::Primitive(ptype) => (
            matches!(ptype, PrimitiveType::Float | PrimitiveType::Double),
            matches!(ptype, PrimitiveType::String | PrimitiveType::Binary),
        ),
        _ => (false, false),
    };
    let has_size_stats = has_size_stats || is_variant;

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
            nullable,
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
    let fields = specs
        .into_iter()
        .filter(|&(.., include)| include)
        .map(|(name, ty, offset, _)| field_with_id(name, ty, true, base_field_id + offset));

    StructType::new_unchecked(fields)
}

/// A [`SchemaTransform`] that rewrites a table schema into its AMT stats schema.
///
/// Each primitive/variant column becomes a nested stats struct (see [`build_stats_struct`]);
/// struct columns recurse; array/map columns and columns whose stats struct would be empty are
/// dropped. Uses a fallible filtering carrier: a missing field ID or an (as-yet unimplemented)
/// geospatial column aborts with an error, while an out-of-range field ID or empty stats struct
/// drops the column.
struct StatsSchemaTransform;

impl<'a> SchemaTransform<'a> for StatsSchemaTransform {
    transform_output_type!(|'a, T| Result<Option<Cow<'a, T>>, Error>);

    fn transform_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> Result<Option<Cow<'a, StructField>>, Error> {
        // A field ID is required for every field (missing => error). The spec limits which fields
        // may carry stats, so a field ID outside the supported range is expected for some reserved
        // metadata columns; skip (warn) rather than error in that case.
        let field_id = get_field_id(field).ok_or_else(|| {
            Error::generic(format!(
                "Field '{}' is missing field ID! metadata: {:#?}",
                field.name(),
                field.metadata()
            ))
        })?;
        let Some(base_stats_id) = field_id_to_statistics_base(field_id) else {
            warn!(
                "Skipping stats for field '{}' (field_id={field_id}): outside supported stats range",
                field.name(),
            );
            return Ok(None);
        };

        // Build the stats data type for this column; `None` means "drop this column".
        let stats_data_type = match field.data_type() {
            // Geospatial stats generation is not implemented yet. Error (rather than silently
            // dropping the column) so this is not forgotten once geospatial support lands; the arm
            // is reachable only with the `geo-type-in-dev` feature enabled.
            // TODO: emit proper stats for geospatial columns.
            #[cfg(feature = "geo-type-in-dev")]
            DataType::Primitive(PrimitiveType::Geometry(_) | PrimitiveType::Geography(_)) => {
                return Err(Error::unsupported(format!(
                    "AMT stats schema generation is not yet implemented for geospatial column \
                     '{}' (type {})",
                    field.name(),
                    field.data_type(),
                )));
            }
            DataType::Primitive(_) => Some(DataType::from(build_stats_struct(
                base_stats_id,
                field.data_type(),
                field.is_nullable(),
                false,
            ))),
            // A variant's inner fields carry no field IDs, so we cannot recurse through
            // `transform_struct`. The base stats ID covers the whole variant; stats are tracked
            // only when it has a `value` field (else drop), with the bounds recorded as unshredded
            // variants. `null_value_count` keys off the variant column's own nullability -- a NULL
            // variant is a column-level SQL null, independent of the physical encoding.
            DataType::Variant(inner) => inner.field(VARIANT_VALUE_FIELD_NAME).map(|_| {
                DataType::from(build_stats_struct(
                    base_stats_id,
                    &DataType::unshredded_variant(),
                    field.is_nullable(),
                    true,
                ))
            }),
            // `None` (every nested column dropped) => drop this struct column too.
            DataType::Struct(inner) => self
                .transform_struct(inner)?
                .map(|stats| DataType::from(stats.into_owned())),
            // Array/map element/key/value nodes carry no field-id context, so they have no stats.
            DataType::Array(_) | DataType::Map(_) => None,
        };

        let Some(stats_data_type) = stats_data_type else {
            return Ok(None);
        };

        // The stats group field is a fresh physical field: it uses the base stats field ID (e.g.
        // 10_200 for field_id 1), not the original column field ID, and carries no logical
        // (column-mapping) metadata from the source column.
        let stats_field = field_with_id(field.name(), stats_data_type, true, base_stats_id);
        Ok(Some(Cow::Owned(stats_field)))
    }
}

/// Generates the AMT stats schema for the given table struct.
///
/// Traverses the schema and produces a stats schema mirroring its structure: each primitive/variant
/// column becomes a nested per-column stats struct (see [`build_stats_struct`] for its fields and
/// field-id offsets), and struct columns recurse.
///
/// `table_struct` must be a physical schema carrying `parquet.field.id` metadata on each field (as
/// produced by [`StructField::make_physical`]); logical schemas that annotate field IDs only under
/// `delta.columnMapping.id` are not accepted.
///
/// A column is omitted when: it is an array/map column; its stats struct would be empty (e.g. a
/// struct of only array/map fields, or a variant lacking a `value` field); or its field ID is
/// outside the supported stats range (e.g. reserved metadata columns like `_file`/`_pos`, or data
/// field IDs above the reserved range), which are skipped with a warning. Returns an error if a
/// field is missing its field-id metadata entirely, or if it is an (as-yet unimplemented)
/// geospatial column.
pub(crate) fn stats_schema(table_struct: &StructType) -> DeltaResult<StructType> {
    // `Ok(None)` means every column was dropped, yielding an empty stats schema.
    match StatsSchemaTransform.transform_struct(table_struct)? {
        Some(stats) => Ok(stats.into_owned()),
        None => Ok(StructType::new_unchecked([])),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{ArrayType, MapType};
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};

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

    /// Returns the stats struct for `field.name()` in `stats`, panicking if absent or not a struct.
    fn field_stats_struct_for(field: &StructField, stats: &StructType) -> StructType {
        let field_stats = stats.field(field.name()).expect("stats field should exist");
        match field_stats.data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            other => panic!("expected struct stats, got {other:?}"),
        }
    }

    /// Asserts every stats sub-field of `stats_struct` carries the expected offset from `base_id`,
    /// gated on the same nullability/type rules used to build the struct. Variants omit
    /// `tight_bounds` and always carry the size stat.
    fn assert_stats_field_ids(stats_struct: &StructType, base_id: i32, field: &StructField) {
        let is_variant = matches!(field.data_type(), DataType::Variant(_));
        assert_eq!(
            get_field_id(stats_struct.field(VALUE_COUNT).unwrap()),
            Some(base_id + STATS_OFFSET_VALUE_COUNT)
        );
        if field.is_nullable() {
            assert_eq!(
                get_field_id(stats_struct.field(NULL_VALUE_COUNT).unwrap()),
                Some(base_id + STATS_OFFSET_NULL_VALUE_COUNT)
            );
        }
        if field.data_type() == &DataType::FLOAT || field.data_type() == &DataType::DOUBLE {
            assert_eq!(
                get_field_id(stats_struct.field(NAN_VALUE_COUNT).unwrap()),
                Some(base_id + STATS_OFFSET_NAN_VALUE_COUNT)
            );
        }
        if is_variant
            || field.data_type() == &DataType::STRING
            || field.data_type() == &DataType::BINARY
        {
            assert_eq!(
                get_field_id(stats_struct.field(AVG_VALUE_SIZE_IN_BYTES).unwrap()),
                Some(base_id + STATS_OFFSET_AVG_VALUE_SIZE_IN_BYTES)
            );
        }
        assert_eq!(
            get_field_id(stats_struct.field(LOWER_BOUND).unwrap()),
            Some(base_id + STATS_OFFSET_LOWER_BOUND)
        );
        assert_eq!(
            get_field_id(stats_struct.field(UPPER_BOUND).unwrap()),
            Some(base_id + STATS_OFFSET_UPPER_BOUND)
        );
        if !is_variant {
            assert_eq!(
                get_field_id(stats_struct.field(TIGHT_BOUNDS).unwrap()),
                Some(base_id + STATS_OFFSET_TIGHT_BOUNDS)
            );
        }
    }

    /// `expected_count`: lower/upper/tight/value (4) + null_value_count (if nullable) +
    /// nan_value_count (float/double) + avg_value_size_in_bytes (string/binary).
    #[rstest]
    #[case(DataType::INTEGER, false, 1, 10_200, 4)] // fixed-length, non-null
    #[case(DataType::STRING, true, 2, 10_400, 6)] // size stats + null count
    #[case(DataType::DOUBLE, true, 5, 11_000, 6)] // nan count + null count
    #[case(DataType::FLOAT, false, 100, 30_000, 5)] // nan count, non-null
    #[case(DataType::LONG, true, 42, 18_400, 5)] // fixed-length + null count
    #[case(DataType::BINARY, true, 3, 10_600, 6)] // size stats + null count
    #[case(DataType::BINARY, false, 4, 10_800, 5)] // size stats, non-null
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
        let stats_struct = field_stats_struct_for(&field, &stats);

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
        // { a: struct { b: int (non-null), c: double (nullable) } }
        let field_b = field_with_id("b", DataType::INTEGER, false, 2);
        let field_c = field_with_id("c", DataType::DOUBLE, true, 3);
        let inner = StructType::new_unchecked([field_b.clone(), field_c.clone()]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert_eq!(stats.fields().count(), 1);

        let a_stats = field_stats_struct_for(stats.field("a").unwrap(), &stats);
        assert_eq!(a_stats.fields().count(), 2);

        let b_stats = field_stats_struct_for(&field_b, &a_stats);
        assert_eq!(b_stats.fields().count(), 4); // fixed-length, non-null
        assert_stats_field_ids(&b_stats, 10_400, &field_b);

        let c_stats = field_stats_struct_for(&field_c, &a_stats);
        assert_eq!(c_stats.fields().count(), 6); // null + nan count
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
        assert_eq!(stats.fields().count(), 1);
        let s_stats = field_stats_struct_for(stats.field("s").unwrap(), &stats);
        // Only the primitive leaf 'p' survives; the array field 'a' is omitted.
        assert_eq!(s_stats.fields().count(), 1);
        assert!(s_stats.field("p").is_some());
        assert!(s_stats.field("a").is_none());
    }

    #[test]
    fn stats_schema_deeply_nested() {
        // { a: struct { b: struct { c: int } } }
        let innermost =
            StructType::new_unchecked([field_with_id("c", DataType::INTEGER, false, 3)]);
        let middle = StructType::new_unchecked([field_with_id("b", innermost.into(), true, 2)]);
        let schema = StructType::new_unchecked([field_with_id("a", middle.into(), true, 1)]);

        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let a_stats = field_stats_struct_for(stats.field("a").unwrap(), &stats);
        let b_stats = field_stats_struct_for(a_stats.field("b").unwrap(), &a_stats);
        let c_stats = field_stats_struct_for(b_stats.field("c").unwrap(), &b_stats);

        assert_eq!(c_stats.fields().count(), 4); // non-nullable int
        assert!(c_stats.field(VALUE_COUNT).is_some());
        assert!(c_stats.field(LOWER_BOUND).is_some());
    }

    /// Both unshredded and shredded variant inputs produce the same ordinary stats struct: the
    /// bounds are recorded as unshredded variants, `tight_bounds` is excluded, and the size stat is
    /// present. Only the presence of a `value` inner field matters; extra inner fields (e.g. a
    /// shredded `typed_value`) are ignored.
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

        let v_stats = field_stats_struct_for(&field, &stats);
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
        let record_struct = field_stats_struct_for(stats.field("record").unwrap(), &stats);
        assert!(record_struct.field("id").is_some());
        // The nested variant column becomes an ordinary stats struct with unshredded-variant
        // bounds.
        let data_stats = field_stats_struct_for(&data, &record_struct);
        assert_eq!(
            data_stats.field(LOWER_BOUND).unwrap().data_type(),
            &DataType::unshredded_variant()
        );
    }

    #[test]
    fn stats_schema_nullable_variant_gets_null_value_count() {
        // A nullable variant column must record null_value_count even though the inner `value`
        // field of an unshredded variant is not_null -- null tracking follows the column.
        let field = field_with_id("v", DataType::unshredded_variant(), true, 3);
        let schema = StructType::new_unchecked([field.clone()]);
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        let v_stats = field_stats_struct_for(&field, &stats);
        assert!(v_stats.field(NULL_VALUE_COUNT).is_some());
    }

    #[test]
    fn stats_schema_variant_without_value_field_is_omitted() {
        // A variant whose inner struct has no `value` field has no stats to track -> drop.
        let variant = DataType::Variant(Box::new(StructType::new_unchecked([
            StructField::not_null("metadata", DataType::BINARY),
        ])));
        let schema = StructType::new_unchecked([field_with_id("v", variant, false, 3)]);
        let stats = stats_schema(&schema).expect("stats_schema should succeed");
        assert!(stats.field("v").is_none());
        assert_eq!(stats.fields().count(), 0);
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest]
    #[case::geometry(DataType::from(GeometryType::try_new("EPSG:4326").expect("valid crs")))]
    #[case::geography(DataType::from(
        GeographyType::try_new("EPSG:4326", EdgeInterpolationAlgorithm::Spherical).expect("valid crs")
    ))]
    fn stats_schema_geospatial_column_errors(#[case] geo_type: DataType) {
        // Geospatial stats generation is not implemented yet: a geo column errors rather than being
        // silently dropped, so it is not forgotten once support lands.
        let schema = StructType::new_unchecked([field_with_id("g", geo_type, true, 1)]);
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
        // A child whose field ID is out of range (or negative) is warn-dropped, not an error; the
        // parent keeps its surviving children.
        let inner = StructType::new_unchecked([
            field_with_id("keep", DataType::INTEGER, false, 2),
            field_with_id("drop", DataType::INTEGER, false, bad_id),
        ]);
        let schema = StructType::new_unchecked([field_with_id("a", inner.into(), true, 1)]);
        let stats = stats_schema(&schema).expect("out-of-range child warn-drops, not errors");
        let a_stats = field_stats_struct_for(stats.field("a").unwrap(), &stats);
        assert!(a_stats.field("keep").is_some());
        assert!(a_stats.field("drop").is_none());
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
        assert_stats_field_ids(&field_stats_struct_for(&id, &stats), 10_000, &id);
        assert_stats_field_ids(&field_stats_struct_for(&name, &stats), 10_200, &name);
        assert_stats_field_ids(&field_stats_struct_for(&score, &stats), 10_400, &score);
        assert!(stats.field("_file").is_none());
        assert!(stats.field("_pos").is_none());
        assert_stats_field_ids(&field_stats_struct_for(&row_id, &stats), 9_200, &row_id);
        assert_stats_field_ids(
            &field_stats_struct_for(&last_updated_seq_no, &stats),
            9_000,
            &last_updated_seq_no,
        );
    }
}
