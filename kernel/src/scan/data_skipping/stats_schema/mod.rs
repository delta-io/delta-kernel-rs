//! This module contains logic to compute the expected schema for file statistics

mod column_filter;

use std::borrow::Cow;

use crate::{
    schema::{
        ArrayType, ColumnName, DataType, MapType, PrimitiveType, Schema, SchemaTransform,
        StructField, StructType,
    },
    table_properties::TableProperties,
    DeltaResult,
};

use column_filter::StatsColumnFilter;

/// Generates the expected schema for file statistics.
///
/// The base stats schema is dependent on the current table configuration and derived via:
/// - only fields present in data files are included (use physical names, no partition columns)
/// - if the table property `delta.dataSkippingStatsColumns` is set, include only those columns.
///   Column names may refer to struct fields in which case all child fields are included.
/// - otherwise the first `dataSkippingNumIndexedCols` (default 32) leaf fields are included.
/// - all fields are made nullable.
///
/// The `nullCount` struct field is a nested structure mirroring the table's column hierarchy.
/// It tracks the count of null values for each column. All leaf fields from the base schema
/// are converted to LONG type (since null counts are always integers).
///
/// Note: Map, Array, and Variant types are excluded from statistics entirely (including
/// `nullCount`) as they are not eligible for data skipping. The `nullCount` schema includes
/// primitive types that aren't eligible for min/max (e.g., Boolean, Binary) since null counts
/// are still meaningful for those types.
///
/// The `minValues`/`maxValues` struct fields are also nested structures mirroring the table's
/// column hierarchy. They additionally filter out leaf fields with non-eligible data types
/// (e.g., Boolean, Binary) via [`is_skipping_eligible_datatype`].
///
/// The `tightBounds` field is a boolean indicating whether the min/max statistics are "tight"
/// (accurate) or "wide" (potentially outdated). When `tightBounds` is `true`, the statistics
/// accurately reflect the data in the file. When `false`, the file may have deletion vectors
/// and the statistics haven't been recomputed to exclude deleted rows.
///
/// See the Delta protocol for more details on statistics:
/// <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics>
///
/// The overall schema is then:
/// ```ignored
/// {
///    numRecords: long,
///    nullCount: <derived null count schema>,
///    minValues: <derived min/max schema>,
///    maxValues: <derived min/max schema>,
///    tightBounds: boolean,
/// }
/// ```
///
/// For a table with physical schema:
///
/// ```ignore
/// {
///    id: long,
///    user: {
///      name: string,
///      age: integer,
///    },
/// }
/// ```
///
/// the expected stats schema would be:
/// ```ignore
/// {
///   numRecords: long,
///   nullCount: {
///     id: long,
///     user: {
///       name: long,
///       age: long,
///     },
///   },
///   minValues: {
///     id: long,
///     user: {
///       name: string,
///       age: integer,
///     },
///   },
///   maxValues: {
///     id: long,
///     user: {
///       name: string,
///       age: integer,
///     },
///   },
///   tightBounds: boolean,
/// }
/// ```
#[allow(unused)]
pub(crate) fn expected_stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(5);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_columns, ...)
    let mut base_transform = BaseStatsTransform::new(table_properties);
    if let Some(base_schema) = base_transform.transform_struct(physical_file_schema) {
        let base_schema = base_schema.into_owned();

        // convert all leaf fields to data type LONG for null count
        let mut null_count_transform = NullCountStatsTransform;
        if let Some(null_count_schema) = null_count_transform.transform_struct(&base_schema) {
            fields.push(StructField::nullable(
                "nullCount",
                null_count_schema.into_owned(),
            ));
        };

        // include only min/max skipping eligible fields (data types)
        let mut min_max_transform = MinMaxStatsTransform;
        if let Some(min_max_schema) = min_max_transform.transform_struct(&base_schema) {
            let min_max_schema = min_max_schema.into_owned();
            fields.push(StructField::nullable("minValues", min_max_schema.clone()));
            fields.push(StructField::nullable("maxValues", min_max_schema));
        }
    }

    // tightBounds indicates whether min/max statistics are accurate (true) or potentially
    // outdated due to deletion vectors (false)
    fields.push(StructField::nullable("tightBounds", DataType::BOOLEAN));

    StructType::try_new(fields)
}

/// Returns the list of column names that should have statistics collected.
///
/// This extracts just the column names without building the full stats schema,
/// making it more efficient when only the column list is needed.
#[allow(unused)]
pub(crate) fn stats_column_names(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> Vec<ColumnName> {
    let mut filter = StatsColumnFilter::new(table_properties);
    let mut columns = Vec::new();
    filter.collect_columns(physical_file_schema, &mut columns);
    columns
}

/// Transforms a schema to make all fields nullable.
/// Used for stats schemas where stats may not be available for all columns.
pub(crate) struct NullableStatsTransform;
impl<'a> SchemaTransform<'a> for NullableStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;
        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: field.metadata.clone(),
            }),
        };
        Some(field)
    }
}

/// Converts a stats schema into a nullCount schema where all leaf fields become LONG.
///
/// The nullCount struct field tracks the number of null values for each column.
/// All leaf fields (primitives, arrays, maps, variants) are converted to LONG type
/// since null counts are always integers, while struct fields are recursed into
/// to preserve the nested structure.
#[allow(unused)]
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        // Only recurse into struct fields; convert all other types (leaf fields) to LONG
        match &field.data_type {
            DataType::Struct(_) => self.recurse_into_struct_field(field),
            _ => Some(Cow::Owned(StructField::nullable(
                &field.name,
                DataType::LONG,
            ))),
        }
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats - min/max/nullcount/... -
/// additional transformations may be applied.
/// Transforms a schema to filter columns for statistics based on table properties.
///
/// All fields in the output are nullable.
#[allow(unused)]
struct BaseStatsTransform<'col> {
    filter: StatsColumnFilter<'col>,
}

impl<'col> BaseStatsTransform<'col> {
    #[allow(unused)]
    fn new(props: &'col TableProperties) -> Self {
        Self {
            filter: StatsColumnFilter::new(props),
        }
    }
}

impl<'a, 'col> SchemaTransform<'a> for BaseStatsTransform<'col> {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        if self.filter.at_column_limit() {
            return None;
        }

        self.filter.enter_field(field.name());
        let data_type = field.data_type();

        // Map, Array, and Variant types are not eligible for statistics - skip entirely.
        if matches!(
            data_type,
            DataType::Map(_) | DataType::Array(_) | DataType::Variant(_)
        ) {
            self.filter.exit_field();
            return None;
        }

        // We always traverse struct fields (they don't count against the column limit),
        // but we only include leaf fields if they qualify based on column_trie config.
        // When column_trie is None, all leaf fields are included (up to n_columns limit).
        if !matches!(data_type, DataType::Struct(_)) {
            if !self.filter.should_include_current() {
                self.filter.exit_field();
                return None;
            }
            self.filter.record_included();
        }

        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: Default::default(),
            }),
        };

        self.filter.exit_field();

        // exclude struct fields with no children
        if matches!(field.data_type(), DataType::Struct(dt) if dt.fields().len() == 0) {
            None
        } else {
            Some(field)
        }
    }
}

// removes all fields with non eligible data types
//
// should only be applied to schema processed via `BaseStatsTransform`.
#[allow(unused)]
struct MinMaxStatsTransform;

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    // Array, Map, and Variant fields are filtered out by BaseStatsTransform, so these methods
    // are typically not called. They're kept as a safety net in case the transform is used
    // independently or the filtering logic changes.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        None
    }

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        is_skipping_eligible_datatype(ptype).then_some(Cow::Borrowed(ptype))
    }
}

/// Checks if a data type is eligible for min/max file skipping.
///
/// Note: Boolean and Binary are intentionally excluded as min/max statistics provide minimal
/// skipping benefit for low-cardinality or opaque data types.
///
/// See: <https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61>
#[allow(unused)]
fn is_skipping_eligible_datatype(data_type: &PrimitiveType) -> bool {
    matches!(
        data_type,
        &PrimitiveType::Byte
            | &PrimitiveType::Short
            | &PrimitiveType::Integer
            | &PrimitiveType::Long
            | &PrimitiveType::Float
            | &PrimitiveType::Double
            | &PrimitiveType::Date
            | &PrimitiveType::Timestamp
            | &PrimitiveType::TimestampNtz
            | &PrimitiveType::String
            | PrimitiveType::Decimal(_)
    )
}

#[cfg(test)]
mod tests {
    use crate::schema::ArrayType;

    use super::*;

    #[test]
    fn test_stats_schema_simple() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", file_schema.clone()),
            StructField::nullable("minValues", file_schema.clone()),
            StructField::nullable("maxValues", file_schema),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested() {
        let properties: TableProperties = [("key", "value")].into();

        let user_struct = StructType::new_unchecked([
            StructField::not_null("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("user", DataType::Struct(Box::new(user_struct.clone()))),
        ]);
        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // Expected result: The stats schema should maintain the nested structure
        // but make all fields nullable
        let expected_min_max = NullableStatsTransform
            .transform_struct(&file_schema)
            .unwrap()
            .into_owned();
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_min_max)
            .unwrap()
            .into_owned();

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_with_non_eligible_field() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested logical schema with:
        // - top-level field "id" (LONG) - eligible for data skipping
        // - nested struct "metadata" containing:
        //   - "name" (STRING) - eligible for data skipping
        //   - "tags" (ARRAY) - NOT eligible for data skipping
        //   - "score" (DOUBLE) - eligible for data skipping

        // Create array type for a field that's not eligible for data skipping
        let array_type = DataType::Array(Box::new(ArrayType::new(DataType::STRING, false)));
        let metadata_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("tags", array_type),
            StructField::nullable("score", DataType::DOUBLE),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "metadata",
                DataType::Struct(Box::new(metadata_struct.clone())),
            ),
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // nullCount excludes array fields (tags) - only eligible primitive types
        let expected_null_nested = StructType::new_unchecked([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("score", DataType::LONG),
        ]);
        let expected_null = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("metadata", DataType::Struct(Box::new(expected_null_nested))),
        ]);

        let expected_nested = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("score", DataType::DOUBLE),
        ]);
        let expected_fields = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("metadata", DataType::Struct(Box::new(expected_nested))),
        ]);

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_col_names() {
        let properties: TableProperties = [(
            "delta.dataSkippingStatsColumns".to_string(),
            "`user.info`.name".to_string(),
        )]
        .into();

        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user.info", DataType::Struct(Box::new(user_struct.clone()))),
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        let expected_nested =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let expected_fields = StructType::new_unchecked([StructField::nullable(
            "user.info",
            DataType::Struct(Box::new(expected_nested)),
        )]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_n_cols() {
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "1".to_string(),
        )]
        .into();

        let logical_schema = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);

        let stats_schema = expected_stats_schema(&logical_schema, &properties).unwrap();

        let expected_fields =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a schema with fields that have different eligibility for min/max vs null count
        // - "id" (LONG) - eligible for both null count and min/max
        // - "is_active" (BOOLEAN) - eligible for null count but NOT for min/max
        // - "metadata" (BINARY) - eligible for null count but NOT for min/max
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: only eligible fields (no boolean, no binary)
        let expected_min_max =
            StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested schema where some nested fields are eligible for min/max and others aren't
        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING), // eligible for min/max
            StructField::nullable("is_admin", DataType::BOOLEAN), // NOT eligible for min/max
            StructField::nullable("age", DataType::INTEGER), // eligible for min/max
            StructField::nullable("profile_pic", DataType::BINARY), // NOT eligible for min/max
        ]);

        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct.clone()))),
            StructField::nullable("is_deleted", DataType::BOOLEAN), // NOT eligible for min/max
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // Expected nullCount schema: all fields converted to LONG, maintaining structure
        let expected_null_user = StructType::new_unchecked([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("is_admin", DataType::LONG),
            StructField::nullable("age", DataType::LONG),
            StructField::nullable("profile_pic", DataType::LONG),
        ]);
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_null_user))),
            StructField::nullable("is_deleted", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: only eligible fields
        let expected_minmax_user = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let expected_min_max = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_minmax_user))),
        ]);

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_only_non_eligible_fields() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a schema with only fields that are NOT eligible for min/max skipping
        let file_schema = StructType::new_unchecked([
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // nullCount includes boolean and binary (primitives) but excludes array
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
        ]);

        // minValues/maxValues: no fields are eligible (boolean/binary excluded)
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_map_array_dont_count_against_limit() {
        // Test that Map and Array fields don't count against the column limit.
        // With a limit of 2, if we have: array, map, col1, col2, col3
        // We should get stats for col1 and col2 (the first 2 eligible columns),
        // not be limited by the array and map fields.
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "2".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
            StructField::nullable(
                "metadata",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
            StructField::nullable("col1", DataType::LONG),
            StructField::nullable("col2", DataType::STRING),
            StructField::nullable("col3", DataType::INTEGER), // Should be excluded by limit
        ]);

        let stats_schema = expected_stats_schema(&file_schema, &properties).unwrap();

        // nullCount has only eligible primitive columns (col1 and col2).
        // Map/Array/Variant are excluded from all stats.
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("col1", DataType::LONG),
            StructField::nullable("col2", DataType::LONG),
        ]);

        // minValues/maxValues only have eligible primitive types (col1 and col2).
        // Map/Array are filtered out by MinMaxStatsTransform.
        let expected_min_max = StructType::new_unchecked([
            StructField::nullable("col1", DataType::LONG),
            StructField::nullable("col2", DataType::STRING),
        ]);

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    // ==================== stats_column_names tests ====================

    #[test]
    fn test_stats_column_names_default() {
        let properties: TableProperties = [("key", "value")].into();

        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct))),
        ]);

        let columns = stats_column_names(&file_schema, &properties);

        // With default settings, all leaf columns should be included
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["user", "name"]),
                ColumnName::new(["user", "age"]),
            ]
        );
    }

    #[test]
    fn test_stats_column_names_with_num_indexed_cols() {
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "2".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("c", DataType::INTEGER),
            StructField::nullable("d", DataType::DOUBLE),
        ]);

        let columns = stats_column_names(&file_schema, &properties);

        // Only first 2 columns should be included
        assert_eq!(
            columns,
            vec![ColumnName::new(["a"]), ColumnName::new(["b"]),]
        );
    }

    #[test]
    fn test_stats_column_names_with_stats_columns() {
        let properties: TableProperties = [(
            "delta.dataSkippingStatsColumns".to_string(),
            "id,user.age".to_string(),
        )]
        .into();

        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct))),
            StructField::nullable("extra", DataType::STRING),
        ]);

        let columns = stats_column_names(&file_schema, &properties);

        // Only specified columns should be included (user.name and extra excluded)
        assert_eq!(
            columns,
            vec![ColumnName::new(["id"]), ColumnName::new(["user", "age"]),]
        );
    }

    #[test]
    fn test_stats_column_names_skips_non_eligible_types() {
        let properties: TableProperties = [("key", "value")].into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
            StructField::nullable(
                "metadata",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
            StructField::nullable("name", DataType::STRING),
        ]);

        let columns = stats_column_names(&file_schema, &properties);

        // Array and Map types should be excluded
        assert_eq!(
            columns,
            vec![ColumnName::new(["id"]), ColumnName::new(["name"]),]
        );
    }
}
