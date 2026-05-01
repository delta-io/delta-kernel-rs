//! This module contains logic to compute the expected schema for file statistics

mod column_filter;

use std::borrow::Cow;
use std::sync::Arc;

use column_filter::StatsColumnFilter;
pub(crate) use column_filter::StatsConfig;

use crate::schema::{
    ArrayType, ColumnName, DataType, MapType, PrimitiveType, Schema, SchemaRef, StructField,
    StructType,
};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::DeltaResult;

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
/// Note: Array, Map, and Variant types are included in `nullCount` (null counts are meaningful
/// for these types) but excluded from `minValues`/`maxValues` (not eligible for data skipping).
/// They count as leaf columns against the indexed column limit. The `nullCount` schema also
/// includes primitive types that aren't eligible for min/max (e.g., Boolean, Binary) since null
/// counts are still meaningful for those types.
///
/// The `minValues`/`maxValues` struct fields are also nested structures mirroring the table's
/// column hierarchy. They additionally filter out leaf fields with non-eligible data types
/// (e.g., Boolean, Binary) via [`is_skipping_eligible_datatype`].
///
/// ## Stats value rules
///
/// Statistics returned to kernel must follow these rules:
///
/// - `numRecords`: the total number of rows in the file.
/// - `nullCount`: the number of null values in the column. Always present for included columns.
/// - `minValues`/`maxValues`: the smallest/largest non-null value in the column. When a column
///   contains only null values, there are no non-null values to aggregate, so the column has no
///   entry in `minValues`/`maxValues`. The `nullCount` entry is still present and equals
///   `numRecords`.
/// - String min/max values must be truncated to a prefix no longer than 32 characters. For min
///   values, simple prefix truncation is valid (the truncated value is always <= the original). For
///   max values, a tie-breaker character must be appended after truncation to ensure the result is
///   greater than or equal to all actual values: ASCII DEL (0x7F) when the truncated character is
///   ASCII, or U+10FFFF otherwise. If a valid truncation point cannot be found within 64
///   characters, the max value is omitted (returning `None`).
/// - Binary min/max values are not collected (Binary is not eligible for data skipping).
/// - Boolean values are not eligible for min/max statistics but do have `nullCount`.
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
/// Generates the expected schema for file statistics.
///
/// All inputs (schema, config, and column names) must use the same column naming
/// mode -- either all physical or all logical. The output uses the same naming mode.
///
/// # Parameters
///
/// - `data_schema`: The table's data schema (partition columns excluded).
/// - `config`: Stats configuration controlling which columns are included.
/// - `required_columns`: Columns that must always be included in statistics (write path). Per the
///   Delta protocol, clustering columns must have statistics regardless of table property settings.
/// - `requested_columns`: Filter output to only these columns (read path). If specified, only
///   columns that also pass the `config` filtering will be included.
#[allow(unused)]
pub(crate) fn expected_stats_schema(
    data_schema: &Schema,
    config: &StatsConfig<'_>,
    required_columns: Option<&[ColumnName]>,
    requested_columns: Option<&[ColumnName]>,
) -> DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(5);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_columns, ...)
    // - always include required columns (e.g. clustering columns, per Delta protocol)
    // - optionally filter output to only requested columns
    let mut base_transform = BaseStatsTransform::new(config, required_columns, requested_columns);
    if let Some(base_schema) = base_transform.transform_struct(data_schema) {
        let base_schema = base_schema.into_owned();

        // convert all leaf fields to data type LONG for null count
        let mut null_count_transform = NullCountStatsTransform;
        let null_count_schema = null_count_transform.transform_struct(&base_schema);
        fields.push(StructField::nullable(
            "nullCount",
            null_count_schema.into_owned(),
        ));

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

/// Returns the column names that should have statistics collected.
///
/// This extracts just the column names without building the full stats schema,
/// making it more efficient when only the column list is needed.
///
/// Per the Delta protocol, required columns (e.g. clustering columns) are always included in
/// statistics, regardless of the `delta.dataSkippingStatsColumns` or
/// `delta.dataSkippingNumIndexedCols` settings.
#[allow(unused)]
pub(crate) fn stats_column_names(
    data_schema: &Schema,
    config: &StatsConfig<'_>,
    required_columns: Option<&[ColumnName]>,
) -> Vec<ColumnName> {
    let mut filter = StatsColumnFilter::new(config, required_columns, None);
    let mut columns = Vec::new();
    filter.collect_columns(data_schema, &mut columns);
    columns
}

/// Creates a stats schema from a referenced schema (e.g. columns from a predicate).
/// Returns schema: `{ numRecords, nullCount, minValues, maxValues }`
///
/// This is used to build the schema for parsing JSON stats and for reading stats_parsed
/// from checkpoints when only a subset of columns is needed (e.g. predicate-referenced columns).
pub(crate) fn build_stats_schema(referenced_schema: &StructType) -> Option<SchemaRef> {
    let stats_schema = schema_with_all_fields_nullable(referenced_schema);

    let nullcount_schema = NullCountStatsTransform
        .transform_struct(&stats_schema)
        .into_owned();

    let schema = StructType::new_unchecked([
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("nullCount", nullcount_schema),
        StructField::nullable("minValues", stats_schema.clone()),
        StructField::nullable("maxValues", stats_schema),
    ]);

    // Strip field metadata. The stats types are derived from the table schema, but the metadata on
    // the fields should not be included in the stats fields
    let schema = StripFieldMetadataTransform.transform_struct(&schema);
    Some(Arc::new(schema.into_owned()))
}

/// Strips all field metadata from a schema.
///
/// Field metadata describes the logical table column, not the stats values themselves. This
/// transform strips that metadata, and must be applied to stats schemas to avoid schema possible
/// mismatches when reading `stats_parsed` from older data since that field metadata could have
/// changed.
pub(crate) struct StripFieldMetadataTransform;
impl<'a> SchemaTransform<'a> for StripFieldMetadataTransform {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Cow<'a, StructField> {
        match self.transform(&field.data_type) {
            Cow::Borrowed(_) if field.metadata.is_empty() => Cow::Borrowed(field),
            data_type => Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: field.is_nullable(),
                metadata: Default::default(),
            }),
        }
    }
}

/// Make all fields of a schema nullable.
/// Used for stats schemas where stats may not be available for all columns.
pub(crate) fn schema_with_all_fields_nullable(schema: &Schema) -> Schema {
    NullableStatsTransform.transform_struct(schema).into_owned()
}

/// Transforms a schema to make all fields nullable.
/// Used for stats schemas where stats may not be available for all columns.
pub(crate) struct NullableStatsTransform;
impl<'a> SchemaTransform<'a> for NullableStatsTransform {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Cow<'a, StructField> {
        let data_type = self.transform(&field.data_type);
        make_nullable_field(field, data_type)
    }
}

// helper used by both NullableStatsTransform and BaseStatsTransform
fn make_nullable_field<'a>(
    field: &'a StructField,
    data_type: Cow<'a, DataType>,
) -> Cow<'a, StructField> {
    match data_type {
        Cow::Borrowed(_) if field.is_nullable() => Cow::Borrowed(field),
        data_type => Cow::Owned(StructField {
            name: field.name.clone(),
            data_type: data_type.into_owned(),
            nullable: true,
            metadata: field.metadata.clone(),
        }),
    }
}

/// Converts a stats schema into a nullCount schema where all leaf fields become LONG.
///
/// The nullCount struct field tracks the number of null values for each column.
/// All leaf fields (primitives, arrays, maps, variants) are converted to LONG type
/// since null counts are always integers, while struct fields are recursed into
/// to preserve the nested structure. Field metadata (including column mapping info)
/// is preserved for all fields.
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Cow<'a, StructField> {
        // Only recurse into struct fields; convert all other types (leaf fields) to LONG
        match &field.data_type {
            DataType::Struct(_) => self.recurse_into_struct_field(field),
            _ => Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: DataType::LONG,
                nullable: true,
                metadata: field.metadata.clone(),
            }),
        }
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats -
/// min/max/nullcount/... - additional transformations may be applied.
///
/// All fields in the output are nullable. Clustering columns are always included per
/// the Delta protocol.
#[allow(unused)]
struct BaseStatsTransform<'col> {
    filter: StatsColumnFilter<'col>,
}

impl<'col> BaseStatsTransform<'col> {
    #[allow(unused)]
    fn new(
        config: &StatsConfig<'col>,
        required_columns: Option<&'col [ColumnName]>,
        requested_columns: Option<&'col [ColumnName]>,
    ) -> Self {
        Self {
            filter: StatsColumnFilter::new(config, required_columns, requested_columns),
        }
    }

    /// Checks whether a leaf column (primitive, array, map, or variant) should be included in the
    /// stats schema and records it against the column limit if so. The column limit is based on
    /// schema order, so we count all leaf columns that pass the table filter, but only generate
    /// stats for requested columns.
    fn include_leaf(&mut self) -> bool {
        if !self.filter.should_include_for_table() {
            return false;
        }
        self.filter.record_included();
        self.filter.should_include_for_requested()
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform<'_> {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    // Always traverse struct fields. All non-struct leaf types count against the column limit.
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.filter.enter_field(field.name());
        let data_type = self.transform(&field.data_type);
        self.filter.exit_field();
        Some(make_nullable_field(field, data_type?))
    }

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        self.include_leaf().then_some(Cow::Borrowed(ptype))
    }

    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.include_leaf().then_some(Cow::Borrowed(atype))
    }

    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.include_leaf().then_some(Cow::Borrowed(mtype))
    }

    fn transform_variant(&mut self, vtype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.include_leaf().then_some(Cow::Borrowed(vtype))
    }
}

// removes all fields with non eligible data types
//
// should only be applied to schema processed via `BaseStatsTransform`.
#[allow(unused)]
struct MinMaxStatsTransform;

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    // Array, Map, and Variant fields pass through BaseStatsTransform (for nullCount) but must
    // be excluded from min/max stats.
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
/// This is also used to validate clustering column types, since clustering requires
/// per-file statistics on clustering columns.
///
/// Note: Boolean and Binary are intentionally excluded as min/max statistics provide minimal
/// skipping benefit for low-cardinality or opaque data types.
///
/// See: <https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61>
pub(crate) fn is_skipping_eligible_datatype(data_type: &PrimitiveType) -> bool {
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
            | PrimitiveType::Geometry(_)
            | PrimitiveType::Geography(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArrayType;
    use crate::table_properties::TableProperties;

    fn stats_config_from_table_properties(properties: &TableProperties) -> StatsConfig<'_> {
        StatsConfig {
            data_skipping_stats_columns: properties.data_skipping_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: properties.data_skipping_num_indexed_cols,
        }
    }

    /// Builds an expected stats schema from the given null count and min/max nested schemas.
    fn expected_stats(null_count: StructType, min_max: StructType) -> StructType {
        StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", min_max.clone()),
            StructField::nullable("maxValues", min_max),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ])
    }

    #[test]
    fn test_stats_schema_simple() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();
        let expected = expected_stats(file_schema.clone(), file_schema);

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
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // Expected result: The stats schema should maintain the nested structure
        // but make all fields nullable
        let expected_min_max = NullableStatsTransform
            .transform_struct(&file_schema)
            .into_owned();
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_min_max)
            .into_owned();

        let expected = expected_stats(null_count, expected_min_max);

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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // nullCount includes array fields (tags) as leaf columns
        let expected_null_nested = StructType::new_unchecked([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
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

        let expected = expected_stats(expected_null, expected_fields);

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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        let expected_nested =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let expected_fields = StructType::new_unchecked([StructField::nullable(
            "user.info",
            DataType::Struct(Box::new(expected_nested)),
        )]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .into_owned();

        let expected = expected_stats(null_count, expected_fields);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_stats_columns_with_complex_types() {
        // When dataSkippingStatsColumns explicitly names a complex type column, only that
        // column (plus any other named columns) should appear in the stats schema.
        let properties: TableProperties = [(
            "delta.dataSkippingStatsColumns".to_string(),
            "id,tags".to_string(),
        )]
        .into();

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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // nullCount: only id and tags (explicitly requested)
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
        ]);

        // minValues/maxValues: only id (tags excluded by MinMaxStatsTransform)
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

        let stats_schema = expected_stats_schema(
            &logical_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        let expected_fields =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .into_owned();

        let expected = expected_stats(null_count, expected_fields);

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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: only eligible fields (no boolean, no binary)
        let expected_min_max =
            StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let expected = expected_stats(expected_null_count, expected_min_max);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested schema where some nested fields are eligible for min/max and others
        // aren't
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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

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

        let expected = expected_stats(expected_null_count, expected_min_max);

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

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // nullCount includes boolean, binary, and array (all non-struct types get nullCount)
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
        ]);

        // minValues/maxValues: no fields are eligible (boolean/binary/array excluded)
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_complex_types_count_against_limit() {
        // Array, Map, and Variant are leaf columns that count against the column limit,
        // matching Spark's truncateSchema which counts all non-struct fields.
        // With a limit of 3, if we have: array, map, variant, col1, col2
        // We should get nullCount for the 3 complex types (the first 3 leaf columns),
        // and col1/col2 are excluded by the limit.
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "3".to_string(),
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
            StructField::nullable("v", DataType::unshredded_variant()),
            StructField::nullable("col1", DataType::LONG),
            StructField::nullable("col2", DataType::STRING),
        ]);

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // nullCount includes array, map, and variant (the first 3 leaf columns).
        // col1/col2 are excluded by the limit.
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("tags", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
            StructField::nullable("v", DataType::LONG),
        ]);

        // minValues/maxValues: all 3 complex types are excluded by MinMaxStatsTransform,
        // and col1/col2 are past the limit, so no min/max fields at all.
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_complex_type_consumes_slot_before_primitive() {
        // Validates that a complex type consuming a slot causes a subsequent primitive to
        // be excluded. With limit=2: id (long), tags (array), name (string), only id and
        // tags get stats, name is excluded.
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "2".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
            StructField::nullable("name", DataType::STRING),
        ]);

        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // nullCount: id and tags (first 2 leaf columns). name is excluded.
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
        ]);

        // minValues/maxValues: only id (tags excluded by MinMaxStatsTransform, name past limit)
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

        let config = StatsConfig {
            data_skipping_stats_columns: properties.data_skipping_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: properties.data_skipping_num_indexed_cols,
        };
        let columns = stats_column_names(&file_schema, &config, None);

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

        let config = StatsConfig {
            data_skipping_stats_columns: properties.data_skipping_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: properties.data_skipping_num_indexed_cols,
        };
        let columns = stats_column_names(&file_schema, &config, None);

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

        let config = StatsConfig {
            data_skipping_stats_columns: properties.data_skipping_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: properties.data_skipping_num_indexed_cols,
        };
        let columns = stats_column_names(&file_schema, &config, None);

        // Only specified columns should be included (user.name and extra excluded)
        assert_eq!(
            columns,
            vec![ColumnName::new(["id"]), ColumnName::new(["user", "age"]),]
        );
    }

    #[test]
    fn test_stats_column_names_includes_complex_types() {
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
            StructField::nullable("v", DataType::unshredded_variant()),
            StructField::nullable("name", DataType::STRING),
        ]);

        let config = StatsConfig {
            data_skipping_stats_columns: properties.data_skipping_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: properties.data_skipping_num_indexed_cols,
        };
        let columns = stats_column_names(&file_schema, &config, None);

        // Array, Map, and Variant are leaf columns and included in the stats column list
        assert_eq!(
            columns,
            vec![
                ColumnName::new(["id"]),
                ColumnName::new(["tags"]),
                ColumnName::new(["metadata"]),
                ColumnName::new(["v"]),
                ColumnName::new(["name"]),
            ]
        );
    }

    // ==================== clustering column tests ====================

    #[test]
    fn test_stats_schema_with_clustering_past_limit() {
        // Test that clustering columns are included in stats schema even when past the limit
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "1".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("c", DataType::INTEGER),
        ]);

        // "c" is a clustering column, should be included even though limit is 1
        let clustering_columns = vec![ColumnName::new(["c"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            Some(&clustering_columns),
            None,
        )
        .unwrap();

        // Only "a" (first column) and "c" (clustering) should be included
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
        ]);
        let expected_min_max = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("c", DataType::INTEGER),
        ]);

        let expected = expected_stats(expected_null_count, expected_min_max);

        assert_eq!(&expected, &stats_schema);
    }

    // ==================== requested_columns filtering tests ====================

    #[test]
    fn test_requested_filters_to_single_column() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("value", DataType::INTEGER),
        ]);

        let columns = [ColumnName::new(["id"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        let expected_nested =
            StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let expected = expected_stats(expected_nested.clone(), expected_nested);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_none_requested_returns_full_schema() {
        // None for requested_columns means no output filtering — include all columns
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);

        let with_none = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        // Should include both columns
        let min_values = with_none.field("minValues").expect("should have minValues");
        if let DataType::Struct(inner) = min_values.data_type() {
            assert!(inner.field("id").is_some());
            assert!(inner.field("name").is_some());
        } else {
            panic!("minValues should be a struct");
        }
    }

    #[test]
    fn test_requested_column_outside_limit_excluded() {
        // requested_columns alone does NOT bypass the column limit — only required_columns does
        let properties: TableProperties = [("delta.dataSkippingNumIndexedCols", "1")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);

        // "name" is outside the limit (limit is 1), and is only requested, not required
        let columns = [ColumnName::new(["name"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        // No data columns pass both filters, so only numRecords + tightBounds
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_required_bypasses_limit_with_requested_filter() {
        // When a column is both required AND requested, it bypasses the limit and
        // appears in the output. This is the pattern used by the read path.
        let properties: TableProperties = [("delta.dataSkippingNumIndexedCols", "1")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);

        let columns = [ColumnName::new(["name"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            Some(&columns),
            Some(&columns),
        )
        .unwrap();

        let expected_nested =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let expected_null =
            StructType::new_unchecked([StructField::nullable("name", DataType::LONG)]);

        let expected = expected_stats(expected_null, expected_nested);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_requested_does_not_affect_column_counting() {
        // With num_indexed_cols=2, "id" and "name" are within the limit.
        // requested_columns=["name"] filters the output to just "name",
        // but "id" still counts toward the limit (so "value" stays excluded).
        let properties: TableProperties = [("delta.dataSkippingNumIndexedCols", "2")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("value", DataType::INTEGER),
        ]);

        let columns = [ColumnName::new(["name"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        // Only "name" appears in the output (filtered), even though "id" counted toward the limit
        let expected_nested =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let expected_null =
            StructType::new_unchecked([StructField::nullable("name", DataType::LONG)]);

        let expected = expected_stats(expected_null, expected_nested);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_multiple_requested_columns() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("value", DataType::INTEGER),
        ]);

        let columns = [ColumnName::new(["id"]), ColumnName::new(["name"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        let expected_nested = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);
        let expected_null = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::LONG),
        ]);

        let expected = expected_stats(expected_null, expected_nested);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_nested_requested_column() {
        let properties: TableProperties = [("key", "value")].into();
        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct))),
        ]);

        let columns = [ColumnName::new(["user", "name"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        let expected_user_nested =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let expected_nested = StructType::new_unchecked([StructField::nullable(
            "user",
            DataType::Struct(Box::new(expected_user_nested)),
        )]);

        let expected_user_null =
            StructType::new_unchecked([StructField::nullable("name", DataType::LONG)]);
        let expected_null = StructType::new_unchecked([StructField::nullable(
            "user",
            DataType::Struct(Box::new(expected_user_null)),
        )]);

        let expected = expected_stats(expected_null, expected_nested);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_empty_requested_columns() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);

        // Empty columns list should return the full schema (same as None)
        let columns: [ColumnName; 0] = [];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();
        let full_stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            None,
        )
        .unwrap();

        assert_eq!(&full_stats_schema, &stats_schema);
    }

    #[test]
    fn test_mixed_nested_and_top_requested() {
        let properties: TableProperties = [("key", "value")].into();
        let user_struct = StructType::new_unchecked([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct))),
            StructField::nullable("value", DataType::DOUBLE),
        ]);

        let columns = [ColumnName::new(["id"]), ColumnName::new(["user", "age"])];
        let stats_schema = expected_stats_schema(
            &file_schema,
            &stats_config_from_table_properties(&properties),
            None,
            Some(&columns),
        )
        .unwrap();

        let expected_user_nested =
            StructType::new_unchecked([StructField::nullable("age", DataType::INTEGER)]);
        let expected_nested = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "user",
                DataType::Struct(Box::new(expected_user_nested.clone())),
            ),
        ]);

        let expected_user_null =
            StructType::new_unchecked([StructField::nullable("age", DataType::LONG)]);
        let expected_null = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_user_null))),
        ]);

        let expected = expected_stats(expected_null, expected_nested);

        assert_eq!(&expected, &stats_schema);
    }
}
