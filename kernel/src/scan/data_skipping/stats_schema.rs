use std::borrow::Cow;

use crate::{
    schema::{
        ArrayType, ColumnName, DataType, MapType, PrimitiveType, Schema, SchemaTransform,
        StructField, StructType,
    },
    table_properties::{DataSkippingNumIndexedCols, TableProperties},
};

/// Generates the expected schema for file statistics.
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> Schema {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - omit map, array, and variant fields
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_coliumns, ...)
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

    StructType::new(fields)
}

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

// Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_primitive(&mut self, _ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Owned(PrimitiveType::Long))
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats - min/max/nullcount/... -
/// additional transformations may be applied.
///
/// The concrete shape of the schema depends on the table configuration.
/// * `dataSkippingStatsColumns` - used to explicitly specify the columns
///   to be used for data skipping statistics. (takes precedence)
/// * `dataSkippingNumIndexedCols` - used to specify the number of columns
///   to be used for data skipping statistics. Defaults to 32.
///
/// All fields are nullable.
struct BaseStatsTransform {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    path: Vec<String>,
}

impl BaseStatsTransform {
    fn new(props: &TableProperties) -> Self {
        // if data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified
        if let Some(columns_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_names: Some(columns_names.clone()),
                path: Vec::new(),
            }
        } else {
            Self {
                n_columns: Some(
                    props
                        .data_skipping_num_indexed_cols
                        .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
                ),
                added_columns: 0,
                column_names: None,
                path: Vec::new(),
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform {
    // array and map fields are not eligible for data skipping, so filter them out.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        None
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        // Check if the number of columns is set and if the added columns exceed the limit
        // In the constructor we assert this will always be None if column_names are specified
        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
            if self.added_columns >= n_cols {
                return None;
            }
        }

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        // keep the field if it:
        // - is a struct field and we need to traverse its children
        // - OR it is referenced by the column names
        // - OR it is a primitive type / leaf field
        let should_include = matches!(data_type, DataType::Struct(_))
            || self
                .column_names
                .as_ref()
                .map(|ns| should_include_column(&ColumnName::new(&self.path), ns))
                .unwrap_or_else(|| {
                    !matches!(
                        data_type,
                        DataType::Map(_) | DataType::Array(_) | DataType::Variant(_)
                    )
                });

        if !should_include {
            self.path.pop();
            return None;
        }

        // increment count only for leaf columns.
        if matches!(data_type, DataType::Primitive(_)) {
            self.added_columns += 1;
        }

        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                // TODO: should we consider the metadata here?
                // Things like column ids would surely not make sense as this is a different field
                // in a different file
                metadata: field.metadata.clone(),
            }),
        };

        self.path.pop();

        // exclude struct fields with no children
        if matches!(field.data_type(), DataType::Struct(dt) if dt.fields.is_empty()) {
            None
        } else {
            Some(field)
        }
    }
}

// removes all fields with non eligible data types
//
// should only be applied to schema oricessed via `BaseStatsTransform`.
struct MinMaxStatsTransform;
impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if is_skipping_eligeble_datatype(ptype) {
            Some(Cow::Borrowed(ptype))
        } else {
            None
        }
    }
}

// Checks if a column should be included or traversed into.
//
// Returns true if the column name is included in the list of column names
// or if the column name is a prefix of any column name in the list.
fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names
        .iter()
        .any(|name| name.as_ref().starts_with(column_name))
}

/// Checks if a data type is eligible for min/max file skipping.
/// https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61
fn is_skipping_eligeble_datatype(data_type: &PrimitiveType) -> bool {
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
            // | &PrimitiveType::Boolean
            | PrimitiveType::Decimal(_)
    )
}

#[cfg(test)]
mod tests {
    use crate::schema::ArrayType;

    use super::*;

    #[test]
    fn test_should_include_column() {
        let full_name = vec![ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl4"])];
        let parent = ColumnName::new(["lvl1", "lvl2", "lvl3"]);
        assert!(should_include_column(&parent, &full_name));
        assert!(should_include_column(&full_name[0], &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl5"]);
        assert!(!should_include_column(&not_parent, &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl3", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));
    }

    #[test]
    fn test_stats_schema_simple() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new([StructField::nullable("id", DataType::LONG)]);

        let stats_schema = stats_schema(&file_schema, &properties);
        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", file_schema.clone()),
            StructField::nullable("minValues", file_schema.clone()),
            StructField::nullable("maxValues", file_schema),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested() {
        let properties: TableProperties = [("key", "value")].into();

        let user_struct = StructType::new([
            StructField::not_null("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("user", DataType::Struct(Box::new(user_struct.clone()))),
        ]);
        let stats_schema = stats_schema(&file_schema, &properties);

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

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
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
        let metadata_struct = StructType::new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("tags", array_type),
            StructField::nullable("score", DataType::DOUBLE),
        ]);
        let file_schema = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "metadata",
                DataType::Struct(Box::new(metadata_struct.clone())),
            ),
        ]);

        let stats_schema = stats_schema(&file_schema, &properties);

        let expected_nested = StructType::new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("score", DataType::DOUBLE),
        ]);
        let expected_fields = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("metadata", DataType::Struct(Box::new(expected_nested))),
        ]);

        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
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

        let user_struct = StructType::new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let file_schema = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user.info", DataType::Struct(Box::new(user_struct.clone()))),
        ]);

        let stats_schema = stats_schema(&file_schema, &properties);

        let expected_nested = StructType::new([StructField::nullable("name", DataType::STRING)]);
        let expected_fields = StructType::new([StructField::nullable(
            "user.info",
            DataType::Struct(Box::new(expected_nested)),
        )]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
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

        let logical_schema = StructType::new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);

        let stats_schema = stats_schema(&logical_schema, &properties);

        let expected_fields = StructType::new([StructField::nullable("name", DataType::STRING)]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
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
        let file_schema = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
        ]);

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: only eligible fields (no boolean, no binary)
        let expected_min_max = StructType::new([StructField::nullable("id", DataType::LONG)]);

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested schema where some nested fields are eligible for min/max and others aren't
        let user_struct = StructType::new([
            StructField::nullable("name", DataType::STRING), // eligible for min/max
            StructField::nullable("is_admin", DataType::BOOLEAN), // NOT eligible for min/max
            StructField::nullable("age", DataType::INTEGER), // eligible for min/max
            StructField::nullable("profile_pic", DataType::BINARY), // NOT eligible for min/max
        ]);

        let file_schema = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct.clone()))),
            StructField::nullable("is_deleted", DataType::BOOLEAN), // NOT eligible for min/max
        ]);

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG, maintaining structure
        let expected_null_user = StructType::new([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("is_admin", DataType::LONG),
            StructField::nullable("age", DataType::LONG),
            StructField::nullable("profile_pic", DataType::LONG),
        ]);
        let expected_null_count = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_null_user))),
            StructField::nullable("is_deleted", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: only eligible fields
        let expected_minmax_user = StructType::new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]);
        let expected_min_max = StructType::new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_minmax_user))),
        ]);

        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_only_non_eligible_fields() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a schema with only fields that are NOT eligible for min/max skipping
        let file_schema = StructType::new([
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
        ]);

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::new([
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
            // Note: array fields are filtered out by BaseStatsTransform, so "tags" won't appear
        ]);

        // Expected minValues/maxValues schema: empty since no fields are eligible
        // Since there are no eligible fields, minValues and maxValues should not be present
        let expected = StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            // No minValues or maxValues fields since no primitive fields are eligible
        ]);

        assert_eq!(&expected, &stats_schema);
    }
}
