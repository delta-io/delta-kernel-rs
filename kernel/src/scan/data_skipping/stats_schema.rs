//! This module contains logic to compute the expected schema for file statistics

use std::borrow::Cow;

use crate::{
    schema::{
        ArrayType, ColumnName, DataType, MapType, PrimitiveType, Schema, SchemaTransform,
        StructField, StructType,
    },
    table_properties::{DataSkippingNumIndexedCols, TableProperties},
    DeltaResult,
};

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
/// are converted to LONG type (since null counts are always integers). Maps, arrays, and
/// variants are considered leaf fields. Unlike `minValues`/`maxValues`, `nullCount` includes
/// all columns from the base schema regardless of data type - every column can have nulls counted.
///
/// Note: `nullCount` still respects the column limit from `dataSkippingNumIndexedCols` or
/// `dataSkippingStatsColumns` (via the base schema). The difference from `minValues`/`maxValues`
/// is only that `nullCount` does not filter by data type eligibility.
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
/// Generates the expected schema for file statistics.
///
/// # Arguments
///
/// * `physical_file_schema` - The physical schema of the data files (no partition columns)
/// * `table_properties` - Table properties containing stats configuration
/// * `clustering_columns` - Optional list of clustering columns that must always be included
///   in statistics regardless of `dataSkippingNumIndexedCols` limits
#[allow(unused)]
pub(crate) fn expected_stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
    clustering_columns: Option<&[ColumnName]>,
) -> DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(5);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_columns, ...)
    // - always include clustering columns (they don't count against the column limit)
    let mut base_transform = BaseStatsTransform::new(table_properties, clustering_columns);
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
///
/// The concrete shape of the schema depends on the table configuration.
/// * `dataSkippingStatsColumns` - used to explicitly specify the columns
///   to be used for data skipping statistics. (takes precedence)
/// * `dataSkippingNumIndexedCols` - used to specify the number of columns
///   to be used for data skipping statistics. Defaults to 32.
/// * Clustering columns are always included regardless of the above limits.
///
/// All fields are nullable.
#[allow(unused)]
struct BaseStatsTransform<'a> {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    /// Clustering columns that must always be included (don't count against limit)
    clustering_columns: Option<&'a [ColumnName]>,
    path: Vec<String>,
}

impl<'a> BaseStatsTransform<'a> {
    #[allow(unused)]
    fn new(props: &TableProperties, clustering_columns: Option<&'a [ColumnName]>) -> Self {
        // If data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified.
        if let Some(column_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_names: Some(column_names.clone()),
                clustering_columns,
                path: Vec::new(),
            }
        } else {
            let n_cols = props
                .data_skipping_num_indexed_cols
                .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32));
            Self {
                n_columns: Some(n_cols),
                added_columns: 0,
                column_names: None,
                clustering_columns,
                path: Vec::new(),
            }
        }
    }

    /// Checks if the current path matches a clustering column.
    /// Clustering columns are always included regardless of column limits.
    fn is_clustering_column(&self) -> bool {
        self.clustering_columns
            .map(|cols| {
                let current = ColumnName::new(&self.path);
                cols.iter().any(|c| c == &current)
            })
            .unwrap_or(false)
    }
}

impl<'a, 'b> SchemaTransform<'a> for BaseStatsTransform<'b> {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        // Clustering columns are always included, regardless of limits
        let is_clustering = self.is_clustering_column();

        // Check if the number of columns is set and if the added columns exceed the limit
        // Clustering columns bypass this limit
        if !is_clustering {
            if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
                if self.added_columns >= n_cols {
                    self.path.pop();
                    return None;
                }
            }
        }

        // We always traverse struct fields (they don't count against the column limit),
        // but we only include leaf fields if they qualify based on column_names config.
        // When column_names is None, all leaf fields are included (up to n_columns limit).
        // Clustering columns are always included regardless of column_names config.
        if !matches!(data_type, DataType::Struct(_)) {
            let should_include = is_clustering
                || self
                    .column_names
                    .as_ref()
                    .map(|ns| should_include_column(&ColumnName::new(&self.path), ns))
                    .unwrap_or(true);

            if !should_include {
                self.path.pop();
                return None;
            }

            // Increment count only for non-clustering leaf columns
            // Clustering columns don't count against the limit
            if !is_clustering {
                self.added_columns += 1;
            }
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

        self.path.pop();

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
    // array and map and variant fields are not eligible for data skipping, so filter them out.
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

// Checks if a column should be included or traversed into.
//
// Returns true if the column name is included in the list of column names
// or if the column name is a prefix of any column name in the list
// or if the column name is a child of any column name in the list
#[allow(unused)]
fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names.iter().any(|name| {
        name.as_ref().starts_with(column_name) || column_name.as_ref().starts_with(name)
    })
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
    fn test_should_include_column() {
        let full_name = vec![ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl4"])];
        let parent = ColumnName::new(["lvl1", "lvl2", "lvl3"]);
        assert!(should_include_column(&parent, &full_name));
        assert!(should_include_column(&full_name[0], &full_name));
        // child fields should also be included
        assert!(should_include_column(&full_name[0], &[parent]));

        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl5"]);
        assert!(!should_include_column(&not_parent, &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl3", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));

        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));
    }

    #[test]
    fn test_stats_schema_simple() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema = StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();
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
        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&logical_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None).unwrap();

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::new_unchecked([
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
        ]);

        // Expected minValues/maxValues schema: empty since no fields are eligible
        // Since there are no eligible fields, minValues and maxValues should not be present
        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            // No minValues or maxValues fields since no primitive fields are eligible
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_clustering_columns_bypass_limit() {
        // Set limit to 1 column, but clustering columns should still be included
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "1".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("cluster_col", DataType::INTEGER),
        ]);

        // Without clustering columns: only first column (id) should be included
        let stats_schema_no_cluster =
            expected_stats_schema(&file_schema, &properties, None).unwrap();

        let expected_no_cluster =
            StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);
        let null_count_no_cluster = NullCountStatsTransform
            .transform_struct(&expected_no_cluster)
            .unwrap()
            .into_owned();

        let expected_no_cluster = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count_no_cluster),
            StructField::nullable("minValues", expected_no_cluster.clone()),
            StructField::nullable("maxValues", expected_no_cluster),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);
        assert_eq!(&expected_no_cluster, &stats_schema_no_cluster);

        // With clustering columns: id (from limit) + cluster_col (clustering) should be included
        let clustering_cols = vec![ColumnName::new(["cluster_col"])];
        let stats_schema_with_cluster =
            expected_stats_schema(&file_schema, &properties, Some(&clustering_cols)).unwrap();

        let expected_with_cluster = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("cluster_col", DataType::INTEGER),
        ]);
        let null_count_with_cluster = NullCountStatsTransform
            .transform_struct(&expected_with_cluster)
            .unwrap()
            .into_owned();

        let expected_with_cluster = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count_with_cluster),
            StructField::nullable("minValues", expected_with_cluster.clone()),
            StructField::nullable("maxValues", expected_with_cluster),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);
        assert_eq!(&expected_with_cluster, &stats_schema_with_cluster);
    }

    #[test]
    fn test_stats_schema_clustering_columns_with_stats_columns() {
        // When dataSkippingStatsColumns is set, clustering columns should still be included
        let properties: TableProperties = [(
            "delta.dataSkippingStatsColumns".to_string(),
            "id".to_string(),
        )]
        .into();

        let file_schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("cluster_col", DataType::INTEGER),
        ]);

        // With dataSkippingStatsColumns=id and clustering_columns=[cluster_col],
        // both should be included
        let clustering_cols = vec![ColumnName::new(["cluster_col"])];
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, Some(&clustering_cols)).unwrap();

        let expected_fields = StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("cluster_col", DataType::INTEGER),
        ]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);
        assert_eq!(&expected, &stats_schema);
    }
}
