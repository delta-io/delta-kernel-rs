//! This module contains logic to compute the expected schema for file statistics

mod column_filter;

use std::borrow::Cow;
use std::sync::Arc;

use crate::{
    schema::{
        ArrayType, ColumnMetadataKey, ColumnName, DataType, MapType, PrimitiveType, Schema,
        SchemaRef, SchemaTransform, StructField, StructType,
    },
    table_features::ColumnMappingMode,
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
/// Generates the expected schema for file statistics using logical column names.
///
/// To get the physical stats schema (with physical column names for column mapping),
/// call `make_physical(column_mapping_mode)` on the result.
///
/// # Parameters
///
/// - `clustering_columns`: Columns that must always be included in statistics (write path).
///   Per the Delta protocol, clustering columns must have statistics regardless of table
///   property settings.
/// - `requested_columns`: Filter output to only these columns (read path). If specified,
///   only columns that also pass table property filtering will be included.
#[allow(unused)]
pub(crate) fn expected_stats_schema(
    logical_data_schema: &Schema,
    table_properties: &TableProperties,
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
    let mut base_transform =
        BaseStatsTransform::new(table_properties, required_columns, requested_columns);
    if let Some(base_schema) = base_transform.transform_struct(logical_data_schema) {
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

/// Returns the list of logical column names that should have statistics collected.
///
/// This extracts just the column names without building the full stats schema,
/// making it more efficient when only the column list is needed.
///
/// Per the Delta protocol, required columns (e.g. clustering columns) are always included in
/// statistics, regardless of the `delta.dataSkippingStatsColumns` or
/// `delta.dataSkippingNumIndexedCols` settings.
#[allow(unused)]
pub(crate) fn stats_column_names(
    logical_data_schema: &Schema,
    table_properties: &TableProperties,
    required_columns: Option<&[ColumnName]>,
) -> Vec<ColumnName> {
    let mut filter = StatsColumnFilter::new(table_properties, required_columns, None);
    let mut columns = Vec::new();
    filter.collect_columns(logical_data_schema, &mut columns);
    columns
}

/// Creates a stats schema from a referenced schema (e.g. columns from a predicate).
/// Returns schema: `{ numRecords, nullCount, minValues, maxValues }`
///
/// This is used to build the schema for parsing JSON stats and for reading stats_parsed
/// from checkpoints when only a subset of columns is needed (e.g. predicate-referenced columns).
pub(crate) fn build_stats_schema(referenced_schema: &StructType) -> Option<SchemaRef> {
    let stats_schema = NullableStatsTransform
        .transform_struct(referenced_schema)?
        .into_owned();

    let nullcount_schema = NullCountStatsTransform
        .transform_struct(&stats_schema)?
        .into_owned();

    Some(Arc::new(StructType::new_unchecked([
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("nullCount", nullcount_schema),
        StructField::nullable("minValues", stats_schema.clone()),
        StructField::nullable("maxValues", stats_schema),
    ])))
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
/// to preserve the nested structure. Field metadata (including column mapping info)
/// is preserved for all fields.
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        // Only recurse into struct fields; convert all other types (leaf fields) to LONG
        match &field.data_type {
            DataType::Struct(_) => self.recurse_into_struct_field(field),
            _ => Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: DataType::LONG,
                nullable: true,
                metadata: field.metadata.clone(),
            })),
        }
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats - min/max/nullcount/... -
/// additional transformations may be applied.
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
        props: &'col TableProperties,
        required_columns: Option<&'col [ColumnName]>,
        requested_columns: Option<&'col [ColumnName]>,
    ) -> Self {
        Self {
            filter: StatsColumnFilter::new(props, required_columns, requested_columns),
        }
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform<'_> {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

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
        // but we only include leaf fields if they qualify based on table properties.
        // The n_columns limit is based on schema order, so we count columns that pass
        // the table filter regardless of requested_columns.
        if !matches!(data_type, DataType::Struct(_)) {
            if !self.filter.should_include_for_table() {
                self.filter.exit_field();
                return None;
            }
            // Count this column toward the n_columns limit
            self.filter.record_included();

            // After recording the column for counting purposes, check if it passes the
            // requested columns filter. This does not affect the column count.
            if !self.filter.should_include_for_requested() {
                self.filter.exit_field();
                return None;
            }
        }

        let field = match self.transform(&field.data_type) {
            Some(Borrowed(_)) if field.is_nullable() => Borrowed(field),
            Some(data_type) => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: field.metadata.clone(),
            }),
            None => {
                self.filter.exit_field();
                return None;
            }
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
    )
}

/// Converts a stats schema's nested data fields to use physical column names without injecting
/// parquet field IDs.
///
/// The stats schema has wrapper fields (`numRecords`, `nullCount`, `minValues`, `maxValues`,
/// `tightBounds`) that don't have column mapping metadata. Only the nested struct fields inside
/// `nullCount`, `minValues`, and `maxValues` need physical name conversion.
///
/// Unlike [`StructField::make_physical`], this transform does not inject
/// [`ColumnMetadataKey::ParquetFieldId`] metadata. The physical stats schema is used to read stats
/// from JSON commit files and from `stats_parsed` in checkpoint Parquet files. Neither format uses
/// parquet field IDs: JSON doesn't use them at all, and checkpoint files are written without them.
/// Injecting field IDs would cause engines to attempt field-ID-based column matching against
/// checkpoint files that have no field IDs.
///
/// [`StructField::make_physical`]: crate::schema::StructField::make_physical
/// [`ColumnMetadataKey::ParquetFieldId`]: crate::schema::ColumnMetadataKey::ParquetFieldId
pub(crate) struct PhysicalStatsSchemaTransform {
    pub column_mapping_mode: ColumnMappingMode,
}

impl<'a> SchemaTransform<'a> for PhysicalStatsSchemaTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        match field.data_type() {
            DataType::Struct(inner) => {
                let physical_inner = MakePhysicalStatsNames {
                    column_mapping_mode: self.column_mapping_mode,
                }
                .transform_struct(inner)?
                .into_owned();
                Some(Cow::Owned(StructField {
                    name: field.name.clone(),
                    data_type: DataType::Struct(Box::new(physical_inner)),
                    nullable: field.nullable,
                    metadata: field.metadata.clone(),
                }))
            }
            // Primitive fields (numRecords, tightBounds) don't need conversion
            _ => Some(Cow::Borrowed(field)),
        }
    }
}

/// Recursively converts fields to physical names, stripping all column mapping metadata
/// (including [`ColumnMetadataKey::ParquetFieldId`]).
///
/// [`ColumnMetadataKey::ParquetFieldId`]: crate::schema::ColumnMetadataKey::ParquetFieldId
struct MakePhysicalStatsNames {
    column_mapping_mode: ColumnMappingMode,
}

impl<'a> SchemaTransform<'a> for MakePhysicalStatsNames {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        let field = self.recurse_into_struct_field(field)?;
        let name = field.physical_name(self.column_mapping_mode).to_owned();
        let metadata = field
            .metadata
            .iter()
            .filter(|(k, _)| {
                k.as_str() != ColumnMetadataKey::ColumnMappingId.as_ref()
                    && k.as_str() != ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()
                    && k.as_str() != ColumnMetadataKey::ParquetFieldId.as_ref()
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Some(Cow::Owned(StructField {
            name,
            data_type: field.data_type.clone(),
            nullable: field.nullable,
            metadata,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::ArrayType;

    use super::*;

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();
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
        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let expected = expected_stats(null_count, expected_fields);

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

        let stats_schema = expected_stats_schema(&logical_schema, &properties, None, None).unwrap();

        let expected_fields =
            StructType::new_unchecked([StructField::nullable("name", DataType::STRING)]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let stats_schema = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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

        let expected = expected_stats(expected_null_count, expected_min_max);

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

        let columns = stats_column_names(&file_schema, &properties, None);

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

        let columns = stats_column_names(&file_schema, &properties, None);

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

        let columns = stats_column_names(&file_schema, &properties, None);

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

        let columns = stats_column_names(&file_schema, &properties, None);

        // Array and Map types should be excluded
        assert_eq!(
            columns,
            vec![ColumnName::new(["id"]), ColumnName::new(["name"]),]
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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, Some(&clustering_columns), None)
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

    // ==================== PhysicalStatsSchemaTransform tests ====================

    fn field_with_physical_name(
        logical_name: &str,
        physical_name: &str,
        data_type: DataType,
    ) -> StructField {
        serde_json::from_value(serde_json::json!({
            "name": logical_name,
            "type": data_type,
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": physical_name
            }
        }))
        .unwrap()
    }

    #[test]
    fn test_physical_stats_transform_preserves_wrapper_field_names() {
        // The stats schema wrapper fields (numRecords, tightBounds) should keep their names
        let stats_schema = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ]);

        let result = PhysicalStatsSchemaTransform {
            column_mapping_mode: ColumnMappingMode::Name,
        }
        .transform_struct(&stats_schema)
        .unwrap()
        .into_owned();

        // Wrapper fields should be unchanged
        assert!(result.field("numRecords").is_some());
        assert!(result.field("tightBounds").is_some());
    }

    #[test]
    fn test_physical_stats_transform_converts_nested_fields() {
        // Create a stats schema with nested struct fields that have column mapping metadata
        let inner_schema = StructType::new_unchecked([
            field_with_physical_name("col_a", "phys_a", DataType::LONG),
            field_with_physical_name("col_b", "phys_b", DataType::STRING),
        ]);

        let stats_schema = expected_stats(inner_schema.clone(), inner_schema);

        let result = PhysicalStatsSchemaTransform {
            column_mapping_mode: ColumnMappingMode::Name,
        }
        .transform_struct(&stats_schema)
        .unwrap()
        .into_owned();

        // Wrapper field names should be preserved
        assert!(result.field("numRecords").is_some());
        assert!(result.field("nullCount").is_some());
        assert!(result.field("minValues").is_some());
        assert!(result.field("maxValues").is_some());
        assert!(result.field("tightBounds").is_some());

        // Nested fields should have physical names
        if let DataType::Struct(inner) = result.field("minValues").unwrap().data_type() {
            assert!(
                inner.field("phys_a").is_some(),
                "Should have physical name phys_a"
            );
            assert!(
                inner.field("phys_b").is_some(),
                "Should have physical name phys_b"
            );
            assert!(
                inner.field("col_a").is_none(),
                "Should not have logical name"
            );
        } else {
            panic!("Expected minValues to be a struct");
        }
    }

    #[test]
    fn test_physical_stats_transform_with_deeply_nested_struct() {
        // Test with nested struct inside data columns
        let user_field: StructField = serde_json::from_value(serde_json::json!({
            "name": "user",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "name",
                        "type": "string",
                        "nullable": true,
                        "metadata": {
                            "delta.columnMapping.id": 2,
                            "delta.columnMapping.physicalName": "phys_name"
                        }
                    },
                    {
                        "name": "address",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "city",
                                    "type": "string",
                                    "nullable": true,
                                    "metadata": {
                                        "delta.columnMapping.id": 3,
                                        "delta.columnMapping.physicalName": "phys_city"
                                    }
                                }
                            ]
                        },
                        "nullable": true,
                        "metadata": {
                            "delta.columnMapping.id": 4,
                            "delta.columnMapping.physicalName": "phys_address"
                        }
                    }
                ]
            },
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": "phys_user"
            }
        }))
        .unwrap();

        let inner_schema = StructType::new_unchecked([user_field]);

        let stats_schema = StructType::new_unchecked([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("minValues", DataType::Struct(Box::new(inner_schema))),
        ]);

        let result = PhysicalStatsSchemaTransform {
            column_mapping_mode: ColumnMappingMode::Name,
        }
        .transform_struct(&stats_schema)
        .unwrap()
        .into_owned();

        // Check that deeply nested fields have physical names
        let min_values = result.field("minValues").unwrap();
        if let DataType::Struct(inner) = min_values.data_type() {
            // Top-level data field should have physical name
            assert!(inner.field("phys_user").is_some());
            assert!(inner.field("user").is_none());

            // Nested struct field should also have physical names
            if let DataType::Struct(user_inner) = inner.field("phys_user").unwrap().data_type() {
                assert!(user_inner.field("phys_name").is_some());
                assert!(user_inner.field("phys_address").is_some());

                // Deeply nested field
                if let DataType::Struct(addr_inner) =
                    user_inner.field("phys_address").unwrap().data_type()
                {
                    assert!(addr_inner.field("phys_city").is_some());
                } else {
                    panic!("Expected address to be a struct");
                }
            } else {
                panic!("Expected user to be a struct");
            }
        } else {
            panic!("Expected minValues to be a struct");
        }
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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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

        let with_none = expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, Some(&columns), Some(&columns))
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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();
        let full_stats_schema =
            expected_stats_schema(&file_schema, &properties, None, None).unwrap();

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
        let stats_schema =
            expected_stats_schema(&file_schema, &properties, None, Some(&columns)).unwrap();

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
