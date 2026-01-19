//! Transforms for populating stats and partition values fields in checkpoint data.
//!
//! When writing checkpoints, statistics and partition values can be stored in two formats:
//! - `stats`/`partitionValues`: JSON string/map format (controlled by `writeStatsAsJson`)
//! - `stats_parsed`/`partitionValues_parsed`: Native struct format (controlled by `writeStatsAsStruct`)
//!
//! This module provides transforms to populate these fields using COALESCE expressions,
//! ensuring that data is preserved regardless of the source format (commits vs checkpoints).

use std::sync::Arc;

use crate::actions::ADD_NAME;
use crate::expressions::{
    Expression, ExpressionRef, Transform, UnaryExpressionOp, VariadicExpressionOp,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_properties::TableProperties;

pub(crate) const STATS_FIELD: &str = "stats";
pub(crate) const STATS_PARSED_FIELD: &str = "stats_parsed";
pub(crate) const PARTITION_VALUES_FIELD: &str = "partitionValues";
pub(crate) const PARTITION_VALUES_PARSED_FIELD: &str = "partitionValues_parsed";

/// Configuration for stats transformation based on table properties.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatsTransformConfig {
    pub write_stats_as_json: bool,
    pub write_stats_as_struct: bool,
}

impl StatsTransformConfig {
    pub(super) fn from_table_properties(properties: &TableProperties) -> Self {
        Self {
            write_stats_as_json: properties.checkpoint_write_stats_as_json.unwrap_or(true),
            write_stats_as_struct: properties.checkpoint_write_stats_as_struct.unwrap_or(false),
        }
    }
}

/// Builds a transform for the Add action to populate and/or drop stats and partition values fields.
///
/// The transform handles all scenarios based on table properties:
///
/// Stats fields:
/// - When `writeStatsAsJson=true`: `stats = COALESCE(stats, ToJson(stats_parsed))`
/// - When `writeStatsAsJson=false`: drop `stats` field
/// - When `writeStatsAsStruct=true`: `stats_parsed = COALESCE(stats_parsed, ParseJson(stats))`
/// - When `writeStatsAsStruct=false`: drop `stats_parsed` field
///
/// Partition values fields (when table is partitioned and writeStatsAsStruct=true):
/// - `partitionValues_parsed = COALESCE(partitionValues_parsed, ParsePartitionValues(partitionValues))`
/// - `partitionValues = COALESCE(partitionValues, PartitionValuesToMap(partitionValues_parsed))`
///
/// Returns a top-level transform that wraps the nested Add transform, ensuring the
/// full checkpoint batch is produced with the modified Add action.
pub(crate) fn build_stats_transform(
    config: &StatsTransformConfig,
    stats_schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
) -> ExpressionRef {
    let mut add_transform = Transform::new_nested([ADD_NAME]);

    // Handle stats field
    if config.write_stats_as_json {
        // Populate stats from stats_parsed if needed (for old checkpoints that only had stats_parsed)
        let stats_expr = build_stats_json_expr();
        add_transform = add_transform.with_replaced_field(STATS_FIELD, stats_expr);
    } else {
        // Drop stats field when not writing as JSON
        add_transform = add_transform.with_dropped_field(STATS_FIELD);
    }

    // Handle stats_parsed field
    // Note: stats_parsed was added to read schema (via build_checkpoint_read_schema_with_stats),
    // so we always need to either replace it (with COALESCE) or drop it.
    if config.write_stats_as_struct {
        // Populate stats_parsed from JSON stats (for commits that only have JSON stats)
        let stats_parsed_expr = build_stats_parsed_expr(stats_schema);
        add_transform = add_transform.with_replaced_field(STATS_PARSED_FIELD, stats_parsed_expr);
    } else {
        // Drop stats_parsed field when not writing as struct
        add_transform = add_transform.with_dropped_field(STATS_PARSED_FIELD);
    }

    // Handle partition values fields (only for partitioned tables)
    if let Some(partition_schema) = partition_schema {
        if config.write_stats_as_struct {
            // Add partitionValues_parsed with COALESCE expression
            let pv_parsed_expr = build_partition_values_parsed_expr(partition_schema);
            add_transform =
                add_transform.with_replaced_field(PARTITION_VALUES_PARSED_FIELD, pv_parsed_expr);
        } else {
            // Drop partitionValues_parsed field when not writing as struct
            add_transform = add_transform.with_dropped_field(PARTITION_VALUES_PARSED_FIELD);
        }

        // Always populate partitionValues for backward compatibility
        let pv_map_expr = build_partition_values_map_expr();
        add_transform = add_transform.with_replaced_field(PARTITION_VALUES_FIELD, pv_map_expr);
    }

    // Wrap the nested Add transform in a top-level transform that replaces the Add field
    let add_transform_expr: ExpressionRef = Arc::new(Expression::transform(add_transform));
    let outer_transform =
        Transform::new_top_level().with_replaced_field(ADD_NAME, add_transform_expr);

    Arc::new(Expression::transform(outer_transform))
}

/// Builds expression: `stats_parsed = COALESCE(stats_parsed, ParseJson(stats, schema))`
///
/// This expression prefers existing stats_parsed, falling back to parsing JSON stats.
/// Column paths are relative to the full batch (not the nested Add struct), so we use
/// ["add", "stats"] instead of just ["stats"].
fn build_stats_parsed_expr(stats_schema: SchemaRef) -> ExpressionRef {
    Arc::new(Expression::variadic(
        VariadicExpressionOp::Coalesce,
        vec![
            Expression::column([ADD_NAME, STATS_PARSED_FIELD]),
            Expression::parse_json(Expression::column([ADD_NAME, STATS_FIELD]), stats_schema),
        ],
    ))
}

/// Builds expression: `stats = COALESCE(stats, ToJson(stats_parsed))`
///
/// This expression prefers existing JSON stats, falling back to converting stats_parsed.
/// Column paths are relative to the full batch (not the nested Add struct), so we use
/// ["add", "stats"] instead of just ["stats"].
fn build_stats_json_expr() -> ExpressionRef {
    Arc::new(Expression::variadic(
        VariadicExpressionOp::Coalesce,
        vec![
            Expression::column([ADD_NAME, STATS_FIELD]),
            Expression::unary(
                UnaryExpressionOp::ToJson,
                Expression::column([ADD_NAME, STATS_PARSED_FIELD]),
            ),
        ],
    ))
}

/// Builds expression: `partitionValues_parsed = COALESCE(partitionValues_parsed, ParsePartitionValues(partitionValues, schema))`
///
/// This expression prefers existing partitionValues_parsed, falling back to parsing the string map.
fn build_partition_values_parsed_expr(partition_schema: SchemaRef) -> ExpressionRef {
    Arc::new(Expression::variadic(
        VariadicExpressionOp::Coalesce,
        vec![
            Expression::column([ADD_NAME, PARTITION_VALUES_PARSED_FIELD]),
            Expression::parse_partition_values(
                Expression::column([ADD_NAME, PARTITION_VALUES_FIELD]),
                partition_schema,
            ),
        ],
    ))
}

/// Builds expression: `partitionValues = COALESCE(partitionValues, PartitionValuesToMap(partitionValues_parsed))`
///
/// This expression prefers existing partitionValues map, falling back to converting from typed struct.
fn build_partition_values_map_expr() -> ExpressionRef {
    Arc::new(Expression::variadic(
        VariadicExpressionOp::Coalesce,
        vec![
            Expression::column([ADD_NAME, PARTITION_VALUES_FIELD]),
            Expression::partition_values_to_map(Expression::column([
                ADD_NAME,
                PARTITION_VALUES_PARSED_FIELD,
            ])),
        ],
    ))
}

/// Builds a read schema that includes `stats_parsed` and optionally `partitionValues_parsed`
/// in the Add action.
///
/// The read schema must include these fields for ALL reads (checkpoints + commits)
/// even though commits don't have them. This ensures the columns exist
/// (as nulls) so COALESCE can operate correctly.
pub(crate) fn build_checkpoint_read_schema_with_stats(
    base_schema: &StructType,
    stats_schema: &StructType,
    partition_schema: Option<&StructType>,
) -> SchemaRef {
    let fields: Vec<StructField> = base_schema
        .fields()
        .map(|field| {
            if field.name == ADD_NAME {
                if let DataType::Struct(add_struct) = &field.data_type {
                    let modified_add =
                        add_parsed_fields_to_add_schema(add_struct, stats_schema, partition_schema);
                    return StructField {
                        name: field.name.clone(),
                        data_type: DataType::Struct(Box::new(modified_add)),
                        nullable: field.nullable,
                        metadata: field.metadata.clone(),
                    };
                }
            }
            field.clone()
        })
        .collect();

    Arc::new(StructType::new_unchecked(fields))
}

/// Adds `stats_parsed` and optionally `partitionValues_parsed` fields to the Add action schema.
fn add_parsed_fields_to_add_schema(
    add_schema: &StructType,
    stats_schema: &StructType,
    partition_schema: Option<&StructType>,
) -> StructType {
    let mut fields: Vec<StructField> = Vec::new();

    for field in add_schema.fields() {
        fields.push(field.clone());
        if field.name == PARTITION_VALUES_FIELD {
            // Insert partitionValues_parsed right after partitionValues (if partitioned)
            if let Some(pv_schema) = partition_schema {
                fields.push(StructField::nullable(
                    PARTITION_VALUES_PARSED_FIELD,
                    DataType::Struct(Box::new(pv_schema.clone())),
                ));
            }
        } else if field.name == STATS_FIELD {
            // Insert stats_parsed right after stats
            fields.push(StructField::nullable(
                STATS_PARSED_FIELD,
                DataType::Struct(Box::new(stats_schema.clone())),
            ));
        }
    }

    StructType::new_unchecked(fields)
}

/// Builds the output schema based on configuration.
///
/// The output schema determines which fields are included in the checkpoint:
/// - If `writeStatsAsJson=false`: `stats` field is excluded
/// - If `writeStatsAsStruct=true`: `stats_parsed` and `partitionValues_parsed` fields are included
pub(crate) fn build_checkpoint_output_schema(
    config: &StatsTransformConfig,
    base_schema: &StructType,
    stats_schema: &StructType,
    partition_schema: Option<&StructType>,
) -> SchemaRef {
    let fields: Vec<StructField> = base_schema
        .fields()
        .map(|field| {
            if field.name == ADD_NAME {
                if let DataType::Struct(add_struct) = &field.data_type {
                    let modified_add =
                        build_add_output_schema(config, add_struct, stats_schema, partition_schema);
                    return StructField {
                        name: field.name.clone(),
                        data_type: DataType::Struct(Box::new(modified_add)),
                        nullable: field.nullable,
                        metadata: field.metadata.clone(),
                    };
                }
            }
            field.clone()
        })
        .collect();

    Arc::new(StructType::new_unchecked(fields))
}

fn build_add_output_schema(
    config: &StatsTransformConfig,
    add_schema: &StructType,
    stats_schema: &StructType,
    partition_schema: Option<&StructType>,
) -> StructType {
    let mut fields: Vec<StructField> = Vec::new();

    for field in add_schema.fields() {
        if field.name == PARTITION_VALUES_FIELD {
            // Always include partitionValues for backward compatibility
            fields.push(field.clone());
            // Add partitionValues_parsed after partitionValues position if writing as struct
            if config.write_stats_as_struct {
                if let Some(pv_schema) = partition_schema {
                    fields.push(StructField::nullable(
                        PARTITION_VALUES_PARSED_FIELD,
                        DataType::Struct(Box::new(pv_schema.clone())),
                    ));
                }
            }
        } else if field.name == STATS_FIELD {
            // Include stats if writing as JSON
            if config.write_stats_as_json {
                fields.push(field.clone());
            }
            // Add stats_parsed after stats position if writing as struct
            if config.write_stats_as_struct {
                fields.push(StructField::nullable(
                    STATS_PARSED_FIELD,
                    DataType::Struct(Box::new(stats_schema.clone())),
                ));
            }
        } else {
            fields.push(field.clone());
        }
    }

    StructType::new_unchecked(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        // Default: writeStatsAsJson=true, writeStatsAsStruct=false
        let props = TableProperties::default();
        let config = StatsTransformConfig::from_table_properties(&props);
        assert!(config.write_stats_as_json);
        assert!(!config.write_stats_as_struct);
    }

    #[test]
    fn test_config_with_struct_enabled() {
        let props = TableProperties {
            checkpoint_write_stats_as_struct: Some(true),
            ..Default::default()
        };
        let config = StatsTransformConfig::from_table_properties(&props);
        assert!(config.write_stats_as_json);
        assert!(config.write_stats_as_struct);
    }

    #[test]
    fn test_build_transform_with_json_only() {
        // writeStatsAsJson=true, writeStatsAsStruct=false (default)
        // Should produce a transform expression that replaces stats with COALESCE and drops stats_parsed
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: false,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, stats_schema, None);

        // Verify we get a Transform expression
        let Expression::Transform(_) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };
    }

    #[test]
    fn test_build_transform_drops_both_when_false() {
        // writeStatsAsJson=false, writeStatsAsStruct=false
        // Should produce a transform expression that drops both stats and stats_parsed
        let config = StatsTransformConfig {
            write_stats_as_json: false,
            write_stats_as_struct: false,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, stats_schema, None);

        // Verify we get a Transform expression
        let Expression::Transform(_) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };
    }

    #[test]
    fn test_build_transform_with_both_enabled() {
        // writeStatsAsJson=true, writeStatsAsStruct=true
        // Should produce a transform expression that populates both stats and stats_parsed
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: true,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, stats_schema, None);

        // Verify we get a Transform expression
        let Expression::Transform(_) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };
    }

    #[test]
    fn test_build_transform_struct_only() {
        // writeStatsAsJson=false, writeStatsAsStruct=true
        // Should produce a transform expression that drops stats and populates stats_parsed
        let config = StatsTransformConfig {
            write_stats_as_json: false,
            write_stats_as_struct: true,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, stats_schema, None);

        // Verify we get a Transform expression
        let Expression::Transform(_) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };
    }

    #[test]
    fn test_build_transform_with_partition_values() {
        // Test transform with partition values
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: true,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let partition_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("year", DataType::INTEGER),
            StructField::nullable("month", DataType::INTEGER),
        ]));
        let transform_expr = build_stats_transform(&config, stats_schema, Some(partition_schema));

        // Verify we get a Transform expression
        let Expression::Transform(_) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };
    }

    #[test]
    fn test_add_parsed_fields_to_add_schema() {
        use crate::schema::MapType;
        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable(
                "partitionValues",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
            StructField::nullable("stats", DataType::STRING),
            StructField::nullable("tags", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);

        let partition_schema =
            StructType::new_unchecked([StructField::nullable("year", DataType::INTEGER)]);

        let result =
            add_parsed_fields_to_add_schema(&add_schema, &stats_schema, Some(&partition_schema));

        // Should have 6 fields: path, partitionValues, partitionValues_parsed, stats, stats_parsed, tags
        assert_eq!(result.fields().count(), 6);

        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "path",
                "partitionValues",
                "partitionValues_parsed",
                "stats",
                "stats_parsed",
                "tags"
            ]
        );
    }

    #[test]
    fn test_add_parsed_fields_to_add_schema_no_partition() {
        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
            StructField::nullable("tags", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);

        let result = add_parsed_fields_to_add_schema(&add_schema, &stats_schema, None);

        // Should have 4 fields: path, stats, stats_parsed, tags
        assert_eq!(result.fields().count(), 4);

        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["path", "stats", "stats_parsed", "tags"]);
    }

    #[test]
    fn test_build_add_output_schema_json_only() {
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: false,
        };

        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
        ]);

        let stats_schema = StructType::new_unchecked([]);

        let result = build_add_output_schema(&config, &add_schema, &stats_schema, None);

        // Should have path and stats, no stats_parsed
        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["path", "stats"]);
    }

    #[test]
    fn test_build_add_output_schema_struct_only() {
        let config = StatsTransformConfig {
            write_stats_as_json: false,
            write_stats_as_struct: true,
        };

        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);

        let result = build_add_output_schema(&config, &add_schema, &stats_schema, None);

        // Should have path and stats_parsed (stats dropped)
        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["path", "stats_parsed"]);
    }

    #[test]
    fn test_build_add_output_schema_both() {
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: true,
        };

        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);

        let result = build_add_output_schema(&config, &add_schema, &stats_schema, None);

        // Should have path, stats, and stats_parsed
        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["path", "stats", "stats_parsed"]);
    }

    #[test]
    fn test_build_add_output_schema_with_partition_values() {
        use crate::schema::MapType;
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: true,
        };

        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable(
                "partitionValues",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
            StructField::nullable("stats", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);
        let partition_schema =
            StructType::new_unchecked([StructField::nullable("year", DataType::INTEGER)]);

        let result =
            build_add_output_schema(&config, &add_schema, &stats_schema, Some(&partition_schema));

        // Should have path, partitionValues, partitionValues_parsed, stats, stats_parsed
        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "path",
                "partitionValues",
                "partitionValues_parsed",
                "stats",
                "stats_parsed"
            ]
        );
    }
}
