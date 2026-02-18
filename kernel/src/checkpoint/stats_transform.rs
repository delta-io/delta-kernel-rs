//! Transforms for populating stats_parsed and stats fields in checkpoint data.
//!
//! This module ensures that Add actions in checkpoints have the correct statistics format
//! based on the table configuration. Statistics can be stored in two formats as fields on
//! the `Add` action:
//! - `stats`: JSON string format, controlled by `delta.checkpoint.writeStatsAsJson` (default: true)
//! - `stats_parsed`: Native struct format, controlled by `delta.checkpoint.writeStatsAsStruct` (default: false)
//!
//! This module provides transforms to populate these fields using COALESCE expressions,
//! ensuring that stats are preserved regardless of the source format (commits vs checkpoints).

use std::sync::{Arc, LazyLock};

use crate::actions::ADD_NAME;
use crate::expressions::{Expression, ExpressionRef, Transform, UnaryExpressionOp};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

pub(crate) const STATS_FIELD: &str = "stats";
pub(crate) const STATS_PARSED_FIELD: &str = "stats_parsed";

/// Configuration for stats transformation based on table properties.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatsTransformConfig {
    pub write_stats_as_json: bool,
    pub write_stats_as_struct: bool,
}

impl StatsTransformConfig {
    pub(super) fn from_table_properties(properties: &TableProperties) -> Self {
        Self {
            write_stats_as_json: properties.should_write_stats_as_json(),
            write_stats_as_struct: properties.should_write_stats_as_struct(),
        }
    }
}

/// Builds a transform for the Add action to populate and/or drop stats fields.
///
/// The transform handles all four scenarios based on table properties:
/// - When `writeStatsAsJson=true`: `stats = COALESCE(stats, ToJson(stats_parsed))`
/// - When `writeStatsAsJson=false`: drop `stats` field
/// - When `writeStatsAsStruct=true`: `stats_parsed = COALESCE(stats_parsed, ParseJson(stats))`
/// - When `writeStatsAsStruct=false`: drop `stats_parsed` field
///
/// Returns a top-level transform that wraps the nested Add transform, ensuring the
/// full checkpoint batch is produced with the modified Add action.
///
/// # Arguments
///
/// * `stats_schema` - The expected schema for parsed file statistics, typically generated
///   by [`expected_stats_schema`]. This schema has the following structure:
///   ```ignore
///   {
///      numRecords: long,
///      nullCount: <nested struct with all leaf fields as long>,
///      minValues: <nested struct matching eligible column types>,
///      maxValues: <nested struct matching eligible column types>,
///   }
///   ```
///   The schema is derived from the table's physical file schema and table properties
///   (`dataSkippingNumIndexedCols`, `dataSkippingStatsColumns`). Only columns eligible
///   for data skipping are included in `minValues`/`maxValues`.
///
/// [`expected_stats_schema`]: crate::scan::data_skipping::stats_schema::expected_stats_schema
pub(crate) fn build_stats_transform(
    config: &StatsTransformConfig,
    stats_schema: &SchemaRef,
) -> ExpressionRef {
    let mut add_transform = Transform::new_nested([ADD_NAME]);

    // Handle stats field
    if config.write_stats_as_json {
        // Populate stats from stats_parsed if needed (for old checkpoints that only had stats_parsed)
        add_transform = add_transform.with_replaced_field(STATS_FIELD, STATS_JSON_EXPR.clone());
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

    // Wrap the nested Add transform in a top-level transform that replaces the Add field
    let add_transform_expr: ExpressionRef = Arc::new(Expression::transform(add_transform));
    let outer_transform =
        Transform::new_top_level().with_replaced_field(ADD_NAME, add_transform_expr);

    Arc::new(Expression::transform(outer_transform))
}

/// Builds a read schema that includes `stats_parsed` in the Add action.
///
/// The read schema must include `stats_parsed` for ALL reads (checkpoints + commits)
/// even though commits don't have `stats_parsed`. This ensures the column exists
/// (as nulls) so COALESCE can operate correctly.
///
/// # Errors
///
/// Returns an error if:
/// - The `add` field is not found or is not a struct type
/// - The `stats_parsed` field already exists in the Add schema
pub(crate) fn build_checkpoint_read_schema_with_stats(
    base_schema: &StructType,
    stats_schema: &StructType,
) -> DeltaResult<SchemaRef> {
    transform_add_schema(base_schema, |add_struct| {
        // Validate stats_parsed isn't already present
        if add_struct.field(STATS_PARSED_FIELD).is_some() {
            return Err(Error::generic(
                "stats_parsed field already exists in Add schema",
            ));
        }
        Ok(add_stats_parsed_to_add_schema(add_struct, stats_schema))
    })
}

/// Builds the output schema based on configuration.
///
/// The output schema determines which fields are included in the checkpoint:
/// - If `writeStatsAsJson=false`: `stats` field is excluded
/// - If `writeStatsAsStruct=true`: `stats_parsed` field is included
///
/// # Errors
///
/// Returns an error if the `add` field is not found or is not a struct type.
pub(crate) fn build_checkpoint_output_schema(
    config: &StatsTransformConfig,
    base_schema: &StructType,
    stats_schema: &StructType,
) -> DeltaResult<SchemaRef> {
    transform_add_schema(base_schema, |add_struct| {
        Ok(build_add_output_schema(config, add_struct, stats_schema))
    })
}

// ========================
// Private helpers
// ========================

/// Builds expression: `stats_parsed = COALESCE(stats_parsed, ParseJson(stats, schema))`
///
/// This expression prefers existing stats_parsed, falling back to parsing JSON stats.
/// If `stats_parsed` is non-null, the data originated from a checkpoint (commits only
/// contain JSON stats, so `stats_parsed` will be null for commit-sourced rows).
///
/// Column paths are relative to the full batch (not the nested Add struct), so we use
/// ["add", "stats"] instead of just ["stats"].
fn build_stats_parsed_expr(stats_schema: &SchemaRef) -> ExpressionRef {
    Arc::new(Expression::coalesce([
        Expression::column([ADD_NAME, STATS_PARSED_FIELD]),
        Expression::parse_json(
            Expression::column([ADD_NAME, STATS_FIELD]),
            stats_schema.clone(),
        ),
    ]))
}

/// Static expression: `stats = COALESCE(stats, ToJson(stats_parsed))`
///
/// This expression prefers existing JSON stats, falling back to converting stats_parsed.
/// Column paths are relative to the full batch (not the nested Add struct), so we use
/// ["add", "stats"] instead of just ["stats"].
static STATS_JSON_EXPR: LazyLock<ExpressionRef> = LazyLock::new(|| {
    Arc::new(Expression::coalesce([
        Expression::column([ADD_NAME, STATS_FIELD]),
        Expression::unary(
            UnaryExpressionOp::ToJson,
            Expression::column([ADD_NAME, STATS_PARSED_FIELD]),
        ),
    ]))
});

/// Transforms the Add action schema within a checkpoint schema.
///
/// This helper applies a transformation function to the Add struct and returns
/// a new schema with the modified Add field.
///
// TODO(https://github.com/delta-io/delta-kernel-rs/issues/1820): Replace manual field
// iteration with StructType helper methods (e.g., with_field_inserted, with_field_removed).
///
/// # Errors
///
/// Returns an error if:
/// - The `add` field is not found in the schema
/// - The `add` field is not a struct type
fn transform_add_schema(
    base_schema: &StructType,
    transform_fn: impl FnOnce(&StructType) -> DeltaResult<StructType>,
) -> DeltaResult<SchemaRef> {
    // Find and validate the add field
    let add_field = base_schema
        .field(ADD_NAME)
        .ok_or_else(|| Error::generic("Expected 'add' field in checkpoint schema"))?;

    let DataType::Struct(add_struct) = &add_field.data_type else {
        return Err(Error::generic(format!(
            "Expected 'add' field to be a struct type, got {:?}",
            add_field.data_type
        )));
    };

    let modified_add = transform_fn(add_struct)?;
    let new_schema = base_schema.with_field_replaced(
        ADD_NAME,
        StructField {
            name: ADD_NAME.to_string(),
            data_type: DataType::Struct(Box::new(modified_add)),
            nullable: add_field.nullable,
            metadata: add_field.metadata.clone(),
        },
    )?;

    Ok(Arc::new(new_schema))
}

/// Adds `stats_parsed` field after `stats` in the Add action schema.
fn add_stats_parsed_to_add_schema(
    add_schema: &StructType,
    stats_schema: &StructType,
) -> StructType {
    add_schema.with_field_inserted(
        STATS_FIELD,
        StructField::nullable(
            STATS_PARSED_FIELD,
            DataType::Struct(Box::new(stats_schema.clone())),
        ),
    )
}

fn build_add_output_schema(
    config: &StatsTransformConfig,
    add_schema: &StructType,
    stats_schema: &StructType,
) -> StructType {
    let new_schema = if config.write_stats_as_struct {
        add_schema.with_field_inserted(
            STATS_FIELD,
            StructField::nullable(
                STATS_PARSED_FIELD,
                DataType::Struct(Box::new(stats_schema.clone())),
            ),
        )
    } else {
        add_schema.clone()
    };

    if config.write_stats_as_json {
        new_schema
    } else {
        new_schema.with_field_removed(STATS_FIELD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        // Default: writeStatsAsJson=true, writeStatsAsStruct=false (per protocol)
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

    /// Helper to extract the outer and inner transforms from a stats transform expression.
    /// Returns (outer_transform, inner_transform).
    fn extract_transforms(expr: &Expression) -> (&Transform, &Transform) {
        let Expression::Transform(outer) = expr else {
            panic!("Expected outer Transform expression");
        };

        // Outer should be top-level (no input path)
        assert!(
            outer.input_path.is_none(),
            "Outer transform should be top-level"
        );

        // Outer should replace "add" field
        let add_field_transform = outer
            .field_transforms
            .get(ADD_NAME)
            .expect("Outer transform should have 'add' field transform");
        assert!(add_field_transform.is_replace, "Should replace 'add' field");
        assert_eq!(
            add_field_transform.exprs.len(),
            1,
            "Should have exactly one replacement expression"
        );

        // Extract inner transform
        let Expression::Transform(inner) = add_field_transform.exprs[0].as_ref() else {
            panic!("Expected inner Transform expression for 'add' field");
        };

        // Inner should target "add" path
        assert_eq!(
            inner.input_path.as_ref().map(|p| p.to_string()),
            Some("add".to_string()),
            "Inner transform should target 'add' path"
        );

        (outer, inner)
    }

    /// Helper to check if a field transform is a drop (replace with nothing).
    fn is_drop(transform: &Transform, field: &str) -> bool {
        transform
            .field_transforms
            .get(field)
            .map(|ft| ft.is_replace && ft.exprs.is_empty())
            .unwrap_or(false)
    }

    /// Helper to check if a field transform is a replacement with an expression.
    fn is_replacement(transform: &Transform, field: &str) -> bool {
        transform
            .field_transforms
            .get(field)
            .map(|ft| ft.is_replace && ft.exprs.len() == 1)
            .unwrap_or(false)
    }

    #[test]
    fn test_build_transform_with_json_only() {
        // writeStatsAsJson=true, writeStatsAsStruct=false (default)
        // Inner transform: stats=COALESCE, stats_parsed=drop
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: false,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, &stats_schema);

        let (_, inner) = extract_transforms(&transform_expr);

        // stats should be replaced with COALESCE expression
        assert!(
            is_replacement(inner, STATS_FIELD),
            "stats should be replaced"
        );

        // stats_parsed should be dropped
        assert!(
            is_drop(inner, STATS_PARSED_FIELD),
            "stats_parsed should be dropped"
        );
    }

    #[test]
    fn test_build_transform_drops_both_when_false() {
        // writeStatsAsJson=false, writeStatsAsStruct=false
        // Inner transform: stats=drop, stats_parsed=drop
        let config = StatsTransformConfig {
            write_stats_as_json: false,
            write_stats_as_struct: false,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, &stats_schema);

        let (_, inner) = extract_transforms(&transform_expr);

        // Both fields should be dropped
        assert!(is_drop(inner, STATS_FIELD), "stats should be dropped");
        assert!(
            is_drop(inner, STATS_PARSED_FIELD),
            "stats_parsed should be dropped"
        );
    }

    #[test]
    fn test_build_transform_with_both_enabled() {
        // writeStatsAsJson=true, writeStatsAsStruct=true
        // Inner transform: stats=COALESCE, stats_parsed=COALESCE
        let config = StatsTransformConfig {
            write_stats_as_json: true,
            write_stats_as_struct: true,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, &stats_schema);

        let (_, inner) = extract_transforms(&transform_expr);

        // Both fields should be replaced with COALESCE expressions
        assert!(
            is_replacement(inner, STATS_FIELD),
            "stats should be replaced"
        );
        assert!(
            is_replacement(inner, STATS_PARSED_FIELD),
            "stats_parsed should be replaced"
        );
    }

    #[test]
    fn test_build_transform_struct_only() {
        // writeStatsAsJson=false, writeStatsAsStruct=true
        // Inner transform: stats=drop, stats_parsed=COALESCE
        let config = StatsTransformConfig {
            write_stats_as_json: false,
            write_stats_as_struct: true,
        };
        let stats_schema = Arc::new(StructType::new_unchecked([]));
        let transform_expr = build_stats_transform(&config, &stats_schema);

        let (_, inner) = extract_transforms(&transform_expr);

        // stats should be dropped
        assert!(is_drop(inner, STATS_FIELD), "stats should be dropped");

        // stats_parsed should be replaced with COALESCE expression
        assert!(
            is_replacement(inner, STATS_PARSED_FIELD),
            "stats_parsed should be replaced"
        );
    }

    #[test]
    fn test_add_stats_parsed_to_add_schema() {
        let add_schema = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
            StructField::nullable("tags", DataType::STRING),
        ]);

        let stats_schema =
            StructType::new_unchecked([StructField::nullable("numRecords", DataType::LONG)]);

        let result = add_stats_parsed_to_add_schema(&add_schema, &stats_schema);

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

        let result = build_add_output_schema(&config, &add_schema, &stats_schema);

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

        let result = build_add_output_schema(&config, &add_schema, &stats_schema);

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

        let result = build_add_output_schema(&config, &add_schema, &stats_schema);

        // Should have path, stats, and stats_parsed
        let field_names: Vec<&str> = result.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["path", "stats", "stats_parsed"]);
    }
}
