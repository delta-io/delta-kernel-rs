//! Preserves source collation bounds independently of the binary data-skipping schema.
//!
//! Reads serialize the complete source stats before projecting parsed columns. Structured writes
//! discover the identifier and bound trees from existing stats without evaluating collations.

use std::sync::{Arc, LazyLock};

use serde_json::Value;

use crate::engine_data::{GetData, RowVisitor, TypedGetData};
use crate::expressions::{
    col, column_name, ColumnName, Expression, ExpressionRef, UnaryExpressionOp,
};
use crate::schema::{
    schema, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::{Engine, KernelError, KernelResult, Result, Snapshot};

const COLLATION_STATS: &str = "statsWithCollation";

/// Returns the complete source parsed-stats schema when it contains collation bounds.
pub(crate) fn source_collation_stats(schema: &StructType) -> Option<&StructType> {
    let DataType::Struct(stats) = schema
        .field_at(&column_name!("add.stats_parsed"))
        .ok()?
        .data_type()
    else {
        return None;
    };
    stats.contains(COLLATION_STATS).then_some(stats)
}

/// Read and output schemas plus a projection that restores JSON before narrowing parsed stats.
pub(crate) struct StatsNormalization {
    pub(crate) read_schema: SchemaRef,
    pub(crate) output_schema: SchemaRef,
    pub(crate) expression: ExpressionRef,
}

/// Builds a normalization for JSON-requesting reads; absent extensions leave the read unchanged.
///
/// Returns an error if the requested Add schema cannot be patched.
pub(crate) fn normalize_collation_stats(
    requested: &StructType,
    source: Option<&StructType>,
) -> KernelResult<Option<StatsNormalization>> {
    if !requested.contains_col(["add", "stats"]) {
        return Ok(None);
    }
    let Some(source) = source else {
        return Ok(None);
    };
    let parsed_path = column_name!("add.stats_parsed");
    let target = requested.field_at(&parsed_path).ok();
    let raw_field = StructField::nullable("stats_parsed", source.clone());
    let read_schema = Arc::new(
        if target.is_some() {
            SchemaStructPatchBuilder::new().replace_at(["add"], "stats_parsed", raw_field)
        } else {
            SchemaStructPatchBuilder::new().append_at(["add"], raw_field)
        }
        .build(requested)?,
    );
    let json = Expression::coalesce([
        col!("add.stats"),
        Expression::unary(UnaryExpressionOp::ToJson, col!("add.stats_parsed")),
    ]);
    let mut patch = ProjectionStructPatchBuilder::new(&read_schema).replace_expr_at(
        ["add"],
        "stats",
        json.clone(),
    );
    patch = match target {
        Some(field) => {
            let DataType::Struct(stats) = field.data_type() else {
                return Err(KernelError::schema("stats_parsed must be a struct"));
            };
            patch.replace_at(
                ["add"],
                "stats_parsed",
                field.clone(),
                Expression::parse_json(json, Arc::new(stats.as_ref().clone())),
            )
        }
        None => patch.drop_at(["add"], "stats_parsed"),
    };
    let (output_schema, expression) = patch.build()?;
    Ok(Some(StatsNormalization {
        read_schema,
        output_schema,
        expression,
    }))
}

/// Extends only the checkpoint stats schema with identifiers and bounds present in the log.
///
/// Returns an error for unreadable log data or non-STRING collation bounds.
pub(super) fn checkpoint_stats_schema(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    binary_stats: SchemaRef,
) -> KernelResult<SchemaRef> {
    let mut visitor = CollationStatsVisitor::default();
    let read_schema = crate::schema::schema_ref! {
        nullable "add": { nullable "path": STRING, nullable "stats": STRING },
    };
    for batch in snapshot.log_segment().read_actions(engine, read_schema)? {
        visitor.visit_rows_of(batch?.actions.as_ref())?;
    }
    if visitor.schema.num_fields() == 0 {
        return Ok(binary_stats);
    }
    Ok(Arc::new(
        SchemaStructPatchBuilder::new()
            .append(StructField::nullable(COLLATION_STATS, visitor.schema))
            .build(&binary_stats)?,
    ))
}

struct CollationStatsVisitor {
    schema: StructType,
}

impl Default for CollationStatsVisitor {
    fn default() -> Self {
        Self { schema: schema! {} }
    }
}

impl RowVisitor for CollationStatsVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<[ColumnName; 2]> =
            LazyLock::new(|| [column_name!("add.path"), column_name!("add.stats")]);
        (&*COLUMNS, &[DataType::STRING, DataType::STRING])
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
        for row in 0..row_count {
            let path: Option<&str> = getters[0].get_opt(row, "add.path")?;
            if path.is_none() {
                continue;
            }
            let json: Option<&str> = getters[1].get_opt(row, "add.stats")?;
            let Some(json) = json else {
                continue;
            };
            // Malformed optional JSON stats cannot supply a structured schema.
            let Ok(stats) = serde_json::from_str::<Value>(json) else {
                continue;
            };
            if let Some(Value::Object(identifiers)) = stats.get(COLLATION_STATS) {
                for (identifier, stats) in identifiers {
                    let mut bounds = serde_json::Map::new();
                    for name in ["minValues", "maxValues"] {
                        if let Some(value @ Value::Object(_)) = stats.get(name) {
                            bounds.insert(name.to_string(), value.clone());
                        }
                    }
                    if bounds.is_empty() {
                        continue;
                    }
                    self.schema = merge_bound_schema(
                        &self.schema,
                        &serde_json::json!({ identifier: bounds }),
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn merge_bound_schema(schema: &StructType, bounds: &Value) -> KernelResult<StructType> {
    let Value::Object(bounds) = bounds else {
        return Err(KernelError::schema("Collation bounds must be objects"));
    };
    let mut fields: Vec<_> = schema.fields().cloned().collect();
    for (name, value) in bounds {
        let existing = fields.iter().position(|field| field.name() == name);
        let data_type = match value {
            Value::Object(_) => {
                let empty = schema! {};
                let child = match existing.map(|index| fields[index].data_type()) {
                    Some(DataType::Struct(child)) => child.as_ref(),
                    None => &empty,
                    Some(_) => {
                        return Err(KernelError::schema("Conflicting collation bound types"))
                    }
                };
                let child = merge_bound_schema(child, value)?;
                if child.num_fields() == 0 {
                    continue;
                }
                DataType::from(child)
            }
            Value::String(_) | Value::Null => {
                if existing.is_some_and(|index| fields[index].data_type() != &DataType::STRING) {
                    return Err(KernelError::schema("Conflicting collation bound types"));
                }
                DataType::STRING
            }
            _ => {
                return Err(KernelError::schema(
                    "Collation bounds must have STRING leaves",
                ))
            }
        };
        let field = StructField::nullable(name, data_type);
        match existing {
            Some(index) => fields[index] = field,
            None => fields.push(field),
        }
    }
    StructType::try_new(fields)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::arrow::array::{StringArray, StructArray};
    use crate::engine::arrow_data::EngineDataArrowExt;
    use crate::engine::sync::SyncEngine;
    use crate::schema::{schema, schema_ref};

    #[test]
    fn discovers_literal_identifiers_and_sparse_nested_bounds() {
        let first = json!({
            "ICU.en_US.72": { "minValues": { "physical.with.dot": { "leaf": "B" } } },
            "spark.UTF8_LCASE.75.1": { "maxValues": { "other": "a" } },
        });
        let schema = merge_bound_schema(&schema! {}, &first).unwrap();
        let schema = merge_bound_schema(
            &schema,
            &json!({ "ICU.en_US.73": { "maxValues": { "physical.with.dot": { "leaf": "Z" } } } }),
        )
        .unwrap();
        for identifier in ["ICU.en_US.72", "ICU.en_US.73", "spark.UTF8_LCASE.75.1"] {
            assert!(schema.contains(identifier));
        }
        assert_eq!(
            schema
                .field_at(&ColumnName::new([
                    "ICU.en_US.72",
                    "minValues",
                    "physical.with.dot",
                    "leaf"
                ]))
                .unwrap()
                .data_type(),
            &DataType::STRING
        );
    }

    #[rstest]
    #[case::json_only(false)]
    #[case::json_and_parsed(true)]
    fn normalization_preserves_source_before_projecting(#[case] parsed: bool) {
        let binary = schema! { nullable "numRecords": LONG };
        let mut requested = schema! { nullable "stats": STRING };
        if parsed {
            requested = SchemaStructPatchBuilder::new()
                .append(StructField::nullable("stats_parsed", binary))
                .build(&requested)
                .unwrap();
        }
        let requested = schema_ref! { nullable "add": (requested) };
        let source = schema! {
            nullable "numRecords": LONG,
            nullable COLLATION_STATS: {
                nullable "ICU.en_US.72": { nullable "minValues": { nullable "col-a": STRING } },
            },
        };
        let normalized = normalize_collation_stats(&requested, Some(&source))
            .unwrap()
            .unwrap();
        assert_eq!(normalized.output_schema, requested);
        assert_eq!(
            source_collation_stats(&normalized.read_schema),
            Some(&source)
        );
        assert!(normalize_collation_stats(&requested, None)
            .unwrap()
            .is_none());
        let struct_only = schema! { nullable "add": { nullable "stats_parsed": (source.clone()) } };
        assert!(normalize_collation_stats(&struct_only, Some(&source))
            .unwrap()
            .is_none());
    }
    #[rstest]
    #[case::struct_only_source(false)]
    #[case::existing_json_wins(true)]
    fn evaluator_preserves_versioned_bounds_before_binary_projection(#[case] has_json: bool) {
        let bounds = json!({
            "ICU.en_US.72": { "minValues": { "col.with.dot": { "leaf": "B" } } },
            "ICU.en_US.73": { "maxValues": { "col.with.dot": { "leaf": "a" } } },
            "spark.UTF8_LCASE.75.1": { "minValues": { "other": "Alice" } },
        });
        let source = schema! {
            nullable "numRecords": LONG,
            nullable COLLATION_STATS: (merge_bound_schema(&schema! {}, &bounds).unwrap()),
        };
        let requested = schema_ref! {
            nullable "add": {
                nullable "stats": STRING,
                nullable "stats_parsed": { nullable "numRecords": LONG },
            },
        };
        let normalization = normalize_collation_stats(&requested, Some(&source))
            .unwrap()
            .unwrap();
        let source_stats = json!({ "numRecords": 2, COLLATION_STATS: bounds });
        let expected = if has_json {
            json!({ "numRecords": 1, COLLATION_STATS: { "custom.ci.1": { "minValues": { "x": "X" } } } })
        } else {
            source_stats.clone()
        };
        let input = json!({ "add": {
            "stats": has_json.then(|| expected.to_string()),
            "stats_parsed": source_stats,
        } });
        let engine = SyncEngine::new();
        let data = engine
            .json_handler()
            .parse_json(
                crate::unit_test_utils::string_array_to_engine_data(StringArray::from(vec![
                    input.to_string()
                ])),
                normalization.read_schema.clone(),
            )
            .unwrap();
        let output = engine
            .evaluation_handler()
            .new_expression_evaluator(
                normalization.read_schema,
                normalization.expression,
                normalization.output_schema.as_ref().clone().into(),
            )
            .unwrap()
            .evaluate(data.as_ref())
            .unwrap()
            .try_into_record_batch()
            .unwrap();
        let add = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let stats = add
            .column_by_name("stats")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actual: Value = serde_json::from_str(stats.value(0)).unwrap();
        // ToJson may materialize missing nullable branches; compare every supplied bound.
        for (identifier, expected_bounds) in expected[COLLATION_STATS].as_object().unwrap() {
            for (bound, expected_values) in expected_bounds.as_object().unwrap() {
                assert_eq!(&actual[COLLATION_STATS][identifier][bound], expected_values);
            }
        }
        assert_eq!(actual["numRecords"], expected["numRecords"]);
        let parsed = add
            .column_by_name("stats_parsed")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(parsed.num_columns(), 1);
    }
}
