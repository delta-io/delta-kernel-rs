//! Generated columns: top-level columns whose value is defined by a SQL expression stored in the
//! column's `delta.generationExpression` metadata.

use crate::schema::{ColumnMetadataKey, StructType};

/// Whether any top-level column of `schema` carries `delta.generationExpression` metadata, of any
/// value type and regardless of the table's protocol.
pub(crate) fn has_top_level_generation_expression(schema: &StructType) -> bool {
    schema.fields().any(|field| {
        field
            .get_config_value(&ColumnMetadataKey::GenerationExpression)
            .is_some()
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{schema, DataType};
    use crate::unit_test_utils::field_with_generation_expression;

    #[rstest]
    #[case::top_level_string(
        schema! { (field_with_generation_expression("event_year", DataType::INTEGER, "1")) },
        true
    )]
    #[case::top_level_non_string(
        schema! { (field_with_generation_expression("event_year", DataType::INTEGER, 7_i64)) },
        true
    )]
    #[case::nested_only(
        schema! {
            nullable "details": (schema! {
                (field_with_generation_expression("nested", DataType::INTEGER, "1")),
            }),
        },
        false
    )]
    #[case::none(schema! { nullable "event_time": TIMESTAMP }, false)]
    fn detects_top_level_generation_expressions(
        #[case] schema: StructType,
        #[case] expected: bool,
    ) {
        let has_generation_expression = has_top_level_generation_expression(&schema);
        assert_eq!(has_generation_expression, expected);
    }
}
