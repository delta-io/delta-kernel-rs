//! Generated columns: top-level columns whose value is defined by a SQL expression stored in the
//! column's `delta.generationExpression` metadata.

use crate::schema::{ColumnMetadataKey, StructType};
use crate::KernelResult;

/// A [generated column]: a top-level column whose value is defined by a SQL expression over the
/// row's other columns.
///
/// The expression is stored in the column's `delta.generationExpression` metadata. Only top-level
/// columns of a table whose protocol supports the `generatedColumns` writer feature are generated
/// columns.
///
/// [generated column]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedColumn {
    name: String,
    raw_sql: String,
}

impl GeneratedColumn {
    /// The generated column's logical name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The column's generation expression, verbatim from its `delta.generationExpression`
    /// metadata. The SQL refers to logical column names.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }
}

/// Returns the top-level columns of `schema` that declare a generation expression, in schema
/// order.
///
/// # Errors
///
/// Returns a schema error if a top-level column's `delta.generationExpression` metadata is not a
/// string.
pub(crate) fn collect_generated_columns(schema: &StructType) -> KernelResult<Vec<GeneratedColumn>> {
    let mut generated_columns = Vec::new();
    for field in schema.fields() {
        let key = ColumnMetadataKey::GenerationExpression;
        if let Some(raw_sql) = field.get_string_config_value(&key)? {
            generated_columns.push(GeneratedColumn {
                name: field.name().clone(),
                raw_sql: raw_sql.to_string(),
            });
        }
    }
    Ok(generated_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{schema, DataType};
    use crate::unit_test_utils::{
        assert_result_error_with_message, field_with_generation_expression,
    };

    #[test]
    fn collects_top_level_generated_columns_in_schema_order() {
        let schema = schema! {
            (field_with_generation_expression("event_year", DataType::INTEGER, "YEAR(event_time)")),
            nullable "event_time": TIMESTAMP,
            nullable "details": (schema! {
                (field_with_generation_expression("nested", DataType::INTEGER, "1")),
            }),
            (field_with_generation_expression(
                "event_date",
                DataType::DATE,
                "CAST(event_time AS DATE)"
            )),
        };

        let generated_columns = collect_generated_columns(&schema).unwrap();

        let discovered: Vec<_> = generated_columns
            .iter()
            .map(|column| (column.name(), column.raw_sql()))
            .collect();
        let expected = [
            ("event_year", "YEAR(event_time)"),
            ("event_date", "CAST(event_time AS DATE)"),
        ];
        assert_eq!(discovered, expected);
    }

    #[test]
    fn rejects_non_string_generation_expression() {
        let schema = schema! {
            (field_with_generation_expression("event_year", DataType::INTEGER, 7_i64)),
        };

        let result = collect_generated_columns(&schema);

        assert_result_error_with_message(result, "non-string `delta.generationExpression`");
    }
}
