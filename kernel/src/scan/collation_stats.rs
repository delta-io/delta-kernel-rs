//! Resolves explicitly requested collation bounds into scan read and output schemas.

use std::sync::Arc;

use crate::actions::{MAX_VALUES, MIN_VALUES, NUM_RECORDS, TIGHT_BOUNDS};
use crate::checkpoint::collation_stats::COLLATION_STATS;
use crate::expressions::ColumnName;
use crate::schema::{
    schema_ref, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::table_configuration::{StatsOutputSchemas, TableConfiguration};
use crate::{KernelError, KernelResult};

/// Adds requested collation bounds to both naming modes, rejecting invalid identifiers or paths.
pub(super) fn extend_output_schemas(
    table: &TableConfiguration,
    schemas: Option<StatsOutputSchemas>,
    requests: &[(String, Vec<ColumnName>)],
) -> KernelResult<Option<StatsOutputSchemas>> {
    if requests.is_empty() {
        return Ok(schemas);
    }
    let mut logical = Vec::with_capacity(requests.len());
    let mut physical = Vec::with_capacity(requests.len());
    for (identifier, columns) in requests {
        if !is_versioned_collation_identifier(identifier) {
            return Err(KernelError::schema(format!(
                "Collation statistics require provider.name.version, got '{identifier}'"
            )));
        }
        for column in columns {
            let field = table.logical_schema_ref().field_at(column)?;
            if field.data_type() != &DataType::STRING
                || column
                    .path()
                    .first()
                    .is_some_and(|name| table.logical_partition_columns().contains(name))
            {
                return Err(KernelError::schema(format!(
                    "Collation statistics require a non-partition STRING column: {column}"
                )));
            }
        }
        let Some(bounds) = table.build_selected_stats_output_schemas(columns, false)? else {
            continue;
        };
        logical.push(identifier_field(identifier, &bounds.logical)?);
        physical.push(identifier_field(identifier, &bounds.physical)?);
    }
    if logical.is_empty() {
        return Ok(schemas);
    }
    let (logical_schema, physical_schema) = match schemas {
        Some(schemas) => (Some(schemas.logical), Some(schemas.physical)),
        None => (None, None),
    };
    let logical = append_collation_stats(
        logical_schema,
        StructField::nullable(COLLATION_STATS, StructType::try_new(logical)?),
    )?;
    let physical = append_collation_stats(
        physical_schema,
        StructField::nullable(COLLATION_STATS, StructType::try_new(physical)?),
    )?;
    Ok(Some(StatsOutputSchemas::try_new(logical, physical)?))
}

/// Adds the output's collation bounds to `read`, returning an error for an invalid schema patch.
pub(super) fn extend_read_schema(
    read: Option<SchemaRef>,
    output: Option<&SchemaRef>,
) -> KernelResult<Option<SchemaRef>> {
    match output.and_then(|schema| schema.field(COLLATION_STATS)) {
        Some(field) => append_collation_stats(read, field.clone()).map(Some),
        None => Ok(read),
    }
}

fn is_versioned_collation_identifier(identifier: &str) -> bool {
    let mut parts = identifier.splitn(3, '.');
    parts.next().is_some_and(|provider| !provider.is_empty())
        && parts.next().is_some_and(|name| !name.is_empty())
        && parts.next().is_some_and(|version| !version.is_empty())
}

fn identifier_field(identifier: &str, bounds: &SchemaRef) -> KernelResult<StructField> {
    let fields = bounds
        .project_as_struct(&[MIN_VALUES, MAX_VALUES])
        .map_err(|_| {
            KernelError::internal_error("Requested STRING bounds are missing from stats schema")
        })?;
    Ok(StructField::nullable(identifier, fields))
}

fn append_collation_stats(
    schema: Option<SchemaRef>,
    field: StructField,
) -> KernelResult<SchemaRef> {
    let schema = schema.unwrap_or_else(|| {
        schema_ref! {
            nullable NUM_RECORDS: LONG,
            nullable TIGHT_BOUNDS: BOOLEAN,
        }
    });
    Ok(Arc::new(
        SchemaStructPatchBuilder::new()
            .append(field)
            .build(&schema)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{col, column_name, lit};
    use crate::scan::state_info::tests::get_state_info_with_stats;
    use crate::scan::StatsOptions;
    use crate::schema::ArrayType;

    #[rstest]
    #[case::dotted_version("test.ci.75.1")]
    #[case::consecutive_dots_in_version("test.ci.75..1")]
    fn collation_read_and_output_schemas_select_exact_bounds_without_predicate_only_fields(
        #[case] identifier: &str,
    ) {
        let state = get_state_info_with_stats(
            schema_ref! {
                nullable "id": LONG,
                nullable "name": STRING,
                nullable "other": LONG,
                nullable "nested": { nullable "child": STRING },
            },
            vec![],
            Some(Arc::new(col!("other").gt(lit(10i64)))),
            &[],
            HashMap::new(),
            vec![],
            StatsOptions::struct_columns(vec![column_name!("id")])
                .with_collation_stats(identifier, [column_name!("name")])
                .with_collation_stats(identifier, [column_name!("nested.child")]),
        )
        .unwrap();
        let read = state.physical_stats_read_schema().unwrap();
        let output = state.physical_stats_output_schema().unwrap();
        assert!(read.contains_col(["minValues", "other"]));
        assert!(!output.contains_col(["minValues", "other"]));
        for schema in [read, output] {
            for bound in [MIN_VALUES, MAX_VALUES] {
                assert!(schema.contains_col([COLLATION_STATS, identifier, bound, "name"]));
                assert!(schema.contains_col([
                    COLLATION_STATS,
                    identifier,
                    bound,
                    "nested",
                    "child"
                ]));
                assert!(!schema.contains_col([COLLATION_STATS, "test.ci.75", bound, "name"]));
                assert!(!schema.contains_col([COLLATION_STATS, identifier, bound, "id"]));
            }
        }
    }

    #[rstest]
    #[case::unversioned("test.ci", column_name!("name"), vec![])]
    #[case::empty_provider(".ci.1", column_name!("name"), vec![])]
    #[case::empty_version("test.ci.", column_name!("name"), vec![])]
    #[case::unresolved("test.ci.1", column_name!("missing"), vec![])]
    #[case::numeric("test.ci.1", column_name!("id"), vec![])]
    #[case::array("test.ci.1", column_name!("array"), vec![])]
    #[case::partition("test.ci.1", column_name!("name"), vec!["name".to_string()])]
    fn collation_stats_reject_invalid_requests(
        #[case] identifier: &str,
        #[case] column: ColumnName,
        #[case] partitions: Vec<String>,
    ) {
        let result = get_state_info_with_stats(
            schema_ref! {
                nullable "id": LONG,
                nullable "name": STRING,
                nullable "array": (ArrayType::new(DataType::STRING, true)),
            },
            partitions,
            None,
            &[],
            HashMap::new(),
            vec![],
            StatsOptions::all_struct().with_collation_stats(identifier, [column]),
        );
        assert!(result.is_err());
    }
}
