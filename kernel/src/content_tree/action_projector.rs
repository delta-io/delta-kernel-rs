//! Aligns batches of one action kind with a log-replay read schema.

use std::sync::Arc;

use super::struct_expr_from_schema;
use crate::expressions::{null_lit, Expression};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{Engine, EngineData, ExpressionEvaluator, KernelError, KernelResult};

/// Pads batches holding one top-level action out to a log-replay read schema, with every other
/// top-level action null. Each source an adaptiveMetadata `checkpoint` action reads from yields
/// batches of a single action kind.
///
/// For example, projecting `add` batches onto a read schema of `{add, remove}` keeps each row's
/// `add` and makes its `remove` null.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct ActionProjector {
    evaluator: Arc<dyn ExpressionEvaluator>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ActionProjector {
    /// Builds a projector from `source`, the schema of the batches to project, onto `read_schema`.
    /// Fields of `read_schema.<action>` that `source.<action>` has are copied; every other nullable
    /// field is null. Only the action's top-level fields are matched, so a nested struct is copied
    /// whole. A null source action stays null.
    ///
    /// For example, projecting `LOG_ADD_SCHEMA` batches onto a read schema whose `add` omits
    /// `stats` copies `add.path`, `add.size`, and the other shared fields, and drops `add.stats`.
    ///
    /// # Errors
    /// Returns an error if `action` is not a struct in both schemas, if a field both have differs
    /// in type, or if `read_schema` has a non-nullable field `source` cannot supply.
    pub(super) fn try_new(
        engine: &dyn Engine,
        action: &str,
        source: SchemaRef,
        read_schema: SchemaRef,
    ) -> KernelResult<Self> {
        let source_fields = action_struct(&source, action)?;
        let fields = action_struct(&read_schema, action)?
            .fields()
            .map(|field| {
                let name = field.name();
                match source_fields.field(name).map(StructField::data_type) {
                    Some(source_type) if source_type == field.data_type() => {
                        Ok(Expression::column([action, name]))
                    }
                    Some(source_type) => Err(KernelError::internal_error(format!(
                        "`{action}.{name}` is {:?} in the read schema but {source_type:?} in the \
                         source",
                        field.data_type()
                    ))),
                    None if field.is_nullable() => Ok(null_lit(field.data_type().clone())),
                    None => Err(KernelError::internal_error(format!(
                        "the source has no `{action}.{name}`, which the read schema requires"
                    ))),
                }
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let action_expr = Expression::struct_with_nullability_from(
            fields,
            Expression::from_pred(Expression::column([action]).is_not_null()),
        );
        let expr = struct_expr_from_schema(&read_schema, |name| {
            (name == action).then(|| action_expr.clone())
        })?;
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            source,
            Arc::new(expr),
            read_schema.as_ref().clone().into(),
        )?;
        Ok(ActionProjector { evaluator })
    }

    /// Projects `data`, a batch matching the projector's source schema, onto its read schema.
    pub(super) fn project(&self, data: &dyn EngineData) -> KernelResult<Box<dyn EngineData>> {
        self.evaluator.evaluate(data)
    }
}

/// The `action` struct of `schema`.
fn action_struct<'a>(schema: &'a StructType, action: &str) -> KernelResult<&'a StructType> {
    match schema.field(action).map(StructField::data_type) {
        Some(DataType::Struct(fields)) => Ok(fields),
        _ => Err(KernelError::internal_error(format!(
            "schema has no `{action}` struct: {schema:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::actions::{Add, ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA, REMOVE_FIELD, REMOVE_NAME};
    use crate::arrow::array::{AsArray as _, RecordBatch, StringArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::schema_ref;
    use crate::unit_test_utils::{
        assert_result_error_with_message, string_array_to_engine_data, Action,
    };

    /// A batch of `add` actions (matching [`LOG_ADD_SCHEMA`]) for `paths`, with a null `add` for
    /// each `None`.
    fn adds(engine: &SyncEngine, paths: &[Option<&str>]) -> Box<dyn EngineData> {
        let lines: StringArray = paths
            .iter()
            .map(|path| match path {
                Some(path) => serde_json::to_string(&Action::Add(Add {
                    path: path.to_string(),
                    size: 1,
                    modification_time: 1,
                    data_change: true,
                    ..Default::default()
                }))
                .unwrap(),
                None => "{}".to_string(),
            })
            .collect::<Vec<_>>()
            .into();
        engine
            .json_handler()
            .parse_json(string_array_to_engine_data(lines), LOG_ADD_SCHEMA.clone())
            .unwrap()
    }

    /// The `add.path` values of `batch`.
    fn add_paths(batch: &RecordBatch) -> Vec<Option<String>> {
        let add = batch.column_by_name(ADD_NAME).unwrap().as_struct();
        let paths = add.column_by_name("path").unwrap().as_string::<i32>();
        paths.iter().map(|p| p.map(str::to_string)).collect()
    }

    /// A read schema holding `add` without its `stats`, followed by `remove`.
    fn add_without_stats_and_remove() -> SchemaRef {
        schema_ref! {
            nullable ADD_NAME: { ..(ADD_SCHEMA.fields().filter(|f| f.name() != "stats")) },
            (&REMOVE_FIELD),
        }
    }

    #[test]
    fn projects_onto_read_schema_with_other_actions_null() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let read_schema = add_without_stats_and_remove();
        let projector = ActionProjector::try_new(
            &engine,
            ADD_NAME,
            LOG_ADD_SCHEMA.clone(),
            read_schema.clone(),
        )
        .unwrap();

        let batch = projector
            .project(adds(&engine, &[Some("a.parquet"), None, Some("b.parquet")]).as_ref())
            .unwrap()
            .try_into_record_batch()
            .unwrap();

        let field_names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let expected_names: Vec<_> = read_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(field_names, expected_names);
        assert_eq!(
            add_paths(&batch),
            vec![
                Some("a.parquet".to_string()),
                None,
                Some("b.parquet".to_string())
            ]
        );
        assert!(batch.column_by_name(ADD_NAME).unwrap().is_null(1));
        assert_eq!(batch.column_by_name(REMOVE_NAME).unwrap().null_count(), 3);
    }

    #[rstest]
    #[case::read_schema_without_action(
        LOG_ADD_SCHEMA.clone(),
        schema_ref! { (&REMOVE_FIELD) },
        "schema has no `add` struct"
    )]
    #[case::source_without_action(
        schema_ref! { (&REMOVE_FIELD) },
        add_without_stats_and_remove(),
        "schema has no `add` struct"
    )]
    #[case::missing_required_field(
        LOG_ADD_SCHEMA.clone(),
        schema_ref! { nullable ADD_NAME: { not_null "missing": STRING } },
        "the source has no `add.missing`"
    )]
    #[case::mismatched_type(
        LOG_ADD_SCHEMA.clone(),
        schema_ref! { nullable ADD_NAME: { not_null "path": LONG } },
        "`add.path` is Primitive(Long) in the read schema but Primitive(String) in the source"
    )]
    fn rejects_schemas_it_cannot_project(
        #[case] source: SchemaRef,
        #[case] read_schema: SchemaRef,
        #[case] message: &str,
    ) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        assert_result_error_with_message(
            ActionProjector::try_new(&engine, ADD_NAME, source, read_schema),
            message,
        );
    }
}
