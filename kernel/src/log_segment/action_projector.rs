//! Aligns batches of one action kind with a log-replay read schema.

use std::sync::Arc;

use crate::content_tree::struct_expr_from_schema;
use crate::expressions::Expression;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{Engine, EngineData, ExpressionEvaluator, KernelError, KernelResult};

/// Aligns batches of one top-level action with the full read schema a log-replay consumer asked
/// for. An adaptiveMetadata `checkpoint` action supplies each action kind from a different source
/// (`add` from its content tree; `protocol`, `metaData`, `txn`, and `domainMetadata` inline in the
/// action; `txn` and `domainMetadata` also from sidecar files), and each source yields batches
/// holding only its own action. A projector fills the read schema's other top-level actions with
/// nulls.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct ActionProjector {
    evaluator: Arc<dyn ExpressionEvaluator>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ActionProjector {
    /// Builds a projector from `source`, the schema of the batches to project, onto `read_schema`.
    /// Fields of `read_schema.<action>` that `source.<action>` has are copied; every other nullable
    /// field is null.
    ///
    /// # Errors
    /// Returns an error if `action` is not a struct in both schemas, or if `read_schema` has a
    /// non-nullable field `source` cannot supply.
    pub(super) fn try_new(
        engine: &dyn Engine,
        action: &str,
        source: SchemaRef,
        read_schema: SchemaRef,
    ) -> KernelResult<Self> {
        Self::try_new_with(engine, action, source, read_schema, |_| None)
    }

    /// Like [`Self::try_new`], but `field_expr` supplies the expression for the
    /// `read_schema.<action>` fields it names, instead of copying them from `source`.
    ///
    /// # Errors
    /// See [`Self::try_new`].
    pub(super) fn try_new_with(
        engine: &dyn Engine,
        action: &str,
        source: SchemaRef,
        read_schema: SchemaRef,
        field_expr: impl Fn(&str) -> Option<Expression>,
    ) -> KernelResult<Self> {
        let source_fields = action_struct(&source, action)?;
        let action_expr = struct_expr_from_schema(action_struct(&read_schema, action)?, |name| {
            field_expr(name).or_else(|| {
                source_fields
                    .field(name)
                    .is_some()
                    .then(|| Expression::column([action, name]))
            })
        })?;
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

    use super::*;
    use crate::actions::{ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA, REMOVE_FIELD, REMOVE_NAME};
    use crate::arrow::array::{AsArray as _, RecordBatch, StringArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::lit;
    use crate::object_store::memory::InMemory;
    use crate::schema::schema_ref;
    use crate::unit_test_utils::{assert_result_error_with_message, string_array_to_engine_data};

    /// A batch of `add` actions (matching [`LOG_ADD_SCHEMA`]) for `paths`.
    fn adds(engine: &SyncEngine, paths: &[&str]) -> Box<dyn EngineData> {
        let lines: StringArray = paths
            .iter()
            .map(|path| {
                format!(
                    r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":1,"dataChange":true,"stats":"{{}}"}}}}"#
                )
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
            .project(adds(&engine, &["a.parquet", "b.parquet"]).as_ref())
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
            vec![Some("a.parquet".to_string()), Some("b.parquet".to_string())]
        );
        assert_eq!(batch.column_by_name(REMOVE_NAME).unwrap().null_count(), 2);
    }

    #[test]
    fn try_new_with_overrides_the_named_fields() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let projector = ActionProjector::try_new_with(
            &engine,
            ADD_NAME,
            LOG_ADD_SCHEMA.clone(),
            add_without_stats_and_remove(),
            |name| (name == "path").then(|| lit("replaced.parquet")),
        )
        .unwrap();

        let batch = projector
            .project(adds(&engine, &["a.parquet"]).as_ref())
            .unwrap()
            .try_into_record_batch()
            .unwrap();

        assert_eq!(
            add_paths(&batch),
            vec![Some("replaced.parquet".to_string())]
        );
    }

    #[test]
    fn rejects_read_schema_without_the_action() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let read_schema = schema_ref! { (&REMOVE_FIELD) };
        assert_result_error_with_message(
            ActionProjector::try_new(&engine, ADD_NAME, LOG_ADD_SCHEMA.clone(), read_schema),
            "schema has no `add` struct",
        );
    }
}
