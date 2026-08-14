use std::collections::HashMap;
use std::sync::Arc;

use super::Transaction;
use crate::actions::{CommitInfo, COMMIT_INFO_NAME, LOG_COMMIT_INFO_SCHEMA};
use crate::expressions::{MapData, Scalar};
use crate::schema::{schema_ref, MapType, ToSchema};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::{
    DataType, DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, IntoEngineData,
};

/// Builds a `(field_name, literal_expression)` pair for every [`CommitInfo`] field. Field names
/// match the camelCase `ToSchema` names. The pairs keep CommitInfo schema order, which the merge
/// relies on to append kernel-only fields after the engine fields.
fn commit_info_literal_exprs(
    commit_info: CommitInfo,
) -> DeltaResult<Vec<(&'static str, ExpressionRef)>> {
    // operationParameters and operationMetrics are `map<string, string>` with non-nullable values.
    // The type must match `LOG_COMMIT_INFO_SCHEMA`, or the evaluator rejects the literal.
    let map_type = MapType::new(DataType::STRING, DataType::STRING, false);
    let map_literal = |map: Option<HashMap<String, String>>| -> DeltaResult<Expression> {
        let scalar = match map {
            Some(map) => Scalar::Map(MapData::try_new(
                map_type.clone(),
                map.into_iter()
                    .map(|(k, v)| (Scalar::String(k), Scalar::String(v))),
            )?),
            None => Scalar::null(map_type.clone()),
        };
        Ok(Expression::literal(scalar))
    };

    let literal_exprs = vec![
        (
            "timestamp",
            Arc::new(Expression::literal(commit_info.timestamp)),
        ),
        (
            "inCommitTimestamp",
            Arc::new(Expression::literal(commit_info.in_commit_timestamp)),
        ),
        (
            "operation",
            Arc::new(Expression::literal(commit_info.operation)),
        ),
        (
            "operationParameters",
            Arc::new(map_literal(commit_info.operation_parameters)?),
        ),
        (
            "kernelVersion",
            Arc::new(Expression::literal(commit_info.kernel_version)),
        ),
        (
            "isBlindAppend",
            Arc::new(Expression::literal(commit_info.is_blind_append)),
        ),
        (
            "operationMetrics",
            Arc::new(map_literal(commit_info.operation_metrics)?),
        ),
        (
            "engineInfo",
            Arc::new(Expression::literal(commit_info.engine_info)),
        ),
        ("txnId", Arc::new(Expression::literal(commit_info.txn_id))),
    ];
    let expected_expr_len = CommitInfo::to_schema().fields().len();
    if literal_exprs.len() != expected_expr_len {
        return Err(Error::internal_error(format!(
            "commit_info_literal_exprs produced {} expressions but CommitInfo has \
             {expected_expr_len} fields; update this function when CommitInfo fields change",
            literal_exprs.len()
        )));
    }
    Ok(literal_exprs)
}

impl<S> Transaction<S> {
    /// Builds the `commitInfo` action for this transaction.
    ///
    /// The kernel-managed fields come from `commit_info`. If the caller added fields through
    /// [`with_additional_commit_info`](Transaction::with_additional_commit_info), kernel merges
    /// them: engine fields stay, a field that collides with a kernel field takes kernel's value,
    /// and kernel-only fields are appended in schema order.
    pub(super) fn build_commit_info_action(
        &self,
        engine: &dyn Engine,
        commit_info: CommitInfo,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let Some((engine_commit_info, engine_commit_info_schema)) =
            &self.commit_info_options.additional_commit_info
        else {
            return commit_info.into_engine_data(LOG_COMMIT_INFO_SCHEMA.clone(), engine);
        };

        let kernel_schema = CommitInfo::to_schema();
        let literal_exprs = commit_info_literal_exprs(commit_info)?;

        // A kernel field that collides with an engine field replaces it in place. Kernel-only
        // fields are appended after, in CommitInfo schema order.
        let mut patch = ProjectionStructPatchBuilder::new(engine_commit_info_schema);
        for (field_name, expr_ref) in &literal_exprs {
            let field = kernel_schema.field(*field_name).cloned().ok_or_else(|| {
                Error::internal_error(format!("CommitInfo schema is missing field '{field_name}'"))
            })?;
            if engine_commit_info_schema.contains(*field_name) {
                patch = patch.replace(*field_name, field, expr_ref.clone());
            }
        }
        for (field_name, expr_ref) in &literal_exprs {
            let field = kernel_schema.field(*field_name).cloned().ok_or_else(|| {
                Error::internal_error(format!("CommitInfo schema is missing field '{field_name}'"))
            })?;
            if !engine_commit_info_schema.contains(*field_name) {
                patch = patch.append(field, expr_ref.clone());
            }
        }
        let (output_schema, patch) = patch.build()?;

        // Wrap in `{ "commitInfo": { ... } }`, like the no-extras branch via
        // `LOG_COMMIT_INFO_SCHEMA`.
        let wrapped_expr = Expression::struct_from([patch]);
        let wrapped_schema = schema_ref! { nullable (COMMIT_INFO_NAME): (output_schema) };
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            engine_commit_info_schema.clone(),
            Arc::new(wrapped_expr),
            wrapped_schema.into(),
        )?;
        evaluator.evaluate(engine_commit_info.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::actions::CommitInfo;
    use crate::arrow::array::{
        Array, ArrayRef, BooleanArray, Int64Array, MapArray, StringArray, StructArray,
    };
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoKernel;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::schema::{Schema, SchemaRef, ToSchema};
    use crate::transaction::{CommitInfoClientOptions, Transaction};
    use crate::unit_test_utils::load_test_table;
    use crate::utils::FoldWithOption as _;
    use crate::{DeltaResult, Engine, EngineData};

    /// Kernel `CommitInfo` mirroring what `Transaction::commit` produces.
    fn make_kernel_commit_info() -> CommitInfo {
        CommitInfo::new(
            1_700_000_000_000i64,
            Some(134_000_000i64),
            Some("WRITE".to_string()),
            Some("test_engine/1.0".to_string()),
            false,
        )
    }

    /// Build an Arrow `RecordBatch` + kernel `SchemaRef` for use as engine commit info.
    fn make_engine_commit_info(
        arrow_fields: Vec<ArrowField>,
        columns: Vec<ArrayRef>,
    ) -> (Box<dyn EngineData>, SchemaRef) {
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let kernel_schema: Schema = arrow_schema.as_ref().try_into_kernel().unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(arrow_schema), columns).expect("valid RecordBatch");
        (
            Box::new(ArrowEngineData::new(batch)),
            Arc::new(kernel_schema),
        )
    }

    /// Extract the inner "commitInfo" StructArray. Both branches produce `{ "commitInfo": {...} }`.
    fn commit_info_struct(result: &ArrowEngineData) -> &StructArray {
        let batch = result.record_batch();
        assert_eq!(
            batch.num_columns(),
            1,
            "expected single 'commitInfo' column"
        );
        assert_eq!(batch.schema().field(0).name(), "commitInfo");
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("commitInfo column should be a StructArray")
    }

    fn get_str<'a>(s: &'a StructArray, col: &str) -> &'a str {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("field '{col}' is not a StringArray"))
            .value(0)
    }

    fn get_i64(s: &StructArray, col: &str) -> i64 {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("field '{col}' is not an Int64Array"))
            .value(0)
    }

    fn get_map(s: &StructArray, col: &str) -> StructArray {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap_or_else(|| panic!("field '{col}' is not a MapArray"))
            .value(0)
    }

    fn get_bool(s: &StructArray, col: &str) -> bool {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| panic!("field '{col}' is not a BooleanArray"))
            .value(0)
    }

    /// Transaction over the shared test table, setting any extra fields through
    /// `CommitInfoClientOptions::with_additional_commit_info`.
    fn make_txn(
        engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    ) -> DeltaResult<(Arc<dyn Engine>, Transaction)> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("WRITE".to_string())
            .fold_with(engine_commit_info, |txn, (data, schema)| {
                txn.with_commit_info_options(
                    CommitInfoClientOptions::new().with_additional_commit_info(data, schema),
                )
            });
        Ok((engine, txn))
    }

    /// No engine commit info: output is the kernel `CommitInfo` wrapped in a `commitInfo` struct.
    #[test]
    fn test_build_commit_info_none_branch() -> DeltaResult<()> {
        let (engine, txn) = make_txn(None)?;
        let result = ArrowEngineData::try_from_engine_data(
            txn.build_commit_info_action(engine.as_ref(), make_kernel_commit_info())?,
        )?;
        let ci = commit_info_struct(&result);

        assert_eq!(ci.num_columns(), CommitInfo::to_schema().fields().count());
        assert_eq!(get_str(ci, "operation"), "WRITE");
        assert!(!get_str(ci, "kernelVersion").is_empty());
        assert!(!get_str(ci, "txnId").is_empty());
        Ok(())
    }

    /// Engine fields disjoint from CommitInfo: kernel fields are appended after the engine fields,
    /// in schema order, and the engine values are unchanged.
    #[test]
    fn test_build_commit_info_disjoint_schemas() -> DeltaResult<()> {
        let (data, schema) = make_engine_commit_info(
            vec![
                ArrowField::new("customApp", ArrowDataType::Utf8, false),
                ArrowField::new("customVersion", ArrowDataType::Int64, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["myApp"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42i64])) as ArrayRef,
            ],
        );
        let (engine, txn) = make_txn(Some((data, schema)))?;
        let result = ArrowEngineData::try_from_engine_data(
            txn.build_commit_info_action(engine.as_ref(), make_kernel_commit_info())?,
        )?;
        let ci = commit_info_struct(&result);

        assert_eq!(
            ci.num_columns(),
            2 + CommitInfo::to_schema().fields().count()
        );
        assert_eq!(ci.fields()[0].name(), "customApp");
        assert_eq!(ci.fields()[1].name(), "customVersion");
        assert_eq!(get_str(ci, "customApp"), "myApp");
        assert_eq!(get_i64(ci, "customVersion"), 42);

        assert_eq!(get_str(ci, "operation"), "WRITE");
        assert!(!get_str(ci, "kernelVersion").is_empty());
        assert_eq!(get_map(ci, "operationParameters").len(), 0);
        assert!(uuid::Uuid::parse_str(get_str(ci, "txnId")).is_ok());
        assert!(get_i64(ci, "timestamp") > 0);
        assert_eq!(get_i64(ci, "inCommitTimestamp"), 134_000_000);
        assert_eq!(get_str(ci, "engineInfo"), "test_engine/1.0");
        assert!(!get_bool(ci, "isBlindAppend"));
        Ok(())
    }

    /// Overlap: a colliding field takes kernel's value; engine-only fields stay; kernel-only fields
    /// are appended after.
    #[test]
    fn test_build_commit_info_overlap_replaced_by_kernel() -> DeltaResult<()> {
        let (data, schema) = make_engine_commit_info(
            vec![
                ArrowField::new("operation", ArrowDataType::Utf8, true),
                ArrowField::new("myCustomField", ArrowDataType::Utf8, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["keep_me"])) as ArrayRef,
            ],
        );
        let (engine, txn) = make_txn(Some((data, schema)))?;
        let result = ArrowEngineData::try_from_engine_data(
            txn.build_commit_info_action(engine.as_ref(), make_kernel_commit_info())?,
        )?;
        let ci = commit_info_struct(&result);

        // Engine-only field passes through; the colliding field takes kernel's value.
        assert_eq!(get_str(ci, "myCustomField"), "keep_me");
        assert_eq!(get_str(ci, "operation"), "WRITE");
        // Engine fields keep their positions; kernel-only fields are appended after.
        assert_eq!(ci.fields()[0].name(), "operation");
        assert_eq!(ci.fields()[1].name(), "myCustomField");
        // 2 engine fields + (kernel fields - 1 overlap) appended.
        assert_eq!(
            ci.num_columns(),
            2 + CommitInfo::to_schema().fields().count() - 1
        );
        Ok(())
    }
}
