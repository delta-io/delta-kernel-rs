use std::sync::Arc;

use crate::actions::{get_log_commit_info_schema, CommitInfo, COMMIT_INFO_NAME};
use crate::expressions::{MapData, Scalar, Transform};
use crate::schema::{MapType, StructField, StructType, ToSchema};
use crate::{DataType, Engine, EngineData, Error, Expression, ExpressionRef, IntoEngineData};

use super::Transaction;

/// Builds a list of `(field_name, literal_expression)` pairs covering every [`CommitInfo`]
/// field. Field names match the camelCase schema names produced by the `ToSchema` derive macro.
/// The returned vec preserves CommitInfo schema field order, which callers rely on when
/// inserting kernel-only fields after the last engine field.
fn commit_info_literal_exprs(
    commit_info: CommitInfo,
) -> Result<Vec<(&'static str, ExpressionRef)>, Error> {
    let op_params_map_type = MapType::new(DataType::STRING, DataType::STRING, true);
    Ok(vec![
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
            Arc::new(Expression::literal(
                match commit_info.operation_parameters {
                    Some(map) => Scalar::Map(MapData::try_new(
                        op_params_map_type.clone(),
                        map.into_iter()
                            .map(|(k, v)| (Scalar::String(k), Scalar::String(v))),
                    )?),
                    None => Scalar::Null(DataType::Map(Box::new(op_params_map_type))),
                },
            )),
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
            "engineInfo",
            Arc::new(Expression::literal(commit_info.engine_info)),
        ),
        ("txnId", Arc::new(Expression::literal(commit_info.txn_id))),
    ])
}

impl<S> Transaction<S> {
    pub(super) fn generate_commit_info(
        &self,
        engine: &dyn Engine,
    ) -> Result<Box<dyn EngineData>, Error> {
        let commit_info = CommitInfo::new(
            self.commit_timestamp,
            self.get_in_commit_timestamp(engine)?,
            self.operation.clone(),
            self.engine_info.clone(),
            self.is_blind_append,
        );
        match &self.engine_commit_info {
            Some((engine_commit_info, engine_commit_info_schema)) => {
                let kernel_schema = CommitInfo::to_schema();

                // Step 1: Build output schema - all engine fields first, then any kernel-only
                // fields that are not already present in the engine schema appended at the end.
                let mut output_fields: Vec<StructField> =
                    engine_commit_info_schema.fields().cloned().collect();
                for kernel_field in kernel_schema.fields() {
                    if !engine_commit_info_schema.contains(kernel_field.name()) {
                        output_fields.push(kernel_field.clone());
                    }
                }

                let output_schema = StructType::new_unchecked(output_fields);

                // Step 2: Build literal expressions for each CommitInfo field.
                let literal_exprs = commit_info_literal_exprs(commit_info)?;

                // Step 3: Build Transform. Replacements must be registered before insertions so
                // that for the last engine field (which may itself be replaced), exprs is ordered
                // as [replace_expr, insert_exprs...]. The evaluator emits exprs in declaration
                // order, so the replace value must come first.
                let last_engine_field = engine_commit_info_schema.field_names().last().cloned();
                let mut transform = Transform::new_top_level();

                // First pass: replace fields that already exist in the engine schema.
                for (field_name, expr_ref) in &literal_exprs {
                    if engine_commit_info_schema.contains(*field_name) {
                        transform = transform.with_replaced_field(*field_name, expr_ref.clone());
                    }
                }
                // Second pass: append kernel-only fields after the last engine field.
                for (field_name, expr_ref) in &literal_exprs {
                    if !engine_commit_info_schema.contains(*field_name) {
                        transform = transform
                            .with_inserted_field(last_engine_field.as_deref(), expr_ref.clone());
                    }
                }

                // Step 4: Wrap the transform in a struct expression so the output matches the
                // Delta log action format `{ "commitInfo": { merged fields... } }`, consistent
                // with the None branch which uses `get_log_commit_info_schema()`.
                let wrapped_expr =
                    Expression::struct_from([Arc::new(Expression::transform(transform))]);
                let wrapped_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
                    COMMIT_INFO_NAME,
                    output_schema,
                )]));
                let evaluator = engine.evaluation_handler().new_expression_evaluator(
                    engine_commit_info_schema.clone(),
                    Arc::new(wrapped_expr),
                    wrapped_schema.into(),
                )?;
                evaluator.evaluate(engine_commit_info.as_ref())
            }
            None => commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::actions::CommitInfo;
    use crate::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::schema::{SchemaRef, StructField, StructType, ToSchema};
    use crate::transaction::Transaction;
    use crate::utils::test_utils::load_test_table;
    use crate::{DataType, DeltaResult, Engine, EngineData};

    // ── build_commit_info tests ────────────────────────────────────────────────

    /// Helper: build an Arrow RecordBatch + kernel SchemaRef for use as engine_commit_info.
    fn make_engine_commit_info(
        arrow_fields: Vec<ArrowField>,
        columns: Vec<ArrayRef>,
        kernel_fields: Vec<StructField>,
    ) -> (Box<dyn EngineData>, SchemaRef) {
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
        let batch = RecordBatch::try_new(arrow_schema, columns).expect("valid RecordBatch");
        let schema = Arc::new(StructType::new_unchecked(kernel_fields));
        (Box::new(ArrowEngineData::new(batch)), schema)
    }

    /// Helper: extract the inner "commitInfo" StructArray from a top-level RecordBatch.
    /// Both branches of `build_commit_info` produce `{ "commitInfo": { ... } }`.
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

    /// Helper: pull a non-null string value from a named column in a StructArray.
    fn get_str<'a>(s: &'a StructArray, col: &str) -> &'a str {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("field '{col}' is not a StringArray"))
            .value(0)
    }

    /// Helper: pull a non-null i64 value from a named column in a StructArray.
    fn get_i64(s: &StructArray, col: &str) -> i64 {
        s.column_by_name(col)
            .unwrap_or_else(|| panic!("field '{col}' not found"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("field '{col}' is not an Int64Array"))
            .value(0)
    }

    /// Create a transaction with the given engine_commit_info, using the shared test table.
    fn make_txn(
        engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    ) -> DeltaResult<(Arc<dyn Engine>, Transaction)> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("WRITE".to_string())
            .with_commit_info(engine_commit_info);
        Ok((engine, txn))
    }

    /// Case 1: no engine_commit_info -- output is the kernel CommitInfo wrapped in a "commitInfo"
    /// outer struct, matching the Delta log action format produced by `get_log_commit_info_schema`.
    #[test]
    fn test_build_commit_info_none_branch() -> DeltaResult<()> {
        let (engine, txn) = make_txn(None)?;
        let result =
            ArrowEngineData::try_from_engine_data(txn.generate_commit_info(engine.as_ref())?)?;
        let ci = commit_info_struct(&result);

        let kernel_schema = CommitInfo::to_schema();
        assert_eq!(ci.num_columns(), kernel_schema.fields().count());
        assert_eq!(get_str(ci, "operation"), "WRITE");
        assert!(!get_str(ci, "kernelVersion").is_empty());
        assert!(!get_str(ci, "txnId").is_empty());
        Ok(())
    }

    /// Case 2: engine schema has fields that are fully disjoint from CommitInfo -- all CommitInfo
    /// fields are appended after the engine-only fields, in CommitInfo schema order.
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
            vec![
                StructField::not_null("customApp", DataType::STRING),
                StructField::not_null("customVersion", DataType::LONG),
            ],
        );
        let (engine, txn) = make_txn(Some((data, schema)))?;

        let result =
            ArrowEngineData::try_from_engine_data(txn.generate_commit_info(engine.as_ref())?)?;
        let ci = commit_info_struct(&result);

        // Engine fields are first and their values pass through unchanged.
        assert_eq!(ci.fields()[0].name(), "customApp");
        assert_eq!(ci.fields()[1].name(), "customVersion");
        assert_eq!(get_str(ci, "customApp"), "myApp");
        assert_eq!(get_i64(ci, "customVersion"), 42);

        // All CommitInfo fields are appended -- total = 2 engine + 8 CommitInfo.
        assert_eq!(
            ci.num_columns(),
            2 + CommitInfo::to_schema().fields().count()
        );

        // Spot-check a couple of the appended kernel fields.
        assert_eq!(get_str(ci, "operation"), "WRITE");
        assert!(!get_str(ci, "kernelVersion").is_empty());
        Ok(())
    }

    /// Case 3: engine schema contains every CommitInfo field (minus the map field) with stale
    /// values -- all overlapping fields must be replaced by kernel values, no new fields added
    /// (except operationParameters which the engine schema omits).
    #[test]
    fn test_build_commit_info_full_overlap() -> DeltaResult<()> {
        let (data, schema) = make_engine_commit_info(
            vec![
                ArrowField::new("timestamp", ArrowDataType::Int64, true),
                ArrowField::new("inCommitTimestamp", ArrowDataType::Int64, true),
                ArrowField::new("operation", ArrowDataType::Utf8, true),
                ArrowField::new("kernelVersion", ArrowDataType::Utf8, true),
                ArrowField::new("isBlindAppend", ArrowDataType::Boolean, true),
                ArrowField::new("engineInfo", ArrowDataType::Utf8, true),
                ArrowField::new("txnId", ArrowDataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Some(0i64)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["v0.0.0"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![None::<bool>])) as ArrayRef,
                Arc::new(StringArray::from(vec!["stale_engine"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["stale_txn"])) as ArrayRef,
            ],
            vec![
                StructField::nullable("timestamp", DataType::LONG),
                StructField::nullable("inCommitTimestamp", DataType::LONG),
                StructField::nullable("operation", DataType::STRING),
                StructField::nullable("kernelVersion", DataType::STRING),
                StructField::nullable("isBlindAppend", DataType::BOOLEAN),
                StructField::nullable("engineInfo", DataType::STRING),
                StructField::nullable("txnId", DataType::STRING),
            ],
        );
        let (engine, txn) = make_txn(Some((data, schema)))?;

        let result =
            ArrowEngineData::try_from_engine_data(txn.generate_commit_info(engine.as_ref())?)?;
        let ci = commit_info_struct(&result);

        // Stale values must be replaced by kernel values.
        assert_ne!(get_str(ci, "operation"), "STALE_OP");
        assert_eq!(get_str(ci, "operation"), "WRITE");
        assert_ne!(get_str(ci, "kernelVersion"), "v0.0.0");
        assert!(!get_str(ci, "kernelVersion").is_empty());
        assert_ne!(get_str(ci, "txnId"), "stale_txn");
        assert!(!get_str(ci, "txnId").is_empty());

        // The only added field is operationParameters (not in engine schema).
        // Total columns = 7 engine fields + 1 appended (operationParameters).
        assert_eq!(ci.num_columns(), 8);
        Ok(())
    }

    /// Case 4: engine schema has partial overlap -- overlapping fields are replaced, engine-only
    /// fields pass through, and remaining CommitInfo fields are appended after the last engine field.
    #[test]
    fn test_build_commit_info_partial_overlap() -> DeltaResult<()> {
        let (data, schema) = make_engine_commit_info(
            vec![
                ArrowField::new("timestamp", ArrowDataType::Int64, true),
                ArrowField::new("operation", ArrowDataType::Utf8, true),
                ArrowField::new("myCustomField", ArrowDataType::Utf8, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Some(0i64)])) as ArrayRef,
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["keep_me"])) as ArrayRef,
            ],
            vec![
                StructField::nullable("timestamp", DataType::LONG),
                StructField::nullable("operation", DataType::STRING),
                StructField::not_null("myCustomField", DataType::STRING),
            ],
        );
        let (engine, txn) = make_txn(Some((data, schema)))?;

        let result =
            ArrowEngineData::try_from_engine_data(txn.generate_commit_info(engine.as_ref())?)?;
        let ci = commit_info_struct(&result);

        // Engine-only field passes through unchanged.
        assert_eq!(get_str(ci, "myCustomField"), "keep_me");

        // Overlapping fields are replaced with kernel values.
        assert_ne!(get_str(ci, "operation"), "STALE_OP");
        assert_eq!(get_str(ci, "operation"), "WRITE");

        // Engine fields keep their original schema positions (first 3 columns).
        assert_eq!(ci.fields()[0].name(), "timestamp");
        assert_eq!(ci.fields()[1].name(), "operation");
        assert_eq!(ci.fields()[2].name(), "myCustomField");

        // Remaining CommitInfo fields (6 not in engine schema) are appended after myCustomField.
        // Total = 3 engine fields + 6 kernel-only fields.
        assert_eq!(
            ci.num_columns(),
            3 + CommitInfo::to_schema().fields().count() - 2
        );
        Ok(())
    }

    /// Case 5: engine schema is empty -- all CommitInfo fields are prepended (which, with no engine
    /// fields preceding them, is equivalent to producing the full CommitInfo schema).
    #[test]
    fn test_build_commit_info_empty_engine_schema() -> DeltaResult<()> {
        // A 0-row, 0-column RecordBatch with an empty kernel schema.
        let empty_batch = RecordBatch::new_empty(Arc::new(ArrowSchema::empty()));
        let empty_schema = Arc::new(StructType::new_unchecked(Vec::<StructField>::new()));
        let (engine, txn) = make_txn(Some((
            Box::new(ArrowEngineData::new(empty_batch)),
            empty_schema,
        )))?;

        let result =
            ArrowEngineData::try_from_engine_data(txn.generate_commit_info(engine.as_ref())?)?;
        let ci = commit_info_struct(&result);

        // With no engine fields, the inner schema matches CommitInfo::to_schema().
        let kernel_schema = CommitInfo::to_schema();
        assert_eq!(ci.num_columns(), kernel_schema.fields().count());

        // Column order matches CommitInfo schema field order.
        for (i, field) in kernel_schema.fields().enumerate() {
            assert_eq!(ci.fields()[i].name(), field.name());
        }
        Ok(())
    }
}
