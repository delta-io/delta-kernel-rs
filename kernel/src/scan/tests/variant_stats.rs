use ::test_utils::add_commit;

use super::*;
use crate::actions::{get_commit_schema, TIGHT_BOUNDS};
use crate::arrow::array::{ArrayRef, AsArray, BinaryArray};
use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
use crate::engine::arrow_expression::opaque::{ArrowOpaqueExpression, ArrowOpaqueExpressionOp};
use crate::expressions::{ParseJsonExpression, ScalarExpressionEvaluator};
use crate::schema::SchemaStructPatchBuilder;
use crate::unit_test_utils::{load_test_table, string_array_to_engine_data};
use crate::{EvaluationHandler, ExpressionEvaluator, PredicateEvaluator};

#[rstest]
#[case::all_struct(StatsOptions::all_struct())]
#[case::struct_columns(StatsOptions::struct_columns(vec![column_name!("id")]))]
fn scan_builder_accepts_variant_min_max_stats_with_struct_stats_without_json_synthesis(
    #[case] stats: StatsOptions,
) {
    let (_, snapshot, _tempdir) = load_test_table("parsed-stats").unwrap();

    snapshot
        .scan_builder()
        .with_stats(stats.with_variant_min_max_stats(true))
        .build()
        .unwrap();
}

#[rstest]
#[case::json_only(StatsOptions::json_only(), "requires struct stats output")]
#[case::none(StatsOptions::none(), "requires struct stats output")]
#[case::all(StatsOptions::all(), "cannot be combined with JSON stats synthesis")]
fn scan_builder_rejects_variant_min_max_stats_without_struct_stats_or_with_json_synthesis(
    #[case] stats: StatsOptions,
    #[case] message: &str,
) {
    let (_, snapshot, _tempdir) = load_test_table("parsed-stats").unwrap();

    let result = snapshot
        .scan_builder()
        .with_stats(stats.with_variant_min_max_stats(true))
        .build();

    assert_result_error_with_message(result, message);
}

/// With `with_variant_min_max_stats`, a VARIANT column's min/max statistic reaches `stats_parsed`
/// from both sources: a commit's stats JSON through the connector's `ParseJson`, and a checkpoint's
/// `stats_parsed`, whose footer reports the statistic as a plain struct of binaries.
#[tokio::test]
async fn scan_metadata_variant_stats_from_commit_json_and_checkpoint_stats_parsed() {
    let (engine, table_root) = setup_variant_stats_table().await;

    let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(1));
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct().with_variant_min_max_stats(true))
        .build()
        .unwrap();

    let expected =
        |path: &str, bound, n: u8| (path.to_string(), bound, vec![1, 0, 0], vec![0x0c, n]);
    assert_eq!(
        collect_variant_bounds(&scan, &engine),
        vec![
            expected("a.parquet", MAX_VALUES, 3),
            expected("a.parquet", MIN_VALUES, 1),
            expected("b.parquet", MAX_VALUES, 6),
            expected("b.parquet", MIN_VALUES, 4),
        ]
    );
}

/// Builds a table whose VARIANT column `v` has min/max stats for `a.parquet` (commit 1, then a
/// checkpoint at version 1) and `b.parquet` (commit 2). Returns an engine with
/// [`VariantStatsEvaluationHandler`] and the table root.
async fn setup_variant_stats_table() -> (DelegatingEngine, String) {
    let table_root = String::from("memory:///");
    let store = Arc::new(InMemory::new());
    let sync = Arc::new(SyncEngine::new_with_store(store.clone()));
    let engine = DelegatingEngine::new(sync.clone()).with_evaluation_handler(Arc::new(
        VariantStatsEvaluationHandler {
            inner: sync.evaluation_handler(),
        },
    ));
    let commit = |actions: &[serde_json::Value]| {
        actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    };

    let table_schema = schema! {
        nullable "id": LONG,
        nullable "v": (DataType::unshredded_variant()),
    };
    let protocol = serde_json::json!({"protocol": {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["variantType"],
        "writerFeatures": ["variantType"],
    }});
    let metadata = serde_json::json!({"metaData": {
        "id": "variant-stats",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::to_string(&table_schema).unwrap(),
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1,
    }});
    // Each bound of `v` is the variant int8 equal to the `id` bound, rendered by `variant`.
    let stats = |min: u8, max: u8, variant: fn(u8) -> serde_json::Value| {
        serde_json::json!({
            "numRecords": 3,
            "nullCount": {"id": 0, "v": 0},
            "minValues": {"id": min, "v": variant(min)},
            "maxValues": {"id": max, "v": variant(max)},
            "tightBounds": true,
        })
    };
    let connector_variant = |n: u8| serde_json::json!(n);
    let add = |path: &str, stats_field: &str, stats: serde_json::Value| {
        serde_json::json!({"add": {
            "path": path,
            "partitionValues": {},
            "size": 1,
            "modificationTime": 1,
            "dataChange": true,
            stats_field: stats,
        }})
    };

    add_commit(
        &table_root,
        store.as_ref(),
        0,
        commit(&[protocol.clone(), metadata.clone()]),
    )
    .await
    .unwrap();
    add_commit(
        &table_root,
        store.as_ref(),
        1,
        commit(&[add(
            "a.parquet",
            "stats",
            stats(1, 3, connector_variant).to_string().into(),
        )]),
    )
    .await
    .unwrap();
    // The checkpoint carries `a.parquet`'s stats only as `stats_parsed`, so they cannot come from
    // JSON.
    let bounds = schema! {
        nullable "id": LONG,
        nullable "v": {
            not_null "metadata": BINARY,
            not_null "value": BINARY,
        },
    };
    let stats_parsed = StructField::nullable(
        STATS_PARSED,
        schema! {
            nullable NUM_RECORDS: LONG,
            nullable NULL_COUNT: { nullable "id": LONG, nullable "v": LONG },
            nullable MIN_VALUES: (bounds.clone()),
            nullable MAX_VALUES: (bounds),
            nullable TIGHT_BOUNDS: BOOLEAN,
        },
    );
    let checkpoint_schema = Arc::new(
        SchemaStructPatchBuilder::new()
            .append_at(["add"], stats_parsed)
            .build(get_commit_schema().as_ref())
            .unwrap(),
    );
    let checkpoint_rows = [
        protocol,
        metadata,
        add(
            "a.parquet",
            STATS_PARSED,
            stats(1, 3, physical_variant_int8_json),
        ),
    ];
    let checkpoint = engine
        .json_handler()
        .parse_json(
            string_array_to_engine_data(StringArray::from_iter_values(
                checkpoint_rows.iter().map(ToString::to_string),
            )),
            checkpoint_schema,
        )
        .unwrap();
    engine
        .parquet_handler()
        .write_parquet_file(
            Url::parse(&table_root)
                .unwrap()
                .join("_delta_log/00000000000000000001.checkpoint.parquet")
                .unwrap(),
            Box::new(std::iter::once(Ok(checkpoint))),
        )
        .unwrap();
    add_commit(
        &table_root,
        store.as_ref(),
        2,
        commit(&[add(
            "b.parquet",
            "stats",
            stats(4, 6, connector_variant).to_string().into(),
        )]),
    )
    .await
    .unwrap();

    (engine, table_root)
}

/// Returns `(path, bound, metadata, value)` of VARIANT column `v` for each selected scan file and
/// each of `minValues`/`maxValues`, sorted.
fn collect_variant_bounds(
    scan: &Scan,
    engine: &dyn Engine,
) -> Vec<(String, &'static str, Vec<u8>, Vec<u8>)> {
    let mut actual = Vec::new();
    for scan_metadata in scan.scan_metadata(engine).unwrap() {
        let (data, selection_vector) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        let paths = get_column!(batch, "path", StringArray);
        let stats_parsed = get_column!(batch, STATS_PARSED, StructArray);
        for bound in [MIN_VALUES, MAX_VALUES] {
            let bounds = get_column!(stats_parsed, bound, StructArray);
            let variant = get_column!(bounds, "v", StructArray);
            let metadata = get_column!(variant, "metadata", BinaryArray);
            let value = get_column!(variant, "value", BinaryArray);
            for row in 0..batch.num_rows() {
                actual.push((
                    paths.value(row).to_string(),
                    bound,
                    metadata.value(row).to_vec(),
                    value.value(row).to_vec(),
                ));
            }
        }
    }
    actual.sort();
    actual
}

/// A test connector's [`EvaluationHandler`]. Its [`ParseJson`] reads each VARIANT statistic `v`
/// written as the bare int8 the variant holds, an encoding the default engine cannot read. All
/// other work forwards to `inner`.
///
/// [`ParseJson`]: crate::expressions::ParseJsonExpression
struct VariantStatsEvaluationHandler {
    inner: Arc<dyn EvaluationHandler>,
}

impl EvaluationHandler for VariantStatsEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: ExpressionRef,
        output_type: DataType,
    ) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
        let expression = Arc::new(
            DecodeVariantStatsTransform
                .transform_expr(&expression)
                .into_owned(),
        );
        self.inner
            .new_expression_evaluator(input_schema, expression, output_type)
    }

    fn new_predicate_evaluator(
        &self,
        input_schema: SchemaRef,
        predicate: PredicateRef,
    ) -> DeltaResult<Arc<dyn PredicateEvaluator>> {
        self.inner.new_predicate_evaluator(input_schema, predicate)
    }

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.inner.create_many(schema, rows)
    }
}

/// Feeds every `ParseJson` input through [`DecodeVariantStatsOp`].
struct DecodeVariantStatsTransform;

impl<'a> ExpressionTransform<'a> for DecodeVariantStatsTransform {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_expr_parse_json(
        &mut self,
        expr: &'a ParseJsonExpression,
    ) -> Cow<'a, ParseJsonExpression> {
        let json_expr = Expr::arrow_opaque(DecodeVariantStatsOp, [expr.json_expr.as_ref().clone()]);
        Cow::Owned(ParseJsonExpression::new(
            json_expr,
            expr.output_schema.clone(),
        ))
    }
}

/// Rewrites each bare-int8 `minValues.v`/`maxValues.v` as its physical struct, which the default
/// engine's JSON reader can parse.
#[derive(Debug, PartialEq)]
struct DecodeVariantStatsOp;

impl ArrowOpaqueExpressionOp for DecodeVariantStatsOp {
    fn name(&self) -> &str {
        "decode_variant_stats"
    }

    fn eval_expr(
        &self,
        args: &[Expr],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        let [json] = args else {
            panic!("expected one argument, got {}", args.len());
        };
        let json = evaluate_expression(json, batch, Some(&DataType::STRING))?;
        let decoded: StringArray = json
            .as_string::<i32>()
            .iter()
            .map(|stats| {
                let mut stats: serde_json::Value = serde_json::from_str(stats?).unwrap();
                for bound in [MIN_VALUES, MAX_VALUES] {
                    if let Some(n) = stats[bound]["v"].as_u64() {
                        stats[bound]["v"] = physical_variant_int8_json(n.try_into().unwrap());
                    }
                }
                Some(stats.to_string())
            })
            .collect();
        Ok(Arc::new(decoded))
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expr],
    ) -> DeltaResult<Scalar> {
        unimplemented!()
    }
}

/// The physical `{metadata, value}` struct of the variant int8 `n` with an empty dictionary. Each
/// binary is a hex string, which is how the default engine's JSON reader reads BINARY.
fn physical_variant_int8_json(n: u8) -> serde_json::Value {
    serde_json::json!({"metadata": "010000", "value": format!("0c{n:02x}")})
}
